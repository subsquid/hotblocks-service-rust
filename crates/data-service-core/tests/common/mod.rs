//! Shared scaffolding for the observability conformance tests: a scripted
//! source the test steps through, and a scrape reader.
//!
//! Every assertion here reads `/metrics` over HTTP rather than the `Metrics`
//! handle — INV-31 is about what an orchestrator can see, and an in-process
//! getter would pass even if the series were never registered.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use futures::stream::BoxStream;
use tokio::sync::Notify;

pub fn block(number: u64, hash: &str, parent_number: u64, parent_hash: &str) -> Block {
    let json = format!("{{\"number\":{number},\"hash\":\"{hash}\"}}\n");
    Block {
        number,
        hash: hash.to_string(),
        parent_number,
        parent_hash: parent_hash.to_string(),
        timestamp: Some(number * 1000),
        json_line_zstd: Bytes::from(zstd::encode_all(json.as_bytes(), 1).unwrap()),
        timings: None,
    }
}

/// `number` linked to `number - 1`, both hashes derived from the height.
pub fn linked(number: u64) -> Block {
    block(
        number,
        &format!("h{number}"),
        number - 1,
        &format!("h{}", number - 1),
    )
}

/// One thing the head stream does next.
pub enum Step {
    Batch(Vec<Block>, Option<BlockRef>),
    Fork(Vec<BlockRef>),
    Error(&'static str),
    /// Hold the stream open until the test releases it.
    Gate(Arc<Notify>),
}

impl Step {
    pub fn batch(blocks: Vec<Block>) -> Step {
        Step::Batch(blocks, None)
    }

    pub fn reporting(blocks: Vec<Block>, finalized: &Block) -> Step {
        Step::Batch(blocks, Some(finalized.block_ref()))
    }
}

/// A source whose head stream plays a script once. The steps are shared, so a
/// session torn down mid-script does not replay what it already consumed — a
/// re-opened stream sees only what is left, and an empty script idles.
#[derive(Clone)]
pub struct Scripted {
    seed: Arc<Mutex<Block>>,
    head: Arc<Mutex<VecDeque<Step>>>,
    /// Batches the *finalized* stream serves, for a window-underflow read.
    finalized: Arc<Vec<Result<Vec<Block>, String>>>,
}

impl Scripted {
    pub fn new(seed: Block, steps: Vec<Step>) -> Self {
        Scripted {
            seed: Arc::new(Mutex::new(seed)),
            head: Arc::new(Mutex::new(steps.into())),
            finalized: Arc::new(Vec::new()),
        }
    }

    /// Change what the next T1 seeds from.
    pub fn reseed(&self, seed: Block) {
        *self.seed.lock().unwrap() = seed;
    }

    fn seed(&self) -> Block {
        self.seed.lock().unwrap().clone()
    }

    /// What a window-underflow acquisition serves, batch by batch; an `Err`
    /// entry ends the acquisition after the prefix above it.
    pub fn with_backfill(mut self, batches: Vec<Result<Vec<Block>, String>>) -> Self {
        self.finalized = Arc::new(batches);
        self
    }
}

#[async_trait]
impl DataSource for Scripted {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed().block_ref())
    }

    fn get_finalized_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let seed = self.seed();
        // T1 asks for the seed alone; anything else is a window-underflow read.
        if req.to == Some(seed.number) && req.from == seed.number {
            return Box::pin(async_stream::stream! {
                yield Ok(BlockBatch {
                    blocks: vec![seed.clone()],
                    finalized_head: Some(seed.block_ref()),
                });
            });
        }
        let batches = Arc::clone(&self.finalized);
        Box::pin(async_stream::stream! {
            for batch in batches.iter() {
                match batch {
                    Ok(blocks) => yield Ok(BlockBatch {
                        blocks: blocks.clone(),
                        finalized_head: None,
                    }),
                    Err(message) => {
                        yield Err(StreamError::Other(anyhow::anyhow!("{message}")));
                        return;
                    }
                }
            }
        })
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let head = Arc::clone(&self.head);
        Box::pin(async_stream::stream! {
            loop {
                let step = head.lock().unwrap().pop_front();
                match step {
                    None => std::future::pending::<()>().await,
                    Some(Step::Gate(gate)) => gate.notified().await,
                    Some(Step::Error(message)) => {
                        yield Err(StreamError::Other(anyhow::anyhow!("{message}")));
                        return;
                    }
                    Some(Step::Fork(previous_blocks)) => {
                        yield Err(StreamError::Fork { previous_blocks });
                        return;
                    }
                    Some(Step::Batch(blocks, finalized_head)) => {
                        yield Ok(BlockBatch { blocks, finalized_head });
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Scrape
// ---------------------------------------------------------------------------

/// One `/metrics` scrape, keyed by series name plus its sorted label set.
pub struct Scrape(HashMap<String, f64>);

impl Scrape {
    pub async fn read(port: u16) -> Scrape {
        let text = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
            .await
            .expect("scrape /metrics")
            .text()
            .await
            .expect("scrape body");
        Scrape::parse(&text)
    }

    pub fn parse(text: &str) -> Scrape {
        let mut samples = HashMap::new();
        for line in text.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let (key, value) = match line.rsplit_once(' ') {
                Some(split) => split,
                None => continue,
            };
            if let Ok(value) = value.parse::<f64>() {
                samples.insert(key.to_string(), value);
            }
        }
        Scrape(samples)
    }

    /// A registered sample; panics if the series is absent, which is itself the
    /// INV-31 failure a level-that-is-only-logged produces.
    pub fn get(&self, key: &str) -> f64 {
        *self
            .0
            .get(key)
            .unwrap_or_else(|| panic!("no sample {key} on the scrape surface"))
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

/// Scrape until `done` accepts a sample set, or fail naming what was last seen.
pub async fn scrape_until(port: u16, what: &str, done: impl Fn(&Scrape) -> bool) -> Scrape {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let scrape = Scrape::read(port).await;
        if done(&scrape) {
            return scrape;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the scrape surface never showed {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
