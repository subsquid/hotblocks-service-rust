//! Scripted upstream (HC-3) for differential tests.
//!
//! States a history as a literal list of deliveries, so the SUT and the
//! reference model can be driven through exactly the same one and their
//! observable state compared. The simulator (`crate::sim`) generates
//! well-formed histories; this is for the hand-written pathological ones.

use crate::model::{ApplyOutcome, ModelBlock, RefModel};
use async_trait::async_trait;
use bytes::Bytes;
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// One scripted head-stream delivery.
pub struct Event {
    pub blocks: Vec<ModelBlock>,
    pub finalized_head: Option<BlockRef>,
}

impl Event {
    /// A delivery carrying no finality report.
    pub fn blocks(blocks: Vec<ModelBlock>) -> Self {
        Event {
            blocks,
            finalized_head: None,
        }
    }

    pub fn with_finality(mut self, r: BlockRef) -> Self {
        self.finalized_head = Some(r);
        self
    }
}

/// The payload bytes a model block stands for. Deterministic, so the same
/// linkage tuple always produces the same record.
pub fn block_of(b: &ModelBlock) -> Block {
    let json = format!("{{\"number\":{}}}\n", b.number);
    Block {
        number: b.number,
        hash: b.hash.clone(),
        parent_number: b.parent_number,
        parent_hash: b.parent_hash.clone(),
        timestamp: Some(b.number * 1000),
        json_line_zstd: Bytes::from(zstd::encode_all(json.as_bytes(), 1).unwrap()),
        timings: None,
    }
}

pub fn mb(number: u64, hash: &str, parent_number: u64, parent_hash: &str) -> ModelBlock {
    ModelBlock {
        number,
        hash: hash.to_string(),
        parent_number,
        parent_hash: parent_hash.to_string(),
    }
}

/// Replay a history through the reference model, stopping where the SUT's
/// session would be torn down. Returns the model in the state the SUT must
/// match, and the outcome that stopped the replay.
pub fn replay(
    seed: &ModelBlock,
    events: &[Event],
    cache_size: usize,
    auto_adjust: bool,
) -> (RefModel, ApplyOutcome) {
    let mut m = RefModel::init(seed.clone(), cache_size, auto_adjust).expect("seed is well formed");
    for ev in events {
        match m.apply_batch(&ev.blocks, ev.finalized_head.as_ref()) {
            ApplyOutcome::Applied => {}
            stopped => return (m, stopped),
        }
    }
    (m, ApplyOutcome::Applied)
}

/// Replays `events` on the first head-stream call, then idles. Later calls —
/// the ladder's replacement sessions — idle immediately, so a test observes
/// the state the scripted history left behind.
pub struct ScriptedSource {
    seed: Block,
    events: Arc<Vec<Event>>,
    stream_calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicUsize>,
}

impl ScriptedSource {
    pub fn new(seed: &ModelBlock, events: Vec<Event>) -> Self {
        ScriptedSource {
            seed: block_of(seed),
            events: Arc::new(events),
            stream_calls: Arc::new(AtomicUsize::new(0)),
            delivered: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Head-stream opens so far. A session teardown bumps it, so a test can
    /// wait for the ladder to restart instead of sleeping.
    pub fn stream_calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.stream_calls)
    }

    /// Events the SUT has *finished* applying. Incremented when the consumer
    /// comes back for the next batch, which it does only after processing the
    /// last one — so a test can compare state at an exact point in the
    /// history instead of at whatever the head happens to reach.
    pub fn delivered(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.delivered)
    }
}

#[async_trait]
impl DataSource for ScriptedSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed.block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed.block_ref())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let seed = self.seed.clone();
        Box::pin(async_stream::stream! {
            yield Ok(BlockBatch {
                blocks: vec![seed.clone()],
                finalized_head: Some(seed.block_ref()),
            });
        })
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let call = self.stream_calls.fetch_add(1, Ordering::SeqCst);
        let events = Arc::clone(&self.events);
        let delivered = Arc::clone(&self.delivered);
        Box::pin(async_stream::stream! {
            if call == 0 {
                for ev in events.iter() {
                    yield Ok(BlockBatch {
                        blocks: ev.blocks.iter().map(block_of).collect(),
                        finalized_head: ev.finalized_head.clone(),
                    });
                    // Reached only when the consumer asks for the next batch,
                    // i.e. once this one is applied.
                    delivered.fetch_add(1, Ordering::SeqCst);
                }
            }
            futures::future::pending::<()>().await;
        })
    }
}
