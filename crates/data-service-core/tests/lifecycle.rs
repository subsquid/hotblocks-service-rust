//! Lifecycle conformance: WP-9/FM-31 (a session that dies before the first
//! ever block is a startup failure, never a zombie), WP-20/INV-12 (a re-seed
//! contradicting the buffer it discards is terminal divergence), and FM-30
//! (rollback below finality ends the run terminally, not silently).

use async_trait::async_trait;
use bytes::Bytes;
use data_service_core::service::{DataService, DivergentReseed, RunEnd};
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use data_service_core::{run_data_service, DataServiceOptions};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

fn block(number: u64, hash: &str, parent_number: u64, parent_hash: &str) -> Block {
    let json = format!("{{\"number\":{number}}}\n");
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

/// Seeds T1 normally, then every head-stream session dies at once — the
/// shape of one transient RPC error in the very first session.
struct SeedThenFailSource {
    seed: Block,
}

#[async_trait]
impl DataSource for SeedThenFailSource {
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
            yield Ok(BlockBatch { blocks: vec![seed.clone()], finalized_head: Some(seed.block_ref()) });
        })
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(async_stream::stream! {
            yield Err(StreamError::Other(anyhow::anyhow!("transient upstream error")));
        })
    }
}

/// WP-9: the run ends as a startup failure the binary can observe and exit
/// on — the process must never keep serving with ingestion permanently dead.
#[tokio::test]
async fn first_session_death_is_a_startup_failure_not_a_zombie() {
    let mut handle = run_data_service(DataServiceOptions {
        source: SeedThenFailSource {
            seed: block(5, "h5", 4, "h4"),
        },
        block_cache_size: 10,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();

    let started = tokio::time::timeout(Duration::from_secs(5), &mut handle.started)
        .await
        .expect("started must resolve")
        .expect("supervisor alive");
    assert!(started.is_err(), "ingestion died before the first block");

    let end = tokio::time::timeout(Duration::from_secs(5), &mut handle.ended)
        .await
        .expect("run must end, not idle as a zombie")
        .expect("run task alive");
    assert!(
        matches!(end, RunEnd::StartupFailure(_)),
        "expected StartupFailure, got {end:?}"
    );

    handle.shutdown().await;
}

/// Seeds from a mutable slot, so a test can change what a re-INIT sees;
/// the head stream idles forever.
#[derive(Clone)]
struct ReseedSource {
    seed: Arc<Mutex<Block>>,
}

#[async_trait]
impl DataSource for ReseedSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed.lock().unwrap().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed.lock().unwrap().block_ref())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let seed = self.seed.lock().unwrap().clone();
        Box::pin(async_stream::stream! {
            yield Ok(BlockBatch { blocks: vec![seed.clone()], finalized_head: Some(seed.block_ref()) });
        })
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(async_stream::stream! {
            futures::future::pending::<()>().await;
            yield Err(StreamError::Other(anyhow::anyhow!("unreachable")));
        })
    }
}

/// WP-20: a seed naming a height the discarded buffer holds under a different
/// hash is unrecoverable divergence — refused, buffer untouched.
#[tokio::test]
async fn divergent_reseed_is_terminal_and_leaves_the_buffer() {
    let seed = Arc::new(Mutex::new(block(5, "h5", 4, "h4")));
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let svc = DataService::new(
        ReseedSource {
            seed: Arc::clone(&seed),
        },
        10,
        false,
        cancel_rx,
    );

    svc.init().await.unwrap();
    assert_eq!(svc.get_head().hash, "h5");

    *seed.lock().unwrap() = block(5, "h5-evil", 4, "h4");
    let err = svc.init().await.unwrap_err();
    assert!(
        err.is::<DivergentReseed>(),
        "expected DivergentReseed, got: {err:#}"
    );
    assert_eq!(svc.get_head().hash, "h5", "the buffer must be untouched");
}

/// WP-20/DEF-8: same ref, another parent — invisible to a hash-only check.
#[tokio::test]
async fn reseed_equivocating_on_the_parent_link_is_terminal() {
    let seed = Arc::new(Mutex::new(block(5, "h5", 4, "h4")));
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let svc = DataService::new(
        ReseedSource {
            seed: Arc::clone(&seed),
        },
        10,
        false,
        cancel_rx,
    );

    svc.init().await.unwrap();
    *seed.lock().unwrap() = block(5, "h5", 4, "h4-other");
    let err = svc.init().await.unwrap_err();
    assert!(
        err.is::<DivergentReseed>(),
        "expected DivergentReseed, got: {err:#}"
    );
    assert_eq!(svc.get_head().hash, "h5", "the buffer must be untouched");
}

/// Seeds a block other than the one `get_finalized_head` named.
#[derive(Clone)]
struct LyingSeedSource {
    announced: BlockRef,
    delivered: Vec<Block>,
}

#[async_trait]
impl DataSource for LyingSeedSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.announced.clone())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.announced.clone())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let blocks = self.delivered.clone();
        Box::pin(async_stream::stream! {
            yield Ok(BlockBatch { blocks, finalized_head: None });
        })
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(async_stream::stream! {
            futures::future::pending::<()>().await;
            yield Err(StreamError::Other(anyhow::anyhow!("unreachable")));
        })
    }
}

/// WP-20: T1 seeds at the *reported* finalized head — an unchecked block
/// would become the finality anchor. Malformed shapes reach the ladder.
#[tokio::test]
async fn seed_must_be_the_announced_finalized_head() {
    let announced = BlockRef {
        number: 5,
        hash: "h5".into(),
    };
    for (case, delivered) in [
        ("wrong hash", vec![block(5, "h5-other", 4, "h4")]),
        ("wrong height", vec![block(6, "h6", 5, "h5")]),
        (
            "more than one block",
            vec![block(5, "h5", 4, "h4"), block(6, "h6", 5, "h5")],
        ),
        ("empty batch", vec![]),
    ] {
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let svc = DataService::new(
            LyingSeedSource {
                announced: announced.clone(),
                delivered,
            },
            10,
            false,
            cancel_rx,
        );
        let err = svc.init().await.unwrap_err();
        assert!(
            !err.is::<DivergentReseed>(),
            "{case}: a source fault is a ladder retry, not FM-30: {err:#}"
        );
    }
}

/// WP-20: a lower seed at an unheld height is a legal epoch reset (alarmed,
/// not refused) — and an identical re-seed is trivially legal.
#[tokio::test]
async fn epochal_reseed_is_allowed() {
    let seed = Arc::new(Mutex::new(block(5, "h5", 4, "h4")));
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let svc = DataService::new(
        ReseedSource {
            seed: Arc::clone(&seed),
        },
        10,
        false,
        cancel_rx,
    );

    svc.init().await.unwrap();
    svc.init().await.expect("identical re-seed is legal");

    *seed.lock().unwrap() = block(3, "h3", 2, "h2");
    svc.init()
        .await
        .expect("lower unheld seed opens a new epoch");
    assert_eq!(svc.get_head(), seed.lock().unwrap().block_ref());
}

/// A root is already the lowest response-eligible block, so querying it must
/// stay on the cache path. A second finalized-stream call would be an
/// observable RP-3 violation and is made fatal by this source.
#[derive(Clone)]
struct RootOnceSource {
    root: Block,
    finalized_stream_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl DataSource for RootOnceSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.root.block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.root.block_ref())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let call = self.finalized_stream_calls.fetch_add(1, Ordering::SeqCst);
        let root = self.root.clone();
        Box::pin(async_stream::stream! {
            if call == 0 {
                yield Ok(BlockBatch {
                    blocks: vec![root.clone()],
                    finalized_head: Some(root.block_ref()),
                });
            } else {
                yield Err(StreamError::Other(anyhow::anyhow!(
                    "root query incorrectly opened a finalized backfill"
                )));
            }
        })
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(async_stream::stream! {
            futures::future::pending::<()>().await;
            yield Err(StreamError::Other(anyhow::anyhow!("unreachable")));
        })
    }
}

#[tokio::test]
async fn buffered_root_query_does_not_open_backfill() {
    let finalized_stream_calls = Arc::new(AtomicUsize::new(0));
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let svc = DataService::new(
        RootOnceSource {
            root: block(0, "h0", 0, "0x0"),
            finalized_stream_calls: Arc::clone(&finalized_stream_calls),
        },
        10,
        false,
        cancel_rx,
    );

    svc.init().await.unwrap();
    let response = svc.query(0, None).await.expect("root is cache-servable");
    let tail = response.tail.expect("root query returns cached data");

    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].number, 0);
    assert_eq!(finalized_stream_calls.load(Ordering::SeqCst), 1);
}

/// Seeds and serves a short chain, then signals a fork whose refs sit
/// entirely below the finalized head.
struct SubFinalityForkSource {
    chain: Arc<Vec<Block>>,
}

#[async_trait]
impl DataSource for SubFinalityForkSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain.last().unwrap().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain[0].block_ref())
    }

    fn get_finalized_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let chain = Arc::clone(&self.chain);
        Box::pin(async_stream::stream! {
            for b in chain.iter() {
                if b.number < req.from { continue; }
                if let Some(t) = req.to { if b.number > t { break; } }
                yield Ok(BlockBatch { blocks: vec![b.clone()], finalized_head: Some(b.block_ref()) });
            }
        })
    }

    fn get_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let chain = Arc::clone(&self.chain);
        Box::pin(async_stream::stream! {
            let blocks: Vec<Block> =
                chain.iter().filter(|b| b.number >= req.from).cloned().collect();
            if !blocks.is_empty() {
                let fin = blocks.last().unwrap().block_ref();
                yield Ok(BlockBatch { blocks, finalized_head: Some(fin) });
            }
            yield Err(StreamError::Fork {
                previous_blocks: vec![BlockRef { number: 1, hash: "0xdeadbeef".into() }],
            });
        })
    }
}

/// FM-30: divergence below finality ends the run terminally — observable, so
/// the binary can exit non-zero instead of serving stale data forever.
#[tokio::test]
async fn sub_finality_fork_ends_the_run_terminally() {
    let chain = vec![
        block(5, "h5", 4, "h4"),
        block(6, "h6", 5, "h5"),
        block(7, "h7", 6, "h6"),
    ];
    let mut handle = run_data_service(DataServiceOptions {
        source: SubFinalityForkSource {
            chain: Arc::new(chain),
        },
        block_cache_size: 10,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();

    let end = tokio::time::timeout(Duration::from_secs(5), &mut handle.ended)
        .await
        .expect("run must end terminally, not idle as a zombie")
        .expect("run task alive");
    assert!(
        matches!(end, RunEnd::Terminal(_)),
        "expected Terminal, got {end:?}"
    );

    handle.shutdown().await;
}
