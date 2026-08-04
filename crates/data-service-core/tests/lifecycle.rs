//! Lifecycle conformance: WP-9/FM-31 (a session that dies before the first
//! ever block is a startup failure, never a zombie), WP-20/INV-12 (a re-seed
//! contradicting the buffer it discards is terminal divergence), and FM-30
//! (rollback below finality ends the run terminally, not silently).

use async_trait::async_trait;
use bytes::Bytes;
use data_service_core::metrics::get_block_ingestion_timestamp;
use data_service_core::service::{DataService, DivergentReseed, RunEnd};
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use data_service_core::{run_data_service, DataServiceOptions};
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, Notify};

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

/// Seeds T1 normally, optionally replays that seed on the head stream, then
/// every session dies with a transient RPC error.
struct SeedThenFailSource {
    seed: Block,
    replay_seed: bool,
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
        let replay = self.replay_seed.then(|| self.seed.clone());
        Box::pin(async_stream::stream! {
            if let Some(seed) = replay {
                yield Ok(BlockBatch { blocks: vec![seed], finalized_head: None });
            }
            yield Err(StreamError::Other(anyhow::anyhow!("transient upstream error")));
        })
    }
}

async fn assert_startup_failure(replay_seed: bool) {
    let mut handle = run_data_service(DataServiceOptions {
        source: SeedThenFailSource {
            seed: block(5, "h5", 4, "h4"),
            replay_seed,
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

/// WP-9: the run ends as a startup failure the binary can observe and exit
/// on — the process must never keep serving with ingestion permanently dead.
#[tokio::test]
async fn first_session_death_is_a_startup_failure_not_a_zombie() {
    assert_startup_failure(false).await;
}

/// DEF-20 permits a new stream to redeliver the seed. WP-6 absorbs it as a
/// no-op, so a subsequent error still happened before the first block ingest.
#[tokio::test]
async fn absorbed_seed_replay_does_not_complete_startup() {
    assert_startup_failure(true).await;
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

#[tokio::test]
async fn zero_cache_size_is_rejected_as_configuration_error() {
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let svc = DataService::new(
        LyingSeedSource {
            announced: BlockRef {
                number: 5,
                hash: "h5".into(),
            },
            delivered: vec![block(5, "h5", 4, "h4")],
        },
        0,
        false,
        cancel_rx,
    );

    let err = svc.init().await.unwrap_err();
    assert!(err.to_string().contains("P-CACHE-SIZE"), "{err:#}");
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

#[tokio::test]
async fn seed_must_satisfy_the_block_shape_contract() {
    for (case, announced, delivered) in [
        (
            "empty block hash",
            BlockRef {
                number: 5,
                hash: String::new(),
            },
            block(5, "", 4, "h4"),
        ),
        (
            "empty parent hash",
            BlockRef {
                number: 5,
                hash: "h5".into(),
            },
            block(5, "h5", 4, ""),
        ),
        (
            "header-unsafe finalized head hash",
            BlockRef {
                number: 5,
                hash: "bad\nhead".into(),
            },
            block(5, "bad\nhead", 4, "h4"),
        ),
        (
            "header-unsafe block hash",
            BlockRef {
                number: 5,
                hash: "h5".into(),
            },
            block(5, "bad\nhash", 4, "h4"),
        ),
        (
            "header-unsafe parent hash",
            BlockRef {
                number: 5,
                hash: "h5".into(),
            },
            block(5, "h5", 4, "bad\rparent"),
        ),
        (
            "parent above block",
            BlockRef {
                number: 5,
                hash: "h5".into(),
            },
            block(5, "h5", 6, "h6"),
        ),
    ] {
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let svc = DataService::new(
            LyingSeedSource {
                announced,
                delivered: vec![delivered],
            },
            10,
            false,
            cancel_rx,
        );

        let err = svc.init().await.unwrap_err();
        assert!(
            !err.is::<DivergentReseed>(),
            "{case}: malformed source data is not FM-30: {err:#}"
        );
        assert!(
            err.to_string().starts_with("DEF-"),
            "{case}: expected a structural source fault, got: {err:#}"
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

/// A head stream that reports finality above its current delivery, follows it
/// across a fork rebase with a lower contradictory report, then reaches the
/// retained obligation without attaching another finality update.
struct RegressiveFinalitySource {
    chain: Arc<Vec<Block>>,
    stream_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl DataSource for RegressiveFinalitySource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain.last().unwrap().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain[0].block_ref())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let seed = self.chain[0].clone();
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
        let chain = Arc::clone(&self.chain);
        Box::pin(async_stream::stream! {
            if call == 0 {
                yield Ok(BlockBatch {
                    blocks: vec![chain[1].clone()],
                    finalized_head: Some(chain[3].block_ref()),
                });
                yield Err(StreamError::Fork {
                    previous_blocks: vec![chain[1].block_ref()],
                });
            } else {
                yield Ok(BlockBatch {
                    blocks: vec![chain[2].clone()],
                    finalized_head: Some(BlockRef {
                        number: chain[1].number,
                        hash: "wrong-hash".into(),
                    }),
                });
                yield Ok(BlockBatch {
                    blocks: vec![chain[3].clone()],
                    finalized_head: None,
                });
                futures::future::pending::<()>().await;
            }
        })
    }
}

#[tokio::test]
async fn session_max_ignores_regressive_finality_and_settles_later_blocks() {
    let chain = Arc::new(vec![
        block(1, "h1", 0, "h0"),
        block(2, "h2", 1, "h1"),
        block(3, "h3", 2, "h2"),
        block(4, "h4", 3, "h3"),
    ]);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let svc = Arc::new(DataService::new(
        RegressiveFinalitySource {
            chain,
            stream_calls: Arc::new(AtomicUsize::new(0)),
        },
        10,
        false,
        cancel_rx,
    ));
    svc.init().await.unwrap();

    let runner = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run().await })
    };

    tokio::time::timeout(Duration::from_secs(2), async {
        while svc.get_head().number < 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("scripted head must be ingested");

    assert_eq!(
        svc.get_finalized_head(),
        BlockRef {
            number: 4,
            hash: "h4".into(),
        },
        "the retained maximum must settle even without a fresh report"
    );

    cancel_tx.send(true).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("ingestion run must stop")
            .expect("ingestion task must not panic"),
        RunEnd::Stopped
    ));
}

/// The first session establishes an above-head maximum, then attaches a
/// contradictory report to the next block. The replacement session records
/// the height from which the service retries.
struct ConflictingFinalitySource {
    chain: Arc<Vec<Block>>,
    stream_from: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl DataSource for ConflictingFinalitySource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain.last().unwrap().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain[0].block_ref())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let seed = self.chain[0].clone();
        Box::pin(async_stream::stream! {
            yield Ok(BlockBatch {
                blocks: vec![seed.clone()],
                finalized_head: Some(seed.block_ref()),
            });
        })
    }

    fn get_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let call = {
            let mut stream_from = self.stream_from.lock().unwrap();
            let call = stream_from.len();
            stream_from.push(req.from);
            call
        };
        let chain = Arc::clone(&self.chain);
        Box::pin(async_stream::stream! {
            if call == 0 {
                yield Ok(BlockBatch {
                    blocks: vec![chain[1].clone()],
                    finalized_head: Some(BlockRef {
                        number: 4,
                        hash: "final-a".into(),
                    }),
                });
                yield Ok(BlockBatch {
                    blocks: vec![chain[2].clone()],
                    finalized_head: Some(BlockRef {
                        number: 4,
                        hash: "final-b".into(),
                    }),
                });
            } else {
                futures::future::pending::<()>().await;
            }
        })
    }
}

#[tokio::test]
async fn conflicting_finality_rejects_the_batch_before_mutation() {
    let chain = Arc::new(vec![
        block(1, "h1", 0, "h0"),
        block(2, "h2", 1, "h1"),
        block(3, "h3", 2, "h2"),
    ]);
    let stream_from = Arc::new(Mutex::new(Vec::new()));
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let svc = Arc::new(DataService::new(
        ConflictingFinalitySource {
            chain,
            stream_from: Arc::clone(&stream_from),
        },
        10,
        false,
        cancel_rx,
    ));
    svc.init().await.unwrap();

    let runner = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run().await })
    };

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stream_from.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the conflicting session must restart");

    assert_eq!(
        svc.get_head(),
        BlockRef {
            number: 2,
            hash: "h2".into()
        }
    );
    assert_eq!(
        &stream_from.lock().unwrap()[..2],
        &[2, 3],
        "the retry must start after the last committed head"
    );

    cancel_tx.send(true).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("ingestion run must stop")
            .expect("ingestion task must not panic"),
        RunEnd::Stopped
    ));
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

/// `(from, parent_hash)` of each head-stream request.
type StreamLog = Arc<Mutex<Vec<(u64, Option<String>)>>>;

/// Reports finality above its own delivery, then delivers that height under a
/// different hash — the WP-23 revalidation the retained obligation owes.
struct ContradictedObligationSource {
    seed: Block,
    stream_calls: Arc<AtomicUsize>,
    /// Every `(from, parent_hash)` the ladder re-opened the stream with.
    requested: StreamLog,
}

#[async_trait]
impl DataSource for ContradictedObligationSource {
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
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        self.requested
            .lock()
            .unwrap()
            .push((req.from, req.parent_hash));
        let call = self.stream_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async_stream::stream! {
            if call == 0 {
                yield Ok(BlockBatch {
                    blocks: vec![block(2, "h2", 1, "h1")],
                    finalized_head: Some(BlockRef { number: 4, hash: "h4".into() }),
                });
                yield Ok(BlockBatch {
                    blocks: vec![block(3, "h3", 2, "h2")],
                    finalized_head: None,
                });
                // Height 4 is not what the obligation named.
                yield Ok(BlockBatch {
                    blocks: vec![block(4, "h4-forged", 3, "h3")],
                    finalized_head: None,
                });
            }
            futures::future::pending::<()>().await;
        })
    }
}

/// WP-5: a finality report the buffer contradicts ends the session and
/// re-enters the ladder. It must not take the process with it, and it must not
/// commit the batch that carried the contradiction — otherwise the forged
/// block becomes the base every later block descends from.
#[tokio::test]
async fn a_contradicted_finality_obligation_ends_the_session_not_the_process() {
    let stream_calls = Arc::new(AtomicUsize::new(0));
    let requested = Arc::new(Mutex::new(Vec::new()));
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let svc = Arc::new(DataService::new(
        ContradictedObligationSource {
            seed: block(1, "h1", 0, "h0"),
            stream_calls: Arc::clone(&stream_calls),
            requested: Arc::clone(&requested),
        },
        10,
        false,
        cancel_rx,
    ));
    svc.init().await.unwrap();

    let runner = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run().await })
    };

    // The ladder re-opens the stream: the session died, the run did not.
    tokio::time::timeout(Duration::from_secs(2), async {
        while stream_calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the session must restart after the contradiction");

    assert!(!runner.is_finished(), "the run must survive the violation");
    // WP-5: the batch carrying the contradiction never landed.
    assert_eq!(
        svc.get_head(),
        BlockRef {
            number: 3,
            hash: "h3".into()
        },
        "the forged block must not survive the rejected batch"
    );
    // Finality stays where the last settled obligation put it, not where the
    // rolled-back one provisionally advanced it.
    assert_eq!(svc.get_finalized_head().number, 3);
    // The ladder asks for height 4 again instead of descending from the forgery.
    assert_eq!(
        requested.lock().unwrap().as_slice(),
        [(2, Some("h1".to_string())), (4, Some("h3".to_string()))]
    );

    cancel_tx.send(true).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("ingestion run must stop")
            .expect("ingestion task must not panic"),
        RunEnd::Stopped
    ));
}

/// Ingest-time observables belong to the committed batch. A rolled-back block
/// must not answer `/block-time`, and an absorbed duplicate must not rewrite
/// the time its block was first seen (WP-6).
#[tokio::test]
async fn ingest_time_is_published_only_by_a_committed_batch() {
    // Heights unique to this test: the block-time cache is process-global.
    const BASE: u64 = 910_000;
    let h = |n: u64| format!("h{n}");
    let seed = block(BASE, &h(BASE), BASE - 1, &h(BASE - 1));

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let stream_calls = Arc::new(AtomicUsize::new(0));
    let first_batch_committed = Arc::new(Notify::new());
    let replay = Arc::new(Notify::new());
    let svc = Arc::new(DataService::new(
        ObservableSource {
            seed: seed.clone(),
            stream_calls: Arc::clone(&stream_calls),
            base: BASE,
            first_batch_committed: Arc::clone(&first_batch_committed),
            replay: Arc::clone(&replay),
        },
        10,
        false,
        cancel_rx,
    ));
    svc.init().await.unwrap();

    let runner = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run().await })
    };

    tokio::time::timeout(Duration::from_secs(2), first_batch_committed.notified())
        .await
        .expect("the first batch must commit");

    let seen = |n: u64| get_block_ingestion_timestamp(&n.to_string());
    let stamped = seen(BASE + 1).expect("a committed block is timestamped");
    assert!(
        seen(BASE + 2).is_some(),
        "every inserted block in the committed batch is timestamped"
    );

    // Make an incorrect restamp distinguishable, then let the source replay
    // the already committed block and deliver the rejected batch.
    tokio::time::sleep(Duration::from_millis(20)).await;
    replay.notify_one();

    tokio::time::timeout(Duration::from_secs(2), async {
        while stream_calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the rejected batch must tear the session down");

    assert_eq!(
        seen(BASE + 1),
        Some(stamped),
        "an absorbed redelivery must not restamp the block it duplicates"
    );
    assert_eq!(
        seen(BASE + 3),
        None,
        "a rolled-back block must not answer /block-time"
    );

    cancel_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), runner).await;
}

/// Delivers two good blocks, redelivers one of them, then a block whose parent
/// link contradicts the buffer — so the last batch is rejected whole.
struct ObservableSource {
    seed: Block,
    stream_calls: Arc<AtomicUsize>,
    base: u64,
    first_batch_committed: Arc<Notify>,
    replay: Arc<Notify>,
}

#[async_trait]
impl DataSource for ObservableSource {
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
        let b = self.base;
        let first_batch_committed = Arc::clone(&self.first_batch_committed);
        let replay = Arc::clone(&self.replay);
        Box::pin(async_stream::stream! {
            if call == 0 {
                let one = block(b + 1, &format!("h{}", b + 1), b, &format!("h{b}"));
                let two = block(b + 2, &format!("h{}", b + 2), b + 1, &format!("h{}", b + 1));
                yield Ok(BlockBatch { blocks: vec![one.clone(), two], finalized_head: None });
                // The stream is polled again only after the service commits
                // the yielded batch. Let the test snapshot its timestamp
                // before allowing the identical redelivery.
                first_batch_committed.notify_one();
                replay.notified().await;
                yield Ok(BlockBatch { blocks: vec![one], finalized_head: None });
                // Rejected whole: its block must leave no trace.
                yield Ok(BlockBatch {
                    blocks: vec![block(b + 3, &format!("h{}", b + 3), b + 2, "forged")],
                    finalized_head: None,
                });
            }
            futures::future::pending::<()>().await;
        })
    }
}
