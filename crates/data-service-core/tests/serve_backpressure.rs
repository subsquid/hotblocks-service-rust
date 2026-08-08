//! Serve-path backpressure and budget bounds (GAP-31).
//!
//! A stalled reader must cost buffered batches, never in-flight upstream
//! capacity; the response budget bounds waiting — for data or for the reader —
//! but never discards a record already in hand (RP-12, RP-20).

mod common;

use std::io::Read as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use common::{block, scrape_until, Scrape};
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use data_service_core::{run_data_service, DataServiceOptions};
use futures::stream::BoxStream;

/// The buffer's first block is not a root, so anything below `FIRST - 1` is a
/// window underflow the service must acquire (RP-8).
const FIRST: u64 = 100;

fn seed() -> Block {
    block(FIRST, "h100", FIRST - 1, "h99")
}

fn payload_block(number: u64, payload: &Bytes) -> Block {
    Block {
        number,
        hash: format!("h{number}"),
        parent_number: number - 1,
        parent_hash: format!("h{}", number - 1),
        timestamp: Some(number * 1000),
        json_line_zstd: payload.clone(),
        timings: None,
    }
}

/// Deterministic incompressible bytes: the gzip transcode of a record this
/// size stays well above the tiny budgets these tests inject.
fn incompressible(len: usize) -> Vec<u8> {
    let mut data = vec![0u8; len];
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for chunk in data.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
    }
    data
}

/// What the window-underflow acquisition does.
#[derive(Clone)]
enum Backfill {
    /// Linked blocks for the whole requested range, one per batch, all
    /// sharing `payload`.
    Wide { payload: Bytes },
    /// One block, delivered after `delay`.
    Delayed { block: Block, delay: Duration },
    /// The range's first `count` blocks, `gap` before each after the first,
    /// then parks forever.
    Trickle {
        payload: Bytes,
        count: u64,
        gap: Duration,
    },
    /// The whole range, `stride` blocks per batch, acquired the way the EVM
    /// source's eager backfill does it: each stride spawned as a task, at
    /// most `concurrency` in flight, yielded in ascending order. `held`
    /// counts running stride calls (the probe permits), `peak` their
    /// high-water mark, `launched` every call ever started.
    Strided {
        payload: Bytes,
        stride: u64,
        concurrency: usize,
        held: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        launched: Arc<AtomicUsize>,
    },
    /// Never yields.
    Stuck,
}

/// Sets the shared flag when the finalized stream is dropped — the observable
/// for "in-flight strides were cancelled".
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct TestSource {
    seed: Block,
    backfill: Backfill,
    backfill_dropped: Arc<AtomicBool>,
}

#[async_trait]
impl DataSource for TestSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed.block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.seed.block_ref())
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(futures::stream::pending())
    }

    fn get_finalized_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let seed = self.seed.clone();
        // T1 asks for the seed alone; anything else is a window-underflow read.
        if req.from == seed.number && req.to == Some(seed.number) {
            return Box::pin(async_stream::stream! {
                yield Ok(BlockBatch {
                    blocks: vec![seed.clone()],
                    finalized_head: Some(seed.block_ref()),
                });
            });
        }
        let backfill = self.backfill.clone();
        let guard = DropFlag(Arc::clone(&self.backfill_dropped));
        let (from, to) = (req.from, req.to.unwrap_or(req.from));
        Box::pin(async_stream::stream! {
            let _guard = guard;
            match backfill {
                Backfill::Wide { payload } => {
                    for number in from..=to {
                        yield Ok(BlockBatch {
                            blocks: vec![payload_block(number, &payload)],
                            finalized_head: None,
                        });
                    }
                }
                Backfill::Delayed { block, delay } => {
                    tokio::time::sleep(delay).await;
                    yield Ok(BlockBatch { blocks: vec![block], finalized_head: None });
                }
                Backfill::Trickle { payload, count, gap } => {
                    for number in from..from + count.min(to - from + 1) {
                        if number > from {
                            tokio::time::sleep(gap).await;
                        }
                        yield Ok(BlockBatch {
                            blocks: vec![payload_block(number, &payload)],
                            finalized_head: None,
                        });
                    }
                    std::future::pending::<()>().await;
                }
                Backfill::Strided {
                    payload,
                    stride,
                    concurrency,
                    held,
                    peak,
                    launched,
                } => {
                    let mut starts = (from..=to).step_by(stride as usize);
                    let spawn_stride = |start: u64| {
                        let payload = payload.clone();
                        let held = Arc::clone(&held);
                        let peak = Arc::clone(&peak);
                        let launched = Arc::clone(&launched);
                        let end = (start + stride - 1).min(to);
                        tokio::spawn(async move {
                            launched.fetch_add(1, Ordering::SeqCst);
                            let running = held.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(running, Ordering::SeqCst);
                            // Long enough for spawned calls to overlap, far
                            // under the test budgets.
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            let batch = BlockBatch {
                                blocks: (start..=end)
                                    .map(|n| payload_block(n, &payload))
                                    .collect(),
                                finalized_head: None,
                            };
                            held.fetch_sub(1, Ordering::SeqCst);
                            batch
                        })
                    };
                    let mut pending = std::collections::VecDeque::new();
                    for start in starts.by_ref().take(concurrency) {
                        pending.push_back(spawn_stride(start));
                    }
                    while let Some(task) = pending.pop_front() {
                        let batch = task.await.expect("stride task");
                        if let Some(start) = starts.next() {
                            pending.push_back(spawn_stride(start));
                        }
                        yield Ok(batch);
                    }
                }
                Backfill::Stuck => std::future::pending::<()>().await,
            }
        })
    }
}

async fn start(source: TestSource, budget: Duration) -> data_service_core::DataServiceHandle {
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
        metrics: None,
    })
    .await
    .expect("service starts");
    handle.set_response_budget(budget);
    handle
}

async fn wait_for(flag: &AtomicBool, within: Duration, what: &str) {
    let deadline = Instant::now() + within;
    while !flag.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A client that stops reading without disconnecting must not pin the source
/// stream: the response terminates within the budget, the backfill stream is
/// dropped (cancelling in-flight strides), and the cut is counted (RP-12).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_reader_releases_source() {
    let budget = Duration::from_millis(1500);
    let backfill_dropped = Arc::new(AtomicBool::new(false));
    // 99 blocks x 2 MiB: far more than the channels, socket buffers and client
    // read-ahead can absorb, so the serve path must park on the stalled reader.
    let source = TestSource {
        seed: seed(),
        backfill: Backfill::Wide {
            payload: Bytes::from(incompressible(2 * 1024 * 1024)),
        },
        backfill_dropped: Arc::clone(&backfill_dropped),
    };
    let handle = start(source, budget).await;
    let port = handle.port;

    let mut response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(r#"{"fromBlock": 1}"#)
        .send()
        .await
        .expect("the backfill request is admitted");
    assert_eq!(response.status().as_u16(), 200);
    let first = response.chunk().await.expect("the body starts");
    assert!(first.is_some(), "some bytes arrive before the stall");

    // Stall: keep `response` (and its connection) alive, but poll nothing.
    // The bound is the 1.5 s budget plus slack — post-fix release is prompt,
    // while the untimed send it replaces held the stream far longer.
    wait_for(
        &backfill_dropped,
        Duration::from_secs(3),
        "the stalled reader still held the source stream past the budget",
    )
    .await;

    let scrape = scrape_until(port, "a budget truncation", |s| {
        s.get("sqd_hotblocks_response_truncations_total{cause=\"budget\"}") >= 1.0
    })
    .await;
    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"error\"}"),
        0.0
    );

    drop(response);
    handle.shutdown().await;
}

/// A slow-but-connected reader must neither pin nor grow in-flight
/// acquisition. Strides are spawned tasks, so every launched call runs to
/// completion and releases its probe permit even while the producer's send
/// is parked on the stalled client — and with no producer-side look-ahead,
/// launching stops at the serve buffers plus the source's own concurrency,
/// far short of the range (P-RESP-BUFFER).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_reader_does_not_hold_acquisition() {
    // Budget far above the window, so only eager completion — not budget
    // truncation — can release anything.
    let budget = Duration::from_secs(30);
    const STRIDES: usize = 40;
    const CONCURRENCY: usize = 3;
    // The serve path absorbs ~5 strides (its 32-frame channel plus socket
    // slack), the batch channel 2, one parks in send, and the source keeps
    // CONCURRENCY spawned: ~11 launches. The removed DRAIN(8) look-ahead
    // pulled ~8 more, so the bound separates the designs with margin.
    const LAUNCH_BOUND: usize = 14;
    let held = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let launched = Arc::new(AtomicUsize::new(0));
    let source = TestSource {
        // A window starting at 400 leaves 320 blocks below it: 40 strides of 8,
        // longer than every buffer on the serve path combined.
        seed: block(400, "h400", 399, "h399"),
        backfill: Backfill::Strided {
            payload: Bytes::from(incompressible(2 * 1024 * 1024)),
            stride: 8,
            concurrency: CONCURRENCY,
            held: Arc::clone(&held),
            peak: Arc::clone(&peak),
            launched: Arc::clone(&launched),
        },
        backfill_dropped: Arc::new(AtomicBool::new(false)),
    };
    let handle = start(source, budget).await;
    let port = handle.port;

    // fromBlock 80 makes the underflow range 80..=399: 320 blocks, 40 strides.
    let mut response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(r#"{"fromBlock": 80}"#)
        .send()
        .await
        .expect("the backfill request is admitted");
    assert_eq!(response.status().as_u16(), 200);
    let first = response.chunk().await.expect("the body starts");
    assert!(first.is_some(), "some bytes arrive before the stall");

    // Stall: keep `response` (and its connection) alive, but poll nothing.
    // Wait for acquisition to go quiet: every launched call completed and no
    // new launches for a settle period.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            Instant::now() < deadline,
            "acquisition never went quiet under a stalled reader: {} held, {} launched",
            held.load(Ordering::SeqCst),
            launched.load(Ordering::SeqCst),
        );
        let before = launched.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        if held.load(Ordering::SeqCst) == 0 && launched.load(Ordering::SeqCst) == before {
            break;
        }
    }
    assert!(
        peak.load(Ordering::SeqCst) <= CONCURRENCY,
        "in-flight stride calls exceeded the source's own bound: {}",
        peak.load(Ordering::SeqCst),
    );
    let total = launched.load(Ordering::SeqCst);
    assert!(
        total <= LAUNCH_BOUND,
        "a stalled reader kept the serve path pulling strides: {total} of {STRIDES} launched \
         (bound {LAUNCH_BOUND})",
    );

    drop(response);
    handle.shutdown().await;
}

/// A record in hand when the budget runs out is still served: the budget
/// bounds waiting for data, not the transcode of data already acquired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_frame_forward_progress() {
    let budget = Duration::from_millis(3000);
    let delay = Duration::from_millis(2700);
    // Big enough that its gzip transcode overruns the ~300 ms left of the
    // budget once the batch lands (~1.7 s in a debug build).
    let raw = incompressible(16 * 1024 * 1024);
    let slow = Block {
        json_line_zstd: Bytes::from(zstd::encode_all(&raw[..], 1).unwrap()),
        ..payload_block(FIRST - 1, &Bytes::new())
    };
    let source = TestSource {
        seed: seed(),
        backfill: Backfill::Delayed { block: slow, delay },
        backfill_dropped: Arc::new(AtomicBool::new(false)),
    };
    let handle = start(source, budget).await;
    let port = handle.port;

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "gzip")
        .header("content-type", "application/json")
        .body(format!(r#"{{"fromBlock": {}}}"#, FIRST - 1))
        .send()
        .await
        .expect("the backfill request is admitted");
    assert_eq!(
        response.status().as_u16(),
        200,
        "a record acquired within the budget must be served, not discarded as empty"
    );
    // reqwest's gzip feature may have decompressed the body already (it strips
    // content-encoding when it does); decode manually only when it did not.
    let still_encoded = response.headers().get("content-encoding").is_some();
    let body = response.bytes().await.expect("read the body to its end");
    let decoded = if still_encoded {
        let mut decoded = Vec::new();
        flate2::read::MultiGzDecoder::new(&body[..])
            .read_to_end(&mut decoded)
            .expect("gzip members decode whole");
        decoded
    } else {
        body.to_vec()
    };
    assert!(
        decoded.len() >= raw.len() && decoded[..raw.len()] == raw[..],
        "the first record must arrive intact"
    );

    let scrape = Scrape::read(port).await;
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"backfill\"}"),
        1.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"wait_empty\"}"),
        0.0
    );

    handle.shutdown().await;
}

/// A source whose finalized stream never yields must not hold the request
/// open: the wait for the first batch is bounded by the budget and expires
/// into RP-12's empty form.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn below_query_wait_bounded() {
    let budget = Duration::from_millis(500);
    let backfill_dropped = Arc::new(AtomicBool::new(false));
    let source = TestSource {
        seed: seed(),
        backfill: Backfill::Stuck,
        backfill_dropped: Arc::clone(&backfill_dropped),
    };
    let handle = start(source, budget).await;
    let port = handle.port;

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}/stream"))
            .header("accept-encoding", "zstd")
            .header("content-type", "application/json")
            .body(r#"{"fromBlock": 95}"#)
            .send(),
    )
    .await
    .expect("the response must terminate within the budget, not hang")
    .expect("the backfill request is admitted");

    assert_eq!(response.status().as_u16(), 204);
    assert_eq!(
        response
            .headers()
            .get("x-sqd-finalized-head-number")
            .and_then(|v| v.to_str().ok()),
        Some("100"),
        "the empty form still names the finalized head"
    );

    // The expired wait released the acquisition stream.
    wait_for(
        &backfill_dropped,
        Duration::from_secs(2),
        "the bounded wait must drop the source stream",
    )
    .await;

    let scrape = Scrape::read(port).await;
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"wait_empty\"}"),
        1.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"backfill\"}"),
        0.0
    );

    handle.shutdown().await;
}

/// A client that disconnects while the source is parked must not leak the
/// producer: the torn-down serve task closes the channel, and the producer —
/// parked on a source that will never yield again — must go with it, dropping
/// the source stream. The budget is far above the test window, so only the
/// disconnect can release anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_releases_parked_source() {
    let budget = Duration::from_secs(30);
    let backfill_dropped = Arc::new(AtomicBool::new(false));
    let source = TestSource {
        seed: seed(),
        // One frame before the disconnect, one after it: the failed send of
        // the second tears the serve task down mid-stream.
        backfill: Backfill::Trickle {
            payload: Bytes::from(incompressible(1024)),
            count: 2,
            gap: Duration::from_millis(500),
        },
        backfill_dropped: Arc::clone(&backfill_dropped),
    };
    let handle = start(source, budget).await;
    let port = handle.port;

    let mut response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(r#"{"fromBlock": 95}"#)
        .send()
        .await
        .expect("the backfill request is admitted");
    assert_eq!(response.status().as_u16(), 200);
    let first = response.chunk().await.expect("the body starts");
    assert!(first.is_some(), "some bytes arrive before the disconnect");
    drop(response);

    wait_for(
        &backfill_dropped,
        Duration::from_secs(3),
        "the disconnected client left the producer parked on the source stream",
    )
    .await;

    let scrape = scrape_until(port, "a disconnect truncation", |s| {
        s.get("sqd_hotblocks_response_truncations_total{cause=\"disconnect\"}") >= 1.0
    })
    .await;
    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"budget\"}"),
        0.0,
        "the release must come from the disconnect, not the budget"
    );

    handle.shutdown().await;
}

/// A source that parks mid-body must not hold the 200 open on a client that
/// keeps reading: the wait for the next record is bounded by the budget, and
/// the body ends at the last record boundary as a counted budget cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_body_budget_bounds_a_parked_source() {
    let budget = Duration::from_millis(700);
    let payload = incompressible(1024);
    let source = TestSource {
        seed: seed(),
        backfill: Backfill::Trickle {
            payload: Bytes::from(payload.clone()),
            count: 1,
            gap: Duration::ZERO,
        },
        backfill_dropped: Arc::new(AtomicBool::new(false)),
    };
    let handle = start(source, budget).await;
    let port = handle.port;

    let started = Instant::now();
    let mut response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(r#"{"fromBlock": 95}"#)
        .send()
        .await
        .expect("the backfill request is admitted");
    assert_eq!(response.status().as_u16(), 200);

    let read_to_end = async {
        let mut got = 0usize;
        while let Some(chunk) = response.chunk().await.expect("read the body") {
            got += chunk.len();
        }
        got
    };
    let got = tokio::time::timeout(budget + Duration::from_secs(3), read_to_end)
        .await
        .expect("a source parked mid-body must not hold the response open past the budget");
    assert!(
        started.elapsed() < budget + Duration::from_secs(3),
        "the body must end within the budget plus slack, took {:?}",
        started.elapsed()
    );
    assert!(
        got >= payload.len(),
        "the record in hand must be served whole before the cut, got {got} bytes"
    );

    let scrape = scrape_until(port, "a budget truncation", |s| {
        s.get("sqd_hotblocks_response_truncations_total{cause=\"budget\"}") >= 1.0
    })
    .await;
    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"error\"}"),
        0.0
    );

    handle.shutdown().await;
}
