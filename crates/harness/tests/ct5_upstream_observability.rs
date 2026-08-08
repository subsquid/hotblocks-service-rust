//! CT-5 — upstream-interaction observability (GAP-25).
//!
//! REQ-16 declares one shared upstream budget, and OB-4 is how an operator
//! sees it being spent. The real adapter runs against the scripted upstream,
//! and the counters are read back off the same registry the service scrapes.

use std::sync::Arc;
use std::time::Duration;

use data_service_core::metrics::Metrics;
use data_service_core::source::{DataSource, StreamRequest};
use evm_source::observability::MetricsRpcObserver;
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use futures::StreamExt;
use harness::upstream::{Chain, Fault, Upstream};
use rpc_client::{RpcClient, RpcClientConfig};

const FIRST: u64 = 1000;
const COUNT: u64 = 5;
const FAULTED: u64 = 1002;
const TXS: usize = 2;
/// The default EVM selection, whose component call is the one faulted below.
const DEBUG_METHOD: &str = "debug_traceBlockByHash";

/// Over the retry budget (P-ENRICH-RETRIES × P-ENRICH-DELAY), under a CI hang.
const BUDGET: Duration = Duration::from_secs(20);

struct Wired {
    source: EvmRpcDataSource,
    metrics: Arc<Metrics>,
}

fn wire(upstream: &Upstream, retry_attempts: usize) -> Wired {
    wire_for(upstream, retry_attempts, DataRequest::default())
}

fn wire_for(upstream: &Upstream, retry_attempts: usize, request: DataRequest) -> Wired {
    let metrics = Arc::new(Metrics::new());
    let client = Arc::new(
        RpcClient::new(RpcClientConfig {
            url: upstream.url().to_string(),
            capacity: 5,
            retry_attempts,
            retry_schedule: vec![Duration::from_millis(1)],
            ..Default::default()
        })
        .with_observer(Arc::new(MetricsRpcObserver::new(Arc::clone(&metrics)))),
    );
    let source = EvmRpcDataSource::with_metrics(
        client,
        EvmRpcDataSourceOptions {
            data_request: request,
            ..Default::default()
        },
        Arc::clone(&metrics),
    );
    Wired { source, metrics }
}

/// Read one sample off the text exposition, as an operator's scrape would.
fn sample(metrics: &Metrics, key: &str) -> f64 {
    let text = metrics.gather_text().expect("gather");
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some((name, value)) = line.rsplit_once(' ') {
            if name == key {
                return value.parse().expect("sample is numeric");
            }
        }
    }
    panic!("no sample {key} on the scrape surface");
}

/// Drain the head stream until it errors, ends, or the budget runs out.
async fn drive(source: &EvmRpcDataSource, from: u64, want: usize) -> Option<String> {
    let mut stream = source.get_stream(StreamRequest {
        from,
        to: None,
        parent_hash: None,
    });
    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut served = 0usize;
    while served < want {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) | Ok(None) => break,
            Ok(Some(Err(e))) => return Some(e.to_string()),
            Ok(Some(Ok(batch))) => served += batch.blocks.len(),
        }
    }
    None
}

/// A healthy run still has to make the budget visible: round trips and calls
/// are counted, batch items individually, and no failure class moves.
#[tokio::test]
async fn a_healthy_run_counts_the_budget_it_spends() {
    let upstream = Upstream::start(Chain::linear(FIRST, COUNT, 1)).await;
    let wired = wire(&upstream, 0);

    let finalized = wired
        .source
        .get_finalized_head()
        .await
        .expect("finalized head");
    assert_eq!(finalized.number, FIRST);

    let requests = sample(
        &wired.metrics,
        "sqd_hotblocks_upstream_requests_total{kind=\"single\"}",
    );
    assert!(requests >= 1.0, "the watermark read is a round trip");
    assert!(sample(&wired.metrics, "sqd_hotblocks_upstream_calls_total") >= requests);

    for class in ["rpc", "http", "connection", "timeout", "protocol"] {
        let key = format!("sqd_hotblocks_upstream_errors_total{{class=\"{class}\"}}");
        assert_eq!(
            sample(&wired.metrics, &key),
            0.0,
            "{class} moved on a healthy run"
        );
    }
}

/// OB-4's view: the watermarks the adapter last observed, each stamped so a
/// stale view reads as stale rather than as a plausible number.
#[tokio::test]
async fn the_upstream_view_is_recorded_with_its_observation_time() {
    let upstream = Upstream::start(Chain::linear(FIRST, COUNT, 1)).await;
    upstream.set_finalized(FIRST + 1);
    let wired = wire(&upstream, 0);

    wired
        .source
        .get_finalized_head()
        .await
        .expect("finalized head");
    assert_eq!(
        sample(&wired.metrics, "sqd_hotblocks_upstream_finalized_head"),
        (FIRST + 1) as f64
    );
    assert!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_upstream_finalized_head_timestamp_ms"
        ) > 0.0
    );

    // The head view comes from the speculative poll, which never reads the head
    // tag: a delivered block proves the upstream reached it.
    drive(&wired.source, FIRST + 1, 2).await;
    assert!(sample(&wired.metrics, "sqd_hotblocks_upstream_head") >= (FIRST + 2) as f64);
    assert!(sample(&wired.metrics, "sqd_hotblocks_upstream_head_timestamp_ms") > 0.0);
}

/// A failing endpoint is visible as failures by class, and a retried one as
/// retries — the two REQ-16 needs kept apart, since a run that recovers on
/// retry looks identical to a healthy one on any counter that merges them.
#[tokio::test]
async fn upstream_failures_and_retries_are_counted_by_class() {
    let upstream = Upstream::start(Chain::linear(FIRST, COUNT, 1)).await;
    // A rate-limit code, so FM-22 classes it retryable and the ladder runs.
    upstream.inject_for(
        "eth_getBlockByNumber",
        FIRST + 1,
        Fault::Error {
            code: -32005,
            message: "limit exceeded".into(),
        },
        2,
    );
    let wired = wire(&upstream, 4);

    drive(&wired.source, FIRST + 1, 2).await;

    assert!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_upstream_retries_total{class=\"rpc\"}"
        ) >= 2.0,
        "each faulted call that was retried is counted"
    );
    assert_eq!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_upstream_errors_total{class=\"timeout\"}"
        ),
        0.0,
        "an RPC error must not be reported as a timeout"
    );
}

/// WP-11.3's exhaustion is an OB-7 event, not just the session error that
/// follows it: a session dying is ordinary, giving up on a block is not.
#[tokio::test]
async fn acquisition_retry_exhaustion_is_an_event() {
    let upstream = Upstream::start(Chain::linear(FIRST, COUNT, TXS)).await;
    upstream.inject(
        DEBUG_METHOD,
        FAULTED,
        Fault::error("trace backend unavailable"),
    );
    let wired = wire_for(
        &upstream,
        0,
        DataRequest {
            receipts: true,
            traces: true,
            ..Default::default()
        },
    );

    let before = sample(
        &wired.metrics,
        "sqd_hotblocks_acquisition_retry_exhaustions_total",
    );
    assert_eq!(before, 0.0);

    let error = drive(&wired.source, FIRST + 1, (COUNT - 1) as usize).await;
    assert!(
        error.is_some_and(|e| e.contains(&FAULTED.to_string())),
        "the session must fail loud naming the block it gave up on"
    );
    assert_eq!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_acquisition_retry_exhaustions_total"
        ),
        1.0
    );
}

/// OB-3's detection half. Arrival lag as one number cannot say whether a block
/// was produced late or merely noticed late, and a poller can only do something
/// about the second. The gap is the wait preceding the poll that found the
/// block — the interval it may have sat unnoticed in — and the interval beside
/// it is what the cadence predictor is fed, so a mispredicting poller and a
/// chain with spread-out arrivals read differently.
#[tokio::test]
async fn head_detection_gap_and_interval_are_observed() {
    let upstream = Upstream::start(Chain::linear(FIRST, COUNT, 1)).await;
    // Above FIRST+1 the upstream answers "not produced yet", so the poller has
    // to wait for the head to move instead of walking a chain already there.
    upstream.set_head(FIRST + 1);
    let wired = wire(&upstream, 0);

    assert!(drive(&wired.source, FIRST + 1, 1).await.is_none());
    assert_eq!(
        sample(&wired.metrics, "sqd_hotblocks_head_detection_gap_ms_count"),
        0.0,
        "a block that was already there was not waited for"
    );

    // Same session, two arrivals: the second only exists after the head moves,
    // so the poller parks on it and discovers it a poll interval later.
    let (error, ()) = tokio::join!(drive(&wired.source, FIRST + 1, 2), async {
        tokio::time::sleep(Duration::from_millis(250)).await;
        upstream.set_head(FIRST + 2);
    });
    assert!(error.is_none(), "the head moved, the session must not fail");

    assert_eq!(
        sample(&wired.metrics, "sqd_hotblocks_head_detection_gap_ms_count"),
        1.0,
        "exactly the one discovery that ended a wait is charged"
    );
    let gap = sample(&wired.metrics, "sqd_hotblocks_head_detection_gap_ms_sum");
    assert!(
        (25.0..1000.0).contains(&gap),
        "the gap is the poll interval that hid the block, not the whole wait: {gap} ms"
    );

    assert_eq!(
        sample(&wired.metrics, "sqd_hotblocks_head_interval_ms_count"),
        1.0,
        "one interval per consecutive pair of arrivals"
    );
    let interval = sample(&wired.metrics, "sqd_hotblocks_head_interval_ms_sum");
    assert!(
        interval >= 200.0,
        "the arrival interval spans the wait for the head to move: {interval} ms"
    );
}

/// The same gap, with the consumer holding the handoff. A stream parked on its
/// reader is not a poller that mispredicted the cadence, and charging one as
/// the other would make the histogram read like a prediction problem in exactly
/// the incident where downstream is the problem.
#[tokio::test]
async fn consumer_backpressure_is_not_charged_to_detection() {
    const STALL: Duration = Duration::from_millis(600);

    let upstream = Upstream::start(Chain::linear(FIRST, COUNT, 1)).await;
    upstream.set_head(FIRST + 1);
    let wired = wire(&upstream, 0);

    let mut stream = wired.source.get_stream(StreamRequest {
        from: FIRST + 1,
        to: None,
        parent_hash: None,
    });

    // First batch in hand, the stream is parked at the handoff and the next
    // height only appears while the reader is away.
    let first = tokio::time::timeout(BUDGET, stream.next())
        .await
        .expect("first batch");
    assert!(first.is_some_and(|batch| batch.is_ok()));
    upstream.set_head(FIRST + 2);
    tokio::time::sleep(STALL).await;

    let second = tokio::time::timeout(BUDGET, stream.next())
        .await
        .expect("second batch");
    assert!(second.is_some_and(|batch| batch.is_ok()));

    assert!(
        sample(&wired.metrics, "sqd_hotblocks_head_detection_gap_ms_count") >= 1.0,
        "the discovery this pins has to be charged at all"
    );
    let gap = sample(&wired.metrics, "sqd_hotblocks_head_detection_gap_ms_sum");
    assert!(
        gap < STALL.as_millis() as f64,
        "the reader's {STALL:?} stall must not read as detection delay: {gap} ms"
    );
}
