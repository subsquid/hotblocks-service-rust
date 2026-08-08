//! CT-4 — FM-16 not-ready lag on the speculative head path.
//!
//! A provider whose receipts lag its headers answers null for a block it has
//! already announced — routine on load-balanced fleets. That is data catching
//! up, not incoherence: the enrichment loop budgets it by wall clock
//! (P-NOT-READY-BUDGET, ~100 ms per poll), while verification failures keep
//! the bounded P-ENRICH-RETRIES × P-ENRICH-DELAY budget (WP-11.2/WP-11.3).

use std::sync::Arc;
use std::time::Duration;

use data_service_core::metrics::Metrics;
use data_service_core::source::{DataSource, StreamRequest};
use evm_source::fetch::RpcOptions;
use evm_source::rpc_data::RpcReceipt;
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use evm_source::verification;
use futures::StreamExt;
use harness::upstream::{Chain, Fault, Upstream};
use rpc_client::{CallOptions, RpcClient, RpcClientConfig};
use serde_json::{json, Value};

/// spec/15 — P-ENRICH-RETRIES: the whole budget an *incoherent* block gets.
const P_ENRICH_RETRIES: usize = 10;
const ACQUISITIONS: usize = 1 + P_ENRICH_RETRIES;

const FIRST: u64 = 1000;
/// Two blocks precede it, so a test tells "served nothing" from "served up to
/// the fault".
const FAULTED: u64 = 1002;
const TXS: usize = 2;

const HEADER_METHOD: &str = "eth_getBlockByNumber";
const RECEIPTS_METHOD: &str = "eth_getBlockReceipts";

/// Over every budget under test, under a CI hang.
const BUDGET: Duration = Duration::from_secs(15);

#[derive(Default)]
struct Run {
    served: Vec<Value>,
    error: Option<String>,
}

impl Run {
    fn numbers(&self) -> Vec<u64> {
        self.served
            .iter()
            .map(|b| b["header"]["number"].as_u64().expect("header.number"))
            .collect()
    }

    fn assert_failed_loud(&self, faulted: u64) {
        assert!(
            !self.numbers().contains(&faulted),
            "block {faulted} was served despite its fault; served {:?}",
            self.numbers()
        );
        let error = self
            .error
            .as_deref()
            .expect("acquisition must fail the session after its budget, not stall");
        assert!(
            error.contains(&faulted.to_string()),
            "the session error must name the block it gave up on, got: {error}"
        );
    }
}

struct Wired {
    source: EvmRpcDataSource,
    metrics: Arc<Metrics>,
}

fn wire(upstream: &Upstream, rpc_options: RpcOptions, req: DataRequest) -> Wired {
    let metrics = Arc::new(Metrics::new());
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));
    let source = EvmRpcDataSource::with_metrics(
        client,
        EvmRpcDataSourceOptions {
            rpc_options,
            data_request: req,
            ..Default::default()
        },
        Arc::clone(&metrics),
    );
    Wired { source, metrics }
}

/// Drive the head stream until it errors, ends, serves `want` blocks, or runs
/// out of budget.
async fn drive(source: &EvmRpcDataSource, from: u64, want: usize) -> Run {
    let mut stream = source.get_stream(StreamRequest {
        from,
        to: None,
        parent_hash: None,
    });
    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut run = Run::default();
    while run.served.len() < want {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_elapsed) => break,
            Ok(None) => break,
            Ok(Some(Err(e))) => {
                run.error = Some(e.to_string());
                break;
            }
            Ok(Some(Ok(batch))) => {
                for block in batch.blocks {
                    let line = zstd::decode_all(block.json_line_zstd.as_ref()).expect("zstd");
                    run.served
                        .push(serde_json::from_slice(&line).expect("payload is one JSON line"));
                }
            }
        }
    }
    run
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

fn with_receipts() -> DataRequest {
    DataRequest {
        receipts: true,
        ..Default::default()
    }
}

fn not_ready_budget(budget: Duration) -> RpcOptions {
    RpcOptions {
        not_ready_budget: Some(budget),
        ..Default::default()
    }
}

/// Receipts lag past the old 10-retry bound, then arrive: ordinary
/// head-following, never a session error.
#[tokio::test]
async fn receipts_lag_beyond_the_incoherent_bound_heals() {
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject_for(RECEIPTS_METHOD, FAULTED, Fault::Null, 15);

    let wired = wire(
        &upstream,
        not_ready_budget(Duration::from_secs(5)),
        with_receipts(),
    );
    let run = drive(&wired.source, FIRST, 4).await;

    assert_eq!(
        run.error, None,
        "receipts lag inside the not-ready budget must not end the session"
    );
    assert!(
        run.numbers().contains(&FAULTED),
        "the block must commit once its receipts arrive; served {:?}",
        run.numbers()
    );
    assert_eq!(
        upstream.calls(RECEIPTS_METHOD, FAULTED),
        16,
        "fifteen not-ready answers and the one that succeeded"
    );
}

/// Receipts never ready: the session fails loud only once the wall-clock
/// budget elapses — however many polls that takes — and the exhaustion is an
/// OB-7 event on the scrape surface.
#[tokio::test]
async fn receipts_never_ready_fails_after_the_wall_clock_budget() {
    const NOT_READY_BUDGET: Duration = Duration::from_secs(2);
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject(RECEIPTS_METHOD, FAULTED, Fault::Null);

    let wired = wire(
        &upstream,
        not_ready_budget(NOT_READY_BUDGET),
        with_receipts(),
    );
    let started = std::time::Instant::now();
    let run = drive(&wired.source, FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    assert!(
        started.elapsed() >= NOT_READY_BUDGET,
        "the session must outlast the not-ready wall-clock budget, failed after {:?}",
        started.elapsed()
    );
    assert!(
        upstream.calls(RECEIPTS_METHOD, FAULTED) > ACQUISITIONS,
        "not-ready polls are budgeted by wall clock, not by the incoherent attempt count; \
         got {} calls",
        upstream.calls(RECEIPTS_METHOD, FAULTED)
    );
    assert_eq!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_acquisition_retry_exhaustions_total"
        ),
        1.0
    );
}

/// The not-ready budget is a hard wall over the whole phase: a re-fetch that
/// hangs cannot stretch the window by its own round-trip time — the deadline
/// cancels it mid-flight and fails the session on time.
#[tokio::test]
async fn not_ready_budget_is_a_hard_wall_clock_bound() {
    const NOT_READY_BUDGET: Duration = Duration::from_millis(700);
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject(RECEIPTS_METHOD, FAULTED, Fault::Null);
    // The speculative body poll must stay instant; every whole-block re-fetch
    // after the first not-ready answer hangs far past the budget.
    upstream.inject_for(HEADER_METHOD, FAULTED, Fault::Delay(Duration::ZERO), 1);
    upstream.inject(HEADER_METHOD, FAULTED, Fault::Delay(Duration::from_secs(5)));

    let wired = wire(
        &upstream,
        not_ready_budget(NOT_READY_BUDGET),
        with_receipts(),
    );
    let started = std::time::Instant::now();
    let run = drive(&wired.source, FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    let error = run.error.as_deref().expect("checked by assert_failed_loud");
    assert!(
        error.contains("still not ready"),
        "the hard bound must surface the not-ready exhaustion, got: {error}"
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed >= NOT_READY_BUDGET,
        "the budget itself must elapse first, failed after {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the deadline must cancel the hung re-fetch, not wait it out; failed after {elapsed:?}"
    );
    assert_eq!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_acquisition_retry_exhaustions_total"
        ),
        1.0
    );
}

/// A short receipts array under an enabled receipts-root check is still
/// receipts catching up (FM-16): the count verdict must come before
/// verification, or the partial set fails the commitment and burns the bounded
/// incoherent budget instead of the not-ready one (GAP-40).
#[tokio::test]
async fn short_receipts_under_verification_heal_as_not_ready() {
    // Transactions only in the faulted block: every other receiptsRoot stays
    // the honest empty-trie root the stub serves.
    let mut chain = Chain::linear(FIRST, 5, TXS);
    for b in &mut chain.blocks {
        if b.number != FAULTED {
            b.txs.clear();
        }
    }
    let upstream = Upstream::start(chain).await;

    // The stub's receiptsRoot for a non-empty block is an opaque digest; forge
    // it to the honest root of the full receipts set, so the healed answer
    // verifies and only a short one fails the commitment. The probe costs one
    // receipts call.
    let probe = RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 1,
        retry_attempts: 0,
        ..Default::default()
    });
    let truthful = probe
        .call(
            RECEIPTS_METHOD,
            Some(json!([format!("0x{FAULTED:x}")])),
            CallOptions::default(),
        )
        .await
        .expect("the truthful receipts answer");
    let receipts: Vec<RpcReceipt> = serde_json::from_value(truthful).expect("receipts parse");
    let refs: Vec<&RpcReceipt> = receipts.iter().collect();
    let honest_root = verification::receipts_root(&refs, false).expect("receipts root");
    upstream.inject(
        HEADER_METHOD,
        FAULTED,
        Fault::ForgedField {
            key: "receiptsRoot".to_string(),
            value: json!(honest_root),
        },
    );
    upstream.inject_for(RECEIPTS_METHOD, FAULTED, Fault::Truncated, 15);

    let rpc_options = RpcOptions {
        verify_receipts_root: true,
        ..not_ready_budget(Duration::from_secs(5))
    };
    let wired = wire(&upstream, rpc_options, with_receipts());
    let run = drive(&wired.source, FIRST, 4).await;

    assert_eq!(
        run.error, None,
        "a short receipts answer under verification is data catching up, not incoherence"
    );
    assert!(
        run.numbers().contains(&FAULTED),
        "the block must commit once its receipts are complete; served {:?}",
        run.numbers()
    );
    assert!(
        upstream.calls(RECEIPTS_METHOD, FAULTED) > ACQUISITIONS + 1,
        "short answers are budgeted by wall clock, past the 1 + P_ENRICH_RETRIES incoherent \
         bound; got {} calls (one of them the probe)",
        upstream.calls(RECEIPTS_METHOD, FAULTED)
    );
}

/// A head block whose header call answers a non-block payload is incoherence
/// (WP-11.4), not FM-16 lag: the session fails on the bounded incoherent
/// budget, well inside a not-ready wall clock sized to expose the
/// misclassification as a visible hang.
#[tokio::test]
async fn a_malformed_head_block_keeps_the_bounded_incoherent_budget() {
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject(HEADER_METHOD, FAULTED, Fault::Malformed);

    let wired = wire(
        &upstream,
        not_ready_budget(Duration::from_secs(30)),
        with_receipts(),
    );
    let started = std::time::Instant::now();
    let run = drive(&wired.source, FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an unparsable header must burn the bounded incoherent budget, not the 30 s \
         not-ready wall clock; failed after {:?}",
        started.elapsed()
    );
    assert_eq!(
        upstream.calls(HEADER_METHOD, FAULTED),
        ACQUISITIONS,
        "the incoherent budget stays 1 + P_ENRICH_RETRIES"
    );
    assert_eq!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_acquisition_retry_exhaustions_total"
        ),
        1.0
    );
}

/// A per-item RPC error on the header call is the same incoherence: only a
/// genuine null may claim the not-ready budget.
#[tokio::test]
async fn a_head_block_rpc_error_keeps_the_bounded_incoherent_budget() {
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject(HEADER_METHOD, FAULTED, Fault::error("backend exploded"));

    let wired = wire(
        &upstream,
        not_ready_budget(Duration::from_secs(30)),
        with_receipts(),
    );
    let started = std::time::Instant::now();
    let run = drive(&wired.source, FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an errored header must burn the bounded incoherent budget, not the 30 s \
         not-ready wall clock; failed after {:?}",
        started.elapsed()
    );
    assert_eq!(
        upstream.calls(HEADER_METHOD, FAULTED),
        ACQUISITIONS,
        "the incoherent budget stays 1 + P_ENRICH_RETRIES"
    );
}

/// The same split inside the enrichment loop: a whole-block re-fetch whose
/// header answer turns unparsable is incoherent, and must not keep the
/// not-ready wall clock the null receipts answer before it earned.
#[tokio::test]
async fn a_malformed_refetch_during_enrichment_keeps_the_bounded_incoherent_budget() {
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject(RECEIPTS_METHOD, FAULTED, Fault::Null);
    // The speculative body poll answers truthfully; every whole-block re-fetch
    // after the first not-ready answer is unparsable.
    upstream.inject_for(HEADER_METHOD, FAULTED, Fault::Delay(Duration::ZERO), 1);
    upstream.inject(HEADER_METHOD, FAULTED, Fault::Malformed);

    let wired = wire(
        &upstream,
        not_ready_budget(Duration::from_secs(30)),
        with_receipts(),
    );
    let started = std::time::Instant::now();
    let run = drive(&wired.source, FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an unparsable re-fetch must burn the bounded incoherent budget, not the 30 s \
         not-ready wall clock; failed after {:?}",
        started.elapsed()
    );
    assert_eq!(
        upstream.calls(HEADER_METHOD, FAULTED),
        // The body poll, then the not-ready re-fetch and the incoherent ones.
        1 + ACQUISITIONS,
        "the incoherent budget stays 1 + P_ENRICH_RETRIES past the first not-ready answer"
    );
    assert_eq!(
        sample(
            &wired.metrics,
            "sqd_hotblocks_acquisition_retry_exhaustions_total"
        ),
        1.0
    );
}

/// Control: a genuinely absent (null) head block is FM-16 lag — the poll loop
/// waits it out and serves the block once produced, never burning the
/// incoherent budget.
#[tokio::test]
async fn an_absent_head_block_still_heals_as_not_ready() {
    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    upstream.inject_for(HEADER_METHOD, FAULTED, Fault::Null, 15);

    let wired = wire(
        &upstream,
        not_ready_budget(Duration::from_secs(30)),
        with_receipts(),
    );
    let run = drive(&wired.source, FIRST, 4).await;

    assert_eq!(
        run.error, None,
        "an absent head block is ordinary head-following, not a session error"
    );
    assert!(
        run.numbers().contains(&FAULTED),
        "the block must commit once produced; served {:?}",
        run.numbers()
    );
    assert_eq!(
        upstream.calls(HEADER_METHOD, FAULTED),
        16,
        "fifteen null answers and the one that succeeded"
    );
}

/// Control: a forged commitment is incoherence — waiting cannot heal a wrong
/// answer — and keeps the exact 1 + P_ENRICH_RETRIES acquisition bound.
#[tokio::test]
async fn a_verification_failure_keeps_the_bounded_incoherent_budget() {
    // Empty blocks, so every honest commitment verifies and only the forged
    // one fails.
    let upstream = Upstream::start(Chain::linear(FIRST, 5, 0)).await;
    upstream.inject(
        HEADER_METHOD,
        FAULTED,
        Fault::ForgedField {
            key: "receiptsRoot".to_string(),
            value: json!(format!("0x{:064x}", 0xdeadu64)),
        },
    );

    let rpc_options = RpcOptions {
        verify_receipts_root: true,
        ..Default::default()
    };
    let wired = wire(&upstream, rpc_options, with_receipts());
    let run = drive(&wired.source, FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    assert_eq!(
        upstream.calls(RECEIPTS_METHOD, FAULTED),
        ACQUISITIONS,
        "the incoherent budget stays 1 + P_ENRICH_RETRIES, untouched by the not-ready split"
    );
}
