//! CT-4 — component-fault corpus for the trace and state-diff acquisition
//! paths (GAP-3).
//!
//! Each case injects one fault into one component call of one block and asserts
//! the same contract: the whole block is re-acquired a bounded number of times
//! (WP-11.2), then the session fails loud (WP-11.3) — never served with the
//! component absent, emptied, or partly filled (INV-28, REQ-9), never with
//! another block's (IB-15). Driving the real adapter also pins 14 §upstream:
//! the methods, their parameters, and which component each one carries.

use std::sync::Arc;
use std::time::Duration;

use data_service_core::source::{DataSource, StreamRequest};
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use futures::StreamExt;
use harness::upstream::{Chain, Fault, Upstream};
use rpc_client::{RpcClient, RpcClientConfig};
use serde_json::Value;

/// spec/15 — P-ENRICH-RETRIES on the head path: the whole budget a
/// persistently faulted block gets.
const P_ENRICH_RETRIES: usize = 10;
const ACQUISITIONS: usize = 1 + P_ENRICH_RETRIES;

const FIRST: u64 = 1000;
/// Two blocks precede it, so a test tells "served nothing" from "served up to
/// the fault".
const FAULTED: u64 = 1002;
const TXS: usize = 2;

/// Over the retry budget (10 × 50 ms), under a CI hang.
const BUDGET: Duration = Duration::from_secs(20);

// ─── Driver ───────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Run {
    /// Decoded payload lines in delivery order.
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

    /// Every served block must carry an entry per transaction under `key`; a
    /// thinned component is the defect.
    fn assert_components_complete(&self, key: &str) {
        for block in &self.served {
            let number = block["header"]["number"].as_u64().expect("header.number");
            let txs = block["transactions"]
                .as_array()
                .expect("transactions")
                .len();
            let entries = block[key]
                .as_array()
                .unwrap_or_else(|| panic!("block {number} served without the selected {key}"));
            let covered: std::collections::HashSet<u64> = entries
                .iter()
                .filter_map(|e| e["transactionIndex"].as_u64())
                .collect();
            assert_eq!(
                covered.len(),
                txs,
                "block {number} served with {key} covering {covered:?} of {txs} transactions"
            );
        }
    }

    fn assert_failed_loud(&self, faulted: u64) {
        assert!(
            !self.numbers().contains(&faulted),
            "block {faulted} was served despite its component fault; served {:?}",
            self.numbers()
        );
        let error = self
            .error
            .as_deref()
            .expect("acquisition must fail the session after the retry budget, not stall");
        assert!(
            error.contains(&faulted.to_string()),
            "the session error must name the block it gave up on, got: {error}"
        );
    }
}

/// Drive the real adapter's head stream against `upstream` until it errors,
/// ends, serves `want` blocks, or runs out of budget.
async fn drive(upstream: &Upstream, req: DataRequest, from: u64, want: usize) -> Run {
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));
    let source = EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            data_request: req,
            ..Default::default()
        },
    );

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

#[derive(Clone, Copy)]
enum PrimedStream {
    Head,
    Finalized,
}

/// Drive an adapter path after priming its local finality view.
async fn drive_primed(
    upstream: &Upstream,
    req: DataRequest,
    from: u64,
    to: Option<u64>,
    expected_finalized: u64,
    budget: Duration,
    stream_kind: PrimedStream,
) -> Run {
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));
    let source = EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            data_request: req,
            ..Default::default()
        },
    );
    let finalized = source
        .get_finalized_head()
        .await
        .expect("prime the adapter's local finalized-head view");
    assert_eq!(finalized.number, expected_finalized);

    let request = StreamRequest {
        from,
        to,
        parent_hash: None,
    };
    let mut stream = match stream_kind {
        PrimedStream::Head => source.get_stream(request),
        PrimedStream::Finalized => source.get_finalized_stream(request),
    };
    let deadline = tokio::time::Instant::now() + budget;
    let mut run = Run::default();
    loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) | Ok(None) => break,
            Ok(Some(Err(error))) => {
                run.error = Some(error.to_string());
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

async fn head_chain() -> Upstream {
    // A head four above `FIRST` keeps ingestion on the speculative head path;
    // the range path needs a wider gap.
    Upstream::start(Chain::linear(FIRST, 5, TXS)).await
}

// ─── Data selections ──────────────────────────────────────────────────────────

/// Execution traces over the debug API — the default EVM selection.
fn debug_traces() -> DataRequest {
    DataRequest {
        receipts: true,
        traces: true,
        ..Default::default()
    }
}

/// State diffs over the debug API (prestateTracer).
fn debug_state_diffs() -> DataRequest {
    DataRequest {
        receipts: true,
        state_diffs: true,
        use_debug_api_for_state_diffs: true,
        ..Default::default()
    }
}

/// Traces and state diffs over the trace API — one replay call carries both.
fn trace_api() -> DataRequest {
    DataRequest {
        receipts: true,
        traces: true,
        state_diffs: true,
        use_trace_api: true,
        ..Default::default()
    }
}

/// Traces over `trace_block`, the selection that takes neither replay tracer.
fn trace_block() -> DataRequest {
    DataRequest {
        receipts: true,
        traces: true,
        use_trace_api: true,
        ..Default::default()
    }
}

const DEBUG_METHOD: &str = "debug_traceBlockByHash";
const REPLAY_METHOD: &str = "trace_replayBlockTransactions";
const TRACE_BLOCK_METHOD: &str = "trace_block";

// ─── Control: an upstream that answers truthfully ─────────────────────────────

#[tokio::test]
async fn healthy_upstream_serves_complete_components() {
    let upstream = head_chain().await;
    let run = drive(&upstream, debug_traces(), FIRST, 3).await;

    assert_eq!(run.numbers(), vec![FIRST, FIRST + 1, FIRST + 2]);
    run.assert_components_complete("traces");
    assert_eq!(
        run.error, None,
        "a truthful upstream must not fail a session"
    );
}

#[tokio::test]
async fn healthy_trace_api_serves_traces_and_state_diffs() {
    let upstream = head_chain().await;
    let run = drive(&upstream, trace_api(), FIRST, 3).await;

    run.assert_components_complete("traces");
    run.assert_components_complete("stateDiffs");
    assert_eq!(run.error, None);
}

#[tokio::test]
async fn trace_block_uses_number_addressing() {
    let upstream = head_chain().await;

    let run = drive(&upstream, trace_block(), FIRST, 1).await;

    assert_eq!(run.numbers(), vec![FIRST]);
    assert!(upstream.calls_by_number(TRACE_BLOCK_METHOD, FIRST) > 0);
    assert_eq!(upstream.calls_by_hash(TRACE_BLOCK_METHOD, FIRST), 0);
}

#[tokio::test]
async fn trace_replay_keeps_hash_addressing() {
    let upstream = head_chain().await;

    let run = drive(&upstream, trace_api(), FIRST, 1).await;

    assert_eq!(run.numbers(), vec![FIRST]);
    assert!(upstream.calls_by_hash(REPLAY_METHOD, FIRST) > 0);
    assert_eq!(upstream.calls_by_number(REPLAY_METHOD, FIRST), 0);
}

// ─── Fault classes on the execution-trace path ────────────────────────────────

#[tokio::test]
async fn trace_upstream_error_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(
        DEBUG_METHOD,
        FAULTED,
        Fault::error("trace backend unavailable"),
    );

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(
        upstream.calls(DEBUG_METHOD, FAULTED),
        ACQUISITIONS,
        "the block must be re-acquired P-ENRICH-RETRIES times, no more and no fewer"
    );
}

#[tokio::test]
async fn trace_null_result_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::Null);

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(DEBUG_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn trace_of_another_block_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::WrongBlock);

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(DEBUG_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn trace_covering_part_of_the_block_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::Truncated);

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(DEBUG_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn unparsable_trace_payload_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::Malformed);

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(DEBUG_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn unparsable_trace_frame_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::MalformedEntry);

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(DEBUG_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn structurally_unmappable_trace_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::InvalidDebugFrameStructure);

    let run = drive(&upstream, debug_traces(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(
        upstream.calls(DEBUG_METHOD, FAULTED),
        ACQUISITIONS,
        "a schema-valid but unmappable tree gets the bounded whole-block retry budget"
    );
}

// ─── The same faults on the state-diff path ───────────────────────────────────

#[tokio::test]
async fn state_diff_upstream_error_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject_tracer(
        DEBUG_METHOD,
        "prestateTracer",
        FAULTED,
        Fault::error("prestate tracer unavailable"),
    );

    let run = drive(&upstream, debug_state_diffs(), FIRST, 100).await;

    run.assert_components_complete("stateDiffs");
    run.assert_failed_loud(FAULTED);
    assert_eq!(
        upstream.calls_with_tracer(DEBUG_METHOD, "prestateTracer", FAULTED),
        ACQUISITIONS
    );
}

#[tokio::test]
async fn state_diff_null_result_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject_tracer(DEBUG_METHOD, "prestateTracer", FAULTED, Fault::Null);

    let run = drive(&upstream, debug_state_diffs(), FIRST, 100).await;

    run.assert_components_complete("stateDiffs");
    run.assert_failed_loud(FAULTED);
}

#[tokio::test]
async fn state_diff_of_another_block_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject_tracer(DEBUG_METHOD, "prestateTracer", FAULTED, Fault::WrongBlock);

    let run = drive(&upstream, debug_state_diffs(), FIRST, 100).await;

    run.assert_components_complete("stateDiffs");
    run.assert_failed_loud(FAULTED);
}

// ─── The trace-API paths ──────────────────────────────────────────────────────

#[tokio::test]
async fn replay_of_another_block_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::WrongBlock);

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_components_complete("stateDiffs");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(REPLAY_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn replay_covering_part_of_the_block_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::Truncated);

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_components_complete("stateDiffs");
    run.assert_failed_loud(FAULTED);
}

#[tokio::test]
async fn replay_without_the_requested_state_diff_retries_then_fails_loud() {
    let upstream = head_chain().await;
    // Every transaction answered, none with the tracer's data: coverage by hash
    // alone passes this and serves `"stateDiffs": []`.
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::DropKey("stateDiff".into()));

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_components_complete("stateDiffs");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(REPLAY_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn replay_without_the_requested_trace_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::DropKey("trace".into()));

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
}

#[tokio::test]
async fn replay_with_an_empty_trace_retries_then_fails_loud() {
    let upstream = head_chain().await;
    // Every transaction executes at least its root frame, so an empty list is a
    // missing component, not a transaction that did nothing.
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::EmptyKey("trace".into()));

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(REPLAY_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn replay_mixing_two_transactions_retries_then_fails_loud() {
    let upstream = head_chain().await;
    // An unlabelled entry whose frames span two transactions still counts one
    // per transaction, and normalization files every frame under the first.
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::MixedFrames);

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(REPLAY_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn duplicated_replay_entry_retries_then_fails_loud() {
    let upstream = head_chain().await;
    // A repeated entry emits that transaction's traces twice.
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::Duplicated);

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_failed_loud(FAULTED);
}

/// The result of the second call was discarded (GAP-17), so a provider without
/// the replay method paid for it — and would now fail ingestion over an answer
/// nothing reads.
#[tokio::test]
async fn trace_block_selection_does_not_call_the_replay_method() {
    let upstream = head_chain().await;
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::error("method not supported"));

    let run = drive(&upstream, trace_block(), FIRST, 4).await;

    assert_eq!(
        upstream.calls(REPLAY_METHOD, FAULTED),
        0,
        "one selection, one trace method"
    );
    assert!(run.numbers().contains(&FAULTED));
    run.assert_components_complete("traces");
    assert_eq!(run.error, None);
}

#[tokio::test]
async fn unparsable_replay_payload_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(REPLAY_METHOD, FAULTED, Fault::MalformedEntry);

    let run = drive(&upstream, trace_api(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
}

#[tokio::test]
async fn trace_block_of_another_block_retries_then_fails_loud() {
    let upstream = head_chain().await;
    // Models a reorg between fetching header A at this height and asking
    // trace_block(number): the provider now answers with branch B's frames.
    upstream.inject(TRACE_BLOCK_METHOD, FAULTED, Fault::WrongBlock);

    let run = drive(&upstream, trace_block(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(TRACE_BLOCK_METHOD, FAULTED), ACQUISITIONS);
    assert_eq!(
        upstream.calls_by_number(TRACE_BLOCK_METHOD, FAULTED),
        ACQUISITIONS,
        "the by-number compatibility path must still reject frames bound to another hash"
    );
}

#[tokio::test]
async fn trace_block_without_block_hash_retries_then_fails_loud() {
    let upstream = head_chain().await;
    upstream.inject(
        TRACE_BLOCK_METHOD,
        FAULTED,
        Fault::DropKey("blockHash".into()),
    );

    let run = drive(&upstream, trace_block(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(TRACE_BLOCK_METHOD, FAULTED), ACQUISITIONS);
}

#[tokio::test]
async fn trace_block_missing_a_transaction_retries_then_fails_loud() {
    let upstream = head_chain().await;
    // Pin the coverage guard behind the portable by-number request: even a
    // reward-only answer must never be served as a coherent block.
    upstream.inject(TRACE_BLOCK_METHOD, FAULTED, Fault::Truncated);

    let run = drive(&upstream, trace_block(), FIRST, 100).await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
}

// ─── The retry has to heal, not just fail ─────────────────────────────────────

#[tokio::test]
async fn transient_trace_fault_heals_inside_the_retry_budget() {
    let upstream = head_chain().await;
    upstream.inject_for(DEBUG_METHOD, FAULTED, Fault::Null, 3);

    let run = drive(&upstream, debug_traces(), FIRST, 4).await;

    assert_eq!(
        run.error, None,
        "a fault inside the budget must not end the session"
    );
    assert!(
        run.numbers().contains(&FAULTED),
        "the block must be served once re-acquisition succeeds; served {:?}",
        run.numbers()
    );
    run.assert_components_complete("traces");
    assert_eq!(
        upstream.calls(DEBUG_METHOD, FAULTED),
        4,
        "three faulted acquisitions and the one that succeeded"
    );
}

// ─── The range path ───────────────────────────────────────────────────────────

#[tokio::test]
async fn range_incoherence_retries_then_fails_loud() {
    // A distant finalized head puts ingestion on the strided range path.
    let upstream = Upstream::start(Chain::linear(FIRST, 20, TXS)).await;
    upstream.set_finalized(FIRST + 19);
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::Null);

    let run = drive_primed(
        &upstream,
        debug_traces(),
        FIRST,
        None,
        FIRST + 19,
        BUDGET,
        PrimedStream::Head,
    )
    .await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(
        run.numbers(),
        vec![FIRST, FIRST + 1],
        "complete blocks below the fault must be delivered before it fails"
    );
    assert_eq!(
        upstream.calls(DEBUG_METHOD, FAULTED),
        ACQUISITIONS,
        "the range path owes the same per-block retry budget as the head path"
    );
}

#[tokio::test]
async fn short_finalized_range_incoherence_retries_then_fails_loud() {
    // A bounded request no wider than one stride takes finalized poll mode,
    // even when the adapter already knows a much higher finalized head.
    let upstream = Upstream::start(Chain::linear(FIRST, 20, TXS)).await;
    upstream.set_finalized(FIRST + 19);
    upstream.inject(DEBUG_METHOD, FAULTED, Fault::Null);

    let run = drive_primed(
        &upstream,
        debug_traces(),
        FIRST,
        Some(FIRST + 3),
        FIRST + 19,
        BUDGET,
        PrimedStream::Finalized,
    )
    .await;

    run.assert_components_complete("traces");
    run.assert_failed_loud(FAULTED);
    assert_eq!(
        run.numbers(),
        vec![FIRST, FIRST + 1],
        "the complete prefix must be delivered before the short range fails"
    );
    assert_eq!(
        upstream.calls(DEBUG_METHOD, FAULTED),
        ACQUISITIONS,
        "finalized poll mode owes the same bounded acquisition budget"
    );
}
