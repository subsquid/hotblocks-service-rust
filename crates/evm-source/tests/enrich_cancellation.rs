//! The enrichment legs run concurrently, but their errors keep a fixed
//! precedence (logs → receipts → traces, and inside traces: debug frames →
//! debug diffs → replay → trace_block). A leg that fails must therefore drop
//! every leg below it instead of awaiting it: the lower leg's answer can no
//! longer be read, and with the request timeout disabled (the client default) a
//! provider that accepted the call and went quiet holds the batch open forever.
//!
//! Each test pairs a failing leg with a lower-priority leg that never answers
//! and asserts the failure still surfaces, carrying the failing leg's own
//! error. Without the drop the call hangs and the test fails on `PATIENCE`.
//!
//! Two properties of the real path shape the setup:
//!   - Only transport-level failures make a leg return `Err`; a per-call
//!     JSON-RPC error is captured as an incoherent component instead. So the
//!     mock fails whole requests with a non-retryable HTTP status, one status
//!     per method so the assertions can tell the legs apart.
//!   - A batch of one is issued as a single call whose error is likewise
//!     captured, so the fixture is enriched as two blocks.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use evm_source::fetch::{Rpc, RpcOptions};
use evm_source::rpc_data::{RawRpcBlock, RpcBlock};
use evm_source::types::DataRequest;

/// Long enough that a hung leg cannot slip through, short enough to fail fast.
const PATIENCE: Duration = Duration::from_secs(5);

const LOGS_STATUS: u16 = 400;
const RECEIPTS_STATUS: u16 = 403;
const FRAMES_STATUS: u16 = 404;

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {name}: {e}")),
    )
    .unwrap_or_else(|e| panic!("parse {name}: {e}"))
}

struct MockState {
    chain_id: Value,
    /// Methods answered with a non-retryable HTTP status.
    failing: Vec<(&'static str, u16)>,
    /// Methods that are accepted and never answered.
    stuck: Vec<&'static str>,
}

/// Answers the calls the enrichment makes on its way to the legs (chain id, the
/// receipts-method probe), then applies the per-method failing/stuck policy.
async fn handler(State(s): State<Arc<MockState>>, body: axum::body::Bytes) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = req.is_array();
    let reqs: Vec<Value> = if batched {
        req.as_array().expect("array").clone()
    } else {
        vec![req]
    };

    // `eth_getBlockReceipts` against "latest" is the probe that picks the
    // per-block method; answering it with an array keeps the receipts leg on
    // `eth_getBlockReceipts` instead of falling back to per-transaction calls.
    let is_probe = |r: &Value| {
        r["method"] == "eth_getBlockReceipts" && r["params"].get(0) == Some(&json!("latest"))
    };
    let policy_methods: Vec<&str> = reqs
        .iter()
        .filter(|r| !is_probe(r))
        .filter_map(|r| r.get("method").and_then(Value::as_str))
        .collect();

    if policy_methods.iter().any(|m| s.stuck.contains(m)) {
        // Never answer and never hang up: the provider that took the call and
        // went quiet. Only the caller dropping this future ends the wait.
        std::future::pending::<()>().await;
    }

    if let Some((_, status)) = s
        .failing
        .iter()
        .find(|(m, _)| policy_methods.contains(m))
        .copied()
    {
        return (StatusCode::from_u16(status).expect("status"), String::new()).into_response();
    }

    let mut resps = Vec::new();
    for r in &reqs {
        let id = r.get("id").cloned().unwrap_or(json!(1));
        let result = match r.get("method").and_then(Value::as_str).unwrap_or("") {
            "eth_chainId" => s.chain_id.clone(),
            "eth_getBlockReceipts" if is_probe(r) => json!([]),
            _ => Value::Null,
        };
        resps.push(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    let body = if batched {
        Value::Array(resps)
    } else {
        resps.remove(0)
    };
    axum::Json(body).into_response()
}

/// Two blocks built from the captured header, so every leg issues a real batch.
fn two_blocks(cassette: &Value) -> Vec<RawRpcBlock> {
    let block: RpcBlock =
        serde_json::from_value(cassette["getBlockByNumber"].clone()).expect("cassette header");
    let first = u64::from_str_radix(block.number.trim_start_matches("0x"), 16).expect("number");
    (0..2)
        .map(|i| {
            let mut b = block.clone();
            let number = first + i;
            b.number = format!("0x{number:x}");
            b.hash = format!("0x{number:064x}");
            RawRpcBlock::new(number, b.hash.clone(), b)
        })
        .collect()
}

/// Enrich under `req` against a server that fails `failing` and stonewalls
/// `stuck`. Returns the reported error, or panics if the call never settled.
async fn enrich_error(
    req: DataRequest,
    failing: Vec<(&'static str, u16)>,
    stuck: Vec<&'static str>,
) -> String {
    let cassette = fixture("gnosis-pipeline.json");
    let blocks = two_blocks(&cassette);
    let state = Arc::new(MockState {
        chain_id: cassette["chain_id"].clone(),
        failing,
        stuck,
    });

    let app = Router::new().route("/", post(handler)).with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let client = {
        use rpc_client::{RpcClient, RpcClientConfig};
        Arc::new(RpcClient::new(RpcClientConfig {
            url: format!("http://127.0.0.1:{}", addr.port()),
            // Enough permits for every leg to be in flight at once, no retries
            // to muddy the timing, and the default zero request timeout — the
            // configuration in which a stuck leg never resolves on its own.
            capacity: 8,
            retry_attempts: 0,
            ..Default::default()
        }))
    };
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));

    match tokio::time::timeout(PATIENCE, rpc.enrich_blocks(blocks, &req)).await {
        Err(_) => panic!("enrichment waited on a leg below the failure instead of returning"),
        Ok(Ok(_)) => panic!("expected the failing leg to surface an error"),
        Ok(Err(e)) => format!("{e:#}"),
    }
}

fn base_req() -> DataRequest {
    DataRequest {
        logs: false,
        receipts: false,
        traces: false,
        state_diffs: false,
        use_trace_api: false,
        use_debug_api_for_state_diffs: false,
        use_debug_trace_block_by_number: true,
        debug_trace_timeout: None,
    }
}

#[tokio::test]
async fn logs_failure_does_not_wait_for_a_stuck_receipts_leg() {
    let req = DataRequest {
        logs: true,
        receipts: true,
        ..base_req()
    };
    let err = enrich_error(
        req,
        vec![("eth_getLogs", LOGS_STATUS)],
        vec!["eth_getBlockReceipts"],
    )
    .await;
    assert!(
        err.contains(&LOGS_STATUS.to_string()),
        "the logs error must be the one reported, got: {err}"
    );
}

#[tokio::test]
async fn logs_failure_does_not_wait_for_a_stuck_trace_leg() {
    let req = DataRequest {
        logs: true,
        traces: true,
        ..base_req()
    };
    let err = enrich_error(
        req,
        vec![("eth_getLogs", LOGS_STATUS)],
        vec!["debug_traceBlockByNumber"],
    )
    .await;
    assert!(
        err.contains(&LOGS_STATUS.to_string()),
        "the logs error must be the one reported, got: {err}"
    );
}

#[tokio::test]
async fn receipts_failure_does_not_wait_for_a_stuck_trace_leg() {
    let req = DataRequest {
        receipts: true,
        traces: true,
        ..base_req()
    };
    let err = enrich_error(
        req,
        vec![("eth_getBlockReceipts", RECEIPTS_STATUS)],
        vec!["debug_traceBlockByNumber"],
    )
    .await;
    assert!(
        err.contains(&RECEIPTS_STATUS.to_string()),
        "the receipts error must be the one reported, got: {err}"
    );
}

#[tokio::test]
async fn a_failing_trace_sub_leg_does_not_wait_for_a_stuck_one_below_it() {
    // Traces over the debug API + statediffs over replay: the debug-frames
    // sub-leg outranks the replay sub-leg inside `fetch_traces`.
    let req = DataRequest {
        traces: true,
        state_diffs: true,
        ..base_req()
    };
    let err = enrich_error(
        req,
        vec![("debug_traceBlockByNumber", FRAMES_STATUS)],
        vec!["trace_replayBlockTransactions"],
    )
    .await;
    assert!(
        err.contains(&FRAMES_STATUS.to_string()),
        "the debug-frames error must be the one reported, got: {err}"
    );
}

#[tokio::test]
async fn a_higher_priority_leg_still_decides_when_a_lower_one_fails() {
    // Both legs fail: the drop is one-directional, so logs still wins.
    let req = DataRequest {
        logs: true,
        receipts: true,
        ..base_req()
    };
    let err = enrich_error(
        req,
        vec![
            ("eth_getBlockReceipts", RECEIPTS_STATUS),
            ("eth_getLogs", LOGS_STATUS),
        ],
        vec![],
    )
    .await;
    assert!(
        err.contains(&LOGS_STATUS.to_string()),
        "logs outranks receipts regardless of which failed first, got: {err}"
    );
}
