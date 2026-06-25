//! Whole-pipeline correctness tests driven entirely by REAL captured RPC
//! responses (no synthetic block/trace JSON). A mock server replays a cassette
//! of actual responses, and the test runs the real fetch → deserialize → enrich
//! → normalize path and asserts on the output — so a regression at ANY stage is
//! caught (serde renames, tracer selection, normalization).
//!
//! Fixtures (captured from the gnosis chain-100 uniblock endpoint):
//!   - fixtures/gnosis-pipeline.json: full cassette for block 46873189
//!     (eth_chainId / eth_getBlockByNumber / eth_getBlockReceipts /
//!     trace_replayBlockTransactions[trace,stateDiff]). gnosis runs
//!     use_trace_api + traces + diffs, so this one block exercises both
//!     trace-API bugs at once.
//!   - fixtures/gnosis-block-no-total-difficulty.json: a real block response
//!     that omits totalDifficulty (the aggregator returns it inconsistently).
//!
//! These pinned two production bugs (now fixed): empty statediffs (camelCase
//! `stateDiff` not deserialized) and empty traces under use_trace_api
//! (need_replay_trace dropped the `trace` tracer when statediffs were also on).

use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use evm_source::fetch::{Rpc, RpcOptions};
use evm_source::normalization::{map_block_header, map_rpc_block, MappingOptions, NormalizedBlock};
use evm_source::rpc_data::{RpcBlock, TraceTransactionReplay};
use evm_source::types::DataRequest;

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {name}: {e}")))
        .unwrap_or_else(|e| panic!("parse {name}: {e}"))
}

/// Replay the cassette: answer each JSON-RPC call from the captured responses.
async fn cassette_handler(State(c): State<Arc<Value>>, body: axum::body::Bytes) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let arr = req.is_array();
    let reqs: Vec<Value> = if arr { req.as_array().unwrap().clone() } else { vec![req] };
    let mut resps = Vec::new();
    for r in &reqs {
        let id = r.get("id").cloned().unwrap_or(json!(1));
        let method = r.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = r.get("params").cloned().unwrap_or(json!([]));
        let p0 = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
        let result = match method {
            "eth_chainId" => c["chain_id"].clone(),
            "eth_getBlockByNumber" => {
                if p0 == "latest" || p0 == "finalized" { Value::Null } else { c["getBlockByNumber"].clone() }
            }
            "eth_getBlockReceipts" => {
                if p0 == "latest" { json!([]) } else { c["getBlockReceipts"].clone() }
            }
            "trace_replayBlockTransactions" => {
                // Honor the requested tracers like a real node: only return the
                // `trace`/`stateDiff` keys that were asked for. This makes the
                // traces test actually exercise the fetch tracer-selection fix.
                let want: Vec<&str> = params.get(1).and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|x| x.as_str()).collect()).unwrap_or_default();
                let mut reps = c["traceReplay"].clone();
                for rep in reps.as_array_mut().into_iter().flatten() {
                    if let Some(o) = rep.as_object_mut() {
                        if !want.contains(&"trace") { o.remove("trace"); }
                        if !want.contains(&"stateDiff") { o.remove("stateDiff"); }
                    }
                }
                reps
            }
            _ => Value::Null,
        };
        resps.push(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }
    let out = if arr { serde_json::to_vec(&resps).unwrap() } else { serde_json::to_vec(&resps[0]).unwrap() };
    (axum::http::StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "application/json")], out)
}

/// Run the real pipeline against a cassette and normalize the block.
async fn run_cassette(cassette: Value, req: DataRequest) -> NormalizedBlock {
    let block_number = cassette["block_number"].as_u64().expect("block_number");
    let app = Router::new().route("/", post(cassette_handler)).with_state(Arc::new(cassette));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    let url = format!("http://127.0.0.1:{}", addr.port());
    let client = {
        use rpc_client::{RpcClient, RpcClientConfig};
        Arc::new(RpcClient::new(RpcClientConfig { url, capacity: 5, retry_attempts: 0, ..Default::default() }))
    };
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));
    let blocks = rpc.get_block_batch(&[block_number], &req).await.expect("get_block_batch");
    assert_eq!(blocks.len(), 1, "block fetched from cassette");
    map_rpc_block(&blocks[0], &MappingOptions { with_traces: req.traces, with_state_diffs: req.state_diffs })
}

/// gnosis-mainnet uniblock data request: receipts + traces + statediffs via the trace API.
fn gnosis_req() -> DataRequest {
    DataRequest {
        logs: false, receipts: true, traces: true, state_diffs: true,
        use_trace_api: true, use_debug_api_for_state_diffs: false,
        use_debug_trace_block_by_number: false, debug_trace_timeout: None,
    }
}

#[tokio::test]
async fn pipeline_gnosis_statediffs_present() {
    let nb = run_cassette(fixture("gnosis-pipeline.json"), gnosis_req()).await;
    let sd = nb.state_diffs.expect("state_diffs requested");
    assert!(!sd.is_empty(), "statediffs lost on the trace-API path (real gnosis block)");
}

#[tokio::test]
async fn pipeline_gnosis_traces_present() {
    let nb = run_cassette(fixture("gnosis-pipeline.json"), gnosis_req()).await;
    let tr = nb.traces.expect("traces requested");
    assert!(!tr.is_empty(), "traces lost under use_trace_api + statediffs (real gnosis block)");
}

#[tokio::test]
async fn pipeline_gnosis_keeps_total_difficulty_when_present() {
    // This captured block includes totalDifficulty -> the normalized header must keep it.
    let nb = run_cassette(fixture("gnosis-pipeline.json"), gnosis_req()).await;
    assert!(nb.header.total_difficulty.is_some(), "totalDifficulty dropped despite RPC providing it");
}

#[test]
fn real_gnosis_replay_deserializes_statediff_and_hash() {
    // Pin the serde stage on the real response: camelCase stateDiff / transactionHash must populate.
    let replays: Vec<TraceTransactionReplay> =
        serde_json::from_value(fixture("gnosis-pipeline.json")["traceReplay"].clone()).unwrap();
    assert_eq!(replays.len(), 2, "two transactions in the captured block");
    assert!(replays[0].state_diff.is_some(), "real `stateDiff` (camelCase) not deserialized");
    assert!(replays[0].transaction_hash.is_some(), "real `transactionHash` (camelCase) not deserialized");
    assert!(replays[0].trace.as_ref().map(|t| !t.is_empty()).unwrap_or(false), "trace frames present");
}

#[test]
fn header_omits_total_difficulty_when_rpc_omits_it() {
    // Real gnosis block response that omits totalDifficulty: rust must omit it too
    // (None -> key absent), matching TS. Confirms the gnosis/binance prod diff was
    // a provider-side omission, not a normalization bug.
    let block: RpcBlock = serde_json::from_value(
        fixture("gnosis-block-no-total-difficulty.json")["getBlockByNumber"].clone(),
    )
    .expect("block without totalDifficulty must deserialize");
    let header = map_block_header(&block);
    assert!(header.total_difficulty.is_none(), "totalDifficulty must be None when the RPC omits it");
    let out = serde_json::to_string(&header).unwrap();
    assert!(!out.contains("totalDifficulty"), "output must omit totalDifficulty when absent");
}
