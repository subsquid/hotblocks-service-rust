//! Regression test for the debug-API trace/stateDiff path.
//!
//! `fetch_debug_frames` / `fetch_debug_state_diffs` collapse any RPC error or a
//! `null` result from `debug_traceBlockByHash` (callTracer / prestateTracer)
//! into an empty `vec![]`. Before the completeness check in `add_traces`, that
//! empty vector was stored — and cached — as if the block legitimately had no
//! traces, so a block with transactions whose trace call returned null/error was
//! silently served with empty traces. This reproduced in production on the Rust
//! hotblocks providers (evm-data-service-rs 0.1.4): ~40% of an eth-sepolia
//! window had empty trace data while the upstream returned it, forcing a revert
//! of all trace/statediff datasets back to the TS image.
//!
//! These tests drive the REAL fetch pipeline against a mock node that returns a
//! block with one transaction but `null` for `debug_traceBlockByHash`. The block
//! must be flagged not-ready (`is_invalid`) so the whole-block retry re-fetches
//! it, instead of being returned with empty traces.

use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use evm_source::fetch::{Rpc, RpcOptions};
use evm_source::types::DataRequest;

/// A real-shaped block with exactly one transaction. `debug_traceBlockByHash`
/// returns `null` for it (simulating a transient error / "cannot query
/// unfinalized data" that the fetch layer normalizes to Null).
fn block_with_one_tx() -> Value {
    json!({
        "number": "0x10",
        "hash": "0xbbbb000000000000000000000000000000000000000000000000000000000010",
        "parentHash": "0xaaaa00000000000000000000000000000000000000000000000000000000000f",
        "difficulty": "0x0",
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x5208",
        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "transactionsRoot": "0x56e81f171bcac1a3c1c0c12c1f2e2e6f0e3a8a1b2c3d4e5f60718293a4b5c6d7e",
        "receiptsRoot": "0x56e81f171bcac1a3c1c0c12c1f2e2e6f0e3a8a1b2c3d4e5f60718293a4b5c6d7e",
        "stateRoot": "0x56e81f171bcac1a3c1c0c12c1f2e2e6f0e3a8a1b2c3d4e5f60718293a4b5c6d7e",
        "miner": "0x0000000000000000000000000000000000000000",
        "size": "0x100",
        "timestamp": "0x66800000",
        "uncles": [],
        "transactions": [
            {
                "blockNumber": "0x10",
                "blockHash": "0xbbbb000000000000000000000000000000000000000000000000000000000010",
                "hash": "0xdead00000000000000000000000000000000000000000000000000000000beef",
                "transactionIndex": "0x0",
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "gas": "0x5208",
                "gasPrice": "0x1",
                "input": "0x",
                "nonce": "0x0",
                "value": "0x0",
                "type": "0x0"
            }
        ]
    })
}

async fn handler(State(block): State<Arc<Value>>, body: axum::body::Bytes) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let arr = req.is_array();
    let reqs: Vec<Value> = if arr {
        req.as_array().unwrap().clone()
    } else {
        vec![req]
    };
    let mut resps = Vec::new();
    for r in &reqs {
        let id = r.get("id").cloned().unwrap_or(json!(1));
        let method = r.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let p0 = r
            .get("params")
            .and_then(|p| p.get(0))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let result = match method {
            "eth_chainId" => json!("0x1"),
            "eth_getBlockByNumber" => {
                if p0 == "latest" || p0 == "finalized" {
                    Value::Null
                } else {
                    (*block).clone()
                }
            }
            // The bug trigger: the node cannot serve traces for this block yet.
            "debug_traceBlockByHash" | "debug_traceBlockByNumber" => Value::Null,
            _ => Value::Null,
        };
        resps.push(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }
    let out = if arr {
        serde_json::to_vec(&resps).unwrap()
    } else {
        serde_json::to_vec(&resps[0]).unwrap()
    };
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        out,
    )
}

async fn fetch_block(req: DataRequest) -> Vec<evm_source::rpc_data::RawRpcBlock> {
    let app = Router::new()
        .route("/", post(handler))
        .with_state(Arc::new(block_with_one_tx()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("http://127.0.0.1:{}", addr.port());
    let client = {
        use rpc_client::{RpcClient, RpcClientConfig};
        Arc::new(RpcClient::new(RpcClientConfig {
            url,
            capacity: 5,
            retry_attempts: 0,
            ..Default::default()
        }))
    };
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));
    rpc.get_block_batch(&[0x10], &req)
        .await
        .expect("get_block_batch")
}

fn debug_traces_req() -> DataRequest {
    DataRequest {
        logs: false,
        receipts: false,
        traces: true,
        state_diffs: false,
        use_trace_api: false,
        use_debug_api_for_state_diffs: false,
        use_debug_trace_block_by_number: false,
        debug_trace_timeout: None,
    }
}

#[tokio::test]
async fn empty_debug_traces_for_block_with_txs_marks_block_not_ready() {
    let blocks = fetch_block(debug_traces_req()).await;
    assert_eq!(blocks.len(), 1, "block fetched from mock node");
    let block = &blocks[0];
    assert_eq!(
        block.block.transactions.len(),
        1,
        "block has one transaction"
    );
    // The trace call returned null -> empty traces. A block WITH transactions and
    // no traces must be flagged not-ready so the whole-block retry re-fetches it,
    // instead of being silently served with empty traces.
    assert!(
        block.is_invalid,
        "block with transactions but empty debug traces must be marked invalid (was silently served with empty traces)"
    );
}
