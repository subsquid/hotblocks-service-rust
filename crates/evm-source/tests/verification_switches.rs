//! REQ-14 / INV-36 — every accepted switch is enforced on the acquisition path
//! (GAP-8), and a failed check makes the block incoherent rather than an
//! immediate session error (GAP-32).
//!
//! Each case drives the real fetch layer against a recorded block, once honest
//! and once with a single forged field, and asserts the switch decides.

use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use evm_source::fetch::{Rpc, RpcOptions};
use evm_source::rpc_data::RawRpcBlock;
use evm_source::types::DataRequest;
use rpc_client::{RpcClient, RpcClientConfig};
use serde_json::{json, Value};
use tokio::net::TcpListener;

const CHAIN: &str = "ethereum";
const NUMBER: u64 = 18500000;

fn fixture(file: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/verification")
        .join(CHAIN)
        .join(NUMBER.to_string())
        .join(file);
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
    )
    .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// The recorded block and receipts, with whatever the case forged.
#[derive(Clone)]
struct Upstream {
    block: Value,
    receipts: Value,
}

impl Upstream {
    fn recorded() -> Self {
        Upstream {
            block: fixture("block.json"),
            receipts: fixture("receipts.json"),
        }
    }

    fn forge_block(mut self, f: impl FnOnce(&mut Value)) -> Self {
        f(&mut self.block);
        self
    }

    fn forge_receipts(mut self, f: impl FnOnce(&mut Value)) -> Self {
        f(&mut self.receipts);
        self
    }

    fn logs(&self) -> Value {
        let logs: Vec<Value> = self
            .receipts
            .as_array()
            .expect("receipts array")
            .iter()
            .flat_map(|r| r["logs"].as_array().cloned().unwrap_or_default())
            .collect();
        json!(logs)
    }
}

async fn answer(State(up): State<Arc<Upstream>>, body: axum::body::Bytes) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = req.is_array();
    let calls: Vec<Value> = if batched {
        req.as_array().cloned().unwrap_or_default()
    } else {
        vec![req]
    };

    let mut responses = Vec::with_capacity(calls.len());
    for call in &calls {
        let id = call.get("id").cloned().unwrap_or(json!(1));
        let method = call.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = call.get("params").cloned().unwrap_or(json!([]));
        let p0 = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
        let result = match method {
            "eth_chainId" => json!("0x1"),
            "eth_getBlockByNumber" => up.block.clone(),
            // The probe that picks the receipts method.
            "eth_getBlockReceipts" if p0 == "latest" => json!([]),
            "eth_getBlockReceipts" => up.receipts.clone(),
            "eth_getLogs" => up.logs(),
            _ => Value::Null,
        };
        responses.push(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    let out = match responses.first() {
        Some(single) if !batched => serde_json::to_vec(single),
        _ => serde_json::to_vec(&responses),
    }
    .expect("serialize");

    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        out,
    )
}

/// One acquisition under the given policy.
async fn acquire(up: Upstream, options: RpcOptions, req: DataRequest) -> Vec<RawRpcBlock> {
    let app = Router::new()
        .route("/", post(answer))
        .with_state(Arc::new(up));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: format!("http://127.0.0.1:{}", addr.port()),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));

    Rpc::new(client, options)
        .get_block_batch(&[NUMBER], &req)
        .await
        .expect("acquisition reports incoherence through the block, never as an error (GAP-32)")
}

fn headers_only() -> DataRequest {
    DataRequest::default()
}

fn with_receipts() -> DataRequest {
    DataRequest {
        receipts: true,
        ..DataRequest::default()
    }
}

fn with_logs() -> DataRequest {
    DataRequest {
        logs: true,
        ..DataRequest::default()
    }
}

fn assert_accepted(blocks: &[RawRpcBlock], what: &str) {
    let block = blocks.first().unwrap_or_else(|| panic!("{what}: no block"));
    assert!(
        !block.is_invalid,
        "{what}: honest block rejected — {}",
        block.error_message.as_deref().unwrap_or("no reason given")
    );
}

fn assert_rejected(blocks: &[RawRpcBlock], what: &str) {
    let block = blocks.first().unwrap_or_else(|| panic!("{what}: no block"));
    assert!(
        block.is_invalid,
        "{what}: a forged field passed an enabled check"
    );
}

fn all_switches_on() -> RpcOptions {
    RpcOptions {
        verify_block_hash: true,
        verify_tx_sender: true,
        verify_tx_root: true,
        verify_receipts_root: true,
        verify_withdrawals_root: true,
        verify_logs_bloom: true,
        ..RpcOptions::default()
    }
}

// ─── Every switch enforces, and only when it is on ────────────────────────────

#[tokio::test]
async fn the_recorded_block_passes_every_switch() {
    let blocks = acquire(Upstream::recorded(), all_switches_on(), with_receipts()).await;
    assert_accepted(&blocks, "all switches on");

    let blocks = acquire(Upstream::recorded(), all_switches_on(), with_logs()).await;
    assert_accepted(&blocks, "all switches on, logs path");
}

#[tokio::test]
async fn a_forged_transaction_is_caught_only_by_the_transactions_root_switch() {
    let forged = Upstream::recorded().forge_block(|b| {
        b["transactions"][0]["nonce"] = json!("0xdead");
    });

    let blocks = acquire(forged.clone(), RpcOptions::default(), headers_only()).await;
    assert_accepted(&blocks, "switch off");

    let blocks = acquire(
        forged,
        RpcOptions {
            verify_tx_root: true,
            ..RpcOptions::default()
        },
        headers_only(),
    )
    .await;
    assert_rejected(&blocks, "verify_tx_root");
}

#[tokio::test]
async fn a_forged_sender_is_caught_only_by_the_sender_switch() {
    let forged = Upstream::recorded().forge_block(|b| {
        b["transactions"][0]["from"] = json!("0x0000000000000000000000000000000000001234");
    });

    let blocks = acquire(forged.clone(), RpcOptions::default(), headers_only()).await;
    assert_accepted(&blocks, "switch off");

    let blocks = acquire(
        forged,
        RpcOptions {
            verify_tx_sender: true,
            ..RpcOptions::default()
        },
        headers_only(),
    )
    .await;
    assert_rejected(&blocks, "verify_tx_sender");
}

#[tokio::test]
async fn a_forged_withdrawal_is_caught_only_by_the_withdrawals_root_switch() {
    let forged = Upstream::recorded().forge_block(|b| {
        b["withdrawals"][0]["amount"] = json!("0xdeadbeef");
    });

    let blocks = acquire(forged.clone(), RpcOptions::default(), headers_only()).await;
    assert_accepted(&blocks, "switch off");

    let blocks = acquire(
        forged,
        RpcOptions {
            verify_withdrawals_root: true,
            ..RpcOptions::default()
        },
        headers_only(),
    )
    .await;
    assert_rejected(&blocks, "verify_withdrawals_root");
}

#[tokio::test]
async fn a_forged_header_field_is_caught_only_by_the_block_hash_switch() {
    let forged = Upstream::recorded().forge_block(|b| {
        b["stateRoot"] =
            json!("0x0000000000000000000000000000000000000000000000000000000000000001");
    });

    let blocks = acquire(forged.clone(), RpcOptions::default(), headers_only()).await;
    assert_accepted(&blocks, "switch off");

    let blocks = acquire(
        forged,
        RpcOptions {
            verify_block_hash: true,
            ..RpcOptions::default()
        },
        headers_only(),
    )
    .await;
    assert_rejected(&blocks, "verify_block_hash");
}

#[tokio::test]
async fn a_forged_receipt_is_caught_only_by_the_receipts_root_switch() {
    let forged = Upstream::recorded().forge_receipts(|r| {
        // The trie commits to cumulative gas, not to the per-receipt figure.
        r[0]["cumulativeGasUsed"] = json!("0xdead");
    });

    let blocks = acquire(forged.clone(), RpcOptions::default(), with_receipts()).await;
    assert_accepted(&blocks, "switch off");

    let blocks = acquire(
        forged,
        RpcOptions {
            verify_receipts_root: true,
            ..RpcOptions::default()
        },
        with_receipts(),
    )
    .await;
    assert_rejected(&blocks, "verify_receipts_root");
}

#[tokio::test]
async fn a_forged_log_is_caught_only_by_the_logs_bloom_switch() {
    let forged = Upstream::recorded().forge_receipts(|r| {
        let receipts = r.as_array_mut().expect("receipts array");
        let receipt = receipts
            .iter_mut()
            .find(|r| !r["logs"].as_array().expect("logs").is_empty())
            .expect("a receipt with logs");
        receipt["logs"][0]["address"] = json!("0x0000000000000000000000000000000000009999");
    });

    let blocks = acquire(forged.clone(), RpcOptions::default(), with_logs()).await;
    assert_accepted(&blocks, "switch off");

    let blocks = acquire(
        forged,
        RpcOptions {
            verify_logs_bloom: true,
            ..RpcOptions::default()
        },
        with_logs(),
    )
    .await;
    assert_rejected(&blocks, "verify_logs_bloom");
}
