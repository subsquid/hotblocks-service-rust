//! REQ-14 / INV-36 — every accepted switch is enforced on the acquisition path
//! (GAP-8), and a failed check makes the block incoherent rather than an
//! immediate session error (GAP-32).
//!
//! Each case drives the real fetch layer against a recorded block, once honest
//! and once with a single forged field, and asserts the switch decides.

use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use evm_source::fetch::{CallFrameValidationMode, Rpc, RpcOptions};
use evm_source::rpc_data::{RawRpcBlock, RpcReceipt};
use evm_source::types::DataRequest;
use evm_source::verification::receipts_root;
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
    chain_id: Value,
    block: Value,
    receipts: Value,
    debug_frames: Value,
}

impl Upstream {
    fn recorded() -> Self {
        let block = fixture("block.json");
        Upstream {
            chain_id: json!("0x1"),
            debug_frames: debug_frames_for(&block),
            block,
            receipts: fixture("receipts.json"),
        }
    }

    fn on_chain(mut self, chain_id: &str) -> Self {
        self.chain_id = json!(chain_id);
        self
    }

    fn forge_block(mut self, f: impl FnOnce(&mut Value)) -> Self {
        f(&mut self.block);
        self
    }

    fn forge_receipts(mut self, f: impl FnOnce(&mut Value)) -> Self {
        f(&mut self.receipts);
        self
    }

    fn forge_debug_frames(mut self, f: impl FnOnce(&mut Value)) -> Self {
        f(&mut self.debug_frames);
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

fn debug_frames_for(block: &Value) -> Value {
    let frames: Vec<Value> = block["transactions"]
        .as_array()
        .expect("transactions array")
        .iter()
        .map(|tx| {
            let to = tx.get("to").cloned().unwrap_or(Value::Null);
            let frame_type = if to.is_null() { "CREATE" } else { "CALL" };
            json!({
                "txHash": tx["hash"].clone(),
                "result": {
                    "type": frame_type,
                    "from": tx["from"].clone(),
                    "to": to,
                    "input": tx.get("input").cloned().unwrap_or_else(|| json!("0x")),
                    "output": "0x",
                    "value": tx.get("value").cloned().unwrap_or_else(|| json!("0x0")),
                    "gas": tx["gas"].clone(),
                    "gasUsed": "0x0"
                }
            })
        })
        .collect();
    json!(frames)
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
            "eth_chainId" => up.chain_id.clone(),
            "eth_getBlockByNumber" => up.block.clone(),
            // The probe that picks the receipts method.
            "eth_getBlockReceipts" if p0 == "latest" => json!([]),
            "eth_getBlockReceipts" => up.receipts.clone(),
            "eth_getLogs" => up.logs(),
            "debug_traceBlockByHash" | "debug_traceBlockByNumber" => up.debug_frames.clone(),
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
async fn try_acquire(
    up: Upstream,
    options: RpcOptions,
    req: DataRequest,
) -> anyhow::Result<Vec<RawRpcBlock>> {
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
}

async fn acquire(up: Upstream, options: RpcOptions, req: DataRequest) -> Vec<RawRpcBlock> {
    try_acquire(up, options, req)
        .await
        .expect("acquisition reports block incoherence through the block (GAP-32)")
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

fn with_debug_traces() -> DataRequest {
    DataRequest {
        traces: true,
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

fn call_frame_options(call_frame_validation: CallFrameValidationMode) -> RpcOptions {
    RpcOptions {
        verify_tx_sender: true,
        verify_tx_root: true,
        call_frame_validation,
        ..RpcOptions::default()
    }
}

// ─── Every switch enforces, and only when it is on ────────────────────────────

#[tokio::test]
async fn the_recorded_block_passes_every_switch() {
    let blocks = acquire(Upstream::recorded(), all_switches_on(), with_receipts()).await;
    assert_accepted(&blocks, "all switches on");

    let blocks = acquire(
        Upstream::recorded(),
        RpcOptions {
            verify_receipts_root: false,
            ..all_switches_on()
        },
        with_logs(),
    )
    .await;
    assert_accepted(&blocks, "all applicable switches on, logs path");
}

#[tokio::test]
async fn optional_log_and_pre_status_receipt_fields_are_tolerated() {
    let without_optionals = Upstream::recorded().forge_receipts(|receipts| {
        for receipt in receipts.as_array_mut().expect("receipts array") {
            receipt
                .as_object_mut()
                .expect("receipt object")
                .remove("status");
            receipt["root"] =
                json!("0x1111111111111111111111111111111111111111111111111111111111111111");
            receipt
                .as_object_mut()
                .expect("receipt object")
                .remove("type");
            for log in receipt["logs"].as_array_mut().expect("logs array") {
                log.as_object_mut().expect("log object").remove("removed");
            }
        }
    });

    let blocks = acquire(without_optionals, RpcOptions::default(), with_receipts()).await;

    assert_accepted(&blocks, "baseline optional receipt fields");
    let receipts = blocks[0].receipts.as_deref().expect("receipts attached");
    assert!(receipts
        .iter()
        .all(|receipt| receipt.status.is_none() && receipt.root.is_some()));
    assert!(receipts
        .iter()
        .flat_map(|receipt| &receipt.logs)
        .all(|log| log.removed.is_none()));
}

#[test]
fn receipt_status_takes_precedence_and_legacy_root_is_the_fallback() {
    let mut value = fixture("receipts.json")[0].clone();
    value
        .as_object_mut()
        .expect("receipt object")
        .remove("status");
    value["root"] = json!("0x1111111111111111111111111111111111111111111111111111111111111111");
    value["type"] = json!("0x0");
    let with_root: RpcReceipt = serde_json::from_value(value.clone()).expect("pre-status receipt");

    let mut without_type_value = value;
    without_type_value
        .as_object_mut()
        .expect("receipt object")
        .remove("type");
    let without_type: RpcReceipt =
        serde_json::from_value(without_type_value).expect("pre-EIP-2718 receipt without type");

    let root_commitment = receipts_root(&[&with_root], false).expect("root-backed receipt");
    assert_eq!(
        receipts_root(&[&without_type], false).expect("missing type defaults to legacy"),
        root_commitment
    );

    let mut non_minimal_root_type = with_root.clone();
    non_minimal_root_type.receipt_type = "0x00".to_string();
    assert_eq!(
        receipts_root(&[&non_minimal_root_type], false).expect("non-minimal legacy type"),
        root_commitment
    );

    let mut with_status = with_root.clone();
    with_status.root = None;
    with_status.status = Some("0x1".to_string());
    let status_commitment = receipts_root(&[&with_status], false).expect("status-backed receipt");
    assert_ne!(root_commitment, status_commitment);

    let mut non_minimal_status_type = with_status.clone();
    non_minimal_status_type.receipt_type = "0x00".to_string();
    assert_eq!(
        receipts_root(&[&non_minimal_status_type], false)
            .expect("non-minimal zero remains an untyped receipt"),
        status_commitment
    );

    let mut with_both = with_status.clone();
    with_both.root = Some("0x".to_string());
    assert_eq!(
        receipts_root(&[&with_both], false).expect("status takes precedence"),
        status_commitment
    );

    let mut typed_with_root = with_root.clone();
    typed_with_root.receipt_type = "0x1".to_string();
    assert!(receipts_root(&[&typed_with_root], false)
        .expect_err("typed receipts cannot fall back to a state root")
        .to_string()
        .contains("typed receipt"));

    with_status.status = None;
    assert!(receipts_root(&[&with_status], false)
        .expect_err("a receipt needs either root or status")
        .to_string()
        .contains("receipt.status is missing"));
}

#[tokio::test]
async fn structurally_unmappable_debug_frames_are_always_rejected() {
    let malformed = Upstream::recorded().forge_debug_frames(|frames| {
        let result = frames[0]["result"]
            .as_object_mut()
            .expect("debug frame result");
        result.insert(
            "calls".to_string(),
            json!([{
                "type": "SELFDESTRUCT",
                "from": result["to"].clone(),
                "gas": "0x0"
            }]),
        );
    });

    let blocks = acquire(malformed, RpcOptions::default(), with_debug_traces()).await;

    assert_rejected(&blocks, "structural call-frame validation");
    assert!(blocks[0]
        .error_message
        .as_deref()
        .is_some_and(|reason| reason.contains("selfdestruct frame 0 has no beneficiary")));
}

#[tokio::test]
async fn semantic_call_frame_validation_observes_or_rejects_by_mode() {
    let inconsistent = Upstream::recorded().forge_debug_frames(|frames| {
        frames[0]["result"]["from"] = json!("0x0000000000000000000000000000000000000000");
    });

    let blocks = acquire(
        inconsistent.clone(),
        call_frame_options(CallFrameValidationMode::Off),
        with_debug_traces(),
    )
    .await;
    assert_accepted(&blocks, "semantic validation off");

    let blocks = acquire(
        inconsistent.clone(),
        call_frame_options(CallFrameValidationMode::Observe),
        with_debug_traces(),
    )
    .await;
    assert_accepted(&blocks, "semantic validation observe");

    let blocks = acquire(
        inconsistent,
        call_frame_options(CallFrameValidationMode::Reject),
        with_debug_traces(),
    )
    .await;
    assert_rejected(&blocks, "semantic validation reject");
    assert!(blocks[0]
        .error_message
        .as_deref()
        .is_some_and(|reason| reason.contains("root frame is executed by")));
}

#[tokio::test]
async fn rejecting_semantic_call_frames_requires_verification_switches() {
    let error = try_acquire(
        Upstream::recorded(),
        RpcOptions {
            call_frame_validation: CallFrameValidationMode::Reject,
            ..RpcOptions::default()
        },
        with_debug_traces(),
    )
    .await
    .expect_err("reject mode without verification switches must be invalid configuration");

    assert!(error
        .to_string()
        .contains("call-frame validation reject requires verify_tx_root and verify_tx_sender"));
}

#[tokio::test]
async fn receipts_root_verification_rejects_the_logs_only_path() {
    let forged = Upstream::recorded().forge_receipts(|r| {
        r[0]["cumulativeGasUsed"] = json!("0xdead");
    });

    let error = try_acquire(
        forged,
        RpcOptions {
            verify_receipts_root: true,
            ..RpcOptions::default()
        },
        with_logs(),
    )
    .await
    .expect_err("a receipt-root check without receipts is an invalid request");
    assert!(error
        .to_string()
        .contains("verify_receipts_root requires receipt acquisition"));
}

#[tokio::test]
async fn receipt_root_encoding_override_requires_root_verification() {
    let error = try_acquire(
        Upstream::recorded(),
        RpcOptions {
            use_gas_used_for_receipts_root: true,
            ..RpcOptions::default()
        },
        with_receipts(),
    )
    .await
    .expect_err("a receipt encoding override without its root check is invalid");
    assert!(error
        .to_string()
        .contains("use_gas_used_for_receipts_root requires verify_receipts_root"));
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
async fn withdrawals_root_verification_rejects_asymmetric_presence() {
    for (what, forged) in [
        (
            "missing withdrawals",
            Upstream::recorded().forge_block(|b| b["withdrawals"] = Value::Null),
        ),
        (
            "missing withdrawals root",
            Upstream::recorded().forge_block(|b| b["withdrawalsRoot"] = Value::Null),
        ),
    ] {
        let blocks = acquire(
            forged,
            RpcOptions {
                verify_withdrawals_root: true,
                ..RpcOptions::default()
            },
            headers_only(),
        )
        .await;
        assert_rejected(&blocks, what);
    }
}

#[tokio::test]
async fn verification_accepts_uppercase_hex_commitments() {
    let upper = Upstream::recorded().forge_block(|block| {
        for field in [
            "transactionsRoot",
            "receiptsRoot",
            "withdrawalsRoot",
            "logsBloom",
        ] {
            let value = block[field].as_str().expect("hex field");
            block[field] = json!(format!("0x{}", value[2..].to_ascii_uppercase()));
        }
    });

    let blocks = acquire(upper, all_switches_on(), with_receipts()).await;
    assert_accepted(&blocks, "uppercase hex commitments");

    let upper_hash = Upstream::recorded().forge_block(|block| {
        let value = block["hash"].as_str().expect("block hash");
        block["hash"] = json!(format!("0x{}", value[2..].to_ascii_uppercase()));
    });
    let blocks = acquire(
        upper_hash,
        RpcOptions {
            verify_block_hash: true,
            ..RpcOptions::default()
        },
        headers_only(),
    )
    .await;
    assert_accepted(&blocks, "uppercase block hash");
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
async fn hyperliquid_system_receipt_is_excluded_from_cumulative_gas_check() {
    let system_receipt = Upstream::recorded()
        .forge_block(|block| {
            let tx = block["transactions"]
                .as_array_mut()
                .expect("transactions array")
                .last_mut()
                .expect("a transaction");
            tx["gasPrice"] = json!("0x0");
        })
        .forge_receipts(|receipts| {
            let receipt = receipts
                .as_array_mut()
                .expect("receipts array")
                .last_mut()
                .expect("a receipt");
            receipt["cumulativeGasUsed"] = json!("0x0");
        });

    let blocks = acquire(
        system_receipt.clone(),
        RpcOptions {
            check_cumulative_gas_used: true,
            ..RpcOptions::default()
        },
        with_receipts(),
    )
    .await;
    assert_rejected(&blocks, "baseline cumulative-gas policy");

    let blocks = acquire(
        system_receipt.on_chain("0x3e7"),
        RpcOptions {
            check_cumulative_gas_used: true,
            ..RpcOptions::default()
        },
        with_receipts(),
    )
    .await;
    assert_accepted(&blocks, "Hyperliquid cumulative-gas exemption");
}

#[tokio::test]
async fn cumulative_gas_overflow_marks_the_block_incoherent() {
    let forged = Upstream::recorded().forge_receipts(|receipts| {
        let receipts = receipts.as_array_mut().expect("receipts array");
        assert!(receipts.len() >= 2, "fixture needs two receipts");
        receipts[0]["gasUsed"] = json!("0xffffffffffffffffffffffffffffffff");
        receipts[0]["cumulativeGasUsed"] = json!("0xffffffffffffffffffffffffffffffff");
        receipts[1]["gasUsed"] = json!("0x1");
    });

    let blocks = acquire(
        forged,
        RpcOptions {
            check_cumulative_gas_used: true,
            ..RpcOptions::default()
        },
        with_receipts(),
    )
    .await;
    assert_rejected(&blocks, "cumulative-gas overflow");
    assert!(blocks[0]
        .error_message
        .as_deref()
        .is_some_and(|reason| reason.contains("cumulative gas used overflow")));
}

#[tokio::test]
async fn malformed_cumulative_gas_quantities_mark_the_block_incoherent() {
    for value in ["0xzz", "0x100000000000000000000000000000000"] {
        let forged = Upstream::recorded().forge_receipts(|receipts| {
            for receipt in receipts.as_array_mut().expect("receipts array") {
                receipt["gasUsed"] = json!(value);
                receipt["cumulativeGasUsed"] = json!(value);
            }
        });

        let blocks = acquire(
            forged,
            RpcOptions {
                check_cumulative_gas_used: true,
                ..RpcOptions::default()
            },
            with_receipts(),
        )
        .await;
        assert_rejected(&blocks, "malformed cumulative-gas quantity");
        assert!(
            blocks[0]
                .error_message
                .as_deref()
                .is_some_and(|reason| reason.contains("receipt.cumulativeGasUsed")),
            "unexpected error for {value}: {:?}",
            blocks[0].error_message
        );
    }
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
