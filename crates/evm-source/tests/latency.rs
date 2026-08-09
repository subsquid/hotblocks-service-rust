//! Latency-optimization tests:
//! 1. Cadence predictor unit tests.
//! 2. Enrichment retry: body present, receipts initially mismatched then correct.
//! 3. Pipeline overlap: stuck block N does not prevent body polling of N+1.
//! 4. Finalizer stall fix: slow probe does not block fresh batch passthrough.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use evm_source::ingest::CadencePredictor;
use rpc_client::{RpcClient, RpcClientConfig};

// ─── Cadence predictor tests ──────────────────────────────────────────────────

#[test]
fn cadence_no_data_returns_100ms() {
    let pred = CadencePredictor::new();
    let delay = pred.next_poll_delay(Instant::now());
    assert_eq!(delay, Duration::from_millis(100));
}

#[test]
fn cadence_caps_a_long_quiet_sleep() {
    let mut pred = CadencePredictor::new();
    let t0 = Instant::now();

    // Simulate 3 blocks arriving ~12s apart (like Ethereum mainnet)
    pred.record_block(t0);
    let t1 = t0 + Duration::from_secs(12);
    pred.record_block(t1);
    let t2 = t1 + Duration::from_secs(12);
    pred.record_block(t2);

    // 1s in, the next arrival is still ~11s out. The sleep is capped so a wrong
    // prediction cannot park the poller for a whole interval.
    let now = t2 + Duration::from_secs(1);
    let delay = pred.next_poll_delay(now);
    assert_eq!(delay, Duration::from_millis(1000));
}

#[test]
fn cadence_clamps_minimum_to_hot_poll() {
    let mut pred = CadencePredictor::new();
    let t0 = Instant::now();

    // Simulate very fast blocks (100ms)
    pred.record_block(t0);
    let t1 = t0 + Duration::from_millis(100);
    pred.record_block(t1);

    // Query: already 200ms past t1 → inside the hot window → tight 25ms poll
    let now = t1 + Duration::from_millis(200);
    let delay = pred.next_poll_delay(now);
    assert_eq!(delay, Duration::from_millis(25));
}

/// The regression this estimator exists to prevent: on a chain that skips ~2.5%
/// of its slots, an averaging predictor absorbs the doubled interval and then
/// over-predicts — staying quiet through the arrival — for several blocks after.
#[test]
fn cadence_floor_ignores_a_skipped_slot() {
    let mut pred = CadencePredictor::new();
    let mut t = Instant::now();
    pred.record_block(t);
    for _ in 0..3 {
        t += Duration::from_millis(5000);
        pred.record_block(t);
    }
    t += Duration::from_millis(10_000); // one slot skipped
    pred.record_block(t);

    // 4.5s on, the next block is inside the hot window and we must be polling
    // for it. An EMA at α=0.3 would predict 6.5s here and sleep instead.
    let delay = pred.next_poll_delay(t + Duration::from_millis(4500));
    assert_eq!(delay, Duration::from_millis(25));
}

/// Under-prediction costs polls, over-prediction costs latency — so the floor
/// follows the chain downward with no smoothing at all.
#[test]
fn cadence_floor_follows_the_chain_down() {
    let mut pred = CadencePredictor::new();
    let mut t = Instant::now();
    pred.record_block(t);
    t += Duration::from_millis(5000);
    pred.record_block(t);
    t += Duration::from_millis(1000);
    pred.record_block(t);

    let delay = pred.next_poll_delay(t + Duration::from_millis(500));
    assert_eq!(delay, Duration::from_millis(25));
}

#[test]
fn cadence_floor_ages_out_a_stale_minimum() {
    let mut pred = CadencePredictor::new();
    let mut t = Instant::now();
    pred.record_block(t);
    t += Duration::from_millis(1000); // one anomalously short interval
    pred.record_block(t);
    for _ in 0..8 {
        t += Duration::from_millis(5000);
        pred.record_block(t);
    }

    // The outlier has left the window, so we go quiet right after a block
    // instead of hot-polling the whole interval forever.
    let delay = pred.next_poll_delay(t + Duration::from_millis(100));
    assert!(
        delay > Duration::from_millis(25),
        "expected a quiet sleep, got {delay:?}"
    );
}

// ─── Mock JSON-RPC server helpers ─────────────────────────────────────────────

fn make_rpc_block(number: u64, hash: &str, parent_hash: &str) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": hash,
        "parentHash": parent_hash,
        "difficulty": "0x0",
        "totalDifficulty": "0x0",
        "excessBlobGas": null,
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "miner": "0x0000000000000000000000000000000000000000",
        "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x1",
        "size": "0x220",
        "timestamp": format!("0x{:x}", 1700000000u64 + number * 12),
        "transactions": [],
        "uncles": [],
        "withdrawals": []
    })
}

fn make_rpc_receipt(block_hash: &str, block_number: u64, tx_hash: &str) -> Value {
    json!({
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionHash": tx_hash,
        "transactionIndex": "0x0",
        "contractAddress": null,
        "cumulativeGasUsed": "0x5208",
        "from": "0x0000000000000000000000000000000000000001",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "logs": [],
        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "status": "0x1",
        "to": "0x0000000000000000000000000000000000000002",
        "type": "0x2"
    })
}

/// Block 100 with a single transaction. Shared by the mismatch test's seed body
/// and its mock's `eth_getBlockByNumber` response, so a whole-block re-fetch
/// returns a block consistent with the served receipts.
fn make_block_100_with_tx(hash: &str) -> Value {
    let mut b = make_rpc_block(
        100,
        hash,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
    );
    b["transactions"] = json!([{
        "hash": "0xdeadbeef",
        "nonce": "0x0",
        "blockHash": hash,
        "blockNumber": "0x64",
        "transactionIndex": "0x0",
        "from": "0x0000000000000000000000000000000000000001",
        "to": "0x0000000000000000000000000000000000000002",
        "value": "0x0",
        "gas": "0x5208",
        "gasPrice": "0x1",
        "input": "0x",
        "type": "0x2",
        "chainId": "0x1",
        "maxFeePerGas": "0x1",
        "maxPriorityFeePerGas": "0x1",
        "accessList": [],
        "v": "0x0",
        "r": "0x0",
        "s": "0x0"
    }]);
    b
}

fn make_client(url: &str) -> Arc<rpc_client::RpcClient> {
    use rpc_client::{RpcClient, RpcClientConfig};
    Arc::new(RpcClient::new(RpcClientConfig {
        url: url.to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }))
}

// ─── Enrichment retry test ────────────────────────────────────────────────────

/// A method-routing mock where eth_getBlockReceipts returns:
/// - An array for "latest" (probe) → ByBlock mode selected
/// - For the actual block: wrong hash on first call, correct on second
#[derive(Clone)]
struct EnrichRetryState {
    block_hash: &'static str,
    wrong_hash: &'static str,
    receipt_call_count: Arc<AtomicUsize>,
}

async fn enrich_retry_handler(
    State(s): State<EnrichRetryState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let requests: Vec<Value> = if req.is_array() {
        req.as_array().unwrap().clone()
    } else {
        vec![req.clone()]
    };

    let mut responses = Vec::new();
    for r in &requests {
        let id = r.get("id").cloned().unwrap_or(json!(1));
        let method = r
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let params = r.get("params").cloned().unwrap_or(json!([]));

        let result = match method.as_str() {
            "eth_chainId" => json!("0x1"),
            "eth_getBlockByNumber" => {
                let tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
                if tag == "latest" || tag == "finalized" {
                    Value::Null
                } else {
                    // A real node always serves the header; the whole-block retry
                    // re-fetches it each attempt.
                    make_block_100_with_tx(s.block_hash)
                }
            }
            "eth_getBlockReceipts" => {
                let tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
                if tag == "latest" {
                    // Probe: return empty array to indicate ByBlock
                    json!([])
                } else {
                    // Actual call: first time return wrong hash, second time correct
                    let call_num = s.receipt_call_count.fetch_add(1, Ordering::SeqCst);
                    if call_num == 0 {
                        json!([make_rpc_receipt(s.wrong_hash, 100, "0xdeadbeef")])
                    } else {
                        json!([make_rpc_receipt(s.block_hash, 100, "0xdeadbeef")])
                    }
                }
            }
            _ => Value::Null,
        };
        responses.push(json!({"jsonrpc":"2.0","id":id,"result":result}));
    }

    let body = if responses.len() == 1 && !req.is_array() {
        serde_json::to_vec(&responses[0]).unwrap()
    } else {
        serde_json::to_vec(&responses).unwrap()
    };
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

/// Test that enrich_block_with_retry retries when receipts have the wrong blockHash
/// (load-balanced inconsistency) and succeeds on the next attempt.
#[tokio::test]
async fn test_enrich_retry_on_receipt_hash_mismatch() {
    use evm_source::fetch::{Rpc, RpcOptions};
    use evm_source::types::DataRequest;

    let block_hash: &'static str =
        "0xaaaa000000000000000000000000000000000000000000000000000000000001";
    let wrong_hash: &'static str =
        "0xbbbb000000000000000000000000000000000000000000000000000000000002";

    let receipt_call_count = Arc::new(AtomicUsize::new(0));
    let state = EnrichRetryState {
        block_hash,
        wrong_hash,
        receipt_call_count: receipt_call_count.clone(),
    };

    let app = Router::new()
        .route("/", post(enrich_retry_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("http://127.0.0.1:{}", addr.port());

    let client = make_client(&url);
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));

    // Build a raw block body for block 100 with 1 transaction (same shape the
    // mock's eth_getBlockByNumber serves on the whole-block retry).
    let rpc_block: evm_source::rpc_data::RpcBlock =
        serde_json::from_value(make_block_100_with_tx(block_hash)).unwrap();
    let body = evm_source::rpc_data::RawRpcBlock::new(100, block_hash.to_string(), rpc_block);

    let req = DataRequest {
        receipts: true,
        ..Default::default()
    };
    let enriched = tokio::time::timeout(
        Duration::from_secs(10),
        rpc.enrich_block_with_retry(body, &req),
    )
    .await
    .expect("enrich timed out")
    .unwrap();

    // Must not be invalid
    assert!(
        !enriched.is_invalid,
        "block should not be invalid after enrichment"
    );
    // Must have receipts
    let receipts = enriched
        .receipts
        .as_ref()
        .expect("receipts should be present");
    assert_eq!(receipts.len(), 1);
    // All receipts must have the correct block hash
    assert_eq!(receipts[0].block_hash, block_hash);

    // The mock should have served receipts at least twice (once bad, once good)
    let total = receipt_call_count.load(Ordering::SeqCst);
    assert!(total >= 2, "expected at least 2 receipt calls, got {total}");
}

/// Test that when receipts are initially null (not ready), we retry and succeed.
#[tokio::test]
async fn test_enrich_retry_on_null_receipts() {
    use evm_source::fetch::{Rpc, RpcOptions};
    use evm_source::types::DataRequest;

    let block_hash: &'static str =
        "0xaaaa000000000000000000000000000000000000000000000000000000000001";

    // Use a counter-based mock: first actual receipt call returns null, second returns []
    let receipt_call_count = Arc::new(AtomicUsize::new(0));
    let receipt_count_clone = receipt_call_count.clone();

    #[derive(Clone)]
    struct NullReceiptsState {
        block_hash: &'static str,
        receipt_call_count: Arc<AtomicUsize>,
    }

    async fn null_receipts_handler(
        State(s): State<NullReceiptsState>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        let requests: Vec<Value> = if req.is_array() {
            req.as_array().unwrap().clone()
        } else {
            vec![req.clone()]
        };

        let mut responses = Vec::new();
        for r in &requests {
            let id = r.get("id").cloned().unwrap_or(json!(1));
            let method = r
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let params = r.get("params").cloned().unwrap_or(json!([]));

            let result = match method.as_str() {
                "eth_chainId" => json!("0x1"),
                "eth_getBlockByNumber" => {
                    let tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
                    if tag == "latest" || tag == "finalized" {
                        Value::Null
                    } else {
                        make_rpc_block(
                            50,
                            s.block_hash,
                            "0x0000000000000000000000000000000000000000000000000000000000000000",
                        )
                    }
                }
                "eth_getBlockReceipts" => {
                    let tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
                    if tag == "latest" {
                        json!([]) // probe → ByBlock
                    } else {
                        let n = s.receipt_call_count.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            Value::Null
                        } else {
                            json!([])
                        }
                    }
                }
                _ => Value::Null,
            };
            responses.push(json!({"jsonrpc":"2.0","id":id,"result":result}));
        }

        let body = if responses.len() == 1 && !req.is_array() {
            serde_json::to_vec(&responses[0]).unwrap()
        } else {
            serde_json::to_vec(&responses).unwrap()
        };
        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
    }

    let ns = NullReceiptsState {
        block_hash,
        receipt_call_count: receipt_count_clone,
    };
    let app = Router::new()
        .route("/", post(null_receipts_handler))
        .with_state(ns);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("http://127.0.0.1:{}", addr.port());

    let client = make_client(&url);
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));

    let raw_block_json = make_rpc_block(
        50,
        block_hash,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
    );
    let rpc_block: evm_source::rpc_data::RpcBlock = serde_json::from_value(raw_block_json).unwrap();
    let body = evm_source::rpc_data::RawRpcBlock::new(50, block_hash.to_string(), rpc_block);

    let req = DataRequest {
        receipts: true,
        ..Default::default()
    };
    let enriched = tokio::time::timeout(
        Duration::from_secs(10),
        rpc.enrich_block_with_retry(body, &req),
    )
    .await
    .expect("enrich timed out")
    .unwrap();

    // Block 50 has no transactions, so empty receipts is correct
    assert!(!enriched.is_invalid);
    let receipts = enriched.receipts.as_ref().expect("receipts present");
    assert_eq!(receipts.len(), 0);

    // Should have retried once (null → empty)
    let total = receipt_call_count.load(Ordering::SeqCst);
    assert!(
        total >= 2,
        "expected at least 2 receipt calls (null then success), got {total}"
    );
}

// ─── Pipeline overlap test ────────────────────────────────────────────────────

/// Test that while block N's enrichment is stuck (receipts always returning wrong hash),
/// body polling for N+1 still proceeds and N+1 is eventually emitted.
///
/// We test this by running ingest_range with a mock where:
/// - Block 100 body is available immediately
/// - Block 100 receipts are initially null → then correct
/// - Block 101 body is available
/// - Block 101 receipts are correct
/// and assert that both blocks come out, in order.
///
/// Note: The current speculative implementation runs body fetch then enrichment
/// sequentially per block. This test verifies that both blocks are emitted correctly
/// in order, using a method-routing mock that doesn't depend on call order.
#[tokio::test]
async fn test_speculative_two_blocks_emitted_in_order() {
    use evm_source::ingest::ingest_range;
    use evm_source::normalization::MappingOptions;
    use evm_source::types::DataRequest;
    use futures::StreamExt;

    let hash100 = "0xaaaa000000000000000000000000000000000000000000000000000000000100";
    let hash101 = "0xbbbb000000000000000000000000000000000000000000000000000000000101";
    let hash99 = "0xcccc000000000000000000000000000000000000000000000000000000000099";

    // Use a method-routing mock: each method returns a deterministic result
    // based on its params, so call order doesn't matter.
    #[derive(Clone)]
    struct RoutingState {
        hash100: &'static str,
        hash101: &'static str,
        hash99: &'static str,
    }

    async fn routing_handler(
        State(s): State<RoutingState>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        let requests: Vec<Value> = if req.is_array() {
            req.as_array().unwrap().clone()
        } else {
            vec![req.clone()]
        };

        let mut responses = Vec::new();
        for r in &requests {
            let id = r.get("id").cloned().unwrap_or(json!(1));
            let method = r
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let params = r.get("params").cloned().unwrap_or(json!([]));

            let result = match method.as_str() {
                "eth_chainId" => json!("0x1"),
                "eth_getBlockReceipts" => {
                    // Probe call with "latest" or receipts call with block number
                    json!([])
                }
                "eth_getBlockByNumber" => {
                    let tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
                    match tag {
                        "latest" => make_rpc_block(101, s.hash101, s.hash100), // head
                        "0x64" => make_rpc_block(100, s.hash100, s.hash99),
                        "0x65" => make_rpc_block(101, s.hash101, s.hash100),
                        _ => Value::Null,
                    }
                }
                _ => Value::Null,
            };
            responses.push(json!({"jsonrpc":"2.0","id":id,"result":result}));
        }

        let body = if responses.len() == 1 && !req.is_array() {
            serde_json::to_vec(&responses[0]).unwrap()
        } else {
            serde_json::to_vec(&responses).unwrap()
        };

        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
    }

    let routing_state = RoutingState {
        hash100,
        hash101,
        hash99,
    };
    let app = Router::new()
        .route("/", post(routing_handler))
        .with_state(routing_state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("http://127.0.0.1:{}", addr.port());

    let client = make_client(&url);

    use evm_source::fetch::{Rpc, RpcOptions};
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));

    let req = Arc::new(DataRequest {
        receipts: true,
        ..Default::default()
    });
    let opts = Arc::new(MappingOptions {
        with_traces: false,
        with_state_diffs: false,
    });

    let mut stream =
        Box::pin(ingest_range(rpc, req, opts, 100, Some(101), 5, 5, "latest", false).await);

    let mut all_block_numbers: Vec<u64> = Vec::new();
    // Collect with timeout to avoid hanging
    tokio::select! {
        _ = async {
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                for b in &batch.blocks {
                    all_block_numbers.push(b.number);
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
    }

    assert_eq!(
        all_block_numbers,
        vec![100, 101],
        "expected blocks 100 and 101 in order, got: {all_block_numbers:?}"
    );
}

// ─── Speculative-head incoherence budget tests ──────────────────────────────

#[derive(Clone)]
struct AlternatingHeadState {
    block_calls: Arc<AtomicUsize>,
    block_hash: &'static str,
    parent_hash: &'static str,
}

async fn alternating_head_handler(
    State(state): State<AlternatingHeadState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = request.is_array();
    let calls = request.as_array().cloned().unwrap_or_else(|| vec![request]);

    let responses: Vec<Value> = calls
        .iter()
        .map(|call| {
            let id = call.get("id").cloned().unwrap_or(json!(1));
            let method = call.get("method").and_then(Value::as_str).unwrap_or("");
            let tag = call
                .get("params")
                .and_then(|params| params.get(0))
                .and_then(Value::as_str)
                .unwrap_or("");
            let result = match method {
                "eth_chainId" => json!("0x1"),
                "eth_getBlockByNumber" if tag == "0x64" => {
                    let call = state.block_calls.fetch_add(1, Ordering::SeqCst);
                    if call % 2 == 0 {
                        make_rpc_block(100, state.block_hash, state.parent_hash)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };
            json!({"jsonrpc": "2.0", "id": id, "result": result})
        })
        .collect();

    let response = if batched {
        serde_json::to_vec(&responses)
    } else {
        serde_json::to_vec(&responses[0])
    }
    .expect("serialize response");
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        response,
    )
}

#[tokio::test]
async fn absent_head_answers_do_not_reset_the_incoherence_budget() {
    use evm_source::fetch::{Rpc, RpcOptions, P_ENRICH_RETRIES};
    use evm_source::ingest::ingest_range;
    use evm_source::normalization::MappingOptions;
    use evm_source::types::DataRequest;
    use futures::StreamExt;

    let state = AlternatingHeadState {
        block_calls: Arc::new(AtomicUsize::new(0)),
        block_hash: "0xaaaa000000000000000000000000000000000000000000000000000000000100",
        parent_hash: "0xbbbb000000000000000000000000000000000000000000000000000000000099",
    };
    let app = Router::new()
        .route("/", post(alternating_head_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let rpc = Arc::new(Rpc::new(
        make_client(&format!("http://127.0.0.1:{}", addr.port())),
        RpcOptions {
            verify_block_hash: true,
            ..RpcOptions::default()
        },
    ));
    let mut stream = Box::pin(
        ingest_range(
            rpc,
            Arc::new(DataRequest::default()),
            Arc::new(MappingOptions {
                with_traces: false,
                with_state_diffs: false,
            }),
            100,
            Some(100),
            5,
            5,
            "latest",
            false,
        )
        .await,
    );

    let item = tokio::time::timeout(Duration::from_secs(4), stream.next())
        .await
        .expect("per-block retry budget must terminate the stream")
        .expect("stream must report the acquisition failure");
    let error = item.expect_err("alternating incoherent/null answers must fail loud");
    assert!(
        error.to_string().contains(&format!(
            "failed to acquire block 100 after {P_ENRICH_RETRIES} retries"
        )),
        "unexpected error: {error:#}"
    );
}

#[derive(Clone)]
struct DrainFailureState {
    hash_100: &'static str,
    hash_101: &'static str,
    hash_99: &'static str,
}

async fn drain_failure_handler(
    State(state): State<DrainFailureState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = request.is_array();
    let calls = request.as_array().cloned().unwrap_or_else(|| vec![request]);

    let responses: Vec<Value> = calls
        .iter()
        .map(|call| {
            let id = call.get("id").cloned().unwrap_or(json!(1));
            let method = call.get("method").and_then(Value::as_str).unwrap_or("");
            let tag = call
                .get("params")
                .and_then(|params| params.get(0))
                .and_then(Value::as_str)
                .unwrap_or("");
            let result = match method {
                "eth_chainId" => json!("0x1"),
                "eth_getBlockByNumber" if tag == "0x64" => {
                    let mut block = make_rpc_block(100, state.hash_100, state.hash_99);
                    block
                        .as_object_mut()
                        .expect("block object")
                        .remove("withdrawals");
                    block
                }
                "eth_getBlockByNumber" if tag == "0x65" => {
                    make_rpc_block(101, state.hash_101, state.hash_100)
                }
                "eth_getBlockReceipts" if tag == "latest" => json!([]),
                "eth_getBlockReceipts" => Value::Null,
                _ => Value::Null,
            };
            json!({"jsonrpc": "2.0", "id": id, "result": result})
        })
        .collect();

    let response = if batched {
        serde_json::to_vec(&responses)
    } else {
        serde_json::to_vec(&responses[0])
    }
    .expect("serialize response");
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        response,
    )
}

#[tokio::test]
async fn head_incoherence_remains_the_primary_error_when_pending_enrichment_fails() {
    use evm_source::fetch::{Rpc, RpcOptions, P_ENRICH_RETRIES};
    use evm_source::ingest::ingest_range;
    use evm_source::normalization::MappingOptions;
    use evm_source::types::DataRequest;
    use futures::StreamExt;

    let state = DrainFailureState {
        hash_100: "0xaaaa000000000000000000000000000000000000000000000000000000000100",
        hash_101: "0xbbbb000000000000000000000000000000000000000000000000000000000101",
        hash_99: "0xcccc000000000000000000000000000000000000000000000000000000000099",
    };
    let app = Router::new()
        .route("/", post(drain_failure_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let rpc = Arc::new(Rpc::new(
        make_client(&format!("http://127.0.0.1:{}", addr.port())),
        RpcOptions {
            verify_withdrawals_root: true,
            // Block 100's receipts stay null (not-ready); shrink its wall-clock
            // budget so the drain sees the failure inside the test timeout.
            not_ready_budget: Some(Duration::from_millis(500)),
            ..RpcOptions::default()
        },
    ));
    let mut stream = Box::pin(
        ingest_range(
            rpc,
            Arc::new(DataRequest {
                receipts: true,
                ..DataRequest::default()
            }),
            Arc::new(MappingOptions {
                with_traces: false,
                with_state_diffs: false,
            }),
            100,
            Some(101),
            5,
            5,
            "latest",
            false,
        )
        .await,
    );

    let item = tokio::time::timeout(Duration::from_secs(4), stream.next())
        .await
        .expect("retry budgets must terminate the stream")
        .expect("stream must report the acquisition failure");
    let error = item.expect_err("the incoherent head must fail loud");
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "failed to acquire block 101 after {P_ENRICH_RETRIES} retries"
        )),
        "head failure was replaced: {error:#}"
    );
    assert!(
        message.contains("while draining earlier blocks: block 100 enrichment failed"),
        "secondary enrichment failure was lost: {error:#}"
    );
}

// ─── Finalizer stall fix test ─────────────────────────────────────────────────

/// Test that a slow probe does not prevent fresh batch passthrough.
/// We set up a mock where:
/// - The finalized probe endpoint takes ~300ms to respond
/// - The ingest stream produces batches quickly
/// - We assert that batches are yielded without waiting for the probe
#[tokio::test]
async fn test_finalizer_no_stall_on_slow_probe() {
    let upstream = slow_finality_upstream().await;
    let first = first_block(
        RpcClientConfig {
            url: upstream.url.clone(),
            capacity: 5,
            retry_attempts: 0,
            ..Default::default()
        },
        &upstream,
    )
    .await;

    // The first batch should arrive quickly — well under 400ms (the probe delay)
    assert!(
        first.elapsed < Duration::from_millis(300),
        "first batch took {:?} — probe stall not fixed (probe takes 400ms)",
        first.elapsed
    );
}

/// ADR-6 against ADR-3's shared budget: not *awaiting* the finality read is not
/// enough, since it still spends the budget the block fetch needs and that budget
/// has no priority (HZ-1). Stated over the calls, so it holds at any budget.
///
/// Latency is pinned only in the single-permit case, where the two are strictly
/// serial. At one call per second the budget alone puts the first block seconds
/// out whatever precedes it — HZ-1's to answer, not this rule's.
#[tokio::test]
async fn a_finality_read_never_precedes_the_first_block() {
    // One upstream per case: a finished run's prober outlives its stream by a
    // round, and its calls would land in the next run's log.
    let permit_upstream = slow_finality_upstream().await;
    let single_permit = first_block(
        RpcClientConfig {
            url: permit_upstream.url.clone(),
            capacity: 1,
            retry_attempts: 0,
            ..Default::default()
        },
        &permit_upstream,
    )
    .await;
    single_permit.assert_no_finality_read("one permit");
    assert!(
        single_permit.elapsed < Duration::from_millis(300),
        "first block took {:?} behind a 400 ms finality read holding the only permit",
        single_permit.elapsed
    );

    let metered_upstream = slow_finality_upstream().await;
    let metered = first_block(
        RpcClientConfig {
            url: metered_upstream.url.clone(),
            capacity: usize::MAX,
            rate_limit: Some(1.0),
            retry_attempts: 0,
            ..Default::default()
        },
        &metered_upstream,
    )
    .await;
    metered.assert_no_finality_read("one call per second");
}

/// Answers block bodies at once and the `finalized` tag after 400 ms, logging
/// every block tag it is asked for in arrival order.
struct SlowFinalityUpstream {
    url: String,
    tags: Arc<std::sync::Mutex<Vec<String>>>,
}

/// What one head stream did up to its first delivered block.
struct FirstBlock {
    elapsed: Duration,
    /// Block tags the upstream was asked for before that block came out.
    tags_before: Vec<String>,
}

impl FirstBlock {
    fn assert_no_finality_read(&self, budget: &str) {
        assert!(
            !self.tags_before.iter().any(|tag| tag == "finalized"),
            "with {budget}, the finality read went out before the first block: {:?}",
            self.tags_before
        );
    }
}

async fn slow_finality_upstream() -> SlowFinalityUpstream {
    #[derive(Clone)]
    struct SlowProbeState {
        _call_count: Arc<AtomicUsize>,
        tags: Arc<std::sync::Mutex<Vec<String>>>,
    }

    async fn slow_probe_handler(
        State(state): State<SlowProbeState>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        let requests: Vec<Value> = if req.is_array() {
            req.as_array().unwrap().clone()
        } else {
            vec![req.clone()]
        };

        let hash_genesis = "0x0000000000000000000000000000000000000000000000000000000000000000";
        let hash1 = "0xaaaa000000000000000000000000000000000000000000000000000000000001";
        let hash2 = "0xbbbb000000000000000000000000000000000000000000000000000000000002";

        let mut responses = Vec::new();
        for r in &requests {
            let id = r.get("id").cloned().unwrap_or(json!(1));
            let method = r
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let params = r.get("params").cloned().unwrap_or(json!([]));

            state._call_count.fetch_add(1, Ordering::SeqCst);

            let result = match method.as_str() {
                "eth_chainId" => json!("0x1"),
                "eth_getBlockByNumber" => {
                    let tag = params.get(0).and_then(|v| v.as_str()).unwrap_or("");
                    // On arrival, before the slow arm sleeps: the rule is about
                    // which calls go out, not which return.
                    state.tags.lock().expect("tag log").push(tag.to_string());
                    if tag == "finalized" {
                        // Simulate slow finalized probe
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        // Return block 1 as finalized
                        make_rpc_block_local(1, hash1, hash_genesis)
                    } else if tag == "latest" || tag == "0x1" {
                        make_rpc_block_local(1, hash1, hash_genesis)
                    } else if tag == "0x2" {
                        make_rpc_block_local(2, hash2, hash1)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };

            responses.push(json!({"jsonrpc":"2.0","id":id,"result":result}));
        }

        let body = if responses.len() == 1 && !req.is_array() {
            serde_json::to_vec(&responses[0]).unwrap()
        } else {
            serde_json::to_vec(&responses).unwrap()
        };

        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
    }

    fn make_rpc_block_local(number: u64, hash: &str, parent_hash: &str) -> Value {
        json!({
            "number": format!("0x{number:x}"),
            "hash": hash,
            "parentHash": parent_hash,
            "difficulty": "0x0",
            "totalDifficulty": "0x0",
            "excessBlobGas": null,
            "extraData": "0x",
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x0",
            "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "miner": "0x0000000000000000000000000000000000000000",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "nonce": "0x0000000000000000",
            "baseFeePerGas": "0x1",
            "size": "0x220",
            "timestamp": format!("0x{:x}", 1700000000u64 + number * 12),
            "transactions": [],
            "uncles": [],
            "withdrawals": []
        })
    }

    let tags = Arc::new(std::sync::Mutex::new(Vec::new()));
    let probe_state = SlowProbeState {
        _call_count: Arc::new(AtomicUsize::new(0)),
        tags: Arc::clone(&tags),
    };
    let app = Router::new()
        .route("/", post(slow_probe_handler))
        .with_state(probe_state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    SlowFinalityUpstream {
        url: format!("http://127.0.0.1:{}", addr.port()),
        tags,
    }
}

/// Drive a head stream against `upstream` and report what it took to deliver its
/// first block, and what the upstream had been asked for by then.
async fn first_block(config: RpcClientConfig, upstream: &SlowFinalityUpstream) -> FirstBlock {
    use data_service_core::source::{DataSource, StreamRequest};
    use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
    use futures::StreamExt;

    let ds = EvmRpcDataSource::new(
        Arc::new(RpcClient::new(config)),
        EvmRpcDataSourceOptions {
            stride_size: 1,
            stride_concurrency: 1,
            ..Default::default()
        },
    );

    let mut stream = ds.get_stream(StreamRequest {
        from: 1,
        to: Some(2),
        parent_hash: None,
    });

    let t_start = Instant::now();
    let mut blocks_yielded = 0usize;
    let mut first_batch_elapsed = None;
    let mut tags_before = None;

    // Collect until the stream terminates or 5 seconds pass
    tokio::select! {
        _ = async {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(batch) => {
                        if !batch.blocks.is_empty() {
                            if first_batch_elapsed.is_none() {
                                first_batch_elapsed = Some(t_start.elapsed());
                                tags_before =
                                    Some(upstream.tags.lock().expect("tag log").clone());
                            }
                            blocks_yielded += batch.blocks.len();
                        }
                        // Stop once we've got blocks 1 and 2
                        if blocks_yielded >= 2 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(8)) => {}
    }

    assert!(
        blocks_yielded >= 1,
        "should have yielded at least 1 block, got {blocks_yielded}"
    );
    FirstBlock {
        elapsed: first_batch_elapsed.expect("a delivered block is timed"),
        tags_before: tags_before.expect("a delivered block has a tag log"),
    }
}
