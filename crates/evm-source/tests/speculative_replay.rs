//! Asserts on what the upstream was *asked*: the defect worth catching is a
//! replay that validates and is then fetched a second time anyway.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use evm_source::fetch::{Rpc, RpcOptions};
use evm_source::rpc_data::{RawRpcBlock, RpcBlock};
use evm_source::types::DataRequest;
use rpc_client::{RpcClient, RpcClientConfig};
use serde_json::{json, Value};
use tokio::net::TcpListener;

fn gnosis_block() -> RawRpcBlock {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/gnosis-block-no-total-difficulty.json");
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let block: RpcBlock = serde_json::from_value(fixture["getBlockByNumber"].clone()).unwrap();
    RawRpcBlock::new(
        u64::from_str_radix(block.number.trim_start_matches("0x"), 16).unwrap(),
        block.hash.clone(),
        block,
    )
}

/// `replays_of` rejects an empty trace list, so the frame has to be well-formed.
fn root_frame() -> Value {
    json!({
        "type": "call",
        "action": {
            "callType": "call",
            "from": "0x0000000000000000000000000000000000000001",
            "gas": "0x0",
            "input": "0x",
            "to": "0x0000000000000000000000000000000000000002",
            "value": "0x0"
        },
        "result": {"gasUsed": "0x0", "output": "0x"},
        "subtraces": 0,
        "traceAddress": []
    })
}

fn replay_for(block: &RawRpcBlock, bind: bool) -> Value {
    Value::Array(
        block
            .block
            .transactions
            .iter()
            .map(|tx| {
                let mut frame = root_frame();
                frame["transactionHash"] = json!(tx.hash);
                if bind {
                    frame["blockHash"] = json!(block.hash);
                }
                json!({"transactionHash": tx.hash, "trace": [frame], "stateDiff": {}})
            })
            .collect(),
    )
}

/// What the upstream answers a number-addressed replay with.
#[derive(Clone, Copy, PartialEq)]
enum Bet {
    Good,
    /// Right transactions, names no block — indistinguishable from a competing
    /// block's replay.
    Unbound,
    Foreign,
    /// Arrived mid-import, short of the block's transactions.
    Short,
}

struct Upstream {
    url: String,
    replays: Arc<AtomicUsize>,
}

/// Answers hash-addressed asks correctly and number-addressed ones with `bet`,
/// counting every replay call either way.
async fn upstream(bet: Bet) -> Upstream {
    let replays = Arc::new(AtomicUsize::new(0));

    async fn handler(
        State((bet, replays)): State<(Bet, Arc<AtomicUsize>)>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        let calls: Vec<Value> = match req.as_array() {
            Some(b) => b.clone(),
            None => vec![req.clone()],
        };
        let block = gnosis_block();
        let mut out = Vec::new();
        for c in &calls {
            let id = c.get("id").cloned().unwrap_or(json!(1));
            let method = c.get("method").and_then(|v| v.as_str()).unwrap_or("");
            let by_hash = c
                .get("params")
                .and_then(|p| p.get(0))
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.len() == 66);
            let result = match method {
                "eth_chainId" => json!("0x64"),
                "trace_replayBlockTransactions" => {
                    replays.fetch_add(1, Ordering::SeqCst);
                    if by_hash {
                        replay_for(&block, true)
                    } else {
                        match bet {
                            Bet::Good => replay_for(&block, true),
                            Bet::Unbound => replay_for(&block, false),
                            Bet::Foreign => {
                                let mut other = gnosis_block();
                                other.hash = format!("0x{:064x}", 0xdead_u64);
                                for (i, tx) in other.block.transactions.iter_mut().enumerate() {
                                    tx.hash = format!("0x{:064x}", i + 1);
                                }
                                replay_for(&other, true)
                            }
                            Bet::Short => {
                                let mut v = replay_for(&block, true);
                                v.as_array_mut().unwrap().truncate(2);
                                v
                            }
                        }
                    }
                }
                _ => Value::Null,
            };
            out.push(json!({"jsonrpc":"2.0","id":id,"result":result}));
        }
        let body = if out.len() == 1 && !req.is_array() {
            serde_json::to_vec(&out[0]).unwrap()
        } else {
            serde_json::to_vec(&out).unwrap()
        };
        (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
    }

    let app = Router::new()
        .route("/", post(handler))
        .with_state((bet, replays.clone()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Upstream {
        url: format!("http://127.0.0.1:{}", addr.port()),
        replays,
    }
}

fn traced_request() -> DataRequest {
    DataRequest {
        traces: true,
        state_diffs: true,
        use_trace_api: true,
        ..DataRequest::default()
    }
}

fn rpc_for(url: String) -> Arc<Rpc> {
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url,
        ..RpcClientConfig::default()
    }));
    Arc::new(Rpc::new(client, RpcOptions::default()))
}

async fn enrich_with_bet(bet: Bet) -> (RawRpcBlock, usize) {
    let up = upstream(bet).await;
    let replays = up.replays.clone();
    let rpc = rpc_for(up.url);
    let block = gnosis_block();
    let req = traced_request();

    let wager = rpc
        .spawn_speculative_replay(
            block.number,
            &req,
            Duration::from_millis(10),
            Duration::from_secs(2),
        )
        .expect("the replay leg runs for this request");
    // Let the bet land before the body would have — the case it exists for.
    tokio::time::sleep(Duration::from_millis(60)).await;

    let (enriched, _) = rpc
        .enrich_block_with_retry(block, &req, Some(wager))
        .await
        .expect("enrichment");
    (enriched, replays.load(Ordering::SeqCst))
}

#[tokio::test]
async fn an_adopted_replay_is_the_only_replay_the_upstream_sees() {
    let (enriched, calls) = enrich_with_bet(Bet::Good).await;
    assert!(enriched.trace_replays.is_some());
    assert_eq!(
        calls, 1,
        "adopting must replace the hash-addressed fetch, not precede it"
    );
}

#[tokio::test]
async fn a_replay_naming_no_block_is_refused_and_refetched() {
    let (enriched, calls) = enrich_with_bet(Bet::Unbound).await;
    assert!(enriched.trace_replays.is_some(), "traces still arrive");
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn a_replay_of_another_block_is_refused_and_refetched() {
    let (enriched, calls) = enrich_with_bet(Bet::Foreign).await;
    assert!(enriched.trace_replays.is_some());
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn a_short_replay_is_refused_and_refetched() {
    let (enriched, calls) = enrich_with_bet(Bet::Short).await;
    assert!(enriched.trace_replays.is_some());
    assert_eq!(calls, 2);
}

#[tokio::test]
async fn a_zero_grain_starts_no_bet() {
    let up = upstream(Bet::Good).await;
    assert!(rpc_for(up.url)
        .spawn_speculative_replay(
            gnosis_block().number,
            &traced_request(),
            Duration::ZERO,
            Duration::from_secs(1)
        )
        .is_none());
}
