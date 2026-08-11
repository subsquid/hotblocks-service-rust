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
        .enrich_block_with_retry(block, &req, Some(wager), None)
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

// ─── The bet's lifecycle, driven through the real ingest loop ────────────────

/// Withholds each block until `delay_ms` after the previous one was served, so
/// the loop has to sit in its cadence sleep the way it does on a real chain.
/// Counts number-addressed replays, which only the bet issues.
mod chain {
    use super::*;
    use std::sync::Mutex;
    use std::time::Instant;

    pub struct Counts {
        pub spec_replays: AtomicUsize,
        pub adopted_leg: AtomicUsize,
    }

    pub struct Chain {
        pub url: String,
        pub counts: Arc<Counts>,
    }

    struct Clock {
        started: Instant,
        interval: Duration,
    }

    /// Block N becomes visible N intervals in, so a bet opened a whole interval
    /// early expires before the block it is betting on exists.
    pub async fn serve(interval: Duration, first: u64) -> Chain {
        let counts = Arc::new(Counts {
            spec_replays: AtomicUsize::new(0),
            adopted_leg: AtomicUsize::new(0),
        });
        let clock = Arc::new(Mutex::new(Clock {
            started: Instant::now(),
            interval,
        }));

        async fn handler(
            State((clock, counts, first)): State<(Arc<Mutex<Clock>>, Arc<Counts>, u64)>,
            body: axum::body::Bytes,
        ) -> impl IntoResponse {
            let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
            let calls: Vec<Value> = match req.as_array() {
                Some(b) => b.clone(),
                None => vec![req.clone()],
            };
            let visible_through = {
                let c = clock.lock().unwrap();
                first + (c.started.elapsed().as_millis() as u64 / c.interval.as_millis() as u64)
            };
            let block = gnosis_block();
            let mut out = Vec::new();
            for c in &calls {
                let id = c.get("id").cloned().unwrap_or(json!(1));
                let method = c.get("method").and_then(|v| v.as_str()).unwrap_or("");
                let p0 = c.get("params").and_then(|p| p.get(0)).cloned();
                let asked = p0
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .filter(|s| s.starts_with("0x") && s.len() < 20)
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok());
                let result = match method {
                    "eth_chainId" => json!("0x64"),
                    "eth_getBlockByNumber" => match asked {
                        Some(n) if n <= visible_through => block_at(&block, n),
                        _ => Value::Null,
                    },
                    "trace_replayBlockTransactions" => {
                        let by_hash = p0
                            .as_ref()
                            .and_then(|v| v.as_str())
                            .is_some_and(|s| s.len() == 66);
                        if !by_hash {
                            counts.spec_replays.fetch_add(1, Ordering::SeqCst);
                        }
                        match asked {
                            Some(n) if n > visible_through => Value::Null,
                            _ => {
                                if !by_hash {
                                    counts.adopted_leg.fetch_add(1, Ordering::SeqCst);
                                }
                                replay_for(&block, true)
                            }
                        }
                    }
                    "eth_getBlockReceipts" => json!([]),
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

        let app =
            Router::new()
                .route("/", post(handler))
                .with_state((clock, counts.clone(), first));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Chain {
            url: format!("http://127.0.0.1:{}", addr.port()),
            counts,
        }
    }

    fn block_at(template: &RawRpcBlock, n: u64) -> Value {
        let mut v = serde_json::to_value(&template.block).unwrap();
        v["number"] = json!(format!("0x{n:x}"));
        v
    }
}

/// The bug this catches: the bet was opened on the first poll after the previous
/// block, so on a chain with a real interval it spent its whole budget while the
/// block did not yet exist and had expired by the time it appeared. Asserted on
/// the adoption counter, because "an ask reached the upstream" is true either
/// way — only adoption distinguishes a bet that was still alive.
#[tokio::test]
async fn the_bet_is_in_flight_when_the_block_appears() {
    use data_service_core::metrics::Metrics;
    use evm_source::ingest::ingest_range;
    use evm_source::normalization::MappingOptions;
    use futures::StreamExt;

    const INTERVAL: Duration = Duration::from_millis(700);
    let first = gnosis_block().number;
    let chain = chain::serve(INTERVAL, first).await;

    let metrics = Arc::new(Metrics::new());
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: chain.url,
        ..RpcClientConfig::default()
    }));
    let rpc = Arc::new(Rpc::new(client, RpcOptions::default()).with_metrics(metrics.clone()));

    let mut stream = Box::pin(
        ingest_range(
            rpc,
            Arc::new(traced_request()),
            Arc::new(MappingOptions {
                with_traces: true,
                with_state_diffs: true,
            }),
            first,
            Some(first + 6),
            5,
            5,
            "latest",
            false,
            Some(Duration::from_millis(50)),
        )
        .await,
    );

    let mut served = 0usize;
    tokio::select! {
        _ = async { while let Some(b) = stream.next().await { served += b.unwrap().blocks.len(); } } => {}
        _ = tokio::time::sleep(Duration::from_secs(12)) => {}
    }

    let adopted = adopted_count(&metrics);
    assert!(
        served >= 4,
        "the chain should have advanced, served {served}"
    );
    // Catch-up blocks are visible the instant they are asked for, so a couple of
    // adoptions prove nothing — a bet opened an interval early still caught those.
    // Nearly every block adopting is what says the bet is alive at the arrival.
    assert!(
        adopted + 2 >= served as u64,
        "only {adopted} of {served} blocks adopted a bet: it is expiring before \
         the block it bets on appears"
    );
}

fn adopted_count(metrics: &data_service_core::metrics::Metrics) -> u64 {
    metrics
        .gather_text()
        .unwrap()
        .lines()
        .find(|l| {
            l.starts_with("sqd_hotblocks_speculative_replays_total{") && l.contains("adopted")
        })
        .and_then(|l| l.rsplit(' ').next().map(str::to_string))
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v as u64)
        .unwrap_or(0)
}
