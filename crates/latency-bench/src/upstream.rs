//! Scripted JSON-RPC upstream serving the fabricated chain with a
//! test-controlled head watermark and a simulated round-trip time.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use crate::{unix_ms, BenchChain, BASE_HEIGHT};

pub struct Upstream {
    pub url: String,
    state: Arc<UpstreamState>,
}

pub struct UpstreamState {
    chain: BenchChain,
    /// Highest released block index; blocks above answer null.
    head_idx: AtomicUsize,
    finalized_idx: AtomicUsize,
    rtt: Duration,
    /// callTracer answer (per-tx frame entries) served for any visible block.
    trace_frames: Value,
    /// prestateTracer answer served for any visible block.
    state_diffs: Value,
    /// Simulated node-side re-execution time per debug_trace* request.
    trace_ms: Duration,
    /// height → unix ms the first full body left for the service
    /// (adjusted by rtt/2, so it approximates service-side receive time).
    pub body_served_ms: Mutex<HashMap<u64, u64>>,
}

impl Upstream {
    pub async fn start(
        chain: BenchChain,
        rtt: Duration,
        trace_frames: Value,
        state_diffs: Value,
        trace_ms: Duration,
    ) -> anyhow::Result<Upstream> {
        let state = Arc::new(UpstreamState {
            chain,
            head_idx: AtomicUsize::new(0),
            finalized_idx: AtomicUsize::new(0),
            rtt,
            trace_frames,
            state_diffs,
            trace_ms,
            body_served_ms: Mutex::new(HashMap::new()),
        });
        let app = Router::new()
            .route("/", post(handler))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("upstream serve");
        });
        Ok(Upstream {
            url: format!("http://127.0.0.1:{}", addr.port()),
            state,
        })
    }

    /// Release block `idx`; from now on it is the chain head.
    pub fn set_head(&self, idx: usize) {
        self.state.head_idx.store(idx, Ordering::SeqCst);
    }

    pub fn set_finalized(&self, idx: usize) {
        self.state.finalized_idx.store(idx, Ordering::SeqCst);
    }

    pub fn body_served_ms(&self, height: u64) -> Option<u64> {
        self.state
            .body_served_ms
            .lock()
            .unwrap()
            .get(&height)
            .copied()
    }
}

fn resolve_tag(state: &UpstreamState, tag: &str) -> Option<usize> {
    match tag {
        "latest" | "safe" | "pending" => Some(state.head_idx.load(Ordering::SeqCst)),
        "finalized" => Some(state.finalized_idx.load(Ordering::SeqCst)),
        "earliest" => Some(0),
        hex => {
            let n = u64::from_str_radix(hex.trim_start_matches("0x"), 16).ok()?;
            if n < BASE_HEIGHT {
                return None;
            }
            let idx = (n - BASE_HEIGHT) as usize;
            (idx <= state.head_idx.load(Ordering::SeqCst) && idx < state.chain.blocks.len())
                .then_some(idx)
        }
    }
}

fn answer(state: &UpstreamState, method: &str, params: &Value) -> Value {
    match method {
        "eth_chainId" => json!("0x1"),
        "eth_blockNumber" => {
            let head = state.head_idx.load(Ordering::SeqCst);
            json!(crate::qty(state.chain.height_of(head)))
        }
        "eth_getLogs" => json!([]),
        "eth_getBlockByNumber" => {
            let tag = params.get(0).and_then(Value::as_str).unwrap_or("");
            let full_txs = params.get(1).and_then(Value::as_bool).unwrap_or(false);
            match resolve_tag(state, tag) {
                None => Value::Null,
                Some(idx) => {
                    let block = &state.chain.blocks[idx];
                    if full_txs {
                        // A numbered full-body answer is what the speculative
                        // poll (and backfill) consumes: stamp first service.
                        state
                            .body_served_ms
                            .lock()
                            .unwrap()
                            .entry(block.number)
                            .or_insert_with(|| unix_ms() + state.rtt.as_millis() as u64 / 2);
                        block.full.clone()
                    } else {
                        block.header_only.clone()
                    }
                }
            }
        }
        "eth_getBlockReceipts" => {
            let tag = params.get(0).and_then(Value::as_str).unwrap_or("");
            match resolve_tag(state, tag) {
                None => Value::Null,
                Some(idx) => state.chain.blocks[idx].receipts.clone(),
            }
        }
        "debug_traceBlockByNumber" | "debug_traceBlockByHash" => {
            let target = params.get(0).and_then(Value::as_str).unwrap_or("");
            let visible = if target.len() >= 66 {
                let head = state.head_idx.load(Ordering::SeqCst);
                state.chain.blocks[..=head]
                    .iter()
                    .any(|b| b.hash.eq_ignore_ascii_case(target))
            } else {
                resolve_tag(state, target).is_some()
            };
            if !visible {
                return Value::Null;
            }
            let tracer = params
                .get(1)
                .and_then(|c| c.get("tracer"))
                .and_then(Value::as_str)
                .unwrap_or("");
            match tracer {
                "callTracer" => state.trace_frames.clone(),
                "prestateTracer" => state.state_diffs.clone(),
                _ => Value::Null,
            }
        }
        _ => Value::Null,
    }
}

async fn handler(
    State(state): State<Arc<UpstreamState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Request flight time.
    tokio::time::sleep(state.rtt / 2).await;

    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = request.is_array();
    let calls = request.as_array().cloned().unwrap_or_else(|| vec![request]);

    // A trace request re-executes the block node-side.
    if calls.iter().any(|c| {
        c.get("method")
            .and_then(Value::as_str)
            .is_some_and(|m| m.starts_with("debug_trace"))
    }) {
        tokio::time::sleep(state.trace_ms).await;
    }

    let responses: Vec<Value> = calls
        .iter()
        .map(|call| {
            let id = call.get("id").cloned().unwrap_or(json!(1));
            let method = call.get("method").and_then(Value::as_str).unwrap_or("");
            let params = call.get("params").cloned().unwrap_or(json!([]));
            let result = answer(&state, method, &params);
            json!({"jsonrpc": "2.0", "id": id, "result": result})
        })
        .collect();

    // Response flight time.
    tokio::time::sleep(state.rtt / 2).await;

    let bytes = if batched {
        serde_json::to_vec(&responses)
    } else {
        serde_json::to_vec(&responses[0])
    }
    .expect("serialize response");
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    )
}
