//! Fault-injecting upstream stub (HC-3): the EVM JSON-RPC surface of
//! 14 §upstream over a deterministic scripted chain.
//!
//! Faults are keyed by `(method, tracer, block)` — the debug tracers share one
//! method, so only the tracer names a single component. Calls are counted, so a
//! test can assert the *bound* on WP-11.2 retries, not just that they happen.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use crate::sim::splitmix64;

/// 32-byte digest of `(kind, a, b)`: chain data is a pure function of the
/// script, so a failing run replays from its parameters.
fn digest(kind: u64, a: u64, b: u64) -> String {
    let mut s = splitmix64(kind ^ a.rotate_left(17) ^ b.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut out = String::with_capacity(66);
    out.push_str("0x");
    for _ in 0..4 {
        s = splitmix64(s);
        out.push_str(&format!("{s:016x}"));
    }
    out
}

fn qty(n: u64) -> String {
    format!("0x{n:x}")
}

fn qty2_u64(s: &str) -> u64 {
    u64::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16).unwrap_or(0)
}

fn zero_bloom() -> String {
    format!("0x{}", "0".repeat(512))
}

fn addr(n: u64) -> String {
    format!("0x{n:040x}")
}

/// What an empty transaction or receipt list commits to.
pub const EMPTY_TRIE_ROOT: &str =
    "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421";

/// Honest where the script leaves the block empty; an opaque digest otherwise,
/// since the stub builds no tries.
fn commitment(b: &ChainBlock, salt: u64) -> String {
    if b.txs.is_empty() {
        EMPTY_TRIE_ROOT.to_string()
    } else {
        digest(7, b.number, salt)
    }
}

// ─── Scripted chain ───────────────────────────────────────────────────────────

/// Identity plus the transaction hashes every component payload derives from.
#[derive(Clone, Debug)]
pub struct ChainBlock {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub txs: Vec<String>,
}

/// A linear chain of blocks. `branch` separates two chains that share heights:
/// the wrong-block fault answers from branch `n+1` of the same script.
#[derive(Clone, Debug)]
pub struct Chain {
    pub blocks: Vec<ChainBlock>,
    branch: u64,
}

impl Chain {
    pub fn linear(from: u64, count: u64, txs_per_block: usize) -> Self {
        Chain::branched(from, count, txs_per_block, 0)
    }

    pub fn branched(from: u64, count: u64, txs_per_block: usize, branch: u64) -> Self {
        let blocks = (from..from + count)
            .map(|number| ChainBlock {
                number,
                hash: digest(branch, number, 1),
                parent_hash: digest(branch, number.saturating_sub(1), 1),
                txs: (0..txs_per_block as u64)
                    .map(|i| digest(branch, number, 100 + i))
                    .collect(),
            })
            .collect();
        Chain { blocks, branch }
    }

    pub fn first(&self) -> &ChainBlock {
        self.blocks.first().expect("chain is non-empty")
    }

    pub fn last(&self) -> &ChainBlock {
        self.blocks.last().expect("chain is non-empty")
    }

    pub fn at(&self, number: u64) -> Option<&ChainBlock> {
        self.blocks.iter().find(|b| b.number == number)
    }

    fn by_hash(&self, hash: &str) -> Option<&ChainBlock> {
        self.blocks.iter().find(|b| b.hash == hash)
    }

    /// The same height on a chain nobody asked for.
    fn foreign(&self, number: u64) -> ChainBlock {
        let txs_per_block = self.first().txs.len();
        Chain::branched(number, 1, txs_per_block, self.branch + 1)
            .blocks
            .remove(0)
    }
}

// ─── Faults ───────────────────────────────────────────────────────────────────

/// What a faulted call answers instead of the truth.
#[derive(Clone, Debug)]
pub enum Fault {
    /// A JSON-RPC error object.
    Error { code: i64, message: String },
    /// `"result": null` — the not-yet-indexed answer of FM-16/IB-16.
    Null,
    /// A well-formed answer that describes a different block.
    WrongBlock,
    /// The truthful answer minus the last transaction's entry: the provider
    /// that covers only part of a block (IB-15).
    Truncated,
    /// A payload that is not the method's result type at all.
    Malformed,
    /// The method's container holding one entry that violates its schema.
    MalformedEntry,
    /// One key removed from every entry: a tracer request answered without the
    /// tracer's data.
    DropKey(String),
    /// One key emptied in every entry: the tracer that answers with no records.
    EmptyKey(String),
    /// The truthful answer with its last entry repeated.
    Duplicated,
    /// One entry stripped of its own label and given another transaction's
    /// frames: the misattribution that survives per-transaction counting.
    MixedFrames,
    /// A schema-valid debug response whose nested self-destruct frame has no
    /// beneficiary, so normalization cannot map the tree without data loss.
    InvalidDebugFrameStructure,
    /// One header field replaced by a value the block's own contents do not
    /// commit to — the forged input REQ-14's switches exist to catch.
    ForgedField { key: String, value: Value },
}

impl Fault {
    pub fn error(message: &str) -> Fault {
        Fault::Error {
            code: -32003,
            message: message.to_string(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Key {
    method: String,
    tracer: Option<String>,
    block: u64,
    addressed_by_hash: bool,
}

struct Injection {
    method: String,
    /// `None` matches any tracer.
    tracer: Option<String>,
    block: u64,
    fault: Fault,
    /// Remaining calls to fault; `None` never heals.
    remaining: Option<usize>,
}

struct Inner {
    chain: Chain,
    head: u64,
    finalized: u64,
    injections: Vec<Injection>,
    calls: HashMap<Key, usize>,
}

/// A running stub; dropping it leaves the server task alive until the test
/// process exits, as the other mock upstreams here do.
pub struct Upstream {
    url: String,
    inner: Arc<Mutex<Inner>>,
}

impl Upstream {
    /// Head starts at the chain's last block, finality at its first.
    pub async fn start(chain: Chain) -> Upstream {
        let head = chain.last().number;
        let finalized = chain.first().number;
        let inner = Arc::new(Mutex::new(Inner {
            chain,
            head,
            finalized,
            injections: Vec::new(),
            calls: HashMap::new(),
        }));

        let app = Router::new()
            .route("/", post(handle))
            .with_state(Arc::clone(&inner));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Upstream {
            url: format!("http://127.0.0.1:{port}"),
            inner,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn set_head(&self, number: u64) {
        self.inner.lock().expect("upstream lock").head = number;
    }

    pub fn set_finalized(&self, number: u64) {
        self.inner.lock().expect("upstream lock").finalized = number;
    }

    /// Fault every call of `method` for `block`, whatever tracer it carries.
    pub fn inject(&self, method: &str, block: u64, fault: Fault) {
        self.push(method, None, block, fault, None);
    }

    /// Fault only the calls carrying `tracer` — one debug method serves two
    /// components.
    pub fn inject_tracer(&self, method: &str, tracer: &str, block: u64, fault: Fault) {
        self.push(method, Some(tracer.to_string()), block, fault, None);
    }

    /// Fault the first `n` calls — a transient WP-11.2 must heal.
    pub fn inject_for(&self, method: &str, block: u64, fault: Fault, n: usize) {
        self.push(method, None, block, fault, Some(n));
    }

    fn push(
        &self,
        method: &str,
        tracer: Option<String>,
        block: u64,
        fault: Fault,
        remaining: Option<usize>,
    ) {
        self.inner
            .lock()
            .expect("upstream lock")
            .injections
            .push(Injection {
                method: method.to_string(),
                tracer,
                block,
                fault,
                remaining,
            });
    }

    /// Calls of `method` addressed at `block`, summed over tracers.
    pub fn calls(&self, method: &str, block: u64) -> usize {
        self.inner
            .lock()
            .expect("upstream lock")
            .calls
            .iter()
            .filter(|(k, _)| k.method == method && k.block == block)
            .map(|(_, n)| *n)
            .sum()
    }

    pub fn calls_with_tracer(&self, method: &str, tracer: &str, block: u64) -> usize {
        self.inner
            .lock()
            .expect("upstream lock")
            .calls
            .iter()
            .filter(|(k, _)| {
                k.method == method && k.tracer.as_deref() == Some(tracer) && k.block == block
            })
            .map(|(_, n)| *n)
            .sum()
    }

    /// Calls whose first parameter addressed `block` by its hash.
    pub fn calls_by_hash(&self, method: &str, block: u64) -> usize {
        self.calls_by_address(method, block, true)
    }

    /// Calls whose first parameter addressed `block` by its number.
    pub fn calls_by_number(&self, method: &str, block: u64) -> usize {
        self.calls_by_address(method, block, false)
    }

    fn calls_by_address(&self, method: &str, block: u64, addressed_by_hash: bool) -> usize {
        self.inner
            .lock()
            .expect("upstream lock")
            .calls
            .iter()
            .filter(|(k, _)| {
                k.method == method && k.block == block && k.addressed_by_hash == addressed_by_hash
            })
            .map(|(_, n)| *n)
            .sum()
    }
}

// ─── Request handling ─────────────────────────────────────────────────────────

async fn handle(
    State(inner): State<Arc<Mutex<Inner>>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = req.is_array();
    let calls: Vec<Value> = if batched {
        req.as_array().cloned().unwrap_or_default()
    } else {
        vec![req]
    };

    let mut responses = Vec::with_capacity(calls.len());
    {
        let mut inner = inner.lock().expect("upstream lock");
        for call in &calls {
            let id = call.get("id").cloned().unwrap_or(json!(1));
            let method = call.get("method").and_then(|v| v.as_str()).unwrap_or("");
            let params = call.get("params").cloned().unwrap_or(json!([]));
            responses.push(match inner.answer(method, &params) {
                Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                Err((code, message)) => {
                    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
                }
            });
        }
    }

    let out = match responses.first() {
        Some(single) if !batched => serde_json::to_vec(single),
        _ => serde_json::to_vec(&responses),
    }
    .expect("serialize response");

    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        out,
    )
}

impl Inner {
    fn answer(&mut self, method: &str, params: &Value) -> Result<Value, (i64, String)> {
        let p0 = params.get(0).cloned().unwrap_or(Value::Null);
        let tracer = tracer_of(method, params);
        let block = self.addressed_block(method, &p0);

        if let Some(number) = block {
            let key = Key {
                method: method.to_string(),
                tracer: tracer.clone(),
                block: number,
                addressed_by_hash: p0.as_str().is_some_and(|value| value.len() > 42),
            };
            *self.calls.entry(key).or_insert(0) += 1;

            if let Some(fault) = self.take_fault(method, tracer.as_deref(), number) {
                return self.faulted(method, params, tracer.as_deref(), number, &fault);
            }
        }

        self.truth(method, params, &p0, tracer.as_deref(), block)
    }

    /// The block a call is about, by number or by hash. `None` for calls that
    /// address no single block (chain id, head, log ranges).
    fn addressed_block(&self, method: &str, p0: &Value) -> Option<u64> {
        match method {
            "eth_getBlockByNumber"
            | "eth_getBlockReceipts"
            | "debug_traceBlockByNumber"
            | "debug_traceBlockByHash"
            | "trace_replayBlockTransactions"
            | "trace_block" => {
                let s = p0.as_str()?;
                match s {
                    "latest" | "safe" | "pending" => Some(self.head),
                    "finalized" => Some(self.finalized),
                    _ if s.len() > 42 => self.chain.by_hash(s).map(|b| b.number),
                    _ => Some(qty2_u64(s)),
                }
            }
            "eth_getTransactionReceipt" => {
                let hash = p0.as_str()?;
                self.chain
                    .blocks
                    .iter()
                    .find(|b| b.txs.iter().any(|t| t == hash))
                    .map(|b| b.number)
            }
            _ => None,
        }
    }

    fn take_fault(&mut self, method: &str, tracer: Option<&str>, block: u64) -> Option<Fault> {
        let inj = self.injections.iter_mut().find(|i| {
            i.method == method
                && i.block == block
                && i.tracer.as_deref().is_none_or(|t| Some(t) == tracer)
                && i.remaining != Some(0)
        })?;
        if let Some(n) = inj.remaining.as_mut() {
            *n -= 1;
        }
        Some(inj.fault.clone())
    }

    fn faulted(
        &self,
        method: &str,
        params: &Value,
        tracer: Option<&str>,
        block: u64,
        fault: &Fault,
    ) -> Result<Value, (i64, String)> {
        match fault {
            Fault::Error { code, message } => Err((*code, message.clone())),
            Fault::Null => Ok(Value::Null),
            Fault::Malformed => Ok(json!({"unexpected": "payload"})),
            Fault::WrongBlock => {
                let foreign = self.chain.foreign(block);
                Ok(self.component(method, tracer, &foreign))
            }
            Fault::Truncated => {
                let dropped = match self.visible(Some(block)) {
                    Some(b) => b.txs.last().cloned().unwrap_or_default(),
                    None => return Ok(Value::Null),
                };
                let mut value = self.truthful(method, tracer, block);
                if let Some(arr) = value.as_array_mut() {
                    // trace_block is flat, so the reward frame stays; every
                    // other method is one entry per transaction.
                    if method == "trace_block" {
                        arr.retain(|f| {
                            f.get("transactionHash").and_then(|h| h.as_str())
                                != Some(dropped.as_str())
                        });
                    } else {
                        arr.pop();
                    }
                }
                Ok(value)
            }
            Fault::MalformedEntry => Ok(malformed_entry(method, tracer)),
            Fault::DropKey(key) => {
                let mut value = self.truthful(method, tracer, block);
                for entry in value.as_array_mut().into_iter().flatten() {
                    if let Some(object) = entry.as_object_mut() {
                        object.remove(key);
                    }
                }
                Ok(value)
            }
            Fault::EmptyKey(key) => {
                let mut value = self.truthful(method, tracer, block);
                for entry in value.as_array_mut().into_iter().flatten() {
                    if let Some(object) = entry.as_object_mut() {
                        if object.contains_key(key) {
                            object.insert(key.clone(), json!([]));
                        }
                    }
                }
                Ok(value)
            }
            Fault::Duplicated => {
                let mut value = self.truthful(method, tracer, block);
                if let Some(arr) = value.as_array_mut() {
                    if let Some(last) = arr.last().cloned() {
                        arr.push(last);
                    }
                }
                Ok(value)
            }
            Fault::MixedFrames => {
                let mut value = self.truthful(method, tracer, block);
                let arr = match value.as_array_mut() {
                    Some(arr) if arr.len() > 1 => arr,
                    _ => return Ok(value),
                };
                let borrowed = arr.last().and_then(|e| e.get("trace")).cloned();
                let Some(first) = arr.first_mut().and_then(|e| e.as_object_mut()) else {
                    return Ok(value);
                };
                first.remove("transactionHash");
                if let (Some(Value::Array(extra)), Some(Value::Array(own))) =
                    (borrowed, first.get_mut("trace"))
                {
                    own.extend(extra);
                }
                Ok(value)
            }
            Fault::InvalidDebugFrameStructure => {
                let mut value = self.truthful(method, tracer, block);
                if let Some(result) = value
                    .as_array_mut()
                    .and_then(|entries| entries.first_mut())
                    .and_then(|entry| entry.get_mut("result"))
                    .and_then(Value::as_object_mut)
                {
                    result.insert(
                        "calls".to_string(),
                        json!([{
                            "type": "SELFDESTRUCT",
                            "from": addr(2),
                            "gas": "0x0"
                        }]),
                    );
                }
                Ok(value)
            }
            Fault::ForgedField { key, value } => {
                let full = params.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                let mut answer = match self.visible(Some(block)) {
                    Some(b) => header(b, full),
                    None => return Ok(Value::Null),
                };
                if let Some(object) = answer.as_object_mut() {
                    object.insert(key.clone(), value.clone());
                }
                Ok(answer)
            }
        }
    }

    fn truth(
        &self,
        method: &str,
        params: &Value,
        p0: &Value,
        tracer: Option<&str>,
        block: Option<u64>,
    ) -> Result<Value, (i64, String)> {
        match method {
            "eth_chainId" => Ok(json!("0x1")),
            "eth_blockNumber" => Ok(json!(qty(self.head))),
            "eth_getLogs" => Ok(json!([])),
            "eth_getBlockByNumber" => {
                let full = params.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                Ok(match self.visible(block) {
                    Some(b) => header(b, full),
                    None => Value::Null,
                })
            }
            "eth_getTransactionReceipt" => {
                let hash = p0.as_str().unwrap_or_default();
                Ok(self
                    .visible(block)
                    .and_then(|b| b.txs.iter().position(|t| t == hash).map(|i| receipt(b, i)))
                    .unwrap_or(Value::Null))
            }
            _ => Ok(match self.visible(block) {
                Some(b) => self.component(method, tracer, b),
                None => Value::Null,
            }),
        }
    }

    /// The answer this call gets when no fault is registered for it.
    fn truthful(&self, method: &str, tracer: Option<&str>, block: u64) -> Value {
        match self.visible(Some(block)) {
            Some(b) => self.component(method, tracer, b),
            None => Value::Null,
        }
    }

    /// A block the upstream admits to having: in the script and at or below the
    /// current head.
    fn visible(&self, block: Option<u64>) -> Option<&ChainBlock> {
        let number = block?;
        if number > self.head {
            return None;
        }
        self.chain.at(number)
    }

    fn component(&self, method: &str, tracer: Option<&str>, b: &ChainBlock) -> Value {
        match method {
            "eth_getBlockReceipts" => {
                json!((0..b.txs.len()).map(|i| receipt(b, i)).collect::<Vec<_>>())
            }
            "debug_traceBlockByNumber" | "debug_traceBlockByHash" => match tracer {
                Some("prestateTracer") => debug_state_diffs(b),
                _ => debug_frames(b),
            },
            "trace_replayBlockTransactions" => trace_replays(b, tracer),
            "trace_block" => trace_block_frames(b),
            _ => Value::Null,
        }
    }
}

/// The tracer a call selects. Replay asks for its tracers as a set in one call,
/// so its components cannot be faulted apart.
fn tracer_of(method: &str, params: &Value) -> Option<String> {
    match method {
        "debug_traceBlockByNumber" | "debug_traceBlockByHash" => params
            .get(1)
            .and_then(|c| c.get("tracer"))
            .and_then(|t| t.as_str())
            .map(|s| s.to_string()),
        _ => None,
    }
}

// ─── Payloads ─────────────────────────────────────────────────────────────────

fn header(b: &ChainBlock, full_txs: bool) -> Value {
    let transactions: Vec<Value> = if full_txs {
        b.txs
            .iter()
            .enumerate()
            .map(|(i, hash)| {
                json!({
                    "hash": hash,
                    "nonce": qty(i as u64),
                    "blockHash": b.hash,
                    "blockNumber": qty(b.number),
                    "transactionIndex": qty(i as u64),
                    "from": addr(1),
                    "to": addr(2),
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
                    "r": "0x1",
                    "s": "0x1"
                })
            })
            .collect()
    } else {
        b.txs.iter().map(|h| json!(h)).collect()
    };

    json!({
        "number": qty(b.number),
        "hash": b.hash,
        "parentHash": b.parent_hash,
        "difficulty": "0x0",
        "totalDifficulty": "0x0",
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "gasUsed": qty(21000 * b.txs.len() as u64),
        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "logsBloom": zero_bloom(),
        "transactionsRoot": commitment(b, 2),
        "receiptsRoot": commitment(b, 3),
        "stateRoot": digest(7, b.number, 4),
        "miner": addr(0),
        "mixHash": digest(7, b.number, 5),
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x1",
        "size": "0x220",
        "timestamp": qty(1_700_000_000 + b.number * 12),
        "transactions": transactions,
        "uncles": [],
        "withdrawals": []
    })
}

fn receipt(b: &ChainBlock, i: usize) -> Value {
    json!({
        "blockHash": b.hash,
        "blockNumber": qty(b.number),
        "transactionHash": b.txs[i],
        "transactionIndex": qty(i as u64),
        "contractAddress": null,
        "cumulativeGasUsed": qty(21000 * (i as u64 + 1)),
        "gasUsed": "0x5208",
        "from": addr(1),
        "to": addr(2),
        "effectiveGasPrice": "0x1",
        "logs": [],
        "logsBloom": zero_bloom(),
        "status": "0x1",
        "type": "0x2"
    })
}

fn debug_frames(b: &ChainBlock) -> Value {
    json!(b
        .txs
        .iter()
        .map(|hash| json!({
            "txHash": hash,
            "result": {
                "type": "CALL",
                "from": addr(1),
                "to": addr(2),
                "value": "0x0",
                "gas": "0x5208",
                "gasUsed": "0x5208",
                "input": "0x",
                "output": "0x"
            }
        }))
        .collect::<Vec<_>>())
}

fn debug_state_diffs(b: &ChainBlock) -> Value {
    json!(b
        .txs
        .iter()
        .map(|hash| json!({
            "txHash": hash,
            "result": {
                "pre": {addr(1): {"balance": "0x2", "nonce": 1}},
                "post": {addr(1): {"balance": "0x1", "nonce": 2}}
            }
        }))
        .collect::<Vec<_>>())
}

fn replay_trace_frame(b: &ChainBlock, i: usize) -> Value {
    json!({
        "action": {
            "callType": "call",
            "from": addr(1),
            "to": addr(2),
            "value": "0x0",
            "gas": "0x5208",
            "input": "0x"
        },
        "result": {"gasUsed": "0x5208", "output": "0x"},
        "blockHash": b.hash,
        "blockNumber": b.number,
        "subtraces": 0,
        "traceAddress": [],
        "transactionHash": b.txs[i],
        "transactionPosition": i,
        "type": "call"
    })
}

fn trace_replays(b: &ChainBlock, _tracer: Option<&str>) -> Value {
    // Both tracers always: the fetch layer reads only the keys it asked for,
    // and a node returning more is not the fault under test.
    json!(b
        .txs
        .iter()
        .enumerate()
        .map(|(i, hash)| json!({
            "transactionHash": hash,
            "output": "0x",
            "trace": [replay_trace_frame(b, i)],
            "stateDiff": {addr(1): {"balance": {"*": {"from": "0x2", "to": "0x1"}}}}
        }))
        .collect::<Vec<_>>())
}

fn trace_block_frames(b: &ChainBlock) -> Value {
    let mut frames: Vec<Value> = (0..b.txs.len()).map(|i| replay_trace_frame(b, i)).collect();
    // Real nodes put the block reward at the end; it carries no transaction.
    frames.push(json!({
        "action": {"author": addr(0), "rewardType": "block", "value": "0x1"},
        "blockHash": b.hash,
        "blockNumber": b.number,
        "result": null,
        "subtraces": 0,
        "traceAddress": [],
        "transactionHash": null,
        "transactionPosition": null,
        "type": "reward"
    }));
    json!(frames)
}

/// The method's own container with one entry that cannot deserialize.
fn malformed_entry(method: &str, tracer: Option<&str>) -> Value {
    match (method, tracer) {
        ("debug_traceBlockByNumber" | "debug_traceBlockByHash", Some("prestateTracer")) => {
            json!([{"result": {"pre": "not-a-map"}}])
        }
        ("debug_traceBlockByNumber" | "debug_traceBlockByHash", _) => {
            json!([{"result": {"type": "CALL"}}])
        }
        ("trace_replayBlockTransactions", _) => json!([{"trace": "not-an-array"}]),
        ("trace_block", _) => json!([{"type": "call", "subtraces": "many"}]),
        _ => json!([{"unexpected": true}]),
    }
}
