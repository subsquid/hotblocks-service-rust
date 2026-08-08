//! HC-12 / CT-6 head-path latency benchmark support.
//!
//! Fabricates a verification-clean chain from the real mainnet block
//! 18500000 fixture (157 real signed txs + receipts): each bench block keeps
//! the fixture's transactions and receipts byte-for-byte at the RLP level, so
//! tx root, receipts root, logs bloom and withdrawals root stay valid, while
//! number/parentHash/timestamp are rewritten per height and the header hash is
//! recomputed with the crate's own `ChainUtils`. All `--verify-*` switches
//! therefore do real crypto work against a scripted upstream.

pub mod upstream;

use anyhow::{ensure, Context, Result};
use serde_json::Value;

use evm_source::chain_utils::ChainUtils;
use evm_source::rpc_data::{RpcBlock, RpcReceipt};

pub const BASE_HEIGHT: u64 = 18_500_000;
const FIXTURE_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/verification/ethereum/18500000"
);

pub fn qty(n: u64) -> String {
    format!("0x{n:x}")
}

pub struct BenchBlock {
    pub number: u64,
    pub hash: String,
    /// eth_getBlockByNumber(_, true) answer.
    pub full: Value,
    /// eth_getBlockByNumber(_, false) answer: transactions as hashes.
    pub header_only: Value,
    /// eth_getBlockReceipts answer.
    pub receipts: Value,
}

pub struct BenchChain {
    pub blocks: Vec<BenchBlock>,
}

impl BenchChain {
    pub fn height_of(&self, idx: usize) -> u64 {
        BASE_HEIGHT + idx as u64
    }
}

fn load_fixture(name: &str) -> Result<Value> {
    let path = format!("{FIXTURE_DIR}/{name}");
    let bytes = std::fs::read(&path).with_context(|| format!("reading {path}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {path}"))
}

pub fn load_fixture_block() -> Result<Value> {
    load_fixture("block.json")
}

pub fn load_fixture_receipts() -> Result<Value> {
    load_fixture("receipts.json")
}

/// Build a chain of `count` consecutive blocks starting at BASE_HEIGHT.
/// Block 0 is the untouched fixture; later heights re-chain it.
pub fn build_chain(count: usize) -> Result<BenchChain> {
    let template = load_fixture_block()?;
    let receipts_template = load_fixture_receipts()?;
    let utils = ChainUtils::new(1, false);

    let base_ts = template["timestamp"]
        .as_str()
        .map(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or(0))
        .context("fixture timestamp")?;

    let mut blocks = Vec::with_capacity(count);
    let mut parent_hash = template["parentHash"]
        .as_str()
        .context("fixture parentHash")?
        .to_string();

    for i in 0..count {
        let number = BASE_HEIGHT + i as u64;
        let mut full = template.clone();
        full["number"] = Value::String(qty(number));
        full["parentHash"] = Value::String(parent_hash.clone());
        full["timestamp"] = Value::String(qty(base_ts + 12 * i as u64));

        let rpc_block: RpcBlock =
            serde_json::from_value(full.clone()).context("fixture block does not parse")?;
        let hash = if i == 0 {
            // The genuine mainnet block keeps its genuine hash.
            template["hash"]
                .as_str()
                .context("fixture hash")?
                .to_string()
        } else {
            utils.calculate_block_hash(&rpc_block)?
        };

        full["hash"] = Value::String(hash.clone());
        let mut tx_hashes = Vec::new();
        for tx in full["transactions"].as_array_mut().context("txs")? {
            tx["blockHash"] = Value::String(hash.clone());
            tx["blockNumber"] = Value::String(qty(number));
            tx_hashes.push(tx["hash"].clone());
        }

        let mut header_only = full.clone();
        header_only["transactions"] = Value::Array(tx_hashes);

        let mut receipts = receipts_template.clone();
        for receipt in receipts.as_array_mut().context("receipts")? {
            receipt["blockHash"] = Value::String(hash.clone());
            receipt["blockNumber"] = Value::String(qty(number));
            for log in receipt["logs"].as_array_mut().into_iter().flatten() {
                log["blockHash"] = Value::String(hash.clone());
                log["blockNumber"] = Value::String(qty(number));
            }
        }

        blocks.push(BenchBlock {
            number,
            hash: hash.clone(),
            full,
            header_only,
            receipts,
        });
        parent_hash = hash;
    }

    Ok(BenchChain { blocks })
}

/// Fail fast if a fabricated block would not survive the service's own
/// verification battery (the point of the benchmark is measuring it, not
/// tripping it).
pub fn self_check(chain: &BenchChain) -> Result<()> {
    let utils = ChainUtils::new(1, false);
    for (i, b) in chain.blocks.iter().enumerate() {
        let block: RpcBlock = serde_json::from_value(b.full.clone())?;
        let computed = utils.calculate_block_hash(&block)?;
        ensure!(
            computed.eq_ignore_ascii_case(&b.hash),
            "block {} hash mismatch: {} vs {}",
            b.number,
            b.hash,
            computed
        );
        if i == 0 {
            let tx_root = utils
                .calculate_transactions_root(&block)?
                .context("tx root not computable")?;
            ensure!(
                tx_root.eq_ignore_ascii_case(&block.transactions_root),
                "tx root mismatch"
            );
            for tx in &block.transactions {
                let recovered = utils.recover_tx_sender(tx)?;
                if let Some(sender) = recovered {
                    ensure!(
                        sender.eq_ignore_ascii_case(&tx.from),
                        "sender mismatch for {}",
                        tx.hash
                    );
                }
            }
            let receipts: Vec<RpcReceipt> = serde_json::from_value(b.receipts.clone())?;
            let refs: Vec<&RpcReceipt> = receipts.iter().collect();
            let receipts_root = utils.calculate_receipts_root(&block, &refs)?;
            ensure!(
                receipts_root.eq_ignore_ascii_case(&block.receipts_root),
                "receipts root mismatch"
            );
            let logs: Vec<&evm_source::rpc_data::RpcLog> =
                receipts.iter().flat_map(|r| r.logs.iter()).collect();
            let bloom = utils.calculate_logs_bloom(&block, &logs);
            ensure!(
                bloom.eq_ignore_ascii_case(&block.logs_bloom),
                "logs bloom mismatch"
            );
        }
    }
    Ok(())
}

/// Real call frames for the traces leg, lifted from the captured Base blocks
/// in `fixtures/base-traces.json` (1078 genuine call trees) and realigned to
/// `count` transactions: `txHash` labels are stripped (an unlabelled frame
/// passes the label check) and frames cycle if the block has more txs than
/// the fixture pool. Also returns minimal prestate diffs (`{pre:{},post:{}}`)
/// for the statediffs leg — no captured diff fixture exists, and the bench
/// measures the round trip, which the mock's delay simulates.
pub fn build_trace_fixture(count: usize) -> Result<(Value, Value)> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/base-traces.json"
    );
    let bytes = std::fs::read(path).with_context(|| format!("reading {path}"))?;
    let fixtures: Value = serde_json::from_slice(&bytes)?;
    let pool: Vec<Value> = fixtures
        .as_array()
        .context("base-traces is an array")?
        .iter()
        .flat_map(|f| {
            f["raw"]["debugFrames"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    ensure!(!pool.is_empty(), "no frames in base-traces.json");

    let frames: Vec<Value> = (0..count)
        .map(|i| {
            let mut entry = pool[i % pool.len()].clone();
            entry.as_object_mut().map(|o| o.remove("txHash"));
            entry
        })
        .collect();
    let diffs: Vec<Value> = (0..count)
        .map(|_| serde_json::json!({"result": {"pre": {}, "post": {}}}))
        .collect();
    Ok((Value::Array(frames), Value::Array(diffs)))
}

/// Milliseconds since the epoch — the same clock `data-service-core` stamps
/// commits with, so cross-process deltas are valid.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// min/median/p90/max over a sample.
pub fn stats(samples: &[f64]) -> (f64, f64, f64, f64) {
    let mut s: Vec<f64> = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    if n == 0 {
        return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
    }
    let med = if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2.0
    };
    let p90 = s[((n as f64 * 0.9).ceil() as usize).min(n) - 1];
    (s[0], med, p90, s[n - 1])
}
