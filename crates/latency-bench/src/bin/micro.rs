//! CPU microbenchmarks for the head-path inline costs, on the real mainnet
//! block 18500000 (157 signed txs) fixture. Hand-rolled timing: warmup, then
//! N timed iterations, min/median/p90 reported.
//!
//! Env knobs: BENCH_ITERS (30), BENCH_OUT (json path).

use std::io::Write;
use std::time::Instant;

use serde_json::{json, Value};

use evm_source::chain_utils::ChainUtils;
use evm_source::mapping::map_raw_block;
use evm_source::normalization::MappingOptions;
use evm_source::rpc_data::{RawRpcBlock, RpcBlock, RpcLog, RpcReceipt};
use latency_bench::{load_fixture_block, load_fixture_receipts, stats};

fn bench<F: FnMut() -> u128>(name: &str, iters: usize, mut once: F) -> serde_json::Value {
    for _ in 0..3 {
        once();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        samples.push(once() as f64 / 1000.0);
    }
    let (min, med, p90, max) = stats(&samples);
    println!("  {name:<28} min {min:8.3}  median {med:8.3}  p90 {p90:8.3}  max {max:8.3}  ms");
    json!({"name": name, "n": iters, "min_ms": min, "median_ms": med, "p90_ms": p90, "max_ms": max})
}

/// Time one closure run in µs, keeping its result alive till after the stop.
fn timed<T>(f: impl FnOnce() -> T) -> u128 {
    let t0 = Instant::now();
    let out = f();
    let dt = t0.elapsed().as_micros();
    std::hint::black_box(out);
    dt
}

fn main() -> anyhow::Result<()> {
    let iters = std::env::var("BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30usize);

    let block_value = load_fixture_block()?;
    let block_bytes = serde_json::to_vec(&block_value)?;
    let receipts_value = load_fixture_receipts()?;
    let block: RpcBlock = serde_json::from_value(block_value.clone())?;
    let receipts: Vec<RpcReceipt> = serde_json::from_value(receipts_value)?;
    let utils = ChainUtils::new(1, false);
    let n_txs = block.transactions.len();

    println!(
        "block 18500000: {} txs, {} receipts, {} KB json",
        n_txs,
        receipts.len(),
        block_bytes.len() / 1024
    );
    println!();

    let mut results = Vec::new();

    results.push(bench("dom_parse(bytes→Value)", iters, || {
        timed(|| serde_json::from_slice::<Value>(&block_bytes).unwrap())
    }));

    results.push(bench("from_value(v.clone())", iters, || {
        timed(|| serde_json::from_value::<RpcBlock>(block_value.clone()).unwrap())
    }));

    results.push(bench("from_value(v) move", iters, || {
        let v = block_value.clone();
        timed(|| serde_json::from_value::<RpcBlock>(v).unwrap())
    }));

    results.push(bench("verify_block_hash", iters, || {
        timed(|| utils.calculate_block_hash(&block).unwrap())
    }));

    results.push(bench("verify_tx_root", iters, || {
        timed(|| utils.calculate_transactions_root(&block).unwrap())
    }));

    results.push(bench("sender_recovery(all txs)", iters, || {
        timed(|| {
            for tx in &block.transactions {
                let recovered = utils.recover_tx_sender(tx).unwrap();
                if let Some(s) = recovered {
                    assert!(s.eq_ignore_ascii_case(&tx.from));
                }
            }
        })
    }));

    results.push(bench("verify_receipts_root", iters, || {
        timed(|| {
            let refs: Vec<&RpcReceipt> = receipts.iter().collect();
            utils.calculate_receipts_root(&block, &refs).unwrap()
        })
    }));

    results.push(bench("verify_logs_bloom", iters, || {
        timed(|| {
            let logs: Vec<&RpcLog> = receipts.iter().flat_map(|r| r.logs.iter()).collect();
            utils.calculate_logs_bloom(&block, &logs)
        })
    }));

    results.push(bench("verify_withdrawals_root", iters, || {
        timed(|| {
            let w: Vec<_> = block.withdrawals.as_ref().unwrap().iter().collect();
            utils.calculate_withdrawals_root(&w).unwrap()
        })
    }));

    // The full normalize+compress leg, receipts attached (as the commit path
    // runs it in spawn_blocking).
    let raw_with_receipts = {
        let mut raw = RawRpcBlock::new(
            18_500_000,
            block.hash.clone(),
            serde_json::from_value(block_value.clone())?,
        );
        raw.receipts = Some(receipts.clone());
        raw
    };
    let opts = MappingOptions {
        with_traces: false,
        with_state_diffs: false,
    };
    results.push(bench("map_raw_block(norm+zstd)", iters, || {
        timed(|| map_raw_block(&raw_with_receipts, &opts, None).unwrap())
    }));

    // The serve-path gzip transcode exactly as encode_block does it.
    let mapped = map_raw_block(&raw_with_receipts, &opts, None)?;
    println!(
        "  (json line: {} KB, zstd: {} KB)",
        { zstd::decode_all(std::io::Cursor::new(mapped.json_line_zstd.as_ref()))?.len() / 1024 },
        mapped.json_line_zstd.len() / 1024
    );
    results.push(bench("gzip_transcode(serve path)", iters, || {
        let zstd_bytes = mapped.json_line_zstd.clone();
        timed(|| {
            let raw = zstd::decode_all(std::io::Cursor::new(zstd_bytes.as_ref())).unwrap();
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(1));
            enc.write_all(&raw).unwrap();
            enc.finish().unwrap()
        })
    }));

    if let Ok(out) = std::env::var("BENCH_OUT") {
        std::fs::write(
            &out,
            serde_json::to_string_pretty(&json!({"iters": iters, "results": results}))?,
        )?;
        eprintln!("wrote {out}");
    }

    Ok(())
}
