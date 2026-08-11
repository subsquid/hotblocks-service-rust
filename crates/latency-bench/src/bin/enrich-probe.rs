//! Times the enrichment pair from outside the service, to separate what the
//! provider costs from what we add.
//!
//! `--profile-block-timings` reports `enrich_ms` as one number covering both
//! legs and everything around them. This issues the same two calls the service
//! issues, concurrently and hash-addressed exactly as it does, starting the
//! moment the header is visible — so the result is the floor any implementation
//! of this pipeline pays at this provider. Our `enrich_ms` minus this is what is
//! ours to fix.
//!
//! Not a substitute for the service's own numbers: it skips verification,
//! normalization and the commit path, and it never retries a not-ready answer.
//! That is the point — it is a lower bound, deliberately.
//!
//! Run it where the service runs, and alone: at this grain it and any other
//! probe would measure each other.
//!
//! Env: RPC_URL (required), DURATION_S (300), POLL_MS (25), OUT
//! (enrich-probe.json).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

/// What the service asks for when traces and state diffs are both on and the
/// trace API is in use (`fetch.rs`, `fetch_trace_replays`).
const TRACERS: [&str; 2] = ["trace", "stateDiff"];

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn qty(v: &Value) -> Option<u64> {
    u64::from_str_radix(v.as_str()?.strip_prefix("0x")?, 16).ok()
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or_default()
}

fn rpc_request(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
}

struct Header {
    number: u64,
    timestamp_s: u64,
    hash: String,
    tx_count: usize,
}

fn parse_header(response: &Value) -> Result<Option<Header>> {
    if let Some(error) = response.get("error") {
        return Err(anyhow!("json-rpc error response: {error}"));
    }
    let block = response
        .get("result")
        .ok_or_else(|| anyhow!("json-rpc response is missing result"))?;
    if block.is_null() {
        return Ok(None);
    }
    Ok(Some(Header {
        number: qty(&block["number"]).ok_or_else(|| anyhow!("block has an invalid number"))?,
        timestamp_s: qty(&block["timestamp"])
            .ok_or_else(|| anyhow!("block has an invalid timestamp"))?,
        hash: block["hash"]
            .as_str()
            .ok_or_else(|| anyhow!("block has no hash"))?
            .to_string(),
        tx_count: block["transactions"].as_array().map_or(0, Vec::len),
    }))
}

async fn call(client: &reqwest::Client, url: &str, body: &Value) -> Result<Value> {
    Ok(client
        .post(url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("calling {}", body["method"]))?
        .error_for_status()?
        .json()
        .await?)
}

/// One leg's cost and the size of what it returned — payload is the reason to
/// suspect a leg, so it is worth carrying alongside the time.
async fn timed_leg(client: &reqwest::Client, url: &str, body: Value) -> (f64, usize, bool) {
    let started = now_ms();
    match call(client, url, &body).await {
        Ok(v) => {
            let bytes = serde_json::to_vec(&v).map_or(0, |b| b.len());
            let empty = v.get("result").is_none_or(Value::is_null);
            (now_ms() - started, bytes, !empty)
        }
        Err(_) => (now_ms() - started, 0, false),
    }
}

struct Sample {
    number: u64,
    timestamp_s: u64,
    header_lag_ms: f64,
    receipts_ms: f64,
    replay_ms: f64,
    enrich_ms: f64,
    receipts_bytes: usize,
    replay_bytes: usize,
    tx_count: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RPC_URL").map_err(|_| anyhow!("RPC_URL is required"))?;
    let duration = Duration::from_secs(env("DURATION_S", 300));
    let poll = Duration::from_millis(env("POLL_MS", 25));
    let out = std::env::var("OUT").unwrap_or_else(|_| "enrich-probe.json".to_string());

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(30))
        .build()?;

    let seed = parse_header(
        &call(
            &client,
            &url,
            &rpc_request("eth_getBlockByNumber", json!(["latest", false])),
        )
        .await?,
    )?
    .ok_or_else(|| anyhow!("latest block is unavailable"))?;
    let mut next = seed.number.saturating_add(1);
    println!("seeded at block {}; tracers {TRACERS:?}", seed.number);

    let deadline = tokio::time::Instant::now() + duration;
    let mut samples: Vec<Sample> = Vec::new();
    let mut errors = 0u32;

    while tokio::time::Instant::now() < deadline {
        let tag = format!("0x{next:x}");
        let header = match call(
            &client,
            &url,
            &rpc_request("eth_getBlockByNumber", json!([tag, false])),
        )
        .await
        .and_then(|r| parse_header(&r))
        {
            Ok(Some(h)) => h,
            Ok(None) => {
                tokio::time::sleep(poll).await;
                continue;
            }
            Err(_) => {
                errors = errors.saturating_add(1);
                tokio::time::sleep(poll).await;
                continue;
            }
        };
        let header_at = now_ms();
        let timestamp_ms = Duration::from_secs(header.timestamp_s).as_secs_f64() * 1000.0;
        if header.tx_count == 0 {
            next = next.saturating_add(1);
            continue;
        }

        // Concurrently and hash-addressed for the replay, as the service does.
        // Enrichment finishes when the slower leg does, so the max is what the
        // pipeline actually waits for.
        let (receipts, replay) = tokio::join!(
            timed_leg(
                &client,
                &url,
                rpc_request("eth_getBlockReceipts", json!([tag]))
            ),
            timed_leg(
                &client,
                &url,
                rpc_request(
                    "trace_replayBlockTransactions",
                    json!([header.hash, TRACERS])
                )
            ),
        );

        if !receipts.2 || !replay.2 {
            errors = errors.saturating_add(1);
        }
        samples.push(Sample {
            number: header.number,
            timestamp_s: header.timestamp_s,
            header_lag_ms: header_at - timestamp_ms,
            receipts_ms: receipts.0,
            replay_ms: replay.0,
            enrich_ms: receipts.0.max(replay.0),
            receipts_bytes: receipts.1,
            replay_bytes: replay.1,
            tx_count: header.tx_count,
        });
        next = next.saturating_add(1);
    }

    if samples.is_empty() {
        return Err(anyhow!("collected no samples with {errors} error(s)"));
    }

    let pct = |mut v: Vec<f64>, q: f64| {
        v.sort_by(f64::total_cmp);
        v[(((v.len() - 1) as f64) * q).round() as usize]
    };
    let col = |f: fn(&Sample) -> f64| samples.iter().map(f).collect::<Vec<_>>();

    println!("{} non-empty blocks, {errors} incomplete", samples.len());
    for (name, values) in [
        ("receipts", col(|s| s.receipts_ms)),
        ("replay", col(|s| s.replay_ms)),
        ("enrichment (the slower leg)", col(|s| s.enrich_ms)),
    ] {
        println!(
            "  {name:<28} p50 {:6.0}  p90 {:6.0}  p99 {:6.0}  max {:6.0} ms",
            pct(values.clone(), 0.50),
            pct(values.clone(), 0.90),
            pct(values.clone(), 0.99),
            pct(values, 1.0),
        );
    }
    let replay_slower = samples
        .iter()
        .filter(|s| s.replay_ms > s.receipts_ms)
        .count();
    println!(
        "  replay was the slower leg on {replay_slower}/{} blocks ({:.0}%)",
        samples.len(),
        100.0 * replay_slower as f64 / samples.len() as f64,
    );
    println!(
        "  response size p50: receipts {:.0} kB, replay {:.0} kB",
        pct(col(|s| s.receipts_bytes as f64), 0.50) / 1024.0,
        pct(col(|s| s.replay_bytes as f64), 0.50) / 1024.0,
    );

    let rows: Vec<Value> = samples
        .iter()
        .map(|s| {
            json!({
                "block": s.number,
                "ts": s.timestamp_s,
                "header_lag_ms": s.header_lag_ms,
                "receipts_ms": s.receipts_ms,
                "replay_ms": s.replay_ms,
                "enrich_ms": s.enrich_ms,
                "receipts_bytes": s.receipts_bytes,
                "replay_bytes": s.replay_bytes,
                "txs": s.tx_count,
            })
        })
        .collect();
    std::fs::write(&out, serde_json::to_vec(&rows)?).with_context(|| format!("writing {out}"))?;
    println!("wrote {out}");
    Ok(())
}
