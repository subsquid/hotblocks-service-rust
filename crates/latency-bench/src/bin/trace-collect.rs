//! Collects a real block-arrival trace for `cadence` to replay.
//!
//! Polls `eth_getBlockByNumber("latest")` on a tight grain and records, per new
//! head, the block's own timestamp and how long after it the answer was in hand.
//! Output is `[[block_ts_s, arrival_lag_ms], ...]`, which `cadence` replays
//! directly instead of synthesising arrivals from `PROP_MS` + `JITTER_MS`.
//!
//! Run it where the service runs: arrival lag is a property of the provider
//! that pod reaches, and a laptop measures a different one.
//!
//! Env: RPC_URL (required), DURATION_S (300), POLL_MS (25), OUT (trace.json).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

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

async fn head(client: &reqwest::Client, url: &str, body: &Value) -> Result<Option<(u64, u64)>> {
    let res: Value = client
        .post(url)
        .json(body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let Some(block) = res.get("result").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    Ok(qty(&block["number"]).zip(qty(&block["timestamp"])))
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RPC_URL").map_err(|_| anyhow!("RPC_URL is required"))?;
    let duration = Duration::from_secs(env("DURATION_S", 300));
    let poll = Duration::from_millis(env("POLL_MS", 25));
    let out = std::env::var("OUT").unwrap_or_else(|_| "trace.json".to_string());

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(10))
        .build()?;
    let body = json!({
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_getBlockByNumber", "params": ["latest", false],
    });

    let deadline = tokio::time::Instant::now() + duration;
    let mut samples: Vec<(f64, f64)> = Vec::new();
    let (mut last, mut errors) = (None, 0u32);

    while tokio::time::Instant::now() < deadline {
        match head(&client, &url, &body).await {
            Ok(Some((number, ts_s))) if last.is_none_or(|l| number > l) => {
                let ts_ms = ts_s as f64 * 1000.0;
                samples.push((ts_s as f64, now_ms() - ts_ms));
                last = Some(number);
            }
            Ok(_) => {}
            Err(_) => errors += 1,
        }
        tokio::time::sleep(poll).await;
    }

    if samples.len() < 2 {
        return Err(anyhow!(
            "collected {} sample(s) with {errors} error(s) — nothing to replay",
            samples.len()
        ));
    }

    let mut lags: Vec<f64> = samples.iter().map(|s| s.1).collect();
    lags.sort_by(f64::total_cmp);
    let pct = |q: f64| lags[(((lags.len() - 1) as f64) * q).round() as usize];
    println!(
        "{} blocks, {errors} errors\narrival lag  min {:.0}  p50 {:.0}  p90 {:.0}  max {:.0} ms\n\
         spread above the floor: p90 {:.0} ms — compare with the hot window before trusting a \
         synthetic JITTER_MS",
        samples.len(),
        lags[0],
        pct(0.50),
        pct(0.90),
        lags[lags.len() - 1],
        pct(0.90) - lags[0],
    );

    let rows: Vec<Value> = samples.iter().map(|(t, l)| json!([t, l])).collect();
    std::fs::write(&out, serde_json::to_vec(&rows)?).with_context(|| format!("writing {out}"))?;
    println!("wrote {out}");
    Ok(())
}
