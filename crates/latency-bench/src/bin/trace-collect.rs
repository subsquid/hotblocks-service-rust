//! Collects a real block-arrival trace for `cadence` to replay.
//!
//! Seeds from `eth_getBlockByNumber("latest", false)`, then follows the production
//! head path exactly: request each numbered successor with full transactions,
//! retrying null responses on a tight grain. Output is
//! `[[block_number, block_ts_s, arrival_lag_ms], ...]`, which `cadence` validates
//! and replays instead of synthesising arrivals from `PROP_MS` + `JITTER_MS`.
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

fn block_request(tag: &str, with_transactions: bool) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [tag, with_transactions],
    })
}

fn parse_block_response(response: &Value) -> Result<Option<(u64, u64)>> {
    if let Some(error) = response.get("error") {
        return Err(anyhow!("json-rpc error response: {error}"));
    }
    let block = response
        .get("result")
        .ok_or_else(|| anyhow!("json-rpc response is missing result"))?;
    if block.is_null() {
        return Ok(None);
    }
    let number =
        qty(&block["number"]).ok_or_else(|| anyhow!("block response has an invalid number"))?;
    let timestamp = qty(&block["timestamp"])
        .ok_or_else(|| anyhow!("block {number} response has an invalid timestamp"))?;
    Ok(Some((number, timestamp)))
}

fn validate_expected_height(expected: u64, actual: u64) -> Result<()> {
    if actual != expected {
        return Err(anyhow!(
            "requested block {expected}, upstream returned block {actual}"
        ));
    }
    Ok(())
}

async fn block(
    client: &reqwest::Client,
    url: &str,
    tag: &str,
    with_transactions: bool,
) -> Result<Option<(u64, u64)>> {
    let body = block_request(tag, with_transactions);
    let res: Value = client
        .post(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("requesting block {tag}"))?
        .error_for_status()?
        .json()
        .await?;
    parse_block_response(&res)
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    number: u64,
    timestamp_s: u64,
    arrival_lag_ms: f64,
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

    let (seed, _) = block(&client, &url, "latest", false)
        .await?
        .ok_or_else(|| anyhow!("latest block is unavailable"))?;
    let mut next = seed
        .checked_add(1)
        .ok_or_else(|| anyhow!("latest block number cannot be incremented"))?;
    println!(
        "seeded at block {seed}; collecting exact numbered blocks with transactions, poll {} ms",
        poll.as_millis()
    );

    let deadline = tokio::time::Instant::now() + duration;
    let mut samples: Vec<Sample> = Vec::new();
    let mut errors = 0u32;

    while tokio::time::Instant::now() < deadline {
        let tag = format!("0x{next:x}");
        match block(&client, &url, &tag, true).await {
            Ok(Some((number, timestamp_s))) => {
                validate_expected_height(next, number)?;
                let timestamp_ms = Duration::from_secs(timestamp_s).as_secs_f64() * 1000.0;
                samples.push(Sample {
                    number,
                    timestamp_s,
                    arrival_lag_ms: now_ms() - timestamp_ms,
                });
                next = next
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("collected block number cannot be incremented"))?;
            }
            Ok(None) => tokio::time::sleep(poll).await,
            Err(_) => {
                errors = errors.saturating_add(1);
                tokio::time::sleep(poll).await;
            }
        }
    }

    if samples.len() < 2 {
        return Err(anyhow!(
            "collected {} sample(s) with {errors} error(s) — nothing to replay",
            samples.len()
        ));
    }

    let mut lags: Vec<f64> = samples.iter().map(|s| s.arrival_lag_ms).collect();
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

    let rows: Vec<Value> = samples
        .iter()
        .map(|sample| json!([sample.number, sample.timestamp_s, sample.arrival_lag_ms]))
        .collect();
    std::fs::write(&out, serde_json::to_vec(&rows)?).with_context(|| format!("writing {out}"))?;
    println!("wrote {out}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_block_request_uses_number_and_full_transactions() {
        let request = block_request("0x2a", true);

        assert_eq!(request["method"], "eth_getBlockByNumber");
        assert_eq!(request["params"], json!(["0x2a", true]));
    }

    #[test]
    fn response_parser_returns_block_identity() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"number": "0x2a", "timestamp": "0x65"},
        });

        let block = parse_block_response(&response).expect("valid response should parse");

        assert_eq!(block, Some((42, 101)));
    }

    #[test]
    fn response_parser_rejects_json_rpc_errors() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32000, "message": "failed"},
        });

        let error = parse_block_response(&response)
            .expect_err("json-rpc error response must not look like a null block");

        assert!(error.to_string().contains("json-rpc error response"));
    }

    #[test]
    fn exact_height_validation_rejects_a_jump() {
        let error = validate_expected_height(42, 44)
            .expect_err("a numbered poll must return the requested height");

        assert_eq!(
            error.to_string(),
            "requested block 42, upstream returned block 44"
        );
    }
}
