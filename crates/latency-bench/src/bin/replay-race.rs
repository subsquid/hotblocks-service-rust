//! Prices `trace_replayBlockTransactions` against how early it is asked.
//!
//! The one asymmetry left between the two implementations is *when* they issue
//! the identical replay: we detect ~70 ms sooner and ask ~70 ms sooner. If a
//! node computes a replay more slowly the earlier it is asked — state still
//! settling, caches cold, the block still importing on some fleet members —
//! then our detection advantage buys an enrichment penalty. This probe measures
//! that curve directly.
//!
//! A reference leg polls `latest` on a tight grain and stamps the earliest
//! moment each block is known to exist. Each new block is then given exactly
//! one replay ask — the service's own call shape, hash-addressed with
//! `["trace","stateDiff"]` — at an offset from that stamp chosen by block
//! number modulo the offset table, so the arms interleave through identical
//! chain conditions and never contend for the same answer. A JSON-RPC error is
//! retried on the same 100 ms ladder the service pays, and every attempt is
//! recorded: the first attempt's fate prices the not-ready incidence per
//! offset, the total prices what the service would experience.
//!
//! Empty blocks are recorded but carry no replay: nothing is replayed, so the
//! answer is trivially cheap and would pile a spike on zero.
//!
//! Blocks the head tag jumps over have no trustworthy first-seen stamp and are
//! dropped rather than scored against a late anchor.
//!
//! Run it where the service runs, and not beside another probe or a shadow leg.
//!
//! Env: RPC_URL (required), DURATION_S (2400), REF_POLL_MS (25), OFFSETS_MS
//! ("0,50,100,200"), RETRY_MS (100), MAX_ATTEMPTS (30), OUT (replay-race.json).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::sync::Mutex;

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

struct Head {
    number: u64,
    hash: String,
    timestamp_s: u64,
    tx_count: usize,
}

fn parse_head(response: &Value) -> Result<Option<Head>> {
    if let Some(error) = response.get("error") {
        return Err(anyhow!("json-rpc error response: {error}"));
    }
    let block = response
        .get("result")
        .ok_or_else(|| anyhow!("json-rpc response is missing result"))?;
    if block.is_null() {
        return Ok(None);
    }
    Ok(Some(Head {
        number: qty(&block["number"]).ok_or_else(|| anyhow!("block has an invalid number"))?,
        hash: block["hash"]
            .as_str()
            .ok_or_else(|| anyhow!("block has an invalid hash"))?
            .to_string(),
        timestamp_s: qty(&block["timestamp"])
            .ok_or_else(|| anyhow!("block has an invalid timestamp"))?,
        tx_count: block["transactions"].as_array().map_or(0, Vec::len),
    }))
}

#[derive(Default)]
struct Record {
    ref_at: Option<f64>,
    timestamp_s: u64,
    tx_count: usize,
    offset_ms: u64,
    /// First attempt: wall time and how it ended — the not-ready incidence.
    first_rtt_ms: Option<f64>,
    first_error: Option<String>,
    /// Ask start (post-offset) to a good answer in hand, ladder included.
    total_ms: Option<f64>,
    /// The successful call's own round trip.
    replay_rtt_ms: Option<f64>,
    attempts: u32,
    result_bytes: usize,
    gave_up: bool,
}

type Ledger = Arc<Mutex<BTreeMap<u64, Record>>>;

/// One replay ask, issued when called — the caller sleeps out the arm's offset.
/// JSON-RPC errors retry on the service's own 100 ms ladder. The replay result
/// for a present block is a non-null array; anything else is an error worth the
/// retry.
async fn price_replay(
    client: reqwest::Client,
    url: String,
    number: u64,
    hash: String,
    retry: Duration,
    max_attempts: u32,
    ledger: Ledger,
) {
    let request = rpc_request(
        "trace_replayBlockTransactions",
        json!([hash, ["trace", "stateDiff"]]),
    );
    let started = now_ms();
    let mut attempts = 0u32;
    let mut first_rtt = None;
    let mut first_error: Option<String> = None;

    loop {
        attempts += 1;
        let issued_at = now_ms();
        let outcome = call(&client, &url, &request).await;
        let rtt = now_ms() - issued_at;
        if first_rtt.is_none() {
            first_rtt = Some(rtt);
        }
        let error = match outcome {
            Ok(response) => match response.get("error") {
                Some(e) => Some(e.to_string()),
                None => match response.get("result") {
                    Some(r) if r.is_array() => {
                        let done = now_ms();
                        let mut ledger = ledger.lock().await;
                        let rec = ledger.entry(number).or_default();
                        rec.first_rtt_ms = first_rtt;
                        rec.first_error = first_error;
                        rec.total_ms = Some(done - started);
                        rec.replay_rtt_ms = Some(rtt);
                        rec.attempts = attempts;
                        rec.result_bytes = r.to_string().len();
                        return;
                    }
                    other => Some(format!("non-array result: {:.60?}", other)),
                },
            },
            Err(e) => Some(format!("{e:#}")),
        };
        if first_error.is_none() {
            first_error = error;
        }
        if attempts >= max_attempts {
            let mut ledger = ledger.lock().await;
            let rec = ledger.entry(number).or_default();
            rec.first_rtt_ms = first_rtt;
            rec.first_error = first_error;
            rec.attempts = attempts;
            rec.gave_up = true;
            return;
        }
        tokio::time::sleep(retry).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RPC_URL").map_err(|_| anyhow!("RPC_URL is required"))?;
    let duration = Duration::from_secs(env("DURATION_S", 2400));
    let ref_poll = Duration::from_millis(env("REF_POLL_MS", 25));
    let retry = Duration::from_millis(env("RETRY_MS", 100));
    let max_attempts: u32 = env("MAX_ATTEMPTS", 30);
    let out = std::env::var("OUT").unwrap_or_else(|_| "replay-race.json".to_string());
    let offsets: Vec<u64> = std::env::var("OFFSETS_MS")
        .unwrap_or_else(|_| "0,50,100,200".to_string())
        .split(',')
        .map(|s| s.trim().parse().context("OFFSETS_MS must be integers"))
        .collect::<Result<_>>()?;
    if offsets.is_empty() {
        return Err(anyhow!("OFFSETS_MS must name at least one offset"));
    }

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(30))
        .build()?;

    let latest = rpc_request("eth_getBlockByNumber", json!(["latest", false]));
    let seed = {
        let mut attempt = 0;
        loop {
            match call(&client, &url, &latest)
                .await
                .and_then(|r| parse_head(&r))
            {
                Ok(Some(head)) => break head,
                other => {
                    attempt += 1;
                    if attempt >= 10 {
                        return Err(match other {
                            Err(e) => e.context("seeding failed"),
                            _ => anyhow!("latest block is unavailable after {attempt} attempts"),
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    };
    println!(
        "seeded at block {}; offsets {:?} ms by number % {}, reference grain {} ms",
        seed.number,
        offsets,
        offsets.len(),
        ref_poll.as_millis()
    );

    let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));
    let deadline = tokio::time::Instant::now() + duration;
    let mut highest = seed.number;
    let mut ref_errors = 0u32;
    let mut skipped_over = 0u32;
    let mut tasks = Vec::new();

    while tokio::time::Instant::now() < deadline {
        match call(&client, &url, &latest)
            .await
            .and_then(|r| parse_head(&r))
        {
            Ok(Some(head)) if head.number > highest => {
                let seen_at = now_ms();
                // Only a single-step advance gives a trustworthy first-seen
                // stamp; passed-over blocks are dropped, not scored late.
                skipped_over += (head.number - highest - 1) as u32;
                let offset = offsets[(head.number % offsets.len() as u64) as usize];
                {
                    let mut guard = ledger.lock().await;
                    let rec = guard.entry(head.number).or_default();
                    rec.ref_at = Some(seen_at);
                    rec.timestamp_s = head.timestamp_s;
                    rec.tx_count = head.tx_count;
                    rec.offset_ms = offset;
                }
                if head.tx_count > 0 {
                    let (client, url, ledger) = (client.clone(), url.clone(), Arc::clone(&ledger));
                    let hash = head.hash.clone();
                    tasks.push(tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(offset)).await;
                        price_replay(client, url, head.number, hash, retry, max_attempts, ledger)
                            .await;
                    }));
                }
                highest = head.number;
            }
            Ok(_) => {}
            Err(_) => ref_errors = ref_errors.saturating_add(1),
        }
        tokio::time::sleep(ref_poll).await;
    }

    for task in &tasks {
        if !task.is_finished() {
            task.abort();
        }
    }

    let ledger = ledger.lock().await;
    let rows: Vec<Value> = ledger
        .iter()
        .filter(|(_, r)| r.ref_at.is_some())
        .map(|(number, r)| {
            json!({
                "number": number,
                "timestamp_s": r.timestamp_s,
                "ref_at_ms": r.ref_at,
                "ref_lag_ms": r.ref_at.unwrap() - (r.timestamp_s as f64) * 1000.0,
                "offset_ms": r.offset_ms,
                "tx_count": r.tx_count,
                "first_rtt_ms": r.first_rtt_ms,
                "first_error": r.first_error,
                "total_ms": r.total_ms,
                "replay_rtt_ms": r.replay_rtt_ms,
                "attempts": r.attempts,
                "result_bytes": r.result_bytes,
                "gave_up": r.gave_up,
            })
        })
        .collect();

    if rows.is_empty() {
        return Err(anyhow!(
            "collected no rows with {ref_errors} reference error(s)"
        ));
    }

    let pct = |v: &[f64], q: f64| -> f64 {
        if v.is_empty() {
            return f64::NAN;
        }
        v[(((v.len() - 1) as f64) * q).round() as usize]
    };
    println!(
        "\n{} blocks, {} skipped over by the tag, {} reference error(s)",
        rows.len(),
        skipped_over,
        ref_errors
    );
    println!("offset   n  err1st   p50 rtt   p90 rtt   p50 tot   p90 tot  gave-up");
    for &offset in &offsets {
        let arm: Vec<&Value> = rows
            .iter()
            .filter(|r| r["offset_ms"] == offset && r["tx_count"].as_u64() != Some(0))
            .collect();
        let done: Vec<&&Value> = arm.iter().filter(|r| r["total_ms"].is_number()).collect();
        let mut rtt: Vec<f64> = done
            .iter()
            .filter_map(|r| r["replay_rtt_ms"].as_f64())
            .collect();
        let mut tot: Vec<f64> = done.iter().filter_map(|r| r["total_ms"].as_f64()).collect();
        rtt.sort_by(f64::total_cmp);
        tot.sort_by(f64::total_cmp);
        let errs = arm.iter().filter(|r| !r["first_error"].is_null()).count();
        let gave_up = arm.iter().filter(|r| r["gave_up"] == true).count();
        println!(
            "{offset:>4}  {:>4}  {errs:>5}  {:>8.0}  {:>8.0}  {:>8.0}  {:>8.0}  {gave_up:>7}",
            arm.len(),
            pct(&rtt, 0.50),
            pct(&rtt, 0.90),
            pct(&tot, 0.50),
            pct(&tot, 0.90),
        );
    }

    std::fs::write(&out, serde_json::to_string_pretty(&rows)?)?;
    println!("wrote {} rows to {out}", rows.len());
    Ok(())
}
