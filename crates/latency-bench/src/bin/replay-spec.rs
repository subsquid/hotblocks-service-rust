//! Prices the number-addressed speculative replay — the ask that could land
//! inside the cheap window `replay-race` found.
//!
//! That probe showed a hash-addressed `trace_replayBlockTransactions` asked the
//! instant the head tag admits a block returns in ~78 ms, against ~250 ms for
//! the same ask 50 ms later. Landing there in production means asking before
//! the body round trip completes, which means asking by *number*, before the
//! block exists. Two things are unmeasured: the true floor of an ask already in
//! flight when the block appears, and what the error path costs — the answers a
//! replay poll burns before the block exists, and whether early number-asks
//! poison anything (the negative-cache worry `detect-race` killed for
//! `eth_getBlockByNumber`, re-asked for a different code path).
//!
//! A reference leg polls `latest` on a tight grain. Blocks alternate by parity:
//!
//! - **spec-number** — poll `trace_replayBlockTransactions(0x<n>, tracers)` from
//!   the moment `n-1` is known, on its own grain, until a non-empty array comes
//!   back. Error answers are counted and their round trips recorded — that is
//!   the error path's price list. An empty array is treated as not-ready, not
//!   as a win: a node that answers `[]` for an unimported block must not fake
//!   the floor.
//! - **hash-at-0** — yesterday's winner as the in-window control: the
//!   hash-addressed ask issued the instant the reference leg admits the block,
//!   retried on the service's 100 ms ladder.
//!
//! Both arms score `win_at − ref_at`; the spec arm may legitimately go
//! negative — a backend can serve the replay before the backend behind our
//! `latest` poll admits the block. Spec answers are validated against the
//! header once the reference leg has it: entry count against the header's tx
//! count, per-entry `transactionHash` against the header's tx hashes when the
//! node labels entries. A mismatch is the reorg/foreign-answer case the
//! validation design rests on; it is recorded, not repaired.
//!
//! Run it where the service runs, and not beside another probe or a shadow leg.
//!
//! Env: RPC_URL (required), DURATION_S (1800), REF_POLL_MS (25), SPEC_POLL_MS
//! (50), RETRY_MS (100), MAX_ATTEMPTS (30), OUT (replay-spec.json).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::sync::Mutex;

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
    tx_hashes: Vec<String>,
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
        tx_hashes: block["transactions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
    }))
}

/// What one replay answer was: a win, not-yet, or an error worth a class.
enum Answer {
    Win { entries: Vec<Value>, bytes: usize },
    NotReady(String),
}

fn classify(outcome: Result<Value>) -> Answer {
    match outcome {
        Ok(response) => {
            if let Some(e) = response.get("error") {
                return Answer::NotReady(e.to_string());
            }
            match response.get("result") {
                Some(r) if r.is_array() => {
                    let entries = r.as_array().unwrap().clone();
                    if entries.is_empty() {
                        Answer::NotReady("empty array".to_string())
                    } else {
                        let bytes = r.to_string().len();
                        Answer::Win { entries, bytes }
                    }
                }
                Some(Value::Null) => Answer::NotReady("null result".to_string()),
                other => Answer::NotReady(format!("non-array result: {:.60?}", other)),
            }
        }
        Err(e) => Answer::NotReady(format!("{e:#}")),
    }
}

fn entry_tx_hashes(entries: &[Value]) -> Option<Vec<String>> {
    let hashes: Vec<String> = entries
        .iter()
        .filter_map(|e| {
            e.get("transactionHash")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    (hashes.len() == entries.len()).then_some(hashes)
}

#[derive(Default)]
struct Record {
    mode: &'static str,
    ref_at: Option<f64>,
    timestamp_s: u64,
    tx_count: usize,
    header_hashes: Vec<String>,
    poll_started_at: Option<f64>,
    win_issued_at: Option<f64>,
    win_at: Option<f64>,
    win_rtt_ms: Option<f64>,
    /// Spec arm: answers burned before the win, and what the error path cost.
    not_ready_polls: u32,
    err_rtt_sum_ms: f64,
    err_rtt_max_ms: f64,
    first_error: Option<String>,
    /// Gap between the last not-ready answer landing and the win being issued.
    last_gap_ms: Option<f64>,
    result_len: Option<usize>,
    result_bytes: Option<usize>,
    result_tx_hashes: Option<Vec<String>>,
    gave_up: bool,
}

type Ledger = Arc<Mutex<BTreeMap<u64, Record>>>;

/// What an arm's not-ready answers added up to before its win.
#[derive(Default)]
struct Tally {
    not_ready: u32,
    err_rtt_sum_ms: f64,
    err_rtt_max_ms: f64,
    first_error: Option<String>,
}

impl Tally {
    fn note(&mut self, error: String, rtt: f64) {
        self.not_ready += 1;
        self.err_rtt_sum_ms += rtt;
        self.err_rtt_max_ms = self.err_rtt_max_ms.max(rtt);
        self.first_error.get_or_insert(error);
    }
}

struct WinReport {
    mode: &'static str,
    poll_started_at: f64,
    issued_at: f64,
    win_at: f64,
    entries: Vec<Value>,
    bytes: usize,
    tally: Tally,
    last_gap_ms: Option<f64>,
}

async fn record_win(ledger: &Ledger, number: u64, w: WinReport) {
    let mut guard = ledger.lock().await;
    let rec = guard.entry(number).or_default();
    rec.mode = w.mode;
    rec.poll_started_at = Some(w.poll_started_at);
    rec.win_issued_at = Some(w.issued_at);
    rec.win_at = Some(w.win_at);
    rec.win_rtt_ms = Some(w.win_at - w.issued_at);
    rec.not_ready_polls = w.tally.not_ready;
    rec.err_rtt_sum_ms = w.tally.err_rtt_sum_ms;
    rec.err_rtt_max_ms = w.tally.err_rtt_max_ms;
    rec.first_error = w.tally.first_error;
    rec.last_gap_ms = w.last_gap_ms;
    rec.result_len = Some(w.entries.len());
    rec.result_bytes = Some(w.bytes);
    rec.result_tx_hashes = entry_tx_hashes(&w.entries);
}

/// Poll the replay by number from before the block exists until it answers.
async fn acquire_spec(
    client: reqwest::Client,
    url: String,
    number: u64,
    grain: Duration,
    ledger: Ledger,
) {
    let request = rpc_request(
        "trace_replayBlockTransactions",
        json!([format!("0x{number:x}"), TRACERS]),
    );
    let started = now_ms();
    let mut tally = Tally::default();
    let mut last_not_ready_at: Option<f64> = None;

    loop {
        let issued_at = now_ms();
        match classify(call(&client, &url, &request).await) {
            Answer::Win { entries, bytes } => {
                let report = WinReport {
                    mode: "spec-number",
                    poll_started_at: started,
                    issued_at,
                    win_at: now_ms(),
                    entries,
                    bytes,
                    tally,
                    last_gap_ms: last_not_ready_at.map(|t| issued_at - t),
                };
                record_win(&ledger, number, report).await;
                return;
            }
            Answer::NotReady(err) => {
                tally.note(err, now_ms() - issued_at);
                last_not_ready_at = Some(now_ms());
            }
        }
        tokio::time::sleep(grain).await;
    }
}

/// Yesterday's winner, as the same-window control: hash-addressed, issued at
/// the reference stamp, retried on the service's ladder.
async fn acquire_hash_at_0(
    client: reqwest::Client,
    url: String,
    number: u64,
    hash: String,
    retry: Duration,
    max_attempts: u32,
    ledger: Ledger,
) {
    let request = rpc_request("trace_replayBlockTransactions", json!([hash, TRACERS]));
    let started = now_ms();
    let mut tally = Tally::default();

    loop {
        let issued_at = now_ms();
        match classify(call(&client, &url, &request).await) {
            Answer::Win { entries, bytes } => {
                let report = WinReport {
                    mode: "hash-at-0",
                    poll_started_at: started,
                    issued_at,
                    win_at: now_ms(),
                    entries,
                    bytes,
                    tally,
                    last_gap_ms: None,
                };
                record_win(&ledger, number, report).await;
                return;
            }
            Answer::NotReady(err) => {
                tally.note(err, now_ms() - issued_at);
                if tally.not_ready >= max_attempts {
                    let mut guard = ledger.lock().await;
                    let rec = guard.entry(number).or_default();
                    rec.mode = "hash-at-0";
                    rec.not_ready_polls = tally.not_ready;
                    rec.first_error = tally.first_error;
                    rec.gave_up = true;
                    return;
                }
            }
        }
        tokio::time::sleep(retry).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RPC_URL").map_err(|_| anyhow!("RPC_URL is required"))?;
    let duration = Duration::from_secs(env("DURATION_S", 1800));
    let ref_poll = Duration::from_millis(env("REF_POLL_MS", 25));
    let spec_poll = Duration::from_millis(env("SPEC_POLL_MS", 50));
    let retry = Duration::from_millis(env("RETRY_MS", 100));
    let max_attempts: u32 = env("MAX_ATTEMPTS", 30);
    let out = std::env::var("OUT").unwrap_or_else(|_| "replay-spec.json".to_string());

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
        "seeded at block {}; spec grain {} ms, reference grain {} ms",
        seed.number,
        spec_poll.as_millis(),
        ref_poll.as_millis()
    );

    let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));
    let deadline = tokio::time::Instant::now() + duration;
    let mut highest = seed.number;
    let mut ref_errors = 0u32;
    let mut speculating_for: Option<u64> = None;
    let mut tasks = Vec::new();

    while tokio::time::Instant::now() < deadline {
        // Launch the spec arm for the next even block as soon as its
        // predecessor is known — polling starts before the block exists.
        if let Some(next) = highest.checked_add(1) {
            if speculating_for != Some(next) && next % 2 == 0 {
                speculating_for = Some(next);
                tasks.push(tokio::spawn(acquire_spec(
                    client.clone(),
                    url.clone(),
                    next,
                    spec_poll,
                    Arc::clone(&ledger),
                )));
            }
        }

        match call(&client, &url, &latest)
            .await
            .and_then(|r| parse_head(&r))
        {
            Ok(Some(head)) if head.number > highest => {
                let seen_at = now_ms();
                for number in (highest + 1)..=head.number {
                    let single_step = number == head.number && number == highest + 1;
                    let mut guard = ledger.lock().await;
                    let rec = guard.entry(number).or_default();
                    rec.ref_at.get_or_insert(seen_at);
                    if number == head.number {
                        rec.timestamp_s = head.timestamp_s;
                        rec.tx_count = head.tx_hashes.len();
                        rec.header_hashes = head.tx_hashes.clone();
                    }
                    drop(guard);
                    // The hash arm needs the block's own hash, which the ref
                    // poll only carries for the head it reported; passed-over
                    // blocks are dropped from that arm like before.
                    if single_step && number % 2 == 1 {
                        tasks.push(tokio::spawn(acquire_hash_at_0(
                            client.clone(),
                            url.clone(),
                            number,
                            head.hash.clone(),
                            retry,
                            max_attempts,
                            Arc::clone(&ledger),
                        )));
                    }
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
        .filter(|(_, r)| r.ref_at.is_some() && !r.mode.is_empty())
        .map(|(number, r)| {
            let ref_at = r.ref_at.unwrap();
            let hash_match = r.result_tx_hashes.as_ref().map(|h| *h == r.header_hashes);
            json!({
                "number": number,
                "mode": r.mode,
                "timestamp_s": r.timestamp_s,
                "ref_at_ms": r.ref_at,
                "ref_lag_ms": ref_at - (r.timestamp_s as f64) * 1000.0,
                "tx_count": r.tx_count,
                "poll_started_at_ms": r.poll_started_at,
                "win_issued_rel_ms": r.win_issued_at.map(|t| t - ref_at),
                "win_lag_ms": r.win_at.map(|t| t - ref_at),
                "win_rtt_ms": r.win_rtt_ms,
                "not_ready_polls": r.not_ready_polls,
                "err_rtt_avg_ms": (r.not_ready_polls > 0)
                    .then(|| r.err_rtt_sum_ms / r.not_ready_polls as f64),
                "err_rtt_max_ms": (r.not_ready_polls > 0).then_some(r.err_rtt_max_ms),
                "first_error": r.first_error,
                "last_gap_ms": r.last_gap_ms,
                "result_len": r.result_len,
                "result_bytes": r.result_bytes,
                "len_match": r.result_len.map(|l| r.tx_count > 0 && l == r.tx_count),
                "hash_match": hash_match,
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
    println!("\n{} rows, {} reference error(s)", rows.len(), ref_errors);
    for mode in ["spec-number", "hash-at-0"] {
        let arm: Vec<&Value> = rows
            .iter()
            .filter(|r| r["mode"] == mode && r["tx_count"].as_u64().unwrap_or(0) > 0)
            .collect();
        let mut lag: Vec<f64> = arm
            .iter()
            .filter_map(|r| r["win_lag_ms"].as_f64())
            .collect();
        lag.sort_by(f64::total_cmp);
        let polls: Vec<f64> = arm
            .iter()
            .filter_map(|r| r["not_ready_polls"].as_f64())
            .collect();
        let mism = arm
            .iter()
            .filter(|r| r["hash_match"] == false || r["len_match"] == false)
            .count();
        println!(
            "{mode:>12}: n={} win_lag p10 {:.0} p50 {:.0} p90 {:.0} max {:.0}  \
             not-ready/blk p50 {:.0}  mismatches {}",
            lag.len(),
            pct(&lag, 0.10),
            pct(&lag, 0.50),
            pct(&lag, 0.90),
            lag.last().copied().unwrap_or(f64::NAN),
            pct(
                &{
                    let mut p = polls.clone();
                    p.sort_by(f64::total_cmp);
                    p
                },
                0.50
            ),
            mism,
        );
    }

    std::fs::write(&out, serde_json::to_string_pretty(&rows)?)?;
    println!("wrote {} rows to {out}", rows.len());
    Ok(())
}
