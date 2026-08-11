//! Races the two head-access patterns — speculative and tag-confirmed — against
//! the same endpoint, on the same blocks, in one process.
//!
//! Both implementations fetch a head block with the identical call,
//! `eth_getBlockByNumber(0x<n>, true)`. What differs is *when* it is issued. We
//! ask for `n` before `n` exists and keep asking on a tight grain; the
//! predecessor never names `n` until a `latest` poll has already reported it,
//! then asks once. Every detection figure in the investigation was measured
//! either as the spacing between our own polls or by replaying our schedule
//! against a `latest` timeline, so both are blind to a provider that keeps
//! answering `null` for a while after the block exists. This probe is not.
//!
//! A reference leg polls `latest` on a tight grain and stamps, per block, the
//! earliest moment the chain is known to have reached it. Each new block is then
//! acquired by one of the two patterns and scored as `body_lag_ms`, the delay
//! from that stamp to the body being in hand. The patterns **alternate by block
//! number** rather than running side by side: they would otherwise contend for
//! the same cache entry, and a provider that serves a stale `null` would serve
//! it to both legs, hiding the very effect being measured. Alternating keeps the
//! comparison inside one window while giving each block a private key.
//!
//! `poll_gap_ms` mirrors what `head_detection_gap_ms` computes — the interval
//! between the last empty poll and the one that hit. It is reported next to
//! `body_lag_ms` for the speculative blocks because the distance between the two
//! is the size of the service's blind spot.
//!
//! The tag-confirmed leg is triggered off the reference leg's tight grain, not
//! off a 100 ms tick, so it measures the provider alone. The predecessor
//! additionally pays a uniform 0–`TS_TICK_MS` wait for its own tick; that is
//! added back in the summary rather than measured, to keep the two legs
//! differing in exactly one thing.
//!
//! Run it where the service runs, and not beside another probe or a shadow leg:
//! the reference and speculative legs together push ~60 requests a second,
//! several times the feed's own.
//!
//! Env: RPC_URL (required), DURATION_S (900), REF_POLL_MS (25), SPEC_POLL_MS
//! (25), MODE (alternate | spec | tagged), TS_TICK_MS (100), OUT
//! (detect-race.json).

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

/// A non-null `eth_getBlockByNumber` answer, reduced to what the scoring needs.
struct Body {
    number: u64,
    timestamp_s: u64,
    tx_count: usize,
    bytes: usize,
}

/// `Ok(None)` is "not produced yet" — the answer the speculative pattern spends
/// its polls on. A JSON-RPC error is not that and must not be counted as one.
fn parse_body(response: &Value) -> Result<Option<Body>> {
    if let Some(error) = response.get("error") {
        return Err(anyhow!("json-rpc error response: {error}"));
    }
    let block = response
        .get("result")
        .ok_or_else(|| anyhow!("json-rpc response is missing result"))?;
    if block.is_null() {
        return Ok(None);
    }
    Ok(Some(Body {
        number: qty(&block["number"]).ok_or_else(|| anyhow!("block has an invalid number"))?,
        timestamp_s: qty(&block["timestamp"])
            .ok_or_else(|| anyhow!("block has an invalid timestamp"))?,
        tx_count: block["transactions"].as_array().map_or(0, Vec::len),
        bytes: block.to_string().len(),
    }))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Poll `n` by number from before it exists, as the service does.
    Spec,
    /// Ask once, after a `latest` poll has already reported `n`.
    Tagged,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Spec => "spec",
            Mode::Tagged => "tagged",
        }
    }
}

#[derive(Default)]
struct Record {
    /// Earliest moment the reference leg saw the chain reach this block.
    ref_at: Option<f64>,
    timestamp_s: Option<u64>,
    mode: Option<Mode>,
    body_at: Option<f64>,
    /// Wall time of the call that returned the body, round trip included.
    body_rtt_ms: Option<f64>,
    /// Speculative only: null answers spent before the hit.
    null_polls: u32,
    /// Speculative only: last empty poll → the poll that hit, which is what
    /// `head_detection_gap_ms` records.
    poll_gap_ms: Option<f64>,
    tx_count: Option<usize>,
    bytes: Option<usize>,
    errors: u32,
}

type Ledger = Arc<Mutex<BTreeMap<u64, Record>>>;

/// Poll `number` by number until it answers with a body, starting before the
/// block exists. Mirrors the service's speculative loop, including that a
/// JSON-RPC error is retried rather than counted as "not produced yet".
async fn acquire_speculative(
    client: reqwest::Client,
    url: String,
    number: u64,
    grain: Duration,
    ledger: Ledger,
) {
    let tag = format!("0x{number:x}");
    let request = rpc_request("eth_getBlockByNumber", json!([tag, true]));
    let mut null_polls = 0u32;
    let mut errors = 0u32;
    let mut last_empty_at: Option<f64> = None;

    loop {
        let issued_at = now_ms();
        match call(&client, &url, &request)
            .await
            .and_then(|r| parse_body(&r))
        {
            Ok(Some(body)) => {
                let body_at = now_ms();
                let mut ledger = ledger.lock().await;
                let record = ledger.entry(number).or_default();
                record.mode = Some(Mode::Spec);
                record.body_at = Some(body_at);
                record.body_rtt_ms = Some(body_at - issued_at);
                record.null_polls = null_polls;
                record.poll_gap_ms = last_empty_at.map(|empty| issued_at - empty);
                record.tx_count = Some(body.tx_count);
                record.bytes = Some(body.bytes);
                record.timestamp_s.get_or_insert(body.timestamp_s);
                record.errors += errors;
                return;
            }
            Ok(None) => {
                null_polls = null_polls.saturating_add(1);
                last_empty_at = Some(now_ms());
            }
            Err(_) => errors = errors.saturating_add(1),
        }
        tokio::time::sleep(grain).await;
    }
}

/// Ask once, the way the predecessor does after its `latest` poll has moved.
/// A null answer here is the provider lagging its own head tag; retry on the
/// same 100 ms ladder both implementations use, counting the polls so those
/// blocks stay identifiable.
async fn acquire_tagged(client: reqwest::Client, url: String, number: u64, ledger: Ledger) {
    let tag = format!("0x{number:x}");
    let request = rpc_request("eth_getBlockByNumber", json!([tag, true]));
    let mut null_polls = 0u32;
    let mut errors = 0u32;

    loop {
        let issued_at = now_ms();
        match call(&client, &url, &request)
            .await
            .and_then(|r| parse_body(&r))
        {
            Ok(Some(body)) => {
                let body_at = now_ms();
                let mut ledger = ledger.lock().await;
                let record = ledger.entry(number).or_default();
                record.mode = Some(Mode::Tagged);
                record.body_at = Some(body_at);
                record.body_rtt_ms = Some(body_at - issued_at);
                record.null_polls = null_polls;
                record.tx_count = Some(body.tx_count);
                record.bytes = Some(body.bytes);
                record.timestamp_s.get_or_insert(body.timestamp_s);
                record.errors += errors;
                return;
            }
            Ok(None) => null_polls = null_polls.saturating_add(1),
            Err(_) => errors = errors.saturating_add(1),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RPC_URL").map_err(|_| anyhow!("RPC_URL is required"))?;
    let duration = Duration::from_secs(env("DURATION_S", 900));
    let ref_poll = Duration::from_millis(env("REF_POLL_MS", 25));
    let spec_poll = Duration::from_millis(env("SPEC_POLL_MS", 25));
    let ts_tick_ms: f64 = env("TS_TICK_MS", 100.0);
    let out = std::env::var("OUT").unwrap_or_else(|_| "detect-race.json".to_string());
    let mode_env = std::env::var("MODE").unwrap_or_else(|_| "alternate".to_string());

    let assign: Box<dyn Fn(u64) -> Mode> = match mode_env.as_str() {
        "alternate" => Box::new(|n| if n % 2 == 0 { Mode::Spec } else { Mode::Tagged }),
        "spec" => Box::new(|_| Mode::Spec),
        "tagged" => Box::new(|_| Mode::Tagged),
        other => {
            return Err(anyhow!(
                "MODE must be alternate, spec or tagged, got {other}"
            ))
        }
    };

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(10))
        .build()?;

    // A rate-limited or briefly unhealthy endpoint must not cost the whole run
    // before it starts; the legs below tolerate errors, so the seed should too.
    let seed = {
        let request = rpc_request("eth_getBlockByNumber", json!(["latest", false]));
        let mut attempt = 0;
        loop {
            match call(&client, &url, &request)
                .await
                .and_then(|r| parse_body(&r))
            {
                Ok(Some(body)) => break body,
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
        "seeded at block {}; mode {mode_env}, reference grain {} ms, speculative grain {} ms",
        seed.number,
        ref_poll.as_millis(),
        spec_poll.as_millis()
    );

    let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));
    let latest = rpc_request("eth_getBlockByNumber", json!(["latest", false]));
    let deadline = tokio::time::Instant::now() + duration;
    let mut highest = seed.number;
    let mut ref_errors = 0u32;
    let mut tasks = Vec::new();

    // The speculative pattern has to be polling before the block exists, so it
    // is launched for `highest + 1` as soon as `highest` is known — which is
    // where the service launches it too, one block behind the head.
    let mut speculating_for: Option<u64> = None;

    while tokio::time::Instant::now() < deadline {
        if let Some(next) = highest.checked_add(1) {
            if speculating_for != Some(next) && assign(next) == Mode::Spec {
                speculating_for = Some(next);
                tasks.push(tokio::spawn(acquire_speculative(
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
            .and_then(|r| parse_body(&r))
        {
            Ok(Some(head)) if head.number > highest => {
                let seen_at = now_ms();
                // The tag can jump more than one block; every block it passed
                // over is known to exist by now, and none of them can be scored
                // as speculative because nothing was polling for them.
                for number in (highest + 1)..=head.number {
                    let mut ledger_guard = ledger.lock().await;
                    let record = ledger_guard.entry(number).or_default();
                    record.ref_at.get_or_insert(seen_at);
                    if number == head.number {
                        record.timestamp_s.get_or_insert(head.timestamp_s);
                    }
                    let already_speculative = speculating_for == Some(number);
                    drop(ledger_guard);

                    if !already_speculative && assign(number) == Mode::Tagged {
                        tasks.push(tokio::spawn(acquire_tagged(
                            client.clone(),
                            url.clone(),
                            number,
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

    // A speculative task for a block that never arrived would hang; the ledger
    // drops incomplete records anyway, so abandon rather than await them.
    for task in &tasks {
        if !task.is_finished() {
            task.abort();
        }
    }

    let ledger = ledger.lock().await;
    let rows: Vec<(u64, &Record)> = ledger
        .iter()
        .filter(|(_, r)| r.ref_at.is_some() && r.body_at.is_some() && r.mode.is_some())
        .map(|(n, r)| (*n, r))
        .collect();

    if rows.is_empty() {
        return Err(anyhow!(
            "collected no complete rows with {ref_errors} reference error(s)"
        ));
    }

    let lag = |mode: Mode| -> Vec<f64> {
        let mut v: Vec<f64> = rows
            .iter()
            .filter(|(_, r)| r.mode == Some(mode))
            .map(|(_, r)| r.body_at.unwrap() - r.ref_at.unwrap())
            .collect();
        v.sort_by(f64::total_cmp);
        v
    };
    let pct = |v: &[f64], q: f64| -> f64 {
        if v.is_empty() {
            return f64::NAN;
        }
        v[(((v.len() - 1) as f64) * q).round() as usize]
    };

    let spec = lag(Mode::Spec);
    let tagged = lag(Mode::Tagged);

    println!(
        "\n{} complete blocks ({} spec, {} tagged), {ref_errors} reference error(s)\n\
         body in hand, measured from the reference leg first seeing the block:\n\
         \x20 spec    p50 {:>6.0}  p90 {:>6.0}  max {:>6.0}  (n = {})\n\
         \x20 tagged  p50 {:>6.0}  p90 {:>6.0}  max {:>6.0}  (n = {})\n\
         \x20 spec − tagged at p50: {:+.0} ms",
        rows.len(),
        spec.len(),
        tagged.len(),
        pct(&spec, 0.50),
        pct(&spec, 0.90),
        spec.last().copied().unwrap_or(f64::NAN),
        spec.len(),
        pct(&tagged, 0.50),
        pct(&tagged, 0.90),
        tagged.last().copied().unwrap_or(f64::NAN),
        tagged.len(),
        pct(&spec, 0.50) - pct(&tagged, 0.50),
    );

    // The predecessor's own tick is not in the tagged leg, which is triggered on
    // the reference grain. Adding its mean back gives the like-for-like figure.
    println!(
        " tagged + the predecessor's {ts_tick_ms:.0} ms tick (mean {:.0} ms): p50 {:.0} ms",
        ts_tick_ms / 2.0,
        pct(&tagged, 0.50) + ts_tick_ms / 2.0,
    );

    // A tagged block that needed retries means the provider lags its own head
    // tag for everyone, which is a different mechanism from our polls poisoning
    // an entry — and it would show up on both legs rather than one.
    let retried = |mode: Mode| -> (usize, usize) {
        let rows: Vec<&Record> = rows
            .iter()
            .filter(|(_, r)| r.mode == Some(mode))
            .map(|(_, r)| *r)
            .collect();
        (rows.iter().filter(|r| r.null_polls > 0).count(), rows.len())
    };
    let (spec_null, spec_n) = retried(Mode::Spec);
    let (tagged_null, tagged_n) = retried(Mode::Tagged);
    println!(
        " blocks that saw at least one null: spec {spec_null}/{spec_n}, tagged {tagged_null}/{tagged_n}"
    );

    // The punchline, if there is one: what the service's own metric would have
    // reported for these same blocks against what they actually cost.
    let mut gaps: Vec<f64> = rows
        .iter()
        .filter(|(_, r)| r.mode == Some(Mode::Spec))
        .filter_map(|(_, r)| r.poll_gap_ms)
        .collect();
    gaps.sort_by(f64::total_cmp);
    if !gaps.is_empty() {
        println!(
            " what head_detection_gap_ms would report for the spec blocks: p50 {:.0}  p90 {:.0} ms\n\
             \x20 the difference from the row above is the blind spot: {:+.0} ms at p50",
            pct(&gaps, 0.50),
            pct(&gaps, 0.90),
            pct(&spec, 0.50) - pct(&gaps, 0.50),
        );
    }

    let json: Vec<Value> = rows
        .iter()
        .map(|(number, r)| {
            json!({
                "number": number,
                "timestamp_s": r.timestamp_s,
                "mode": r.mode.unwrap().as_str(),
                "ref_at_ms": r.ref_at,
                "body_at_ms": r.body_at,
                "body_lag_ms": r.body_at.unwrap() - r.ref_at.unwrap(),
                "body_rtt_ms": r.body_rtt_ms,
                "null_polls": r.null_polls,
                "poll_gap_ms": r.poll_gap_ms,
                "tx_count": r.tx_count,
                "bytes": r.bytes,
                "errors": r.errors,
            })
        })
        .collect();
    std::fs::write(&out, serde_json::to_string_pretty(&json)?)?;
    println!(" wrote {} rows to {out}", json.len());

    Ok(())
}
