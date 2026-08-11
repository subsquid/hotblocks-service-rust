//! Measures when a block's receipts become available, relative to its header.
//!
//! Both tail explanations in the head-latency investigation rest on this and
//! neither had measured it: we ask for receipts sooner than the predecessor did,
//! and being early is only free if receipts are ready by then. A not-ready
//! answer costs `NOT_READY_DELAY` plus a re-acquisition, so what matters is the
//! shape of this distribution across the first tens of milliseconds after the
//! header, not its mean.
//!
//! Follows each numbered head block, then polls `eth_getBlockReceipts` for it on
//! a tight grain until the answer satisfies what `fetch.rs` demands before it
//! will accept receipts: non-null, one per transaction, every one naming our
//! header. Anything weaker would measure a different quantity than the service
//! reacts to.
//!
//! Run it where the service runs, and not beside another probe: at this grain
//! two of them measure each other.
//!
//! Env: RPC_URL (required), DURATION_S (300), POLL_MS (25), OUT
//! (receipts-ready.json).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

/// `evm_source::fetch::NOT_READY_DELAY`, mirrored rather than imported so the
/// probe stays a standalone binary that cross-compiles without the workspace.
const NOT_READY_DELAY_MS: f64 = 100.0;

/// What one not-ready verdict costs: the delay above plus a re-acquisition,
/// taken at `enrich_ms` p50. Only used to price a hypothetical wait against the
/// retries it would avoid, so a rough figure is enough.
const RETRY_COST_MS: f64 = 375.0;

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

/// The header fields the readiness test needs: identity, time, and how many
/// receipts a complete answer has to carry.
#[derive(Debug, Clone)]
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

/// Why an `eth_getBlockReceipts` answer was not usable yet — the same three
/// verdicts `fetch.rs` reaches, kept apart because they mean different things:
/// a short or absent answer is the provider catching up, a foreign block hash
/// is a reorg or an inconsistent fleet member and is not what we are timing.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Readiness {
    Ready,
    Null,
    Short,
    ForeignBlock,
}

fn classify_receipts(response: &Value, header: &Header) -> Result<Readiness> {
    if let Some(error) = response.get("error") {
        return Err(anyhow!("json-rpc error response: {error}"));
    }
    let result = response
        .get("result")
        .ok_or_else(|| anyhow!("json-rpc response is missing result"))?;
    if result.is_null() {
        return Ok(Readiness::Null);
    }
    let receipts = result
        .as_array()
        .ok_or_else(|| anyhow!("eth_getBlockReceipts did not return an array"))?;
    if receipts.iter().any(|r| {
        !r["blockHash"]
            .as_str()
            .is_some_and(|h| h.eq_ignore_ascii_case(&header.hash))
    }) {
        return Ok(Readiness::ForeignBlock);
    }
    if receipts.len() != header.tx_count {
        return Ok(Readiness::Short);
    }
    Ok(Readiness::Ready)
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

#[derive(Debug, Clone)]
struct Sample {
    number: u64,
    timestamp_s: u64,
    header_lag_ms: f64,
    receipts_delay_ms: f64,
    tx_count: usize,
    polls: u32,
    foreign_block_seen: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RPC_URL").map_err(|_| anyhow!("RPC_URL is required"))?;
    let duration = Duration::from_secs(env("DURATION_S", 300));
    let poll = Duration::from_millis(env("POLL_MS", 25));
    let out = std::env::var("OUT").unwrap_or_else(|_| "receipts-ready.json".to_string());

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(10))
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

    let mut next = seed
        .number
        .checked_add(1)
        .ok_or_else(|| anyhow!("latest block number cannot be incremented"))?;
    println!(
        "seeded at block {}; polling receipts on a {} ms grain",
        seed.number,
        poll.as_millis()
    );

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
            Ok(Some(header)) => header,
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

        // An empty block has nothing to wait for; recording it as instantly
        // ready would put a spike at zero that says nothing about the provider.
        if header.tx_count == 0 {
            next = next.saturating_add(1);
            continue;
        }

        let mut polls = 0u32;
        let mut foreign_block_seen = false;
        let receipts_delay_ms = loop {
            polls = polls.saturating_add(1);
            let verdict = call(
                &client,
                &url,
                &rpc_request("eth_getBlockReceipts", json!([tag])),
            )
            .await
            .and_then(|r| classify_receipts(&r, &header));
            match verdict {
                Ok(Readiness::Ready) => break now_ms() - header_at,
                Ok(Readiness::ForeignBlock) => {
                    foreign_block_seen = true;
                    tokio::time::sleep(poll).await;
                }
                Ok(_) => tokio::time::sleep(poll).await,
                Err(_) => {
                    errors = errors.saturating_add(1);
                    tokio::time::sleep(poll).await;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break f64::NAN;
            }
        };

        if receipts_delay_ms.is_finite() {
            samples.push(Sample {
                number: header.number,
                timestamp_s: header.timestamp_s,
                header_lag_ms: header_at - timestamp_ms,
                receipts_delay_ms,
                tx_count: header.tx_count,
                polls,
                foreign_block_seen,
            });
        }
        next = next.saturating_add(1);
    }

    if samples.is_empty() {
        return Err(anyhow!("collected no samples with {errors} error(s)"));
    }

    let mut delays: Vec<f64> = samples.iter().map(|s| s.receipts_delay_ms).collect();
    delays.sort_by(f64::total_cmp);
    let pct = |q: f64| delays[(((delays.len() - 1) as f64) * q).round() as usize];
    let ready_first_ask =
        samples.iter().filter(|s| s.polls == 1).count() as f64 / samples.len() as f64;

    println!(
        "{} non-empty blocks, {errors} errors\n\
         receipts ready after the header: p50 {:.0}  p90 {:.0}  p99 {:.0}  max {:.0} ms\n\
         ready on the first ask: {:.1}%",
        samples.len(),
        pct(0.50),
        pct(0.90),
        pct(0.99),
        delays[delays.len() - 1],
        ready_first_ask * 100.0,
    );

    // `receipts_delay_ms` is measured to the answer being in hand, so it carries
    // a round trip that has nothing to do with the provider's readiness. The
    // fastest first-ask success is very nearly that round trip alone: receipts
    // were ready well before we asked, leaving only the wire. Subtracting it
    // turns the polled blocks' delays into when receipts actually appeared.
    // First-ask successes get no such estimate and need none — they were ready
    // by the time we asked, so they stay ready however much later we ask.
    let Some(rtt) = samples
        .iter()
        .filter(|s| s.polls == 1)
        .map(|s| s.receipts_delay_ms)
        .min_by(f64::total_cmp)
    else {
        println!("no block was ready on the first ask — cannot separate the round trip out");
        return write_rows(&samples, &out);
    };
    let late: Vec<f64> = samples
        .iter()
        .filter(|s| s.polls > 1)
        .map(|s| s.receipts_delay_ms - rtt)
        .collect();
    println!("round-trip floor {rtt:.0} ms");

    // What a deliberate pause before the first receipts call would buy, against
    // what it costs on every block including the ones that never needed it.
    println!("wait before asking   still not ready   expected cost/block");
    for wait_ms in [0.0, 25.0, 50.0, 100.0, 150.0, 200.0] {
        let share = late.iter().filter(|a| **a > wait_ms).count() as f64 / samples.len() as f64;
        println!(
            "  {wait_ms:>6.0} ms          {:>5.1}%           {:>6.0} ms",
            share * 100.0,
            share * RETRY_COST_MS + wait_ms,
        );
    }

    // How deep the ladder has to go, which is what bounds LEGS_ONLY_RETRIES.
    // Attempt k reaches the provider at rtt/2 + NOT_READY_DELAY * (k - 1).
    let mut needed = [0usize; 8];
    for available_at in &late {
        let retries = (((available_at - rtt / 2.0) / NOT_READY_DELAY_MS).ceil() as usize).max(1);
        needed[retries.min(needed.len() - 1)] += 1;
    }
    print!("retries needed by the {} not-ready blocks:", late.len());
    for (retries, count) in needed.iter().enumerate().skip(1).filter(|(_, c)| **c > 0) {
        print!("  {retries}: {count}");
    }
    println!();

    write_rows(&samples, &out)
}

fn write_rows(samples: &[Sample], out: &str) -> Result<()> {
    let rows: Vec<Value> = samples
        .iter()
        .map(|s| {
            json!({
                "block": s.number,
                "ts": s.timestamp_s,
                "header_lag_ms": s.header_lag_ms,
                "receipts_delay_ms": s.receipts_delay_ms,
                "txs": s.tx_count,
                "polls": s.polls,
                "foreign_block_seen": s.foreign_block_seen,
            })
        })
        .collect();
    std::fs::write(out, serde_json::to_vec(&rows)?).with_context(|| format!("writing {out}"))?;
    println!("wrote {out}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header {
        Header {
            number: 42,
            timestamp_s: 101,
            hash: "0xAbC".to_string(),
            tx_count: 2,
        }
    }

    fn response(receipts: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": 1, "result": receipts})
    }

    #[test]
    fn a_complete_answer_naming_our_header_is_ready() {
        let res = response(json!([{"blockHash": "0xabc"}, {"blockHash": "0xABC"}]));

        assert_eq!(
            classify_receipts(&res, &header()).unwrap(),
            Readiness::Ready
        );
    }

    #[test]
    fn a_short_answer_is_the_provider_catching_up() {
        let res = response(json!([{"blockHash": "0xabc"}]));

        assert_eq!(
            classify_receipts(&res, &header()).unwrap(),
            Readiness::Short
        );
    }

    #[test]
    fn null_is_not_ready_rather_than_an_error() {
        assert_eq!(
            classify_receipts(&response(Value::Null), &header()).unwrap(),
            Readiness::Null
        );
    }

    #[test]
    fn receipts_naming_another_block_are_not_a_short_answer() {
        // Judged before the count, exactly as fetch.rs orders it: a full set
        // from the wrong block must not read as this block being ready.
        let res = response(json!([{"blockHash": "0xdef"}, {"blockHash": "0xdef"}]));

        assert_eq!(
            classify_receipts(&res, &header()).unwrap(),
            Readiness::ForeignBlock
        );
    }

    #[test]
    fn a_json_rpc_error_is_not_a_readiness_verdict() {
        let res = json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "x"}});

        assert!(classify_receipts(&res, &header()).is_err());
    }

    #[test]
    fn header_parser_counts_transactions_and_keeps_identity() {
        let res = json!({"jsonrpc": "2.0", "id": 1, "result": {
            "number": "0x2a", "timestamp": "0x65", "hash": "0xabc",
            "transactions": ["0x1", "0x2", "0x3"],
        }});

        let parsed = parse_header(&res).unwrap().unwrap();

        assert_eq!(
            (parsed.number, parsed.timestamp_s, parsed.tx_count),
            (42, 101, 3)
        );
    }
}
