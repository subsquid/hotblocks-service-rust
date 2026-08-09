//! Replays a block-arrival trace through the real `CadencePredictor` and reports
//! what the head poll loop would have detected: added detection delay per block
//! and polls issued for it. Answers whether a missed slot actually costs
//! latency, and how much, before anything is changed.
//!
//! Variants over the same trace: `real` is `evm_source`'s predictor (floor of
//! the last N intervals), `ema` is the averaging predictor it replaced,
//! `clamped` is the middle candidate that was measured and rejected, and the
//! rest are `real` with one knob moved — a wider hot window, or a coarser grain
//! inside it, which is the traffic lever. `fixed-25ms` is the traffic-blind
//! floor. Everything but `real` is a copy on purpose: they do not exist in the
//! source, and `ema` is kept so the regression stays reproducible.
//!
//! Trace, in preference order: `TRACE_JSON` holding `[[block_ts_s,
//! arrival_lag_ms], ...]` from `trace-collect` — real arrivals, replayed as
//! measured; the same variable holding a bare `[block_ts_s, ...]`, whose
//! arrivals are synthesised as `PROP_MS` + `U(0, JITTER_MS)`; or a synthetic
//! slot grid with `MISS_RATE` slots dropped. Every poll costs `RTT_MS` before
//! its answer is in hand.
//!
//! Prefer a collected trace. Under a uniform jitter model every candidate looks
//! fine — they separate only once the jitter approaches the hot window, and real
//! arrivals are heavy-tailed, not uniform. If you must synthesise, set
//! `JITTER_MS` from a measurement rather than from the default.
//!
//! Env knobs: BENCH_BLOCKS (5000), MISS_RATE (0.01), SLOT_MS (12000),
//! PROP_MS (500), JITTER_MS (300), RTT_MS (50), SEED (42), TRACE_JSON,
//! BENCH_OUT.

use std::io::Write;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use evm_source::ingest::CadencePredictor;

/// Deterministic uniform source — a fixed seed keeps A/B runs comparable.
struct Lcg(u64);

impl Lcg {
    fn next_f64(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// The predictor the floor estimator replaced, kept verbatim so the regression
/// stays reproducible.
struct Ema {
    ema_ms: Option<f64>,
    last_arrival: Option<Instant>,
    alpha: f64,
}

impl Ema {
    fn new() -> Self {
        Ema {
            ema_ms: None,
            last_arrival: None,
            alpha: 0.3,
        }
    }

    fn record_block(&mut self, now: Instant) {
        if let Some(last) = self.last_arrival {
            let interval_ms = now.duration_since(last).as_millis() as f64;
            self.ema_ms = Some(match self.ema_ms {
                None => interval_ms,
                Some(prev) => self.alpha * interval_ms + (1.0 - self.alpha) * prev,
            });
        }
        self.last_arrival = Some(now);
    }

    fn next_poll_delay(&self, now: Instant) -> Duration {
        const HOT_WINDOW_MS: f64 = 600.0;
        const HOT_POLL_MS: f64 = 25.0;

        let (Some(ema), Some(last)) = (self.ema_ms, self.last_arrival) else {
            return Duration::from_millis(100);
        };
        let elapsed_ms = now.duration_since(last).as_millis() as f64;
        let remaining = ema - elapsed_ms;
        let sleep_ms = if remaining > HOT_WINDOW_MS {
            (remaining - HOT_WINDOW_MS).min(1000.0)
        } else {
            HOT_POLL_MS
        };
        Duration::from_millis(sleep_ms.max(HOT_POLL_MS) as u64)
    }
}

/// Measured and rejected: capping how far one interval may raise the EMA fixes
/// the skipped slot and nothing else, so it collapses once jitter alone exceeds
/// the window.
struct Clamped {
    ema_ms: Option<f64>,
    last_arrival: Option<Instant>,
    alpha: f64,
    max_rise_ms: f64,
}

impl Clamped {
    fn new(max_rise_ms: f64) -> Self {
        Clamped {
            ema_ms: None,
            last_arrival: None,
            alpha: 0.3,
            max_rise_ms,
        }
    }

    fn record_block(&mut self, now: Instant) {
        if let Some(last) = self.last_arrival {
            let interval_ms = now.duration_since(last).as_millis() as f64;
            self.ema_ms = Some(match self.ema_ms {
                None => interval_ms,
                Some(prev) => (self.alpha * interval_ms + (1.0 - self.alpha) * prev)
                    .min(prev + self.max_rise_ms),
            });
        }
        self.last_arrival = Some(now);
    }

    fn next_poll_delay(&self, now: Instant) -> Duration {
        const HOT_WINDOW_MS: f64 = 600.0;
        const HOT_POLL_MS: f64 = 25.0;

        let (Some(ema), Some(last)) = (self.ema_ms, self.last_arrival) else {
            return Duration::from_millis(100);
        };
        let elapsed_ms = now.duration_since(last).as_millis() as f64;
        let remaining = ema - elapsed_ms;
        let sleep_ms = if remaining > HOT_WINDOW_MS {
            (remaining - HOT_WINDOW_MS).min(1000.0)
        } else {
            HOT_POLL_MS
        };
        Duration::from_millis(sleep_ms.max(HOT_POLL_MS) as u64)
    }
}

/// The source's estimator, parameterised — used here to score a hot window or a
/// grain other than the shipped 600 ms / 25 ms.
struct MinOfN {
    intervals: Vec<f64>,
    last_arrival: Option<Instant>,
    n: usize,
    hot_window_ms: f64,
    hot_poll_ms: f64,
}

impl MinOfN {
    fn new(n: usize, hot_window_ms: f64, hot_poll_ms: f64) -> Self {
        MinOfN {
            intervals: Vec::new(),
            last_arrival: None,
            n,
            hot_window_ms,
            hot_poll_ms,
        }
    }

    fn record_block(&mut self, now: Instant) {
        if let Some(last) = self.last_arrival {
            self.intervals
                .push(now.duration_since(last).as_millis() as f64);
            if self.intervals.len() > self.n {
                self.intervals.remove(0);
            }
        }
        self.last_arrival = Some(now);
    }

    fn next_poll_delay(&self, now: Instant) -> Duration {
        let hot_poll_ms = self.hot_poll_ms;

        let Some(last) = self.last_arrival else {
            return Duration::from_millis(100);
        };
        let Some(floor) = self.intervals.iter().copied().reduce(f64::min) else {
            return Duration::from_millis(100);
        };
        let remaining = floor - now.duration_since(last).as_millis() as f64;
        let sleep_ms = if remaining > self.hot_window_ms {
            (remaining - self.hot_window_ms).min(1000.0)
        } else {
            hot_poll_ms
        };
        Duration::from_millis(sleep_ms.max(hot_poll_ms) as u64)
    }
}

enum Poller {
    Real(CadencePredictor),
    Ema(Ema),
    Clamped(Clamped),
    MinOfN(MinOfN),
    /// Traffic-blind: poll on a constant grain, whatever the chain is doing.
    Fixed(u64),
}

impl Poller {
    fn next_delay_ms(&self, now: Instant) -> u64 {
        match self {
            Poller::Real(p) => p.next_poll_delay(now).as_millis() as u64,
            Poller::Ema(p) => p.next_poll_delay(now).as_millis() as u64,
            Poller::Clamped(p) => p.next_poll_delay(now).as_millis() as u64,
            Poller::MinOfN(p) => p.next_poll_delay(now).as_millis() as u64,
            Poller::Fixed(ms) => *ms,
        }
    }

    fn record(&mut self, now: Instant) {
        match self {
            Poller::Real(p) => p.record_block(now),
            Poller::Ema(p) => p.record_block(now),
            Poller::Clamped(p) => p.record_block(now),
            Poller::MinOfN(p) => p.record_block(now),
            Poller::Fixed(_) => {}
        }
    }
}

struct Trace {
    /// Absolute arrival time of each block, ms from trace start.
    arrivals: Vec<u64>,
    /// Blocks since the last multi-slot gap; 0 is the block that closed it.
    since_gap: Vec<usize>,
    gaps: usize,
}

fn synthetic(
    blocks: usize,
    slot_ms: u64,
    miss_rate: f64,
    prop_ms: u64,
    jitter_ms: u64,
    seed: u64,
) -> Trace {
    let mut rng = Lcg(seed);
    let (mut arrivals, mut since_gap) = (Vec::with_capacity(blocks), Vec::with_capacity(blocks));
    let (mut slot, mut pos, mut gaps) = (0u64, usize::MAX, 0usize);
    while arrivals.len() < blocks {
        slot += 1;
        if rng.next_f64() < miss_rate {
            pos = usize::MAX; // the next produced block closes a gap
            continue;
        }
        let jitter = (rng.next_f64() * jitter_ms as f64) as u64;
        arrivals.push(slot * slot_ms + prop_ms + jitter);
        if pos == usize::MAX {
            if !arrivals.is_empty() && arrivals.len() > 1 {
                gaps += 1;
            }
            pos = 0;
        } else {
            pos += 1;
        }
        since_gap.push(pos);
    }
    Trace {
        arrivals,
        since_gap,
        gaps,
    }
}

/// Blocks since the last multi-slot gap, and the gap count.
fn gap_positions(ts: &[f64], slot_ms: u64) -> (Vec<usize>, usize) {
    let (mut since_gap, mut pos, mut gaps) = (Vec::with_capacity(ts.len()), 0usize, 0usize);
    for i in 0..ts.len() {
        if i > 0 {
            let delta_ms = ((ts[i] - ts[i - 1]) * 1000.0) as u64;
            if delta_ms > slot_ms + slot_ms / 2 {
                gaps += 1;
                pos = 0;
            } else {
                pos += 1;
            }
        }
        since_gap.push(pos);
    }
    (since_gap, gaps)
}

fn from_timestamps(ts: &[f64], slot_ms: u64, prop_ms: u64, jitter_ms: u64, seed: u64) -> Trace {
    let mut rng = Lcg(seed);
    let t0 = ts.first().copied().unwrap_or(0.0);
    let arrivals = ts
        .iter()
        .map(|t| {
            let jitter = (rng.next_f64() * jitter_ms as f64) as u64;
            (((t - t0) * 1000.0) as u64) + prop_ms + jitter
        })
        .collect();
    let (since_gap, gaps) = gap_positions(ts, slot_ms);
    Trace {
        arrivals,
        since_gap,
        gaps,
    }
}

/// Replay of a collected trace: arrival is the measured offset from the block's
/// own timestamp, so propagation and jitter are the real ones rather than
/// `PROP_MS` + `U(0, JITTER_MS)`. Prefer this — a uniform jitter model flatters
/// every predictor, and the tail is where they differ.
fn from_measured(samples: &[(f64, f64)], slot_ms: u64) -> Trace {
    let ts: Vec<f64> = samples.iter().map(|s| s.0).collect();
    let t0 = ts.first().copied().unwrap_or(0.0);
    let arrivals = samples
        .iter()
        .map(|(t, lag_ms)| (((t - t0) * 1000.0) + lag_ms).max(0.0) as u64)
        .collect();
    let (since_gap, gaps) = gap_positions(&ts, slot_ms);
    Trace {
        arrivals,
        since_gap,
        gaps,
    }
}

struct Sim {
    delays: Vec<f64>,
    polls: u64,
}

/// A poll answers about the chain as it was when the request went out and lands
/// `rtt_ms` later, so the round trip is not a constant added to the result: it
/// stretches the effective grain, delays the arrival the predictor records, and
/// pushes the next sleep out with it. The first request for a height is issued
/// the moment the previous one lands — the real loop sleeps only after a
/// fruitless answer.
fn simulate(mut poller: Poller, trace: &Trace, rtt_ms: u64) -> Sim {
    let base = Instant::now();
    let mut delays = Vec::with_capacity(trace.arrivals.len());
    let (mut clock, mut polls) = (0u64, 0u64);
    for &arrival in &trace.arrivals {
        loop {
            let issued = clock;
            clock = issued + rtt_ms;
            polls += 1;
            if issued >= arrival {
                break;
            }
            clock += poller.next_delay_ms(base + Duration::from_millis(clock));
        }
        delays.push((clock - arrival) as f64);
        poller.record(base + Duration::from_millis(clock));
    }
    Sim { delays, polls }
}

fn pct(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[(((sorted.len() - 1) as f64) * q).round() as usize]
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn report(name: &str, sim: &Sim, trace: &Trace) -> Value {
    let mut sorted = sim.delays.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let over_100 =
        sim.delays.iter().filter(|d| **d > 100.0).count() as f64 / sim.delays.len() as f64;
    let polls_per_block = sim.polls as f64 / sim.delays.len() as f64;
    println!(
        "  {name:<12} mean {:7.1}  p50 {:6.1}  p90 {:7.1}  p95 {:7.1}  p99 {:7.1}  max {:7.1}  >100ms {:5.2}%  polls/blk {:6.1}",
        mean(&sim.delays),
        pct(&sorted, 0.50),
        pct(&sorted, 0.90),
        pct(&sorted, 0.95),
        pct(&sorted, 0.99),
        sorted.last().copied().unwrap_or(0.0),
        over_100 * 100.0,
        polls_per_block,
    );
    json!({
        "variant": name,
        "blocks": sim.delays.len(),
        "gaps": trace.gaps,
        "mean_ms": mean(&sim.delays),
        "p50_ms": pct(&sorted, 0.50),
        "p90_ms": pct(&sorted, 0.90),
        "p95_ms": pct(&sorted, 0.95),
        "p99_ms": pct(&sorted, 0.99),
        "max_ms": sorted.last().copied().unwrap_or(0.0),
        "over_100ms_share": over_100,
        "polls_per_block": polls_per_block,
    })
}

/// Mean delay by distance from the gap — the whole hypothesis is that position
/// 1..N after a gap is where the cost lands, not the late block itself.
fn by_position(sim: &Sim, trace: &Trace) -> Value {
    let mut rows = Vec::new();
    for pos in 0..=8usize {
        let sel: Vec<f64> = trace
            .since_gap
            .iter()
            .zip(&sim.delays)
            .filter(|(p, _)| **p == pos)
            .map(|(_, d)| *d)
            .collect();
        if sel.is_empty() {
            continue;
        }
        println!("    +{pos}  n {:5}  mean {:7.1} ms", sel.len(), mean(&sel));
        rows.push(json!({"position": pos, "n": sel.len(), "mean_ms": mean(&sel)}));
    }
    let steady: Vec<f64> = trace
        .since_gap
        .iter()
        .zip(&sim.delays)
        .filter(|(p, _)| **p > 8)
        .map(|(_, d)| *d)
        .collect();
    println!(
        "    >8  n {:5}  mean {:7.1} ms  (steady state)",
        steady.len(),
        mean(&steady)
    );
    rows.push(json!({"position": "steady", "n": steady.len(), "mean_ms": mean(&steady)}));
    Value::Array(rows)
}

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() -> anyhow::Result<()> {
    let blocks: usize = env("BENCH_BLOCKS", 5000);
    let miss_rate: f64 = env("MISS_RATE", 0.01);
    let slot_ms: u64 = env("SLOT_MS", 12000);
    let prop_ms: u64 = env("PROP_MS", 500);
    let jitter_ms: u64 = env("JITTER_MS", 300);
    let rtt_ms: u64 = env("RTT_MS", 50);
    let seed: u64 = env("SEED", 42);

    let trace = match std::env::var("TRACE_JSON") {
        Ok(path) => {
            let raw: Vec<Value> = serde_json::from_slice(&std::fs::read(&path)?)?;
            // `[[block_ts_s, arrival_lag_ms], ...]` from `trace-collect`, or a
            // bare `[block_ts_s, ...]` whose arrivals have to be synthesised.
            if raw.first().is_some_and(Value::is_array) {
                let samples: Vec<(f64, f64)> = raw
                    .iter()
                    .map(|v| {
                        let pair = v.as_array().ok_or_else(|| {
                            anyhow::anyhow!("mixed trace: expected [ts, lag_ms] pairs throughout")
                        })?;
                        match pair.as_slice() {
                            [t, lag] => Ok((
                                t.as_f64().unwrap_or_default(),
                                lag.as_f64().unwrap_or_default(),
                            )),
                            _ => Err(anyhow::anyhow!(
                                "expected [ts, lag_ms] pairs, got {} fields",
                                pair.len()
                            )),
                        }
                    })
                    .collect::<anyhow::Result<_>>()?;
                println!("measured trace {path}: {} blocks", samples.len());
                from_measured(&samples, slot_ms)
            } else {
                let ts: Vec<f64> = raw.iter().filter_map(Value::as_f64).collect();
                println!(
                    "trace {path}: {} blocks, arrivals synthesised at {prop_ms}+U(0,{jitter_ms}) ms",
                    ts.len()
                );
                from_timestamps(&ts, slot_ms, prop_ms, jitter_ms, seed)
            }
        }
        Err(_) => {
            // A rate of 1 would drop every slot and never terminate; anything
            // outside [0, 1) is a typo, not a scenario.
            if !(0.0..1.0).contains(&miss_rate) {
                anyhow::bail!("MISS_RATE must be within [0, 1), got {miss_rate}");
            }
            println!("synthetic: {blocks} blocks, slot {slot_ms} ms, miss rate {miss_rate}, propagation {prop_ms}+U(0,{jitter_ms}) ms");
            synthetic(blocks, slot_ms, miss_rate, prop_ms, jitter_ms, seed)
        }
    };
    if trace.arrivals.len() < 2 {
        anyhow::bail!(
            "a trace of {} block(s) has no interval to replay",
            trace.arrivals.len()
        );
    }
    println!(
        "  {} blocks, {} gaps ({:.2}% of slots missed)\n",
        trace.arrivals.len(),
        trace.gaps,
        trace.gaps as f64 / trace.arrivals.len() as f64 * 100.0
    );

    println!("detection delay: answer in hand minus true arrival, {rtt_ms} ms round trip modelled");
    let real = simulate(Poller::Real(CadencePredictor::new()), &trace, rtt_ms);
    let real_json = report("real", &real, &trace);
    let ema = simulate(Poller::Ema(Ema::new()), &trace, rtt_ms);
    let ema_json = report("ema (pre-fix)", &ema, &trace);
    let clamped = simulate(Poller::Clamped(Clamped::new(300.0)), &trace, rtt_ms);
    let clamped_json = report("clamped", &clamped, &trace);
    let min8_wide = simulate(Poller::MinOfN(MinOfN::new(8, 1500.0, 25.0)), &trace, rtt_ms);
    let min8_wide_json = report("real/1.5s window", &min8_wide, &trace);
    let grain50 = simulate(Poller::MinOfN(MinOfN::new(8, 600.0, 50.0)), &trace, rtt_ms);
    let grain50_json = report("real/50ms grain", &grain50, &trace);
    let grain100 = simulate(Poller::MinOfN(MinOfN::new(8, 600.0, 100.0)), &trace, rtt_ms);
    let grain100_json = report("real/100ms grain", &grain100, &trace);
    let fixed100 = simulate(Poller::Fixed(100), &trace, rtt_ms);
    let fixed100_json = report("fixed-100ms (TS)", &fixed100, &trace);
    let fixed = simulate(Poller::Fixed(25), &trace, rtt_ms);
    let fixed_json = report("fixed-25ms", &fixed, &trace);

    println!("\nreal, by blocks since the gap:");
    let real_pos = by_position(&real, &trace);
    println!("\nema (pre-fix), by blocks since the gap:");
    let ema_pos = by_position(&ema, &trace);

    let out = json!({
        "trace": {
            "blocks": trace.arrivals.len(),
            "gaps": trace.gaps,
            "slot_ms": slot_ms,
            "miss_rate": miss_rate,
            "prop_ms": prop_ms,
            "jitter_ms": jitter_ms,
            "rtt_ms": rtt_ms,
            "seed": seed,
        },
        "variants": [real_json, ema_json, clamped_json, min8_wide_json, grain50_json, grain100_json, fixed100_json, fixed_json],
        "by_position": {"real": real_pos, "ema": ema_pos},
    });
    if let Ok(path) = std::env::var("BENCH_OUT") {
        let mut f = std::fs::File::create(&path)?;
        f.write_all(serde_json::to_string_pretty(&out)?.as_bytes())?;
        println!("\nwrote {path}");
    }
    Ok(())
}
