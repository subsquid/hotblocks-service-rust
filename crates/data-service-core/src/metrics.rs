//! Prometheus metrics registry: spec 12's OB signals, bound to series by IB-12.
//!
//! Levels naming a *moment* are epoch ms with `-1` for "not reached yet"
//! (ADR-1's absence sentinel) — an unreached stage must not read as 1970.

use prometheus::proto::{Metric, MetricFamily, MetricType};
use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// `P-STALL-ALARM`: zero commit progress against an advancing upstream head
/// for this long raises the LIV-2 stall level.
pub const P_STALL_ALARM: Duration = Duration::from_secs(60);

/// The value every "moment" level holds until the moment happens.
const UNREACHED: f64 = -1.0;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Copy)]
struct UpstreamHeadObservation {
    number_observed_at: Instant,
    explicitly_observed_at: Option<Instant>,
    number: u64,
}

struct StallClock {
    last_commit_at: Option<Instant>,
    behind_since: Option<Instant>,
}

// ---------------------------------------------------------------------------
// Block ingestion timestamp cache (port of `BlockTimestampCache` in metrics.ts)
// ---------------------------------------------------------------------------

struct BlockTimestampCache {
    cache: HashMap<String, u64>,
    max_size: usize,
    ttl: Duration,
    /// Expiry wall-clock instants keyed by block height string.
    expiry: HashMap<String, Instant>,
}

impl BlockTimestampCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            max_size,
            ttl,
            expiry: HashMap::new(),
        }
    }

    fn set(&mut self, height: &str, timestamp: u64) {
        // Evict expired entries lazily.
        let now = Instant::now();
        self.expiry.retain(|k, &mut exp| {
            if exp <= now {
                self.cache.remove(k);
                false
            } else {
                true
            }
        });

        if self.cache.len() >= self.max_size {
            // Evict the entry with the oldest timestamp (mirrors TS behavior).
            if let Some(oldest_key) = self
                .cache
                .iter()
                .min_by_key(|(_, &v)| v)
                .map(|(k, _)| k.clone())
            {
                self.cache.remove(&oldest_key);
                self.expiry.remove(&oldest_key);
            }
        }

        self.cache.insert(height.to_string(), timestamp);
        self.expiry
            .insert(height.to_string(), Instant::now() + self.ttl);
    }

    fn get(&self, height: &str) -> Option<u64> {
        // Check expiry.
        if let Some(&exp) = self.expiry.get(height) {
            if Instant::now() > exp {
                return None;
            }
        }
        self.cache.get(height).copied()
    }
}

// Global singleton (mirrors the module-level `blockTimestampCache` in metrics.ts).
static BLOCK_TIMESTAMP_CACHE: OnceLock<Mutex<BlockTimestampCache>> = OnceLock::new();

#[derive(Serialize)]
struct JsonMetricFamily<'a> {
    help: &'a str,
    name: &'a str,
    #[serde(rename = "type")]
    metric_type: &'static str,
    values: Vec<JsonMetricValue>,
    aggregator: &'static str,
}

#[derive(Serialize)]
struct JsonMetricValue {
    value: Value,
    labels: Map<String, Value>,
    #[serde(rename = "metricName", skip_serializing_if = "Option::is_none")]
    metric_name: Option<String>,
}

fn json_number(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 {
        if value >= 0.0 && value <= u64::MAX as f64 {
            return Value::from(value as u64);
        }
        if value >= i64::MIN as f64 && value <= i64::MAX as f64 {
            return Value::from(value as i64);
        }
    }
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn labels(metric: &Metric) -> Map<String, Value> {
    metric
        .get_label()
        .iter()
        .map(|label| {
            (
                label.name().to_string(),
                Value::String(label.value().to_string()),
            )
        })
        .collect()
}

fn sample(
    value: impl Into<Value>,
    labels: Map<String, Value>,
    metric_name: Option<String>,
) -> JsonMetricValue {
    JsonMetricValue {
        value: value.into(),
        labels,
        metric_name,
    }
}

fn metric_values(family: &MetricFamily) -> Vec<JsonMetricValue> {
    let name = family.name();
    let mut values = Vec::new();

    for metric in family.get_metric() {
        match family.get_field_type() {
            MetricType::GAUGE => values.push(sample(
                json_number(
                    metric
                        .gauge
                        .as_ref()
                        .map(|gauge| gauge.value())
                        .unwrap_or(0.0),
                ),
                labels(metric),
                None,
            )),
            MetricType::COUNTER => values.push(sample(
                json_number(
                    metric
                        .counter
                        .as_ref()
                        .map(|counter| counter.value())
                        .unwrap_or(0.0),
                ),
                labels(metric),
                None,
            )),
            MetricType::HISTOGRAM => {
                let Some(histogram) = metric.histogram.as_ref() else {
                    continue;
                };
                for bucket in histogram.get_bucket() {
                    let mut bucket_labels = labels(metric);
                    let upper_bound = bucket.upper_bound();
                    bucket_labels.insert(
                        "le".to_string(),
                        if upper_bound.is_infinite() {
                            Value::String("+Inf".to_string())
                        } else {
                            json_number(upper_bound)
                        },
                    );
                    values.push(sample(
                        Value::from(bucket.cumulative_count()),
                        bucket_labels,
                        Some(format!("{name}_bucket")),
                    ));
                }
                let mut infinity_labels = labels(metric);
                infinity_labels.insert("le".to_string(), Value::String("+Inf".to_string()));
                values.push(sample(
                    Value::from(histogram.sample_count()),
                    infinity_labels,
                    Some(format!("{name}_bucket")),
                ));
                values.push(sample(
                    json_number(histogram.sample_sum()),
                    labels(metric),
                    Some(format!("{name}_sum")),
                ));
                values.push(sample(
                    Value::from(histogram.sample_count()),
                    labels(metric),
                    Some(format!("{name}_count")),
                ));
            }
            MetricType::SUMMARY => {
                let Some(summary) = metric.summary.as_ref() else {
                    continue;
                };
                for quantile in &summary.quantile {
                    let mut quantile_labels = labels(metric);
                    quantile_labels
                        .insert("quantile".to_string(), json_number(quantile.quantile()));
                    values.push(sample(
                        json_number(quantile.value()),
                        quantile_labels,
                        Some(name.to_string()),
                    ));
                }
                values.push(sample(
                    json_number(summary.sample_sum()),
                    labels(metric),
                    Some(format!("{name}_sum")),
                ));
                values.push(sample(
                    Value::from(summary.sample_count()),
                    labels(metric),
                    Some(format!("{name}_count")),
                ));
            }
            MetricType::UNTYPED => values.push(sample(
                json_number(
                    metric
                        .untyped
                        .as_ref()
                        .map(|untyped| untyped.value())
                        .unwrap_or(0.0),
                ),
                labels(metric),
                None,
            )),
        }
    }

    values
}

fn metric_type_name(metric_type: MetricType) -> &'static str {
    match metric_type {
        MetricType::COUNTER => "counter",
        MetricType::GAUGE => "gauge",
        MetricType::SUMMARY => "summary",
        MetricType::UNTYPED => "untyped",
        MetricType::HISTOGRAM => "histogram",
    }
}

fn block_timestamp_cache() -> &'static Mutex<BlockTimestampCache> {
    BLOCK_TIMESTAMP_CACHE
        .get_or_init(|| Mutex::new(BlockTimestampCache::new(1000, Duration::from_secs(30 * 60))))
}

/// Wall-clock milliseconds since the epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Record when `block_number` became queryable. The instant is the caller's:
/// a batch publishes its observables only after committing, but each block
/// still owns the moment it landed rather than the moment the batch ended.
pub fn record_block_ingestion(block_number: u64, ingested_at_ms: u64) {
    lock_unpoisoned(block_timestamp_cache()).set(&block_number.to_string(), ingested_at_ms);
}

pub fn get_block_ingestion_timestamp(height: &str) -> Option<u64> {
    lock_unpoisoned(block_timestamp_cache()).get(height)
}

// ---------------------------------------------------------------------------
// Closed label sets (OB-8)
// ---------------------------------------------------------------------------

/// A label enum plus the value set the registry pre-registers, so a new
/// outcome cannot be added without being enumerated for registration.
macro_rules! closed_set {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            pub fn as_str(self) -> &'static str {
                match self { $($name::$variant => $label),+ }
            }
        }
    };
}

closed_set! {
    /// OB-5 query classes.
    QueryClass {
        Window => "window",
        WaitEmpty => "wait_empty",
        Backfill => "backfill",
        Conflict => "conflict",
        Error => "error",
    }
}

closed_set! {
    /// RP-12 truncation causes.
    TruncationCause {
        Budget => "budget",
        Error => "error",
        Disconnect => "disconnect",
    }
}

closed_set! {
    /// OB-7 session restart causes; `Reset` is the T1 re-INIT (WP-9/WP-20).
    SessionRestart {
        Error => "error",
        Fork => "fork",
        Reset => "reset",
    }
}

closed_set! {
    /// OB-4 round-trip shapes; batch items are counted separately.
    UpstreamRequestKind {
        Single => "single",
        Batch => "batch",
    }
}

closed_set! {
    /// OB-4 failure classes. Transport-shaped, not per-method: a per-method
    /// set would not be enumerable at startup (OB-8).
    UpstreamErrorClass {
        Rpc => "rpc",
        Http => "http",
        Connection => "connection",
        Disconnected => "disconnected",
        Timeout => "timeout",
        Protocol => "protocol",
        RetryRequested => "retry_requested",
    }
}

// ---------------------------------------------------------------------------
// Metrics struct
// ---------------------------------------------------------------------------

pub struct Metrics {
    pub registry: Registry,

    // OB-1 state levels.
    last_block: Gauge,
    last_block_lag_ms: Gauge,
    first_block: Gauge,
    finalized_block: Gauge,
    stored_blocks: Gauge,
    window_excess: Gauge,

    // OB-2 progress heartbeat.
    commits_total: Counter,
    last_commit_timestamp_ms: Gauge,

    // OB-3 arrival lag / OB-5 ingestion cost.
    block_lag_ms: Histogram,
    processing_time_ms: Histogram,

    // `queries_total` is the predecessor's series, frozen at three values
    // (REQ-24); `query_outcomes_total` is OB-5's own split.
    queries_total: CounterVec,
    query_outcomes_total: CounterVec,
    query_duration_ms: HistogramVec,
    truncations_total: CounterVec,

    // OB-4 upstream view and interaction health.
    upstream_head: Gauge,
    upstream_head_timestamp_ms: Gauge,
    /// Typed copy of the OB-4 head observation for local readiness decisions.
    /// Prometheus gauges are `f64`, so they are not a sound state store for a
    /// `u64` chain coordinate.
    upstream_head_observation: Mutex<Option<UpstreamHeadObservation>>,
    upstream_finalized_head: Gauge,
    upstream_finalized_head_timestamp_ms: Gauge,
    upstream_requests_total: CounterVec,
    upstream_calls_total: Counter,
    upstream_errors_total: CounterVec,
    upstream_retries_total: CounterVec,

    // OB-6 retention alarms.
    over_window: Gauge,
    force_advances_total: Counter,
    force_advanced_past: Gauge,

    // OB-7 ingestion alarms.
    integrity_violations_total: Counter,
    session_restarts_total: CounterVec,
    acquisition_retry_exhaustions_total: Counter,
    fork_rebases_total: Counter,
    watermark_regressions_total: Counter,
    terminal_state: Gauge,
    stall_alarm: Gauge,

    // OB-9 lifecycle.
    first_acceptance_timestamp_ms: Gauge,
    first_commit_timestamp_ms: Gauge,
    shutdown_start_timestamp_ms: Gauge,

    /// `P-STALL-ALARM` for this instance; a test shortens it.
    stall_after: Duration,
    /// Monotonic origin for the pre-first-commit stall bound.
    process_started_at: Instant,
    /// Debounces the OB-10 capture so it fires once per flip, whichever caller
    /// (scrape or ticker) observes the edge first.
    stall_active: AtomicBool,
    /// Monotonic progress state shared by the stall decision. Wall-clock gauges
    /// remain presentation-only and cannot change readiness/alarm ordering.
    stall_clock: Mutex<StallClock>,
    /// WP-9 ladder position and the error that put it there. Not exposed as
    /// series — an error string has no bounded label set (OB-8) — but OB-10's
    /// capture owes both, and only the ingestion loop knows them.
    ladder: Mutex<(i32, Option<String>)>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::with_stall_alarm(P_STALL_ALARM)
    }

    /// Shortened `P-STALL-ALARM`, so a test can drive the LIV-2 level.
    pub fn with_stall_alarm(stall_after: Duration) -> Self {
        let process_started_at = Instant::now();
        let registry = Registry::new();

        let last_block = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_last_block",
            "Number of the last stored block",
        ))
        .unwrap();

        let last_block_lag_ms = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_last_block_lag_ms",
            "Lag of the last stored block in ms",
        ))
        .unwrap();

        let first_block = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_first_block",
            "Number of the first stored block",
        ))
        .unwrap();

        let finalized_block = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_finalized_block",
            "Number of the finalized stored block",
        ))
        .unwrap();

        let stored_blocks = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_stored_blocks",
            "Amount of stored blocks",
        ))
        .unwrap();

        let block_lag_ms = Histogram::with_opts(
            HistogramOpts::new(
                "sqd_hotblocks_block_lag_ms",
                "Time to process a block from creation to end of processing in ms",
            )
            .buckets(vec![
                100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 15000.0, 20000.0, 30000.0,
                60000.0, 300000.0, 600000.0, 1200000.0, 3600000.0,
            ]),
        )
        .unwrap();

        let processing_time_ms = Histogram::with_opts(
            HistogramOpts::new(
                "sqd_hotblocks_processing_time_ms",
                "Time taken to process a block in milliseconds",
            )
            .buckets(vec![
                0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                1000.0,
            ]),
        )
        .unwrap();

        let queries_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_queries_total",
                "Total number of queries by type",
            ),
            &["type"],
        )
        .unwrap();

        let window_excess = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_window_excess",
            "Blocks held above the configured window (INV-4)",
        ))
        .unwrap();

        let commits_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_commits_total",
            "Ingestion batches committed to the buffer",
        ))
        .unwrap();

        let last_commit_timestamp_ms = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_last_commit_timestamp_ms",
            "Wall clock of the last committed batch in epoch ms, -1 before the first",
        ))
        .unwrap();

        let query_outcomes_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_query_outcomes_total",
                "Query outcomes by class (OB-5)",
            ),
            &["class"],
        )
        .unwrap();

        let query_duration_ms = HistogramVec::new(
            HistogramOpts::new(
                "sqd_hotblocks_query_duration_ms",
                "Time to a query verdict in milliseconds, by class",
            )
            .buckets(vec![
                0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
                10000.0, 30000.0, 60000.0,
            ]),
            &["class"],
        )
        .unwrap();

        let truncations_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_response_truncations_total",
                "Responses that ended at a record boundary before their range, by cause (RP-12)",
            ),
            &["cause"],
        )
        .unwrap();

        let upstream_head = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_upstream_head",
            "Upstream head height from the last read or committed source progress",
        ))
        .unwrap();

        let upstream_head_timestamp_ms = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_upstream_head_timestamp_ms",
            "When the upstream head was last read successfully, epoch ms, -1 if never",
        ))
        .unwrap();

        let upstream_finalized_head = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_upstream_finalized_head",
            "Upstream finalized height as last observed",
        ))
        .unwrap();

        let upstream_finalized_head_timestamp_ms = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_upstream_finalized_head_timestamp_ms",
            "When the upstream finalized head was last observed, epoch ms, -1 if never",
        ))
        .unwrap();

        let upstream_requests_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_upstream_requests_total",
                "Upstream JSON-RPC round trips by shape",
            ),
            &["kind"],
        )
        .unwrap();

        let upstream_calls_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_upstream_calls_total",
            "Upstream JSON-RPC calls submitted, counting batch items individually",
        ))
        .unwrap();

        let upstream_errors_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_upstream_errors_total",
                "Upstream failures by class",
            ),
            &["class"],
        )
        .unwrap();

        let upstream_retries_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_upstream_retries_total",
                "Upstream retries by the class that provoked them",
            ),
            &["class"],
        )
        .unwrap();

        let over_window = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_over_window",
            "1 while the buffer is above its window bound (OB-6)",
        ))
        .unwrap();

        let force_advances_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_force_advances_total",
            "Finalized-head force advances under auto-adjust (WP-24)",
        ))
        .unwrap();

        let force_advanced_past = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_force_advanced_past",
            "Height the last force advance moved the finalized head past, -1 if never",
        ))
        .unwrap();

        let integrity_violations_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_integrity_violations_total",
            "Batches rejected whole as integrity violations (WP-5)",
        ))
        .unwrap();

        let session_restarts_total = CounterVec::new(
            Opts::new(
                "sqd_hotblocks_session_restarts_total",
                "Ingestion session restarts by cause (WP-9)",
            ),
            &["cause"],
        )
        .unwrap();

        let acquisition_retry_exhaustions_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_acquisition_retry_exhaustions_total",
            "Blocks that exhausted their whole-block re-acquisition budget (WP-11.3)",
        ))
        .unwrap();

        let fork_rebases_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_fork_rebases_total",
            "Fork rebases applied to the buffer (T6)",
        ))
        .unwrap();

        let watermark_regressions_total = Counter::with_opts(Opts::new(
            "sqd_hotblocks_watermark_regressions_total",
            "Re-seeds opening an epoch below the previous finalized head (WP-20)",
        ))
        .unwrap();

        let terminal_state = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_terminal_state",
            "1 once ingestion has reached the FM-30 terminal state",
        ))
        .unwrap();

        let stall_alarm = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_stall_alarm",
            "1 while the upstream head is ahead and nothing has committed for P-STALL-ALARM (LIV-2)",
        ))
        .unwrap();

        let lifecycle = |name: &str, help: &str| {
            let gauge = Gauge::with_opts(Opts::new(name, help)).unwrap();
            gauge.set(UNREACHED);
            gauge
        };
        let process_start_timestamp_ms = lifecycle(
            "sqd_hotblocks_process_start_timestamp_ms",
            "Process start, epoch ms",
        );
        let first_acceptance_timestamp_ms = lifecycle(
            "sqd_hotblocks_first_acceptance_timestamp_ms",
            "First HTTP request served, epoch ms, -1 before it",
        );
        let first_commit_timestamp_ms = lifecycle(
            "sqd_hotblocks_first_commit_timestamp_ms",
            "First committed batch, epoch ms, -1 before it",
        );
        let shutdown_start_timestamp_ms = lifecycle(
            "sqd_hotblocks_shutdown_start_timestamp_ms",
            "Start of graceful shutdown, epoch ms, -1 before it",
        );
        process_start_timestamp_ms.set(now_ms() as f64);
        last_commit_timestamp_ms.set(UNREACHED);
        upstream_head_timestamp_ms.set(UNREACHED);
        upstream_finalized_head_timestamp_ms.set(UNREACHED);
        force_advanced_past.set(UNREACHED);

        // Register all metrics.
        for collector in [
            Box::new(last_block.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(last_block_lag_ms.clone()),
            Box::new(first_block.clone()),
            Box::new(finalized_block.clone()),
            Box::new(stored_blocks.clone()),
            Box::new(window_excess.clone()),
            Box::new(commits_total.clone()),
            Box::new(last_commit_timestamp_ms.clone()),
            Box::new(block_lag_ms.clone()),
            Box::new(processing_time_ms.clone()),
            Box::new(queries_total.clone()),
            Box::new(query_outcomes_total.clone()),
            Box::new(query_duration_ms.clone()),
            Box::new(truncations_total.clone()),
            Box::new(upstream_head.clone()),
            Box::new(upstream_head_timestamp_ms.clone()),
            Box::new(upstream_finalized_head.clone()),
            Box::new(upstream_finalized_head_timestamp_ms.clone()),
            Box::new(upstream_requests_total.clone()),
            Box::new(upstream_calls_total.clone()),
            Box::new(upstream_errors_total.clone()),
            Box::new(upstream_retries_total.clone()),
            Box::new(over_window.clone()),
            Box::new(force_advances_total.clone()),
            Box::new(force_advanced_past.clone()),
            Box::new(integrity_violations_total.clone()),
            Box::new(session_restarts_total.clone()),
            Box::new(acquisition_retry_exhaustions_total.clone()),
            Box::new(fork_rebases_total.clone()),
            Box::new(watermark_regressions_total.clone()),
            Box::new(terminal_state.clone()),
            Box::new(stall_alarm.clone()),
            Box::new(process_start_timestamp_ms.clone()),
            Box::new(first_acceptance_timestamp_ms.clone()),
            Box::new(first_commit_timestamp_ms.clone()),
            Box::new(shutdown_start_timestamp_ms.clone()),
        ] {
            registry.register(collector).unwrap();
        }

        // Pre-initialise every label value: a scrape before the first event
        // shows the zero, not an absent series (OB-8).
        for kind in ["cache", "backfill", "error"] {
            queries_total.with_label_values(&[kind]);
        }
        for class in QueryClass::ALL {
            query_outcomes_total.with_label_values(&[class.as_str()]);
            query_duration_ms.with_label_values(&[class.as_str()]);
        }
        for cause in TruncationCause::ALL {
            truncations_total.with_label_values(&[cause.as_str()]);
        }
        for cause in SessionRestart::ALL {
            session_restarts_total.with_label_values(&[cause.as_str()]);
        }
        for kind in UpstreamRequestKind::ALL {
            upstream_requests_total.with_label_values(&[kind.as_str()]);
        }
        for class in UpstreamErrorClass::ALL {
            upstream_errors_total.with_label_values(&[class.as_str()]);
            upstream_retries_total.with_label_values(&[class.as_str()]);
        }

        Self {
            registry,
            last_block,
            last_block_lag_ms,
            first_block,
            finalized_block,
            stored_blocks,
            window_excess,
            commits_total,
            last_commit_timestamp_ms,
            block_lag_ms,
            processing_time_ms,
            queries_total,
            query_outcomes_total,
            query_duration_ms,
            truncations_total,
            upstream_head,
            upstream_head_timestamp_ms,
            upstream_head_observation: Mutex::new(None),
            upstream_finalized_head,
            upstream_finalized_head_timestamp_ms,
            upstream_requests_total,
            upstream_calls_total,
            upstream_errors_total,
            upstream_retries_total,
            over_window,
            force_advances_total,
            force_advanced_past,
            integrity_violations_total,
            session_restarts_total,
            acquisition_retry_exhaustions_total,
            fork_rebases_total,
            watermark_regressions_total,
            terminal_state,
            stall_alarm,
            first_acceptance_timestamp_ms,
            first_commit_timestamp_ms,
            shutdown_start_timestamp_ms,
            stall_after,
            process_started_at,
            stall_active: AtomicBool::new(false),
            stall_clock: Mutex::new(StallClock {
                last_commit_at: None,
                behind_since: None,
            }),
            ladder: Mutex::new((0, None)),
        }
    }

    // ----- OB-1 state levels ----------------------------------------------

    pub fn set_last_block(&self, value: u64) {
        self.last_block.set(value as f64);
    }

    pub fn set_last_block_timestamp(&self, value: u64) {
        if value == 0 {
            self.last_block_lag_ms.set(UNREACHED);
        } else {
            self.last_block_lag_ms.set(now_ms() as f64 - value as f64);
        }
    }

    pub fn set_first_block(&self, value: u64) {
        self.first_block.set(value as f64);
    }

    pub fn set_stored_blocks(&self, value: usize) {
        self.stored_blocks.set(value as f64);
    }

    pub fn set_finalized_block(&self, value: u64) {
        self.finalized_block.set(value as f64);
    }

    /// OB-6: level and magnitude move together, so alarming on either sees
    /// the same edge (INV-4, LIV-11).
    pub fn set_window_excess(&self, excess: usize) {
        self.window_excess.set(excess as f64);
        self.over_window.set(if excess > 0 { 1.0 } else { 0.0 });
    }

    // ----- OB-2 heartbeat --------------------------------------------------

    /// Counted even for an all-duplicate batch: OB-2 must answer "commits are
    /// happening" without reading block content.
    pub fn record_commit(&self) {
        self.commits_total.inc();
        lock_unpoisoned(&self.stall_clock).last_commit_at = Some(Instant::now());
        let now = now_ms() as f64;
        self.last_commit_timestamp_ms.set(now);
        if self.first_commit_timestamp_ms.get() < 0.0 {
            self.first_commit_timestamp_ms.set(now);
        }
    }

    // ----- OB-3 / OB-5 ingestion cost --------------------------------------

    /// `observed_at_ms` is when the block became queryable, not when this is
    /// called: deferring publication past a batch commit must not be charged
    /// to the block as lag.
    pub fn observe_block_lag(&self, block_timestamp_ms: u64, observed_at_ms: u64) {
        if block_timestamp_ms == 0 {
            return;
        }
        self.block_lag_ms
            .observe(observed_at_ms as f64 - block_timestamp_ms as f64);
    }

    pub fn track_processing_time(&self, start: Instant) {
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        self.processing_time_ms.observe(duration_ms);
    }

    // ----- OB-5 query observables ------------------------------------------

    /// The predecessor's series (REQ-24); OB-5's classes are in `record_query`.
    pub fn inc_query(&self, kind: &str) {
        self.queries_total.with_label_values(&[kind]).inc();
    }

    pub fn record_query(&self, class: QueryClass, elapsed: Duration) {
        self.query_outcomes_total
            .with_label_values(&[class.as_str()])
            .inc();
        self.query_duration_ms
            .with_label_values(&[class.as_str()])
            .observe(elapsed.as_secs_f64() * 1000.0);
    }

    /// RP-12. For `Error` this counter is the server-side alarm INV-27 owes,
    /// the outcome no longer being signallable in band.
    pub fn record_truncation(&self, cause: TruncationCause) {
        self.truncations_total
            .with_label_values(&[cause.as_str()])
            .inc();
    }

    // ----- OB-4 upstream ---------------------------------------------------

    pub fn observe_upstream_head(&self, number: u64) {
        let observed_at = Instant::now();
        *lock_unpoisoned(&self.upstream_head_observation) = Some(UpstreamHeadObservation {
            number_observed_at: observed_at,
            explicitly_observed_at: Some(observed_at),
            number,
        });
        self.upstream_head.set(number as f64);
        self.upstream_head_timestamp_ms.set(now_ms() as f64);
    }

    /// Record progress proved by a committed source batch without regressing a
    /// newer explicit head read. Progress improves the local height but cannot
    /// make a failed `DataSource::get_head` refresh look fresh.
    pub(crate) fn observe_upstream_progress(&self, number: u64) {
        let mut observation = lock_unpoisoned(&self.upstream_head_observation);
        if observation.is_some_and(|current| current.number >= number) {
            return;
        }
        let explicitly_observed_at = observation
            .as_ref()
            .and_then(|current| current.explicitly_observed_at);
        *observation = Some(UpstreamHeadObservation {
            number_observed_at: Instant::now(),
            explicitly_observed_at,
            number,
        });
        drop(observation);
        self.upstream_head.set(number as f64);
    }

    /// Return the last observed upstream head while it is younger than the
    /// configured `P-STALL-ALARM` bound. Only a successful core-managed
    /// `DataSource::get_head` read refreshes that age; committed batches may
    /// improve the height without masking loss of upstream visibility.
    pub fn fresh_upstream_head(&self) -> Option<u64> {
        let observation = lock_unpoisoned(&self.upstream_head_observation)
            .as_ref()
            .copied()?;
        let explicitly_observed_at = observation.explicitly_observed_at?;
        (explicitly_observed_at.elapsed() < self.stall_after).then_some(observation.number)
    }

    pub(crate) fn upstream_head_refresh_interval(&self) -> Duration {
        (self.stall_after / 2).max(Duration::from_millis(1))
    }

    pub(crate) fn upstream_head_refresh_timeout(&self) -> Duration {
        (self.upstream_head_refresh_interval() / 4).max(Duration::from_millis(1))
    }

    pub fn observe_upstream_finalized_head(&self, number: u64) {
        self.upstream_finalized_head.set(number as f64);
        self.upstream_finalized_head_timestamp_ms
            .set(now_ms() as f64);
    }

    pub fn record_upstream_request(&self, kind: UpstreamRequestKind, calls: usize) {
        self.upstream_requests_total
            .with_label_values(&[kind.as_str()])
            .inc();
        self.upstream_calls_total.inc_by(calls as f64);
    }

    pub fn record_upstream_error(&self, class: UpstreamErrorClass) {
        self.upstream_errors_total
            .with_label_values(&[class.as_str()])
            .inc();
    }

    pub fn record_upstream_retry(&self, class: UpstreamErrorClass) {
        self.upstream_retries_total
            .with_label_values(&[class.as_str()])
            .inc();
    }

    // ----- OB-6 / OB-7 alarms ----------------------------------------------

    pub fn record_force_advance(&self, past_height: u64) {
        self.force_advances_total.inc();
        self.force_advanced_past.set(past_height as f64);
    }

    pub fn record_integrity_violation(&self) {
        self.integrity_violations_total.inc();
    }

    pub fn record_session_restart(&self, cause: SessionRestart) {
        self.session_restarts_total
            .with_label_values(&[cause.as_str()])
            .inc();
    }

    pub fn record_acquisition_retry_exhausted(&self) {
        self.acquisition_retry_exhaustions_total.inc();
    }

    pub fn record_fork_rebase(&self) {
        self.fork_rebases_total.inc();
    }

    pub fn record_watermark_regression(&self) {
        self.watermark_regressions_total.inc();
    }

    /// FM-30. A level, so a scrape during the drain still says why.
    pub fn record_terminal_state(&self) {
        self.terminal_state.set(1.0);
    }

    /// Where the WP-9 ladder stands and what ended the last session, for
    /// OB-10's capture to name when the stall alarm flips.
    pub fn note_session_end(&self, ladder_position: i32, error: Option<&str>) {
        *lock_unpoisoned(&self.ladder) = (ladder_position, error.map(str::to_string));
    }

    // ----- OB-9 lifecycle --------------------------------------------------

    pub fn record_first_acceptance(&self) {
        if self.first_acceptance_timestamp_ms.get() < 0.0 {
            self.first_acceptance_timestamp_ms.set(now_ms() as f64);
        }
    }

    pub fn record_shutdown_start(&self) {
        if self.shutdown_start_timestamp_ms.get() < 0.0 {
            self.shutdown_start_timestamp_ms.set(now_ms() as f64);
        }
    }

    // ----- derived levels --------------------------------------------------

    /// Recompute the clock-derived levels. Called from every scrape, and from
    /// a ticker so OB-10's capture does not wait for one.
    pub fn refresh_derived_levels(&self) {
        self.refresh_stall_alarm();
    }

    fn refresh_stall_alarm(&self) {
        // LIV-2 is about the upstream *advancing* past us; an idle chain
        // leaves the two equal however long it sits there.
        let observation = *lock_unpoisoned(&self.upstream_head_observation);
        let behind =
            observation.is_some_and(|current| current.number as f64 > self.last_block.get());
        let now = Instant::now();
        let (stalled, quiet_ms) = {
            let mut clock = lock_unpoisoned(&self.stall_clock);
            if !behind {
                clock.behind_since = None;
                (false, 0.0)
            } else {
                // Only the observation that first puts the upstream ahead
                // starts this clock; later observations preserve it.
                let observed_at = observation
                    .expect("behind requires an observation")
                    .number_observed_at;
                let behind_since = *clock.behind_since.get_or_insert(observed_at);

                // The bound runs from whichever came later: the moment the
                // upstream got ahead, or the last commit. Both are monotonic.
                let committed_at = clock.last_commit_at.unwrap_or(self.process_started_at);
                let since = committed_at.max(behind_since);
                let quiet = now.saturating_duration_since(since);
                (quiet >= self.stall_after, quiet.as_secs_f64() * 1000.0)
            }
        };

        self.stall_alarm.set(if stalled { 1.0 } else { 0.0 });
        if self.stall_active.swap(stalled, Ordering::Relaxed) == stalled {
            return;
        }
        if stalled {
            // OB-10: one capture per flip, not one per evaluation.
            let (ladder_position, last_error) = {
                let ladder = lock_unpoisoned(&self.ladder);
                (ladder.0, ladder.1.clone())
            };
            tracing::error!(
                upstream_head = self.upstream_head.get(),
                committed_head = self.last_block.get(),
                finalized_head = self.finalized_block.get(),
                stored_blocks = self.stored_blocks.get(),
                quiet_ms,
                ladder_position,
                last_error = last_error.as_deref().unwrap_or("none"),
                "stall alarm raised: upstream advanced and nothing has committed"
            );
        } else {
            tracing::info!(
                committed_head = self.last_block.get(),
                "stall alarm cleared"
            );
        }
    }

    /// Return all metrics as a Prometheus text exposition.
    pub fn gather_text(&self) -> Result<String, prometheus::Error> {
        use prometheus::Encoder;
        self.refresh_derived_levels();
        let encoder = prometheus::TextEncoder::new();
        let mf = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&mf, &mut buf)?;
        Ok(String::from_utf8(buf).unwrap_or_default())
    }

    /// Return the structured metric-family array exposed by prom-client's
    /// `getMetricsAsJSON`, retained for the temporary REQ-24 wire contract.
    ///
    /// # Errors
    ///
    /// Returns an error if a metric family cannot be serialized to JSON.
    pub fn gather_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        self.refresh_derived_levels();
        let gathered = self.registry.gather();
        let families: Vec<_> = gathered
            .iter()
            .map(|family| JsonMetricFamily {
                help: family.help(),
                name: family.name(),
                metric_type: metric_type_name(family.get_field_type()),
                values: metric_values(family),
                aggregator: "sum",
            })
            .collect();
        serde_json::to_value(families)
    }

    /// Look up a single metric family by name (for `/metrics/{name}`).
    pub fn get_single_metric_text(&self, name: &str) -> Option<String> {
        use prometheus::Encoder;
        self.refresh_derived_levels();
        let mfs = self.registry.gather();
        let found: Vec<_> = mfs.into_iter().filter(|mf| mf.name() == name).collect();
        if found.is_empty() {
            return None;
        }
        let encoder = prometheus::TextEncoder::new();
        let mut buf = Vec::new();
        encoder.encode(&found, &mut buf).ok()?;
        Some(String::from_utf8(buf).unwrap_or_default())
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn poisoned_upstream_observation_does_not_break_readiness() {
        let metrics = Arc::new(Metrics::new());
        let poison_target = Arc::clone(&metrics);
        let poisoned = std::thread::spawn(move || {
            let _guard = poison_target.upstream_head_observation.lock().unwrap();
            panic!("poison upstream observation for the regression");
        })
        .join();
        assert!(poisoned.is_err());

        metrics.observe_upstream_head(7);
        assert_eq!(metrics.fresh_upstream_head(), Some(7));
    }

    #[test]
    fn committed_progress_does_not_refresh_explicit_head_freshness() {
        let stall_after = Duration::from_millis(20);
        let metrics = Metrics::with_stall_alarm(stall_after);
        metrics.observe_upstream_head(2);
        assert_eq!(metrics.fresh_upstream_head(), Some(2));
        let explicit_timestamp_ms = metrics.upstream_head_timestamp_ms.get();

        {
            let mut observation = lock_unpoisoned(&metrics.upstream_head_observation);
            observation
                .as_mut()
                .expect("explicit observation")
                .explicitly_observed_at = Instant::now().checked_sub(stall_after * 2);
        }
        metrics.observe_upstream_progress(3);
        assert_eq!(metrics.fresh_upstream_head(), None);

        let observation =
            lock_unpoisoned(&metrics.upstream_head_observation).expect("progress observation");
        assert_eq!(observation.number, 3);
        assert!(observation.explicitly_observed_at.is_some());
        assert_eq!(
            metrics.upstream_head_timestamp_ms.get(),
            explicit_timestamp_ms,
            "committed progress must not rewrite the explicit-read timestamp"
        );
    }

    #[test]
    fn progress_without_an_explicit_read_is_not_fresh() {
        let metrics = Metrics::with_stall_alarm(Duration::from_secs(1));
        metrics.observe_upstream_progress(3);
        assert_eq!(metrics.fresh_upstream_head(), None);
    }

    #[test]
    fn stale_explicit_observation_still_drives_the_stall_alarm() {
        let stall_after = Duration::from_millis(20);
        let metrics = Metrics::with_stall_alarm(stall_after);
        metrics.set_last_block(1);
        metrics.observe_upstream_head(2);

        {
            let mut observation = lock_unpoisoned(&metrics.upstream_head_observation);
            let observation = observation.as_mut().expect("explicit observation");
            let stale_at = Instant::now()
                .checked_sub(stall_after * 2)
                .expect("test instant");
            observation.number_observed_at = stale_at;
            observation.explicitly_observed_at = Some(stale_at);
            lock_unpoisoned(&metrics.stall_clock).last_commit_at = Some(stale_at);
        }
        metrics.refresh_derived_levels();
        assert_eq!(metrics.fresh_upstream_head(), None);
        assert_eq!(metrics.stall_alarm.get(), 1.0);
    }

    #[test]
    fn head_refresh_timeout_leaves_headroom_before_the_next_observation() {
        let metrics = Metrics::with_stall_alarm(Duration::from_secs(60));
        let interval = metrics.upstream_head_refresh_interval();
        let timeout = metrics.upstream_head_refresh_timeout();
        assert!(timeout < interval);
        assert!(interval + timeout < metrics.stall_after);
    }
}
