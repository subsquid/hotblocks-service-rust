//! Prometheus metrics registry — exact port of `metrics.ts`.

use prometheus::proto::{Metric, MetricFamily, MetricType};
use prometheus::{CounterVec, Gauge, Histogram, HistogramOpts, Opts, Registry};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    block_timestamp_cache()
        .lock()
        .unwrap()
        .set(&block_number.to_string(), ingested_at_ms);
}

pub fn get_block_ingestion_timestamp(height: &str) -> Option<u64> {
    block_timestamp_cache().lock().unwrap().get(height)
}

// ---------------------------------------------------------------------------
// Metrics struct
// ---------------------------------------------------------------------------

pub struct Metrics {
    pub registry: Registry,

    last_block: Gauge,
    last_block_lag_ms: Gauge,
    first_block: Gauge,
    finalized_block: Gauge,
    stored_blocks: Gauge,
    block_lag_ms: Histogram,
    processing_time_ms: Histogram,
    queries_total: CounterVec,
    active_workers: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
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

        let active_workers = Gauge::with_opts(Opts::new(
            "sqd_hotblocks_active_workers",
            "Number of currently active worker threads",
        ))
        .unwrap();

        // Register all metrics.
        registry.register(Box::new(last_block.clone())).unwrap();
        registry
            .register(Box::new(last_block_lag_ms.clone()))
            .unwrap();
        registry.register(Box::new(first_block.clone())).unwrap();
        registry
            .register(Box::new(finalized_block.clone()))
            .unwrap();
        registry.register(Box::new(stored_blocks.clone())).unwrap();
        registry.register(Box::new(block_lag_ms.clone())).unwrap();
        registry
            .register(Box::new(processing_time_ms.clone()))
            .unwrap();
        registry.register(Box::new(queries_total.clone())).unwrap();
        registry.register(Box::new(active_workers.clone())).unwrap();

        // Pre-initialise counters so they appear immediately (mirrors TS).
        queries_total.with_label_values(&["cache"]);
        queries_total.with_label_values(&["backfill"]);
        queries_total.with_label_values(&["error"]);
        active_workers.set(0.0);

        Self {
            registry,
            last_block,
            last_block_lag_ms,
            first_block,
            finalized_block,
            stored_blocks,
            block_lag_ms,
            processing_time_ms,
            queries_total,
            active_workers,
        }
    }

    pub fn set_last_block(&self, value: u64) {
        self.last_block.set(value as f64);
    }

    pub fn set_last_block_timestamp(&self, value: u64) {
        if value == 0 {
            self.last_block_lag_ms.set(-1.0);
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

    pub fn inc_query(&self, kind: &str) {
        self.queries_total.with_label_values(&[kind]).inc();
    }

    pub fn inc_active_workers(&self) {
        self.active_workers.inc();
    }

    pub fn dec_active_workers(&self) {
        self.active_workers.dec();
    }

    /// Return all metrics as a Prometheus text exposition.
    pub fn gather_text(&self) -> Result<String, prometheus::Error> {
        use prometheus::Encoder;
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
