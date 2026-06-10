//! Prometheus metrics registry — exact port of `metrics.ts`.

use prometheus::{CounterVec, Gauge, Histogram, HistogramOpts, Opts, Registry};
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

fn block_timestamp_cache() -> &'static Mutex<BlockTimestampCache> {
    BLOCK_TIMESTAMP_CACHE
        .get_or_init(|| Mutex::new(BlockTimestampCache::new(1000, Duration::from_secs(30 * 60))))
}

pub fn record_block_ingestion(block_number: u64) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    block_timestamp_cache()
        .lock()
        .unwrap()
        .set(&block_number.to_string(), now_ms);
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
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as f64;
            self.last_block_lag_ms.set(now_ms - value as f64);
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

    pub fn observe_block_lag(&self, block_timestamp_ms: u64) {
        if block_timestamp_ms == 0 {
            return;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64;
        self.block_lag_ms
            .observe(now_ms - block_timestamp_ms as f64);
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

    /// Return all metrics as JSON (encoded via text format then parsed).
    pub fn gather_json(&self) -> serde_json::Value {
        // Encode as text and return it as a JSON string value (simple approach
        // that avoids the protobuf internal API, which changed in prometheus 0.14).
        match self.gather_text() {
            Ok(text) => serde_json::Value::String(text),
            Err(e) => serde_json::json!({"error": e.to_string()}),
        }
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
