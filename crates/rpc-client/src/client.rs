use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tracing::warn;

use crate::error::{RpcError, RpcErrorInfo};
use crate::observer::{RequestKind, RpcObserver};
use crate::rate::RateMeter;
use crate::transport::ws::{WsTransport, DEFAULT_POOL_SIZE};
use crate::transport::{HttpTransport, OwnedRpcRequest, RpcResponse, RpcTransport};

/// Default aggregate limit for concurrent upstream requests.
pub const DEFAULT_REQUEST_CAPACITY: usize = 10;

// ─── Config ──────────────────────────────────────────────────────────────────

/// Configuration for `RpcClient`.
#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    /// HTTP endpoint URL
    pub url: String,
    /// Max number of JSON-RPC calls in one HTTP batch array.
    /// Defaults to unlimited (or `max(1, rate_limit/5)` when rate_limit is set).
    /// Must be non-zero and no larger than one configured rate window can admit.
    pub max_batch_call_size: Option<usize>,
    /// Max concurrent in-flight HTTP requests.
    pub capacity: usize,
    /// Max calls per second (rate counted by batch items, not requests).
    /// Must be finite and greater than zero when configured.
    pub rate_limit: Option<f64>,
    /// Per-request HTTP timeout (zero = no timeout).
    pub request_timeout: Duration,
    /// How many times to retry a retryable error.
    pub retry_attempts: usize,
    /// Pause schedule between retries (index = attempt number, last value repeated).
    /// Default: [10, 100, 500, 2000, 10000, 20000] ms.
    pub retry_schedule: Vec<Duration>,
    /// Retry RPC errors with code -32000/-32603 / "internal error" messages.
    pub retry_internal_server_errors: bool,
    /// Size of the round-robin connection pool for the WebSocket transport.
    /// Ignored for HTTP. `None` → default (4). Min 1.
    pub ws_pool_size: Option<usize>,
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        RpcClientConfig {
            // A valid, parseable URL so `RpcClient::new(Default::default())`
            // does not panic on the scheme check (see `RpcClient::new` panics
            // contract). Real callers always override this.
            url: "http://localhost".into(),
            max_batch_call_size: None,
            capacity: DEFAULT_REQUEST_CAPACITY,
            rate_limit: None,
            request_timeout: Duration::ZERO,
            retry_attempts: 0,
            retry_schedule: vec![
                Duration::from_millis(10),
                Duration::from_millis(100),
                Duration::from_millis(500),
                Duration::from_millis(2000),
                Duration::from_millis(10000),
                Duration::from_millis(20000),
            ],
            retry_internal_server_errors: false,
            ws_pool_size: None,
        }
    }
}

// ─── Per-call options ────────────────────────────────────────────────────────

type ResultValidator = Box<dyn Fn(&Value) -> Result<Value, RpcError> + Send + Sync>;
type ErrorValidator = Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync>;

/// Options for a single `call` or `batch_call`.
#[derive(Default)]
pub struct CallOptions {
    /// Lower value = higher priority (mirrors TS; not yet used for scheduling —
    /// the semaphore handles concurrency; priority queue is a TODO).
    pub priority: u64,
    /// Override retry attempts for this call.
    pub retry_attempts: Option<usize>,
    /// Override request timeout for this call.
    pub timeout: Option<Duration>,
    /// Hook called on a successful result; may return `Err(RpcError::RetryRequested(...))`
    /// to force retry via built-in machinery.
    pub validate_result: Option<ResultValidator>,
    /// Hook called on an RPC error; may return `Ok(Value)` to override the error, or
    /// `Err(RpcError::RetryRequested(...))` to force retry.
    pub validate_error: Option<ErrorValidator>,
}

// ─── Rate state (interior mutability) ────────────────────────────────────────

struct RateState {
    meter: RateMeter,
    limit: f64,
}

// ─── RpcClient ───────────────────────────────────────────────────────────────

/// Async JSON-RPC 2.0 client over HTTP with batching, rate limiting,
/// concurrency control, and retry.
pub struct RpcClient {
    transport: Arc<dyn RpcTransport>,
    config: Arc<RpcClientConfig>,
    max_batch_call_size: usize,
    batch_concurrency: usize,
    semaphore: Arc<Semaphore>,
    counter: Arc<AtomicU64>,
    rate: Option<Arc<Mutex<RateState>>>,
    observer: Option<Arc<dyn RpcObserver>>,
}

impl RpcClient {
    /// Build a client, selecting the transport by URL scheme:
    /// `http`/`https` → HTTP; `ws`/`wss` → WebSocket.
    ///
    /// # Panics
    ///
    /// Panics if the configuration is invalid, including an unparseable or
    /// unsupported URL, zero capacity/batch size, a non-positive rate, or a
    /// batch that one rate window can never admit. Use [`RpcClient::try_new`]
    /// for the fallible variant.
    pub fn new(config: RpcClientConfig) -> Self {
        Self::try_new(config).expect("failed to build RpcClient")
    }

    /// Fallible constructor: validates request-budget invariants and the URL
    /// scheme before constructing any transport.
    pub fn try_new(config: RpcClientConfig) -> Result<Self, RpcError> {
        const RATE_WINDOW_SIZE: usize = 10;

        if config.capacity == 0 {
            return Err(RpcError::InvalidConfig(
                "capacity must be greater than zero".into(),
            ));
        }
        let rate_limit = config.rate_limit;
        if rate_limit.is_some_and(|limit| !limit.is_finite() || limit <= 0.0) {
            return Err(RpcError::InvalidConfig(
                "rate_limit must be finite and greater than zero".into(),
            ));
        }

        let rate_meter = rate_limit.map(|limit| RateMeter::for_rate_limit(RATE_WINDOW_SIZE, limit));
        if rate_limit
            .zip(rate_meter.as_ref())
            .is_some_and(|(limit, meter)| meter.calls_per_window(limit) == 0)
        {
            return Err(RpcError::InvalidConfig(
                "rate_limit is too small for the rate-meter window".into(),
            ));
        }

        let max_batch_call_size = config.max_batch_call_size.unwrap_or_else(|| {
            if let Some(rl) = rate_limit {
                (rl / 5.0).floor().max(1.0) as usize
            } else {
                usize::MAX
            }
        });
        if max_batch_call_size == 0 {
            return Err(RpcError::InvalidConfig(
                "max_batch_call_size must be greater than zero".into(),
            ));
        }
        if let Some((limit, meter)) = rate_limit.zip(rate_meter.as_ref()) {
            let calls_per_window = meter.calls_per_window(limit);
            if max_batch_call_size > calls_per_window {
                return Err(RpcError::InvalidConfig(format!(
                    "max_batch_call_size ({max_batch_call_size}) exceeds the {calls_per_window} calls admitted by one rate window"
                )));
            }
        }

        let rate = rate_limit
            .zip(rate_meter)
            .map(|(limit, meter)| Arc::new(Mutex::new(RateState { meter, limit })));

        let request_capacity = config.capacity.min(Semaphore::MAX_PERMITS);

        let transport = build_transport(
            &config.url,
            request_capacity,
            config.ws_pool_size.unwrap_or(DEFAULT_POOL_SIZE),
        )?;

        Ok(RpcClient {
            transport,
            max_batch_call_size,
            batch_concurrency: request_capacity,
            semaphore: Arc::new(Semaphore::new(request_capacity)),
            counter: Arc::new(AtomicU64::new(0)),
            rate,
            config: Arc::new(config),
            observer: None,
        })
    }

    /// Report every round trip, error and retry to `observer` (OB-4).
    pub fn with_observer(mut self, observer: Arc<dyn RpcObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    fn observed(&self, report: impl FnOnce(&dyn RpcObserver)) {
        if let Some(observer) = &self.observer {
            report(observer.as_ref());
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Single JSON-RPC call with retry.
    pub async fn call(
        &self,
        method: &str,
        params: Option<Value>,
        options: CallOptions,
    ) -> Result<Value, RpcError> {
        self.call_with_options(method, params.as_ref(), &options)
            .await
    }

    async fn call_with_options(
        &self,
        method: &str,
        params: Option<&Value>,
        options: &CallOptions,
    ) -> Result<Value, RpcError> {
        let retry_attempts = options.retry_attempts.unwrap_or(self.config.retry_attempts);
        let timeout = options.timeout.unwrap_or(self.config.request_timeout);

        let mut attempt = 0usize;
        loop {
            let id = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
            let result = self
                .execute_single(id, method, params, timeout, options)
                .await;

            match result {
                Ok(v) => return Ok(v),
                Err(e)
                    if attempt < retry_attempts
                        && e.is_retryable(self.config.retry_internal_server_errors) =>
                {
                    let pause = retry_pause(&self.config.retry_schedule, attempt);
                    self.observed(|o| o.on_retry(&e));
                    warn!(
                        method,
                        pause_ms = pause.as_millis(),
                        attempt,
                        "RPC call failed, retrying"
                    );
                    tokio::time::sleep(pause).await;
                    attempt += 1;
                }
                Err(e) => {
                    self.observed(|o| o.on_error(&e));
                    if attempt > 0 {
                        return Err(RpcError::RetryExhausted(Box::new(e)));
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Batch JSON-RPC call. Splits by `max_batch_call_size`, runs capped chunks
    /// concurrently under the shared rate/capacity limits, and preserves input
    /// order. Returns per-call Results (not all-or-nothing).
    pub async fn batch_call(
        &self,
        calls: Vec<(String, Option<Value>)>,
        options: &CallOptions,
    ) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        if calls.is_empty() {
            return Ok(vec![]);
        }

        let call_count = calls.len();
        let chunks = stream::iter(split_calls(calls, self.max_batch_call_size))
            .map(|chunk| async move { self.batch_chunk(&chunk, options).await })
            .buffered(self.batch_concurrency);
        futures::pin_mut!(chunks);
        let mut results: Vec<Result<Value, RpcError>> = Vec::with_capacity(call_count);
        while let Some(chunk_results) = chunks.next().await {
            let chunk_results = chunk_results?;
            results.extend(chunk_results);
        }

        Ok(results)
    }

    /// Batch call with reduce-on-retry: on a batch-retryable failure, split
    /// the batch in half recursively down to single calls (mirrors
    /// `reduceBatchOnRetry` in evm-rpc/src/rpc.ts).
    pub async fn batch_call_reduce_on_retry(
        &self,
        calls: Vec<(String, Option<Value>)>,
        options: &CallOptions,
    ) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        if calls.is_empty() {
            return Ok(vec![]);
        }

        let call_count = calls.len();
        let chunks = stream::iter(split_calls(calls, self.max_batch_call_size))
            .map(|chunk| self.reduce_batch(chunk, options))
            .buffered(self.batch_concurrency);
        futures::pin_mut!(chunks);
        let mut results = Vec::with_capacity(call_count);
        while let Some(chunk_results) = chunks.next().await {
            results.extend(chunk_results?);
        }
        Ok(results)
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    async fn execute_single(
        &self,
        id: u64,
        method: &str,
        params: Option<&Value>,
        timeout: Duration,
        options: &CallOptions,
    ) -> Result<Value, RpcError> {
        self.wait_for_rate(1).await;
        let _permit = self.semaphore.acquire().await.expect("semaphore closed");

        let req = OwnedRpcRequest {
            id,
            method: method.to_string(),
            params: params.cloned(),
        };

        self.observed(|o| o.on_request(RequestKind::Single, 1));
        let resp = self.transport.send_single(req, timeout).await?;
        self.process_response(resp, options)
    }

    async fn batch_chunk(
        &self,
        calls: &[(String, Option<Value>)],
        options: &CallOptions,
    ) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        if calls.is_empty() {
            return Ok(vec![]);
        }
        if calls.len() == 1 {
            let (method, params) = &calls[0];
            let r = self
                .call_with_options(method, params.as_ref(), options)
                .await;
            return Ok(vec![r]);
        }

        let retry_attempts = options.retry_attempts.unwrap_or(self.config.retry_attempts);
        let timeout = options.timeout.unwrap_or(self.config.request_timeout);

        let mut attempt = 0usize;
        loop {
            let result = self.execute_batch(calls, timeout, options).await;
            match result {
                Ok(v) => return Ok(v),
                Err(e)
                    if attempt < retry_attempts
                        && e.is_retryable(self.config.retry_internal_server_errors) =>
                {
                    let pause = retry_pause(&self.config.retry_schedule, attempt);
                    self.observed(|o| o.on_retry(&e));
                    warn!(
                        pause_ms = pause.as_millis(),
                        attempt, "RPC batch failed, retrying"
                    );
                    tokio::time::sleep(pause).await;
                    attempt += 1;
                }
                Err(e) => {
                    self.observed(|o| o.on_error(&e));
                    if attempt > 0 {
                        return Err(RpcError::RetryExhausted(Box::new(e)));
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn execute_batch(
        &self,
        calls: &[(String, Option<Value>)],
        timeout: Duration,
        options: &CallOptions,
    ) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        let count = calls.len();
        self.wait_for_rate(count).await;
        let _permit = self.semaphore.acquire().await.expect("semaphore closed");

        let base_id = self.counter.fetch_add(count as u64, Ordering::Relaxed) + 1;
        let requests: Vec<OwnedRpcRequest> = calls
            .iter()
            .enumerate()
            .map(|(i, (method, params))| OwnedRpcRequest {
                id: base_id + i as u64,
                method: method.clone(),
                params: params.clone(),
            })
            .collect();

        self.observed(|o| o.on_request(RequestKind::Batch, count));
        // The transport returns responses in request order (HTTP reorders via an
        // id→response map + length check; WS guarantees order by construction).
        let responses = self.transport.send_batch(requests, timeout).await?;

        let results: Vec<_> = responses
            .into_iter()
            .map(|resp| self.process_response(resp, options))
            .collect();

        // A per-item failure never reaches the retry ladder — the request
        // itself succeeded — so it is counted here or nowhere.
        for result in &results {
            if let Err(e) = result {
                self.observed(|o| o.on_error(e));
            }
        }

        Ok(results)
    }

    fn process_response(
        &self,
        resp: RpcResponse,
        options: &CallOptions,
    ) -> Result<Value, RpcError> {
        if let Some(err_info) = resp.error {
            if let Some(ve) = &options.validate_error {
                ve(&err_info)
            } else {
                Err(RpcError::from_info(err_info))
            }
        } else {
            let result = resp.result.unwrap_or(Value::Null);
            if let Some(vr) = &options.validate_result {
                vr(&result)
            } else {
                Ok(result)
            }
        }
    }

    /// Wait until the sliding-window rate allows `count` more items.
    ///
    /// Reservations are atomic, but differently sized concurrent callers have
    /// no fairness guarantee. Construction only guarantees that one capped
    /// batch fits when the configured window is otherwise empty.
    async fn wait_for_rate(&self, count: usize) {
        let Some(rate_arc) = &self.rate else { return };

        loop {
            let wait = {
                let mut rate = rate_arc.lock().await;
                let now_ms = now_millis();
                if rate.meter.get_rate(now_ms) + rate.meter.contribution(count as u64) <= rate.limit
                {
                    rate.meter.inc(count as u64, now_ms);
                    return;
                }
                Duration::from_millis(rate.meter.slot_time)
            };

            tokio::time::sleep(wait).await;
        }
    }

    // ── reduce-batch-on-retry ─────────────────────────────────────────────────

    #[async_recursion::async_recursion]
    async fn reduce_batch(
        &self,
        calls: Vec<(String, Option<Value>)>,
        options: &CallOptions,
    ) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        if calls.len() <= 1 {
            return self.batch_chunk(&calls, options).await;
        }

        let timeout = options.timeout.unwrap_or(self.config.request_timeout);
        let result = self.execute_batch(&calls, timeout, options).await;

        match result {
            Ok(v) => return Ok(v),
            Err(ref e) if e.is_retryable_batch(self.config.retry_internal_server_errors) => {
                self.observed(|o| o.on_retry(e));
                warn!(
                    batch_size = calls.len(),
                    "RPC batch failed, retrying with reduced batch"
                );
            }
            Err(e) => {
                self.observed(|o| o.on_error(&e));
                return Err(e);
            }
        }

        let mid = calls.len().div_ceil(2);
        let (left_calls, right_calls) = calls.split_at(mid);
        let (left, right) = tokio::join!(
            self.reduce_batch(left_calls.to_vec(), options),
            self.reduce_batch(right_calls.to_vec(), options),
        );

        let mut out = left?;
        out.extend(right?);
        Ok(out)
    }
}

/// Select and build a transport based on the URL scheme.
fn build_transport(
    url: &str,
    capacity: usize,
    ws_pool_size: usize,
) -> Result<Arc<dyn RpcTransport>, RpcError> {
    let parsed = url::Url::parse(url)
        .map_err(|e| RpcError::Protocol(format!("invalid RPC URL {url:?}: {e}")))?;
    match parsed.scheme() {
        "http" | "https" => Ok(Arc::new(HttpTransport::new(url.to_string(), capacity))),
        "ws" | "wss" => Ok(Arc::new(WsTransport::new(url.to_string(), ws_pool_size))),
        other => Err(RpcError::Protocol(format!(
            "unsupported RPC URL scheme {other:?} (expected http/https/ws/wss)"
        ))),
    }
}

fn retry_pause(schedule: &[Duration], attempt: usize) -> Duration {
    let idx = attempt.min(schedule.len().saturating_sub(1));
    schedule
        .get(idx)
        .copied()
        .unwrap_or(Duration::from_millis(20_000))
}

fn split_calls(
    calls: Vec<(String, Option<Value>)>,
    chunk_size: usize,
) -> impl Iterator<Item = Vec<(String, Option<Value>)>> {
    let mut calls = calls.into_iter();
    std::iter::from_fn(move || {
        let chunk: Vec<_> = calls.by_ref().take(chunk_size).collect();
        (!chunk.is_empty()).then_some(chunk)
    })
}

fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RpcErrorInfo;

    fn make_rpc_error(code: i64, message: &str) -> RpcError {
        RpcError::from_info(RpcErrorInfo {
            code,
            message: message.to_string(),
            data: None,
        })
    }

    // ── Retry classification ──────────────────────────────────────────────────

    #[test]
    fn retry_timeout() {
        assert!(RpcError::Timeout.is_retryable(false));
    }

    #[test]
    fn retry_protocol_not_retryable() {
        assert!(!RpcError::Protocol("x".into()).is_retryable(false));
    }

    #[test]
    fn retry_http_status() {
        for status in [408u16, 429, 502, 503, 504, 529] {
            assert!(
                RpcError::Http {
                    status,
                    body: String::new()
                }
                .is_retryable(false),
                "status {status} should be retryable"
            );
        }
        for status in [200u16, 400, 404, 500] {
            assert!(
                !RpcError::Http {
                    status,
                    body: String::new()
                }
                .is_retryable(false),
                "status {status} should NOT be retryable"
            );
        }
    }

    #[test]
    fn retry_rpc_codes() {
        assert!(make_rpc_error(-32005, "limit").is_retryable(false));
        assert!(make_rpc_error(429, "rate").is_retryable(false));
        assert!(!make_rpc_error(-32600, "invalid request").is_retryable(false));
    }

    #[test]
    fn retry_rate_limit_message() {
        assert!(make_rpc_error(1, "Rate limit exceeded").is_retryable(false));
        assert!(make_rpc_error(1, "Too many requests").is_retryable(false));
    }

    #[test]
    fn retry_execution_timeout() {
        assert!(make_rpc_error(1, "execution timeout exceeded").is_retryable(false));
    }

    #[test]
    fn retry_request_timed_out() {
        assert!(make_rpc_error(1, "request timed out").is_retryable(false));
        assert!(make_rpc_error(1, "request XYZ timed out").is_retryable(false));
    }

    #[test]
    fn retry_internal_errors_off() {
        assert!(!make_rpc_error(-32000, "internal error").is_retryable(false));
        assert!(!make_rpc_error(-32603, "internal server error").is_retryable(false));
    }

    #[test]
    fn retry_internal_errors_on() {
        assert!(make_rpc_error(-32000, "internal error").is_retryable(true));
        assert!(make_rpc_error(-32603, "something").is_retryable(true));
        assert!(make_rpc_error(-32001, "internal server error").is_retryable(true));
    }

    #[test]
    fn retry_error_variant() {
        assert!(RpcError::RetryRequested("hook".into()).is_retryable(false));
    }

    #[test]
    fn batch_retryable_adds_protocol_and_response_too_large() {
        let proto = RpcError::Protocol("oops".into());
        assert!(!proto.is_retryable(false));
        assert!(proto.is_retryable_batch(false));

        // "response too large" with code -32000: batch retryable but NOT plain retryable
        // (retry_internal_server_errors = false)
        let too_large = make_rpc_error(-32000, "response too large");
        assert!(!too_large.is_retryable(false));
        assert!(too_large.is_retryable_batch(false));
    }

    // ── Batch splitting ───────────────────────────────────────────────────────

    #[test]
    fn batch_size_explicit() {
        let config = RpcClientConfig {
            url: "http://localhost".into(),
            max_batch_call_size: Some(3),
            capacity: 10,
            ..Default::default()
        };
        let client = RpcClient::new(config);
        assert_eq!(client.max_batch_call_size, 3);
    }

    #[test]
    fn batch_size_from_rate_limit() {
        let config = RpcClientConfig {
            url: "http://localhost".into(),
            rate_limit: Some(50.0),
            capacity: 10,
            ..Default::default()
        };
        let client = RpcClient::new(config);
        // max(1, floor(50/5)) = 10
        assert_eq!(client.max_batch_call_size, 10);
    }

    #[test]
    fn batch_size_from_small_rate_limit() {
        let config = RpcClientConfig {
            url: "http://localhost".into(),
            rate_limit: Some(0.5), // < 1
            capacity: 10,
            ..Default::default()
        };
        let client = RpcClient::new(config);
        // max(1, floor(0.5/5)) = max(1, 0) = 1
        assert_eq!(client.max_batch_call_size, 1);
    }

    #[test]
    fn zero_batch_size_is_rejected() {
        let error = RpcClient::try_new(RpcClientConfig {
            url: "http://localhost".into(),
            max_batch_call_size: Some(0),
            ..Default::default()
        })
        .err()
        .expect("zero batch size must be rejected");
        assert!(
            matches!(error, RpcError::InvalidConfig(message) if message.contains("greater than zero"))
        );
    }

    #[test]
    fn batch_larger_than_rate_window_is_rejected() {
        let error = RpcClient::try_new(RpcClientConfig {
            url: "http://localhost".into(),
            max_batch_call_size: Some(50),
            rate_limit: Some(10.0),
            ..Default::default()
        })
        .err()
        .expect("an unsatisfiable batch must be rejected");
        assert!(
            matches!(error, RpcError::InvalidConfig(message) if message.contains("exceeds the 10 calls"))
        );
    }

    #[tokio::test]
    async fn fractional_rate_admits_one_call_per_extended_window() {
        let client = RpcClient::new(RpcClientConfig {
            url: "http://localhost".into(),
            rate_limit: Some(0.5),
            ..Default::default()
        });
        tokio::time::timeout(Duration::from_millis(50), client.wait_for_rate(1))
            .await
            .expect("a single call must be satisfiable at a fractional rate");
    }

    /// GAP-21: every concurrent waiter must check and reserve from one rate
    /// state transition. Queueing all waiters behind the state lock makes the
    /// old check-then-act race deterministic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_rate_waiters_reserve_budget_atomically() {
        const LIMIT: usize = 4;
        const CALLERS: usize = LIMIT * 2;

        let client = Arc::new(RpcClient::new(RpcClientConfig {
            url: "http://localhost".into(),
            capacity: CALLERS,
            rate_limit: Some(LIMIT as f64),
            ..Default::default()
        }));
        let rate = Arc::clone(client.rate.as_ref().expect("configured rate state"));
        let state_guard = rate.lock().await;
        let start = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let waiting = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let admitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(CALLERS);

        for _ in 0..CALLERS {
            let client = Arc::clone(&client);
            let start = Arc::clone(&start);
            let waiting = Arc::clone(&waiting);
            let admitted = Arc::clone(&admitted);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                waiting.fetch_add(1, Ordering::Relaxed);
                client.wait_for_rate(1).await;
                admitted.fetch_add(1, Ordering::Relaxed);
            }));
        }

        start.wait().await;
        while waiting.load(Ordering::Relaxed) != CALLERS {
            tokio::task::yield_now().await;
        }
        // Give the final waiter a poll so every first lock acquisition is
        // queued before the guard is released.
        tokio::task::yield_now().await;
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            admitted.load(Ordering::Relaxed),
            LIMIT,
            "one rolling window must not admit more than its configured budget"
        );

        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
}
