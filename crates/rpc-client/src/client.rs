use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tracing::warn;

use crate::error::{RpcError, RpcErrorInfo};
use crate::rate::RateMeter;

// ─── Wire types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a Value>,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    id: Option<Value>,
    result: Option<Value>,
    error: Option<RpcErrorInfo>,
}

// ─── Config ──────────────────────────────────────────────────────────────────

/// Configuration for `RpcClient`.
#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    /// HTTP endpoint URL
    pub url: String,
    /// Max number of JSON-RPC calls in one HTTP batch array.
    /// Defaults to unlimited (or `max(1, rate_limit/5)` when rate_limit is set).
    pub max_batch_call_size: Option<usize>,
    /// Max concurrent in-flight HTTP requests.
    pub capacity: usize,
    /// Max calls per second (rate counted by batch items, not requests).
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
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        RpcClientConfig {
            url: String::new(),
            max_batch_call_size: None,
            capacity: 10,
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
    url: Arc<String>,
    http: reqwest::Client,
    config: Arc<RpcClientConfig>,
    max_batch_call_size: usize,
    semaphore: Arc<Semaphore>,
    counter: Arc<AtomicU64>,
    rate: Option<Arc<Mutex<RateState>>>,
}

impl RpcClient {
    pub fn new(config: RpcClientConfig) -> Self {
        let rate_limit = config.rate_limit;

        let max_batch_call_size = config.max_batch_call_size.unwrap_or_else(|| {
            if let Some(rl) = rate_limit {
                (rl / 5.0).floor().max(1.0) as usize
            } else {
                usize::MAX
            }
        });

        let rate = rate_limit.map(|rl| {
            let window_size = 10usize;
            let slot_time = if rl < 1.0 {
                (1000.0 / (rl * window_size as f64)).ceil() as u64
            } else {
                100u64
            };
            Arc::new(Mutex::new(RateState {
                meter: RateMeter::new(window_size, slot_time),
                limit: rl,
            }))
        });

        // Keep connections warm and reused. A fresh HTTPS connection pays the
        // TCP + TLS handshake and starts with a cold congestion window, so a
        // reused connection is faster, especially for large receipts payloads.
        // TCP keepalive stops the provider's load balancer / NAT from silently
        // dropping idle connections (which would force such a reconnect), and a
        // generous idle timeout keeps the pool warm through quieter chains.
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(config.capacity.min(64))
            .pool_idle_timeout(Duration::from_secs(120))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");

        RpcClient {
            url: Arc::new(config.url.clone()),
            http,
            max_batch_call_size,
            semaphore: Arc::new(Semaphore::new(config.capacity.min(Semaphore::MAX_PERMITS))),
            counter: Arc::new(AtomicU64::new(0)),
            rate,
            config: Arc::new(config),
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
        let retry_attempts = options.retry_attempts.unwrap_or(self.config.retry_attempts);
        let timeout = options.timeout.unwrap_or(self.config.request_timeout);

        let mut attempt = 0usize;
        loop {
            let id = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
            let result = self
                .execute_single(id, method, params.as_ref(), timeout, &options)
                .await;

            match result {
                Ok(v) => return Ok(v),
                Err(e)
                    if attempt < retry_attempts
                        && e.is_retryable(self.config.retry_internal_server_errors) =>
                {
                    let pause = retry_pause(&self.config.retry_schedule, attempt);
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
                    if attempt > 0 {
                        return Err(RpcError::RetryExhausted(Box::new(e)));
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Batch JSON-RPC call. Splits by `max_batch_call_size`, respects rate and
    /// capacity limits.  Returns per-call Results (not all-or-nothing).
    pub async fn batch_call(
        &self,
        calls: Vec<(String, Option<Value>)>,
        options: &CallOptions,
    ) -> Result<Vec<Result<Value, RpcError>>, RpcError> {
        if calls.is_empty() {
            return Ok(vec![]);
        }

        let chunk_size = self.max_batch_call_size;
        let mut results: Vec<Result<Value, RpcError>> = Vec::with_capacity(calls.len());

        for chunk in calls.chunks(chunk_size) {
            let chunk_results = self.batch_chunk(chunk, options).await?;
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
        self.reduce_batch(calls, options).await
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

        let req = RpcRequest {
            jsonrpc: "2.0",
            id,
            method,
            params,
        };
        let body = serde_json::to_vec(&req).expect("serialize");

        let raw = self.post_raw(body, timeout).await?;
        let resp: RpcResponse = serde_json::from_slice(&raw)
            .map_err(|e| RpcError::Protocol(format!("invalid JSON: {e}")))?;

        let resp_id = resp.id.as_ref().and_then(|v| v.as_u64()).unwrap_or(0);
        if resp_id != id {
            return Err(RpcError::Protocol(format!(
                "Got response for unknown request {resp_id}"
            )));
        }

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
                .call(
                    method,
                    params.clone(),
                    CallOptions {
                        priority: options.priority,
                        retry_attempts: options.retry_attempts,
                        timeout: options.timeout,
                        validate_result: None,
                        validate_error: None,
                    },
                )
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
                    warn!(
                        pause_ms = pause.as_millis(),
                        attempt, "RPC batch failed, retrying"
                    );
                    tokio::time::sleep(pause).await;
                    attempt += 1;
                }
                Err(e) => {
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
        let requests: Vec<RpcRequest<'_>> = calls
            .iter()
            .enumerate()
            .map(|(i, (method, params))| RpcRequest {
                jsonrpc: "2.0",
                id: base_id + i as u64,
                method: method.as_str(),
                params: params.as_ref(),
            })
            .collect();

        let body = serde_json::to_vec(&requests).expect("serialize");
        let raw = self.post_raw(body, timeout).await?;

        let responses: Vec<RpcResponse> = serde_json::from_slice(&raw)
            .map_err(|e| RpcError::Protocol(format!("invalid JSON in batch response: {e}")))?;

        if responses.len() != count {
            return Err(RpcError::Protocol(format!(
                "Invalid length of a batch response: expected {count}, got {}",
                responses.len()
            )));
        }

        // Build id→response map (server may reorder, as in TS http.ts)
        let mut map: HashMap<u64, RpcResponse> = responses
            .into_iter()
            .map(|r| {
                let rid = r.id.as_ref().and_then(|v| v.as_u64()).unwrap_or(0);
                (rid, r)
            })
            .collect();

        let mut results = Vec::with_capacity(count);
        for i in 0..count {
            let id = base_id + i as u64;
            let resp = map.remove(&id).ok_or_else(|| {
                RpcError::Protocol(format!("Missing result for call id {id} in batch response"))
            })?;
            results.push(self.process_response(resp, options));
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

    async fn post_raw(&self, body: Vec<u8>, timeout: Duration) -> Result<Vec<u8>, RpcError> {
        let mut req = self
            .http
            .post(self.url.as_str())
            .header("content-type", "application/json")
            .body(body);

        if !timeout.is_zero() {
            req = req.timeout(timeout);
        }

        let response = req.send().await.map_err(|e| {
            if e.is_timeout() {
                RpcError::Timeout
            } else {
                RpcError::Connection(e)
            }
        })?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(RpcError::Http {
                status,
                body: body_text,
            });
        }

        response.bytes().await.map(|b| b.to_vec()).map_err(|e| {
            if e.is_timeout() {
                RpcError::Timeout
            } else {
                RpcError::Connection(e)
            }
        })
    }

    /// Wait until the sliding-window rate allows `count` more items.
    async fn wait_for_rate(&self, count: usize) {
        let Some(rate_arc) = &self.rate else { return };

        loop {
            let now_ms = now_millis();

            let (current_rate, slot_time_ms, limit) = {
                let r = rate_arc.lock().await;
                (r.meter.get_rate(now_ms), r.meter.slot_time, r.limit)
            };

            if current_rate + count as f64 <= limit {
                let mut r = rate_arc.lock().await;
                r.meter.inc(count as u64, now_millis());
                return;
            }

            tokio::time::sleep(Duration::from_millis(slot_time_ms)).await;
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
                warn!(
                    batch_size = calls.len(),
                    "RPC batch failed, retrying with reduced batch"
                );
            }
            Err(e) => return Err(e),
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

fn retry_pause(schedule: &[Duration], attempt: usize) -> Duration {
    let idx = attempt.min(schedule.len().saturating_sub(1));
    schedule
        .get(idx)
        .copied()
        .unwrap_or(Duration::from_millis(20_000))
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
}
