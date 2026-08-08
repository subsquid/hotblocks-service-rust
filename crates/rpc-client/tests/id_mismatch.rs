//! Providers and proxies commonly answer HTTP 200 with a single JSON-RPC
//! error envelope carrying `id: null` (rate limits, oversized requests,
//! gateway failures). Mirrors TS transport/http.ts: such an envelope is the
//! real server error, not a protocol violation, so retry classification
//! applies to it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use rpc_client::{CallOptions, RpcClient, RpcClientConfig, RpcError};

async fn spawn_server(router: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://127.0.0.1:{}", addr.port())
}

fn client(url: &str) -> RpcClient {
    RpcClient::new(RpcClientConfig {
        url: url.to_string(),
        capacity: 4,
        max_batch_call_size: Some(100),
        retry_attempts: 3,
        retry_schedule: vec![std::time::Duration::from_millis(1)],
        ..Default::default()
    })
}

#[derive(Clone)]
struct EnvelopeState {
    requests: Arc<AtomicUsize>,
    error_code: i64,
    /// Envelopes are served while requests_seen < fail_first.
    fail_first: usize,
    /// Batch sizes observed per request; None = single (non-array) request.
    shapes: Arc<tokio::sync::Mutex<Vec<Option<usize>>>>,
}

impl EnvelopeState {
    fn new(error_code: i64, fail_first: usize) -> Self {
        EnvelopeState {
            requests: Arc::new(AtomicUsize::new(0)),
            error_code,
            fail_first,
            shapes: Arc::new(tokio::sync::Mutex::new(vec![])),
        }
    }
}

async fn null_id_envelope_handler(
    State(state): State<EnvelopeState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let n = state.requests.fetch_add(1, Ordering::SeqCst);
    state
        .shapes
        .lock()
        .await
        .push(body.as_array().map(Vec::len));

    if n < state.fail_first {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": state.error_code, "message": "capacity exceeded" }
        }));
    }

    if let Some(arr) = body.as_array() {
        let responses: Vec<Value> = arr
            .iter()
            .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": "ok" }))
            .collect();
        Json(Value::Array(responses))
    } else {
        Json(json!({ "jsonrpc": "2.0", "id": body["id"], "result": "ok" }))
    }
}

fn envelope_app(state: EnvelopeState) -> Router {
    Router::new()
        .route("/", post(null_id_envelope_handler))
        .with_state(state)
}

// ─── (a) single call: retryable null-id envelope heals via retry ─────────────

#[tokio::test]
async fn single_call_null_id_error_envelope_is_retried() {
    let state = EnvelopeState::new(-32005, 1);
    let url = spawn_server(envelope_app(state.clone())).await;

    let result = client(&url)
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .unwrap();

    assert_eq!(result, json!("ok"));
    assert!(
        state.requests.load(Ordering::SeqCst) > 1,
        "the rate-limit envelope must be retried, not fail on attempt 0"
    );
}

// ─── (b) single call: non-retryable envelope surfaces the RPC error ──────────

#[tokio::test]
async fn single_call_null_id_error_envelope_surfaces_the_server_error() {
    let state = EnvelopeState::new(-32600, usize::MAX);
    let url = spawn_server(envelope_app(state.clone())).await;

    let error = client(&url)
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .expect_err("a non-retryable server error must fail the call");

    assert!(
        matches!(error, RpcError::Rpc { code: -32600, .. }),
        "expected the server's RPC error, got {error:?}"
    );
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);
}

// ─── (c) batch: retryable null-id envelope retries the whole batch ───────────

#[tokio::test]
async fn batch_null_id_error_envelope_retries_the_whole_batch() {
    let state = EnvelopeState::new(-32005, 1);
    let url = spawn_server(envelope_app(state.clone())).await;

    let calls: Vec<(String, Option<Value>)> =
        (0..3).map(|i| (format!("method_{i}"), None)).collect();
    let results = client(&url)
        .batch_call(calls, &CallOptions::default())
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    for result in &results {
        assert_eq!(result.as_ref().unwrap(), &json!("ok"));
    }
    assert_eq!(
        state.requests.load(Ordering::SeqCst),
        2,
        "the throttled batch must be retried whole, without splitting"
    );
    let shapes = state.shapes.lock().await.clone();
    assert_eq!(
        shapes,
        vec![Some(3), Some(3)],
        "no single-call fan-out against a throttling provider"
    );
}

// ─── (d) reduce-on-retry: throttling retries the whole batch, no splitting ───

#[tokio::test]
async fn reduce_on_retry_rate_limit_retries_whole_batch() {
    let state = EnvelopeState::new(-32005, 1);
    let url = spawn_server(envelope_app(state.clone())).await;

    let calls: Vec<(String, Option<Value>)> =
        (0..4).map(|i| (format!("method_{i}"), None)).collect();
    let results = client(&url)
        .batch_call_reduce_on_retry(calls, &CallOptions::default())
        .await
        .unwrap();

    assert_eq!(results.len(), 4);
    for result in &results {
        assert_eq!(result.as_ref().unwrap(), &json!("ok"));
    }
    let shapes = state.shapes.lock().await.clone();
    assert_eq!(
        shapes,
        vec![Some(4), Some(4)],
        "a throttled batch must be retried whole, never split"
    );
}

// ─── (e) reduce-on-retry: "response too large" splits the batch ──────────────

#[derive(Clone)]
struct TooLargeState {
    requests: Arc<AtomicUsize>,
    shapes: Arc<tokio::sync::Mutex<Vec<Option<usize>>>>,
    /// Batches of at least this many calls answer "response too large".
    too_large_at: usize,
}

async fn too_large_handler(
    State(state): State<TooLargeState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    state.requests.fetch_add(1, Ordering::SeqCst);
    let len = body.as_array().map(Vec::len);
    state.shapes.lock().await.push(len);

    if len.is_some_and(|n| n >= state.too_large_at) {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32000, "message": "response too large" }
        }));
    }

    if let Some(arr) = body.as_array() {
        let responses: Vec<Value> = arr
            .iter()
            .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": "ok" }))
            .collect();
        Json(Value::Array(responses))
    } else {
        Json(json!({ "jsonrpc": "2.0", "id": body["id"], "result": "ok" }))
    }
}

#[tokio::test]
async fn reduce_on_retry_splits_on_response_too_large() {
    let state = TooLargeState {
        requests: Arc::new(AtomicUsize::new(0)),
        shapes: Arc::new(tokio::sync::Mutex::new(vec![])),
        too_large_at: 4,
    };
    let app = Router::new()
        .route("/", post(too_large_handler))
        .with_state(state.clone());
    let url = spawn_server(app).await;

    let calls: Vec<(String, Option<Value>)> =
        (0..4).map(|i| (format!("method_{i}"), None)).collect();
    let results = client(&url)
        .batch_call_reduce_on_retry(calls, &CallOptions::default())
        .await
        .unwrap();

    assert_eq!(results.len(), 4);
    for result in &results {
        assert_eq!(result.as_ref().unwrap(), &json!("ok"));
    }
    let shapes = state.shapes.lock().await.clone();
    assert_eq!(
        shapes,
        vec![Some(4), Some(2), Some(2)],
        "a batch-shape failure must split, not resend the whole batch"
    );
}

// ─── (e2) reduce-on-retry: shape failure splits even when -32000 is
//         ordinary-retryable via retry_internal_server_errors ────────────────

#[tokio::test]
async fn reduce_on_retry_splits_on_response_too_large_with_internal_retries_on() {
    let state = TooLargeState {
        requests: Arc::new(AtomicUsize::new(0)),
        shapes: Arc::new(tokio::sync::Mutex::new(vec![])),
        too_large_at: 4,
    };
    let app = Router::new()
        .route("/", post(too_large_handler))
        .with_state(state.clone());
    let url = spawn_server(app).await;

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 4,
        max_batch_call_size: Some(100),
        retry_attempts: 3,
        retry_schedule: vec![std::time::Duration::from_millis(1)],
        retry_internal_server_errors: true,
        ..Default::default()
    });

    let calls: Vec<(String, Option<Value>)> =
        (0..4).map(|i| (format!("method_{i}"), None)).collect();
    let results = client
        .batch_call_reduce_on_retry(calls, &CallOptions::default())
        .await
        .expect("shape failure must split into passing halves, not exhaust whole");

    assert_eq!(results.len(), 4);
    for result in &results {
        assert_eq!(result.as_ref().unwrap(), &json!("ok"));
    }
    let shapes = state.shapes.lock().await.clone();
    assert_eq!(
        shapes,
        vec![Some(4), Some(2), Some(2)],
        "a batch-shape failure must split even when -32000 is ordinary-retryable"
    );
}

// ─── (f) reduce-on-retry: persistent throttling exhausts without splitting ───

#[tokio::test]
async fn reduce_on_retry_persistent_rate_limit_exhausts_without_splitting() {
    let state = EnvelopeState::new(-32005, usize::MAX);
    let url = spawn_server(envelope_app(state.clone())).await;

    let calls: Vec<(String, Option<Value>)> =
        (0..4).map(|i| (format!("method_{i}"), None)).collect();
    let error = client(&url)
        .batch_call_reduce_on_retry(calls, &CallOptions::default())
        .await
        .expect_err("a persistently throttled batch must fail, not fan out");

    assert!(
        matches!(
            &error,
            RpcError::RetryExhausted(inner)
                if matches!(**inner, RpcError::Rpc { code: -32005, .. })
        ),
        "expected the rate-limit error after the retry ladder, got {error:?}"
    );
    // client() configures retry_attempts = 3: 1 initial + 3 retries.
    assert_eq!(state.requests.load(Ordering::SeqCst), 4);
    let shapes = state.shapes.lock().await.clone();
    assert_eq!(
        shapes,
        vec![Some(4); 4],
        "every attempt must be the whole batch"
    );
}
