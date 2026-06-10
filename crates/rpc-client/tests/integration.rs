//! Integration tests against a local mock JSON-RPC HTTP server (axum).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use rpc_client::{CallOptions, RpcClient, RpcClientConfig};

// ─── Mock server helpers ──────────────────────────────────────────────────────

/// Spawn a mock server; returns the base URL.
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
        retry_attempts: 3,
        ..Default::default()
    })
}

// ─── Simple echo handler ──────────────────────────────────────────────────────

async fn echo_handler(Json(body): Json<Value>) -> Json<Value> {
    if let Some(arr) = body.as_array() {
        // batch: respond to each with its id and a result
        let responses: Vec<Value> = arr
            .iter()
            .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": req["method"] }))
            .collect();
        Json(Value::Array(responses))
    } else {
        Json(json!({
            "jsonrpc": "2.0",
            "id": body["id"],
            "result": body["method"]
        }))
    }
}

#[tokio::test]
async fn test_single_call_success() {
    let app = Router::new().route("/", post(echo_handler));
    let url = spawn_server(app).await;
    let c = client(&url);

    let result = c
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(result, json!("eth_blockNumber"));
}

// ─── Batch with out-of-order response ids ────────────────────────────────────

async fn reorder_handler(Json(body): Json<Value>) -> Json<Value> {
    if let Some(arr) = body.as_array() {
        // Respond in REVERSE order to test id-based reordering
        let mut responses: Vec<Value> = arr
            .iter()
            .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": req["method"] }))
            .collect();
        responses.reverse();
        Json(Value::Array(responses))
    } else {
        Json(json!({ "jsonrpc": "2.0", "id": body["id"], "result": body["method"] }))
    }
}

#[tokio::test]
async fn test_batch_out_of_order_ids() {
    let app = Router::new().route("/", post(reorder_handler));
    let url = spawn_server(app).await;
    let c = RpcClient::new(RpcClientConfig {
        url: url.clone(),
        capacity: 4,
        max_batch_call_size: Some(100),
        ..Default::default()
    });

    let calls = vec![
        (
            "eth_getBlockByNumber".to_string(),
            Some(json!(["0x1", true])),
        ),
        ("eth_getLogs".to_string(), Some(json!([{}]))),
        ("eth_chainId".to_string(), None),
    ];

    let results = c.batch_call(calls, &CallOptions::default()).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap(), &json!("eth_getBlockByNumber"));
    assert_eq!(results[1].as_ref().unwrap(), &json!("eth_getLogs"));
    assert_eq!(results[2].as_ref().unwrap(), &json!("eth_chainId"));
}

// ─── 429 then success retry ──────────────────────────────────────────────────

#[derive(Clone)]
struct RetryState {
    attempts: Arc<AtomicUsize>,
}

async fn retry_429_handler(
    State(state): State<RetryState>,
    Json(body): Json<Value>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let n = state.attempts.fetch_add(1, Ordering::SeqCst);
    if n == 0 {
        // First attempt: return 429
        (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response()
    } else {
        // Second attempt: success
        let resp = json!({ "jsonrpc": "2.0", "id": body["id"], "result": "ok" });
        Json(resp).into_response()
    }
}

#[tokio::test]
async fn test_retry_on_429() {
    let state = RetryState {
        attempts: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/", post(retry_429_handler))
        .with_state(state.clone());
    let url = spawn_server(app).await;

    let c = RpcClient::new(RpcClientConfig {
        url: url.clone(),
        capacity: 4,
        retry_attempts: 2,
        retry_schedule: vec![
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
        ],
        ..Default::default()
    });

    let result = c
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(result, json!("ok"));
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
}

// ─── RPC error -32005 retried ─────────────────────────────────────────────────

#[derive(Clone)]
struct RpcRetryState {
    attempts: Arc<AtomicUsize>,
}

async fn rpc_32005_handler(
    State(state): State<RpcRetryState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let n = state.attempts.fetch_add(1, Ordering::SeqCst);
    if n == 0 {
        Json(json!({
            "jsonrpc": "2.0",
            "id": body["id"],
            "error": { "code": -32005, "message": "limit exceeded" }
        }))
    } else {
        Json(json!({ "jsonrpc": "2.0", "id": body["id"], "result": "ok" }))
    }
}

#[tokio::test]
async fn test_rpc_32005_retried() {
    let state = RpcRetryState {
        attempts: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/", post(rpc_32005_handler))
        .with_state(state.clone());
    let url = spawn_server(app).await;

    let c = RpcClient::new(RpcClientConfig {
        url: url.clone(),
        capacity: 4,
        retry_attempts: 2,
        retry_schedule: vec![std::time::Duration::from_millis(1)],
        ..Default::default()
    });

    let result = c
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(result, json!("ok"));
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
}

// ─── Non-retryable error not retried ─────────────────────────────────────────

#[derive(Clone)]
struct NonRetryState {
    attempts: Arc<AtomicUsize>,
}

async fn non_retryable_handler(
    State(state): State<NonRetryState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    state.attempts.fetch_add(1, Ordering::SeqCst);
    Json(json!({
        "jsonrpc": "2.0",
        "id": body["id"],
        "error": { "code": -32600, "message": "Invalid request" }
    }))
}

#[tokio::test]
async fn test_non_retryable_not_retried() {
    let state = NonRetryState {
        attempts: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/", post(non_retryable_handler))
        .with_state(state.clone());
    let url = spawn_server(app).await;

    let c = RpcClient::new(RpcClientConfig {
        url: url.clone(),
        capacity: 4,
        retry_attempts: 3,
        retry_schedule: vec![std::time::Duration::from_millis(1)],
        ..Default::default()
    });

    let result = c
        .call("eth_blockNumber", None, CallOptions::default())
        .await;
    assert!(result.is_err());
    // Should only have been called once (no retries)
    assert_eq!(state.attempts.load(Ordering::SeqCst), 1);
}

// ─── Batch reduce-on-retry splitting ─────────────────────────────────────────

#[derive(Clone)]
struct ReduceState {
    calls: Arc<tokio::sync::Mutex<Vec<usize>>>, // batch sizes seen
}

async fn reduce_handler(
    State(state): State<ReduceState>,
    Json(body): Json<Value>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    if let Some(arr) = body.as_array() {
        let size = arr.len();
        state.calls.lock().await.push(size);
        if size > 2 {
            // pretend too large
            return (
                StatusCode::OK,
                axum::Json(json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32000, "message": "response too large" }
                })),
            )
                .into_response();
        }
        // respond normally
        let responses: Vec<Value> = arr
            .iter()
            .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": "ok" }))
            .collect();
        Json(Value::Array(responses)).into_response()
    } else {
        state.calls.lock().await.push(1);
        Json(json!({ "jsonrpc": "2.0", "id": body["id"], "result": "ok" })).into_response()
    }
}

#[tokio::test]
async fn test_batch_reduce_on_retry() {
    let state = ReduceState {
        calls: Arc::new(tokio::sync::Mutex::new(vec![])),
    };
    let app = Router::new()
        .route("/", post(reduce_handler))
        .with_state(state.clone());
    let url = spawn_server(app).await;

    let c = RpcClient::new(RpcClientConfig {
        url: url.clone(),
        capacity: 8,
        max_batch_call_size: Some(100),
        ..Default::default()
    });

    // 4 calls: first attempt as batch of 4 → fails → split to 2+2 → each succeeds
    let calls: Vec<(String, Option<Value>)> =
        (0..4).map(|i| (format!("method_{i}"), None)).collect();

    let results = c
        .batch_call_reduce_on_retry(calls, &CallOptions::default())
        .await
        .unwrap();

    assert_eq!(results.len(), 4);
    for r in &results {
        assert_eq!(r.as_ref().unwrap(), &json!("ok"));
    }

    let seen = state.calls.lock().await.clone();
    // Should have seen batch of 4 (fails), then 2 sub-batches
    assert!(seen.contains(&4), "expected batch of 4, got {seen:?}");
    assert!(
        seen.iter().filter(|&&s| s == 2).count() >= 2,
        "expected two batches of 2, got {seen:?}"
    );
}
