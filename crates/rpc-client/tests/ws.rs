//! WebSocket transport integration tests.
//!
//! Each test spins up an in-process `tokio-tungstenite` mock server with a
//! scripted behavior, points an `RpcClient` at its `ws://` address, and drives
//! the public client API.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rpc_client::{CallOptions, RpcClient, RpcClientConfig, RpcError};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

type WsServer = WebSocketStream<TcpStream>;

/// Bind a server socket and return its `ws://` URL plus the listener.
async fn bind() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (format!("ws://{addr}"), listener)
}

/// Accept one connection and run `handler` on the upgraded WS stream.
async fn serve_one<F, Fut>(listener: TcpListener, handler: F)
where
    F: FnOnce(WsServer) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        handler(ws).await;
    });
}

fn client(url: &str) -> RpcClient {
    RpcClient::new(RpcClientConfig {
        url: url.to_string(),
        capacity: 8,
        retry_attempts: 0,
        ..Default::default()
    })
}

/// Parse a single incoming text frame as JSON.
fn parse(msg: &Message) -> Value {
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}

fn ok_response(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

// ── single call ────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_call() {
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let req = parse(&msg);
        let id = req["id"].clone();
        assert_eq!(req["method"], "eth_blockNumber");
        ws.send(Message::Text(ok_response(&id, json!("0x10")).to_string()))
            .await
            .unwrap();
    })
    .await;

    let client = client(&url);
    let res = client
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(res, json!("0x10"));
}

// ── batch in request order + server-reordered batch ──────────────────────────

#[tokio::test]
async fn batch_request_order_even_when_reordered() {
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let reqs = parse(&msg);
        let arr = reqs.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // Respond REVERSED to exercise the request-order guarantee.
        let mut responses: Vec<Value> = arr
            .iter()
            .map(|r| {
                ok_response(
                    &r["id"],
                    json!(format!("m={}", r["method"].as_str().unwrap())),
                )
            })
            .collect();
        responses.reverse();
        ws.send(Message::Text(Value::Array(responses).to_string()))
            .await
            .unwrap();
    })
    .await;

    let client = client(&url);
    let calls = vec![
        ("a".to_string(), None),
        ("b".to_string(), None),
        ("c".to_string(), None),
    ];
    let res = client
        .batch_call(calls, &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(res[0].as_ref().unwrap(), &json!("m=a"));
    assert_eq!(res[1].as_ref().unwrap(), &json!("m=b"));
    assert_eq!(res[2].as_ref().unwrap(), &json!("m=c"));
}

// ── cross-batch interleaved arrival ──────────────────────────────────────────

#[tokio::test]
async fn cross_batch_interleaved_arrival() {
    // Single connection, two concurrent batches; the server answers each item
    // individually and interleaves them across the two batches.
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let mut singles: Vec<(Value, String)> = Vec::new();
        // Two batch frames are expected.
        for _ in 0..2 {
            let msg = ws.next().await.unwrap().unwrap();
            let reqs = parse(&msg);
            for r in reqs.as_array().unwrap() {
                let method = r["method"].as_str().unwrap().to_string();
                singles.push((r["id"].clone(), method));
            }
        }
        // Interleave: send one frame per item, mixed order.
        singles.sort_by_key(|(id, _)| id.as_u64().unwrap() % 2); // crude reshuffle
        for (id, method) in singles {
            ws.send(Message::Text(ok_response(&id, json!(method)).to_string()))
                .await
                .unwrap();
        }
    })
    .await;

    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        retry_attempts: 0,
        // Pin both batches to a single shared connection to exercise id routing
        // of interleaved cross-batch responses.
        ws_pool_size: Some(1),
        ..Default::default()
    }));
    let c1 = client.clone();
    let c2 = client.clone();
    let h1 = tokio::spawn(async move {
        c1.batch_call(
            vec![("a1".into(), None), ("a2".into(), None)],
            &CallOptions::default(),
        )
        .await
        .unwrap()
    });
    let h2 = tokio::spawn(async move {
        c2.batch_call(
            vec![("b1".into(), None), ("b2".into(), None)],
            &CallOptions::default(),
        )
        .await
        .unwrap()
    });
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    assert_eq!(r1[0].as_ref().unwrap(), &json!("a1"));
    assert_eq!(r1[1].as_ref().unwrap(), &json!("a2"));
    assert_eq!(r2[0].as_ref().unwrap(), &json!("b1"));
    assert_eq!(r2[1].as_ref().unwrap(), &json!("b2"));
}

// ── error response + validate_error path ─────────────────────────────────────

#[tokio::test]
async fn error_response_maps_to_rpc_error() {
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let req = parse(&msg);
        let resp = json!({
            "jsonrpc": "2.0",
            "id": req["id"],
            "error": {"code": -32602, "message": "invalid params"}
        });
        ws.send(Message::Text(resp.to_string())).await.unwrap();
    })
    .await;

    let client = client(&url);
    let err = client
        .call("boom", None, CallOptions::default())
        .await
        .unwrap_err();
    match err {
        RpcError::Rpc { code, message, .. } => {
            assert_eq!(code, -32602);
            assert_eq!(message, "invalid params");
        }
        other => panic!("expected RpcError::Rpc, got {other:?}"),
    }
}

#[tokio::test]
async fn validate_error_hook_runs() {
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let req = parse(&msg);
        let resp = json!({
            "jsonrpc": "2.0",
            "id": req["id"],
            "error": {"code": 123, "message": "soft"}
        });
        ws.send(Message::Text(resp.to_string())).await.unwrap();
    })
    .await;

    let client = client(&url);
    let options = CallOptions {
        validate_error: Some(Box::new(|_info| Ok(json!("overridden")))),
        ..Default::default()
    };
    let res = client.call("x", None, options).await.unwrap();
    assert_eq!(res, json!("overridden"));
}

// ── request timeout resets connection; sibling retries ───────────────────────

#[tokio::test]
async fn timeout_resets_connection_and_sibling_retries() {
    // On connection 0 the server NEVER answers "slow" and holds "sibling"
    // unanswered too (so the sibling is in-flight on the same socket when the
    // slow request times out and resets the connection). On connection 1 (the
    // reconnect) the server answers everything. We assert: the slow call times
    // out + retries to success, AND the concurrent sibling — failed by the
    // reset — retries and succeeds on the fresh connection.
    let (url, listener) = bind().await;
    let conn_count = Arc::new(AtomicUsize::new(0));
    let cc = conn_count.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let n = cc.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                while let Some(Ok(msg)) = ws.next().await {
                    if !msg.is_text() {
                        continue;
                    }
                    let req = parse(&msg);
                    let method = req["method"].as_str().unwrap();
                    if n == 0 {
                        // First connection: drop EVERYTHING on the floor so the
                        // "slow" call times out (resetting the connection) and
                        // the in-flight "sibling" is failed by that reset.
                        continue;
                    }
                    let _ = ws
                        .send(Message::Text(
                            ok_response(&req["id"], json!(method)).to_string(),
                        ))
                        .await;
                }
            });
        }
    });

    // Single connection so both requests share one socket and the slow request's
    // timeout-reset is what fails the sibling.
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: url.clone(),
        capacity: 8,
        retry_attempts: 3,
        request_timeout: Duration::from_millis(300),
        retry_schedule: vec![Duration::from_millis(10)],
        ws_pool_size: Some(1),
        ..Default::default()
    }));

    // Issue the slow call and a concurrent SIBLING call on the same connection.
    let c_slow = client.clone();
    let c_sib = client.clone();
    let slow = tokio::spawn(async move { c_slow.call("slow", None, CallOptions::default()).await });
    let sibling =
        tokio::spawn(async move { c_sib.call("sibling", None, CallOptions::default()).await });

    let slow_res = slow
        .await
        .unwrap()
        .expect("slow should succeed after retry");
    let sib_res = sibling
        .await
        .unwrap()
        .expect("sibling should be failed by the reset, then retried to success");

    assert_eq!(slow_res, json!("slow"));
    assert_eq!(sib_res, json!("sibling"));
    assert!(
        conn_count.load(Ordering::SeqCst) >= 2,
        "should have reconnected after the timeout reset"
    );
}

// ── server closes mid-flight → retryable → reconnect succeeds ─────────────────

#[tokio::test]
async fn server_close_midflight_then_retry_succeeds() {
    let (url, listener) = bind().await;
    let conn_count = Arc::new(AtomicUsize::new(0));
    let cc = conn_count.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let n = cc.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let msg = ws.next().await.unwrap().unwrap();
                let req = parse(&msg);
                if n == 0 {
                    // Close the socket without answering.
                    let _ = ws.close(None).await;
                } else {
                    let _ = ws
                        .send(Message::Text(
                            ok_response(&req["id"], json!("ok")).to_string(),
                        ))
                        .await;
                }
            });
        }
    });

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        retry_attempts: 3,
        retry_schedule: vec![Duration::from_millis(10)],
        ..Default::default()
    });

    let res = client
        .call("ping", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(res, json!("ok"));
    assert!(conn_count.load(Ordering::SeqCst) >= 2);
}

// ── lone batch-error frame (id:null) fails the batch fast ─────────────────────

#[tokio::test]
async fn lone_batch_error_frame_fails_fast() {
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let _reqs = parse(&msg);
        // Whole-batch error object instead of an array.
        let resp = json!({
            "jsonrpc": "2.0",
            "id": Value::Null,
            "error": {"code": -32600, "message": "invalid batch"}
        });
        ws.send(Message::Text(resp.to_string())).await.unwrap();
        // Keep the connection open; the batch must NOT wait for a timeout.
        let _ = ws.next().await;
    })
    .await;

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        retry_attempts: 0,
        // Long timeout: if the implementation waits for it, the test hangs past
        // the harness timeout, proving the fast-fail requirement.
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    });

    let started = std::time::Instant::now();
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        client.batch_call(
            vec![("a".into(), None), ("b".into(), None)],
            &CallOptions::default(),
        ),
    )
    .await
    .expect("batch must fail fast, not wait for the per-request timeout");
    let elapsed = started.elapsed();

    // Promptness: well under the 30s per-request timeout.
    assert!(
        elapsed < Duration::from_secs(5),
        "batch should fail promptly on a whole-batch error frame, took {elapsed:?}"
    );
    // It must fail as a batch/disconnect error (the whole-batch error frame
    // resets the connection), NOT a per-request timeout and NOT a silent hang.
    match res {
        Err(RpcError::Disconnected(_)) => {}
        other => panic!("expected RpcError::Disconnected from connection reset, got {other:?}"),
    }
}

// ── short array reply fails the batch fast (C2) ───────────────────────────────

#[tokio::test]
async fn short_array_batch_response_fails_fast() {
    // Server returns an array with FEWER responses than requested, without
    // closing. The missing id must fail promptly, not hang to the timeout.
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let reqs = parse(&msg);
        let arr = reqs.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // Answer only the first two ids; omit the third.
        let responses: Vec<Value> = arr
            .iter()
            .take(2)
            .map(|r| ok_response(&r["id"], json!("ok")))
            .collect();
        ws.send(Message::Text(Value::Array(responses).to_string()))
            .await
            .unwrap();
        // Keep the connection open: a correct implementation must NOT wait.
        let _ = ws.next().await;
    })
    .await;

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        retry_attempts: 0,
        // Long timeout: if the batch waits for it, the outer timeout fires.
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    });

    let started = std::time::Instant::now();
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        client.batch_call(
            vec![("a".into(), None), ("b".into(), None), ("c".into(), None)],
            &CallOptions::default(),
        ),
    )
    .await
    .expect("short-array batch must fail fast, not wait for the timeout");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "short-array batch should fail promptly, took {elapsed:?}"
    );
    // A short array (missing ids, connection still up) is a protocol fault.
    match res {
        Err(RpcError::Protocol(_)) => {}
        other => panic!("expected RpcError::Protocol for a short array reply, got {other:?}"),
    }
}

// ── notification / null-id frame ignored without breaking the map ─────────────

#[tokio::test]
async fn notification_and_null_id_frames_are_ignored() {
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let req = parse(&msg);
        // Send a notification (has `method`, no id) and a null-id non-error
        // frame first; neither should disturb routing.
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0","method":"eth_subscription","params":{"x":1}}).to_string(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0","id":null,"result":null}).to_string(),
        ))
        .await
        .unwrap();
        // Then the real response.
        ws.send(Message::Text(
            ok_response(&req["id"], json!("real")).to_string(),
        ))
        .await
        .unwrap();
    })
    .await;

    let client = client(&url);
    let res = client
        .call("x", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(res, json!("real"));
}

// ── Ping → Pong reply ─────────────────────────────────────────────────────────

#[tokio::test]
async fn ping_gets_pong() {
    let (url, listener) = bind().await;
    let (tx, rx) = tokio::sync::oneshot::channel::<bool>();
    serve_one(listener, |mut ws| async move {
        // Wait for the call frame, send a ping, expect a pong back.
        let msg = ws.next().await.unwrap().unwrap();
        let req = parse(&msg);
        ws.send(Message::Ping(vec![1, 2, 3])).await.unwrap();
        let mut got_pong = false;
        // Answer the call and watch for the pong (order not guaranteed).
        ws.send(Message::Text(
            ok_response(&req["id"], json!("ok")).to_string(),
        ))
        .await
        .unwrap();
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
                Ok(Some(Ok(Message::Pong(p)))) => {
                    got_pong = p == vec![1, 2, 3];
                    break;
                }
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }
        let _ = tx.send(got_pong);
    })
    .await;

    let client = client(&url);
    let res = client
        .call("x", None, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(res, json!("ok"));
    let got_pong = tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .expect("server timed out waiting for pong")
        .unwrap();
    assert!(got_pong, "client did not reply Pong to server Ping");
}

// ── single-flight dial under concurrent first calls ──────────────────────────

#[tokio::test]
async fn single_flight_dial_under_concurrent_first_calls() {
    // Pool size 1: a burst of concurrent first callers must join ONE dial, not
    // dial per caller. Assert exactly one accepted connection.
    let (url, listener) = bind().await;
    let conn_count = Arc::new(AtomicUsize::new(0));
    let cc = conn_count.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            cc.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                while let Some(Ok(msg)) = ws.next().await {
                    if !msg.is_text() {
                        continue;
                    }
                    let req = parse(&msg);
                    let _ = ws
                        .send(Message::Text(
                            ok_response(&req["id"], json!("ok")).to_string(),
                        ))
                        .await;
                }
            });
        }
    });

    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url,
        capacity: 64,
        retry_attempts: 0,
        ws_pool_size: Some(1),
        ..Default::default()
    }));
    let mut handles = Vec::new();
    // Burst of 32 concurrent first calls onto a single-connection pool.
    for _ in 0..32 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.call("x", None, CallOptions::default()).await.unwrap()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), json!("ok"));
    }
    // Single-flight dial: exactly one connection opened, not one per caller.
    let n = conn_count.load(Ordering::SeqCst);
    assert_eq!(n, 1, "expected exactly one dial (single-flight), got {n}");
}

// ── unknown-id response frame → connection reset ──────────────────────────────

#[tokio::test]
async fn unknown_id_response_resets_connection() {
    // The server replies with an id that was never requested. That is protocol
    // corruption (the id map is desynced) → the connection must be reset, which
    // fails the in-flight request with a retryable Disconnected error.
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let _msg = ws.next().await.unwrap().unwrap();
        // Respond for an id the client never sent (ids start at 1).
        ws.send(Message::Text(
            ok_response(&json!(999_999), json!("stray")).to_string(),
        ))
        .await
        .unwrap();
        let _ = ws.next().await;
    })
    .await;

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        retry_attempts: 0,
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    });

    let res = tokio::time::timeout(
        Duration::from_secs(3),
        client.call("x", None, CallOptions::default()),
    )
    .await
    .expect("unknown-id frame must reset promptly, not wait for the timeout");
    match res {
        Err(RpcError::Disconnected(_)) => {}
        other => panic!("expected Disconnected from unknown-id reset, got {other:?}"),
    }
}

// ── binary frame → connection reset ───────────────────────────────────────────

#[tokio::test]
async fn binary_frame_resets_connection() {
    // A binary frame is a protocol error (we only speak text JSON) → reset,
    // which fails the in-flight request with a retryable Disconnected error.
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let _msg = ws.next().await.unwrap().unwrap();
        ws.send(Message::Binary(vec![0xde, 0xad, 0xbe, 0xef]))
            .await
            .unwrap();
        let _ = ws.next().await;
    })
    .await;

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        retry_attempts: 0,
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    });

    let res = tokio::time::timeout(
        Duration::from_secs(3),
        client.call("x", None, CallOptions::default()),
    )
    .await
    .expect("binary frame must reset promptly, not wait for the timeout");
    match res {
        Err(RpcError::Disconnected(_)) => {}
        other => panic!("expected Disconnected from binary-frame reset, got {other:?}"),
    }
}

// ── non-numeric (string) id response handled without corrupting the map ───────

#[tokio::test]
async fn string_id_response_is_ignored_not_collapsed() {
    // A response carrying a STRING id must not be collapsed to 0 (which would
    // wrongly satisfy/clobber a numeric-0 entry) nor reset the connection — it
    // is logged and dropped. The real numeric-id response that follows must
    // still route correctly, proving the pending map is intact.
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let req = parse(&msg);
        // First: a stray string-id frame (e.g. a subscription-style id).
        ws.send(Message::Text(
            ok_response(&json!("sub-abc"), json!("noise")).to_string(),
        ))
        .await
        .unwrap();
        // Then: the genuine numeric-id response.
        ws.send(Message::Text(
            ok_response(&req["id"], json!("real")).to_string(),
        ))
        .await
        .unwrap();
        let _ = ws.next().await;
    })
    .await;

    let client = client(&url);
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        client.call("x", None, CallOptions::default()),
    )
    .await
    .expect("string-id frame must not stall routing")
    .unwrap();
    assert_eq!(res, json!("real"));
}

// ── per-item error within a batch is per-item, not whole-batch ────────────────

#[tokio::test]
async fn per_item_error_in_batch_is_per_item() {
    // The server replies to a 3-item batch with a well-formed array where ONE
    // element carries a JSON-RPC error and the other two carry results. The
    // batch must succeed as a whole (`Ok`) with the error surfacing only on the
    // offending item; its siblings stay `Ok`.
    let (url, listener) = bind().await;
    serve_one(listener, |mut ws| async move {
        let msg = ws.next().await.unwrap().unwrap();
        let reqs = parse(&msg);
        let arr = reqs.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // Element index 1 (method "b") fails; 0 and 2 succeed.
        let responses: Vec<Value> = arr
            .iter()
            .map(|r| {
                let method = r["method"].as_str().unwrap();
                if method == "b" {
                    json!({
                        "jsonrpc": "2.0",
                        "id": r["id"],
                        "error": {"code": -32099, "message": "item boom"}
                    })
                } else {
                    ok_response(&r["id"], json!(format!("m={method}")))
                }
            })
            .collect();
        ws.send(Message::Text(Value::Array(responses).to_string()))
            .await
            .unwrap();
        // Keep the socket open: the whole batch must NOT be failed/reset.
        let _ = ws.next().await;
    })
    .await;

    let client = client(&url);
    let res = client
        .batch_call(
            vec![("a".into(), None), ("b".into(), None), ("c".into(), None)],
            &CallOptions::default(),
        )
        .await
        .expect("batch with a per-item error must still return Ok overall");

    assert_eq!(res.len(), 3);
    // Siblings succeed.
    assert_eq!(res[0].as_ref().unwrap(), &json!("m=a"));
    assert_eq!(res[2].as_ref().unwrap(), &json!("m=c"));
    // The offending item surfaces as a per-item RPC error, not a batch failure.
    match res[1].as_ref().unwrap_err() {
        RpcError::Rpc { code, message, .. } => {
            assert_eq!(*code, -32099);
            assert_eq!(message, "item boom");
        }
        other => panic!("expected per-item RpcError::Rpc, got {other:?}"),
    }
}

// ── pending map is clean after a per-request timeout reset ────────────────────

#[tokio::test]
async fn pending_map_clean_after_timeout() {
    // Connection 0 swallows the request so it times out, which resets the
    // connection (advancing the generation and draining the pending map). A
    // subsequent call must dial connection 1 and succeed — proving no leaked /
    // half-state survived the reset. Pinned to a single connection (pool size 1)
    // to keep it non-racy, like `timeout_resets_connection_and_sibling_retries`.
    let (url, listener) = bind().await;
    let conn_count = Arc::new(AtomicUsize::new(0));
    let cc = conn_count.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let n = cc.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                while let Some(Ok(msg)) = ws.next().await {
                    if !msg.is_text() {
                        continue;
                    }
                    let req = parse(&msg);
                    let method = req["method"].as_str().unwrap();
                    if n == 0 {
                        // First connection: never answer → the call times out and
                        // resets the connection.
                        continue;
                    }
                    let _ = ws
                        .send(Message::Text(
                            ok_response(&req["id"], json!(method)).to_string(),
                        ))
                        .await;
                }
            });
        }
    });

    let client = RpcClient::new(RpcClientConfig {
        url,
        capacity: 8,
        // No automatic retry: drive the two attempts by hand so we can assert the
        // FIRST call times out and the SECOND (fresh-connection) call succeeds.
        retry_attempts: 0,
        request_timeout: Duration::from_millis(300),
        ws_pool_size: Some(1),
        ..Default::default()
    });

    // First call: times out on connection 0, which tears the connection down.
    let first = client.call("slow", None, CallOptions::default()).await;
    match first {
        Err(RpcError::Timeout) => {}
        other => panic!("expected the first call to time out, got {other:?}"),
    }

    // Second call on a FRESH connection: if any half-state had leaked (a dangling
    // responder, a stale `active` handle, a poisoned generation) this would hang
    // or fail. It must route and succeed promptly.
    let second = tokio::time::timeout(
        Duration::from_secs(3),
        client.call("fresh", None, CallOptions::default()),
    )
    .await
    .expect("second call must complete promptly on a fresh connection")
    .expect("second call must succeed");
    assert_eq!(second, json!("fresh"));

    // The timeout reset forced a reconnect → the connection generation advanced
    // (observable here as a second accepted connection on the pinned pool).
    assert!(
        conn_count.load(Ordering::SeqCst) >= 2,
        "timeout reset should have advanced the connection (reconnect)"
    );
}
