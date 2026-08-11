//! Backend affinity at the client boundary: what is sent, what is taken up,
//! and what a split batch does with it.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use rpc_client::{CallOptions, RpcClient, RpcClientConfig, UpstreamSession};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// What the endpoint saw, and what it named in return.
#[derive(Default)]
struct Endpoint {
    /// The `cookie` header of each request, in arrival order.
    received: Vec<Option<String>>,
    /// Assignments to hand out, oldest first. Empty = an endpoint with no
    /// notion of backends.
    hands_out: Vec<String>,
    next: usize,
}

#[derive(Clone)]
struct EndpointState(Arc<Mutex<Endpoint>>);

async fn endpoint(
    State(state): State<EndpointState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let cookie = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = request.is_array();
    let calls = request.as_array().cloned().unwrap_or_else(|| vec![request]);

    let assigned = {
        let mut endpoint = state.0.lock().expect("endpoint");
        endpoint.received.push(cookie.clone());
        // Silence once a request names one: that is "still alive".
        match (cookie.is_some(), endpoint.next < endpoint.hands_out.len()) {
            (false, true) => {
                let id = endpoint.hands_out[endpoint.next].clone();
                endpoint.next += 1;
                Some(id)
            }
            _ => None,
        }
    };

    let responses: Vec<Value> = calls
        .iter()
        .map(|call| {
            let id = call.get("id").cloned().unwrap_or(json!(1));
            json!({"jsonrpc": "2.0", "id": id, "result": "0x1"})
        })
        .collect();
    let payload = if batched {
        serde_json::to_vec(&responses)
    } else {
        serde_json::to_vec(&responses[0])
    }
    .expect("serialize");

    let mut out = axum::http::HeaderMap::new();
    out.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    if let Some(id) = assigned {
        out.insert(
            axum::http::header::SET_COOKIE,
            // Attributes and all: the client keeps the pair, drops the rest.
            format!("NODE={id}; Path=/; HttpOnly; SameSite=Lax")
                .parse()
                .unwrap(),
        );
    }
    (axum::http::StatusCode::OK, out, payload)
}

async fn serve(hands_out: &[&str]) -> (String, Arc<Mutex<Endpoint>>) {
    let state = Arc::new(Mutex::new(Endpoint {
        hands_out: hands_out.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }));
    let app = Router::new()
        .route("/", post(endpoint))
        .with_state(EndpointState(Arc::clone(&state)));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://127.0.0.1:{}", addr.port()), state)
}

fn received(state: &Arc<Mutex<Endpoint>>) -> Vec<Option<String>> {
    state.lock().expect("endpoint").received.clone()
}

fn calls(count: usize) -> Vec<(String, Option<Value>)> {
    (0..count)
        .map(|i| (format!("method_{i}"), Some(json!([i]))))
        .collect()
}

/// Splitting is internal to one logical call, so every chunk — concurrent ones
/// included — must reach the same backend.
#[tokio::test]
async fn a_split_batch_carries_one_session_across_every_chunk() {
    let (url, state) = serve(&["a"]).await;
    let client = RpcClient::new(RpcClientConfig {
        url,
        // One call per round trip, several in flight: eight concurrent asks.
        max_batch_call_size: Some(1),
        capacity: 8,
        retry_attempts: 0,
        ..Default::default()
    });

    let session = UpstreamSession::new();
    client
        .call(
            "eth_chainId",
            None,
            CallOptions {
                session: Some(session.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("the first call is assigned a backend");
    assert_eq!(session.pinned().as_deref(), Some("NODE=a"));

    let options = CallOptions {
        session: Some(session.clone()),
        ..Default::default()
    };
    let results = client.batch_call(calls(8), &options).await.expect("batch");
    assert_eq!(results.len(), 8);

    let seen = received(&state);
    assert_eq!(seen.len(), 9, "one pinning call plus eight chunks");
    assert!(
        seen[1..].iter().all(|c| c.as_deref() == Some("NODE=a")),
        "every chunk names the same backend: {seen:?}"
    );
}

/// An endpoint with no notion of backends is left exactly as it was.
#[tokio::test]
async fn an_endpoint_that_names_nothing_changes_nothing() {
    let (url, state) = serve(&[]).await;
    let client = RpcClient::new(RpcClientConfig {
        url,
        retry_attempts: 0,
        ..Default::default()
    });

    let session = UpstreamSession::new();
    for _ in 0..3 {
        client
            .call(
                "eth_blockNumber",
                None,
                CallOptions {
                    session: Some(session.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("call");
    }

    assert!(session.pinned().is_none());
    assert!(
        received(&state).iter().all(Option::is_none),
        "nothing may be sent that the endpoint never named"
    );
}

/// The answer carries its own correction: a fresh assignment means we were
/// moved, and the id in hand is the new one.
#[tokio::test]
async fn a_reassignment_is_taken_up() {
    let (url, state) = serve(&["a", "b"]).await;
    let client = RpcClient::new(RpcClientConfig {
        url,
        retry_attempts: 0,
        ..Default::default()
    });

    let session = UpstreamSession::new();
    let ask = |session: UpstreamSession| {
        let options = CallOptions {
            session: Some(session),
            ..Default::default()
        };
        client.call("eth_blockNumber", None, options)
    };

    ask(session.clone()).await.expect("assigned");
    assert_eq!(session.pinned().as_deref(), Some("NODE=a"));

    // A second unbound binding draws the next backend.
    let other = UpstreamSession::new();
    ask(other.clone()).await.expect("assigned again");
    assert_eq!(other.pinned().as_deref(), Some("NODE=b"));

    // Silence left the first binding standing.
    ask(session.clone()).await.expect("served where it was");
    assert_eq!(session.pinned().as_deref(), Some("NODE=a"));

    let seen = received(&state);
    assert_eq!(
        seen,
        vec![None, None, Some("NODE=a".to_string())],
        "an unbound call names nothing; a bound one names its backend"
    );
}

/// A socket is already pinned to whoever accepted it: nothing to name, and
/// the frame must not change.
#[tokio::test]
async fn the_ws_transport_ignores_the_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}");

    let frames = Arc::new(Mutex::new(Vec::<Value>::new()));
    let recorded = Arc::clone(&frames);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        for _ in 0..2 {
            let msg = ws.next().await.unwrap().unwrap();
            let req: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            let id = req["id"].clone();
            recorded.lock().expect("frames").push(req);
            ws.send(Message::Text(
                json!({"jsonrpc": "2.0", "id": id, "result": "0x1"}).to_string(),
            ))
            .await
            .unwrap();
        }
    });

    let client = RpcClient::new(RpcClientConfig {
        url,
        ws_pool_size: Some(1),
        retry_attempts: 0,
        ..Default::default()
    });

    let session = UpstreamSession::new();
    client
        .call("eth_blockNumber", None, CallOptions::default())
        .await
        .expect("plain call");
    client
        .call(
            "eth_blockNumber",
            None,
            CallOptions {
                session: Some(session.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("call carrying a binding");

    let sent = frames.lock().expect("frames").clone();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["method"], sent[1]["method"]);
    assert_eq!(sent[0]["params"], sent[1]["params"]);
    assert_eq!(
        sent[0].as_object().unwrap().keys().collect::<Vec<_>>(),
        sent[1].as_object().unwrap().keys().collect::<Vec<_>>(),
        "the binding adds nothing to a WS frame"
    );
    assert!(session.pinned().is_none());
}
