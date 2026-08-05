//! The adapter's upstream finality view (ADR-18): what it bounds strides by and
//! reports, against a fleet that does not answer with one voice.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, response::IntoResponse, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use evm_source::fetch::{Rpc, RpcOptions};
use rpc_client::{RpcClient, RpcClientConfig};

/// Answers the n-th `finalized` read with `script[n]`, after that entry's delay.
/// Reads past the end repeat the last entry.
async fn scripted_finality(script: Vec<(u64, Duration)>) -> String {
    #[derive(Clone)]
    struct Script {
        entries: Arc<Vec<(u64, Duration)>>,
        served: Arc<AtomicUsize>,
    }

    async fn handle(State(script): State<Script>, body: axum::body::Bytes) -> impl IntoResponse {
        let req: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        let id = req.get("id").cloned().unwrap_or(json!(1));

        let nth = script.served.fetch_add(1, Ordering::SeqCst);
        let (number, delay) = script.entries[nth.min(script.entries.len() - 1)];
        tokio::time::sleep(delay).await;

        axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": { "number": format!("0x{number:x}"), "hash": format!("0x{number:064x}") },
        }))
    }

    let state = Script {
        entries: Arc::new(script),
        served: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new().route("/", post(handle)).with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://127.0.0.1:{port}")
}

fn rpc_over(url: String) -> Arc<Rpc> {
    Arc::new(Rpc::new(
        Arc::new(RpcClient::new(RpcClientConfig {
            url,
            capacity: 5,
            retry_attempts: 0,
            ..Default::default()
        })),
        RpcOptions::default(),
    ))
}

/// An expired view is fetched by both paths at once — the ingest-side refresh
/// and the prober's own miss — so answers can land in either order. Within an
/// epoch the view is a maximum, as WP-12 is for the reports it feeds; taking the
/// last answer instead lets one lagging replica pull the stride bound and the
/// probe filter back for a whole TTL.
#[tokio::test]
async fn a_lagging_answer_landing_last_does_not_lower_the_view() {
    // Whichever path reaches the upstream first draws the low, slow answer, so
    // the higher one always lands first and the lower one last.
    let url = scripted_finality(vec![
        (1_550, Duration::from_millis(300)),
        (1_600, Duration::from_millis(10)),
    ])
    .await;
    let rpc = rpc_over(url);

    rpc.refresh_finalized_head();
    let _ = rpc.get_finalized_head_cached().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        rpc.finalized_head_hint().map(|(number, _)| number),
        Some(1_600),
        "the later, lower answer replaced the higher one"
    );
}

/// The maximum is the epoch's, not the process's: T1 opens the next epoch and
/// its read is authoritative, however far below the last one it lands (WP-20).
#[tokio::test]
async fn a_reseed_lowers_the_view_the_maximum_would_have_held() {
    let url = scripted_finality(vec![
        (1_600, Duration::from_millis(0)),
        (1_200, Duration::from_millis(0)),
    ])
    .await;
    let rpc = rpc_over(url);

    assert_eq!(
        rpc.get_finalized_head_cached().await.expect("first read").0,
        1_600
    );
    assert_eq!(rpc.resync_finalized_head().await.expect("T1 read").0, 1_200);
    assert_eq!(
        rpc.finalized_head_hint().map(|(number, _)| number),
        Some(1_200),
        "the epoch that ended kept its view alive"
    );
}
