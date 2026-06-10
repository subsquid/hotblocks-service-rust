//! Axum HTTP application — exact port of `http-app.ts`.

use crate::metrics::get_block_ingestion_timestamp;
use crate::service::DataService;
use crate::source::DataSource;
use crate::types::{Block, DataResponse, InvalidBaseBlock};
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

pub type SharedService<S> = Arc<DataService<S>>;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamRequest {
    from_block: u64,
    parent_block_hash: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreviousBlocksResponse {
    previous_blocks: Vec<crate::types::BlockRef>,
}

#[derive(Debug, Deserialize)]
struct JsonQueryParam {
    json: Option<String>,
}

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

pub fn build_router<S: DataSource>(service: Arc<DataService<S>>) -> Router {
    Router::new()
        .route("/", get(handle_root::<S>))
        .route("/head", get(handle_head::<S>))
        .route("/finalized-head", get(handle_finalized_head::<S>))
        .route("/readiness", get(handle_readiness::<S>))
        .route("/stream", post(handle_stream::<S>))
        .route("/metrics", get(handle_metrics::<S>))
        .route("/metrics/{name}", get(handle_metrics_name::<S>))
        .route("/block-time/{height}", get(handle_block_time::<S>))
        .with_state(service)
        // Enforce a 1024-byte body limit on POST /stream.
        .layer(axum::extract::DefaultBodyLimit::max(1024))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_root<S: DataSource>(_state: State<SharedService<S>>) -> impl IntoResponse {
    (StatusCode::OK, "Welcome to hot block data service!")
}

async fn handle_head<S: DataSource>(state: State<SharedService<S>>) -> impl IntoResponse {
    Json(state.get_head())
}

async fn handle_finalized_head<S: DataSource>(
    state: State<SharedService<S>>,
) -> impl IntoResponse {
    Json(state.get_finalized_head())
}

async fn handle_readiness<S: DataSource>(state: State<SharedService<S>>) -> impl IntoResponse {
    if state.is_ready().await {
        (StatusCode::OK, "true")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "false")
    }
}

async fn handle_metrics<S: DataSource>(
    state: State<SharedService<S>>,
    Query(params): Query<JsonQueryParam>,
) -> impl IntoResponse {
    if params.json.as_deref() == Some("true") {
        let json = state.metrics.gather_json();
        Json(json).into_response()
    } else {
        match state.metrics.gather_text() {
            Ok(text) => (
                StatusCode::OK,
                [(
                    "content-type",
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                text,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("metrics error: {e}"),
            )
                .into_response(),
        }
    }
}

async fn handle_metrics_name<S: DataSource>(
    state: State<SharedService<S>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.metrics.get_single_metric_text(&name) {
        Some(text) => (StatusCode::OK, text).into_response(),
        None => (StatusCode::NOT_FOUND, "requested metric not found").into_response(),
    }
}

async fn handle_block_time<S: DataSource>(
    _state: State<SharedService<S>>,
    Path(height): Path<String>,
) -> impl IntoResponse {
    match get_block_ingestion_timestamp(&height) {
        Some(ts) => (StatusCode::OK, ts.to_string()).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "Timestamp not found for the specified block",
        )
            .into_response(),
    }
}

/// POST /stream
///
/// Body: `{"fromBlock": <nat>, "parentBlockHash"?: <string>}` (≤ 1024 bytes)
///
/// Response:
/// - 400 on bad request
/// - 409 JSON `{"previousBlocks": [...]}` on base-block mismatch
/// - 204 if nothing to send
/// - 200 streaming body (zstd or gzip per Accept-Encoding)
async fn handle_stream<S: DataSource>(
    state: State<SharedService<S>>,
    req: Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let accept_encoding = parts
        .headers
        .get("accept-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Read body (≤ 1024 bytes enforced by DefaultBodyLimit layer).
    let body_bytes = match axum::body::to_bytes(body, 1024).await {
        Ok(b) => b,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "request body too large or unreadable")
                .into_response()
        }
    };

    let stream_req: StreamRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid request: {e}")).into_response()
        }
    };

    let start = Instant::now();
    let max_duration = std::time::Duration::from_secs(60);

    let result = state
        .query(stream_req.from_block, stream_req.parent_block_hash.as_deref())
        .await;

    let data_response: DataResponse = match result {
        Err(InvalidBaseBlock { prev }) => {
            return (
                StatusCode::CONFLICT,
                Json(PreviousBlocksResponse {
                    previous_blocks: prev,
                }),
            )
                .into_response()
        }
        Ok(r) => r,
    };

    // Build response headers.
    let mut resp_headers = HeaderMap::new();

    if let Some(fh) = &data_response.finalized_head {
        resp_headers.insert(
            "x-sqd-finalized-head-number",
            HeaderValue::from_str(&fh.number.to_string()).unwrap(),
        );
        resp_headers.insert(
            "x-sqd-finalized-head-hash",
            HeaderValue::from_str(&fh.hash).unwrap(),
        );
    }

    let has_content =
        data_response.head.is_some() || data_response.tail.as_ref().map(|t| !t.is_empty()).unwrap_or(false);

    if !has_content {
        return (StatusCode::NO_CONTENT, resp_headers).into_response();
    }

    let use_zstd = accept_encoding.contains("zstd");

    resp_headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=UTF-8"),
    );
    resp_headers.insert(
        "content-encoding",
        HeaderValue::from_static(if use_zstd { "zstd" } else { "gzip" }),
    );
    resp_headers.insert("vary", HeaderValue::from_static("Accept-Encoding"));

    // Stream blocks via a channel.
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(32);

    let head = data_response.head;
    let tail = data_response.tail.unwrap_or_default();

    tokio::spawn(async move {
        // Helper: encode a single block payload.
        async fn encode_block(block: &Block, use_zstd: bool) -> anyhow::Result<Bytes> {
            if use_zstd {
                Ok(block.json_line_zstd.clone())
            } else {
                // Decompress zstd, then recompress gzip level 1.
                let raw = tokio::task::spawn_blocking({
                    let zstd_bytes = block.json_line_zstd.clone();
                    move || {
                        zstd::decode_all(std::io::Cursor::new(zstd_bytes.as_ref()))
                    }
                })
                .await??;

                let gz = tokio::task::spawn_blocking(move || {
                    let mut enc = GzEncoder::new(Vec::new(), Compression::new(1));
                    enc.write_all(&raw)?;
                    enc.finish()
                })
                .await??;

                Ok(Bytes::from(gz))
            }
        }

        // Head batches (backfill stream).
        if let Some(mut head_stream) = head {
            while let Some(batch_result) = head_stream.next().await {
                let blocks = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(%e, "head stream error during HTTP stream");
                        return;
                    }
                };
                for block in &blocks {
                    if start.elapsed() > max_duration {
                        return;
                    }
                    match encode_block(block, use_zstd).await {
                        Ok(payload) => {
                            if tx.send(Ok(payload)).await.is_err() {
                                return; // Client disconnected.
                            }
                            tracing::debug!(
                                stage = "block-served",
                                source = "head",
                                block_number = block.number,
                                block_hash = %block.hash,
                                "block served {}#{}", block.number, block.hash
                            );
                        }
                        Err(e) => {
                            tracing::error!(%e, "encoding error");
                            return;
                        }
                    }
                }
                if start.elapsed() > max_duration {
                    return;
                }
            }
        }

        // Tail blocks (snapshot).
        for block in &tail {
            if start.elapsed() > max_duration {
                return;
            }
            match encode_block(block, use_zstd).await {
                Ok(payload) => {
                    if tx.send(Ok(payload)).await.is_err() {
                        return;
                    }
                    tracing::debug!(
                        stage = "block-served",
                        source = "tail",
                        block_number = block.number,
                        block_hash = %block.hash,
                        "block served {}#{}", block.number, block.hash
                    );
                }
                Err(e) => {
                    tracing::error!(%e, "encoding error");
                    return;
                }
            }
        }
    });

    // Convert the mpsc receiver into an axum Body.
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    (StatusCode::OK, resp_headers, body).into_response()
}
