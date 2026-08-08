//! Axum HTTP application — exact port of `http-app.ts`.

use crate::metrics::{get_block_ingestion_timestamp, QueryClass, TruncationCause};
use crate::service::DataService;
use crate::source::DataSource;
use crate::types::{Block, InvalidBaseBlock, QueryError};
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
use std::error::Error as _;
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
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&service),
            note_acceptance::<S>,
        ))
        .with_state(service)
        // Enforce a 1024-byte body limit on POST /stream.
        .layer(axum::extract::DefaultBodyLimit::max(1024))
}

/// OB-9's first-acceptance timestamp: the moment a request was answered, which
/// is what LIV-5 bounds. Binding the listener proves nothing about reachability.
async fn note_acceptance<S: DataSource>(
    State(state): State<SharedService<S>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    state.metrics.record_first_acceptance();
    next.run(request).await
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

async fn handle_finalized_head<S: DataSource>(state: State<SharedService<S>>) -> impl IntoResponse {
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
        match state.metrics.gather_json() {
            Ok(json) => Json(json).into_response(),
            Err(error) => {
                tracing::error!(?error, "failed to serialize metrics as JSON");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "metrics serialization error",
                )
                    .into_response()
            }
        }
    } else {
        match state.metrics.gather_text() {
            Ok(text) => (
                StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
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
async fn handle_stream<S: DataSource>(state: State<SharedService<S>>, req: Request) -> Response {
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
        Err(error) => {
            let too_large = error
                .source()
                .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
            return if too_large {
                (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response()
            } else {
                (StatusCode::BAD_REQUEST, "request body unreadable").into_response()
            };
        }
    };

    let stream_req: StreamRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid request: {e}")).into_response()
        }
    };

    let start = Instant::now();
    let max_duration = state.response_budget();

    // GAP-31: the verdict wait itself is bounded — a source that never yields
    // its first batch must not hold the request open past the budget. Nothing
    // was produced, so an expiry here is RP-12's empty form; dropping the
    // query future releases the acquisition stream.
    let result = match tokio::time::timeout(
        max_duration,
        state.query(
            stream_req.from_block,
            stream_req.parent_block_hash.as_deref(),
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            state.record_query_outcome(QueryClass::WaitEmpty, start.elapsed());
            let mut resp_headers = HeaderMap::new();
            insert_finalized_head(&mut resp_headers, &state.get_finalized_head());
            return (StatusCode::NO_CONTENT, resp_headers).into_response();
        }
    };

    // Settling the outcome only where the request actually ends keeps a query
    // that 500s off the success counters (OB-5) and puts the whole admission
    // window, first record included, in its duration.
    let settle = |class: QueryClass| state.record_query_outcome(class, start.elapsed());

    let (data_response, class) = match result {
        Err(QueryError::InvalidBaseBlock(InvalidBaseBlock { prev })) => {
            settle(QueryClass::Conflict);
            return (
                StatusCode::CONFLICT,
                Json(PreviousBlocksResponse {
                    previous_blocks: prev,
                }),
            )
                .into_response();
        }
        Err(QueryError::Internal(e)) => {
            settle(QueryClass::Error);
            tracing::error!(%e, "internal error while servicing /stream query");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error\n\n{e:#}"),
            )
                .into_response();
        }
        Ok(outcome) => (outcome.response, outcome.class),
    };

    // Build response headers.
    let mut resp_headers = HeaderMap::new();

    if let Some(fh) = &data_response.finalized_head {
        insert_finalized_head(&mut resp_headers, fh);
    }

    let has_content = data_response.head.is_some()
        || data_response
            .tail
            .as_ref()
            .map(|t| !t.is_empty())
            .unwrap_or(false);

    if !has_content {
        settle(QueryClass::WaitEmpty);
        return (StatusCode::NO_CONTENT, resp_headers).into_response();
    }

    let use_zstd = accept_encoding.contains("zstd");
    let mut blocks = block_stream(data_response.head, data_response.tail.unwrap_or_default());

    // RP-13 owes INTERNAL for a failure before the first record, and once 200
    // is on the wire that can no longer be said — so the first record is
    // produced before the status is chosen. RP-20 keeps the budget out of it:
    // a bound reached before any record is the empty form, not a failure. The
    // budget bounds waiting for data only — a record in hand is encoded
    // outside it, so a slow transcode cannot turn acquired data into a 204.
    let budget = max_duration.saturating_sub(start.elapsed());
    let first_block = match tokio::time::timeout(budget, blocks.next()).await {
        Ok(Some(Ok(block))) => Some(block),
        Ok(Some(Err(e))) => {
            settle(QueryClass::Error);
            tracing::error!(%e, "query failed before its first record");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error\n\n{e:#}"),
            )
                .into_response();
        }
        Ok(None) | Err(_) => None,
    };
    let Some((first_block, first_source)) = first_block else {
        // Nothing was produced, so whatever the path, the outcome is RP-12's
        // empty form rather than the class that path would have served.
        settle(QueryClass::WaitEmpty);
        return (StatusCode::NO_CONTENT, resp_headers).into_response();
    };
    let first = match encode_frame(&first_block, use_zstd, first_source).await {
        Ok(frame) => frame,
        Err(e) => {
            settle(QueryClass::Error);
            tracing::error!(%e, "query failed before its first record");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error\n\n{e:#}"),
            )
                .into_response();
        }
    };
    settle(class);

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
    let metrics = Arc::clone(&state.metrics);

    tokio::spawn(async move {
        // Past the first record the status is committed, so every exit below is
        // a truncation (RP-12) and its counter is the only server-side alarm an
        // error still gets (INV-27). Each send carries one whole frame, so the
        // body ends on a record boundary whatever the cause.
        let truncate = |cause: TruncationCause| metrics.record_truncation(cause);

        let mut pending = Some(first);
        loop {
            let frame = match pending.take() {
                Some(frame) => frame,
                None => {
                    // The budget bounds waiting for data (RP-20); a record in
                    // hand is still encoded and sent past it (GAP-41).
                    let remaining = max_duration.saturating_sub(start.elapsed());
                    if remaining.is_zero() {
                        return truncate(TruncationCause::Budget);
                    }
                    let (block, source) = match tokio::time::timeout(remaining, blocks.next()).await
                    {
                        Err(_) => return truncate(TruncationCause::Budget),
                        Ok(None) => return,
                        Ok(Some(Err(e))) => {
                            tracing::error!(%e, "query failed after its first record");
                            return truncate(TruncationCause::Error);
                        }
                        Ok(Some(Ok(next))) => next,
                    };
                    match encode_frame(&block, use_zstd, source).await {
                        Ok(frame) => frame,
                        Err(e) => {
                            tracing::error!(%e, "query failed after its first record");
                            return truncate(TruncationCause::Error);
                        }
                    }
                }
            };
            // The budget bounds waiting on the reader too: a send still parked
            // when it runs out ends the body at the last record boundary. The
            // send is polled before the deadline, so a ready channel always
            // takes the frame — the first frame in particular is never cut.
            let send_bound = max_duration.saturating_sub(start.elapsed());
            match tokio::time::timeout(send_bound, tx.send(Ok(frame.bytes.clone()))).await {
                Err(_) => return truncate(TruncationCause::Budget),
                Ok(Err(_)) => return truncate(TruncationCause::Disconnect),
                Ok(Ok(())) => frame.note_served(),
            }
        }
    });

    // Convert the mpsc receiver into an axum Body.
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    (StatusCode::OK, resp_headers, body).into_response()
}

/// One record, encoded, still carrying what it is: the "served" log belongs
/// after the send, and only the frame knows which block it holds.
struct Frame {
    bytes: Bytes,
    number: u64,
    hash: String,
    source: &'static str,
}

impl Frame {
    fn note_served(&self) {
        tracing::debug!(
            stage = "block-served",
            source = self.source,
            block_number = self.number,
            block_hash = %self.hash,
            "block served {}#{}", self.number, self.hash
        );
    }
}

/// The body's records in order (REQ-6): the backfill prefix first, then the
/// snapshot tail it splices onto. Encoding happens at the consumer, so the
/// budget can bound waiting for data without cutting a record already in hand.
fn block_stream(
    head: Option<futures::stream::BoxStream<'static, anyhow::Result<Vec<Block>>>>,
    tail: Vec<Block>,
) -> futures::stream::BoxStream<'static, anyhow::Result<(Block, &'static str)>> {
    Box::pin(async_stream::try_stream! {
        if let Some(mut head) = head {
            while let Some(batch) = head.next().await {
                for block in batch? {
                    yield (block, "head");
                }
            }
        }
        for block in tail {
            yield (block, "tail");
        }
    })
}

fn insert_finalized_head(headers: &mut HeaderMap, fh: &crate::types::BlockRef) {
    headers.insert(
        "x-sqd-finalized-head-number",
        HeaderValue::from_str(&fh.number.to_string()).unwrap(),
    );
    headers.insert(
        "x-sqd-finalized-head-hash",
        HeaderValue::from_str(&fh.hash).unwrap(),
    );
}

async fn encode_frame(
    block: &Block,
    use_zstd: bool,
    source: &'static str,
) -> anyhow::Result<Frame> {
    Ok(Frame {
        bytes: encode_block(block, use_zstd).await?,
        number: block.number,
        hash: block.hash.clone(),
        source,
    })
}

/// Stored zstd frames pass through; anything else is re-encoded as one gzip
/// member, so a record is still exactly one independently decodable unit.
async fn encode_block(block: &Block, use_zstd: bool) -> anyhow::Result<Bytes> {
    if use_zstd {
        return Ok(block.json_line_zstd.clone());
    }
    let raw = tokio::task::spawn_blocking({
        let zstd_bytes = block.json_line_zstd.clone();
        move || zstd::decode_all(std::io::Cursor::new(zstd_bytes.as_ref()))
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
