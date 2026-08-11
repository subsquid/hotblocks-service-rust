use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{RpcError, RpcErrorInfo};
use crate::session::UpstreamSession;

pub(crate) mod ws;

// ─── Wire types ─────────────────────────────────────────────────────────────

/// Outgoing JSON-RPC request frame. Borrows method/params so the frame can be
/// serialized without copying. The borrow is released before any await.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct RpcRequest<'a> {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<&'a Value>,
}

/// Incoming JSON-RPC response frame.
#[derive(Debug, Deserialize)]
pub(crate) struct RpcResponse {
    pub id: Option<Value>,
    pub result: Option<Value>,
    pub error: Option<RpcErrorInfo>,
}

/// Owned request descriptor handed to a transport. Owned (not borrowed
/// `RpcRequest<'_>`) so the transport can build the frame and hold the data
/// across awaits without lifetime threading.
#[derive(Debug, Clone)]
pub(crate) struct OwnedRpcRequest {
    pub id: u64,
    pub method: String,
    pub params: Option<Value>,
}

impl OwnedRpcRequest {
    fn as_wire(&self) -> RpcRequest<'_> {
        RpcRequest {
            jsonrpc: "2.0",
            id: self.id,
            method: self.method.as_str(),
            params: self.params.as_ref(),
        }
    }
}

// ─── Transport trait ──────────────────────────────────────────────────────────

/// Pluggable JSON-RPC transport. Operates on owned request descriptors and
/// returns parsed responses. Implementations correlate batch responses by id
/// and MUST return batch results in request order.
///
/// A transport that can address a backend replays `session`'s pinned name and
/// takes up whatever the answer assigns; one that cannot ignores it.
#[async_trait]
pub(crate) trait RpcTransport: Send + Sync {
    async fn send_single(
        &self,
        req: OwnedRpcRequest,
        timeout: Duration,
        session: Option<&UpstreamSession>,
    ) -> Result<RpcResponse, RpcError>;

    async fn send_batch(
        &self,
        reqs: Vec<OwnedRpcRequest>,
        timeout: Duration,
        session: Option<&UpstreamSession>,
    ) -> Result<Vec<RpcResponse>, RpcError>;
}

// ─── HTTP transport ─────────────────────────────────────────────────────────

/// HTTP transport — a straight extraction of the original reqwest behavior,
/// byte-for-byte: same client tuning, same id check, same batch length check
/// and id→response reorder map.
pub(crate) struct HttpTransport {
    url: String,
    http: reqwest::Client,
}

impl HttpTransport {
    pub fn new(url: String, capacity: usize) -> Self {
        // Keep connections warm and reused. A fresh HTTPS connection pays the
        // TCP + TLS handshake and starts with a cold congestion window, so a
        // reused connection is faster, especially for large receipts payloads.
        // TCP keepalive stops the provider's load balancer / NAT from silently
        // dropping idle connections (which would force such a reconnect), and a
        // generous idle timeout keeps the pool warm through quieter chains.
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(capacity.min(64))
            .pool_idle_timeout(Duration::from_secs(120))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");
        HttpTransport { url, http }
    }

    async fn post_raw(
        &self,
        body: Vec<u8>,
        timeout: Duration,
        session: Option<&UpstreamSession>,
    ) -> Result<Vec<u8>, RpcError> {
        let mut req = self
            .http
            .post(self.url.as_str())
            .header("content-type", "application/json")
            .body(body);

        if let Some(pinned) = session.and_then(UpstreamSession::pinned) {
            req = req.header(reqwest::header::COOKIE, pinned.as_ref());
        }
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

        // Before the status check: an assignment says where the request landed,
        // which matters most when that backend failed it.
        if let Some(session) = session {
            session.adopt(assignment_of(response.headers()));
        }

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
}

/// The backend name an answer carries, as one replayable string. Cookies are
/// the mechanism HTTP has for it, so every pair is kept and no name is known;
/// an empty value is a deletion, not an assignment.
fn assignment_of(headers: &reqwest::header::HeaderMap) -> Option<Arc<str>> {
    let mut assigned = String::new();
    for value in headers.get_all(reqwest::header::SET_COOKIE) {
        let Ok(value) = value.to_str() else { continue };
        let pair = value.split(';').next().unwrap_or_default().trim();
        let Some((name, id)) = pair.split_once('=') else {
            continue;
        };
        if name.trim().is_empty() || id.trim().is_empty() {
            continue;
        }
        if !assigned.is_empty() {
            assigned.push_str("; ");
        }
        assigned.push_str(pair);
    }
    (!assigned.is_empty()).then(|| Arc::from(assigned.as_str()))
}

#[async_trait]
impl RpcTransport for HttpTransport {
    async fn send_single(
        &self,
        req: OwnedRpcRequest,
        timeout: Duration,
        session: Option<&UpstreamSession>,
    ) -> Result<RpcResponse, RpcError> {
        let id = req.id;
        let body = serde_json::to_vec(&req.as_wire()).expect("serialize");

        let raw = self.post_raw(body, timeout, session).await?;
        let resp: RpcResponse = serde_json::from_slice(&raw)
            .map_err(|e| RpcError::Protocol(format!("invalid JSON: {e}")))?;

        let resp_id = resp.id.as_ref().and_then(|v| v.as_u64()).unwrap_or(0);
        if resp_id != id {
            // Endpoints/proxies answer 200 with an `id: null` error envelope
            // for rate limits, oversized requests, gateway failures, etc.
            // That is the real server error (retry-classifiable), not a
            // protocol violation (mirrors TS transport/http.ts `call`).
            if resp.error.is_some() {
                return Ok(resp);
            }
            return Err(RpcError::Protocol(format!(
                "Got response for unknown request {resp_id}"
            )));
        }

        Ok(resp)
    }

    async fn send_batch(
        &self,
        reqs: Vec<OwnedRpcRequest>,
        timeout: Duration,
        session: Option<&UpstreamSession>,
    ) -> Result<Vec<RpcResponse>, RpcError> {
        let count = reqs.len();
        let requests: Vec<RpcRequest<'_>> = reqs.iter().map(|r| r.as_wire()).collect();

        let body = serde_json::to_vec(&requests).expect("serialize");
        let raw = self.post_raw(body, timeout, session).await?;

        let responses: Vec<RpcResponse> = match serde_json::from_slice(&raw) {
            Ok(responses) => responses,
            Err(e) => {
                // A server rejecting the whole batch (rate limit, oversized
                // request, gateway failure) often replies with one JSON-RPC
                // error envelope instead of an array. Surface that server
                // error (mirrors TS transport/http.ts `batchCall`).
                if let Ok(RpcResponse {
                    error: Some(info), ..
                }) = serde_json::from_slice::<RpcResponse>(&raw)
                {
                    return Err(RpcError::from_info(info));
                }
                return Err(RpcError::Protocol(format!(
                    "invalid JSON in batch response: {e}"
                )));
            }
        };

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

        let mut ordered = Vec::with_capacity(count);
        for r in &reqs {
            let resp = map.remove(&r.id).ok_or_else(|| {
                RpcError::Protocol(format!(
                    "Missing result for call id {} in batch response",
                    r.id
                ))
            })?;
            ordered.push(resp);
        }

        Ok(ordered)
    }
}
