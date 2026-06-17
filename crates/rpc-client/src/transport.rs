use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{RpcError, RpcErrorInfo};

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
#[async_trait]
pub(crate) trait RpcTransport: Send + Sync {
    async fn send_single(
        &self,
        req: OwnedRpcRequest,
        timeout: Duration,
    ) -> Result<RpcResponse, RpcError>;

    async fn send_batch(
        &self,
        reqs: Vec<OwnedRpcRequest>,
        timeout: Duration,
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
}

#[async_trait]
impl RpcTransport for HttpTransport {
    async fn send_single(
        &self,
        req: OwnedRpcRequest,
        timeout: Duration,
    ) -> Result<RpcResponse, RpcError> {
        let id = req.id;
        let body = serde_json::to_vec(&req.as_wire()).expect("serialize");

        let raw = self.post_raw(body, timeout).await?;
        let resp: RpcResponse = serde_json::from_slice(&raw)
            .map_err(|e| RpcError::Protocol(format!("invalid JSON: {e}")))?;

        let resp_id = resp.id.as_ref().and_then(|v| v.as_u64()).unwrap_or(0);
        if resp_id != id {
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
    ) -> Result<Vec<RpcResponse>, RpcError> {
        let count = reqs.len();
        let requests: Vec<RpcRequest<'_>> = reqs.iter().map(|r| r.as_wire()).collect();

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
