use serde::{Deserialize, Serialize};
use thiserror::Error;

/// JSON-RPC error object from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcErrorInfo {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// All errors that can come out of `RpcClient`
#[derive(Debug, Error)]
pub enum RpcError {
    /// Server returned a JSON-RPC error object
    #[error("RPC error {code}: {message}")]
    Rpc {
        code: i64,
        message: String,
        data: Option<serde_json::Value>,
    },

    /// HTTP-level error (non-2xx)
    #[error("HTTP error {status}")]
    Http { status: u16, body: String },

    /// Network / connection / TLS error from reqwest
    #[error("connection error: {0}")]
    Connection(#[source] reqwest::Error),

    /// Request timed out
    #[error("request timed out")]
    Timeout,

    /// Server violated the JSON-RPC protocol
    #[error("protocol error: {0}")]
    Protocol(String),

    /// All retry attempts exhausted; wraps the last error
    #[error("retry exhausted: {0}")]
    RetryExhausted(Box<RpcError>),

    /// Caller's validate_result / validate_error hook requested a retry
    #[error("retry requested: {0}")]
    RetryRequested(String),
}

impl RpcError {
    pub(crate) fn from_info(info: RpcErrorInfo) -> Self {
        RpcError::Rpc {
            code: info.code,
            message: info.message,
            data: info.data,
        }
    }

    /// Is this error retryable for a plain call?
    /// Mirrors TS `RpcClient.isConnectionError` + PLAN.md retryability table.
    pub fn is_retryable(&self, retry_internal_server_errors: bool) -> bool {
        match self {
            RpcError::RetryRequested(_) => true,
            RpcError::Timeout => true,
            RpcError::Connection(_) => true,
            RpcError::Http { status, .. } => matches!(status, 408 | 429 | 502 | 503 | 504 | 529),
            RpcError::Rpc { code, message, .. } => {
                // code -32005, 429
                if *code == -32005 || *code == 429 {
                    return true;
                }
                // rate limit / too many requests
                if message_matches_rate_limit(message) {
                    return true;
                }
                // execution timeout
                if EXECUTION_TIMEOUT_RE.is_match(message) {
                    return true;
                }
                // request timed out
                if REQUEST_TIMED_OUT_RE.is_match(message) {
                    return true;
                }
                if retry_internal_server_errors {
                    if *code == -32000 || *code == -32603 {
                        return true;
                    }
                    if INTERNAL_ERROR_RE.is_match(message) {
                        return true;
                    }
                }
                false
            }
            RpcError::Protocol(_) => false,
            RpcError::RetryExhausted(_) => false,
        }
    }

    /// Is this error retryable in the reduce-batch-on-retry context?
    /// Adds protocol errors and "response too large" on top of `is_retryable`.
    pub fn is_retryable_batch(&self, retry_internal_server_errors: bool) -> bool {
        if self.is_retryable(retry_internal_server_errors) {
            return true;
        }
        match self {
            RpcError::Protocol(_) => true,
            RpcError::Rpc { message, .. } if message == "response too large" => true,
            // Also code -32000 always retryable for batch (mirrors isBatchRetryableError in TS)
            RpcError::Rpc { code, .. } if *code == -32000 => true,
            _ => false,
        }
    }
}

fn message_matches_rate_limit(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("rate limit") || lower.contains("too many requests")
}

// Lazy regex simulation via simple string search (avoids regex dep)
struct SimpleRe(&'static str);
impl SimpleRe {
    fn is_match(&self, s: &str) -> bool {
        s.to_lowercase().contains(self.0)
    }
}

static EXECUTION_TIMEOUT_RE: SimpleRe = SimpleRe("execution timeout");

struct RequestTimedOutRe;
impl RequestTimedOutRe {
    fn is_match(&self, s: &str) -> bool {
        // /request.*timed out/i
        let lower = s.to_lowercase();
        if let Some(pos) = lower.find("request") {
            lower[pos..].contains("timed out")
        } else {
            false
        }
    }
}
static REQUEST_TIMED_OUT_RE: RequestTimedOutRe = RequestTimedOutRe;

struct InternalErrorRe;
impl InternalErrorRe {
    fn is_match(&self, s: &str) -> bool {
        // /internal( server)? error/i
        let lower = s.to_lowercase();
        lower.contains("internal error") || lower.contains("internal server error")
    }
}
static INTERNAL_ERROR_RE: InternalErrorRe = InternalErrorRe;
