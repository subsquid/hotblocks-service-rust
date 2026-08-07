//! OB-4 wiring: upstream interaction health, onto the registry the service
//! scrapes.

use std::sync::Arc;

use data_service_core::metrics::{Metrics, UpstreamErrorClass, UpstreamRequestKind};
use rpc_client::{RequestKind, RpcError, RpcObserver};

/// Bridges the client's reporting hook onto the registry; neither of those
/// crates sees the other, this one sees both.
pub struct MetricsRpcObserver {
    metrics: Arc<Metrics>,
}

impl MetricsRpcObserver {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl RpcObserver for MetricsRpcObserver {
    fn on_request(&self, kind: RequestKind, calls: usize) {
        let kind = match kind {
            RequestKind::Single => UpstreamRequestKind::Single,
            RequestKind::Batch => UpstreamRequestKind::Batch,
        };
        self.metrics.record_upstream_request(kind, calls);
    }

    fn on_error(&self, error: &RpcError) {
        self.metrics.record_upstream_error(classify(error));
    }

    fn on_retry(&self, error: &RpcError) {
        self.metrics.record_upstream_retry(classify(error));
    }
}

/// Map a client error onto OB-8's closed class set. Exhaustive on purpose.
fn classify(error: &RpcError) -> UpstreamErrorClass {
    match error {
        RpcError::Rpc { .. } => UpstreamErrorClass::Rpc,
        RpcError::Http { .. } => UpstreamErrorClass::Http,
        RpcError::Connection(_) => UpstreamErrorClass::Connection,
        RpcError::Disconnected(_) => UpstreamErrorClass::Disconnected,
        RpcError::Timeout => UpstreamErrorClass::Timeout,
        RpcError::Protocol(_) => UpstreamErrorClass::Protocol,
        RpcError::RetryRequested(_) => UpstreamErrorClass::RetryRequested,
        // Exhaustion is a verdict about the ladder, not the endpoint.
        RpcError::RetryExhausted(inner) => classify(inner),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Paging on `class="timeout"` must not miss timeouts that retried first.
    #[test]
    fn exhaustion_reports_the_error_that_actually_failed() {
        let exhausted = RpcError::RetryExhausted(Box::new(RpcError::RetryExhausted(Box::new(
            RpcError::Timeout,
        ))));
        assert_eq!(classify(&exhausted), UpstreamErrorClass::Timeout);
    }
}
