//! Upstream-interaction reporting hook (OB-4). Errors arrive as `RpcError` so
//! the class set stays with whoever owns the scrape surface.

use crate::error::RpcError;

/// The shape of one upstream round trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Single,
    Batch,
}

pub trait RpcObserver: Send + Sync + 'static {
    /// One round trip left for the endpoint, carrying `calls` JSON-RPC calls.
    fn on_request(&self, kind: RequestKind, calls: usize);

    /// A call failed and will not be attempted again.
    fn on_error(&self, error: &RpcError);

    /// A call failed and is about to be retried (FM-22/FM-23, or a validator).
    fn on_retry(&self, error: &RpcError);
}
