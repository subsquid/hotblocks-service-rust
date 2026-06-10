use crate::types::{Block, BlockRef};
use async_trait::async_trait;
use futures::stream::BoxStream;
use thiserror::Error;

/// A request to open a block stream.
#[derive(Debug, Clone)]
pub struct StreamRequest {
    pub from: u64,
    pub to: Option<u64>,
    pub parent_hash: Option<String>,
}

/// A batch of blocks together with the current finalized head (if known).
#[derive(Debug, Clone)]
pub struct BlockBatch {
    pub blocks: Vec<Block>,
    pub finalized_head: Option<BlockRef>,
}

/// Errors that a block stream can yield.
#[derive(Debug, Error)]
pub enum StreamError {
    /// The upstream chain diverged from what we expected.
    /// `previous_blocks` are the refs emitted by the source just before the
    /// fork — mirrors TS `ForkException.previousBlocks`.
    #[error("fork detected; previous upstream blocks: {previous_blocks:?}")]
    Fork { previous_blocks: Vec<BlockRef> },

    /// Any other error (network, deserialization, …).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl StreamError {
    pub fn is_fork(&self) -> bool {
        matches!(self, StreamError::Fork { .. })
    }
}

/// The trait that chain-specific implementations must satisfy.
///
/// Mirrors the TypeScript `DataSource<Block>` interface from
/// `@subsquid/util-internal-data-source`.
#[async_trait]
pub trait DataSource: Send + Sync + 'static {
    /// Return the current chain head (latest block ref).
    async fn get_head(&self) -> anyhow::Result<BlockRef>;

    /// Return the current finalized head.
    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef>;

    /// Open a stream of unfinalized (head-following) block batches.
    ///
    /// The stream yields batches until the source decides to stop or a fork is
    /// detected, at which point it emits `StreamError::Fork`.
    fn get_stream(&self, req: StreamRequest) -> BoxStream<'static, Result<BlockBatch, StreamError>>;

    /// Open a stream of finalized block batches up to `req.to`.
    fn get_finalized_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>>;
}
