use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A reference to a block (number + hash).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub number: u64,
    pub hash: String,
}

/// Block header fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockHeader {
    pub number: u64,
    pub hash: String,
    pub parent_number: u64,
    pub parent_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

impl BlockHeader {
    pub fn block_ref(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash.clone(),
        }
    }
}

/// A fully-ingested block with its zstd-compressed JSON payload.
#[derive(Debug, Clone)]
pub struct Block {
    pub number: u64,
    pub hash: String,
    pub parent_number: u64,
    pub parent_hash: String,
    /// Millisecond timestamp (optional — not all chains provide it).
    pub timestamp: Option<u64>,
    /// Zstd-compressed JSON line (single \n-terminated JSON object).
    pub json_line_zstd: Bytes,
}

impl Block {
    pub fn block_ref(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash.clone(),
        }
    }

    pub fn header(&self) -> BlockHeader {
        BlockHeader {
            number: self.number,
            hash: self.hash.clone(),
            parent_number: self.parent_number,
            parent_hash: self.parent_hash.clone(),
            timestamp: self.timestamp,
        }
    }
}

/// Error returned to the HTTP caller when the supplied base block is not on
/// the current chain.  Contains up to 100 previous block refs so the client
/// can find a common ancestor.
#[derive(Debug)]
pub struct InvalidBaseBlock {
    pub prev: Vec<BlockRef>,
}

/// Error returned by `DataService::query`.
///
/// Mirrors the TS `query` contract: a fork/invalid-base-block becomes an
/// HTTP 409, while any other error is surfaced as an HTTP 500 (the TS
/// `belowQuery` re-throws non-fork errors, which the HTTP layer turns into a
/// 500). The `Internal` variant exists so a transient backfill error returns
/// a proper response instead of crashing the request task.
#[derive(Debug)]
pub enum QueryError {
    /// The supplied base block is not on the current chain → HTTP 409.
    InvalidBaseBlock(InvalidBaseBlock),
    /// An error occurred while servicing the query → HTTP 500.
    Internal(anyhow::Error),
}

impl From<InvalidBaseBlock> for QueryError {
    fn from(e: InvalidBaseBlock) -> Self {
        QueryError::InvalidBaseBlock(e)
    }
}

/// Response from a query — either a streaming backfill head + snapshot tail,
/// or just a tail (cache hit), or nothing yet (wait for block).
pub struct DataResponse {
    pub finalized_head: Option<BlockRef>,
    /// Async stream of backfill batches (below-query case).
    pub head: Option<futures::stream::BoxStream<'static, anyhow::Result<Vec<Block>>>>,
    /// Snapshot of the in-memory chain from the requested position onward.
    pub tail: Option<Vec<Block>>,
}

impl std::fmt::Debug for DataResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataResponse")
            .field("finalized_head", &self.finalized_head)
            .field("head", &self.head.as_ref().map(|_| "<stream>"))
            .field("tail", &self.tail.as_ref().map(|t| t.len()))
            .finish()
    }
}
