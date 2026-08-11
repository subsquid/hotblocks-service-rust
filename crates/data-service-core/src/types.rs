use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Where an enrichment spent its time.
///
/// `enrich_ms` alone cannot distinguish an upstream that is slow from a ladder
/// that ran several times, nor say which leg the round waited on. Legs are
/// issued concurrently, so their durations overlap and the largest is what the
/// round actually cost; `waited` is ours, not the upstream's.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnrichProfile {
    /// Acquisition rounds. 1 means the first answer was usable.
    pub attempts: u32,
    /// Time slept between rounds, the retry ladder's own cost.
    pub waited: Duration,
    /// Per-leg elapsed of the round that finally succeeded. A leg not requested
    /// stays zero.
    pub logs: Duration,
    pub receipts: Duration,
    pub traces: Duration,
}

impl EnrichProfile {
    /// What the round actually waited for, the legs being concurrent.
    pub fn slowest_leg(&self) -> Duration {
        self.logs.max(self.receipts).max(self.traces)
    }
}

/// Per-block pipeline timing stamps.
/// All fields are wall-clock `Instant` values except `compress_duration` which
/// is measured inside `map_raw_block` and stored as a pre-computed `Duration`.
#[derive(Clone, Debug)]
pub struct BlockTimings {
    /// When the poll that carried the body was issued. Everything before this
    /// is the block not existing yet — or the provider saying so after it does,
    /// which is what `null_polls` counts and no instrument here can separate.
    pub poll_issued: Instant,
    /// Null answers spent on this block's number before the body came back.
    pub null_polls: u32,
    /// When the block body was returned by `get_single_block`.
    pub body_received: Instant,
    /// When `enrich_block_with_retry` finished.
    pub enrich_done: Instant,
    /// How the span between the two above was spent.
    pub enrich_profile: EnrichProfile,
    /// When JSON serialization + normalization finished (before zstd).
    pub normalize_done: Instant,
    /// How long the zstd compression step inside `map_raw_block` took.
    pub compress_duration: Duration,
}

impl BlockTimings {
    pub fn compress_done(&self) -> Instant {
        self.normalize_done + self.compress_duration
    }
}

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
    /// Pipeline timing stamps (only set for hot/speculative blocks).
    pub timings: Option<BlockTimings>,
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

/// Input that contradicts the buffer it is applied to.
///
/// WP-5 handling: the caller rejects the batch whole, tears the session down
/// and re-enters the ladder, leaving the buffer intact and readable. No input
/// content may end the process (WP-14, INV-41).
#[derive(Debug, thiserror::Error)]
pub enum IntegrityViolation {
    /// DEF-8/WP-6: one ref claiming two ancestries. Applied as a reorg it
    /// would rewrite a buffered block's history while its ref stays put, and
    /// no client can see it — the base check compares the parent hashes of
    /// *later* blocks, and those still link.
    #[error("block {number}#{hash} claims a second ancestry")]
    RefEquivocation { number: u64, hash: String },
    /// DEF-4: a parent at or above the block's own height would leave the
    /// buffer unsorted and every bisect blind.
    #[error("block {number} claims parent height {parent_number}, at or above itself")]
    DescendingHeight { number: u64, parent_number: u64 },
    /// The named parent height is not buffered — a gap, not a reorg.
    #[error("block {number} names parent height {parent_number}, which is not buffered")]
    ParentNotBuffered { number: u64, parent_number: u64 },
    /// INV-11: a write at or below the finalized head.
    #[error("block {number} would revert the finalized head")]
    WriteBelowFinality { number: u64 },
    /// DEF-5: the named parent is buffered under another hash.
    #[error("block {number} names parent {claimed}, buffer holds {buffered}")]
    ParentHashMismatch {
        number: u64,
        buffered: String,
        claimed: String,
    },
    /// WP-23: finality named a height the buffer does not hold.
    #[error("finality names unbuffered height {number}")]
    UnbufferedFinality { number: u64 },
    /// WP-23: the block that arrived at that height is not the one finalized.
    #[error("finality at height {number} names {reported}, buffer holds {buffered}")]
    FinalityHashMismatch {
        number: u64,
        buffered: String,
        reported: String,
    },
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
