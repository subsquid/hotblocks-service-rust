/// Ingest layer — stride-parallel block fetching.
/// Ports evm-rpc/src/data-source/ingest.ts and poll-stream.ts.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::stream::{self, StreamExt};
use tokio::time::sleep;

use crate::fetch::Rpc;
use crate::mapping::map_raw_block;
use crate::normalization::MappingOptions as NormOptions;
use crate::rpc_data::RawRpcBlock;
use crate::types::DataRequest;
use data_service_core::{Block, BlockRef};

/// A batch of blocks with optional finalized head info.
#[derive(Debug)]
pub struct IngestBatch {
    pub blocks: Vec<Block>,
    pub finalized: Option<BlockRef>,
}

fn block_ref_from_rpc(b: &RawRpcBlock) -> BlockRef {
    BlockRef {
        number: b.number,
        hash: b.hash.clone(),
    }
}

/// Map raw rpc blocks to core blocks using spawn_blocking for CPU work.
async fn map_blocks_cpu(
    raw_blocks: Vec<RawRpcBlock>,
    options: Arc<NormOptions>,
) -> Result<Vec<Block>> {
    if raw_blocks.is_empty() {
        return Ok(vec![]);
    }
    tokio::task::spawn_blocking(move || {
        let mut blocks = Vec::with_capacity(raw_blocks.len());
        for raw in &raw_blocks {
            blocks.push(map_raw_block(raw, &options)?);
        }
        Ok(blocks)
    })
    .await?
}

/// Ingest all blocks from [from, to] with stride concurrency.
/// Yields IngestBatch items.
pub async fn ingest_range(
    rpc: Arc<Rpc>,
    req: Arc<DataRequest>,
    mapping_options: Arc<NormOptions>,
    from: u64,
    to: Option<u64>,
    stride_size: usize,
    stride_concurrency: usize,
    commitment: &str, // "finalized" or "latest"
) -> impl futures::Stream<Item = Result<IngestBatch>> {
    let rpc_clone = rpc.clone();
    let commitment = commitment.to_string();

    async_stream::try_stream! {
        let mut beg = from;
        let end = to.unwrap_or(u64::MAX);

        // Poll stream state
        let mut poll_head: Option<u64> = None;
        let mut poll_last_read: Option<u64> = None;

        while beg <= end {
            // Head of the stream's commitment level bounds the backfill range
            let (head_num, head_hash) = rpc.get_latest_blockhash(&commitment).await?;

            let top = head_num.min(end);

            if top > beg && (top - beg) > stride_size as u64 {
                // Backfill mode: parallel strides, emitted in order as they
                // complete (up to `stride_concurrency` in flight) — no
                // whole-range barrier.
                let finalized_ref = if commitment == "finalized" {
                    Some(BlockRef { number: head_num, hash: head_hash })
                } else {
                    None
                };

                let ranges: Vec<(u64, u64)> = {
                    let mut r = Vec::new();
                    let mut start = beg;
                    while start <= top {
                        let end_stride = (start + stride_size as u64 - 1).min(top);
                        r.push((start, end_stride));
                        start = end_stride + 1;
                    }
                    r
                };

                let mut strides = stream::iter(ranges.into_iter().map(|(s, e)| {
                    let rpc2 = rpc.clone();
                    let req2 = req.clone();
                    async move {
                        let numbers: Vec<u64> = (s..=e).collect();
                        let blocks = rpc2.get_block_batch(&numbers, &req2).await;
                        (s, blocks)
                    }
                }))
                .buffered(stride_concurrency);

                while let Some((s, block_result)) = strides.next().await {
                    let mut raw_blocks = block_result?;
                    if let Some(inv_pos) = raw_blocks.iter().position(|b| b.is_invalid) {
                        raw_blocks.truncate(inv_pos);
                    }
                    if raw_blocks.is_empty() {
                        // Block not available yet (or invalid) — restart from here
                        beg = s;
                        break;
                    }

                    let last_num = raw_blocks.last().map(|b| b.number).unwrap_or(s);
                    beg = last_num + 1;

                    let mapped = map_blocks_cpu(raw_blocks, mapping_options.clone()).await?;
                    yield IngestBatch { blocks: mapped, finalized: finalized_ref.clone() };
                }
            } else {
                // Poll mode: near-head
                let poll_start = poll_last_read.map(|l| l + 1).unwrap_or(beg);
                if poll_start != beg {
                    poll_last_read = None;
                    poll_head = None;
                }

                // Fetch head if needed
                if poll_last_read.is_none() || poll_head.is_none_or(|h| poll_last_read.unwrap_or(0) >= h) {
                    let (h, _) = rpc.get_latest_blockhash(&commitment).await?;
                    if poll_last_read.is_some() && h <= poll_last_read.unwrap_or(0) {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    poll_head = Some(h);
                }

                let head = poll_head.unwrap_or(beg);
                let stride_end = (beg + stride_size as u64).min(head);
                let numbers: Vec<u64> = (beg..=stride_end).collect();

                let mut raw_blocks = rpc.get_block_batch(&numbers, &req).await?;

                // Strip invalid blocks
                if let Some(inv_pos) = raw_blocks.iter().position(|b| b.is_invalid) {
                    raw_blocks.truncate(inv_pos);
                }

                if raw_blocks.is_empty() {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                let last_num = raw_blocks.last().unwrap().number;
                // Filter to not exceed end
                if last_num > end {
                    raw_blocks.retain(|b| b.number <= end);
                }

                poll_last_read = raw_blocks.last().map(|b| b.number);
                beg = poll_last_read.unwrap_or(beg) + 1;

                let finalized = if commitment == "finalized" {
                    raw_blocks.last().map(|b| BlockRef { number: b.number, hash: b.hash.clone() })
                } else {
                    None
                };

                let mapped = map_blocks_cpu(raw_blocks, mapping_options.clone()).await?;
                yield IngestBatch { blocks: mapped, finalized };
            }
        }
    }
}
