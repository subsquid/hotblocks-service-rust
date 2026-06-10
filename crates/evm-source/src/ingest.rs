/// Ingest layer — stride-parallel block fetching with speculative head polling.
/// Ports evm-rpc/src/data-source/ingest.ts and poll-stream.ts.
use std::sync::Arc;
use std::time::{Duration, Instant};

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

// ─── Cadence predictor ────────────────────────────────────────────────────────

/// Exponential moving average cadence predictor.
/// Tracks inter-block interval from wall-clock arrival times.
pub struct CadencePredictor {
    /// EMA of inter-block intervals in milliseconds.
    ema_ms: Option<f64>,
    /// Wall-clock time of last observed block arrival.
    last_arrival: Option<Instant>,
    /// EMA smoothing factor (α). Lower = slower adaptation.
    alpha: f64,
}

impl CadencePredictor {
    pub fn new() -> Self {
        CadencePredictor {
            ema_ms: None,
            last_arrival: None,
            alpha: 0.3,
        }
    }

    /// Record that a new block arrived now.
    pub fn record_block(&mut self, now: Instant) {
        if let Some(last) = self.last_arrival {
            let interval_ms = now.duration_since(last).as_millis() as f64;
            self.ema_ms = Some(match self.ema_ms {
                None => interval_ms,
                Some(prev) => self.alpha * interval_ms + (1.0 - self.alpha) * prev,
            });
        }
        self.last_arrival = Some(now);
    }

    /// How long to sleep before next poll attempt.
    /// Returns 100ms if no prediction data yet.
    ///
    /// Arrival times have significant jitter (provider propagation,
    /// load-balanced nodes), so a single long sleep until the predicted
    /// arrival risks sleeping through an early block. Instead we only stay
    /// quiet while we're more than 600ms away from the predicted arrival,
    /// and poll every 25ms inside that window.
    pub fn next_poll_delay(&self, now: Instant) -> Duration {
        const HOT_WINDOW_MS: f64 = 600.0;
        const HOT_POLL_MS: f64 = 25.0;

        let (Some(ema), Some(last)) = (self.ema_ms, self.last_arrival) else {
            return Duration::from_millis(100);
        };

        let elapsed_ms = now.duration_since(last).as_millis() as f64;
        let remaining = ema - elapsed_ms;
        let sleep_ms = if remaining > HOT_WINDOW_MS {
            // Quiet period: wake at the edge of the hot window.
            (remaining - HOT_WINDOW_MS).min(1000.0)
        } else {
            HOT_POLL_MS
        };
        Duration::from_millis(sleep_ms.max(HOT_POLL_MS) as u64)
    }
}

impl Default for CadencePredictor {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Mapping helpers ──────────────────────────────────────────────────────────

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

// ─── Main ingest_range ────────────────────────────────────────────────────────

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
    let commitment = commitment.to_string();

    async_stream::try_stream! {
        let mut beg = from;
        let end = to.unwrap_or(u64::MAX);

        // Poll stream state (used only for "finalized" poll mode)
        let mut poll_head: Option<u64> = None;
        let mut poll_last_read: Option<u64> = None;

        // Cadence predictor for speculative ("latest") poll mode
        let mut cadence = CadencePredictor::new();

        // Speculative height: the block number we're currently waiting for
        // in speculative mode. Reset on break-out from that mode.
        let mut speculative_next: Option<u64> = None;

        while beg <= end {
            // Head of the stream's commitment level bounds the backfill range
            let (head_num, head_hash) = rpc.get_latest_blockhash(&commitment).await?;

            let top = head_num.min(end);

            if top > beg && (top - beg) > stride_size as u64 {
                // ── Backfill mode ────────────────────────────────────────────
                // Parallel strides, emitted in order as they complete.
                speculative_next = None;
                poll_head = None;
                poll_last_read = None;

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

            } else if commitment == "latest" {
                // ── Speculative poll mode (latest commitment) ────────────────
                //
                // Directly request block N+1 in a tight loop instead of
                // checking the head first; a null result = not produced yet.
                // No per-cycle head check — we stay in this inner loop until
                // the gap grows large enough to warrant backfill mode.
                //
                // Enrichment (logs/receipts/traces) runs in spawned tasks
                // behind a bounded in-order queue (depth 3), so polling for
                // N+1's body proceeds while N waits out the provider's
                // receipts lag. Emission stays strictly ordered: a stuck
                // block holds emission (and, at queue capacity, polling),
                // never gets skipped.

                let mut next_num = speculative_next.unwrap_or(beg);
                type EnrichTask = tokio::task::JoinHandle<Result<Vec<Block>>>;
                let mut pending: std::collections::VecDeque<(u64, EnrichTask)> = std::collections::VecDeque::new();
                const PIPELINE_DEPTH: usize = 3;
                // Consecutive non-null polls; if the chain is far ahead of us,
                // fall back to stride backfill.
                let mut hot_streak: u64 = 0;

                let spawn_enrich = |body: RawRpcBlock| -> EnrichTask {
                    let rpc2 = rpc.clone();
                    let req2 = req.clone();
                    let opts2 = mapping_options.clone();
                    tokio::spawn(async move {
                        let enriched = rpc2.enrich_block_with_retry(body, &req2).await?;
                        map_blocks_cpu(vec![enriched], opts2).await
                    })
                };

                loop {
                    // Drain the front of the pipeline when full or when the
                    // range is exhausted.
                    while pending.len() >= PIPELINE_DEPTH
                        || (next_num > end && !pending.is_empty())
                    {
                        let (_, task) = pending.pop_front().unwrap();
                        let mapped = task.await.map_err(anyhow::Error::from)??;
                        yield IngestBatch { blocks: mapped, finalized: None };
                    }

                    if next_num > end {
                        break;
                    }
                    if hot_streak > 2 * stride_size as u64 {
                        // Far behind the head — let the outer loop switch to
                        // stride backfill (after draining what's in flight).
                        while let Some((_, task)) = pending.pop_front() {
                            let mapped = task.await.map_err(anyhow::Error::from)??;
                            yield IngestBatch { blocks: mapped, finalized: None };
                        }
                        break;
                    }

                    match rpc.get_single_block(next_num).await? {
                        Some(body) if !body.is_invalid => {
                            cadence.record_block(Instant::now());
                            hot_streak += 1;
                            pending.push_back((next_num, spawn_enrich(body)));
                            next_num += 1;
                        }
                        _ => {
                            // Not produced yet (or an inconsistent LB response,
                            // which we retry the same way). While waiting,
                            // opportunistically drain a completed front task.
                            hot_streak = 0;
                            let delay = cadence.next_poll_delay(Instant::now());
                            if let Some((_, task)) = pending.front_mut() {
                                match tokio::time::timeout(delay, task).await {
                                    Ok(res) => {
                                        pending.pop_front();
                                        let mapped = res.map_err(anyhow::Error::from)??;
                                        yield IngestBatch { blocks: mapped, finalized: None };
                                    }
                                    Err(_elapsed) => {}
                                }
                            } else {
                                sleep(delay).await;
                            }
                        }
                    }
                }

                speculative_next = Some(next_num);
                beg = next_num;
                // Adjust for blocks still unconfirmed? None — pending was fully
                // drained above before any break.

            } else {
                // ── Poll mode for finalized commitment ───────────────────────
                // Keep existing behavior: poll the finalized head, then fetch bodies.

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

                let finalized = raw_blocks.last().map(|b| BlockRef { number: b.number, hash: b.hash.clone() });

                let mapped = map_blocks_cpu(raw_blocks, mapping_options.clone()).await?;
                yield IngestBatch { blocks: mapped, finalized };
            }
        }
    }
}
