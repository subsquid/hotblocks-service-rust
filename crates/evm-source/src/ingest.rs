/// Ingest layer — stride-parallel block fetching with speculative head polling.
/// Ports evm-rpc/src/data-source/ingest.ts and poll-stream.ts.
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use futures::stream::{self, StreamExt};
use tokio::time::sleep;
use tracing::warn;

use crate::fetch::{Rpc, P_ENRICH_DELAY, P_ENRICH_RETRIES};
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
///
/// `timings`: optional per-block `(body_received, enrich_done)` pairs — must
/// have the same length as `raw_blocks` when `Some`.
async fn map_blocks_cpu(
    raw_blocks: Vec<RawRpcBlock>,
    options: Arc<NormOptions>,
    timings: Option<Vec<(std::time::Instant, std::time::Instant)>>,
) -> Result<Vec<Block>> {
    if raw_blocks.is_empty() {
        return Ok(vec![]);
    }
    tokio::task::spawn_blocking(move || {
        let mut blocks = Vec::with_capacity(raw_blocks.len());
        for (i, raw) in raw_blocks.iter().enumerate() {
            let t = timings.as_ref().and_then(|v| v.get(i).copied());
            blocks.push(map_raw_block(raw, &options, t)?);
        }
        Ok(blocks)
    })
    .await?
}

/// Map the complete prefix below a range-acquisition fault without hiding the
/// more actionable, block-naming acquisition error if mapping also fails.
async fn map_range_prefix(
    raw_blocks: Vec<RawRpcBlock>,
    options: Arc<NormOptions>,
    acquisition_failure: Option<&anyhow::Error>,
) -> Result<Vec<Block>> {
    match map_blocks_cpu(raw_blocks, options, None).await {
        Ok(blocks) => Ok(blocks),
        Err(mapping_error) => match acquisition_failure {
            Some(acquisition_error) => Err(anyhow!(
                "{acquisition_error:#}; failed to map the complete prefix below that fault: \
                 {mapping_error:#}"
            )),
            None => Err(mapping_error),
        },
    }
}

/// One finalized range acquisition, including the complete prefix that remains
/// safe to emit when a later block exhausts its coherence budget.
struct RangeAcquisition {
    blocks: Vec<RawRpcBlock>,
    failure: Option<anyhow::Error>,
}

/// Describe why `candidate` cannot be emitted at `number` after `parent_hash`.
/// `None` means the block is coherent with the prefix acquired so far.
fn range_candidate_issue(
    candidate: Option<&RawRpcBlock>,
    number: u64,
    parent_hash: Option<&str>,
) -> Option<String> {
    let Some(candidate) = candidate else {
        return Some("block not available".to_string());
    };
    if candidate.number != number {
        return Some(format!(
            "requested block {number}, upstream returned {}",
            candidate.number
        ));
    }
    if candidate.is_invalid {
        return Some(
            candidate
                .error_message
                .clone()
                .unwrap_or_else(|| "incoherent block".to_string()),
        );
    }
    if let Some(parent_hash) = parent_hash {
        if !parent_hash.eq_ignore_ascii_case(&candidate.block.parent_hash) {
            return Some(format!(
                "parent hash mismatch: expected {parent_hash}, got {}",
                candidate.block.parent_hash
            ));
        }
    }
    None
}

/// Acquire one finalized range batch with the same per-block whole-acquisition
/// budget as the speculative head path (WP-11.2). The initial batched answer
/// is retained for throughput; only a missing or incoherent block is fetched
/// again, including its header and every selected component.
///
/// # Panics
///
/// Panics if `numbers` is empty. Callers must establish that the requested
/// finalized range has begun before delegating its acquisition.
async fn acquire_range_stride(
    rpc: &Arc<Rpc>,
    req: &DataRequest,
    numbers: &[u64],
) -> RangeAcquisition {
    assert!(
        !numbers.is_empty(),
        "range acquisition requires at least one block number"
    );

    let initial = match rpc.get_block_batch(numbers, req).await {
        Ok(blocks) => blocks,
        Err(error) => {
            return RangeAcquisition {
                blocks: Vec::new(),
                failure: Some(error.context("failed to acquire range stride")),
            };
        }
    };

    let mut initial = initial.into_iter();
    let mut complete = Vec::with_capacity(numbers.len());

    for &number in numbers {
        let mut candidate = initial.next();
        let initial_batch_was_short = candidate.is_none();
        let mut retries = 0u32;

        loop {
            let parent_hash = complete
                .last()
                .map(|block: &RawRpcBlock| block.hash.as_str());
            let Some(reason) = range_candidate_issue(candidate.as_ref(), number, parent_hash)
            else {
                complete.push(candidate.expect("a coherent candidate exists"));
                break;
            };

            if retries >= P_ENRICH_RETRIES {
                if let Some(metrics) = rpc.metrics() {
                    metrics.record_acquisition_retry_exhausted();
                }
                return RangeAcquisition {
                    blocks: complete,
                    failure: Some(anyhow!(
                        "failed to acquire block {number} after {P_ENRICH_RETRIES} retries: {reason}"
                    )),
                };
            }
            let first_fetch_after_short_batch = retries == 0 && initial_batch_was_short;
            retries += 1;

            warn!(
                block = number,
                attempt = retries,
                max_retries = P_ENRICH_RETRIES,
                reason = %reason,
                "incoherent block in range acquisition, re-acquiring"
            );
            if !first_fetch_after_short_batch {
                sleep(P_ENRICH_DELAY).await;
            }

            candidate = match rpc
                .get_block_batch(std::slice::from_ref(&number), req)
                .await
            {
                Ok(blocks) => blocks.into_iter().next(),
                Err(error) => {
                    return RangeAcquisition {
                        blocks: complete,
                        failure: Some(error.context(format!(
                            "failed to re-acquire block {number} in range acquisition"
                        ))),
                    };
                }
            };
        }
    }

    RangeAcquisition {
        blocks: complete,
        failure: None,
    }
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
    profile_block_timings: bool,
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
            // Strides are bounded by the *finalized* head on either stream: an
            // out-of-order range is coherent only where no reorg can reach it
            // (ADR-18). The hint is read, never awaited — and not even issued
            // before the first delivery, since it spends the budget the block
            // fetch needs (ADR-6, HZ-1).
            if beg > from {
                rpc.refresh_finalized_head();
            }
            let finalized = rpc.finalized_head_hint();

            let top = finalized.as_ref().map_or(beg, |(num, _)| (*num).min(end));

            if top > beg && (top - beg) > stride_size as u64 {
                // ── Backfill mode ────────────────────────────────────────────
                // Parallel strides, emitted in order as they complete.
                speculative_next = None;
                poll_head = None;
                poll_last_read = None;

                // The range is final, so the report is true of it (WP-11.6).
                let finalized_ref = finalized.map(|(number, hash)| BlockRef { number, hash });

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
                        let acquisition = acquire_range_stride(&rpc2, &req2, &numbers).await;
                        (s, acquisition)
                    }
                }))
                .buffered(stride_concurrency);

                while let Some((s, acquisition)) = strides.next().await {
                    let RangeAcquisition {
                        blocks: raw_blocks,
                        mut failure,
                    } = acquisition;
                    if raw_blocks.is_empty() {
                        debug_assert!(
                            failure.is_some(),
                            "non-empty requested stride at {s} returned neither blocks nor failure"
                        );
                        Err(failure.take().unwrap_or_else(|| anyhow!(
                            "range acquisition at block {s} returned neither blocks nor failure"
                        )))?;
                    }

                    let last_num = raw_blocks
                        .last()
                        .expect("the empty acquisition returned above")
                        .number;
                    beg = last_num + 1;

                    let mapped = map_range_prefix(
                        raw_blocks,
                        mapping_options.clone(),
                        failure.as_ref(),
                    )
                    .await?;
                    yield IngestBatch { blocks: mapped, finalized: finalized_ref.clone() };

                    // Complete blocks below the fault have now been delivered;
                    // fail the session before any later stride can cross the gap.
                    if let Some(error) = failure {
                        Err(error)?;
                    }
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
                // Incoherent answers for `next_num`. A null answer does not
                // reset this per-block budget: mixed-version upstreams may
                // alternate a contradictory block with "not produced yet".
                let mut incoherent_attempts: u32 = 0;

                let spawn_enrich = |body: RawRpcBlock, body_received: Option<Instant>| -> EnrichTask {
                    let rpc2 = rpc.clone();
                    let req2 = req.clone();
                    let opts2 = mapping_options.clone();
                    tokio::spawn(async move {
                        let enriched = rpc2.enrich_block_with_retry(body, &req2).await?;
                        let timings = body_received.map(|br| {
                            let enrich_done = Instant::now();
                            vec![(br, enrich_done)]
                        });
                        map_blocks_cpu(vec![enriched], opts2, timings).await
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
                            let now = Instant::now();
                            cadence.record_block(now);
                            hot_streak += 1;
                            incoherent_attempts = 0;
                            // OB-4: this path never reads the head tag, but a
                            // delivered N proves the upstream reached N.
                            if let Some(metrics) = rpc.metrics() {
                                metrics.observe_upstream_head(next_num);
                            }
                            let body_received = if profile_block_timings { Some(now) } else { None };
                            pending.push_back((next_num, spawn_enrich(body, body_received)));
                            next_num += 1;
                        }
                        Some(body) => {
                            // The block exists and contradicts itself, so waiting
                            // cannot heal it: WP-11.2 bounds the re-acquisition
                            // and WP-11.3 ends the session (the ladder takes
                            // over). A missing block is the arm below, and stays
                            // unbounded — that one is ordinary head-following.
                            hot_streak = 0;
                            let reason = body
                                .error_message
                                .unwrap_or_else(|| "incoherent block".to_string());
                            if incoherent_attempts >= P_ENRICH_RETRIES {
                                if let Some(metrics) = rpc.metrics() {
                                    metrics.record_acquisition_retry_exhausted();
                                }
                                // Deliver what is already in flight first: those
                                // blocks are complete and sit below the failure
                                // (WP-11.5).
                                let mut drain_failure = None;
                                while let Some((pending_num, task)) = pending.pop_front() {
                                    match task.await {
                                        Ok(Ok(mapped)) => {
                                            yield IngestBatch { blocks: mapped, finalized: None };
                                        }
                                        Ok(Err(error)) => {
                                            drain_failure = Some(format!(
                                                "block {pending_num} enrichment failed: {error:#}"
                                            ));
                                            break;
                                        }
                                        Err(error) => {
                                            drain_failure = Some(format!(
                                                "block {pending_num} enrichment task failed: {error}"
                                            ));
                                            break;
                                        }
                                    }
                                }
                                for (_, task) in pending.drain(..) {
                                    task.abort();
                                }
                                let drain_context = drain_failure.map_or_else(String::new, |error| {
                                    format!("; while draining earlier blocks: {error}")
                                });
                                Err(anyhow!(
                                    "failed to acquire block {next_num} after \
                                     {P_ENRICH_RETRIES} retries: {reason}{drain_context}"
                                ))?;
                            }
                            incoherent_attempts += 1;
                            warn!(
                                block = next_num,
                                attempt = incoherent_attempts,
                                max_retries = P_ENRICH_RETRIES,
                                reason = %reason,
                                "incoherent block at the head, re-acquiring"
                            );
                            sleep(P_ENRICH_DELAY).await;
                        }
                        None => {
                            // Not produced yet. While waiting, opportunistically
                            // drain a completed front task.
                            hot_streak = 0;
                            // An exact OB-4 observation, not an absence of one:
                            // N does not exist and N-1 was delivered, so the
                            // upstream head is N-1. LIV-2 needs it to tell an
                            // idle chain from a stalled service here.
                            if let (Some(metrics), Some(below)) =
                                (rpc.metrics(), next_num.checked_sub(1))
                            {
                                metrics.observe_upstream_head(below);
                            }
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
                // Poll the finalized head, then apply the same bounded
                // whole-block acquisition contract as strided backfill.

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
                if head < beg {
                    // No finalized acquisition obligation exists yet. Keep the
                    // range helper's non-empty input contract local and wait
                    // until the polled commitment reaches `beg`.
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                let stride_end = beg
                    .saturating_add(stride_size as u64)
                    .min(head)
                    .min(end);
                let numbers: Vec<u64> = (beg..=stride_end).collect();

                let RangeAcquisition {
                    blocks: raw_blocks,
                    mut failure,
                } = acquire_range_stride(&rpc, &req, &numbers).await;

                if raw_blocks.is_empty() {
                    debug_assert!(
                        failure.is_some(),
                        "non-empty finalized poll range returned neither blocks nor failure"
                    );
                    Err(failure.take().unwrap_or_else(|| anyhow!(
                        "finalized poll acquisition at block {beg} returned neither blocks nor \
                         failure"
                    )))?;
                }

                let last = raw_blocks
                    .last()
                    .expect("the empty acquisition returned above");
                let last_num = last.number;
                let finalized = Some(BlockRef {
                    number: last.number,
                    hash: last.hash.clone(),
                });

                poll_last_read = Some(last_num);
                beg = last_num + 1;

                let mapped =
                    map_range_prefix(raw_blocks, mapping_options.clone(), failure.as_ref()).await?;
                yield IngestBatch { blocks: mapped, finalized };

                // Complete blocks below the fault have now been delivered;
                // terminate instead of polling the same bad block forever.
                if let Some(error) = failure {
                    Err(error)?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetch::RpcOptions;
    use rpc_client::{RpcClient, RpcClientConfig};

    #[tokio::test]
    #[should_panic(expected = "range acquisition requires at least one block number")]
    async fn range_acquisition_rejects_an_empty_number_set() {
        let client = Arc::new(RpcClient::new(RpcClientConfig {
            url: "http://127.0.0.1:1".to_string(),
            retry_attempts: 0,
            ..Default::default()
        }));
        let rpc = Arc::new(Rpc::new(client, RpcOptions::default()));

        let _ = acquire_range_stride(&rpc, &DataRequest::default(), &[]).await;
    }
}
