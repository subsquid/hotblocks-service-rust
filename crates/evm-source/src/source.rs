/// EvmRpcDataSource: the top-level DataSource implementation.
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_stream::stream;
use async_trait::async_trait;
use data_service_core::source::{DataSource, StreamRequest};
use data_service_core::{Block, BlockBatch, BlockRef, Metrics, StreamError};
use futures::stream::{BoxStream, StreamExt};
use tracing::warn;

use crate::fetch::{Rpc, RpcOptions};
use crate::ingest::{ingest_range, IngestBatch};
use crate::normalization::MappingOptions as NormOptions;
use crate::types::DataRequest;
use rpc_client::RpcClient;

/// ADR-6's spacing, applied only to a round that settles nothing: a round that
/// settles is draining a backlog, and holding it back caps the watermark's rate
/// at `PROBE_BATCH` per `PROBE_INTERVAL`.
const PROBE_INTERVAL: Duration = Duration::from_millis(500);
const PROBE_BATCH: usize = 5;
const PROBE_QUEUE_CAP: usize = 50;

/// What a delivered batch tells the confirmation prober.
enum ProbeMsg {
    /// A head block whose finality is not yet known.
    Candidate(BlockRef),
    /// Confirmed by the batch's own report; nothing at or below needs a probe.
    Settled(BlockRef),
}

/// Hand a message to the prober without blocking ingestion.
///
/// Dropping on a full ingress loses nothing: a candidate is subsumed by a later
/// one, a settlement only costs a probe of a ref that will confirm.
fn offer(tx: &tokio::sync::mpsc::Sender<ProbeMsg>, msg: ProbeMsg) {
    let _ = tx.try_send(msg);
}

/// Fold one message into the prober's queue and watermark.
fn absorb(queue: &mut VecDeque<BlockRef>, confirmed: &mut Option<BlockRef>, msg: ProbeMsg) {
    match msg {
        ProbeMsg::Candidate(candidate) => {
            if confirmed
                .as_ref()
                .is_some_and(|c| c.number >= candidate.number)
            {
                return;
            }
            if queue.len() >= PROBE_QUEUE_CAP {
                // Replace the newest, not the oldest: the backlog's head is what
                // still needs confirming, and the probe walk reads it ascending.
                *queue.back_mut().expect("a full queue has a back") = candidate;
            } else {
                queue.push_back(candidate);
            }
        }
        ProbeMsg::Settled(settled) => {
            while queue
                .front()
                .is_some_and(|front| front.number <= settled.number)
            {
                queue.pop_front();
            }
            if confirmed.as_ref().is_none_or(|c| settled.number > c.number) {
                *confirmed = Some(settled);
            }
        }
    }
}

/// Options for creating an EvmRpcDataSource.
#[derive(Debug, Clone)]
pub struct EvmRpcDataSourceOptions {
    pub rpc_options: RpcOptions,
    pub data_request: DataRequest,
    pub stride_size: usize,
    pub stride_concurrency: usize,
    /// Emit per-block pipeline timing logs (target=block_timing) for latency profiling.
    pub profile_block_timings: bool,
}

impl Default for EvmRpcDataSourceOptions {
    fn default() -> Self {
        EvmRpcDataSourceOptions {
            rpc_options: RpcOptions::default(),
            data_request: DataRequest::default(),
            stride_size: 5,
            stride_concurrency: 5,
            profile_block_timings: false,
        }
    }
}

/// EVM RPC data source implementing the DataSource trait.
pub struct EvmRpcDataSource {
    rpc: Arc<Rpc>,
    data_request: Arc<DataRequest>,
    norm_options: Arc<NormOptions>,
    stride_size: usize,
    stride_concurrency: usize,
    profile_block_timings: bool,
}

impl EvmRpcDataSource {
    pub fn new(client: Arc<RpcClient>, options: EvmRpcDataSourceOptions) -> Self {
        Self::build(client, options, None)
    }

    /// Report the OB-4 view and WP-11.3 exhaustions onto the service's registry.
    pub fn with_metrics(
        client: Arc<RpcClient>,
        options: EvmRpcDataSourceOptions,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::build(client, options, Some(metrics))
    }

    fn build(
        client: Arc<RpcClient>,
        options: EvmRpcDataSourceOptions,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        let rpc = Rpc::new(client, options.rpc_options);
        let rpc = Arc::new(match metrics {
            Some(metrics) => rpc.with_metrics(metrics),
            None => rpc,
        });
        let data_request = Arc::new(options.data_request.clone());
        let norm_options = Arc::new(NormOptions {
            with_traces: options.data_request.traces,
            with_state_diffs: options.data_request.state_diffs,
        });

        EvmRpcDataSource {
            rpc,
            data_request,
            norm_options,
            stride_size: options.stride_size.max(1),
            stride_concurrency: options.stride_concurrency.max(1),
            profile_block_timings: options.profile_block_timings,
        }
    }

    fn make_stream(
        &self,
        req: StreamRequest,
        commitment: &str,
    ) -> BoxStream<'static, Result<IngestBatch, anyhow::Error>> {
        let rpc = self.rpc.clone();
        let data_request = self.data_request.clone();
        let norm_options = self.norm_options.clone();
        let stride_size = self.stride_size;
        let stride_concurrency = self.stride_concurrency;
        let commitment = commitment.to_string();
        let profile_block_timings = self.profile_block_timings;

        let s = stream! {
            let mut inner = Box::pin(ingest_range(
                rpc,
                data_request,
                norm_options,
                req.from,
                req.to,
                stride_size,
                stride_concurrency,
                &commitment,
                profile_block_timings,
            ).await);

            while let Some(item) = inner.next().await {
                yield item;
            }
        };

        Box::pin(s)
    }

    fn ensure_continuity<S>(
        stream: S,
        from: u64,
        parent_hash: Option<String>,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>>
    where
        S: futures::Stream<Item = Result<IngestBatch, anyhow::Error>> + Send + 'static,
    {
        let s = stream! {
            let mut expected_parent = parent_hash;
            let mut stream = Box::pin(stream);

            while let Some(item) = stream.next().await {
                let batch = item.map_err(StreamError::Other)?;
                let mut fork_at = None;

                for (i, block) in batch.blocks.iter().enumerate() {
                    let block_parent = block.parent_hash.clone();
                    if let Some(ref ep) = expected_parent {
                        if ep != &block_parent {
                            fork_at = Some(i);
                            break;
                        }
                    }
                    expected_parent = Some(block.hash.clone());
                }

                if let Some(fork_pos) = fork_at {
                    // Yield blocks before the fork
                    if fork_pos > 0 {
                        let pre_fork: Vec<Block> = batch.blocks[..fork_pos].to_vec();
                        yield Ok(BlockBatch {
                            blocks: pre_fork,
                            finalized_head: batch.finalized.clone(),
                        });
                    }

                    // The fork block
                    let fork_block = &batch.blocks[fork_pos];
                    let previous_blocks = vec![BlockRef {
                        number: fork_block.number.saturating_sub(1),
                        hash: fork_block.parent_hash.clone(),
                    }];

                    yield Err(StreamError::Fork { previous_blocks });
                    return;
                }

                if !batch.blocks.is_empty() {
                    yield Ok(BlockBatch {
                        blocks: batch.blocks,
                        finalized_head: batch.finalized,
                    });
                }
            }
        };

        Box::pin(s)
    }

    /// Wrap a stream so that finalization probes run *concurrently* with
    /// passthrough of fresh batches — fixing the stall where an in-flight
    /// probe RTT delays emission of new blocks.
    ///
    /// A confirmed finalized head is not emitted as its own batch: that would
    /// share the consumer with head blocks and stall ingestion during an epoch's
    /// finalization. It updates `current_finalized`, which the head-batch arm
    /// attaches to the next head batch. Finality lags the head by minutes, so
    /// deferring it one head block is safe.
    ///
    /// Design:
    /// - A background task owns the probe queue and fires rounds (≤5 probes,
    ///   ≥500 ms between rounds) over a channel.
    /// - The stream loop does `tokio::select!` over the ingest stream and the
    ///   probe result channel, forwarding fresh batches immediately.
    fn finalize_stream<S>(
        rpc: Arc<Rpc>,
        stream: S,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>>
    where
        S: futures::Stream<Item = Result<BlockBatch, StreamError>> + Send + 'static,
    {
        use tokio::sync::mpsc;

        // Channel from prober task → stream loop: newly confirmed refs only.
        let (probe_tx, mut probe_rx) = mpsc::channel::<BlockRef>(8);

        // Bounded like the queue behind it, or a stalled prober accumulates a
        // backlog outside the cap meant to hold it (`P-PROBE-BACKLOG`, PF-1).
        let (queue_tx, mut queue_rx) = mpsc::channel::<ProbeMsg>(PROBE_QUEUE_CAP);

        // Spawn prober background task.
        tokio::spawn(async move {
            let mut queue: VecDeque<BlockRef> = VecDeque::new();
            let mut confirmed: Option<BlockRef> = None;
            let mut last_round = Instant::now() - PROBE_INTERVAL;

            loop {
                // Nothing to probe: block until the stream loop hands over work.
                while queue.is_empty() {
                    match queue_rx.recv().await {
                        Some(msg) => absorb(&mut queue, &mut confirmed, msg),
                        None => return,
                    }
                }
                // Take everything already waiting before spending a round on it.
                loop {
                    match queue_rx.try_recv() {
                        Ok(msg) => absorb(&mut queue, &mut confirmed, msg),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }
                if queue.is_empty() {
                    continue;
                }

                let wait = PROBE_INTERVAL.saturating_sub(last_round.elapsed());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                last_round = Instant::now();

                let mut probes: Vec<BlockRef> =
                    queue.drain(..queue.len().min(PROBE_BATCH)).collect();
                let numbers: Vec<u64> = probes.iter().map(|p| p.number).collect();

                let infos = match rpc.get_finalized_block_batch(&numbers).await {
                    Ok(infos) => infos,
                    Err(e) => {
                        warn!("finalizer probe error: {e}");
                        for r in probes.into_iter().rev() {
                            queue.push_front(r);
                        }
                        continue;
                    }
                };

                // Finality is a maximum, so the walk runs top-down and stops at
                // the first ref it settles. `infos` answers a prefix: the call
                // drops probes above the finalized head, and the queue ascends.
                let mut hit: Option<BlockRef> = None;
                let mut idx = infos.len() as isize - 1;
                while idx >= 0 {
                    let i = idx as usize;
                    if confirmed
                        .as_ref()
                        .is_some_and(|c| c.number >= probes[i].number)
                    {
                        // This ref and everything below it is already settled.
                        break;
                    }
                    match infos[i].as_ref() {
                        None => {}
                        Some((_, hash)) if *hash == probes[i].hash => {
                            hit = Some(probes[i].clone());
                            break;
                        }
                        Some(_) => {
                            // Upstream holds another block at this height, so it
                            // can never confirm; kept, it repeats every round.
                            probes.remove(i);
                        }
                    }
                    idx -= 1;
                }

                // Above where the walk stopped is open; at or below is settled or
                // unconfirmable.
                debug_assert!(probes.len() >= (idx + 1) as usize);
                let open = probes.split_off((idx + 1) as usize);
                if open.is_empty() {
                    // Settled the lot: drain at the upstream's pace, not the
                    // timer's.
                    last_round = Instant::now() - PROBE_INTERVAL;
                } else {
                    for r in open.into_iter().rev() {
                        queue.push_front(r);
                    }
                }

                if let Some(r) = hit {
                    confirmed = Some(r.clone());
                    if probe_tx.send(r).await.is_err() {
                        return;
                    }
                }
            }
        });

        let s = stream! {
            let mut current_finalized: Option<BlockRef> = None;
            let mut prober_alive = true;
            let mut stream = Box::pin(stream);

            loop {
                tokio::select! {
                    // Fresh batch from the ingest stream — forward immediately
                    batch_opt = stream.next() => {
                        match batch_opt {
                            None => break,
                            Some(Err(e)) => {
                                yield Err(e);
                                return;
                            }
                            Some(Ok(batch)) => {
                                let carried = batch.finalized_head.clone();
                                match (&carried, batch.blocks.last()) {
                                    // Finalized as a whole, so its own head is
                                    // confirmed — and unlike the report, it is a
                                    // ref this stream delivered.
                                    (Some(_), Some(last)) => {
                                        let settled = last.block_ref();
                                        if current_finalized.as_ref().is_none_or(|c| settled.number > c.number) {
                                            current_finalized = Some(settled.clone());
                                        }
                                        offer(&queue_tx, ProbeMsg::Settled(settled));
                                    }
                                    _ => {
                                        for block in &batch.blocks {
                                            offer(&queue_tx, ProbeMsg::Candidate(block.block_ref()));
                                        }
                                    }
                                }

                                let finalized_head = carried.or_else(|| current_finalized.clone());
                                yield Ok(BlockBatch {
                                    blocks: batch.blocks,
                                    finalized_head,
                                });
                            }
                        }
                    }

                    // Probe result from prober task
                    probe_result = probe_rx.recv(), if prober_alive => {
                        match probe_result {
                            // Prober gone; head batches keep the last watermark.
                            None => prober_alive = false,
                            Some(finalized_ref) => {
                                // Recorded only. A finality-only batch here would
                                // serialize behind head ingestion (ADR-6).
                                if current_finalized.as_ref().is_none_or(|c| finalized_ref.number > c.number) {
                                    current_finalized = Some(finalized_ref);
                                }
                            }
                        }
                    }
                }
            }
        };

        Box::pin(s)
    }
}

#[async_trait]
impl DataSource for EvmRpcDataSource {
    async fn get_head(&self) -> Result<BlockRef> {
        let (number, hash) = self.rpc.get_latest_blockhash("latest").await?;
        Ok(BlockRef { number, hash })
    }

    /// T1's read (WP-20): it opens the epoch the finality view belongs to, so it
    /// replaces that view rather than reading alongside it.
    async fn get_finalized_head(&self) -> Result<BlockRef> {
        let (number, hash) = self.rpc.resync_finalized_head().await?;
        Ok(BlockRef { number, hash })
    }

    fn get_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let parent_hash = req.parent_hash.clone();
        let from = req.from;
        let inner = self.make_stream(req, "latest");
        let continuity = Self::ensure_continuity(inner, from, parent_hash);
        Self::finalize_stream(self.rpc.clone(), continuity)
    }

    fn get_finalized_stream(
        &self,
        req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let parent_hash = req.parent_hash.clone();
        let from = req.from;
        let inner = self.make_stream(req, "finalized");
        let continuity = Self::ensure_continuity(inner, from, parent_hash);
        // Map to BoxStream<Result<BlockBatch, StreamError>>
        Box::pin(continuity)
    }
}
