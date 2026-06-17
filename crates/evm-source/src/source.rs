/// EvmRpcDataSource: the top-level DataSource implementation.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_stream::stream;
use async_trait::async_trait;
use data_service_core::source::{DataSource, StreamRequest};
use data_service_core::{Block, BlockBatch, BlockRef, StreamError};
use futures::stream::{BoxStream, StreamExt};
use tracing::warn;

use crate::fetch::{Rpc, RpcOptions};
use crate::ingest::{ingest_range, IngestBatch};
use crate::normalization::MappingOptions as NormOptions;
use crate::types::DataRequest;
use rpc_client::RpcClient;

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
        let rpc = Arc::new(Rpc::new(client, options.rpc_options));
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
    /// Design:
    /// - A background task owns the probe queue and fires rounds (≤5 probes,
    ///   ≥500 ms between rounds) over a watch channel.
    /// - The stream loop does `tokio::select!` over the ingest stream and the
    ///   probe result channel, forwarding fresh batches immediately.
    fn finalize_stream<S>(
        rpc: Arc<Rpc>,
        stream: S,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>>
    where
        S: futures::Stream<Item = Result<BlockBatch, StreamError>> + Send + 'static,
    {
        use std::collections::VecDeque;
        use tokio::sync::mpsc;

        // Channel from prober task → stream loop.
        // The prober sends the newly confirmed finalized BlockRef (or nothing on
        // error/no-advance).
        let (probe_tx, mut probe_rx) = mpsc::channel::<Option<BlockRef>>(8);

        // Channel from stream loop → prober: new refs to enqueue.
        let (queue_tx, mut queue_rx) = mpsc::unbounded_channel::<BlockRef>();

        // Spawn prober background task.
        tokio::spawn(async move {
            let mut probe_queue: VecDeque<BlockRef> = VecDeque::new();
            let mut last_probe_time = std::time::Instant::now() - Duration::from_secs(1);

            loop {
                // Drain any new refs from the queue channel (non-blocking)
                loop {
                    match queue_rx.try_recv() {
                        Ok(r) => {
                            if probe_queue.len() >= 50 {
                                *probe_queue.back_mut().unwrap() = r;
                            } else {
                                probe_queue.push_back(r);
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }

                if probe_queue.is_empty() || last_probe_time.elapsed() < Duration::from_millis(500)
                {
                    // Wait for a new ref or for the 500ms window to open.
                    // Use a short sleep to avoid busy-looping.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    // Re-drain the queue
                    loop {
                        match queue_rx.try_recv() {
                            Ok(r) => {
                                if probe_queue.len() >= 50 {
                                    *probe_queue.back_mut().unwrap() = r;
                                } else {
                                    probe_queue.push_back(r);
                                }
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => return,
                        }
                    }
                    continue;
                }

                last_probe_time = std::time::Instant::now();
                let probes: Vec<BlockRef> = probe_queue.drain(..probe_queue.len().min(5)).collect();
                let probe_numbers: Vec<u64> = probes.iter().map(|p| p.number).collect();

                match rpc.get_finalized_block_batch(&probe_numbers).await {
                    Ok(infos) => {
                        let mut confirmed: Option<BlockRef> = None;
                        let mut confirmed_idx: Option<usize> = None;

                        for i in (0..infos.len()).rev() {
                            if let Some(ref info) = infos[i] {
                                if info.1 == probes[i].hash {
                                    confirmed = Some(probes[i].clone());
                                    confirmed_idx = Some(i);
                                    break;
                                }
                            }
                        }

                        if let (Some(_), Some(idx)) = (&confirmed, confirmed_idx) {
                            // Put back unfinalized refs
                            for r in probes[idx + 1..].iter().rev() {
                                probe_queue.push_front(r.clone());
                            }
                            // Send confirmed finalized ref
                            if probe_tx.send(confirmed).await.is_err() {
                                return;
                            }
                        } else {
                            // None confirmed — put all back
                            for r in probes.into_iter().rev() {
                                probe_queue.push_front(r);
                            }
                            if probe_tx.send(None).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("finalizer probe error: {e}");
                        // Put refs back
                        for r in probes.into_iter().rev() {
                            probe_queue.push_front(r);
                        }
                        if probe_tx.send(None).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let s = stream! {
            let mut current_finalized: Option<BlockRef> = None;
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
                                if let Some(ref fh) = batch.finalized_head {
                                    // Already finalized (finalized stream path)
                                    current_finalized = Some(fh.clone());
                                } else {
                                    // Queue refs for probing
                                    for block in &batch.blocks {
                                        let _ = queue_tx.send(block.block_ref());
                                    }
                                }

                                let finalized_head = batch.finalized_head.or_else(|| current_finalized.clone());
                                yield Ok(BlockBatch {
                                    blocks: batch.blocks,
                                    finalized_head,
                                });
                            }
                        }
                    }

                    // Probe result from prober task
                    probe_result = probe_rx.recv() => {
                        match probe_result {
                            None => {
                                // Prober task exited — no more probes
                            }
                            Some(None) => {
                                // Probe round: nothing confirmed, continue
                            }
                            Some(Some(finalized_ref)) => {
                                // New finalized head confirmed
                                if current_finalized.as_ref().is_none_or(|c| finalized_ref.number > c.number) {
                                    current_finalized = Some(finalized_ref.clone());
                                    // Emit a finalized-head-only batch
                                    yield Ok(BlockBatch {
                                        blocks: vec![],
                                        finalized_head: Some(finalized_ref),
                                    });
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

    async fn get_finalized_head(&self) -> Result<BlockRef> {
        let (number, hash) = self.rpc.get_latest_blockhash("finalized").await?;
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
