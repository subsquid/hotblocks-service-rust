//! DataService ingestion loop — exact port of `data-service.ts`.

use crate::chain::{is_chain, Chain, Push};
use crate::metrics::{now_ms, record_block_ingestion, Metrics, QueryClass, SessionRestart};
use crate::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use crate::types::{Block, BlockHeader, BlockRef, DataResponse, InvalidBaseBlock, QueryError};
use anyhow::Context;
use futures::StreamExt;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::watch;
use tracing::{error, info, warn};

/// Options for `run_data_service`.
pub struct DataServiceOptions<S> {
    pub source: S,
    pub block_cache_size: usize,
    pub port: u16,
    pub auto_adjust_finalized_head: bool,
    /// Registry shared by the acquisition layer and core. `None` builds a
    /// private registry; the core still maintains the upstream-head view.
    pub metrics: Option<Arc<Metrics>>,
}

/// FM-30: a T1 re-seed named a height the buffer it would discard holds under
/// a different ref *or* a different parent link (WP-20/INV-12, DEF-8) —
/// unrecoverable divergence, not a re-seed.
#[derive(Debug)]
pub struct DivergentReseed {
    pub seed: BlockRef,
    pub seed_parent: BlockRef,
    pub held: BlockRef,
    pub held_parent: BlockRef,
}

impl std::fmt::Display for DivergentReseed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "re-seed names {}#{} (parent {}#{}) but the buffer holds {}#{} (parent {}#{}) at that height",
            self.seed.number,
            self.seed.hash,
            self.seed_parent.number,
            self.seed_parent.hash,
            self.held.number,
            self.held.hash,
            self.held_parent.number,
            self.held_parent.hash,
        )
    }
}

impl std::error::Error for DivergentReseed {}

/// Whether `value` can be passed to `HeaderValue::from_str` without error.
/// This is the allocation-free HTTP field-value byte check: HTAB and bytes
/// from SP upwards are allowed, except DEL.
fn is_header_safe_hash(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte == b'\t' || (byte >= b' ' && byte != 0x7f))
}

/// A block a batch actually inserted, and when it became queryable. Absorbed
/// duplicates are absent: WP-6 makes them strict no-ops, observables included.
struct Ingested {
    /// Position in the batch, not the height: unique by construction, so it
    /// does not lean on DEF-20 to tell two entries apart.
    index: usize,
    at_ms: u64,
}

/// Validate everything decidable from an input batch alone. This runs before
/// opening a chain batch so malformed source data cannot touch chain state.
fn validate_ingest_batch(batch: &BlockBatch) -> Result<(), StreamError> {
    if batch.blocks.is_empty() {
        return Err(StreamError::Other(anyhow::anyhow!(
            "DEF-20: empty input batch"
        )));
    }

    // Validate hashes before any error path interpolates them into a message.
    for block in &batch.blocks {
        if block.hash.is_empty() {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-2: block hash at height {} is empty",
                block.number
            )));
        }
        if block.parent_hash.is_empty() {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-2: parent hash of block at height {} is empty",
                block.number
            )));
        }
        if !is_header_safe_hash(&block.hash) {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-2: block hash at height {} is not safe for an HTTP header value",
                block.number
            )));
        }
        if !is_header_safe_hash(&block.parent_hash) {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-2: parent hash of block at height {} is not safe for an HTTP header value",
                block.number
            )));
        }
    }
    if let Some(report) = batch.finalized_head.as_ref() {
        if report.hash.is_empty() {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-2: finality report at height {} carries an empty hash",
                report.number
            )));
        }
        if !is_header_safe_hash(&report.hash) {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-2: finality report at height {} is not safe for an HTTP header value",
                report.number
            )));
        }
    }

    // DEF-20: the adapter owes ascending, pairwise-linked batches, splitting
    // at every discontinuity (WP-11.5).
    for pair in batch.blocks.windows(2) {
        if pair[1].number <= pair[0].number || !is_chain(&pair[0], &pair[1]) {
            return Err(StreamError::Other(anyhow::anyhow!(
                "DEF-20: batch is not ascending and pairwise linked at {}#{} → {}#{}",
                pair[0].number,
                pair[0].hash,
                pair[1].number,
                pair[1].hash
            )));
        }
    }

    Ok(())
}

/// A query verdict together with the OB-5 class it will be counted under.
pub struct QueryOutcome {
    pub response: DataResponse,
    pub class: QueryClass,
}

/// How the ingestion run ended. Anything but `Stopped` requires action from
/// the binary: exit non-zero per IB-11 (FM-31 startup failure, FM-30
/// terminal divergence — ADR-12).
#[derive(Debug)]
pub enum RunEnd {
    /// `stop()` or shutdown cancellation.
    Stopped,
    /// WP-9/FM-31: the first-ever session died before ingesting a block.
    StartupFailure(anyhow::Error),
    /// FM-30: unrecoverable divergence — rollback below finality, or a
    /// divergent re-seed.
    Terminal(anyhow::Error),
}

/// Handle returned by `run_data_service`.
pub struct DataServiceHandle {
    pub port: u16,
    pub started: tokio::sync::oneshot::Receiver<anyhow::Result<()>>,
    /// Resolves when the ingestion run ends; `RunEnd` says how (IB-11).
    pub ended: tokio::sync::oneshot::Receiver<RunEnd>,
    pub metrics: Arc<Metrics>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    cancel_tx: watch::Sender<bool>,
    server_task: tokio::task::JoinHandle<()>,
    service_task: tokio::task::JoinHandle<()>,
    head_observer_task: tokio::task::JoinHandle<()>,
    levels_task: tokio::task::JoinHandle<()>,
}

impl DataServiceHandle {
    pub async fn shutdown(self) {
        self.metrics.record_shutdown_start();
        // Signal the HTTP server to stop accepting new connections.
        let _ = self.shutdown_tx.send(());
        // Signal the ingestion loop to stop (wakes it even if blocked on stream.next()).
        let _ = self.cancel_tx.send(true);
        // Abort the service task so we don't wait for a stuck stream.next().
        self.service_task.abort();
        self.head_observer_task.abort();
        self.levels_task.abort();
        let _ = self.service_task.await;
        let _ = self.server_task.await;
        let _ = self.head_observer_task.await;
        let _ = self.levels_task.await;
    }
}

/// Start the data service: initialise, bind HTTP, run ingestion loop.
/// Mirrors `runDataService` in `index.ts`.
pub async fn run_data_service<S: DataSource>(
    opts: DataServiceOptions<S>,
) -> anyhow::Result<DataServiceHandle> {
    let (cancel_tx, cancel_rx) = watch::channel(false);

    let service = Arc::new(DataService::with_metrics(
        opts.source,
        opts.block_cache_size,
        opts.auto_adjust_finalized_head,
        cancel_rx,
        opts.metrics.unwrap_or_default(),
    ));

    service.init().await?;

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (ended_tx, ended_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Build the axum router.
    let router = crate::http::build_router(Arc::clone(&service));
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", opts.port))
        .await
        .context("failed to bind HTTP listener")?;
    let port = listener.local_addr()?.port();

    let server_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();
    });

    let svc2 = Arc::clone(&service);
    let service_task = tokio::spawn(async move {
        // Wait for the first block to be ingested, then notify.
        let started_watch = svc2.started_watch();
        let mut watcher = started_watch;

        tokio::spawn(async move {
            // Wait for started signal.
            watcher.changed().await.ok();
            let result = if watcher.borrow().is_err() {
                Err(anyhow::anyhow!("data ingestion failed before first block"))
            } else {
                Ok(())
            };
            let _ = started_tx.send(result);
        });

        let end = svc2.run().await;
        let _ = ended_tx.send(end);
    });

    // Maintain the generic DataSource contract behind readiness. The ticker is
    // independent of HTTP probe cadence, and stream batches below provide a
    // lower-bound observation between explicit reads.
    let head_service = Arc::clone(&service);
    let head_observer_task = tokio::spawn(async move {
        let cadence = head_service.metrics.upstream_head_refresh_interval();
        let mut ticker = tokio::time::interval(cadence);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            head_service.refresh_upstream_head().await;
        }
    });

    // LIV-2's level must flip without waiting for a scrape: OB-10's capture
    // is an edge, and nobody may be looking.
    let metrics = Arc::clone(&service.metrics);
    let levels_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            ticker.tick().await;
            metrics.refresh_derived_levels();
        }
    });

    Ok(DataServiceHandle {
        port,
        started: started_rx,
        ended: ended_rx,
        metrics: Arc::clone(&service.metrics),
        shutdown_tx,
        cancel_tx,
        server_task,
        service_task,
        head_observer_task,
        levels_task,
    })
}

// ---------------------------------------------------------------------------

/// State shared between the ingestion loop and HTTP handlers.
///
/// Chain access uses `std::sync::RwLock` — operations are fast and we never
/// hold the lock across an await.
pub struct DataService<S> {
    source: S,
    buffer_size: usize,
    auto_adjust_finalized_head: bool,
    pub metrics: Arc<Metrics>,
    chain: RwLock<Option<Chain>>,
    /// Notified after every ingestion batch; value = last ingested block number.
    block_watch_tx: watch::Sender<u64>,
    block_watch_rx: watch::Receiver<u64>,
    /// `Ok(())` once first block ingested, `Err(())` if ingestion died first.
    started_tx: watch::Sender<Result<(), ()>>,
    started_rx: watch::Receiver<Result<(), ()>>,
    stopped: std::sync::atomic::AtomicBool,
    /// Fires `true` when shutdown has been requested.
    cancel_rx: watch::Receiver<bool>,
}

impl<S: DataSource> DataService<S> {
    pub fn new(
        source: S,
        buffer_size: usize,
        auto_adjust_finalized_head: bool,
        cancel_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::with_metrics(
            source,
            buffer_size,
            auto_adjust_finalized_head,
            cancel_rx,
            Arc::new(Metrics::new()),
        )
    }

    /// Share the acquisition layer's registry, so its OB-4 counters land on
    /// the same scrape surface as the state levels.
    pub fn with_metrics(
        source: S,
        buffer_size: usize,
        auto_adjust_finalized_head: bool,
        cancel_rx: watch::Receiver<bool>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (block_watch_tx, block_watch_rx) = watch::channel(0u64);
        let (started_tx, started_rx) = watch::channel(Err(()));
        Self {
            source,
            buffer_size,
            auto_adjust_finalized_head,
            metrics,
            chain: RwLock::new(None),
            block_watch_tx,
            block_watch_rx,
            started_tx,
            started_rx,
            stopped: std::sync::atomic::AtomicBool::new(false),
            cancel_rx,
        }
    }

    fn chain_read(&self) -> std::sync::RwLockReadGuard<'_, Option<Chain>> {
        self.chain.read().expect("chain lock poisoned")
    }

    fn chain_write(&self) -> std::sync::RwLockWriteGuard<'_, Option<Chain>> {
        self.chain.write().expect("chain lock poisoned")
    }

    fn get_chain_ref<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Chain) -> R,
    {
        let guard = self.chain_read();
        f(guard.as_ref().expect("chain not yet initialized"))
    }

    pub fn get_finalized_head(&self) -> BlockRef {
        self.get_chain_ref(|c| c.get_finalized_head())
    }

    pub fn get_head(&self) -> BlockRef {
        self.get_chain_ref(|c| c.get_head())
    }

    pub async fn is_ready(&self) -> bool {
        self.metrics
            .fresh_upstream_head()
            .is_some_and(|head| head <= self.get_chain_ref(|c| c.get_head().number))
    }

    async fn refresh_upstream_head(&self) {
        let timeout = self.metrics.upstream_head_refresh_timeout();
        match tokio::time::timeout(timeout, self.source.get_head()).await {
            Ok(Ok(head)) => self.metrics.observe_upstream_head(head.number),
            Ok(Err(_)) => warn!(
                "upstream head observation failed; readiness will use the previous view until it becomes stale"
            ),
            Err(_) => warn!(
                timeout_ms = timeout.as_millis(),
                "upstream head observation timed out; readiness will use the previous view until it becomes stale"
            ),
        }
    }

    /// The watch channel that fires once the first block is ingested (or dies).
    pub fn started_watch(&self) -> watch::Receiver<Result<(), ()>> {
        self.started_rx.clone()
    }

    /// Initialise: fetch finalized head, seed chain with that one block.
    /// Mirrors `DataService.init()` in data-service.ts.
    pub async fn init(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.buffer_size > 0,
            "P-CACHE-SIZE must be a positive integer"
        );
        let head = self
            .source
            .get_finalized_head()
            .await
            .context("failed to get finalized head during init")?;
        anyhow::ensure!(!head.hash.is_empty(), "DEF-2: finalized head hash is empty");
        anyhow::ensure!(
            is_header_safe_hash(&head.hash),
            "DEF-2: finalized head hash is not safe for an HTTP header value"
        );

        let mut stream = self.source.get_finalized_stream(StreamRequest {
            from: head.number,
            to: Some(head.number),
            parent_hash: None,
        });

        let batch = match stream.next().await {
            Some(r) => r.context("error fetching seed block")?,
            None => anyhow::bail!("finalized stream yielded no blocks during init"),
        };
        // Source fault, not a defect: to the ladder, never a panic in the task.
        anyhow::ensure!(
            batch.blocks.len() == 1,
            "expected exactly one seed block at {}, got {}",
            head.number,
            batch.blocks.len()
        );
        validate_ingest_batch(&batch)?;
        let seed = batch.blocks.into_iter().next().unwrap();
        anyhow::ensure!(
            seed.parent_number <= seed.number,
            "DEF-4: seed block {} claims parent height {} above itself",
            seed.number,
            seed.parent_number
        );
        // WP-20: the two calls may hit different nodes of a fleet, so the
        // delivered block is the seed only if it is the one that was named.
        anyhow::ensure!(
            seed.number == head.number && seed.hash == head.hash,
            "seed block {}#{} does not match the reported finalized head {}#{}",
            seed.number,
            seed.hash,
            head.number,
            head.hash
        );
        let mut guard = self.chain_write();
        if let Some(prev) = guard.as_ref() {
            // WP-20: a seed contradicting the buffer it would discard — by ref
            // or by parent link (DEF-8) — is FM-30. Discard nothing.
            if let Some(held) = prev.block_at(seed.number) {
                if held.hash != seed.hash
                    || held.parent_number != seed.parent_number
                    || held.parent_hash != seed.parent_hash
                {
                    return Err(anyhow::Error::new(DivergentReseed {
                        seed: seed.block_ref(),
                        seed_parent: BlockRef {
                            number: seed.parent_number,
                            hash: seed.parent_hash.clone(),
                        },
                        held: held.block_ref(),
                        held_parent: BlockRef {
                            number: held.parent_number,
                            hash: held.parent_hash.clone(),
                        },
                    }));
                }
            }
            let prev_finalized = prev.get_finalized_head();
            if seed.number < prev_finalized.number {
                // WP-20: an epoch reset below the previous watermark is
                // legal but MUST be alarmed as a watermark regression.
                self.metrics.record_watermark_regression();
                error!(
                    seed_number = seed.number,
                    seed_hash = %seed.hash,
                    prev_finalized_number = prev_finalized.number,
                    "watermark regression: re-seed opens a new epoch below the previous finalized head"
                );
            }
        }
        *guard = Some(Chain::new(
            seed,
            self.buffer_size,
            self.auto_adjust_finalized_head,
        ));
        self.trigger_update_locked(guard.as_ref().unwrap());
        Ok(())
    }

    fn stop_requested(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Relaxed) || *self.cancel_rx.borrow()
    }

    /// Main ingestion loop. Runs until `stop()` is called or the run ends per
    /// `RunEnd` — the ladder retries everything else forever (LIV-2).
    /// Mirrors `DataService.run()` in data-service.ts.
    pub async fn run(&self) -> RunEnd {
        let mut base: BlockRef = self.get_chain_ref(|c| c.get_header().block_ref());
        let mut stacked = 0i32;
        let mut first_block_ingested = false;
        // A fork rebase stays in the same logical session (WP-12/T6), so the
        // maximum outlives one concrete get_stream call. Other stream endings
        // tear the session down and reset it below.
        let mut finalized_max: Option<BlockRef> = None;

        loop {
            if self.stop_requested() {
                return RunEnd::Stopped;
            }

            let session_result = if stacked > 5 {
                // Full re-init.
                finalized_max = None;
                match self.init().await {
                    // WP-20: a divergent seed is FM-30, not a retry.
                    Err(e) if e.is::<DivergentReseed>() => {
                        error!(%e, "terminal divergence on re-seed");
                        self.metrics.record_terminal_state();
                        return RunEnd::Terminal(e);
                    }
                    Err(e) => Err(StreamError::Other(e)),
                    Ok(()) => {
                        // Only a re-seed that replaced the buffer restarted
                        // anything; a failed one falls back to the ladder.
                        self.metrics.record_session_restart(SessionRestart::Reset);
                        base = self.get_chain_ref(|c| c.get_header().block_ref());
                        info!(block = base.number, hash = %base.hash, "restarted data ingestion");
                        self.ingest_session(&base, &mut first_block_ingested, &mut finalized_max)
                            .await
                    }
                }
            } else {
                self.ingest_session(&base, &mut first_block_ingested, &mut finalized_max)
                    .await
            };

            if self.stop_requested() {
                return RunEnd::Stopped;
            }

            match session_result {
                Ok(()) => {
                    finalized_max = None;
                    // Stream ended normally (shouldn't happen for head streams).
                    if !first_block_ingested {
                        let _ = self.started_tx.send(Err(()));
                        error!("data ingestion unexpectedly terminated before first block");
                        return RunEnd::StartupFailure(anyhow::anyhow!(
                            "head stream ended before the first block was ingested"
                        ));
                    }
                    self.metrics.record_session_restart(SessionRestart::Error);
                    let head = self.get_chain_ref(|c| c.get_header().block_ref());
                    stacked = if head.number == base.number {
                        stacked + 1
                    } else {
                        0
                    };
                    base = head;
                    self.metrics
                        .note_session_end(stacked, Some("stream ended without an error"));
                    let pause = if stacked > 1 { 30 } else { 0 };
                    error!(
                        "data ingestion terminated, will restart in {} seconds",
                        pause
                    );
                    if pause > 0 {
                        tokio::time::sleep(tokio::time::Duration::from_secs(pause)).await;
                    }
                }
                Err(StreamError::Fork { previous_blocks }) => {
                    // T6 rebase: retain finalized_max for the replacement
                    // stream; its obligation still applies to this session.
                    stacked = 0;
                    let fork_base = self.get_chain_ref(|c| c.get_fork_base(&previous_blocks));
                    match fork_base {
                        Some(fb) => {
                            // Only here: a fork the buffer cannot rebase onto
                            // ends the run, and nothing restarts.
                            self.metrics.record_session_restart(SessionRestart::Fork);
                            self.metrics.record_fork_rebase();
                            self.metrics.note_session_end(0, None);
                            info!(
                                fork_base_number = fb.number,
                                fork_base_hash = %fb.hash,
                                upstream_blocks = ?previous_blocks,
                                "fork encountered"
                            );
                            base = fb;
                        }
                        None => {
                            // FM-30: divergence at or below the finalized head.
                            let finalized = self.get_finalized_head();
                            self.metrics.record_terminal_state();
                            error!(
                                finalized_head_number = finalized.number,
                                finalized_head_hash = %finalized.hash,
                                upstream_blocks = ?previous_blocks,
                                "rollback behind finalized head"
                            );
                            if !first_block_ingested {
                                let _ = self.started_tx.send(Err(()));
                            }
                            return RunEnd::Terminal(anyhow::anyhow!(
                                "rollback behind finalized head {}#{} (fork refs: {:?})",
                                finalized.number,
                                finalized.hash,
                                previous_blocks
                            ));
                        }
                    }
                }
                Err(StreamError::Other(err)) => {
                    finalized_max = None;
                    if !first_block_ingested {
                        let _ = self.started_tx.send(Err(()));
                        error!(%err, "data ingestion terminated before first block");
                        return RunEnd::StartupFailure(
                            err.context("first ingestion session died before its first block"),
                        );
                    }
                    self.metrics.record_session_restart(SessionRestart::Error);
                    let head = self.get_chain_ref(|c| c.get_header().block_ref());
                    stacked = if head.number == base.number {
                        stacked + 1
                    } else {
                        0
                    };
                    base = head;
                    self.metrics
                        .note_session_end(stacked, Some(&err.to_string()));
                    let pause = if stacked > 1 { 30u64 } else { 0 };
                    error!(%err, "data ingestion terminated, will restart in {} seconds", pause);
                    if pause > 0 {
                        tokio::time::sleep(tokio::time::Duration::from_secs(pause)).await;
                    }
                }
            }
        }
    }

    /// One ingestion session: open a stream and ingest until it ends or errors.
    async fn ingest_session(
        &self,
        base: &BlockRef,
        first_block_ingested: &mut bool,
        finalized_max: &mut Option<BlockRef>,
    ) -> Result<(), StreamError> {
        let mut stream = self.source.get_stream(StreamRequest {
            // WP-14: at the coordinate ceiling there is no next block to ask for.
            from: base.number.saturating_add(1),
            to: None,
            parent_hash: Some(base.hash.clone()),
        });

        let mut cancel_rx = self.cancel_rx.clone();

        loop {
            let batch_result = tokio::select! {
                biased;
                _ = cancel_rx.wait_for(|v| *v) => return Ok(()),
                item = stream.next() => match item {
                    None => break,
                    Some(r) => r,
                },
            };
            let batch: BlockBatch = match batch_result {
                Err(StreamError::Fork { previous_blocks }) if previous_blocks.is_empty() => {
                    // FM-13/WP-10: an empty fork signal names no rebase target.
                    // Treat it like every other malformed adapter response so
                    // the stalled-session ladder supplies retry/backoff instead
                    // of rebasing to the current head in a hot loop.
                    return Err(StreamError::Other(anyhow::anyhow!(
                        "fork signal contains no previous blocks"
                    )));
                }
                result => result?,
            };

            if self.stopped.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }

            validate_ingest_batch(&batch)?;
            let start = Instant::now();

            // Hold the write lock only while processing this batch.
            {
                let mut guard = self.chain_write();
                let chain = guard.as_mut().expect("chain not initialized");

                // WP-5: the batch lands whole or not at all. A finality report
                // the arriving blocks contradict is only decidable once they
                // are in, so the buffer is restored rather than pre-checked.
                chain.begin_batch();
                let inserted_any = match self.apply_batch_locked(chain, &batch, finalized_max) {
                    Ok(inserted) => {
                        let inserted_any = !inserted.is_empty();
                        chain.commit_batch();
                        // Ingest-time observables are published only by a
                        // batch that survived. Source progress belongs to the
                        // committed tail; each inserted block carries the
                        // instant it became queryable, so a rolled-back block
                        // must not answer `/block-time`, an absorbed duplicate
                        // must not rewrite the time its block was first seen
                        // (WP-6), and no block is charged for the batch it
                        // waited on.
                        if let Some(last) = batch.blocks.last() {
                            self.metrics.observe_upstream_progress(last.number);
                        }
                        for Ingested { index, at_ms } in inserted {
                            let block = &batch.blocks[index];
                            record_block_ingestion(block.number, at_ms);
                            if let Some(ts) = block.timestamp {
                                self.metrics.observe_block_lag(ts, at_ms);
                            }
                        }
                        inserted_any
                    }
                    Err(e) => {
                        chain.rollback_batch();
                        self.metrics.record_integrity_violation();
                        return Err(e);
                    }
                };

                // OB-2 counts the batch, not the blocks: an all-duplicate
                // batch is still ingestion working.
                self.metrics.record_commit();

                // At-least-once delivery admits a non-empty batch made only of
                // absorbed redeliveries. It commits no new head and cannot
                // complete startup (WP-6/WP-9).
                if inserted_any {
                    let header = chain.get_header();
                    log_block_info(&header, "new head");
                    if !*first_block_ingested {
                        *first_block_ingested = true;
                        let _ = self.started_tx.send(Ok(()));
                    }
                }

                let compaction = chain.compact();
                if let Some(past) = compaction.force_advanced_past {
                    self.metrics.record_force_advance(past);
                }
                if !compaction.within_window() {
                    error!(
                        excess = compaction.excess,
                        "block finalization lags behind and prevents cache purging"
                    );
                }

                self.trigger_update_locked(chain);
            }

            self.metrics.track_processing_time(start);
        }
        Ok(())
    }

    /// Apply one batch and its finality report to an open batch scope. Every
    /// error leaves the caller to roll back; nothing here is durable.
    fn apply_batch_locked(
        &self,
        chain: &mut Chain,
        batch: &BlockBatch,
        finalized_max: &mut Option<BlockRef>,
    ) -> Result<Vec<Ingested>, StreamError> {
        let batch_finalized = batch.finalized_head.clone();
        let mut inserted = Vec::new();

        // Spec 13's `note_report`: the WP-12 staleness gate first, then the
        // equal-height contradiction — decidable without applying the batch at
        // all, so it costs nothing to reject it up front.
        if let (Some(report), Some(current)) = (batch_finalized.as_ref(), finalized_max.as_ref()) {
            if report.number >= chain.get_finalized_head().number
                && report.number == current.number
                && report.hash != current.hash
            {
                return Err(StreamError::Other(anyhow::anyhow!(
                    "WP-12: conflicting finality reports at height {}: {} and {}",
                    report.number,
                    current.hash,
                    report.hash
                )));
            }
        }

        for (index, block) in batch.blocks.iter().enumerate() {
            tracing::debug!(
                stage = "batch-received-main",
                block_number = block.number,
                block_hash = %block.hash,
                "batch block received on main thread {}#{}", block.number, block.hash
            );
            let insert_start = Instant::now();
            let outcome = chain.push(block.clone()).map_err(|e| {
                StreamError::Other(
                    anyhow::Error::new(e).context("WP-5: the batch contradicts the buffer"),
                )
            })?;
            if outcome == Push::Inserted {
                inserted.push(Ingested {
                    index,
                    at_ms: now_ms(),
                });
            }
            let insert_elapsed = insert_start.elapsed();
            tracing::debug!(
                stage = "block-queryable",
                block_number = block.number,
                block_hash = %block.hash,
                "block available for query {}#{}", block.number, block.hash
            );
            // Emit per-block pipeline timing log (speculative hot blocks only).
            if let Some(ref t) = block.timings {
                let compress_done = t.compress_done();
                let now = Instant::now();
                let enrich_ms =
                    t.enrich_done.duration_since(t.body_received).as_secs_f64() * 1000.0;
                let normalize_ms =
                    t.normalize_done.duration_since(t.enrich_done).as_secs_f64() * 1000.0;
                let compress_ms = t.compress_duration.as_secs_f64() * 1000.0;
                let queue_ms = insert_start.duration_since(compress_done).as_secs_f64() * 1000.0;
                let insert_ms = insert_elapsed.as_secs_f64() * 1000.0;
                let total_ms = now.duration_since(t.body_received).as_secs_f64() * 1000.0;
                tracing::info!(
                    target: "block_timing",
                    block_number = block.number,
                    enrich_ms,
                    normalize_ms,
                    compress_ms,
                    queue_ms,
                    insert_ms,
                    total_ms,
                    "block_timing"
                );
            }
        }

        // First settle the previous batch's obligation against the blocks that
        // just arrived. A later, higher report may replace it only after this
        // check (spec 13's apply_batch ordering).
        if let Some(fh) = finalized_max.as_ref() {
            settle_obligation(chain, fh)?;
        }

        let mut raised_finalized_max = false;
        if let Some(report) = batch_finalized {
            let applied = chain.get_finalized_head();
            if report.number >= applied.number
                && finalized_max
                    .as_ref()
                    .is_none_or(|current| report.number > current.number)
            {
                *finalized_max = Some(report);
                raised_finalized_max = true;
            }
        }

        // Apply a newly raised maximum immediately. If it is above the head,
        // Chain::finalize provisionally finalizes the whole buffer and this
        // same obligation is checked on later batches.
        if raised_finalized_max {
            if let Some(fh) = finalized_max.as_ref() {
                settle_obligation(chain, fh)?;
            }
        }

        Ok(inserted)
    }

    /// Update metrics and notify block watchers.
    /// Must be called with the chain write lock held (passed as `&Chain`).
    fn trigger_update_locked(&self, chain: &Chain) {
        let last = chain.last_block();
        self.metrics.set_first_block(chain.first_block_number());
        self.metrics.set_last_block(last.number);
        self.metrics
            .set_last_block_timestamp(last.timestamp.unwrap_or(0));
        self.metrics
            .set_finalized_block(chain.get_finalized_head().number);
        self.metrics.set_stored_blocks(chain.size());
        self.metrics.set_window_excess(chain.window_excess());

        let _ = self.block_watch_tx.send(last.number);
    }

    pub fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    /// Query for blocks.  Below-query streams from the finalized source;
    /// in-range queries serve from cache (waiting up to 5s if needed).
    ///
    /// Mirrors `DataService.query` in data-service.ts.
    /// The verdict and the OB-5 class it falls in, counted by neither: RP-13's
    /// outcome is only settled once the first record exists, which happens on
    /// the response path. `record_query_outcome` closes it there.
    pub async fn query(
        &self,
        from: u64,
        parent_hash: Option<&str>,
    ) -> Result<QueryOutcome, QueryError> {
        self.classify_query(from, parent_hash)
            .await
            .map(|(response, class)| QueryOutcome { response, class })
    }

    /// Settle one query's OB-5 outcome. `elapsed` runs from admission, so it
    /// covers producing the first record and not just reaching the verdict.
    pub fn record_query_outcome(&self, class: QueryClass, elapsed: std::time::Duration) {
        // `queries_total` keeps the predecessor's three values (REQ-24);
        // OB-5's classes are the finer split beside it.
        let compat = match class {
            QueryClass::Backfill => "backfill",
            QueryClass::Window | QueryClass::WaitEmpty => "cache",
            QueryClass::Conflict | QueryClass::Error => "error",
        };
        self.metrics.inc_query(compat);
        self.metrics.record_query(class, elapsed);
    }

    /// The query itself, paired with the OB-5 class its verdict falls in.
    async fn classify_query(
        &self,
        from: u64,
        parent_hash: Option<&str>,
    ) -> Result<(DataResponse, QueryClass), QueryError> {
        let below_window = self.get_chain_ref(|c| {
            let first = c.first_block();
            first.parent_number != first.number && from <= first.parent_number
        });

        if below_window {
            return self
                .below_query(from, parent_hash)
                .await
                .map(|response| (response, QueryClass::Backfill));
        }

        // Try cache first.
        match self.get_chain_ref(|c| c.query(from, parent_hash))? {
            resp if resp.tail.is_some() => Ok((resp, QueryClass::Window)),
            _ => {
                // Block not yet available — wait up to 5s.
                self.wait_for_block(from).await;
                let resp = self.get_chain_ref(|c| c.query(from, parent_hash))?;
                // RP-12: an expired wait is a distinct outcome from a window.
                let class = if resp.tail.is_some() {
                    QueryClass::Window
                } else {
                    QueryClass::WaitEmpty
                };
                Ok((resp, class))
            }
        }
    }

    /// Wait until `block_number` is available in the chain, or 5 seconds pass.
    async fn wait_for_block(&self, block_number: u64) {
        let mut rx = self.block_watch_rx.clone();
        let timeout = tokio::time::Duration::from_secs(5);
        let _ = tokio::time::timeout(timeout, async move {
            loop {
                if *rx.borrow_and_update() >= block_number {
                    return;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
    }

    /// Handle a query whose `from` block is below the first buffered block.
    ///
    /// 1. Snapshot chain state (tail + finalized_head) under lock.
    /// 2. Open `get_finalized_stream` from `from` to `first.parent_number`.
    /// 3. Eagerly await the first batch (may return InvalidBaseBlock on fork).
    /// 4. Return a DataResponse whose `head` streams remaining batches and
    ///    asserts chain continuity, then yields the snapshot `tail`.
    ///
    /// Mirrors `DataService.belowQuery` in data-service.ts.
    async fn below_query(
        &self,
        from: u64,
        parent_hash: Option<&str>,
    ) -> Result<DataResponse, QueryError> {
        let (tail, finalized_head, missing) = self.get_chain_ref(|c| {
            let missing = c
                .first_block()
                .parent_number
                .saturating_sub(from)
                .saturating_add(1);
            (c.snapshot(), c.get_finalized_head(), missing)
        });

        assert!(missing > 0, "no blocks are missing");

        info!(from_block = from, missing, "below query");

        let to = from.saturating_add(missing - 1);
        let parent_hash_owned = parent_hash.map(|s| s.to_string());

        let mut stream = self.source.get_finalized_stream(StreamRequest {
            from,
            to: Some(to),
            parent_hash: parent_hash_owned.clone(),
        });

        // Eagerly await the first batch.
        let first_batch = match stream.next().await {
            None => {
                return Err(QueryError::Internal(anyhow::anyhow!(
                    "below-query stream yielded no blocks for requested range {from}..={to}"
                )));
            }
            Some(Err(StreamError::Fork { previous_blocks })) if previous_blocks.is_empty() => {
                return Err(QueryError::Internal(anyhow::anyhow!(
                    "below-query stream reported a fork without previous blocks"
                )));
            }
            Some(Err(StreamError::Fork { previous_blocks })) => {
                return Err(QueryError::InvalidBaseBlock(InvalidBaseBlock {
                    prev: previous_blocks,
                }));
            }
            Some(Err(StreamError::Other(e))) => {
                // The TS `belowQuery` re-throws non-fork errors as-is, which
                // the HTTP layer turns into a 500. Surface it as an internal
                // error rather than crashing the request task.
                return Err(QueryError::Internal(e.context("below-query stream error")));
            }
            Some(Ok(batch)) => batch,
        };

        // Build the streaming head.
        let tail_arc = tail;
        let ph_clone = parent_hash_owned;

        let head_stream: futures::stream::BoxStream<'static, anyhow::Result<Vec<Block>>> = {
            let tail_for_stream = tail_arc.clone();
            // Convert the remaining stream into an owned stream.
            let remaining: futures::stream::BoxStream<'static, Result<crate::source::BlockBatch, StreamError>> =
                // We already consumed the first element from `stream`; the remaining items come next.
                stream;

            Box::pin(async_stream::try_stream! {
                let mut prev: Option<Block> = None;

                // Yield first batch.
                for block in first_batch.blocks.iter() {
                    check_chain_continuity(ph_clone.as_deref(), prev.as_ref(), block)?;
                    prev = Some(block.clone());
                }
                yield first_batch.blocks.clone();

                // Yield remaining batches.
                let mut remaining = remaining;
                while let Some(item) = remaining.next().await {
                    let batch = item.map_err(|e| anyhow::anyhow!("{e}"))?;
                    for block in batch.blocks.iter() {
                        check_chain_continuity(ph_clone.as_deref(), prev.as_ref(), block)?;
                        prev = Some(block.clone());
                    }
                    yield batch.blocks;
                }

                // Continuity with the snapshot tail closes the splice.
                if prev.is_none() {
                    Err(anyhow::anyhow!(
                        "below-query stream ended without delivering a block"
                    ))?;
                }
                if let Some(first_tail) = tail_for_stream.first() {
                    check_chain_continuity(
                        ph_clone.as_deref(),
                        prev.as_ref(),
                        first_tail,
                    )?;
                }
            })
        };

        Ok(DataResponse {
            finalized_head: Some(finalized_head),
            head: Some(head_stream),
            tail: Some(tail_arc),
        })
    }
}

/// Discharge the session's WP-12 obligation against the current buffer. A
/// contradiction ends the session (WP-5 ladder), leaving the buffer servable.
fn settle_obligation(chain: &mut Chain, fh: &BlockRef) -> Result<(), StreamError> {
    match chain.finalize(fh) {
        Ok(true) => {
            let fh_header = chain.get_finalized_header();
            log_block_info(&fh_header, "new finalized head");
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(e) => Err(StreamError::Other(
            anyhow::Error::new(e).context("WP-23: the finality obligation contradicts the buffer"),
        )),
    }
}

fn log_block_info(block: &BlockHeader, msg: &str) {
    let age_ms = block.timestamp.map(|ts| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
            - ts as i64
    });
    info!(
        block_number = block.number,
        block_hash = %block.hash,
        block_age_ms = age_ms.unwrap_or(-1),
        "{msg}"
    );
}

use std::time::{SystemTime, UNIX_EPOCH};

/// A continuity break mid-backfill is a source fault, not a defect: it reaches
/// the response as a countable stream error, not a panic dropping the body.
fn check_chain_continuity(
    parent_hash: Option<&str>,
    prev: Option<&Block>,
    next: &Block,
) -> anyhow::Result<()> {
    let ok = match (prev, parent_hash) {
        (Some(p), _) => p.number == next.parent_number && p.hash == next.parent_hash,
        (None, Some(ph)) => ph == next.parent_hash,
        (None, None) => true,
    };
    anyhow::ensure!(
        ok,
        "chain continuity was violated by the data source at {}#{} (parent {}#{})",
        next.number,
        next.hash,
        next.parent_number,
        next.parent_hash
    );
    Ok(())
}
