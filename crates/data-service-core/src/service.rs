//! DataService ingestion loop — exact port of `data-service.ts`.

use crate::chain::Chain;
use crate::metrics::{record_block_ingestion, Metrics};
use crate::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use crate::types::{Block, BlockHeader, BlockRef, DataResponse, InvalidBaseBlock, QueryError};
use anyhow::Context;
use futures::StreamExt;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::watch;
use tracing::{error, info};

/// Options for `run_data_service`.
pub struct DataServiceOptions<S> {
    pub source: S,
    pub block_cache_size: usize,
    pub port: u16,
    pub auto_adjust_finalized_head: bool,
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
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    cancel_tx: watch::Sender<bool>,
    server_task: tokio::task::JoinHandle<()>,
    service_task: tokio::task::JoinHandle<()>,
}

impl DataServiceHandle {
    pub async fn shutdown(self) {
        // Signal the HTTP server to stop accepting new connections.
        let _ = self.shutdown_tx.send(());
        // Signal the ingestion loop to stop (wakes it even if blocked on stream.next()).
        let _ = self.cancel_tx.send(true);
        // Abort the service task so we don't wait for a stuck stream.next().
        self.service_task.abort();
        let _ = self.service_task.await;
        let _ = self.server_task.await;
    }
}

/// Start the data service: initialise, bind HTTP, run ingestion loop.
/// Mirrors `runDataService` in `index.ts`.
pub async fn run_data_service<S: DataSource>(
    opts: DataServiceOptions<S>,
) -> anyhow::Result<DataServiceHandle> {
    let (cancel_tx, cancel_rx) = watch::channel(false);

    let service = Arc::new(DataService::new(
        opts.source,
        opts.block_cache_size,
        opts.auto_adjust_finalized_head,
        cancel_rx,
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

    Ok(DataServiceHandle {
        port,
        started: started_rx,
        ended: ended_rx,
        shutdown_tx,
        cancel_tx,
        server_task,
        service_task,
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
        let (block_watch_tx, block_watch_rx) = watch::channel(0u64);
        let (started_tx, started_rx) = watch::channel(Err(()));
        Self {
            source,
            buffer_size,
            auto_adjust_finalized_head,
            metrics: Arc::new(Metrics::new()),
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
        match self.source.get_head().await {
            Ok(head) => head.number <= self.get_chain_ref(|c| c.get_head().number),
            Err(_) => false,
        }
    }

    /// The watch channel that fires once the first block is ingested (or dies).
    pub fn started_watch(&self) -> watch::Receiver<Result<(), ()>> {
        self.started_rx.clone()
    }

    /// Initialise: fetch finalized head, seed chain with that one block.
    /// Mirrors `DataService.init()` in data-service.ts.
    pub async fn init(&self) -> anyhow::Result<()> {
        let head = self
            .source
            .get_finalized_head()
            .await
            .context("failed to get finalized head during init")?;

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
        let seed = batch.blocks.into_iter().next().unwrap();
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

        loop {
            if self.stop_requested() {
                return RunEnd::Stopped;
            }

            let session_result = if stacked > 5 {
                // Full re-init.
                match self.init().await {
                    // WP-20: a divergent seed is FM-30, not a retry.
                    Err(e) if e.is::<DivergentReseed>() => {
                        error!(%e, "terminal divergence on re-seed");
                        return RunEnd::Terminal(e);
                    }
                    Err(e) => Err(StreamError::Other(e)),
                    Ok(()) => {
                        base = self.get_chain_ref(|c| c.get_header().block_ref());
                        info!(block = base.number, hash = %base.hash, "restarted data ingestion");
                        self.ingest_session(&base, &mut first_block_ingested).await
                    }
                }
            } else {
                self.ingest_session(&base, &mut first_block_ingested).await
            };

            if self.stop_requested() {
                return RunEnd::Stopped;
            }

            match session_result {
                Ok(()) => {
                    // Stream ended normally (shouldn't happen for head streams).
                    if !first_block_ingested {
                        let _ = self.started_tx.send(Err(()));
                        error!("data ingestion unexpectedly terminated before first block");
                        return RunEnd::StartupFailure(anyhow::anyhow!(
                            "head stream ended before the first block was ingested"
                        ));
                    }
                    let head = self.get_chain_ref(|c| c.get_header().block_ref());
                    stacked = if head.number == base.number {
                        stacked + 1
                    } else {
                        0
                    };
                    base = head;
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
                    stacked = 0;
                    let fork_base = self.get_chain_ref(|c| c.get_fork_base(&previous_blocks));
                    match fork_base {
                        Some(fb) => {
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
                    if !first_block_ingested {
                        let _ = self.started_tx.send(Err(()));
                        error!(%err, "data ingestion terminated before first block");
                        return RunEnd::StartupFailure(
                            err.context("first ingestion session died before its first block"),
                        );
                    }
                    let head = self.get_chain_ref(|c| c.get_header().block_ref());
                    stacked = if head.number == base.number {
                        stacked + 1
                    } else {
                        0
                    };
                    base = head;
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
            let batch: BlockBatch = batch_result?;

            if self.stopped.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }

            let start = Instant::now();

            // Hold the write lock only while processing this batch.
            {
                let mut guard = self.chain_write();
                let chain = guard.as_mut().expect("chain not initialized");

                // Track the highest finalized head seen across batches.
                let batch_finalized = batch.finalized_head.clone();

                for block in &batch.blocks {
                    tracing::debug!(
                        stage = "batch-received-main",
                        block_number = block.number,
                        block_hash = %block.hash,
                        "batch block received on main thread {}#{}", block.number, block.hash
                    );
                    record_block_ingestion(block.number);
                    let insert_start = Instant::now();
                    chain.push(block.clone());
                    let insert_elapsed = insert_start.elapsed();
                    tracing::debug!(
                        stage = "block-queryable",
                        block_number = block.number,
                        block_hash = %block.hash,
                        "block available for query {}#{}", block.number, block.hash
                    );
                    if let Some(ts) = block.timestamp {
                        self.metrics.observe_block_lag(ts);
                    }
                    // Emit per-block pipeline timing log (speculative hot blocks only).
                    if let Some(ref t) = block.timings {
                        let compress_done = t.compress_done();
                        let now = Instant::now();
                        let enrich_ms =
                            t.enrich_done.duration_since(t.body_received).as_secs_f64() * 1000.0;
                        let normalize_ms =
                            t.normalize_done.duration_since(t.enrich_done).as_secs_f64() * 1000.0;
                        let compress_ms = t.compress_duration.as_secs_f64() * 1000.0;
                        let queue_ms =
                            insert_start.duration_since(compress_done).as_secs_f64() * 1000.0;
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

                if !batch.blocks.is_empty() {
                    let header = chain.get_header();
                    log_block_info(&header, "new head");
                    if !*first_block_ingested {
                        *first_block_ingested = true;
                        let _ = self.started_tx.send(Ok(()));
                    }
                }

                // Apply finalized head (take the maximum seen so far).
                // The TS code tracks `finalizedHead` across the whole session
                // and only advances monotonically.
                if let Some(fh) = &batch_finalized {
                    if chain.finalize(fh) {
                        let fh_header = chain.get_finalized_header();
                        log_block_info(&fh_header, "new finalized head");
                    }
                }

                if !chain.compact() {
                    error!("block finalization lags behind and prevents cache purging");
                }

                self.trigger_update_locked(chain);
            }

            self.metrics.track_processing_time(start);
        }
        Ok(())
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
    pub async fn query(
        &self,
        from: u64,
        parent_hash: Option<&str>,
    ) -> Result<DataResponse, QueryError> {
        let below_window = self.get_chain_ref(|c| {
            let first = c.first_block();
            first.parent_number != first.number && from <= first.parent_number
        });

        let result = if below_window {
            let res = self.below_query(from, parent_hash).await;
            match &res {
                Err(_) => self.metrics.inc_query("error"),
                Ok(_) => self.metrics.inc_query("backfill"),
            }
            return res;
        } else {
            // Try cache first.
            let res = self.get_chain_ref(|c| c.query(from, parent_hash));
            match res {
                Err(invalid) => {
                    self.metrics.inc_query("error");
                    return Err(invalid.into());
                }
                Ok(resp) if resp.tail.is_some() => {
                    self.metrics.inc_query("cache");
                    return Ok(resp);
                }
                Ok(_) => {
                    // Block not yet available — wait up to 5s.
                    self.wait_for_block(from).await;
                    let res2 = self.get_chain_ref(|c| c.query(from, parent_hash));
                    match res2 {
                        Err(invalid) => {
                            self.metrics.inc_query("error");
                            Err(invalid.into())
                        }
                        Ok(r) => {
                            self.metrics.inc_query("cache");
                            Ok(r)
                        }
                    }
                }
            }
        };
        result
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
                return Err(QueryError::InvalidBaseBlock(InvalidBaseBlock {
                    prev: vec![],
                }));
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
                    assert_chain_continuity(ph_clone.as_deref(), prev.as_ref(), block);
                    prev = Some(block.clone());
                }
                yield first_batch.blocks.clone();

                // Yield remaining batches.
                let mut remaining = remaining;
                while let Some(item) = remaining.next().await {
                    let batch = item.map_err(|e| anyhow::anyhow!("{e}"))?;
                    for block in batch.blocks.iter() {
                        assert_chain_continuity(ph_clone.as_deref(), prev.as_ref(), block);
                        prev = Some(block.clone());
                    }
                    yield batch.blocks;
                }

                // Assert continuity with the snapshot tail.
                assert!(prev.is_some(), "at least one block was expected");
                if let Some(first_tail) = tail_for_stream.first() {
                    assert_chain_continuity(
                        ph_clone.as_deref(),
                        prev.as_ref(),
                        first_tail,
                    );
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

fn assert_chain_continuity(parent_hash: Option<&str>, prev: Option<&Block>, next: &Block) {
    let ok = match (prev, parent_hash) {
        (Some(p), _) => p.number == next.parent_number && p.hash == next.parent_hash,
        (None, Some(ph)) => ph == next.parent_hash,
        (None, None) => true,
    };
    assert!(ok, "chain continuity was violated by the data source");
}
