//! RPC fetch layer — ports evm-rpc/src/rpc.ts (minus Cronos phantom-tx).
#![allow(clippy::ptr_arg)]
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use data_service_core::metrics::Metrics;
use rpc_client::{CallOptions, RpcClient, RpcError, RpcErrorInfo};

use serde_json::{json, Value};
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use crate::chain_utils::{ChainUtils, Quirk};
use crate::rpc_data::{
    DebugFrameResult, DebugStateDiffResult, RawRpcBlock, RpcBlock, RpcLog, RpcReceipt,
    RpcWithdrawal, TraceTransactionReplay,
};
use crate::types::{qty2_u64, to_qty};
use crate::verification::{check_call_frame_tree, check_debug_frame_structure};

/// A component, or why the block is incoherent (DEF-15). The caller marks the
/// block invalid: WP-11.2 re-acquires, WP-11.3 fails loud. Never emptied or
/// thinned to keep a block servable (INV-28).
type Component<T> = std::result::Result<T, String>;

/// Why an enabled verification check rejected the block — incoherence on the
/// same path, never an immediate session error (WP-11.4).
type VerificationResult = std::result::Result<(), String>;

/// Semantic debug call-tree policy. Structural requirements needed for lossless
/// normalization are enforced in every mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CallFrameValidationMode {
    #[default]
    Off,
    Observe,
    Reject,
}

impl CallFrameValidationMode {
    fn as_str(self) -> &'static str {
        match self {
            CallFrameValidationMode::Off => "off",
            CallFrameValidationMode::Observe => "observe",
            CallFrameValidationMode::Reject => "reject",
        }
    }
}

impl std::str::FromStr for CallFrameValidationMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "off" => Ok(CallFrameValidationMode::Off),
            "observe" => Ok(CallFrameValidationMode::Observe),
            "reject" => Ok(CallFrameValidationMode::Reject),
            _ => Err(format!("unsupported call-frame validation mode: {value}")),
        }
    }
}

/// Options for the Rpc fetch layer.
#[derive(Debug, Clone, Default)]
pub struct RpcOptions {
    pub finality_confirmation: Option<u64>,
    /// How long the upstream finalized head may be served from cache.
    /// `None` → `FINALIZED_HEAD_TTL`.
    pub finalized_head_ttl: Option<std::time::Duration>,
    pub verify_block_hash: bool,
    pub verify_tx_sender: bool,
    pub verify_tx_root: bool,
    pub verify_receipts_root: bool,
    pub verify_withdrawals_root: bool,
    pub verify_logs_bloom: bool,
    pub call_frame_validation: CallFrameValidationMode,
    pub check_log_index: bool,
    pub check_cumulative_gas_used: bool,
    pub use_gas_used_for_receipts_root: bool,
}

impl RpcOptions {
    /// Validate option relationships before acquisition starts.
    pub fn validate(&self) -> Result<()> {
        if self.call_frame_validation == CallFrameValidationMode::Reject
            && (!self.verify_tx_root || !self.verify_tx_sender)
        {
            bail!("call-frame validation reject requires verify_tx_root and verify_tx_sender");
        }
        if self.use_gas_used_for_receipts_root && !self.verify_receipts_root {
            bail!("use_gas_used_for_receipts_root requires verify_receipts_root");
        }
        Ok(())
    }

    fn validate_request(&self, req: &crate::types::DataRequest) -> Result<()> {
        self.validate()?;
        if self.verify_receipts_root && !req.receipts {
            bail!("verify_receipts_root requires receipt acquisition");
        }
        Ok(())
    }
}

/// `P-FINALITY-VIEW-TTL`. The head moves at the chain's finality rate, so asking
/// per use buys no freshness.
const FINALIZED_HEAD_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// `P-ENRICH-RETRIES` / `P-ENRICH-DELAY` (spec/15): the whole re-acquisition
/// budget an incoherent block gets before WP-11.3 fails the session.
pub const P_ENRICH_RETRIES: u32 = 10;
pub const P_ENRICH_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// Fetch state for the Rpc layer.
pub struct Rpc {
    pub client: Arc<RpcClient>,
    pub options: RpcOptions,
    chain_utils: OnceCell<ChainUtils>,
    receipts_method: OnceCell<ReceiptsMethod>,
    finalized_head: std::sync::Mutex<FinalizedHeadCache>,
    /// Where OB-4's view and WP-11.3's exhaustion counter land; absent in the
    /// focused fetch-layer tests, which scrape nothing.
    metrics: Option<Arc<Metrics>>,
}

#[derive(Default)]
struct FinalizedHeadCache {
    /// Last observed upstream finalized head, with when it was observed.
    observed: Option<(std::time::Instant, u64, String)>,
    /// A refresh is already in flight; starting another buys nothing.
    refreshing: bool,
    /// Bumped by T1. A fetch issued under an older epoch is discarded, not
    /// stored: it describes a chain the new epoch has abandoned.
    epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiptsMethod {
    ByBlock,
    ByTx,
}

impl Rpc {
    pub fn new(client: Arc<RpcClient>, options: RpcOptions) -> Self {
        Rpc {
            client,
            options,
            chain_utils: OnceCell::new(),
            receipts_method: OnceCell::new(),
            finalized_head: std::sync::Mutex::new(FinalizedHeadCache::default()),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn metrics(&self) -> Option<&Arc<Metrics>> {
        self.metrics.as_ref()
    }

    /// Report an OB-4 observation of the upstream view.
    fn observe(&self, report: impl FnOnce(&Metrics)) {
        if let Some(metrics) = &self.metrics {
            report(metrics);
        }
    }

    fn finalized_head_ttl(&self) -> std::time::Duration {
        self.options
            .finalized_head_ttl
            .unwrap_or(FINALIZED_HEAD_TTL)
    }

    /// T1's read: replaces the cached view and opens a new epoch.
    ///
    /// A re-INIT may seed *below* the previous epoch (WP-20, ADR-14, FM-26), and
    /// the old view would then stand as a report above the fresh buffer.
    pub async fn resync_finalized_head(&self) -> Result<(u64, String)> {
        let (number, hash) = self.get_latest_blockhash("finalized").await?;
        let mut guard = self.finalized_head.lock().expect("finalized head cache");
        guard.epoch = guard.epoch.wrapping_add(1);
        guard.observed = Some((std::time::Instant::now(), number, hash.clone()));
        Ok((number, hash))
    }

    /// The finalized head, cached and fetched on a miss. For callers that may
    /// block on it: the prober runs off the ingestion path.
    pub async fn get_finalized_head_cached(&self) -> Result<(u64, String)> {
        if let Some(hit) = self.finalized_head_if_fresh() {
            return Ok(hit);
        }
        let epoch = self.finalized_head_epoch();
        let (number, hash) = self.get_latest_blockhash("finalized").await?;
        self.store_finalized_head(epoch, number, hash.clone());
        Ok((number, hash))
    }

    /// The last observed finalized head, never touching the network (ADR-6): a
    /// stale answer costs a shorter stride range, a delayed one costs a block.
    ///
    /// Age is deliberately unchecked. It cannot span a re-seed — T1 replaces the
    /// view — and within an epoch the view only rises.
    pub fn finalized_head_hint(&self) -> Option<(u64, String)> {
        let guard = self.finalized_head.lock().expect("finalized head cache");
        guard
            .observed
            .as_ref()
            .map(|(_, number, hash)| (*number, hash.clone()))
    }

    /// Start a background refresh if the hint has aged out. Idempotent, and
    /// never awaited: a failed refresh leaves the previous hint standing.
    pub fn refresh_finalized_head(self: &Arc<Self>) {
        let epoch = {
            let mut guard = self.finalized_head.lock().expect("finalized head cache");
            if guard.refreshing {
                return;
            }
            let fresh = guard
                .observed
                .as_ref()
                .is_some_and(|(at, _, _)| at.elapsed() < self.finalized_head_ttl());
            if fresh {
                return;
            }
            guard.refreshing = true;
            guard.epoch
        };

        let rpc = Arc::clone(self);
        tokio::spawn(async move {
            let fetched = rpc.get_latest_blockhash("finalized").await;
            rpc.finalized_head
                .lock()
                .expect("finalized head cache")
                .refreshing = false;
            match fetched {
                Ok((number, hash)) => rpc.store_finalized_head(epoch, number, hash),
                Err(e) => warn!("upstream finalized head refresh failed: {e}"),
            }
        });
    }

    fn finalized_head_epoch(&self) -> u64 {
        self.finalized_head
            .lock()
            .expect("finalized head cache")
            .epoch
    }

    fn finalized_head_if_fresh(&self) -> Option<(u64, String)> {
        let ttl = self.finalized_head_ttl();
        let guard = self.finalized_head.lock().expect("finalized head cache");
        let (observed_at, number, hash) = guard.observed.as_ref()?;
        (observed_at.elapsed() < ttl).then(|| (*number, hash.clone()))
    }

    /// Store a fetch issued under `epoch`, keeping the epoch's maximum.
    ///
    /// Discarded outright once T1 has opened a newer epoch. Within one the view
    /// is monotone, as WP-12 is for the reports it feeds: both fetch paths run
    /// when it expires, so a lagging replica answering last would otherwise pull
    /// the stride bound and probe filter back for a whole TTL.
    fn store_finalized_head(&self, epoch: u64, number: u64, hash: String) {
        let mut guard = self.finalized_head.lock().expect("finalized head cache");
        if guard.epoch != epoch {
            return;
        }
        let now = std::time::Instant::now();
        match guard.observed.as_mut() {
            // A lower answer is a lagging replica of the same chain, not a
            // contradiction: the view stands, and it was just re-observed.
            Some(observed) if observed.1 >= number => observed.0 = now,
            _ => guard.observed = Some((now, number, hash)),
        }
    }

    async fn get_chain_utils(&self) -> Result<&ChainUtils> {
        self.chain_utils
            .get_or_try_init(|| async {
                let chain_id: Value = self
                    .client
                    .call("eth_chainId", None, CallOptions::default())
                    .await
                    .map_err(|e| anyhow!("eth_chainId: {e}"))?;
                let chain_id_str = chain_id.as_str().unwrap_or("0x1");
                let chain_id_num = qty2_u64(chain_id_str);
                Ok(ChainUtils::new(
                    chain_id_num,
                    self.options.use_gas_used_for_receipts_root,
                ))
            })
            .await
    }

    /// Get the chain head block ref.
    pub async fn get_latest_blockhash(&self, commitment: &str) -> Result<(u64, String)> {
        let tag: Value = if commitment == "finalized" {
            if let Some(conf) = self.options.finality_confirmation {
                // Use offset from head
                let height = self.get_height().await?;
                let finalized = height.saturating_sub(conf);
                json!(to_qty(finalized))
            } else {
                json!("finalized")
            }
        } else {
            json!("latest")
        };

        let block: Value = self
            .client
            .call(
                "eth_getBlockByNumber",
                Some(json!([tag, false])),
                CallOptions::default(),
            )
            .await
            .map_err(|e| anyhow!("eth_getBlockByNumber(latest): {e}"))?;

        let number_str = block["number"]
            .as_str()
            .ok_or_else(|| anyhow!("missing block.number"))?;
        let hash = block["hash"]
            .as_str()
            .ok_or_else(|| anyhow!("missing block.hash"))?
            .to_string();
        let number = qty2_u64(number_str);

        // OB-4: the only place a watermark is read from the endpoint, so both
        // views are stamped here — a stale view must read as stale.
        self.observe(|metrics| {
            if commitment == "finalized" {
                metrics.observe_upstream_finalized_head(number);
            } else {
                metrics.observe_upstream_head(number);
            }
        });

        Ok((number, hash))
    }

    pub async fn get_height(&self) -> Result<u64> {
        let height: Value = self
            .client
            .call("eth_blockNumber", None, CallOptions::default())
            .await
            .map_err(|e| anyhow!("eth_blockNumber: {e}"))?;
        let height = qty2_u64(height.as_str().unwrap_or("0x0"));
        self.observe(|metrics| metrics.observe_upstream_head(height));
        Ok(height)
    }

    /// Fetch a single block by number (body + txs). Returns None if not yet available.
    pub async fn get_single_block(&self, number: u64) -> Result<Option<RawRpcBlock>> {
        let results = self.get_blocks(&[number], true).await?;
        Ok(results.into_iter().next().flatten())
    }

    /// Fetch finalized blocks for given numbers (used by finalizer).
    pub async fn get_finalized_block_batch(
        &self,
        numbers: &[u64],
    ) -> Result<Vec<Option<(u64, String)>>> {
        let (finalized_num, _) = self.get_finalized_head_cached().await?;
        let numbers: Vec<u64> = numbers
            .iter()
            .filter(|&&n| n <= finalized_num)
            .copied()
            .collect();
        if numbers.is_empty() {
            return Ok(vec![]);
        }

        let calls: Vec<(String, Option<Value>)> = numbers
            .iter()
            .map(|n| {
                (
                    "eth_getBlockByNumber".to_string(),
                    Some(json!([to_qty(*n), false])),
                )
            })
            .collect();

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &CallOptions::default())
            .await?;

        let mut out = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(v) => {
                    if v.is_null() {
                        out.push(None);
                    } else {
                        let n = v["number"].as_str().map(qty2_u64);
                        let h = v["hash"].as_str().map(|s| s.to_string());
                        match (n, h) {
                            (Some(n), Some(h)) => out.push(Some((n, h))),
                            _ => out.push(None),
                        }
                    }
                }
                Err(_) => out.push(None),
            }
        }
        Ok(out)
    }

    /// Fetch a batch of blocks with optional transaction data and attachments.
    pub async fn get_block_batch(
        &self,
        numbers: &[u64],
        req: &crate::types::DataRequest,
    ) -> Result<Vec<RawRpcBlock>> {
        let with_txs = true; // always fetch transactions for normalization

        let blocks = self.get_blocks(numbers, with_txs).await?;

        // Filter to contiguous chain
        let mut chain: Vec<RawRpcBlock> = Vec::new();
        for (i, block) in blocks.into_iter().enumerate() {
            match block {
                None => break,
                Some(b) => {
                    if i > 0 {
                        let prev_hash = &chain[i - 1].hash;
                        if !prev_hash.eq_ignore_ascii_case(&b.block.parent_hash) {
                            break;
                        }
                    }
                    chain.push(b);
                }
            }
        }

        self.add_requested_data(&mut chain, req).await?;
        Ok(chain)
    }

    /// Enrich a slice of block bodies with logs/receipts/traces.
    /// This is the second phase of the two-phase fetch for the speculative poll path.
    /// Each block's enrichment is retried independently on not-ready conditions.
    /// Blocks must be provided in order; returns them in the same order.
    pub async fn enrich_blocks(
        self: &Arc<Self>,
        blocks: Vec<RawRpcBlock>,
        req: &crate::types::DataRequest,
    ) -> Result<Vec<RawRpcBlock>> {
        let mut blocks = blocks;
        self.add_requested_data(&mut blocks, req).await?;
        Ok(blocks)
    }

    /// Enrich a single block with retry for not-ready conditions.
    /// Returns the enriched block once consistent data is available.
    /// This is the per-block retry loop for the pipeline overlap path.
    pub async fn enrich_block_with_retry(
        self: &Arc<Self>,
        body: RawRpcBlock,
        req: &crate::types::DataRequest,
    ) -> Result<RawRpcBlock> {
        let needs_enrichment = req.logs || req.receipts || req.traces || req.state_diffs;
        if !needs_enrichment {
            return Ok(body);
        }

        // Retry by re-fetching the WHOLE block (header + data) as one unit,
        // mirroring the TS `getBlocks` retry (evm-rpc/src/data-source/get-blocks.ts).
        // The first attempt reuses the speculatively-fetched header (`body`) so
        // a block that's ready immediately costs no extra eth_getBlockByNumber;
        // every retry re-fetches via `get_block_batch`, so a reorg / load-balanced
        // hash mismatch heals as soon as the canonical header arrives. Reusing a
        // stale header across retries (the original bug) made such a mismatch
        // permanent and hung the ingestion loop forever with no error.
        //
        // The retry is bounded; on exhaustion we surface the error so the
        // ingestion loop logs it and restarts, like `getBlocks` throwing
        // `_errorMessage` after its retries.
        //
        // Budget: 10 × 50ms = 500ms total — the same wall-clock window as the TS
        // `getBlocks` (5 × 100ms), just polled finer. Unlike TS this runs at the
        // chain *head* (speculative path), where receipts/logs legitimately lag
        // the header, so the window must comfortably exceed that lag for normal
        // head-following not to trip the (now fatal) bound.
        let number = body.number;
        let mut retries: u32 = 0;

        // First attempt: enrich the header we already fetched speculatively.
        // Network/RPC errors propagate (the client already retries transient
        // ones internally via batch_call_reduce_on_retry).
        let mut blocks = vec![body];
        self.add_requested_data(&mut blocks, req).await?;

        loop {
            // Enrichment only populates logs/receipts once they match the header
            // hash (see add_logs/add_receipts), so a ready block is simply one
            // that exists and wasn't marked invalid.
            if blocks.first().is_some_and(|b| !b.is_invalid) {
                return Ok(blocks.remove(0));
            }

            let err_msg = blocks
                .first()
                .and_then(|b| b.error_message.clone())
                .unwrap_or_else(|| "block not available".to_string());

            if retries >= P_ENRICH_RETRIES {
                self.observe(Metrics::record_acquisition_retry_exhausted);
                bail!(
                    "failed to enrich block {number} after {P_ENRICH_RETRIES} retries: {err_msg}"
                );
            }
            retries += 1;

            debug!(
                block = number,
                attempt = retries,
                max_retries = P_ENRICH_RETRIES,
                reason = %err_msg,
                "block enrichment not ready, retrying whole-block fetch"
            );

            tokio::time::sleep(P_ENRICH_DELAY).await;

            // Re-fetch the whole block (header + data) as one unit — TS
            // getBlockBatch semantics. An empty result (not produced yet / chain
            // break) leaves `blocks` empty and we keep retrying until the bound.
            blocks = self
                .get_block_batch(std::slice::from_ref(&number), req)
                .await?;
        }
    }

    async fn get_blocks(
        &self,
        numbers: &[u64],
        with_transactions: bool,
    ) -> Result<Vec<Option<RawRpcBlock>>> {
        if numbers.is_empty() {
            return Ok(vec![]);
        }

        let calls: Vec<(String, Option<Value>)> = numbers
            .iter()
            .map(|n| {
                (
                    "eth_getBlockByNumber".to_string(),
                    Some(json!([to_qty(*n), with_transactions])),
                )
            })
            .collect();

        let validate_error: Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync> =
            Box::new(|info: &RpcErrorInfo| {
                // Avalanche: out-of-range returns this error
                if info.message.contains("cannot query unfinalized data") {
                    return Ok(Value::Null);
                }
                // Hyperliquid: invalid block height — retry
                if info.message.contains("invalid block height") {
                    return Err(RpcError::RetryRequested("invalid block height".into()));
                }
                // Alchemy/Sei -32000 internal error — retry
                if info.code == -32000 {
                    return Err(RpcError::RetryRequested("internal error -32000".into()));
                }
                Err(RpcError::Rpc {
                    code: info.code,
                    message: info.message.clone(),
                    data: info.data.clone(),
                })
            });

        let options = CallOptions {
            validate_error: Some(validate_error),
            ..Default::default()
        };

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &options)
            .await?;

        let utils = self.get_chain_utils().await?;
        let mut blocks = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Err(_) => blocks.push(None),
                Ok(v) if v.is_null() => blocks.push(None),
                Ok(v) => {
                    let rpc_block: RpcBlock = match serde_json::from_value(v.clone()) {
                        Ok(b) => b,
                        Err(e) => {
                            warn!("Failed to parse block {}: {}", numbers[i], e);
                            blocks.push(None);
                            continue;
                        }
                    };

                    let number = qty2_u64(&rpc_block.number);
                    let hash = rpc_block.hash.clone();

                    // Sanity check
                    if number != numbers[i] {
                        let mut raw = RawRpcBlock::new(number, hash, rpc_block);
                        raw.mark_invalid("block number mismatch");
                        blocks.push(Some(raw));
                        continue;
                    }

                    let mut raw = RawRpcBlock::new(number, hash.clone(), rpc_block);

                    if let Err(reason) = self.verify_header(&raw, utils) {
                        raw.mark_invalid(reason);
                    }

                    blocks.push(Some(raw));
                }
            }
        }

        Ok(blocks)
    }

    /// The enabled header checks (DEF-25), as coherence gates: the caller marks
    /// the block, so WP-11.2 re-acquires it and WP-11.3 fails loud rather than
    /// the session ending on the first answer (REQ-14, WP-11.4).
    fn verify_header(&self, raw: &RawRpcBlock, utils: &ChainUtils) -> VerificationResult {
        let block = &raw.block;

        if self.options.verify_block_hash {
            let computed = utils
                .calculate_block_hash(block)
                .map_err(|e| format!("block hash verification failed: {e}"))?;
            if !computed.eq_ignore_ascii_case(&raw.hash) {
                return Err(format!(
                    "block hash mismatch: expected {} got {computed}",
                    raw.hash
                ));
            }
        }

        if self.options.verify_tx_root {
            let computed = utils
                .calculate_transactions_root(block)
                .map_err(|e| format!("transactions root verification failed: {e}"))?;
            // `None` — the block carries a transaction the RPC cannot express.
            if let Some(computed) = computed {
                if !computed.eq_ignore_ascii_case(&block.transactions_root) {
                    return Err(format!(
                        "transactions root mismatch: expected {} got {computed}",
                        block.transactions_root
                    ));
                }
            }
        }

        if self.options.verify_tx_sender {
            for tx in &block.transactions {
                let recovered = utils
                    .recover_tx_sender(tx)
                    .map_err(|e| format!("sender recovery failed for tx {}: {e}", tx.hash))?;
                let Some(sender) = recovered else { continue };
                if !sender.eq_ignore_ascii_case(&tx.from) {
                    return Err(format!(
                        "sender mismatch for tx {}: claimed {} recovered {sender}",
                        tx.hash, tx.from
                    ));
                }
            }
        }

        if self.options.verify_withdrawals_root {
            match (
                block.withdrawals_root.as_deref(),
                block.withdrawals.as_ref(),
            ) {
                (Some(claimed), Some(withdrawals)) => {
                    let refs: Vec<&RpcWithdrawal> = withdrawals.iter().collect();
                    let computed = utils
                        .calculate_withdrawals_root(&refs)
                        .map_err(|e| format!("withdrawals root verification failed: {e}"))?;
                    if !computed.eq_ignore_ascii_case(claimed) {
                        return Err(format!(
                            "withdrawals root mismatch: expected {claimed} got {computed}"
                        ));
                    }
                }
                (None, None) => {}
                (Some(_), None) => {
                    return Err("withdrawals are missing while withdrawalsRoot is present".into())
                }
                (None, Some(_)) => {
                    return Err("withdrawalsRoot is missing while withdrawals are present".into())
                }
            }
        }

        Ok(())
    }

    /// The enabled checks over a block's logs, and the log-index coherence
    /// check. Same gate as `verify_header`.
    fn verify_logs(
        &self,
        block: &RawRpcBlock,
        logs: &[&RpcLog],
        utils: &ChainUtils,
    ) -> VerificationResult {
        if self.options.check_log_index {
            for (i, log) in logs.iter().enumerate() {
                let actual = qty2_u64(&log.log_index);
                if actual != i as u64 {
                    return Err(format!("log index check failed: expected {i} got {actual}"));
                }
            }
        }

        if self.options.verify_logs_bloom {
            let computed = utils.calculate_logs_bloom(&block.block, logs);
            if !computed.eq_ignore_ascii_case(&block.block.logs_bloom) {
                return Err(format!(
                    "logs bloom mismatch: expected {} got {computed}",
                    block.block.logs_bloom
                ));
            }
        }

        Ok(())
    }

    /// The enabled receipt checks, plus cumulative-gas coherence.
    fn verify_receipts(
        &self,
        block: &RawRpcBlock,
        receipts: &[RpcReceipt],
        utils: &ChainUtils,
    ) -> VerificationResult {
        if self.options.check_cumulative_gas_used {
            let mut prev = 0u128;
            for receipt in utils.committed_receipts(&block.block, receipts.iter()) {
                let cumulative =
                    parse_qty_u128(&receipt.cumulative_gas_used, "receipt.cumulativeGasUsed")
                        .map_err(|error| format!("{error} at tx {}", receipt.transaction_hash))?;
                let used = parse_qty_u128(&receipt.gas_used, "receipt.gasUsed")
                    .map_err(|error| format!("{error} at tx {}", receipt.transaction_hash))?;
                let expected = prev.checked_add(used).ok_or_else(|| {
                    format!(
                        "cumulative gas used overflow at tx {}",
                        receipt.transaction_hash
                    )
                })?;
                if cumulative != expected {
                    return Err(format!(
                        "cumulative gas used check failed at tx {}",
                        receipt.transaction_hash
                    ));
                }
                prev = cumulative;
            }
        }

        if self.options.verify_receipts_root {
            let refs: Vec<&RpcReceipt> = receipts.iter().collect();
            let computed = utils
                .calculate_receipts_root(&block.block, &refs)
                .map_err(|e| format!("receipts root verification failed: {e}"))?;
            if !computed.eq_ignore_ascii_case(&block.block.receipts_root) {
                return Err(format!(
                    "receipts root mismatch: expected {} got {computed}",
                    block.block.receipts_root
                ));
            }
        }

        Ok(())
    }

    async fn add_requested_data(
        &self,
        blocks: &mut Vec<RawRpcBlock>,
        req: &crate::types::DataRequest,
    ) -> Result<()> {
        self.options.validate_request(req)?;

        let _tasks: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>> =
            Vec::new();

        // We need to add data sequentially since we can't easily split the &mut vec
        // across multiple concurrent futures. Run them sequentially here.

        if req.logs {
            self.add_logs(blocks).await?;
        }

        if req.receipts {
            self.add_receipts(blocks).await?;
        }

        if req.traces || req.state_diffs {
            self.add_traces(blocks, req).await?;
        }

        Ok(())
    }

    async fn add_logs(&self, blocks: &mut [RawRpcBlock]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let from = &blocks[0].block.number;
        let to = &blocks[blocks.len() - 1].block.number;

        let validate_error: Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync> =
            Box::new(|info: &RpcErrorInfo| {
                if info.message.contains("after last accepted block") {
                    return Ok(json!([]));
                }
                Err(RpcError::Rpc {
                    code: info.code,
                    message: info.message.clone(),
                    data: info.data.clone(),
                })
            });

        let result = self
            .client
            .call(
                "eth_getLogs",
                Some(json!([{"fromBlock": from, "toBlock": to}])),
                CallOptions {
                    validate_error: Some(validate_error),
                    ..Default::default()
                },
            )
            .await?;

        let logs: Vec<RpcLog> = serde_json::from_value(result)?;

        // Group logs by block hash
        let mut logs_by_block: std::collections::HashMap<String, Vec<RpcLog>> =
            std::collections::HashMap::new();
        for log in logs {
            logs_by_block
                .entry(log.block_hash.clone())
                .or_default()
                .push(log);
        }

        let utils = self.get_chain_utils().await?;

        for block in blocks.iter_mut() {
            let mut block_logs = logs_by_block.remove(&block.hash).unwrap_or_default();

            // If logs are empty but logsBloom is non-zero, logs are not yet available
            // (mirrors TS addLogs: only considers bloom to check readiness).
            // We mark the block invalid so the enrich retry loop will retry.
            // Note: an empty result for a block with no logs (bloom == 0x0...0) is correct.
            if block_logs.is_empty() && !is_zero_bloom(&block.block.logs_bloom) {
                block.mark_invalid(
                    "eth_getLogs returned empty result but logsBloom is non-zero (not ready)",
                );
                continue;
            }

            if utils.has(Quirk::NonSequentialLogIndexes) {
                fix_log_indexes(&mut block_logs);
            }

            let log_refs: Vec<&RpcLog> = block_logs.iter().collect();
            if let Err(reason) = self.verify_logs(block, &log_refs, utils) {
                block.mark_invalid(reason);
                continue;
            }

            block.logs = Some(block_logs);
        }

        Ok(())
    }

    async fn get_receipts_method(&self) -> Result<ReceiptsMethod> {
        if let Some(m) = self.receipts_method.get() {
            return Ok(*m);
        }

        // Probe eth_getBlockReceipts
        let result = self
            .client
            .call(
                "eth_getBlockReceipts",
                Some(json!(["latest"])),
                CallOptions::default(),
            )
            .await;

        let method = match result {
            Ok(v) if v.is_array() => ReceiptsMethod::ByBlock,
            _ => ReceiptsMethod::ByTx,
        };

        let _ = self.receipts_method.set(method);
        Ok(method)
    }

    async fn add_receipts(&self, blocks: &mut [RawRpcBlock]) -> Result<()> {
        let method = self.get_receipts_method().await?;
        match method {
            ReceiptsMethod::ByBlock => self.add_receipts_by_block(blocks).await,
            ReceiptsMethod::ByTx => self.add_receipts_by_tx(blocks).await,
        }
    }

    async fn add_receipts_by_block(&self, blocks: &mut [RawRpcBlock]) -> Result<()> {
        let calls: Vec<(String, Option<Value>)> = blocks
            .iter()
            .map(|b| {
                (
                    "eth_getBlockReceipts".to_string(),
                    Some(json!([b.block.number])),
                )
            })
            .collect();

        let validate_error: Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync> =
            Box::new(|info: &RpcErrorInfo| {
                if info.message.contains("invalid block height") {
                    return Err(RpcError::RetryRequested("invalid block height".into()));
                }
                // Not found / unknown block — treat as not-ready (null)
                if info.message.contains("unknown block")
                    || info.message.contains("not found")
                    || info.message.contains("header not found")
                {
                    return Ok(Value::Null);
                }
                Err(RpcError::Rpc {
                    code: info.code,
                    message: info.message.clone(),
                    data: info.data.clone(),
                })
            });

        let options = CallOptions {
            validate_error: Some(validate_error),
            ..Default::default()
        };

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &options)
            .await?;

        let utils = self.get_chain_utils().await?;

        for (i, result) in results.into_iter().enumerate() {
            let block = &mut blocks[i];
            match result {
                Err(e) => {
                    block.mark_invalid(format!("eth_getBlockReceipts error: {e}"));
                    continue;
                }
                Ok(v) if v.is_null() => {
                    block.mark_invalid("eth_getBlockReceipts returned null (block not ready)");
                    continue;
                }
                Ok(v) => {
                    // Parse receipts, filtering nulls
                    let raw_receipts: Vec<Option<RpcReceipt>> =
                        serde_json::from_value(v).unwrap_or_default();
                    let mut receipts: Vec<RpcReceipt> =
                        raw_receipts.into_iter().flatten().collect();

                    // Check all receipts belong to this block (hash consistency)
                    if let Some(bad) = receipts
                        .iter()
                        .find(|r| !r.block_hash.eq_ignore_ascii_case(&block.hash))
                    {
                        let msg = format!(
                            "eth_getBlockReceipts returned receipts for a different block \
                             (header {}, receipt block_hash {}) — reorg / load-balanced \
                             inconsistency, will retry",
                            block.hash, bad.block_hash
                        );
                        block.mark_invalid(msg);
                        continue;
                    }

                    if utils.has(Quirk::NonSequentialLogIndexes) {
                        renumber_logs(&mut receipts);
                    }

                    // After the renumbering: the checks judge what will be served.
                    let log_refs: Vec<&RpcLog> =
                        receipts.iter().flat_map(|r| r.logs.iter()).collect();

                    if let Err(reason) = self
                        .verify_logs(block, &log_refs, utils)
                        .and_then(|()| self.verify_receipts(block, &receipts, utils))
                    {
                        block.mark_invalid(reason);
                        continue;
                    }

                    if block.block.transactions.len() != receipts.len() {
                        block.mark_invalid(
                            "got invalid number of receipts from eth_getBlockReceipts",
                        );
                        continue;
                    }

                    block.receipts = Some(receipts);
                }
            }
        }

        Ok(())
    }

    async fn add_receipts_by_tx(&self, blocks: &mut [RawRpcBlock]) -> Result<()> {
        let mut calls: Vec<(String, Option<Value>)> = Vec::new();
        for block in blocks.iter() {
            for tx in &block.block.transactions {
                calls.push((
                    "eth_getTransactionReceipt".to_string(),
                    Some(json!([tx.hash])),
                ));
            }
        }

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &CallOptions::default())
            .await?;

        let mut result_iter = results.into_iter();
        let utils = self.get_chain_utils().await?;

        for block in blocks.iter_mut() {
            let tx_count = block.block.transactions.len();
            let mut receipts: Vec<RpcReceipt> = Vec::new();

            for _ in 0..tx_count {
                match result_iter.next() {
                    Some(Ok(v)) if !v.is_null() => {
                        if let Ok(r) = serde_json::from_value::<RpcReceipt>(v) {
                            receipts.push(r);
                        }
                    }
                    _ => {}
                }
            }

            if receipts.len() != tx_count {
                block.mark_invalid("failed to get receipts for all transactions");
                continue;
            }

            // Hash consistency check
            if let Some(bad) = receipts
                .iter()
                .find(|r| !r.block_hash.eq_ignore_ascii_case(&block.hash))
            {
                let msg = format!(
                    "eth_getTransactionReceipt returned receipts for a different block \
                     (header {}, receipt block_hash {}) — reorg / load-balanced \
                     inconsistency, will retry",
                    block.hash, bad.block_hash
                );
                block.mark_invalid(msg);
                continue;
            }

            if utils.has(Quirk::NonSequentialLogIndexes) {
                renumber_logs(&mut receipts);
            }

            let log_refs: Vec<&RpcLog> = receipts.iter().flat_map(|r| r.logs.iter()).collect();
            if let Err(reason) = self
                .verify_logs(block, &log_refs, utils)
                .and_then(|()| self.verify_receipts(block, &receipts, utils))
            {
                block.mark_invalid(reason);
                continue;
            }

            block.receipts = Some(receipts);
        }

        Ok(())
    }

    async fn add_traces(
        &self,
        blocks: &mut Vec<RawRpcBlock>,
        req: &crate::types::DataRequest,
    ) -> Result<()> {
        // Skip genesis block (not traceable)
        let traceable: Vec<usize> = blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| b.number != 0)
            .map(|(i, _)| i)
            .collect();

        if traceable.is_empty() {
            return Ok(());
        }

        // One trace method per selection (GAP-17): the replay call carries
        // traces only where it already runs for state diffs — dropping the
        // `trace` tracer there emptied gnosis traces — else `trace_block` alone.
        let need_replay_statediff = req.state_diffs && !req.use_debug_api_for_state_diffs;
        let need_replay_trace = req.traces && req.use_trace_api && need_replay_statediff;
        let need_replay = need_replay_trace || need_replay_statediff;

        // Debug frames (callTracer)
        let debug_frames_opt = if req.traces && !req.use_trace_api {
            let trace_blocks: Vec<&RawRpcBlock> = traceable.iter().map(|&i| &blocks[i]).collect();
            Some(self.fetch_debug_frames(&trace_blocks, req).await?)
        } else {
            None
        };

        // Debug state diffs (prestateTracer)
        let debug_diffs_opt = if req.state_diffs && req.use_debug_api_for_state_diffs {
            let trace_blocks: Vec<&RawRpcBlock> = traceable.iter().map(|&i| &blocks[i]).collect();
            Some(self.fetch_debug_state_diffs(&trace_blocks, req).await?)
        } else {
            None
        };

        // Trace replay
        let trace_replay_opt = if need_replay {
            let trace_blocks: Vec<&RawRpcBlock> = traceable.iter().map(|&i| &blocks[i]).collect();
            let mut tracers = Vec::new();
            if need_replay_trace {
                tracers.push("trace");
            }
            if need_replay_statediff {
                tracers.push("stateDiff");
            }
            Some(self.fetch_trace_replays(&trace_blocks, &tracers).await?)
        } else {
            None
        };

        // trace_block (use_trace_api without statediff)
        let trace_block_opt = if req.traces && req.use_trace_api && !need_replay_statediff {
            let trace_blocks: Vec<&RawRpcBlock> = traceable.iter().map(|&i| &blocks[i]).collect();
            Some(self.fetch_trace_block(&trace_blocks).await?)
        } else {
            None
        };

        // Now assign results (no more borrows of blocks elements)
        if let Some(debug_frames) = debug_frames_opt {
            for (i, frames) in traceable.iter().zip(debug_frames.into_iter()) {
                match frames {
                    Ok(f) => blocks[*i].debug_frames = Some(f),
                    Err(reason) => blocks[*i].mark_invalid(reason),
                }
            }
        }
        if let Some(debug_diffs) = debug_diffs_opt {
            for (i, diffs) in traceable.iter().zip(debug_diffs.into_iter()) {
                match diffs {
                    Ok(d) => blocks[*i].debug_state_diffs = Some(d),
                    Err(reason) => blocks[*i].mark_invalid(reason),
                }
            }
        }
        if let Some(replays) = trace_replay_opt {
            for (i, replay) in traceable.iter().zip(replays.into_iter()) {
                match replay {
                    Ok(r) => blocks[*i].trace_replays = Some(r),
                    Err(reason) => blocks[*i].mark_invalid(reason),
                }
            }
        }
        if let Some(replays) = trace_block_opt {
            for (i, replay) in traceable.iter().zip(replays.into_iter()) {
                match replay {
                    Ok(r) => blocks[*i].trace_replays = Some(r),
                    Err(reason) => blocks[*i].mark_invalid(reason),
                }
            }
        }

        Ok(())
    }

    async fn fetch_debug_frames(
        &self,
        blocks: &[&RawRpcBlock],
        req: &crate::types::DataRequest,
    ) -> Result<Vec<Component<Vec<Option<DebugFrameResult>>>>> {
        let timeout = req
            .debug_trace_timeout
            .as_deref()
            .unwrap_or("60s")
            .to_string();

        let trace_config = json!({
            "tracer": "callTracer",
            "tracerConfig": {
                "onlyTopCall": false,
                "withLog": true
            },
            "timeout": timeout
        });

        let calls: Vec<(String, Option<Value>)> = blocks
            .iter()
            .map(|b| {
                let (method, param) = if req.use_debug_trace_block_by_number {
                    (
                        "debug_traceBlockByNumber".to_string(),
                        json!(b.block.number),
                    )
                } else {
                    ("debug_traceBlockByHash".to_string(), json!(b.hash))
                };
                (method, Some(json!([param, trace_config])))
            })
            .collect();

        let validate_error: Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync> =
            Box::new(|info: &RpcErrorInfo| {
                if info.message.contains("not found") {
                    return Ok(Value::Null);
                }
                if info.message.contains("cannot query unfinalized data") {
                    return Ok(Value::Null);
                }
                Err(RpcError::Rpc {
                    code: info.code,
                    message: info.message.clone(),
                    data: info.data.clone(),
                })
            });

        let options = CallOptions {
            validate_error: Some(validate_error),
            ..Default::default()
        };

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &options)
            .await?;

        let utils = self.get_chain_utils().await?;
        let mut out = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            let block = blocks[i];
            let mut arr = match unwrap_component(result, "execution traces", block) {
                Ok(arr) => arr,
                Err(reason) => {
                    out.push(Err(reason));
                    continue;
                }
            };

            // Moonbeam quirk: may return frames without the `result` wrapper
            for item in arr.iter_mut() {
                if item.is_object() && item.get("result").is_none() {
                    let inner = item.take();
                    *item = json!({"result": inner});
                }
            }

            let frames: Component<Vec<Option<DebugFrameResult>>> =
                if block.block.transactions.len() == arr.len() {
                    arr.into_iter()
                        .map(|item| parse_entry(item, "trace frame", block).map(Some))
                        .collect()
                } else {
                    match_by_tx_hash(
                        arr,
                        block,
                        "execution traces",
                        utils.has(Quirk::PartialTraceCoverage),
                    )
                };

            out.push(frames.and_then(|frames| {
                validate_debug_frames(frames, block, self.options.call_frame_validation)
            }));
        }

        Ok(out)
    }

    async fn fetch_debug_state_diffs(
        &self,
        blocks: &[&RawRpcBlock],
        req: &crate::types::DataRequest,
    ) -> Result<Vec<Component<Vec<Option<DebugStateDiffResult>>>>> {
        let timeout = req
            .debug_trace_timeout
            .as_deref()
            .unwrap_or("60s")
            .to_string();

        let trace_config = json!({
            "tracer": "prestateTracer",
            "tracerConfig": {
                "onlyTopCall": false,
                "diffMode": true
            },
            "timeout": timeout
        });

        let calls: Vec<(String, Option<Value>)> = blocks
            .iter()
            .map(|b| {
                let (method, param) = if req.use_debug_trace_block_by_number {
                    (
                        "debug_traceBlockByNumber".to_string(),
                        json!(b.block.number),
                    )
                } else {
                    ("debug_traceBlockByHash".to_string(), json!(b.hash))
                };
                (method, Some(json!([param, trace_config])))
            })
            .collect();

        let validate_error: Box<dyn Fn(&RpcErrorInfo) -> Result<Value, RpcError> + Send + Sync> =
            Box::new(|info: &RpcErrorInfo| {
                if info.message.contains("not found") {
                    return Ok(Value::Null);
                }
                if info.message.contains("cannot query unfinalized data") {
                    return Ok(Value::Null);
                }
                Err(RpcError::Rpc {
                    code: info.code,
                    message: info.message.clone(),
                    data: info.data.clone(),
                })
            });

        let options = CallOptions {
            validate_error: Some(validate_error),
            ..Default::default()
        };

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &options)
            .await?;

        let utils = self.get_chain_utils().await?;
        let mut out = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            let block = blocks[i];
            let arr = match unwrap_component(result, "state diffs", block) {
                Ok(arr) => arr,
                Err(reason) => {
                    out.push(Err(reason));
                    continue;
                }
            };

            let diffs: Component<Vec<Option<DebugStateDiffResult>>> =
                if block.block.transactions.len() == arr.len() {
                    arr.into_iter()
                        .enumerate()
                        .map(|(j, item)| {
                            let diff: DebugStateDiffResult =
                                parse_entry(item, "state diff", block)?;
                            check_frame_label(diff.tx_hash.as_deref(), j, block)?;
                            Ok(Some(diff))
                        })
                        .collect()
                } else {
                    match_by_tx_hash(
                        arr,
                        block,
                        "state diffs",
                        utils.has(Quirk::PartialTraceCoverage),
                    )
                };

            out.push(diffs);
        }

        Ok(out)
    }

    async fn fetch_trace_replays(
        &self,
        blocks: &[&RawRpcBlock],
        tracers: &[&str],
    ) -> Result<Vec<Component<Vec<TraceTransactionReplay>>>> {
        let tracers_json: Vec<Value> = tracers.iter().map(|&t| json!(t)).collect();

        // Keep replay requests hash-addressed so they stay bound to the exact
        // header fetched before a possible reorg.
        let calls: Vec<(String, Option<Value>)> = blocks
            .iter()
            .map(|b| {
                (
                    "trace_replayBlockTransactions".to_string(),
                    Some(json!([b.hash, tracers_json])),
                )
            })
            .collect();

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &CallOptions::default())
            .await?;

        let mut out = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            let block = blocks[i];
            out.push(replays_of(result, block, tracers, "trace replays"));
        }

        Ok(out)
    }

    async fn fetch_trace_block(
        &self,
        blocks: &[&RawRpcBlock],
    ) -> Result<Vec<Component<Vec<TraceTransactionReplay>>>> {
        // Hash-addressed trace_block is not portable: some providers accept it
        // but return reward-only frames. Use the number, then bind every frame
        // back to the fetched header in trace_block_replays.
        let calls: Vec<(String, Option<Value>)> = blocks
            .iter()
            .map(|b| ("trace_block".to_string(), Some(json!([to_qty(b.number)]))))
            .collect();

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &CallOptions::default())
            .await?;

        let mut out = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            let block = blocks[i];
            out.push(trace_block_replays(result, block));
        }

        Ok(out)
    }
}

// ─── Component coherence helpers (DEF-15 / IB-15) ─────────────────────────────

/// An error, a null, or a non-result payload leaves the block incoherent.
fn unwrap_component(
    result: std::result::Result<Value, RpcError>,
    what: &str,
    block: &RawRpcBlock,
) -> Component<Vec<Value>> {
    let number = block.number;
    match result {
        Err(e) => Err(format!("block {number}: {what} unavailable: {e}")),
        Ok(Value::Null) => Err(format!("block {number}: {what} not available yet")),
        Ok(Value::Array(arr)) => Ok(arr),
        Ok(_) => Err(format!("block {number}: {what} payload is not an array")),
    }
}

fn parse_entry<T: serde::de::DeserializeOwned>(
    item: Value,
    what: &str,
    block: &RawRpcBlock,
) -> Component<T> {
    serde_json::from_value(item)
        .map_err(|e| format!("block {}: unparsable {what}: {e}", block.number))
}

const CALL_FRAME_VIOLATION_SAMPLE_LIMIT: usize = 3;

fn validate_debug_frames(
    frames: Vec<Option<DebugFrameResult>>,
    block: &RawRpcBlock,
    mode: CallFrameValidationMode,
) -> Component<Vec<Option<DebugFrameResult>>> {
    let mut violating_transaction_count = 0usize;
    let mut violation_samples = Vec::new();

    for (position, frame) in frames.iter().enumerate() {
        let Some(frame) = frame else {
            continue;
        };
        let Some(transaction) = block.block.transactions.get(position) else {
            return Err(format!(
                "block {}: debug call frame at position {position} has no transaction",
                block.number
            ));
        };

        check_frame_label(frame.tx_hash.as_deref(), position, block)?;

        if let Some(violation) = check_debug_frame_structure(&frame.result) {
            return Err(format!(
                "block {}: invalid debug call frames for transaction {}: {violation}",
                block.number, transaction.hash
            ));
        }

        if mode == CallFrameValidationMode::Off {
            continue;
        }
        let Some(violation) = check_call_frame_tree(transaction, &frame.result) else {
            continue;
        };

        if mode == CallFrameValidationMode::Reject {
            return Err(format!(
                "block {}: invalid debug call frames for transaction {}: {violation}",
                block.number, transaction.hash
            ));
        }

        violating_transaction_count += 1;
        if violation_samples.len() < CALL_FRAME_VIOLATION_SAMPLE_LIMIT {
            violation_samples.push((position, transaction.hash.clone(), violation));
        }
    }

    if violating_transaction_count > 0 {
        let omitted_violation_count = violating_transaction_count - violation_samples.len();
        warn!(
            block_number = block.number,
            block_hash = %block.hash,
            call_frame_validation = mode.as_str(),
            violating_transaction_count,
            sampled_violation_count = violation_samples.len(),
            omitted_violation_count,
            "debug call frame consistency violations observed; block accepted"
        );
        for (transaction_index, transaction_hash, violation) in violation_samples {
            warn!(
                block_number = block.number,
                block_hash = %block.hash,
                call_frame_validation = mode.as_str(),
                transaction_index,
                transaction_hash = %transaction_hash,
                violation = %violation,
                "debug call frame consistency violation sample"
            );
        }
    }

    Ok(frames)
}

/// A result labelled with another transaction belongs to another block,
/// whatever its length says.
fn check_frame_label(tx_hash: Option<&str>, position: usize, block: &RawRpcBlock) -> Component<()> {
    let Some(labelled) = tx_hash else {
        return Ok(());
    };
    let expected = block
        .block
        .transactions
        .get(position)
        .map(|tx| tx.hash.as_str())
        .unwrap_or_default();
    if labelled.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(format!(
            "block {}: trace result at position {position} is labelled with transaction \
             {labelled}, expected {expected}",
            block.number
        ))
    }
}

/// Length disagreement is recoverable only by label. Foreign entries are
/// ignored; a transaction left without one is incoherence — except on
/// polygon-based chains, which trace fewer than they list (REQ-15 quirk).
fn match_by_tx_hash<T: serde::de::DeserializeOwned>(
    arr: Vec<Value>,
    block: &RawRpcBlock,
    what: &str,
    tolerate_gaps: bool,
) -> Component<Vec<Option<T>>> {
    let index: std::collections::HashMap<&str, usize> = block
        .block
        .transactions
        .iter()
        .enumerate()
        .map(|(i, tx)| (tx.hash.as_str(), i))
        .collect();

    let mut mapped: Vec<Option<T>> = Vec::new();
    mapped.resize_with(block.block.transactions.len(), || None);

    for item in arr {
        let position = item
            .get("txHash")
            .and_then(|v| v.as_str())
            .and_then(|hash| index.get(hash).copied());
        if let Some(position) = position {
            mapped[position] = Some(parse_entry(item, what, block)?);
        }
    }

    if !tolerate_gaps {
        if let Some(missing) = mapped.iter().position(|e| e.is_none()) {
            return Err(format!(
                "block {}: no {what} for transaction {missing} of {}",
                block.number,
                mapped.len()
            ));
        }
    }

    Ok(mapped)
}

/// Exactly one replay per transaction, each carrying the tracers asked for. A
/// replay reaches the payload only through its hash, so an unattributable entry
/// is a missing component and a repeated one doubles its records.
fn replays_of(
    result: std::result::Result<Value, RpcError>,
    block: &RawRpcBlock,
    tracers: &[&str],
    what: &str,
) -> Component<Vec<TraceTransactionReplay>> {
    let number = block.number;
    let arr = unwrap_component(result, what, block)?;
    let mut replays: Vec<TraceTransactionReplay> = arr
        .into_iter()
        .map(|item| parse_entry(item, "trace replay", block))
        .collect::<Component<_>>()?;

    for rep in replays.iter_mut() {
        if rep.transaction_hash.is_none() {
            rep.transaction_hash = rep
                .trace
                .iter()
                .flatten()
                .find_map(|frame| frame.transaction_hash.clone());
        }
    }

    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for rep in &replays {
        let Some(hash) = rep.transaction_hash.as_deref() else {
            return Err(format!(
                "block {number}: a {what} entry names no transaction"
            ));
        };
        // Frames are attributed by the entry's hash, so one naming a different
        // transaction gets filed under this one. Frames carrying no hash of
        // their own are the norm — real captures label only the entry.
        if let Some(foreign) = rep.trace.iter().flatten().find_map(|frame| {
            frame
                .transaction_hash
                .as_deref()
                .filter(|h| !h.eq_ignore_ascii_case(hash))
        }) {
            return Err(format!(
                "block {number}: the {what} of transaction {hash} carries a frame of \
                 transaction {foreign}"
            ));
        }
        // Every transaction executes at least its root frame, so an empty list
        // is as incomplete as an absent one. State diffs are only checked for
        // presence: a system transaction can legitimately change nothing.
        let traced = rep
            .trace
            .as_deref()
            .is_some_and(|frames| !frames.is_empty());
        if tracers.contains(&"trace") && !traced {
            return Err(format!(
                "block {number}: the {what} of transaction {hash} carries no trace"
            ));
        }
        if tracers.contains(&"stateDiff") && rep.state_diff.is_none() {
            return Err(format!(
                "block {number}: the {what} of transaction {hash} carries no state diff"
            ));
        }
        *counts.entry(hash).or_default() += 1;
    }

    for tx in &block.block.transactions {
        match counts.remove(tx.hash.as_str()) {
            None => {
                return Err(format!(
                    "block {number}: no {what} for transaction {}",
                    tx.hash
                ))
            }
            Some(1) => {}
            Some(n) => {
                return Err(format!(
                    "block {number}: {n} {what} for transaction {}",
                    tx.hash
                ))
            }
        }
    }
    if let Some(foreign) = counts.keys().next() {
        return Err(format!(
            "block {number}: {what} name transaction {foreign}, which is not in this block"
        ));
    }

    Ok(replays)
}

/// A flat frame list over the whole block, reward frames included. Because the
/// request is addressed by number, a reorg may occur between fetching the
/// header and fetching its traces. Every frame must therefore carry the fetched
/// block's hash, and every fetched transaction must appear.
fn trace_block_replays(
    result: std::result::Result<Value, RpcError>,
    block: &RawRpcBlock,
) -> Component<Vec<TraceTransactionReplay>> {
    let arr = unwrap_component(result, "block traces", block)?;
    let frames: Vec<crate::rpc_data::TraceFrame> = arr
        .into_iter()
        .map(|item| parse_entry(item, "trace frame", block))
        .collect::<Component<_>>()?;

    if let Some(foreign) = frames.iter().find(|frame| {
        frame
            .block_hash
            .as_deref()
            .is_none_or(|hash| !hash.eq_ignore_ascii_case(&block.hash))
    }) {
        return Err(format!(
            "block {}: trace_block answered with a frame of block {:?}",
            block.number, foreign.block_hash
        ));
    }

    let mut position_by_tx = std::collections::HashMap::<String, usize>::new();
    let mut groups = Vec::<(String, Vec<crate::rpc_data::TraceFrame>)>::new();
    for frame in frames {
        if let Some(tx_hash) = frame.transaction_hash.clone() {
            if let Some(&position) = position_by_tx.get(&tx_hash) {
                groups[position].1.push(frame);
            } else {
                position_by_tx.insert(tx_hash.clone(), groups.len());
                groups.push((tx_hash, vec![frame]));
            }
        }
    }

    // A reward-only response is incomplete regardless of request form. Never
    // turn it into a trace-less block that appears coherent.
    for tx in &block.block.transactions {
        if !position_by_tx.contains_key(&tx.hash) {
            return Err(format!(
                "block {}: trace_block returned no traces for transaction {}",
                block.number, tx.hash
            ));
        }
    }

    // Match the predecessor's Map semantics: lookup is hashed, emission keeps
    // the first-seen frame order and is stable across processes.
    Ok(groups
        .into_iter()
        .map(|(tx_hash, frames)| TraceTransactionReplay {
            transaction_hash: Some(tx_hash),
            trace: Some(frames),
            state_diff: None,
            output: None,
        })
        .collect())
}

/// Check if a logsBloom string is all zeros (no logs).
/// logsBloom is a 256-byte (512 hex char) field.
pub fn is_zero_bloom(bloom: &str) -> bool {
    let s = bloom.strip_prefix("0x").unwrap_or(bloom);
    s.chars().all(|c| c == '0')
}

fn fix_log_indexes(logs: &mut Vec<RpcLog>) {
    for (i, log) in logs.iter_mut().enumerate() {
        log.log_index = to_qty(i as u64);
    }
}

/// The same renumbering across a block's receipts.
fn renumber_logs(receipts: &mut [RpcReceipt]) {
    let mut index = 0u64;
    for receipt in receipts.iter_mut() {
        for log in receipt.logs.iter_mut() {
            log.log_index = to_qty(index);
            index += 1;
        }
    }
}

fn parse_qty_u128(s: &str, field: &str) -> Component<u128> {
    let digits = s.strip_prefix("0x").unwrap_or(s);
    if digits.is_empty() {
        return Err(format!("{field} is an empty hexadecimal quantity"));
    }
    u128::from_str_radix(digits, 16)
        .map_err(|error| format!("{field} is not a valid u128 hexadecimal quantity: {error}"))
}
