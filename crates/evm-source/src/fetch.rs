//! RPC fetch layer — ports evm-rpc/src/rpc.ts (minus Cronos phantom-tx).
#![allow(clippy::ptr_arg)]
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use rpc_client::{CallOptions, RpcClient, RpcError, RpcErrorInfo};

use serde_json::{json, Value};
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use crate::chain_utils::ChainUtils;
use crate::rpc_data::{
    DebugFrameResult, DebugStateDiffResult, RawRpcBlock, RpcBlock, RpcLog, RpcReceipt,
    TraceTransactionReplay,
};
use crate::types::{qty2_u64, to_qty};

/// Options for the Rpc fetch layer.
#[derive(Debug, Clone, Default)]
pub struct RpcOptions {
    pub finality_confirmation: Option<u64>,
    pub verify_block_hash: bool,
    pub verify_tx_sender: bool,
    pub verify_tx_root: bool,
    pub verify_receipts_root: bool,
    pub verify_withdrawals_root: bool,
    pub verify_logs_bloom: bool,
    pub check_log_index: bool,
    pub check_cumulative_gas_used: bool,
    pub use_gas_used_for_receipts_root: bool,
}

/// Fetch state for the Rpc layer.
pub struct Rpc {
    pub client: Arc<RpcClient>,
    pub options: RpcOptions,
    chain_utils: OnceCell<ChainUtils>,
    receipts_method: OnceCell<ReceiptsMethod>,
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

        Ok((qty2_u64(number_str), hash))
    }

    pub async fn get_height(&self) -> Result<u64> {
        let height: Value = self
            .client
            .call("eth_blockNumber", None, CallOptions::default())
            .await
            .map_err(|e| anyhow!("eth_blockNumber: {e}"))?;
        Ok(qty2_u64(height.as_str().unwrap_or("0x0")))
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
        let (finalized_num, _) = self.get_latest_blockhash("finalized").await?;
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
                        if prev_hash != &b.block.parent_hash {
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
        const MAX_RETRIES: u32 = 10;
        const DELAY_MS: u64 = 50;

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

            if retries >= MAX_RETRIES {
                bail!("failed to enrich block {number} after {MAX_RETRIES} retries: {err_msg}");
            }
            retries += 1;

            debug!(
                block = number,
                attempt = retries,
                max_retries = MAX_RETRIES,
                reason = %err_msg,
                "block enrichment not ready, retrying whole-block fetch"
            );

            tokio::time::sleep(std::time::Duration::from_millis(DELAY_MS)).await;

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

                    // Verify block hash
                    if self.options.verify_block_hash {
                        let computed = if utils.is_tempo {
                            crate::verification::tempo_block_hash(&raw.block)
                        } else {
                            crate::verification::block_hash(&raw.block)
                        };
                        match computed {
                            Ok(h) if h == hash => {}
                            Ok(h) => {
                                raw.mark_invalid(format!(
                                    "block hash mismatch: expected {hash} got {h}"
                                ));
                            }
                            Err(e) => {
                                warn!("block hash verification error: {e}");
                            }
                        }
                    }

                    blocks.push(Some(raw));
                }
            }
        }

        Ok(blocks)
    }

    async fn add_requested_data(
        &self,
        blocks: &mut Vec<RawRpcBlock>,
        req: &crate::types::DataRequest,
    ) -> Result<()> {
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

            if utils.is_stable {
                fix_log_indexes(&mut block_logs);
            }

            if self.options.check_log_index {
                for (i, log) in block_logs.iter().enumerate() {
                    let actual = qty2_u64(&log.log_index);
                    if actual != i as u64 {
                        bail!(
                            "log index check failed at block {}: expected {i} got {actual}",
                            block.number
                        );
                    }
                }
            }

            if self.options.verify_logs_bloom {
                let log_refs: Vec<&RpcLog> = block_logs.iter().collect();
                if let Err(e) = crate::verification::verify_logs_bloom(&block.block, &log_refs) {
                    bail!("block {}: {e}", block.number);
                }
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
                    if let Some(bad) = receipts.iter().find(|r| r.block_hash != block.hash) {
                        let msg = format!(
                            "eth_getBlockReceipts returned receipts for a different block \
                             (header {}, receipt block_hash {}) — reorg / load-balanced \
                             inconsistency, will retry",
                            block.hash, bad.block_hash
                        );
                        block.mark_invalid(msg);
                        continue;
                    }

                    // Collect logs
                    let mut logs: Vec<&RpcLog> = Vec::new();
                    for receipt in &receipts {
                        for log in &receipt.logs {
                            logs.push(log);
                        }
                    }

                    if utils.is_stable {
                        let mut all_logs: Vec<RpcLog> =
                            receipts.iter().flat_map(|r| r.logs.clone()).collect();
                        fix_log_indexes(&mut all_logs);
                        let mut idx = 0;
                        for receipt in receipts.iter_mut() {
                            for log in receipt.logs.iter_mut() {
                                log.log_index = to_qty(idx as u64);
                                idx += 1;
                            }
                        }
                    }

                    let all_logs: Vec<RpcLog> =
                        receipts.iter().flat_map(|r| r.logs.clone()).collect();

                    if self.options.check_log_index {
                        for (j, log) in all_logs.iter().enumerate() {
                            let actual = qty2_u64(&log.log_index);
                            if actual != j as u64 {
                                bail!("log index check failed at block {}", block.number);
                            }
                        }
                    }

                    if self.options.check_cumulative_gas_used {
                        let mut prev = 0u128;
                        for receipt in &receipts {
                            let cumulative = parse_qty_u128(&receipt.cumulative_gas_used);
                            let used = parse_qty_u128(&receipt.gas_used);
                            if cumulative != prev + used {
                                bail!("cumulative gas used check failed at block {}", block.number);
                            }
                            prev = cumulative;
                        }
                    }

                    if self.options.verify_logs_bloom {
                        let log_refs: Vec<&RpcLog> = all_logs.iter().collect();
                        crate::verification::verify_logs_bloom(&block.block, &log_refs)
                            .map_err(|e| anyhow!("block {}: {e}", block.number))?;
                    }

                    if self.options.verify_receipts_root {
                        let receipt_refs: Vec<&RpcReceipt> = receipts.iter().collect();
                        let computed = crate::verification::receipts_root(
                            &receipt_refs,
                            self.options.use_gas_used_for_receipts_root,
                        )
                        .map_err(|e| anyhow!("block {}: {e}", block.number))?;
                        if computed != block.block.receipts_root {
                            bail!("block {}: receipts root mismatch", block.number);
                        }
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
            if let Some(bad) = receipts.iter().find(|r| r.block_hash != block.hash) {
                let msg = format!(
                    "eth_getTransactionReceipt returned receipts for a different block \
                     (header {}, receipt block_hash {}) — reorg / load-balanced \
                     inconsistency, will retry",
                    block.hash, bad.block_hash
                );
                block.mark_invalid(msg);
                continue;
            }

            let all_logs: Vec<RpcLog> = receipts.iter().flat_map(|r| r.logs.clone()).collect();

            if utils.is_stable {
                // fix log indexes
                let mut idx = 0;
                for receipt in receipts.iter_mut() {
                    for log in receipt.logs.iter_mut() {
                        log.log_index = to_qty(idx as u64);
                        idx += 1;
                    }
                }
            }

            if self.options.check_log_index {
                for (j, log) in all_logs.iter().enumerate() {
                    let actual = qty2_u64(&log.log_index);
                    if actual != j as u64 {
                        bail!("log index check failed at block {}", block.number);
                    }
                }
            }

            if self.options.check_cumulative_gas_used {
                let mut prev = 0u128;
                for receipt in &receipts {
                    let cumulative = parse_qty_u128(&receipt.cumulative_gas_used);
                    let used = parse_qty_u128(&receipt.gas_used);
                    if cumulative != prev + used {
                        bail!("cumulative gas used check failed at block {}", block.number);
                    }
                    prev = cumulative;
                }
            }

            if self.options.verify_logs_bloom {
                let log_refs: Vec<&RpcLog> = all_logs.iter().collect();
                crate::verification::verify_logs_bloom(&block.block, &log_refs)
                    .map_err(|e| anyhow!("block {}: {e}", block.block.number))?;
            }

            if self.options.verify_receipts_root {
                let receipt_refs: Vec<&RpcReceipt> = receipts.iter().collect();
                let computed = crate::verification::receipts_root(
                    &receipt_refs,
                    self.options.use_gas_used_for_receipts_root,
                )?;
                if computed != block.block.receipts_root {
                    bail!("block {}: receipts root mismatch", block.number);
                }
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

        // Determine what replay tracers we need. Request the `trace` tracer
        // whenever traces are wanted via the trace API — even if statediffs are
        // also requested (both come from one trace_replayBlockTransactions call).
        // The previous `&& !req.state_diffs` dropped traces on use_trace_api
        // chains that also fetch statediffs (e.g. gnosis).
        let need_replay_trace = req.traces && req.use_trace_api;
        let need_replay_statediff = req.state_diffs && !req.use_debug_api_for_state_diffs;
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
                blocks[*i].debug_frames = Some(frames);
            }
        }
        if let Some(debug_diffs) = debug_diffs_opt {
            for (i, diffs) in traceable.iter().zip(debug_diffs.into_iter()) {
                blocks[*i].debug_state_diffs = Some(diffs);
            }
        }
        if let Some(replays) = trace_replay_opt {
            for (i, replay) in traceable.iter().zip(replays.into_iter()) {
                blocks[*i].trace_replays = Some(replay);
            }
        }
        if let Some(replays) = trace_block_opt {
            for (i, replay) in traceable.iter().zip(replays.into_iter()) {
                blocks[*i].trace_replays = Some(replay);
            }
        }

        // Completeness check for the debug-API paths (mirrors add_logs/add_receipts).
        // `debug_traceBlockByHash` (callTracer / prestateTracer) yields exactly one
        // entry per transaction, so a block that has transactions but came back with
        // no trace/stateDiff entries was NOT served correctly: fetch_debug_frames /
        // fetch_debug_state_diffs collapse any RPC error or a `null` result (including
        // the "not found" / "cannot query unfinalized data" responses they normalize
        // to Null) into an empty `vec![]`. Without this check that empty vector is
        // stored — and cached — as if the block legitimately had no traces, silently
        // serving finalized blocks with empty traces/stateDiffs. Mark such blocks
        // invalid so enrich_block_with_retry re-fetches the whole block instead of
        // accepting the gap. A block with zero transactions correctly has no entries
        // and is left untouched.
        for &i in &traceable {
            let block = &mut blocks[i];
            if block.is_invalid || block.block.transactions.is_empty() {
                continue;
            }
            if req.traces && !req.use_trace_api {
                let n_frames = block.debug_frames.as_ref().map_or(0, |f| f.len());
                if n_frames == 0 {
                    block.mark_invalid(
                        "debug_traceBlockByHash returned no traces for a block with transactions (not ready)",
                    );
                    continue;
                }
            }
            if req.state_diffs && req.use_debug_api_for_state_diffs {
                let n_diffs = block.debug_state_diffs.as_ref().map_or(0, |d| d.len());
                if n_diffs == 0 {
                    block.mark_invalid(
                        "debug_traceBlockByHash(prestateTracer) returned no stateDiffs for a block with transactions (not ready)",
                    );
                }
            }
        }

        Ok(())
    }

    async fn fetch_debug_frames(
        &self,
        blocks: &[&RawRpcBlock],
        req: &crate::types::DataRequest,
    ) -> Result<Vec<Vec<Option<DebugFrameResult>>>> {
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
            match result {
                Err(_) | Ok(Value::Null) => {
                    out.push(vec![]);
                }
                Ok(v) => {
                    // Moonbeam quirk: may return frames without the `result` wrapper
                    let arr = match v {
                        Value::Array(mut arr) => {
                            for item in arr.iter_mut() {
                                if item.is_object() && item.get("result").is_none() {
                                    let inner = item.take();
                                    *item = json!({"result": inner});
                                }
                            }
                            arr
                        }
                        _ => vec![],
                    };

                    let mut frames: Vec<Option<DebugFrameResult>> = Vec::new();
                    if block.block.transactions.len() == arr.len() {
                        for item in arr {
                            frames.push(serde_json::from_value(item).ok());
                        }
                    } else {
                        // Match by txHash
                        let tx_hash_to_idx: std::collections::HashMap<String, usize> = block
                            .block
                            .transactions
                            .iter()
                            .enumerate()
                            .map(|(i, tx)| (tx.hash.clone(), i))
                            .collect();

                        let mut mapped: Vec<Option<DebugFrameResult>> =
                            vec![None; block.block.transactions.len()];
                        for item in arr {
                            let tx_hash = item
                                .get("txHash")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                            if let Some(hash) = tx_hash {
                                if let Some(&idx) = tx_hash_to_idx.get(&hash) {
                                    mapped[idx] = serde_json::from_value(item).ok();
                                }
                            }
                        }
                        frames = mapped;
                    }
                    out.push(frames);
                }
            }
        }

        Ok(out)
    }

    async fn fetch_debug_state_diffs(
        &self,
        blocks: &[&RawRpcBlock],
        req: &crate::types::DataRequest,
    ) -> Result<Vec<Vec<Option<DebugStateDiffResult>>>> {
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

        let mut out = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            let block = blocks[i];
            match result {
                Err(_) | Ok(Value::Null) => {
                    out.push(vec![]);
                }
                Ok(v) => {
                    let arr = v.as_array().cloned().unwrap_or_default();
                    if block.block.transactions.len() == arr.len() {
                        let frames: Vec<Option<DebugStateDiffResult>> = arr
                            .into_iter()
                            .map(|item| serde_json::from_value(item).ok())
                            .collect();
                        out.push(frames);
                    } else {
                        // Match by txHash
                        let tx_hash_to_idx: std::collections::HashMap<String, usize> = block
                            .block
                            .transactions
                            .iter()
                            .enumerate()
                            .map(|(i, tx)| (tx.hash.clone(), i))
                            .collect();

                        let mut mapped: Vec<Option<DebugStateDiffResult>> =
                            vec![None; block.block.transactions.len()];
                        for item in arr {
                            let tx_hash = item
                                .get("txHash")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                            if let Some(hash) = tx_hash {
                                if let Some(&idx) = tx_hash_to_idx.get(&hash) {
                                    mapped[idx] = serde_json::from_value(item).ok();
                                }
                            }
                        }
                        out.push(mapped);
                    }
                }
            }
        }

        Ok(out)
    }

    async fn fetch_trace_replays(
        &self,
        blocks: &[&RawRpcBlock],
        tracers: &[&str],
    ) -> Result<Vec<Vec<TraceTransactionReplay>>> {
        let tracers_json: Vec<Value> = tracers.iter().map(|&t| json!(t)).collect();

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
            match result {
                Err(_) => out.push(vec![]),
                Ok(v) => {
                    let mut replays: Vec<TraceTransactionReplay> =
                        serde_json::from_value(v).unwrap_or_default();

                    // Resolve transactionHash from trace frames if not set
                    for rep in replays.iter_mut() {
                        if rep.transaction_hash.is_none() {
                            if let Some(frames) = &rep.trace {
                                for frame in frames {
                                    if let Some(h) = &frame.transaction_hash {
                                        rep.transaction_hash = Some(h.clone());
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    // Validate all replays belong to this block
                    let tx_set: std::collections::HashSet<String> = block
                        .block
                        .transactions
                        .iter()
                        .map(|tx| tx.hash.clone())
                        .collect();

                    let mut invalid = false;
                    for rep in &replays {
                        if let Some(h) = &rep.transaction_hash {
                            if !tx_set.contains(h) {
                                invalid = true;
                                break;
                            }
                        }
                    }

                    if invalid {
                        out.push(vec![]);
                    } else {
                        out.push(replays);
                    }
                }
            }
        }

        Ok(out)
    }

    async fn fetch_trace_block(
        &self,
        blocks: &[&RawRpcBlock],
    ) -> Result<Vec<Vec<TraceTransactionReplay>>> {
        let calls: Vec<(String, Option<Value>)> = blocks
            .iter()
            .map(|b| ("trace_block".to_string(), Some(json!([b.hash]))))
            .collect();

        let results = self
            .client
            .batch_call_reduce_on_retry(calls, &CallOptions::default())
            .await?;

        let mut out = Vec::with_capacity(results.len());

        for (i, result) in results.into_iter().enumerate() {
            let block = blocks[i];
            match result {
                Err(_) => out.push(vec![]),
                Ok(v) => {
                    let frames: Vec<crate::rpc_data::TraceFrame> =
                        serde_json::from_value(v).unwrap_or_default();

                    if frames.is_empty() {
                        if !block.block.transactions.is_empty() {
                            // mark invalid? Not for trace_block at this point
                        }
                        out.push(vec![]);
                        continue;
                    }

                    // Check all frames belong to this block
                    let all_match = frames
                        .iter()
                        .all(|f| f.block_hash.as_deref() == Some(&block.hash));

                    if !all_match {
                        out.push(vec![]);
                        continue;
                    }

                    // Group by transactionHash → TraceTransactionReplay
                    let mut by_tx: std::collections::HashMap<
                        String,
                        Vec<crate::rpc_data::TraceFrame>,
                    > = std::collections::HashMap::new();
                    for frame in frames {
                        if let Some(hash) = &frame.transaction_hash {
                            by_tx.entry(hash.clone()).or_default().push(frame);
                        }
                    }

                    let replays: Vec<TraceTransactionReplay> = by_tx
                        .into_iter()
                        .map(|(tx_hash, frames)| TraceTransactionReplay {
                            transaction_hash: Some(tx_hash),
                            trace: Some(frames),
                            state_diff: None,
                            output: None,
                        })
                        .collect();

                    out.push(replays);
                }
            }
        }

        Ok(out)
    }
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

fn parse_qty_u128(s: &str) -> u128 {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(s, 16).unwrap_or(0)
}
