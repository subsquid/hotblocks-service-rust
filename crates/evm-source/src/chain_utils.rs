//! Per-network deviations from the chain-family baseline (REQ-15), and the
//! verification checks (REQ-14) that must honour them.
//!
//! A network is described by what it deviates in, not by its name: adding one
//! is a registry row, and each check asks for the deviation it cares about
//! instead of testing chain ids. Without the exemptions an enabled check
//! reports a forgery on every block of those networks (GAP-9).

use std::collections::HashSet;

use anyhow::Result;

use crate::rpc_data::{RpcBlock, RpcLog, RpcReceipt, RpcTransaction, RpcWithdrawal};
use crate::verification;

/// A way a network departs from the baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quirk {
    /// Bor synthesises a state-sync transaction; no header commitment covers it.
    StateSyncTx,
    /// System transactions, recognised by a zero gas price and their receipts by
    /// zero cumulative gas, sit outside the header's commitments and carry no
    /// recoverable sender.
    ZeroGasSystemTx,
    /// System transactions carry a fake (r=0, s=0) signature.
    FakeSignatureSystemTx,
    /// The same, but only on legacy transactions.
    LegacyFakeSignatureSystemTx,
    /// The header extends the Ethereum field set and hashes by that network's
    /// own encoding.
    TempoHeader,
    /// Log indexes are not block-sequential and are renumbered on arrival.
    NonSequentialLogIndexes,
    /// Debug traces may leave transactions uncovered (IB-16).
    PartialTraceCoverage,
}

/// One network's deviations.
struct Network {
    chain_id: u64,
    quirks: &'static [Quirk],
    /// Transaction types this network emits that no encoder here expresses, so
    /// a block carrying one cannot be root-verified either way. Scoped per
    /// network on purpose: elsewhere such a type is a malformed answer, and
    /// letting it pass would let an upstream switch verification off by
    /// choosing its input.
    unencodable_txs: &'static [&'static str],
}

const BOR: &[Quirk] = &[Quirk::StateSyncTx, Quirk::PartialTraceCoverage];
const HYPERLIQUID: &[Quirk] = &[Quirk::ZeroGasSystemTx];
const STABLE: &[Quirk] = &[Quirk::FakeSignatureSystemTx, Quirk::NonSequentialLogIndexes];
const TEMPO: &[Quirk] = &[Quirk::TempoHeader, Quirk::LegacyFakeSignatureSystemTx];

/// PIP-74 needs data no RPC method returns.
const PIP74: &[&str] = &["0x7f"];
/// Arbitrum's retryable types; encoders not ported (GAP-16).
const ARBITRUM_RETRYABLES: &[&str] = &["0x66", "0x68", "0x69"];

/// Every network known to deviate. Absent ones are treated as baseline; those
/// the predecessor supports and this table still omits are GAP-16.
const NETWORKS: &[Network] = &[
    // Bor-compatible
    net(137, BOR, PIP74),    // Polygon mainnet
    net(80_002, BOR, PIP74), // Polygon Amoy
    net(109, BOR, PIP74),    // Shibarium mainnet
    // Hyperliquid
    net(999, HYPERLIQUID, &[]), // Mainnet
    net(998, HYPERLIQUID, &[]), // Testnet
    // Stable
    net(988, STABLE, &[]),   // Mainnet
    net(2_201, STABLE, &[]), // Testnet
    // Tempo
    net(4_217, TEMPO, &[]),  // Mainnet
    net(42_431, TEMPO, &[]), // Moderato
    net(42_429, TEMPO, &[]), // Andantino
    // Arbitrum
    net(42_161, &[], ARBITRUM_RETRYABLES), // One
];

const fn net(
    chain_id: u64,
    quirks: &'static [Quirk],
    unencodable_txs: &'static [&'static str],
) -> Network {
    Network {
        chain_id,
        quirks,
        unencodable_txs,
    }
}

const ZERO: &str = "0x0";
const LEGACY_TX_TYPE: &str = "0x0";

#[derive(Debug, Clone)]
pub struct ChainUtils {
    pub chain_id: u64,
    quirks: &'static [Quirk],
    unencodable_txs: &'static [&'static str],
    use_gas_used_for_receipts_root: bool,
}

impl ChainUtils {
    pub fn new(chain_id: u64, use_gas_used_for_receipts_root: bool) -> Self {
        let network = NETWORKS.iter().find(|n| n.chain_id == chain_id);

        ChainUtils {
            chain_id,
            quirks: network.map_or(&[], |n| n.quirks),
            unencodable_txs: network.map_or(&[], |n| n.unencodable_txs),
            use_gas_used_for_receipts_root,
        }
    }

    pub fn has(&self, quirk: Quirk) -> bool {
        self.quirks.contains(&quirk)
    }

    pub fn calculate_block_hash(&self, block: &RpcBlock) -> Result<String> {
        if self.has(Quirk::TempoHeader) {
            verification::tempo_block_hash(block)
        } else {
            verification::block_hash(block)
        }
    }

    /// `None` when the block carries a transaction this build cannot express,
    /// so the claim cannot be checked either way.
    pub fn calculate_transactions_root(&self, block: &RpcBlock) -> Result<Option<String>> {
        if block.transactions.iter().any(|tx| self.is_unencodable(tx)) {
            return Ok(None);
        }

        let phantom = self.phantom_tx_hash(block);
        let txs: Vec<&RpcTransaction> = block
            .transactions
            .iter()
            .filter(|tx| !self.is_uncommitted_tx(tx, phantom.as_deref()))
            .collect();

        verification::transactions_root(&txs).map(Some)
    }

    pub fn calculate_logs_bloom(&self, block: &RpcBlock, logs: &[&RpcLog]) -> String {
        let excluded = self.uncommitted_tx_hashes(block);
        if excluded.is_empty() {
            return verification::logs_bloom(logs);
        }

        let logs: Vec<&RpcLog> = logs
            .iter()
            .copied()
            .filter(|log| !excluded.contains(log.transaction_hash.as_str()))
            .collect();
        verification::logs_bloom(&logs)
    }

    pub fn calculate_receipts_root(
        &self,
        block: &RpcBlock,
        receipts: &[&RpcReceipt],
    ) -> Result<String> {
        let receipts: Vec<&RpcReceipt> = self
            .committed_receipts(block, receipts.iter().copied())
            .collect();

        verification::receipts_root(&receipts, self.use_gas_used_for_receipts_root)
    }

    /// Receipts covered by the block's cumulative-gas sequence and receipt
    /// trie. Every receipt-level coherence check must use this same view.
    pub(crate) fn committed_receipts<'a>(
        &'a self,
        block: &RpcBlock,
        receipts: impl IntoIterator<Item = &'a RpcReceipt> + 'a,
    ) -> impl Iterator<Item = &'a RpcReceipt> + 'a {
        let phantom = self.phantom_tx_hash(block);
        receipts
            .into_iter()
            .filter(move |receipt| !self.is_uncommitted_receipt(receipt, phantom.as_deref()))
    }

    pub fn calculate_withdrawals_root(&self, withdrawals: &[&RpcWithdrawal]) -> Result<String> {
        verification::withdrawals_root(withdrawals)
    }

    /// `None` for a transaction whose signature no recovery answers.
    pub fn recover_tx_sender(&self, tx: &RpcTransaction) -> Result<Option<String>> {
        if self.is_unsigned(tx) {
            return Ok(None);
        }
        verification::recover_tx_sender(tx)
    }

    fn is_unencodable(&self, tx: &RpcTransaction) -> bool {
        tx.tx_type
            .as_deref()
            .is_some_and(|ty| self.unencodable_txs.contains(&ty))
    }

    /// The transaction the node synthesised for this block, if the network does
    /// that. Computed once per block, not per transaction.
    fn phantom_tx_hash(&self, block: &RpcBlock) -> Option<String> {
        self.has(Quirk::StateSyncTx)
            .then(|| verification::calculate_state_sync_tx_hash(&block.number, &block.hash))
    }

    /// A transaction the header's commitments leave out.
    fn is_uncommitted_tx(&self, tx: &RpcTransaction, phantom: Option<&str>) -> bool {
        phantom == Some(tx.hash.as_str())
            || (self.has(Quirk::ZeroGasSystemTx) && tx.gas_price.as_deref() == Some(ZERO))
    }

    fn is_uncommitted_receipt(&self, receipt: &RpcReceipt, phantom: Option<&str>) -> bool {
        phantom == Some(receipt.transaction_hash.as_str())
            || (self.has(Quirk::ZeroGasSystemTx) && receipt.cumulative_gas_used == ZERO)
    }

    fn uncommitted_tx_hashes<'a>(&self, block: &'a RpcBlock) -> HashSet<&'a str> {
        let phantom = self.phantom_tx_hash(block);
        if phantom.is_none() && !self.has(Quirk::ZeroGasSystemTx) {
            return HashSet::new();
        }
        block
            .transactions
            .iter()
            .filter(|tx| self.is_uncommitted_tx(tx, phantom.as_deref()))
            .map(|tx| tx.hash.as_str())
            .collect()
    }

    /// A signature no recovery answers: the system transactions the node mints
    /// itself.
    fn is_unsigned(&self, tx: &RpcTransaction) -> bool {
        let fake_signature = tx.r.as_deref() == Some(ZERO) && tx.s.as_deref() == Some(ZERO);

        (self.has(Quirk::ZeroGasSystemTx) && tx.gas_price.as_deref() == Some(ZERO))
            || (self.has(Quirk::FakeSignatureSystemTx) && fake_signature)
            || (self.has(Quirk::LegacyFakeSignatureSystemTx)
                && fake_signature
                && tx.tx_type.as_deref() == Some(LEGACY_TX_TYPE))
    }
}
