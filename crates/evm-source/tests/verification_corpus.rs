//! REQ-14 — the verification checks over the predecessor's recorded corpus,
//! through the chain-aware layer the fetch path uses.
//!
//! Two obligations per switch: an honest block passes on every supported
//! network, including those whose system transactions the baseline exempts
//! (GAP-9), and a block with one forged field fails (GAP-8).

use evm_source::chain_utils::ChainUtils;
use evm_source::rpc_data::{RpcBlock, RpcLog, RpcReceipt, RpcTransaction, RpcWithdrawal};
use serde_json::Value;

// ─── Corpus ───────────────────────────────────────────────────────────────────

struct Case {
    chain: &'static str,
    number: u64,
    chain_id: u64,
    /// Cosmos/Tendermint-derived chains hash headers with a different tree.
    verifiable_header: bool,
    receipts: bool,
}

const CORPUS: &[Case] = &[
    // Post-Shanghai: withdrawals, type-2 fees, 157 transactions with receipts.
    Case {
        chain: "ethereum",
        number: 18500000,
        chain_id: 0x1,
        verifiable_header: true,
        receipts: true,
    },
    // Last block before Byzantium: receipts carry a state root, not a status,
    // so they stay out of the receipt-side checks until GAP-14.
    Case {
        chain: "ethereum",
        number: 4369999,
        chain_id: 0x1,
        verifiable_header: true,
        receipts: false,
    },
    // Pre-EIP-155: recovery without a chain id in `v`.
    Case {
        chain: "ethereum",
        number: 1000000,
        chain_id: 0x1,
        verifiable_header: true,
        receipts: false,
    },
    // Arbitrum's own transaction types (0x6a) in the trie.
    Case {
        chain: "arbitrum",
        number: 150000000,
        chain_id: 0xa4b1,
        verifiable_header: true,
        receipts: true,
    },
    // Tempo: extended header, 0x76 transactions, a fake-signature system tx.
    Case {
        chain: "tempo-mainnet",
        number: 17708868,
        chain_id: 0x1079,
        verifiable_header: true,
        receipts: true,
    },
    // Stable: a system transaction signed r=0, s=0.
    Case {
        chain: "stable",
        number: 13683593,
        chain_id: 0x3dc,
        verifiable_header: false,
        receipts: false,
    },
    Case {
        chain: "hyperliquid",
        number: 50000,
        chain_id: 0x3e7,
        verifiable_header: true,
        receipts: true,
    },
];

fn fixture_path(chain: &str, number: u64, file: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/verification")
        .join(chain)
        .join(number.to_string())
        .join(file)
}

fn load_json(chain: &str, number: u64, file: &str) -> Value {
    let path = fixture_path(chain, number, file);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn block_of(value: &Value) -> RpcBlock {
    serde_json::from_value(value.clone()).expect("fixture block deserializes")
}

fn receipts_of(value: &Value) -> Vec<RpcReceipt> {
    serde_json::from_value(value.clone()).expect("fixture receipts deserialize")
}

fn load_block(case: &Case) -> RpcBlock {
    block_of(&load_json(case.chain, case.number, "block.json"))
}

fn load_receipts(case: &Case) -> Vec<RpcReceipt> {
    receipts_of(&load_json(case.chain, case.number, "receipts.json"))
}

fn utils(case: &Case) -> ChainUtils {
    ChainUtils::new(case.chain_id, false)
}

fn logs_of(receipts: &[RpcReceipt]) -> Vec<RpcLog> {
    receipts.iter().flat_map(|r| r.logs.clone()).collect()
}

fn label(case: &Case) -> String {
    format!("{} block {}", case.chain, case.number)
}

// ─── The honest half: every check passes on every supported network ───────────

#[test]
fn block_hash_verifies_across_the_corpus() {
    for case in CORPUS.iter().filter(|c| c.verifiable_header) {
        let block = load_block(case);
        let computed = utils(case)
            .calculate_block_hash(&block)
            .unwrap_or_else(|e| panic!("{}: {e}", label(case)));
        assert_eq!(computed, block.hash, "{}", label(case));
    }
}

#[test]
fn transactions_root_verifies_across_the_corpus() {
    let mut checked = 0;
    for case in CORPUS {
        let block = load_block(case);
        if block.transactions.is_empty() {
            continue;
        }
        // `None` — the block carries a type this build cannot encode (tempo).
        let Some(computed) = utils(case)
            .calculate_transactions_root(&block)
            .unwrap_or_else(|e| panic!("{}: {e}", label(case)))
        else {
            continue;
        };
        assert_eq!(computed, block.transactions_root, "{}", label(case));
        checked += 1;
    }
    assert!(checked >= CORPUS.len() - 1, "only tempo is unverifiable");
}

#[test]
fn receipts_root_verifies_across_the_corpus() {
    for case in CORPUS.iter().filter(|c| c.receipts) {
        let block = load_block(case);
        let receipts = load_receipts(case);
        let refs: Vec<&RpcReceipt> = receipts.iter().collect();
        let computed = utils(case)
            .calculate_receipts_root(&block, &refs)
            .unwrap_or_else(|e| panic!("{}: {e}", label(case)));
        assert_eq!(computed, block.receipts_root, "{}", label(case));
    }
}

#[test]
fn logs_bloom_verifies_across_the_corpus() {
    for case in CORPUS.iter().filter(|c| c.receipts) {
        let block = load_block(case);
        let logs = logs_of(&load_receipts(case));
        let refs: Vec<&RpcLog> = logs.iter().collect();
        let computed = utils(case).calculate_logs_bloom(&block, &refs);
        assert_eq!(computed, block.logs_bloom, "{}", label(case));
    }
}

#[test]
fn withdrawals_root_verifies_across_the_corpus() {
    let mut checked = 0;
    for case in CORPUS {
        let block = load_block(case);
        let (Some(claimed), Some(withdrawals)) = (
            block.withdrawals_root.as_deref(),
            block.withdrawals.as_ref(),
        ) else {
            continue;
        };
        let refs: Vec<&RpcWithdrawal> = withdrawals.iter().collect();
        let computed = utils(case)
            .calculate_withdrawals_root(&refs)
            .unwrap_or_else(|e| panic!("{}: {e}", label(case)));
        assert_eq!(computed, claimed, "{}", label(case));
        checked += 1;
    }
    assert!(checked > 0, "the corpus must carry a post-Shanghai block");
}

#[test]
fn sender_recovery_verifies_across_the_corpus() {
    for case in CORPUS {
        let block = load_block(case);
        let utils = utils(case);
        for tx in &block.transactions {
            let recovered = utils
                .recover_tx_sender(tx)
                .unwrap_or_else(|e| panic!("{} tx {}: {e}", label(case), tx.hash));
            let Some(sender) = recovered else { continue };
            assert_eq!(
                sender.to_lowercase(),
                tx.from.to_lowercase(),
                "{} tx {}",
                label(case),
                tx.hash
            );
        }
    }
}

// ─── The exemptions the baseline defines (GAP-9) ──────────────────────────────

/// A system transaction's fake signature recovers to nothing; without the
/// exemption the switch reports a forgery on every block of those networks.
#[test]
fn system_transactions_are_exempt_from_sender_recovery() {
    for (chain, number, chain_id) in [
        ("stable", 13683593u64, 0x3dcu64),
        ("tempo-mainnet", 17708868, 0x1079),
    ] {
        let block = block_of(&load_json(chain, number, "block.json"));
        let utils = ChainUtils::new(chain_id, false);
        let system: Vec<&RpcTransaction> = block
            .transactions
            .iter()
            .filter(|tx| tx.r.as_deref() == Some("0x0") && tx.s.as_deref() == Some("0x0"))
            .collect();
        assert!(
            !system.is_empty(),
            "{chain} block {number} must carry a system transaction"
        );
        for tx in system {
            assert_eq!(
                utils.recover_tx_sender(tx).expect("exempt, not an error"),
                None,
                "{chain} tx {}",
                tx.hash
            );
        }
    }
}

/// The node synthesises the state-sync transaction; it is not in the trie the
/// header commits to.
#[test]
fn polygon_state_sync_transaction_is_excluded_from_the_transactions_root() {
    let mut block = block_of(&load_json("ethereum", 4369999, "block.json"));
    let honest = block.transactions_root.clone();

    let state_sync_hash =
        evm_source::verification::calculate_state_sync_tx_hash(&block.number, &block.hash);
    let mut phantom = block.transactions[0].clone();
    phantom.hash = state_sync_hash;
    phantom.transaction_index = format!("0x{:x}", block.transactions.len());
    block.transactions.push(phantom);

    let polygon = ChainUtils::new(0x89, false);
    let computed = polygon
        .calculate_transactions_root(&block)
        .expect("root computes")
        .expect("polygon blocks without PIP-74 transactions are verifiable");
    assert_eq!(
        computed, honest,
        "the state-sync transaction must not enter the trie"
    );

    // On a chain without the quirk the same payload is a genuine divergence.
    let ethereum = ChainUtils::new(0x1, false);
    let computed = ethereum
        .calculate_transactions_root(&block)
        .expect("root computes")
        .expect("ethereum blocks are verifiable");
    assert_ne!(computed, honest, "the exemption must stay chain-scoped");
}

/// PIP-74 transactions cannot be re-encoded from an RPC response, so the block
/// is unverifiable — calling it forged would stall the chain.
#[test]
fn polygon_pip74_transaction_makes_the_block_unverifiable() {
    let mut block = block_of(&load_json("ethereum", 4369999, "block.json"));
    block.transactions[0].tx_type = Some("0x7f".to_string());

    let computed = ChainUtils::new(0x89, false)
        .calculate_transactions_root(&block)
        .expect("root computes");
    assert_eq!(
        computed, None,
        "a PIP-74 block is exempt, not a verification failure"
    );
}

/// Hyperliquid's system transactions (gas price 0) sit outside the header's
/// commitments.
#[test]
fn hyperliquid_system_transactions_are_exempt() {
    let mut block = block_of(&load_json("ethereum", 4369999, "block.json"));
    let honest_txs = block.transactions_root.clone();

    let mut system = block.transactions[0].clone();
    system.hash = format!("0x{:064x}", 0xfeedu64);
    system.gas_price = Some("0x0".to_string());
    system.transaction_index = format!("0x{:x}", block.transactions.len());
    block.transactions.push(system.clone());

    let utils = ChainUtils::new(0x3e7, false);
    assert_eq!(
        utils
            .calculate_transactions_root(&block)
            .expect("root computes")
            .expect("verifiable"),
        honest_txs,
        "a system transaction must not enter the transaction trie"
    );
    assert_eq!(
        utils.recover_tx_sender(&system).expect("exempt"),
        None,
        "a system transaction has no recoverable sender"
    );
}

/// Zero cumulative gas marks Hyperliquid's system receipt, which the receipt
/// trie does not contain.
#[test]
fn hyperliquid_system_receipts_are_excluded_from_the_receipts_root() {
    let case = &CORPUS[6];
    assert_eq!(case.chain, "hyperliquid");
    let block = load_block(case);
    let mut receipts = load_receipts(case);
    let honest = block.receipts_root.clone();

    let mut system = receipts[0].clone();
    system.cumulative_gas_used = "0x0".to_string();
    system.transaction_hash = format!("0x{:064x}", 0xbeefu64);
    receipts.push(system);

    let refs: Vec<&RpcReceipt> = receipts.iter().collect();
    let computed = ChainUtils::new(0x3e7, false)
        .calculate_receipts_root(&block, &refs)
        .expect("root computes");
    assert_eq!(computed, honest);
}

// ─── The forged half: one changed field must fail its own switch ──────────────

/// The block every forgery starts from: receipts and withdrawals, so one
/// fixture exercises all five commitments.
fn modern() -> &'static Case {
    &CORPUS[0]
}

#[test]
fn a_forged_transaction_fails_the_transactions_root() {
    let case = modern();
    let mut block = load_block(case);
    block.transactions[0].nonce = "0xdead".to_string();

    let computed = utils(case)
        .calculate_transactions_root(&block)
        .expect("root computes")
        .expect("verifiable");
    assert_ne!(computed, block.transactions_root);
}

#[test]
fn a_forged_sender_fails_recovery() {
    let case = modern();
    let block = load_block(case);
    let mut tx = block
        .transactions
        .iter()
        .find(|tx| tx.tx_type.as_deref() != Some("0x7f"))
        .expect("a recoverable transaction")
        .clone();
    tx.from = format!("0x{:040x}", 0x1234u64);

    let recovered = utils(case)
        .recover_tx_sender(&tx)
        .expect("recovery succeeds")
        .expect("a signed transaction has a sender");
    assert_ne!(recovered.to_lowercase(), tx.from.to_lowercase());
}

#[test]
fn a_forged_withdrawal_fails_the_withdrawals_root() {
    let case = modern();
    let block = load_block(case);
    let mut withdrawals = block.withdrawals.clone().expect("post-Shanghai fixture");
    withdrawals[0].amount = "0xdeadbeef".to_string();

    let refs: Vec<&RpcWithdrawal> = withdrawals.iter().collect();
    let computed = utils(case)
        .calculate_withdrawals_root(&refs)
        .expect("root computes");
    assert_ne!(
        computed,
        block.withdrawals_root.expect("post-Shanghai fixture")
    );
}

#[test]
fn a_forged_receipt_fails_the_receipts_root() {
    let case = modern();
    let block = load_block(case);
    let mut receipts = load_receipts(case);
    // The trie commits to cumulative gas, not to the per-receipt figure.
    receipts[0].cumulative_gas_used = "0xdead".to_string();

    let refs: Vec<&RpcReceipt> = receipts.iter().collect();
    let computed = utils(case)
        .calculate_receipts_root(&block, &refs)
        .expect("root computes");
    assert_ne!(computed, block.receipts_root);
}

#[test]
fn a_forged_log_fails_the_logs_bloom() {
    let case = modern();
    let block = load_block(case);
    let mut logs = logs_of(&load_receipts(case));
    logs[0].address = format!("0x{:040x}", 0x9999u64);

    let refs: Vec<&RpcLog> = logs.iter().collect();
    let computed = utils(case).calculate_logs_bloom(&block, &refs);
    assert_ne!(computed, block.logs_bloom);
}

#[test]
fn a_forged_header_fails_the_block_hash() {
    let case = modern();
    let mut block = load_block(case);
    block.state_root = format!("0x{:064x}", 0x1u64);

    let computed = utils(case)
        .calculate_block_hash(&block)
        .expect("hash computes");
    assert_ne!(computed, block.hash);
}
