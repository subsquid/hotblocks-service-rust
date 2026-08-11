//! The speculative replay is asked by *number*, before the block exists, so
//! nothing binds it to the header that eventually arrives. Adoption is what
//! recovers that binding; these cover the cases where it must refuse.

use std::path::Path;
use std::sync::Arc;

use evm_source::fetch::{Rpc, RpcOptions};
use evm_source::rpc_data::{RawRpcBlock, RpcBlock};
use rpc_client::{RpcClient, RpcClientConfig};
use serde_json::{json, Value};

fn gnosis_block() -> RawRpcBlock {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/gnosis-block-no-total-difficulty.json");
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let block: RpcBlock = serde_json::from_value(fixture["getBlockByNumber"].clone()).unwrap();
    RawRpcBlock::new(
        u64::from_str_radix(block.number.trim_start_matches("0x"), 16).unwrap(),
        block.hash.clone(),
        block,
    )
}

fn rpc() -> Arc<Rpc> {
    // Never dialled: adoption is pure validation against the body in hand.
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: "http://127.0.0.1:1".to_string(),
        ..RpcClientConfig::default()
    }));
    Arc::new(Rpc::new(client, RpcOptions::default()))
}

/// A well-formed root call frame; `replays_of` rejects an empty trace list.
fn root_frame() -> Value {
    json!({
        "type": "call",
        "action": {
            "callType": "call",
            "from": "0x0000000000000000000000000000000000000001",
            "gas": "0x0",
            "input": "0x",
            "to": "0x0000000000000000000000000000000000000002",
            "value": "0x0"
        },
        "result": {"gasUsed": "0x0", "output": "0x"},
        "subtraces": 0,
        "traceAddress": []
    })
}

/// One entry per transaction, in order, each carrying a root frame and a diff.
fn replay_for(block: &RawRpcBlock) -> Value {
    Value::Array(
        block
            .block
            .transactions
            .iter()
            .map(|tx| {
                json!({
                    "transactionHash": tx.hash,
                    "trace": [root_frame()],
                    "stateDiff": {},
                })
            })
            .collect(),
    )
}

#[test]
fn a_replay_matching_the_body_is_adopted_and_clears_the_traces_leg() {
    let rpc = rpc();
    let mut block = gnosis_block();
    let replay = replay_for(&block);

    assert!(rpc.adopt_speculative_replay(&mut block, replay, &["trace", "stateDiff"]));
    assert_eq!(
        block.trace_replays.as_ref().map(Vec::len),
        Some(block.block.transactions.len()),
        "an adopted replay must cover every transaction"
    );
}

/// The reorg case the hash-addressed fetch used to prevent structurally: a
/// replay of a *different* block at the same height. Same shape, same count —
/// only the hashes give it away.
#[test]
fn a_replay_of_another_block_is_rejected() {
    let rpc = rpc();
    let mut block = gnosis_block();
    let foreign = Value::Array(
        (0..block.block.transactions.len())
            .map(|i| {
                json!({
                    "transactionHash": format!("0x{:064x}", i + 1),
                    "trace": [root_frame()],
                    "stateDiff": {},
                })
            })
            .collect(),
    );

    assert!(!rpc.adopt_speculative_replay(&mut block, foreign, &["trace", "stateDiff"]));
    assert!(
        block.trace_replays.is_none(),
        "a rejected replay must leave the body clean so the ordinary fetch reruns"
    );
}

/// A replay that arrived while the block was still being imported: fewer
/// entries than transactions.
#[test]
fn a_short_replay_is_rejected() {
    let rpc = rpc();
    let mut block = gnosis_block();
    let mut short = replay_for(&block);
    short.as_array_mut().unwrap().truncate(2);

    assert!(!rpc.adopt_speculative_replay(&mut block, short, &["trace", "stateDiff"]));
    assert!(block.trace_replays.is_none());
}

/// The tracer set is part of the contract: a replay fetched without stateDiff
/// cannot stand in for one the request needs diffs from.
#[test]
fn a_replay_missing_a_requested_tracer_is_rejected() {
    let rpc = rpc();
    let mut block = gnosis_block();
    let traces_only = Value::Array(
        block
            .block
            .transactions
            .iter()
            .map(|tx| {
                json!({
                    "transactionHash": tx.hash,
                    "trace": [root_frame()],
                })
            })
            .collect(),
    );

    assert!(!rpc.adopt_speculative_replay(&mut block, traces_only, &["trace", "stateDiff"]));
    assert!(block.trace_replays.is_none());
}
