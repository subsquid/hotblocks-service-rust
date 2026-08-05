//! CT-4 — an enabled verification check that fails is incoherence, not a
//! session error (GAP-32).
//!
//! The block is re-acquired a bounded number of times (WP-11.2), then the
//! session fails loud naming it (WP-11.3) — the same contract every other
//! component fault takes. The script leaves blocks empty, so their commitments
//! are honest and only the forged one fails.

use std::sync::Arc;
use std::time::Duration;

use data_service_core::source::{DataSource, StreamRequest};
use evm_source::fetch::RpcOptions;
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use futures::StreamExt;
use harness::upstream::{Chain, Fault, Upstream};
use rpc_client::{RpcClient, RpcClientConfig};
use serde_json::{json, Value};

const P_ENRICH_RETRIES: usize = 10;
const ACQUISITIONS: usize = 1 + P_ENRICH_RETRIES;

const FIRST: u64 = 1000;
const FAULTED: u64 = 1002;

const BUDGET: Duration = Duration::from_secs(20);
const HEADER_METHOD: &str = "eth_getBlockByNumber";
const RECEIPTS_METHOD: &str = "eth_getBlockReceipts";

#[derive(Default)]
struct Run {
    served: Vec<Value>,
    error: Option<String>,
}

impl Run {
    fn numbers(&self) -> Vec<u64> {
        self.served
            .iter()
            .map(|b| b["header"]["number"].as_u64().expect("header.number"))
            .collect()
    }

    fn assert_failed_loud(&self, faulted: u64) {
        assert!(
            !self.numbers().contains(&faulted),
            "a block that failed an enabled check must never be served"
        );
        let error = self
            .error
            .as_deref()
            .expect("the session must fail after the retry budget, not stall");
        assert!(
            error.contains(&faulted.to_string()),
            "the session error must name the block it gave up on, got: {error}"
        );
    }
}

async fn drive(upstream: &Upstream, rpc_options: RpcOptions, req: DataRequest, want: usize) -> Run {
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));
    let source = EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            rpc_options,
            data_request: req,
            ..Default::default()
        },
    );

    let mut stream = source.get_stream(StreamRequest {
        from: FIRST,
        to: None,
        parent_hash: None,
    });

    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut run = Run::default();
    while run.served.len() < want {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_elapsed) => break,
            Ok(None) => break,
            Ok(Some(Err(e))) => {
                run.error = Some(e.to_string());
                break;
            }
            Ok(Some(Ok(batch))) => {
                for block in batch.blocks {
                    let line = zstd::decode_all(block.json_line_zstd.as_ref()).expect("zstd");
                    run.served
                        .push(serde_json::from_slice(&line).expect("payload is one JSON line"));
                }
            }
        }
    }
    run
}

/// Empty blocks, and a head four above `FIRST` to keep the speculative path.
async fn empty_chain() -> Upstream {
    Upstream::start(Chain::linear(FIRST, 5, 0)).await
}

fn verify_tx_root() -> RpcOptions {
    RpcOptions {
        verify_tx_root: true,
        ..RpcOptions::default()
    }
}

fn verify_receipts_root() -> RpcOptions {
    RpcOptions {
        verify_receipts_root: true,
        ..RpcOptions::default()
    }
}

fn with_receipts() -> DataRequest {
    DataRequest {
        receipts: true,
        ..DataRequest::default()
    }
}

/// A commitment the block's (empty) contents do not back.
fn forged_root(key: &str) -> Fault {
    Fault::ForgedField {
        key: key.to_string(),
        value: json!(format!("0x{:064x}", 0xdeadu64)),
    }
}

#[tokio::test]
async fn honest_blocks_pass_an_enabled_check() {
    let upstream = empty_chain().await;
    let run = drive(&upstream, verify_tx_root(), DataRequest::default(), 3).await;

    assert_eq!(run.numbers(), vec![FIRST, FIRST + 1, FIRST + 2]);
    assert_eq!(run.error, None);
}

#[tokio::test]
async fn a_forged_commitment_passes_while_its_switch_is_off() {
    let upstream = empty_chain().await;
    upstream.inject(HEADER_METHOD, FAULTED, forged_root("transactionsRoot"));

    let run = drive(&upstream, RpcOptions::default(), DataRequest::default(), 3).await;

    assert_eq!(run.numbers(), vec![FIRST, FIRST + 1, FAULTED]);
    assert_eq!(run.error, None);
}

#[tokio::test]
async fn a_forged_commitment_retries_then_fails_loud() {
    let upstream = empty_chain().await;
    upstream.inject(HEADER_METHOD, FAULTED, forged_root("transactionsRoot"));

    let run = drive(&upstream, verify_tx_root(), DataRequest::default(), 100).await;

    assert_eq!(run.numbers(), vec![FIRST, FIRST + 1]);
    run.assert_failed_loud(FAULTED);
    assert_eq!(
        upstream.calls(HEADER_METHOD, FAULTED),
        ACQUISITIONS,
        "re-acquired P-ENRICH-RETRIES times, no more and no fewer"
    );
}

/// The same bound when components are selected: the header check decides before
/// enrichment starts.
#[tokio::test]
async fn a_forged_commitment_is_bounded_on_the_enriched_path() {
    let upstream = empty_chain().await;
    upstream.inject(HEADER_METHOD, FAULTED, forged_root("transactionsRoot"));

    let run = drive(&upstream, verify_tx_root(), with_receipts(), 100).await;

    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(HEADER_METHOD, FAULTED), ACQUISITIONS);
}

/// A check that fires during enrichment takes the same path.
#[tokio::test]
async fn a_forged_receipts_commitment_retries_then_fails_loud() {
    let upstream = empty_chain().await;
    upstream.inject(HEADER_METHOD, FAULTED, forged_root("receiptsRoot"));

    let run = drive(&upstream, verify_receipts_root(), with_receipts(), 100).await;

    assert_eq!(run.numbers(), vec![FIRST, FIRST + 1]);
    run.assert_failed_loud(FAULTED);
    assert_eq!(upstream.calls(RECEIPTS_METHOD, FAULTED), ACQUISITIONS);
}
