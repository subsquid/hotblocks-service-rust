//! CT-5 — canonical trace bytes are independent of process-local hash seeds
//! (INV-26; retired GAP-13 regression).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use data_service_core::source::{DataSource, StreamRequest};
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use futures::StreamExt;
use harness::upstream::{Chain, Upstream};
use rpc_client::{RpcClient, RpcClientConfig};

const CHILD_OUTPUT: &str = "SQD_CT5_TRACE_OUTPUT";
const CHILD_ACQUISITION_TIMEOUT: Duration = Duration::from_secs(60);
const FIRST: u64 = 1_000;
const TXS: usize = 16;

/// The parent launches this exact test in fresh processes so each acquisition
/// receives an independent std HashMap seed.
#[tokio::test]
#[ignore = "child fixture invoked explicitly by the cross-process parent"]
async fn emit_trace_block_payload() {
    let Some(path) = std::env::var_os(CHILD_OUTPUT).map(PathBuf::from) else {
        return;
    };

    let upstream = Upstream::start(Chain::linear(FIRST, 5, TXS)).await;
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));
    let source = EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            data_request: DataRequest {
                receipts: true,
                traces: true,
                use_trace_api: true,
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let mut stream = source.get_stream(StreamRequest {
        from: FIRST,
        to: None,
        parent_hash: None,
    });
    let deadline = tokio::time::Instant::now() + CHILD_ACQUISITION_TIMEOUT;

    loop {
        let batch = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("trace acquisition timed out")
            .expect("trace stream ended")
            .expect("trace acquisition failed");
        if let Some(block) = batch.blocks.into_iter().find(|block| block.number == FIRST) {
            let payload = zstd::decode_all(block.json_line_zstd.as_ref()).expect("zstd payload");
            std::fs::write(path, payload).expect("write child payload");
            return;
        }
    }
}

#[test]
fn trace_block_bytes_are_stable_across_processes() {
    if std::env::var_os(CHILD_OUTPUT).is_some() {
        return;
    }

    let first = acquire_in_fresh_process(1);
    let second = acquire_in_fresh_process(2);

    assert!(
        first == second,
        "identical upstream input changed payload bytes"
    );

    let block: serde_json::Value = serde_json::from_slice(&first).expect("canonical JSON line");
    let indices: Vec<u64> = block["traces"]
        .as_array()
        .expect("traces")
        .iter()
        .map(|trace| {
            trace["transactionIndex"]
                .as_u64()
                .expect("transactionIndex")
        })
        .collect();
    assert_eq!(
        indices,
        (0..TXS as u64).collect::<Vec<_>>(),
        "trace groups must retain the predecessor's first-seen insertion order"
    );
}

struct ChildOutput(PathBuf);

impl ChildOutput {
    fn new(run: usize) -> Self {
        Self(std::env::temp_dir().join(format!("sqd-ct5-traces-{}-{run}.json", std::process::id())))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ChildOutput {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn acquire_in_fresh_process(run: usize) -> Vec<u8> {
    let output_path = ChildOutput::new(run);
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .args([
            "--exact",
            "emit_trace_block_payload",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_OUTPUT, output_path.path())
        .output()
        .expect("launch child acquisition");
    assert!(
        output.status.success(),
        "child acquisition failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::read(output_path.path()).expect("read child payload")
}
