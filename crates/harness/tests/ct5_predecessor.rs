//! HC-8/REQ-24 — temporary differential oracle against the TypeScript
//! predecessor. The nightly workflow opts into this ignored test and supplies
//! a checkout pinned to the migration oracle revision.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use data_service_core::http::build_router;
use data_service_core::service::DataService;
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::BlockRef;
use evm_source::normalization::{map_rpc_block, MappingOptions};
use evm_source::rpc_data::RawRpcBlock;
use futures::stream::BoxStream;
use serde_json::{json, Value};

const PREDECESSOR_PACKAGE_DIR: &str = "SQD_PREDECESSOR_PACKAGE_DIR";

#[derive(Clone, Copy)]
struct PayloadCase {
    name: &'static str,
    fixture: &'static str,
    shape: FixtureShape,
    with_traces: bool,
    with_state_diffs: bool,
}

#[derive(Clone, Copy)]
enum FixtureShape {
    RecordedArray,
    GnosisPipeline,
}

const PAYLOAD_CASES: &[PayloadCase] = &[
    PayloadCase {
        name: "base logs",
        fixture: "fixtures/base-logs.json",
        shape: FixtureShape::RecordedArray,
        with_traces: false,
        with_state_diffs: false,
    },
    PayloadCase {
        name: "base receipts",
        fixture: "fixtures/base-receipts.json",
        shape: FixtureShape::RecordedArray,
        with_traces: false,
        with_state_diffs: false,
    },
    PayloadCase {
        name: "base traces",
        fixture: "fixtures/base-traces.json",
        shape: FixtureShape::RecordedArray,
        with_traces: true,
        with_state_diffs: false,
    },
    PayloadCase {
        name: "trace replay and state diff",
        fixture: "fixtures/gnosis-pipeline.json",
        shape: FixtureShape::GnosisPipeline,
        with_traces: true,
        with_state_diffs: true,
    },
];

struct OracleOutput(PathBuf);

impl OracleOutput {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sqd-hc8-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create HC-8 output directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for OracleOutput {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone)]
struct UnusedSource;

#[async_trait]
impl DataSource for UnusedSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        anyhow::bail!("source must not run in the HC-8 HTTP probes")
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        anyhow::bail!("source must not run in the HC-8 HTTP probes")
    }

    fn get_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(futures::stream::empty())
    }

    fn get_finalized_stream(
        &self,
        _req: StreamRequest,
    ) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        Box::pin(futures::stream::empty())
    }
}

#[tokio::test]
#[ignore = "temporary migration oracle; the nightly workflow supplies the pinned predecessor"]
async fn predecessor_matches_recorded_payloads_and_live_http_contract() {
    let predecessor = std::env::var_os(PREDECESSOR_PACKAGE_DIR)
        .map(PathBuf::from)
        .expect("SQD_PREDECESSOR_PACKAGE_DIR must name the pinned evm-data-service package");
    assert!(
        predecessor.join("package.json").is_file(),
        "predecessor package is not built at {}",
        predecessor.display()
    );

    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output_dir = OracleOutput::new();
    let cases_path = output_dir.path().join("cases.json");
    let cases = prepare_cases(&repo, output_dir.path());
    let mut cases_writer =
        BufWriter::new(fs::File::create(&cases_path).expect("create oracle case manifest"));
    serde_json::to_writer(&mut cases_writer, &cases).expect("write oracle case manifest");
    cases_writer.flush().expect("flush oracle case manifest");

    let stdout_path = output_dir.path().join("oracle.stdout");
    let stderr_path = output_dir.path().join("oracle.stderr");

    let mut oracle = Command::new("node")
        .arg(repo.join("scripts/hc8-predecessor-oracle.mjs"))
        .arg(&predecessor)
        .arg(&cases_path)
        .arg(output_dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            fs::File::create(&stdout_path).expect("create oracle stdout log"),
        ))
        .stderr(Stdio::from(
            fs::File::create(&stderr_path).expect("create oracle stderr log"),
        ))
        .spawn()
        .expect("launch predecessor oracle");

    let status =
        match tokio::time::timeout(Duration::from_secs(5 * 60), wait_for_oracle(&mut oracle)).await
        {
            Ok(result) => result.expect("wait for predecessor oracle"),
            Err(_) => {
                let _ = oracle.kill();
                let _ = oracle.wait();
                panic!(
                    "predecessor oracle timed out\nstdout:\n{}\nstderr:\n{}",
                    read_oracle_log(&stdout_path),
                    read_oracle_log(&stderr_path),
                );
            }
        };
    assert!(
        status.success(),
        "predecessor oracle failed\nstdout:\n{}\nstderr:\n{}",
        read_oracle_log(&stdout_path),
        read_oracle_log(&stderr_path),
    );

    let manifest: Value = serde_json::from_slice(
        &fs::read(output_dir.path().join("manifest.json")).expect("read oracle manifest"),
    )
    .expect("parse oracle manifest");
    compare_payloads(output_dir.path(), &manifest, &cases);
    compare_http(&manifest).await;
}

async fn wait_for_oracle(child: &mut Child) -> std::io::Result<ExitStatus> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn read_oracle_log(path: &Path) -> String {
    String::from_utf8_lossy(&fs::read(path).unwrap_or_default()).into_owned()
}

fn compare_payloads(output_dir: &Path, manifest: &Value, cases: &Value) {
    let payloads = manifest["payloads"].as_array().expect("payload manifest");
    let mut compared = 0;

    for (case_index, case) in cases
        .as_array()
        .expect("shared oracle cases")
        .iter()
        .enumerate()
    {
        let raw_blocks: Vec<Value> = serde_json::from_reader(BufReader::new(
            fs::File::open(
                case["rawBlocksFile"]
                    .as_str()
                    .expect("raw-block input file"),
            )
            .expect("open raw-block input file"),
        ))
        .expect("parse shared raw-block inputs");

        for (fixture_index, raw_value) in raw_blocks.into_iter().enumerate() {
            let entry = payloads
                .iter()
                .find(|entry| {
                    entry["caseIndex"] == case_index && entry["fixtureIndex"] == fixture_index
                })
                .expect("oracle payload entry");
            let raw: RawRpcBlock =
                serde_json::from_value(raw_value).expect("parse shared raw RPC fixture");
            let normalized = map_rpc_block(
                &raw,
                &MappingOptions {
                    with_traces: case["withTraces"].as_bool().expect("withTraces option"),
                    with_state_diffs: case["withStateDiffs"]
                        .as_bool()
                        .expect("withStateDiffs option"),
                },
            );
            let mut rust = serde_json::to_vec(&normalized).expect("serialize Rust payload");
            rust.push(b'\n');
            let predecessor = fs::read(
                output_dir.join(entry["file"].as_str().expect("oracle payload file name")),
            )
            .expect("read predecessor payload");
            assert_same_bytes(
                case["name"].as_str().expect("oracle case name"),
                fixture_index,
                &predecessor,
                &rust,
            );
            compared += 1;
        }
    }

    assert_eq!(
        payloads.len(),
        compared,
        "every oracle payload must be compared"
    );
}

fn prepare_cases(repo: &Path, output_dir: &Path) -> Value {
    Value::Array(
        PAYLOAD_CASES
            .iter()
            .enumerate()
            .map(|(case_index, case)| {
                let raw_blocks_path = output_dir.join(format!("raw-blocks-{case_index}.json"));
                let raw_blocks = load_raw_blocks(repo, case);
                let mut writer = BufWriter::new(
                    fs::File::create(&raw_blocks_path).expect("create raw-block input file"),
                );
                serde_json::to_writer(&mut writer, &raw_blocks)
                    .expect("write raw-block input file");
                writer.flush().expect("flush raw-block input file");
                json!({
                    "name": case.name,
                    "rawBlocksFile": raw_blocks_path,
                    "withTraces": case.with_traces,
                    "withStateDiffs": case.with_state_diffs,
                })
            })
            .collect(),
    )
}

fn load_raw_blocks(repo: &Path, case: &PayloadCase) -> Vec<Value> {
    let fixture: Value = serde_json::from_reader(BufReader::new(
        fs::File::open(repo.join(case.fixture)).expect("open recorded fixture"),
    ))
    .expect("parse recorded fixture");

    match case.shape {
        FixtureShape::RecordedArray => {
            let Value::Array(entries) = fixture else {
                panic!("recorded fixture must be an array")
            };
            entries
                .into_iter()
                .map(|entry| {
                    let Value::Object(mut entry) = entry else {
                        panic!("recorded fixture entry must be an object")
                    };
                    entry.remove("raw").expect("recorded raw block")
                })
                .collect()
        }
        FixtureShape::GnosisPipeline => {
            let Value::Object(mut fixture) = fixture else {
                panic!("pipeline fixture must be an object")
            };
            vec![json!({
                "number": fixture.remove("block_number").expect("pipeline block number"),
                "hash": fixture.remove("block_hash").expect("pipeline block hash"),
                "block": fixture.remove("getBlockByNumber").expect("pipeline block"),
                "receipts": fixture.remove("getBlockReceipts").expect("pipeline receipts"),
                "traceReplays": fixture.remove("traceReplay").expect("pipeline trace replay"),
            })]
        }
    }
}

async fn compare_http(manifest: &Value) {
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let service = Arc::new(DataService::new(UnusedSource, 1, false, cancel_rx));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Rust HTTP probe");
    let port = listener.local_addr().expect("Rust HTTP address").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, build_router(service))
            .await
            .expect("serve Rust HTTP probe");
    });
    let client = reqwest::Client::new();

    let metrics = client
        .get(format!("http://127.0.0.1:{port}/metrics?json=true"))
        .send()
        .await
        .expect("query Rust JSON metrics");
    assert_eq!(
        metrics.status().as_u16(),
        manifest["http"]["metrics"]["status"]
            .as_u64()
            .expect("oracle metrics status") as u16
    );
    assert_media_type(
        manifest["http"]["metrics"]["contentType"]
            .as_str()
            .expect("oracle metrics content type"),
        metrics
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .expect("Rust metrics content type"),
    );
    let rust_metrics: Value = metrics.json().await.expect("parse Rust JSON metrics");
    assert_eq!(
        metric_families(&manifest["http"]["metrics"]["families"]),
        metric_families(&rust_metrics),
        "structured metric families diverged from the predecessor"
    );

    let oversized = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("content-type", "application/json")
        .body(vec![b'x'; 1025])
        .send()
        .await
        .expect("query Rust oversized request");
    assert_eq!(
        oversized.status().as_u16(),
        manifest["http"]["oversized"]["status"]
            .as_u64()
            .expect("oracle oversized status") as u16
    );
    assert_media_type(
        manifest["http"]["oversized"]["contentType"]
            .as_str()
            .expect("oracle oversized content type"),
        oversized
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .expect("Rust oversized content type"),
    );

    server.abort();
    let _ = server.await;
}

fn metric_families(value: &Value) -> BTreeMap<String, Value> {
    value
        .as_array()
        .expect("metric-family array")
        .iter()
        .filter_map(|family| {
            let name = family["name"].as_str()?;
            name.starts_with("sqd_hotblocks_").then(|| {
                let mut family = family.clone();
                family["values"]
                    .as_array_mut()
                    .expect("metric values")
                    .sort_by_key(metric_value_key);
                (name.to_string(), family)
            })
        })
        .collect()
}

fn metric_value_key(value: &Value) -> String {
    let labels: BTreeMap<_, _> = value["labels"]
        .as_object()
        .expect("metric labels")
        .iter()
        .collect();
    format!(
        "{}|{}|{}",
        value["metricName"].as_str().unwrap_or_default(),
        serde_json::to_string(&labels).expect("serialize canonical labels"),
        value["value"]
    )
}

fn assert_media_type(predecessor: &str, rust: &str) {
    let media_type = |value: &str| {
        value
            .split(';')
            .next()
            .unwrap_or(value)
            .trim()
            .to_ascii_lowercase()
    };
    assert_eq!(media_type(predecessor), media_type(rust));
}

fn assert_same_bytes(case: &str, index: usize, predecessor: &[u8], rust: &[u8]) {
    if predecessor == rust {
        return;
    }
    let offset = predecessor
        .iter()
        .zip(rust)
        .position(|(left, right)| left != right)
        .unwrap_or_else(|| predecessor.len().min(rust.len()));
    let predecessor_end = (offset + 80).min(predecessor.len());
    let rust_end = (offset + 80).min(rust.len());
    panic!(
        "{case} fixture {index} differs at byte {offset}\npredecessor: {}\nrust: {}",
        String::from_utf8_lossy(&predecessor[offset..predecessor_end]),
        String::from_utf8_lossy(&rust[offset..rust_end]),
    );
}
