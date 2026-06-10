//! Integration tests: mock DataSource driving DataService + HTTP.

use async_trait::async_trait;
use bytes::Bytes;
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use data_service_core::{run_data_service, DataServiceOptions};
use futures::stream::BoxStream;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Mock DataSource
// ---------------------------------------------------------------------------

/// A simple scripted data source that plays back a fixed chain, optionally
/// with a reorg.
#[derive(Clone)]
struct MockSource {
    /// Blocks in the canonical chain (in order).
    chain: Arc<Vec<Block>>,
    /// If `Some(n)`, after emitting `n` blocks from `get_stream` the source
    /// emits a Fork error carrying the emitted refs.
    fork_after: Option<usize>,
}

fn make_block(number: u64, hash: &str, parent_number: u64, parent_hash: &str) -> Block {
    // Produce a real 1-byte zstd-compressed newline-terminated JSON line.
    let json = format!("{{\"number\":{number}}}\n");
    let zstd_bytes = zstd::encode_all(json.as_bytes(), 1).unwrap();
    Block {
        number,
        hash: hash.to_string(),
        parent_number,
        parent_hash: parent_hash.to_string(),
        timestamp: Some(number * 1000),
        json_line_zstd: Bytes::from(zstd_bytes),
    }
}

fn simple_chain(len: u64) -> Vec<Block> {
    (0..len)
        .map(|i| make_block(i, &format!("h{i}"), i.saturating_sub(1), &if i == 0 { "".to_string() } else { format!("h{}", i - 1) }))
        .collect()
}

#[async_trait]
impl DataSource for MockSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        let last = self.chain.last().unwrap();
        Ok(last.block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        // Finalize the first block.
        Ok(self.chain[0].block_ref())
    }

    fn get_finalized_stream(&self, req: StreamRequest) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let chain = Arc::clone(&self.chain);
        let from = req.from;
        let to = req.to;
        Box::pin(async_stream::stream! {
            for block in chain.iter() {
                if block.number < from { continue; }
                if let Some(t) = to { if block.number > t { break; } }
                yield Ok(BlockBatch {
                    blocks: vec![block.clone()],
                    finalized_head: Some(block.block_ref()),
                });
            }
        })
    }

    fn get_stream(&self, req: StreamRequest) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let chain = Arc::clone(&self.chain);
        let fork_after = self.fork_after;
        let from = req.from;
        Box::pin(async_stream::stream! {
            let mut emitted = 0usize;
            let mut emitted_refs: Vec<BlockRef> = vec![];
            for block in chain.iter() {
                if block.number < from { continue; }
                if let Some(n) = fork_after {
                    if emitted >= n {
                        yield Err(StreamError::Fork { previous_blocks: emitted_refs.clone() });
                        return;
                    }
                }
                emitted_refs.push(block.block_ref());
                yield Ok(BlockBatch {
                    blocks: vec![block.clone()],
                    finalized_head: Some(block.block_ref()),
                });
                emitted += 1;
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Removed — use run_data_service directly in each test.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_head_endpoint() {
    let source = MockSource {
        chain: Arc::new(simple_chain(5)),
        fork_after: None,
    };
    let opts = DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    };
    let handle = run_data_service(opts).await.unwrap();
    let port = handle.port;

    // Wait a bit for ingestion.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{port}/head"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["number"].as_u64().is_some());
}

#[tokio::test]
async fn test_root_endpoint() {
    let source = MockSource {
        chain: Arc::new(simple_chain(3)),
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    let resp = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("hot block data service"), "got: {text}");
}

#[tokio::test]
async fn test_stream_zstd_response() {
    let chain = Arc::new(simple_chain(5));
    let source = MockSource {
        chain: Arc::clone(&chain),
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    // Allow ingestion.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(r#"{"fromBlock": 1}"#)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    // 200 or 204 are both valid depending on ingestion timing.
    assert!(
        status == 200 || status == 204,
        "unexpected status: {status}"
    );

    if status == 200 {
        let encoding = resp.headers().get("content-encoding").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        assert_eq!(encoding, "zstd");
        let bytes = resp.bytes().await.unwrap();
        // Decode zstd frames.
        let decoded = zstd::decode_all(std::io::Cursor::new(&bytes)).unwrap();
        let text = String::from_utf8(decoded).unwrap();
        // Each line should be valid JSON.
        for line in text.lines() {
            if !line.is_empty() {
                serde_json::from_str::<serde_json::Value>(line).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn test_stream_gzip_response() {
    let chain = Arc::new(simple_chain(5));
    let source = MockSource {
        chain: Arc::clone(&chain),
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // reqwest auto-decompresses gzip when you send accept-encoding: gzip.
    let client = reqwest::ClientBuilder::new().no_gzip().build().unwrap();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "gzip")
        .header("content-type", "application/json")
        .body(r#"{"fromBlock": 1}"#)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    assert!(status == 200 || status == 204, "unexpected status: {status}");

    if status == 200 {
        let encoding = resp.headers().get("content-encoding").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        assert_eq!(encoding, "gzip");
        let gz_bytes = resp.bytes().await.unwrap();
        // Decode gzip.
        use flate2::read::MultiGzDecoder;
        use std::io::Read;
        let mut decoder = MultiGzDecoder::new(std::io::Cursor::new(&gz_bytes));
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).unwrap();
        for line in decoded.lines() {
            if !line.is_empty() {
                serde_json::from_str::<serde_json::Value>(line).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn test_stream_409_invalid_base_block() {
    let chain = Arc::new(simple_chain(5));
    let source = MockSource {
        chain,
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("content-type", "application/json")
        // Ask from block 2, but with a wrong parent hash.
        .body(r#"{"fromBlock": 2, "parentBlockHash": "bad_hash"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["previousBlocks"].is_array());
}

#[tokio::test]
async fn test_stream_400_bad_request() {
    let source = MockSource {
        chain: Arc::new(simple_chain(3)),
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("content-type", "application/json")
        .body(r#"{"invalid": true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let source = MockSource {
        chain: Arc::new(simple_chain(3)),
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    let resp = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("sqd_hotblocks"), "missing sqd metrics: {text}");
}

#[tokio::test]
async fn test_block_time_endpoint() {
    let chain = Arc::new(simple_chain(5));
    let source = MockSource {
        chain,
        fork_after: None,
    };
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;

    // Wait for ingestion.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Block 1 should have been recorded during ingestion.
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/block-time/1"))
        .await
        .unwrap();
    // May be 200 (found) or 404 (not yet ingested).
    assert!(resp.status() == 200 || resp.status() == 404);

    // Definitely non-existent block.
    let resp404 = reqwest::get(format!("http://127.0.0.1:{port}/block-time/99999"))
        .await
        .unwrap();
    assert_eq!(resp404.status(), 404);
}

// ---------------------------------------------------------------------------
// End-to-end fork recovery
// ---------------------------------------------------------------------------

/// A source that serves branch A first, then signals a fork and serves
/// branch B (the new canonical chain) on subsequent `get_stream` calls.
///
/// Chain layout (common ancestor = block 2):
///   common:   0(h0) - 1(h1) - 2(h2)
///   branch A:                   \- 3(a3) - 4(a4)
///   branch B:                   \- 3(b3) - 4(b4) - 5(b5)
#[derive(Clone)]
struct ForkingSource {
    common: Arc<Vec<Block>>,
    branch_a: Arc<Vec<Block>>,
    branch_b: Arc<Vec<Block>>,
    /// Number of get_stream calls so far; call 0 serves A then forks.
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl ForkingSource {
    fn new() -> Self {
        let common = simple_chain(3); // 0,1,2 with h-hashes
        let branch_a = vec![
            make_block(3, "a3", 2, "h2"),
            make_block(4, "a4", 3, "a3"),
        ];
        let branch_b = vec![
            make_block(3, "b3", 2, "h2"),
            make_block(4, "b4", 3, "b3"),
            make_block(5, "b5", 4, "b4"),
        ];
        ForkingSource {
            common: Arc::new(common),
            branch_a: Arc::new(branch_a),
            branch_b: Arc::new(branch_b),
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl DataSource for ForkingSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.branch_b.last().unwrap().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.common[0].block_ref())
    }

    fn get_finalized_stream(&self, req: StreamRequest) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let common = Arc::clone(&self.common);
        Box::pin(async_stream::stream! {
            for block in common.iter() {
                if block.number < req.from { continue; }
                if let Some(t) = req.to { if block.number > t { break; } }
                yield Ok(BlockBatch {
                    blocks: vec![block.clone()],
                    finalized_head: Some(block.block_ref()),
                });
            }
        })
    }

    fn get_stream(&self, req: StreamRequest) -> BoxStream<'static, Result<BlockBatch, StreamError>> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let common = Arc::clone(&self.common);
        let branch = if call == 0 { Arc::clone(&self.branch_a) } else { Arc::clone(&self.branch_b) };
        let branch_b = Arc::clone(&self.branch_b);
        Box::pin(async_stream::stream! {
            for block in common.iter().chain(branch.iter()) {
                if block.number < req.from { continue; }
                yield Ok(BlockBatch { blocks: vec![block.clone()], finalized_head: None });
            }
            if call == 0 {
                // Upstream reorged: report the new canonical chain refs,
                // including the common ancestor, like ensure_continuity does.
                let mut prev = vec![common[2].block_ref()];
                prev.extend(branch_b.iter().map(|b| b.block_ref()));
                yield Err(StreamError::Fork { previous_blocks: prev });
            } else {
                // At head: stay open like a real hot stream.
                futures::future::pending::<()>().await;
            }
        })
    }
}

#[tokio::test]
async fn test_end_to_end_fork_recovery() {
    let source = ForkingSource::new();
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let port = handle.port;
    // The body is concatenated per-block gzip members; disable reqwest's
    // auto-decompression and decode with MultiGzDecoder below.
    let client = reqwest::ClientBuilder::new().no_gzip().build().unwrap();

    // Wait until the service recovered from the fork and reached branch B's head.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        let head: serde_json::Value = client
            .get(format!("http://127.0.0.1:{port}/head"))
            .send().await.unwrap().json().await.unwrap();
        if head["number"] == 5 && head["hash"] == "b5" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "never reached branch B head, got {head}");
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // A client that followed branch A asks for the next block after a4:
    // must get 409 with the new canonical refs so it can find the fork point.
    let resp = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .json(&serde_json::json!({"fromBlock": 5, "parentBlockHash": "a4"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await.unwrap();
    let prev = body["previousBlocks"].as_array().unwrap();
    assert!(
        prev.iter().any(|r| r["number"] == 3 && r["hash"] == "b3"),
        "previousBlocks should expose the new branch: {prev:?}"
    );

    // Re-requesting from the common ancestor serves branch B.
    let resp = client
        .post(format!("http://127.0.0.1:{port}/stream"))
        .json(&serde_json::json!({"fromBlock": 3, "parentBlockHash": "h2"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    let text = {
        use flate2::read::MultiGzDecoder;
        use std::io::Read;
        let mut decoded = String::new();
        MultiGzDecoder::new(std::io::Cursor::new(&body))
            .read_to_string(&mut decoded)
            .unwrap();
        decoded
    };
    let numbers: Vec<u64> = text
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["number"].as_u64().unwrap())
        .collect();
    assert_eq!(numbers, vec![3, 4, 5], "should serve branch B after reorg");
}
