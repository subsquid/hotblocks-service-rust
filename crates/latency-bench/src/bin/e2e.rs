//! Head-path latency benchmark (HC-12 / CT-6): release blocks on a cadence
//! from a scripted upstream and measure, per block,
//!   release → body-served (detection: poll grain + RTT)
//!   release → committed  (parse + verify + enrich + normalize + commit)
//!   release → first-byte (client-observed /stream delivery)
//! with receipts acquisition and every --verify-* switch on.
//!
//! Env knobs: BENCH_BLOCKS (20), BENCH_CADENCE_MS (3000), BENCH_RTT_MS (25),
//! BENCH_WARMUP (3, excluded from stats), BENCH_OUT (json path).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::json;

use data_service_core::service::{run_data_service, DataServiceOptions};
use evm_source::fetch::RpcOptions;
use evm_source::types::DataRequest;
use evm_source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use latency_bench::upstream::Upstream;
use latency_bench::{build_chain, self_check, stats, unix_ms, BASE_HEIGHT};
use rpc_client::{RpcClient, RpcClientConfig};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Burst {
    first_chunk_ms: u64,
    bytes: Vec<u8>,
}

/// Decode one burst (whole frames of one or more blocks) to JSON lines.
fn decode_frames(encoding: &str, bytes: &[u8]) -> Vec<u8> {
    match encoding {
        "zstd" => {
            assert!(
                bytes.len() >= 4 && bytes[..4] == [0x28, 0xB5, 0x2F, 0xFD],
                "burst does not start at a zstd frame boundary"
            );
            zstd::decode_all(std::io::Cursor::new(bytes)).expect("zstd frame decodes")
        }
        _ => {
            assert!(
                bytes.len() >= 2 && bytes[..2] == [0x1f, 0x8b],
                "burst does not start at a gzip member boundary"
            );
            use std::io::Read;
            let mut out = Vec::new();
            flate2::read::MultiGzDecoder::new(std::io::Cursor::new(bytes))
                .read_to_end(&mut out)
                .expect("gzip member decodes");
            out
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,block_timing=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let blocks_n = env_u64("BENCH_BLOCKS", 20) as usize;
    let cadence = Duration::from_millis(env_u64("BENCH_CADENCE_MS", 3000));
    let rtt = Duration::from_millis(env_u64("BENCH_RTT_MS", 25));
    let warmup = env_u64("BENCH_WARMUP", 3) as usize;
    let encoding = std::env::var("BENCH_ENCODING").unwrap_or_else(|_| "zstd".into());
    let clients = env_u64("BENCH_CLIENTS", 1) as usize;
    anyhow::ensure!(
        encoding == "zstd" || encoding == "gzip",
        "BENCH_ENCODING must be zstd or gzip"
    );
    let with_traces = env_u64("BENCH_TRACES", 0) != 0;
    let with_statediffs = env_u64("BENCH_STATEDIFFS", 0) != 0;
    let trace_ms = Duration::from_millis(env_u64("BENCH_TRACE_MS", 300));

    eprintln!(
        "building chain: {} blocks + seed, cadence {:?}, rtt {:?}, traces={} statediffs={} trace_ms={:?}",
        blocks_n, cadence, rtt, with_traces, with_statediffs, trace_ms
    );
    let chain = build_chain(blocks_n + 1)?;
    self_check(&chain)?;
    let heights: Vec<u64> = (1..=blocks_n).map(|i| BASE_HEIGHT + i as u64).collect();

    let tx_count = chain.blocks[0].full["transactions"]
        .as_array()
        .map_or(0, |t| t.len());
    let (trace_frames, state_diffs) = if with_traces || with_statediffs {
        latency_bench::build_trace_fixture(tx_count)?
    } else {
        (serde_json::Value::Null, serde_json::Value::Null)
    };
    let upstream =
        Arc::new(Upstream::start(chain, rtt, trace_frames, state_diffs, trace_ms).await?);

    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url.clone(),
        capacity: 10,
        retry_attempts: 3,
        ..Default::default()
    }));
    let source = EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            rpc_options: RpcOptions {
                verify_block_hash: true,
                verify_tx_sender: true,
                verify_tx_root: true,
                verify_receipts_root: true,
                verify_withdrawals_root: true,
                verify_logs_bloom: true,
                ..Default::default()
            },
            data_request: DataRequest {
                receipts: true,
                traces: with_traces,
                state_diffs: with_statediffs,
                use_debug_trace_block_by_number: true,
                use_debug_api_for_state_diffs: with_statediffs,
                ..Default::default()
            },
            stride_size: 5,
            stride_concurrency: 5,
            profile_block_timings: true,
        },
    );

    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 200,
        port: 0,
        auto_adjust_finalized_head: false,
        metrics: None,
    })
    .await?;
    let base_url = format!("http://127.0.0.1:{}", handle.port);

    let http = reqwest::Client::builder()
        .no_gzip()
        .connect_timeout(Duration::from_secs(2))
        .build()?;

    // Wait for the seed block to be queryable.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(resp) = http.get(format!("{base_url}/head")).send().await {
            if resp.status().is_success() {
                let head: serde_json::Value = resp.json().await?;
                if head["number"].as_u64() == Some(BASE_HEIGHT) {
                    break;
                }
            }
        }
        anyhow::ensure!(Instant::now() < deadline, "service never seeded");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    eprintln!("service seeded at {}", BASE_HEIGHT);

    // Stream clients, following the production wire contract: /stream serves
    // the buffered window and closes; a client re-requests from last+1 and
    // parks in the server's 5 s wait until the next block commits. The parked
    // reconnect is the real head-block first-byte path.
    let last_height = BASE_HEIGHT + blocks_n as u64;
    let mut reader_handles = Vec::new();
    let mut first_byte_maps: Vec<Arc<Mutex<std::collections::HashMap<u64, u64>>>> = Vec::new();
    for _ in 0..clients {
        let first_byte: Arc<Mutex<std::collections::HashMap<u64, u64>>> =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        first_byte_maps.push(first_byte.clone());
        let http = http.clone();
        let base_url = base_url.clone();
        let encoding = encoding.clone();
        reader_handles.push(tokio::spawn(async move {
            let mut next = BASE_HEIGHT;
            'sessions: while next <= last_height {
                let resp = http
                    .post(format!("{base_url}/stream"))
                    .header("accept-encoding", encoding.as_str())
                    .json(&json!({ "fromBlock": next }))
                    .send()
                    .await
                    .expect("stream connect");
                match resp.status().as_u16() {
                    200 => {}
                    204 => {
                        // RP-12 empty wait: the block outran the 5 s budget.
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue 'sessions;
                    }
                    s => panic!("unexpected /stream status {s}"),
                }
                assert_eq!(
                    resp.headers()
                        .get("content-encoding")
                        .and_then(|v| v.to_str().ok()),
                    Some(encoding.as_str()),
                    "stream must take the requested encoding"
                );
                // One response carries one or more whole frames; chunks of the
                // same frame arrive back-to-back, distinct blocks are cadence
                // apart — a quiet gap splits bursts.
                let mut bursts: Vec<Burst> = Vec::new();
                let mut stream = resp.bytes_stream();
                let mut last_chunk: Option<Instant> = None;
                while let Some(Ok(chunk)) = stream.next().await {
                    let ts = unix_ms();
                    let now = Instant::now();
                    let new_burst = last_chunk
                        .is_none_or(|t| now.duration_since(t) > Duration::from_millis(300));
                    if new_burst {
                        bursts.push(Burst {
                            first_chunk_ms: ts,
                            bytes: Vec::new(),
                        });
                    }
                    bursts.last_mut().unwrap().bytes.extend_from_slice(&chunk);
                    last_chunk = Some(now);
                }
                for b in &bursts {
                    let decoded = decode_frames(&encoding, &b.bytes);
                    let mut heights = decoded
                        .split(|&c| c == b'\n')
                        .filter(|l| !l.is_empty())
                        .map(|line| {
                            let v: serde_json::Value =
                                serde_json::from_slice(line).expect("frame is a json line");
                            v["header"]["number"].as_u64().expect("header.number")
                        });
                    let first = heights.next().expect("burst holds at least one line");
                    first_byte
                        .lock()
                        .unwrap()
                        .entry(first)
                        .or_insert(b.first_chunk_ms);
                    let trailing = heights.max().unwrap_or(first);
                    next = next.max(trailing + 1).max(first + 1);
                }
                if bursts.is_empty() {
                    // 200 with no frames should not happen; avoid a hot loop.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }));
    }

    // Let the seed frame arrive before the cadence starts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The cadence: release one block per tick.
    let mut t_release: Vec<u64> = vec![0; blocks_n + 1];
    for i in 1..=blocks_n {
        tokio::time::sleep(cadence).await;
        t_release[i] = unix_ms();
        upstream.set_head(i);
        upstream.set_finalized(i.saturating_sub(4));
    }

    // Wait for the last block to commit, then let the frame flush.
    let last = BASE_HEIGHT + blocks_n as u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    let commit_of = |h: u64| {
        let http = http.clone();
        let base_url = base_url.clone();
        async move {
            let resp = http
                .get(format!("{base_url}/block-time/{h}"))
                .send()
                .await
                .ok()?;
            if !resp.status().is_success() {
                return None;
            }
            resp.text().await.ok()?.trim().parse::<u64>().ok()
        }
    };
    while commit_of(last).await.is_none() {
        anyhow::ensure!(Instant::now() < deadline, "block {last} never committed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for reader in reader_handles {
        match tokio::time::timeout(Duration::from_secs(10), reader).await {
            Ok(r) => r?,
            Err(_) => anyhow::bail!("a stream reader never caught up to {last}"),
        }
    }
    let maps: Vec<std::collections::HashMap<u64, u64>> = first_byte_maps
        .into_iter()
        .map(|m| {
            Arc::try_unwrap(m)
                .expect("readers joined")
                .into_inner()
                .unwrap()
        })
        .collect();
    let first_byte = maps[0].clone();
    eprintln!("client 0 saw {} first-byte stamps", first_byte.len());

    // Prefetch commit stamps once; the rows and the pooled per-client serve
    // stats both read them.
    let mut commit_by_i: Vec<Option<u64>> = Vec::with_capacity(blocks_n + 1);
    commit_by_i.push(None);
    for h in &heights {
        commit_by_i.push(commit_of(*h).await);
    }

    // Assemble rows.
    println!();
    println!("  height    detect_ms  pipeline_ms  commit_ms  first_byte_ms  serve_ms");
    let mut detect = Vec::new();
    let mut pipeline = Vec::new();
    let mut commit = Vec::new();
    let mut e2e = Vec::new();
    let mut serve = Vec::new();
    let mut rows = Vec::new();
    for i in 1..=blocks_n {
        let h = heights[i - 1];
        let rel = t_release[i];
        let body = upstream.body_served_ms(h);
        let com = commit_by_i[i];
        let fb = first_byte.get(&h).copied();
        let d = body.map(|b| b.saturating_sub(rel) as f64);
        let c = com.map(|c| c.saturating_sub(rel) as f64);
        // detect→commit strips the poll-grain jitter: the clean signal for
        // the CPU-side levers.
        let p = match (body, com) {
            (Some(b), Some(c)) => Some(c.saturating_sub(b) as f64),
            _ => None,
        };
        let e = fb.map(|f| f.saturating_sub(rel) as f64);
        let s = match (fb, com) {
            (Some(f), Some(c)) => Some(f.saturating_sub(c) as f64),
            _ => None,
        };
        let warm = i > warmup;
        if warm {
            if let Some(v) = d {
                detect.push(v)
            }
            if let Some(v) = p {
                pipeline.push(v)
            }
            if let Some(v) = c {
                commit.push(v)
            }
            if let Some(v) = e {
                e2e.push(v)
            }
            if let Some(v) = s {
                serve.push(v)
            }
        }
        let fmt = |o: Option<f64>| o.map_or("   -".into(), |v| format!("{v:7.0}"));
        println!(
            "  {h}{}  {}      {}    {}    {}        {}",
            if warm { " " } else { "*" },
            fmt(d),
            fmt(p),
            fmt(c),
            fmt(e),
            fmt(s)
        );
        rows.push(json!({
            "height": h, "warmup": !warm,
            "detect_ms": d, "pipeline_ms": p, "commit_ms": c, "first_byte_ms": e, "serve_ms": s,
        }));
    }
    println!("  (* = warmup, excluded from stats)");
    println!();
    let mut summary = serde_json::Map::new();
    for (name, samples) in [
        ("detect", &detect),
        ("pipeline", &pipeline),
        ("commit", &commit),
        ("first_byte", &e2e),
        ("serve", &serve),
    ] {
        let (min, med, p90, max) = stats(samples);
        println!(
            "  release→{name:<11} n={:<3} min {min:6.1}  median {med:6.1}  p90 {p90:6.1}  max {max:6.1}",
            samples.len()
        );
        summary.insert(
            name.to_string(),
            json!({"n": samples.len(), "min": min, "median": med, "p90": p90, "max": max}),
        );
    }

    if clients > 1 {
        println!();
        for (ci, m) in maps.iter().enumerate() {
            let samples: Vec<f64> = (warmup + 1..=blocks_n)
                .filter_map(|i| {
                    let f = m.get(&heights[i - 1]).copied()?;
                    let c = commit_by_i[i]?;
                    Some(f.saturating_sub(c) as f64)
                })
                .collect();
            let (min, med, p90, max) = stats(&samples);
            println!(
                "  client {ci} serve       n={:<3} min {min:6.1}  median {med:6.1}  p90 {p90:6.1}  max {max:6.1}",
                samples.len()
            );
            summary.insert(
                format!("serve_client_{ci}"),
                json!({"n": samples.len(), "min": min, "median": med, "p90": p90, "max": max}),
            );
        }
    }

    if let Ok(out) = std::env::var("BENCH_OUT") {
        let doc = json!({
            "config": {
                "blocks": blocks_n, "cadence_ms": cadence.as_millis() as u64,
                "rtt_ms": rtt.as_millis() as u64, "warmup": warmup,
                "verify": "all", "receipts": true,
                "traces": with_traces, "statediffs": with_statediffs,
                "trace_ms": trace_ms.as_millis() as u64,
                "encoding": encoding, "clients": clients,
            },
            "summary": summary,
            "rows": rows,
        });
        std::fs::write(&out, serde_json::to_string_pretty(&doc)?)?;
        eprintln!("wrote {out}");
    }

    handle.shutdown().await;
    Ok(())
}
