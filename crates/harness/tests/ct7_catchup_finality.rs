//! CT-7 — finality keep-up while catching up (GAP-2).
//!
//! Spec 13's CT-7 is a multi-hour soak due with HC-10/HC-12 in Phase 4; this is
//! its deterministic entry point, stating one property at two altitudes: the
//! head path must carry finality that tracks upstream (LIV-7), or eviction
//! starves and the buffer grows by the catch-up gap rather than the window
//! (WP-24, INV-4).
//!
//! The buffer sits below upstream finality whenever a session resumes from a
//! head it committed before an outage — T1 does not re-seed on an ordinary
//! session error. `set_finalized` after `init` models that without the outage.

use std::sync::Arc;
use std::time::Duration;

use data_service_core::service::DataService;
use data_service_core::source::{DataSource, StreamRequest};
use data_service_core::Metrics;
use evm_source::fetch::RpcOptions;
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use futures::StreamExt;
use harness::upstream::{Chain, Upstream};
use rpc_client::{RpcClient, RpcClientConfig};
use tokio::sync::watch;

/// Where the buffer stands when the run starts.
const FIRST: u64 = 1_000;
/// Upstream finality, far above `FIRST`: the catch-up acquires blocks that are
/// already irreversible, so nothing but the report is missing.
const FINALIZED: u64 = 1_500;
const HEAD: u64 = 1_600;

/// spec/15 — P-CACHE-SIZE. Above `HEAD - FINALIZED`, so a conforming run ends
/// inside the window with room to spare and cannot pass by luck.
const P_CACHE_SIZE: usize = 200;

/// spec/15 — P-SLO-FINALITY-LAG, still ⚠. A loose stand-in, four strides wide.
const P_SLO_FINALITY_LAG: u64 = 50;

/// Over the catch-up, under the ~40 s the rate-bound prober would have needed.
const BUDGET: Duration = Duration::from_secs(15);

fn upstream_chain() -> Chain {
    Chain::linear(FIRST, HEAD - FIRST + 1, 0)
}

fn source_over_with_ttl(upstream: &Upstream, ttl: Duration) -> EvmRpcDataSource {
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: upstream.url().to_string(),
        capacity: 5,
        retry_attempts: 0,
        ..Default::default()
    }));
    EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            data_request: DataRequest::default(),
            rpc_options: RpcOptions {
                finalized_head_ttl: Some(ttl),
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

fn source_over(upstream: &Upstream) -> EvmRpcDataSource {
    // Falling this far behind finality takes an outage outlasting the default
    // TTL; the run states that timeline rather than sleeping through it.
    source_over_with_ttl(upstream, Duration::from_millis(50))
}

/// One gauge off the scrape surface: INV-4 is decided by what an observer sees.
fn gauge(metrics: &Metrics, name: &str) -> u64 {
    let text = metrics
        .get_single_metric_text(name)
        .unwrap_or_else(|| panic!("{name} is not exposed"));
    text.lines()
        .find(|line| line.starts_with(name))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("{name} carries no readable value in:\n{text}")) as u64
}

/// LIV-7's bound, decidable mid-catch-up: a watermark cannot name a block the
/// buffer does not hold, so finality is owed only up to the buffered head.
fn owed_finality(buffered_head: u64) -> u64 {
    buffered_head
        .min(FINALIZED)
        .saturating_sub(P_SLO_FINALITY_LAG)
}

/// What one head-stream run observed.
struct Run {
    delivered: usize,
    last_block: u64,
    /// Highest finality report the run was handed, if any.
    report: Option<u64>,
}

/// Open a fresh head stream at `from` and pull `want` blocks off it.
async fn drive_head_stream(source: &EvmRpcDataSource, from: u64, want: usize) -> Run {
    let mut stream = source.get_stream(StreamRequest {
        from,
        to: None,
        parent_hash: None,
    });

    let deadline = tokio::time::Instant::now() + BUDGET;
    let mut run = Run {
        delivered: 0,
        last_block: from,
        report: None,
    };

    while run.delivered < want {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_elapsed) => panic!(
                "the head stream from {from} delivered {} of {want} blocks in {BUDGET:?}",
                run.delivered
            ),
            Ok(None) => panic!(
                "the head stream from {from} ended after {} blocks",
                run.delivered
            ),
            Ok(Some(Err(e))) => panic!("the head stream from {from} failed: {e}"),
            Ok(Some(Ok(batch))) => {
                if let Some(last) = batch.blocks.last() {
                    run.last_block = last.number;
                    run.delivered += batch.blocks.len();
                }
                if let Some(head) = batch.finalized_head {
                    run.report = Some(run.report.map_or(head.number, |seen| seen.max(head.number)));
                }
            }
        }
    }
    run
}

/// WP-11.6: batches are the adapter's only channel for finality, so a catch-up
/// that carries no report pins the consumer's watermark where the session began.
#[tokio::test]
async fn head_stream_carries_finality_while_catching_up() {
    let upstream = Upstream::start(upstream_chain()).await;
    upstream.set_finalized(FINALIZED);

    // Past any startup transient, still well below FINALIZED.
    const WANT: usize = 300;

    let source = source_over(&upstream);
    let run = drive_head_stream(&source, FIRST, WANT).await;
    let (delivered, last_block) = (run.delivered, run.last_block);

    let carried = run.report.unwrap_or_else(|| {
        panic!("catch-up delivered {delivered} blocks up to {last_block} without a single finality report")
    });
    let owed = owed_finality(last_block);
    assert!(
        carried >= owed,
        "finality report trails the upstream head: carried {carried}, owed at least {owed} \
         after delivering {delivered} blocks up to {last_block} (upstream finalized {FINALIZED})"
    );
}

/// WP-20/ADR-14: a re-INIT may seed *below* the previous epoch, since upstream
/// finality is permitted to oscillate (FM-26). An observation that outlived its
/// epoch reports above the fresh buffer, finalizing and evicting what upstream
/// no longer calls final — and INV-11 then makes the next reorg FM-30.
#[tokio::test]
async fn a_reseed_below_the_previous_epoch_discards_its_finality_view() {
    /// Above `FIRST`, so stale view and fresh are both plausible stride bounds.
    const RESEED: u64 = 1_200;
    const WANT: usize = 50;

    let upstream = Upstream::start(upstream_chain()).await;
    upstream.set_finalized(FINALIZED);

    // Long enough that only an authoritative read can displace the observation:
    // what this pins is the epoch rule, not the cache ageing out.
    let source = source_over_with_ttl(&upstream, Duration::from_secs(60));

    let epoch1 = drive_head_stream(&source, FIRST, WANT).await;
    assert_eq!(
        epoch1.report,
        Some(FINALIZED),
        "the run must observe upstream finality before the regression, or it pins nothing"
    );

    // Upstream finality regresses and T1 re-seeds the next epoch on it.
    upstream.set_finalized(RESEED);
    let seed = source
        .get_finalized_head()
        .await
        .expect("T1 reads the upstream finalized head");
    assert_eq!(seed.number, RESEED, "the stub must answer the regression");

    let epoch2 = drive_head_stream(&source, RESEED + 1, WANT).await;
    assert!(
        epoch2.report.is_none_or(|carried| carried <= RESEED),
        "the new epoch was handed {:?}, above its own seed at {RESEED} — the finality view \
         of the epoch that ended crossed the re-seed",
        epoch2.report
    );
}

/// INV-4 + LIV-11: eviction is a function of the watermark, so a watermark that
/// cannot keep up is a buffer that cannot stay in its window.
#[tokio::test]
async fn catch_up_keeps_the_buffer_within_the_window() {
    let upstream = Upstream::start(upstream_chain()).await;

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let svc = Arc::new(DataService::new(
        source_over(&upstream),
        P_CACHE_SIZE,
        // ADR-9's default. With auto-adjust on, the bound holds by force-advance
        // and says nothing about tracking.
        false,
        cancel_rx,
    ));

    // T1 seeds at the upstream finalized head, which is FIRST until it moves.
    svc.init().await.expect("T1 seeds from the upstream");
    upstream.set_finalized(FINALIZED);

    let runner = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run().await })
    };

    let caught_up = tokio::time::timeout(BUDGET, async {
        while svc.get_head().number < HEAD {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    // Gauges are published after compaction; let that land before scraping.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let stored = gauge(&svc.metrics, "sqd_hotblocks_stored_blocks");
    let finalized = gauge(&svc.metrics, "sqd_hotblocks_finalized_block");
    let head = gauge(&svc.metrics, "sqd_hotblocks_last_block");

    cancel_tx.send(true).ok();
    runner.abort();

    assert!(
        caught_up.is_ok(),
        "ingestion reached only {head} of {HEAD} in {BUDGET:?} (stored {stored}, finalized {finalized})"
    );

    let owed = owed_finality(head);
    assert!(
        finalized >= owed,
        "watermark trails upstream: finalized {finalized}, owed at least {owed} at head {head}"
    );
    assert!(
        stored as usize <= P_CACHE_SIZE,
        "buffer holds {stored} blocks against a window of {P_CACHE_SIZE} \
         (head {head}, finalized {finalized}) — eviction starved by the lagging watermark"
    );
}
