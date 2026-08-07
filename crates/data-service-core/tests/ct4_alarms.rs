//! CT-4 — alarm conformance (GAP-4).
//!
//! Every condition this suite calls an alarm has to be visible on the scrape
//! surface as a level or a counter, not only in the log stream (INV-31). Each
//! case induces one condition and reads it back off `/metrics`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{block, linked, scrape_until, Scrape, Scripted, Step};
use data_service_core::metrics::Metrics;
use data_service_core::service::RunEnd;
use data_service_core::types::BlockRef;
use data_service_core::{run_data_service, DataServiceOptions};
use tokio::sync::Notify;

/// Away from zero, so a height read off the scrape is unmistakably a height.
const SEED: u64 = 100;

fn seed() -> data_service_core::types::Block {
    block(SEED, "h100", SEED, "h100")
}

struct Service {
    handle: data_service_core::DataServiceHandle,
}

impl Service {
    async fn start(source: Scripted, cache: usize, auto_adjust: bool) -> Service {
        Service::start_with(source, cache, auto_adjust, None).await
    }

    async fn start_with(
        source: Scripted,
        cache: usize,
        auto_adjust: bool,
        metrics: Option<Arc<Metrics>>,
    ) -> Service {
        let handle = run_data_service(DataServiceOptions {
            source,
            block_cache_size: cache,
            port: 0,
            auto_adjust_finalized_head: auto_adjust,
            metrics,
        })
        .await
        .expect("service starts");
        Service { handle }
    }

    fn port(&self) -> u16 {
        self.handle.port
    }

    async fn until(&self, what: &str, done: impl Fn(&Scrape) -> bool) -> Scrape {
        scrape_until(self.port(), what, done).await
    }

    async fn stop(self) {
        self.handle.shutdown().await;
    }
}

// ─── OB-6 retention ───────────────────────────────────────────────────────────

/// INV-4 / LIV-11: while finality lags the buffer past its window the alarm is
/// a level with a magnitude, and the commit that finalizes past the excess
/// clears both.
#[tokio::test]
async fn over_window_is_a_level_with_a_magnitude_that_clears() {
    let gate = Arc::new(Notify::new());
    let grown: Vec<_> = (SEED + 1..=SEED + 5).map(linked).collect();
    let release = linked(SEED + 6);
    let source = Scripted::new(
        seed(),
        vec![
            // Nothing carries a finality report, so the watermark stays at the
            // seed and compaction can trim nothing.
            Step::batch(grown.clone()),
            Step::Gate(Arc::clone(&gate)),
            Step::reporting(vec![release], &grown[4]),
        ],
    );
    let service = Service::start(source, 3, false).await;

    let raised = service
        .until("the over-window alarm", |s| {
            s.get("sqd_hotblocks_over_window") == 1.0
        })
        .await;
    // 6 buffered against a window of 3.
    assert_eq!(raised.get("sqd_hotblocks_window_excess"), 3.0);
    assert_eq!(raised.get("sqd_hotblocks_stored_blocks"), 6.0);

    gate.notify_one();
    let cleared = service
        .until("the over-window alarm clearing", |s| {
            s.get("sqd_hotblocks_over_window") == 0.0
        })
        .await;
    assert_eq!(cleared.get("sqd_hotblocks_window_excess"), 0.0);
    assert_eq!(cleared.get("sqd_hotblocks_stored_blocks"), 3.0);

    service.stop().await;
}

/// WP-24: a force advance is an event, counted, naming the height it moved the
/// watermark past — and it keeps the buffer inside the window, so the standing
/// over-window level stays down.
#[tokio::test]
async fn force_advance_is_counted_with_the_height_it_passed() {
    let source = Scripted::new(
        seed(),
        vec![Step::batch((SEED + 1..=SEED + 4).map(linked).collect())],
    );
    let service = Service::start(source, 3, true).await;

    let advanced = service
        .until("a force advance", |s| {
            s.get("sqd_hotblocks_force_advances_total") >= 1.0
        })
        .await;
    assert_eq!(advanced.get("sqd_hotblocks_force_advanced_past"), 101.0);
    assert_eq!(advanced.get("sqd_hotblocks_over_window"), 0.0);

    service.stop().await;
}

// ─── OB-7 ingestion ───────────────────────────────────────────────────────────

/// WP-5: the violating batch is rejected whole and counted, and the session it
/// tore down is counted under the cause that ended it.
#[tokio::test]
async fn integrity_violations_are_counted_and_the_buffer_survives() {
    let head = linked(SEED + 1);
    // Same ref under a second ancestry — one ref, two parents (WP-6).
    let equivocation = block(SEED + 1, "h101", SEED, "hOTHER");
    let source = Scripted::new(
        seed(),
        vec![Step::batch(vec![head]), Step::batch(vec![equivocation])],
    );
    let service = Service::start(source, 100, false).await;

    let alarmed = service
        .until("an integrity violation", |s| {
            s.get("sqd_hotblocks_integrity_violations_total") >= 1.0
        })
        .await;
    assert_eq!(
        alarmed.get("sqd_hotblocks_session_restarts_total{cause=\"error\"}"),
        1.0,
        "the torn-down session is counted under the cause that ended it"
    );
    // INV-41: the buffer is intact and still serving what it held.
    assert_eq!(alarmed.get("sqd_hotblocks_last_block"), 101.0);
    assert_eq!(alarmed.get("sqd_hotblocks_terminal_state"), 0.0);

    service.stop().await;
}

/// T6: a rebase is its own event, distinct from the error restarts beside it.
#[tokio::test]
async fn fork_rebases_are_counted_apart_from_error_restarts() {
    let head = linked(SEED + 1);
    let source = Scripted::new(
        seed(),
        vec![
            Step::batch(vec![head.clone(), linked(SEED + 2)]),
            Step::Fork(vec![head.block_ref()]),
        ],
    );
    let service = Service::start(source, 100, false).await;

    let rebased = service
        .until("a fork rebase", |s| {
            s.get("sqd_hotblocks_fork_rebases_total") >= 1.0
        })
        .await;
    assert_eq!(
        rebased.get("sqd_hotblocks_session_restarts_total{cause=\"fork\"}"),
        1.0
    );
    assert_eq!(
        rebased.get("sqd_hotblocks_session_restarts_total{cause=\"error\"}"),
        0.0
    );

    service.stop().await;
}

/// FM-30: the terminal state is readable *before* the exit, which is the whole
/// point — an orchestrator that only sees the exit code learns nothing while
/// the process is still draining.
#[tokio::test]
async fn terminal_divergence_raises_a_level_before_the_exit() {
    let source = Scripted::new(
        seed(),
        vec![
            Step::batch(vec![linked(SEED + 1)]),
            // A ref below the finalized head that the buffer never held.
            Step::Fork(vec![BlockRef {
                number: SEED - 1,
                hash: "hGONE".into(),
            }]),
        ],
    );
    let mut service = Service::start(source, 100, false).await;

    service
        .until("the terminal level", |s| {
            s.get("sqd_hotblocks_terminal_state") == 1.0
        })
        .await;

    let end = tokio::time::timeout(Duration::from_secs(5), &mut service.handle.ended)
        .await
        .expect("the run ends")
        .expect("the run task is alive");
    assert!(matches!(end, RunEnd::Terminal(_)), "got {end:?}");

    // The fork that ends the run restarts nothing, so it is not a restart.
    let scrape = Scrape::read(service.port()).await;
    assert_eq!(
        scrape.get("sqd_hotblocks_session_restarts_total{cause=\"fork\"}"),
        0.0
    );
    assert_eq!(scrape.get("sqd_hotblocks_fork_rebases_total"), 0.0);

    service.stop().await;
}

/// WP-20: an epoch that reopens below the previous watermark is legal and
/// alarmed. Driven through `init` directly — reaching a T1 re-seed through the
/// ladder costs `P-STALL-REINIT` sessions of backoff.
#[tokio::test]
async fn a_watermark_regression_is_counted() {
    let source = Scripted::new(seed(), Vec::new());
    let metrics = Arc::new(Metrics::new());
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let service = data_service_core::DataService::with_metrics(
        source.clone(),
        10,
        false,
        cancel_rx,
        Arc::clone(&metrics),
    );

    service.init().await.expect("first epoch");
    assert_eq!(
        Scrape::parse(&metrics.gather_text().unwrap())
            .get("sqd_hotblocks_watermark_regressions_total"),
        0.0
    );

    source.reseed(block(SEED - 5, "h95", SEED - 6, "h94"));
    service
        .init()
        .await
        .expect("a lower epoch is legal, not refused");

    assert_eq!(
        Scrape::parse(&metrics.gather_text().unwrap())
            .get("sqd_hotblocks_watermark_regressions_total"),
        1.0
    );
}

// ─── OB-2 vs OB-4: LIV-2 ──────────────────────────────────────────────────────

/// LIV-2 is decided by the two together: an upstream that has not moved is idle
/// input however long it sits there, and only an upstream that ran ahead while
/// nothing committed is a stall.
#[tokio::test]
async fn the_stall_level_separates_idle_input_from_a_stalled_service() {
    let stall_after = Duration::from_millis(150);
    let metrics = Arc::new(Metrics::with_stall_alarm(stall_after));
    let source = Scripted::new(seed(), vec![Step::batch(vec![linked(SEED + 1)])]);
    let service = Service::start_with(source, 100, false, Some(Arc::clone(&metrics))).await;

    let committed = service
        .until("the first commit", |s| {
            s.get("sqd_hotblocks_last_block") == 101.0
        })
        .await;
    assert!(committed.get("sqd_hotblocks_commits_total") >= 1.0);

    // Idle input: the upstream is level with us and stays there.
    metrics.observe_upstream_head(101);
    tokio::time::sleep(stall_after * 3).await;
    let idle = Scrape::read(service.port()).await;
    assert_eq!(
        idle.get("sqd_hotblocks_stall_alarm"),
        0.0,
        "an upstream that has not advanced is idle input, not a stall"
    );

    // The bound starts when the upstream gets ahead, not at the last commit:
    // the first block after a long idle stretch must not alarm on arrival.
    metrics.observe_upstream_head(150);
    assert_eq!(
        Scrape::read(service.port())
            .await
            .get("sqd_hotblocks_stall_alarm"),
        0.0,
        "the idle stretch before the upstream moved must not count against the bound"
    );

    // Stalled service: the upstream stays ahead and nothing commits.
    let stalled = service
        .until("the stall alarm", |s| {
            s.get("sqd_hotblocks_stall_alarm") == 1.0
        })
        .await;
    assert_eq!(stalled.get("sqd_hotblocks_upstream_head"), 150.0);
    assert!(stalled.get("sqd_hotblocks_upstream_head_timestamp_ms") > 0.0);

    // And it clears on its own once the upstream is level again.
    metrics.observe_upstream_head(101);
    service
        .until("the stall alarm clearing", |s| {
            s.get("sqd_hotblocks_stall_alarm") == 0.0
        })
        .await;

    service.stop().await;
}

/// OB-2's actual obligation: the heartbeat moves for a batch that inserted
/// nothing. Watching the head height instead would read an all-duplicate
/// stretch as a dead service.
#[tokio::test]
async fn the_heartbeat_moves_on_a_batch_that_inserts_nothing() {
    let head = linked(SEED + 1);
    let source = Scripted::new(
        seed(),
        vec![
            Step::batch(vec![head.clone()]),
            Step::batch(vec![head.clone()]),
        ],
    );
    let service = Service::start(source, 100, false).await;

    let beating = service
        .until("two commits", |s| {
            s.get("sqd_hotblocks_commits_total") >= 2.0
        })
        .await;
    assert_eq!(
        beating.get("sqd_hotblocks_last_block"),
        101.0,
        "WP-6 makes the redelivery a no-op, so the head must not move"
    );
    assert!(beating.get("sqd_hotblocks_last_commit_timestamp_ms") > 0.0);

    service.stop().await;
}

/// OB-9: the lifecycle stages are levels, and one that has not happened reads
/// as absent rather than as the epoch.
#[tokio::test]
async fn lifecycle_timestamps_distinguish_unreached_from_zero() {
    let source = Scripted::new(seed(), vec![Step::batch(vec![linked(SEED + 1)])]);
    let service = Service::start(source, 100, false).await;

    let running = service
        .until("the first commit", |s| {
            s.get("sqd_hotblocks_first_commit_timestamp_ms") > 0.0
        })
        .await;
    assert!(running.get("sqd_hotblocks_process_start_timestamp_ms") > 0.0);
    assert!(
        running.get("sqd_hotblocks_first_acceptance_timestamp_ms") > 0.0,
        "the scrape itself is an accepted request"
    );
    assert_eq!(
        running.get("sqd_hotblocks_shutdown_start_timestamp_ms"),
        -1.0,
        "a stage that has not happened must not read as 1970"
    );

    service.stop().await;
}
