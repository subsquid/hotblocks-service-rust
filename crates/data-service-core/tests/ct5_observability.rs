//! CT-5 — metric surface conformance (GAP-24, OB-5, OB-8).
//!
//! INV-30 makes the scrape surface itself a contract: every registered series
//! must be one the suite requires, every label value must be enumerable before
//! the first event, and nothing may be exposed that cannot move.

mod common;

use common::{block, linked, scrape_until, Scrape, Scripted, Step};
use data_service_core::types::Block;
use data_service_core::{run_data_service, DataServiceHandle, DataServiceOptions};

const SEED: u64 = 100;

/// Everything IB-12 binds, and nothing else. A series added without a spec row
/// fails here, which is the only cheap guard against the next dead gauge.
const REQUIRED_SERIES: &[&str] = &[
    // OB-1 state
    "sqd_hotblocks_first_block",
    "sqd_hotblocks_last_block",
    "sqd_hotblocks_finalized_block",
    "sqd_hotblocks_stored_blocks",
    "sqd_hotblocks_window_excess",
    // OB-2 heartbeat
    "sqd_hotblocks_commits_total",
    "sqd_hotblocks_last_commit_timestamp_ms",
    // OB-3 lag
    "sqd_hotblocks_last_block_lag_ms",
    "sqd_hotblocks_block_lag_ms",
    // OB-4 upstream
    "sqd_hotblocks_upstream_head",
    "sqd_hotblocks_upstream_head_timestamp_ms",
    "sqd_hotblocks_upstream_finalized_head",
    "sqd_hotblocks_upstream_finalized_head_timestamp_ms",
    "sqd_hotblocks_upstream_requests_total",
    "sqd_hotblocks_upstream_calls_total",
    "sqd_hotblocks_upstream_errors_total",
    "sqd_hotblocks_upstream_retries_total",
    // OB-5 operations
    "sqd_hotblocks_processing_time_ms",
    "sqd_hotblocks_queries_total",
    "sqd_hotblocks_query_outcomes_total",
    "sqd_hotblocks_query_duration_ms",
    "sqd_hotblocks_response_truncations_total",
    // OB-6 retention
    "sqd_hotblocks_over_window",
    "sqd_hotblocks_force_advances_total",
    "sqd_hotblocks_force_advanced_past",
    // OB-7 ingestion
    "sqd_hotblocks_integrity_violations_total",
    "sqd_hotblocks_session_restarts_total",
    "sqd_hotblocks_acquisition_retry_exhaustions_total",
    "sqd_hotblocks_fork_rebases_total",
    "sqd_hotblocks_watermark_regressions_total",
    "sqd_hotblocks_terminal_state",
    "sqd_hotblocks_stall_alarm",
    // OB-9 lifecycle
    "sqd_hotblocks_process_start_timestamp_ms",
    "sqd_hotblocks_first_acceptance_timestamp_ms",
    "sqd_hotblocks_first_commit_timestamp_ms",
    "sqd_hotblocks_shutdown_start_timestamp_ms",
];

fn seed() -> Block {
    block(SEED, "h100", SEED - 1, "h99")
}

async fn start(source: Scripted) -> DataServiceHandle {
    run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
        metrics: None,
    })
    .await
    .expect("service starts")
}

async fn family_names(port: u16) -> Vec<String> {
    let text = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await
        .expect("scrape")
        .text()
        .await
        .expect("scrape body");
    let mut names: Vec<String> = text
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect();
    names.sort();
    names.dedup();
    names
}

/// ADR-3 left the predecessor's worker gauge naming something this build has
/// none of. A level that cannot move is worse than an absent one: it reads as
/// a healthy zero (INV-30).
#[tokio::test]
async fn the_surface_is_exactly_what_the_spec_binds() {
    let handle = start(Scripted::new(seed(), Vec::new())).await;
    let exposed = family_names(handle.port).await;

    let mut required: Vec<String> = REQUIRED_SERIES.iter().map(|s| s.to_string()).collect();
    required.sort();
    assert_eq!(exposed, required, "the scrape surface drifted from IB-12");
    assert!(!exposed.iter().any(|name| name.contains("active_workers")));

    handle.shutdown().await;
}

/// OB-8: a label value that first appears when its event does is a series an
/// alert cannot be written against beforehand.
#[tokio::test]
async fn every_label_value_is_registered_before_its_first_event() {
    let handle = start(Scripted::new(seed(), Vec::new())).await;
    let scrape = Scrape::read(handle.port).await;

    let closed: &[(&str, &str, &[&str])] = &[
        (
            "sqd_hotblocks_query_outcomes_total",
            "class",
            &["window", "wait_empty", "backfill", "conflict", "error"],
        ),
        (
            "sqd_hotblocks_response_truncations_total",
            "cause",
            &["budget", "error", "disconnect"],
        ),
        (
            "sqd_hotblocks_session_restarts_total",
            "cause",
            &["error", "fork", "reset"],
        ),
        (
            "sqd_hotblocks_upstream_requests_total",
            "kind",
            &["single", "batch"],
        ),
        (
            "sqd_hotblocks_upstream_errors_total",
            "class",
            &[
                "rpc",
                "http",
                "connection",
                "disconnected",
                "timeout",
                "protocol",
                "retry_requested",
            ],
        ),
        (
            "sqd_hotblocks_upstream_retries_total",
            "class",
            &[
                "rpc",
                "http",
                "connection",
                "disconnected",
                "timeout",
                "protocol",
                "retry_requested",
            ],
        ),
    ];

    for (series, label, values) in closed {
        for value in *values {
            let key = format!("{series}{{{label}=\"{value}\"}}");
            assert_eq!(
                scrape.get(&key),
                0.0,
                "{key} was not pre-registered at zero"
            );
        }
        let exposed = scrape
            .names()
            .filter(|name| name.starts_with(&format!("{series}{{")))
            .count();
        assert_eq!(
            exposed,
            values.len(),
            "{series} exposes label values outside its closed set"
        );
    }

    handle.shutdown().await;
}

/// OB-5 splits the outcomes the predecessor's three-valued counter conflated,
/// while that counter keeps its own meaning for the migration (REQ-24).
#[tokio::test]
async fn query_outcomes_are_classified_per_ob5() {
    let source = Scripted::new(seed(), vec![Step::batch(vec![linked(SEED + 1)])])
        .with_backfill(vec![Ok((SEED - 5..SEED).map(linked).collect())]);
    let handle = start(source).await;
    let port = handle.port;
    scrape_until(port, "the first commit", |s| {
        s.get("sqd_hotblocks_last_block") == 101.0
    })
    .await;

    let client = reqwest::Client::new();
    let stream = |body: String| {
        let client = client.clone();
        async move {
            client
                .post(format!("http://127.0.0.1:{port}/stream"))
                .header("accept-encoding", "zstd")
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .expect("request is admitted")
                .status()
                .as_u16()
        }
    };

    assert_eq!(stream(r#"{"fromBlock": 100}"#.into()).await, 200);
    assert_eq!(stream(r#"{"fromBlock": 95}"#.into()).await, 200);
    assert_eq!(
        stream(r#"{"fromBlock": 102, "parentBlockHash": "hWRONG"}"#.into()).await,
        409
    );
    // Nothing above the head arrives within the wait, so the empty form.
    assert_eq!(stream(r#"{"fromBlock": 200}"#.into()).await, 204);

    let scrape = Scrape::read(port).await;
    let outcome = |class: &str| {
        scrape.get(&format!(
            "sqd_hotblocks_query_outcomes_total{{class=\"{class}\"}}"
        ))
    };
    assert_eq!(outcome("window"), 1.0);
    assert_eq!(outcome("backfill"), 1.0);
    assert_eq!(outcome("conflict"), 1.0);
    assert_eq!(outcome("wait_empty"), 1.0);
    assert_eq!(outcome("error"), 0.0);

    // The predecessor's series stays three-valued and keeps its own split.
    assert_eq!(
        scrape.get("sqd_hotblocks_queries_total{type=\"cache\"}"),
        2.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_queries_total{type=\"backfill\"}"),
        1.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_queries_total{type=\"error\"}"),
        1.0
    );

    // Every class carries a duration distribution beside its count (OB-5).
    for class in ["window", "backfill", "conflict", "wait_empty"] {
        assert_eq!(
            scrape.get(&format!(
                "sqd_hotblocks_query_duration_ms_count{{class=\"{class}\"}}"
            )),
            1.0
        );
    }

    handle.shutdown().await;
}

/// OB-9's shutdown stage only becomes readable once the drain begins.
#[tokio::test]
async fn shutdown_start_is_stamped_by_the_drain() {
    let handle = start(Scripted::new(seed(), Vec::new())).await;
    let metrics = std::sync::Arc::clone(&handle.metrics);
    assert_eq!(
        Scrape::read(handle.port)
            .await
            .get("sqd_hotblocks_shutdown_start_timestamp_ms"),
        -1.0
    );

    handle.shutdown().await;
    let after = Scrape::parse(&metrics.gather_text().expect("gather"));
    assert!(after.get("sqd_hotblocks_shutdown_start_timestamp_ms") > 0.0);
}
