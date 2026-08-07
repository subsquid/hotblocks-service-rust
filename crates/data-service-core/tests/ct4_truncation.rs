//! CT-4 — truncation after a prefix (GAP-22).
//!
//! Once the first record is on the wire the outcome can no longer be signalled
//! in band, so a backfill that fails mid-response ends at a record boundary and
//! the failure is alarmed server-side instead (RP-12, ADR-10, INV-27).

mod common;

use common::{block, linked, scrape_until, Scrape, Scripted};
use data_service_core::types::Block;
use data_service_core::{run_data_service, DataServiceOptions};

/// The buffer's first block is not a root, so heights below it are a window
/// underflow the service must acquire (RP-8).
const FIRST: u64 = 100;
const BACKFILL_FROM: u64 = 95;

fn seed() -> Block {
    block(FIRST, "h100", FIRST - 1, "h99")
}

/// Serve two blocks, then fail; return the decoded response and a scrape.
async fn backfill_ending_in(third: Result<Vec<Block>, String>) -> (u16, Vec<String>, Scrape) {
    let source = Scripted::new(seed(), Vec::new()).with_backfill(vec![
        Ok(vec![linked(BACKFILL_FROM)]),
        Ok(vec![linked(BACKFILL_FROM + 1)]),
        third,
    ]);
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
        metrics: None,
    })
    .await
    .expect("service starts");
    let port = handle.port;

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(format!(r#"{{"fromBlock": {BACKFILL_FROM}}}"#))
        .send()
        .await
        .expect("the backfill request is admitted");

    let status = response.status().as_u16();
    let body = response.bytes().await.expect("read the body to its end");

    // A cut inside a frame would not decode at all; a cut between them does.
    let decoded = zstd::decode_all(std::io::Cursor::new(&body)).expect("body decodes whole");
    assert!(
        decoded.is_empty() || decoded.ends_with(b"\n"),
        "the body ended mid-record"
    );
    let records = String::from_utf8(decoded)
        .expect("records are UTF-8")
        .lines()
        .map(str::to_string)
        .collect();

    let scrape = scrape_until(port, "a truncation", |s| {
        s.get("sqd_hotblocks_response_truncations_total{cause=\"error\"}") >= 1.0
    })
    .await;

    handle.shutdown().await;
    (status, records, scrape)
}

fn assert_prefix_delivered(status: u16, records: &[String]) {
    assert_eq!(status, 200, "the prefix already sent is valid data");
    assert_eq!(
        records.len(),
        2,
        "the response must carry the complete prefix below the fault, got {records:?}"
    );
    for (index, record) in records.iter().enumerate() {
        let value: serde_json::Value =
            serde_json::from_str(record).expect("every delivered record is whole");
        assert_eq!(value["number"], BACKFILL_FROM + index as u64);
    }
}

/// The acquisition gives up after a prefix — reachable since bounded retry
/// replaced the hang that used to hold the request open forever.
#[tokio::test]
async fn acquisition_failure_after_a_prefix_truncates_and_alarms() {
    let (status, records, scrape) =
        backfill_ending_in(Err("failed to acquire block 97 after 10 retries".into())).await;

    assert_prefix_delivered(status, &records);
    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"error\"}"),
        1.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"backfill\"}"),
        1.0
    );
}

/// The same contract for a source that breaks continuity mid-splice. This used
/// to be an assertion inside the response task: the body ended identically and
/// nothing on the scrape surface said why.
#[tokio::test]
async fn continuity_failure_after_a_prefix_truncates_and_alarms() {
    let broken = block(BACKFILL_FROM + 2, "h97", BACKFILL_FROM + 1, "hWRONG");
    let (status, records, scrape) = backfill_ending_in(Ok(vec![broken])).await;

    assert_prefix_delivered(status, &records);
    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"error\"}"),
        1.0
    );
}

/// RP-13 draws the line at the first record: a failure before it is still
/// sayable in band, so it owes INTERNAL rather than a 200 that decodes to
/// nothing and reads as RP-12's empty form.
#[tokio::test]
async fn failure_before_the_first_record_is_internal_not_a_truncation() {
    // The break is in the first batch, so it lands before anything is sent.
    let broken = block(BACKFILL_FROM, "h95", BACKFILL_FROM - 1, "hWRONG");
    let source = Scripted::new(seed(), Vec::new()).with_backfill(vec![Ok(vec![broken])]);
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 100,
        port: 0,
        auto_adjust_finalized_head: false,
        metrics: None,
    })
    .await
    .expect("service starts");
    let port = handle.port;

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/stream"))
        .header("accept-encoding", "zstd")
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"fromBlock": {BACKFILL_FROM}, "parentBlockHash": "h94"}}"#
        ))
        .send()
        .await
        .expect("the backfill request is admitted");
    assert_eq!(response.status().as_u16(), 500);

    let scrape = Scrape::read(port).await;
    for cause in ["budget", "error", "disconnect"] {
        assert_eq!(
            scrape.get(&format!(
                "sqd_hotblocks_response_truncations_total{{cause=\"{cause}\"}}"
            )),
            0.0,
            "nothing was sent, so nothing was truncated"
        );
    }
    // The outcome settles where the request ends, so a 500 is not also counted
    // as the backfill it would have been.
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"error\"}"),
        1.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_query_outcomes_total{class=\"backfill\"}"),
        0.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_queries_total{type=\"error\"}"),
        1.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_queries_total{type=\"backfill\"}"),
        0.0
    );

    handle.shutdown().await;
}

/// The causes are separate series, so a routing rule can page on an internal
/// failure without also firing for every client that hung up (RP-12).
#[tokio::test]
async fn the_untriggered_causes_stay_at_zero() {
    let (_, _, scrape) = backfill_ending_in(Err("gave up".into())).await;

    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"budget\"}"),
        0.0
    );
    assert_eq!(
        scrape.get("sqd_hotblocks_response_truncations_total{cause=\"disconnect\"}"),
        0.0
    );
}
