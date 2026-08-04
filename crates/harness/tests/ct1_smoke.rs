//! CT-1 smoke (Phase 0 exit criterion): a happy-path history driven through
//! the SUT and the reference model, every read compared.
//!
//! Covers: INV-1..3 (model asserts per applied event), INV-20/23 (validated
//! DATA response), INV-22 shape (conflict at head and in-window), INV-24
//! (watermarks vs model), INV-25 partially (served bytes == ledger bytes on
//! the passthrough encoding), RP-12 empty form.

use data_service_core::{run_data_service, DataServiceOptions};
use harness::client::{Client, StreamResponse};
use harness::model::QueryVerdict;
use harness::sim::{SimChain, SimSource};
use harness::validators as v;
use std::time::Duration;

fn stream_headers(resp: &StreamResponse) -> v::WireHeaders<'_> {
    v::WireHeaders {
        content_type: resp.content_type.as_deref(),
        content_encoding: resp.content_encoding.as_deref(),
        vary: resp.vary.as_deref(),
        finalized_number: resp.finalized_number.as_deref(),
        finalized_hash: resp.finalized_hash.as_deref(),
    }
}

const SEED: u64 = 0xC0FFEE;
const CHAIN_LEN: u64 = 60;
const INIT_FINALIZED: u64 = 50;
const BATCH_SIZE: usize = 3;
const FIN_LAG: u64 = 5;

async fn wait_until<F: Fn(&data_service_core::types::BlockRef) -> bool>(
    client: &Client,
    pred: F,
    what: &str,
) -> data_service_core::types::BlockRef {
    for _ in 0..200 {
        if let Ok(h) = client.head().await {
            if pred(&h) {
                return h;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn ct1_happy_path_matches_reference_model() {
    let sim = SimChain::generate(SEED, CHAIN_LEN);
    let source = SimSource::new(&sim, BATCH_SIZE, FIN_LAG, INIT_FINALIZED);
    let ledger = source.ledger();

    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let client = Client::new(handle.port);

    // Quiescence: the SUT reaches the simulator's chain tip.
    let tip = sim.block_ref(CHAIN_LEN - 1);
    let head = wait_until(&client, |h| *h == tip, "SUT to reach the chain tip").await;

    // Replay the ledger — exactly what the simulator delivered, preserving
    // every model-relevant linkage field — through the reference model.
    // INV-1..3 are asserted inside every apply.
    let model = ledger.lock().unwrap().replay(1000, false);

    // Watermarks match the model (INV-24).
    assert_eq!(head, model.head());
    assert_eq!(client.finalized_head().await.unwrap(), model.finalized());
    assert_eq!(model.finalized(), sim.block_ref(CHAIN_LEN - 1 - FIN_LAG));
    v::validate_watermark_bounds(&model.finalized(), &head).unwrap();

    // DATA: stream from just above the seed, correct base (INV-20/23/25).
    let from = INIT_FINALIZED + 1;
    let resp = client
        .stream(from, Some(&sim.block_ref(INIT_FINALIZED).hash), "zstd")
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content_encoding.as_deref(), Some("zstd"));
    let wm = v::validate_data_headers(&stream_headers(&resp), "zstd").unwrap();
    assert_eq!(wm, model.finalized());
    v::validate_watermark_bounds(&wm, &head).unwrap();
    // REQ-6/IB-2: independent per-block frames, each a safe truncation point.
    let framed = v::validate_framing(&resp.body, "zstd").unwrap();
    let text = v::decode_body(&resp.body, "zstd").unwrap();
    assert_eq!(framed.concat(), text);
    let records = {
        let lg = ledger.lock().unwrap();
        // The SUT is quiesced at the tip, so the model's chain is the snapshot
        // the response must answer from: coverage start and branch are judged
        // against it, byte provenance against the ledger.
        v::validate_data(
            &text,
            from,
            v::sim_extract,
            &v::DataOracle {
                ledger: Some(&lg),
                eligible: Some(model.blocks()),
            },
        )
        .unwrap()
    };
    // Contiguous-height family: coverage starts exactly at `from` (DEF-30).
    assert_eq!(records[0].number, from);
    // Free variable 1: any prefix is valid; the model gives full coverage.
    match model.query(from, Some(&sim.block_ref(INIT_FINALIZED).hash)) {
        QueryVerdict::Data { blocks, .. } => {
            assert!(records.len() <= blocks.len());
            for (got, want) in records.iter().zip(&blocks) {
                assert_eq!(
                    (got.number, got.hash.as_str()),
                    (want.number, want.hash.as_str())
                );
            }
        }
        other => panic!("model disagrees: {other:?}"),
    }

    // CONFLICT at head+1 with a wrong base (INV-22 shape, immediate).
    let resp = client
        .stream(head.number + 1, Some("0xdeadbeef"), "zstd")
        .await
        .unwrap();
    assert_eq!(resp.status, 409);
    assert_eq!(
        model.query(head.number + 1, Some("0xdeadbeef")),
        QueryVerdict::Conflict
    );
    assert_eq!(
        v::validate_error(resp.status, &stream_headers(&resp), &resp.body).unwrap(),
        v::ErrorClass::Conflict
    );
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let refs = v::validate_conflict(&body).unwrap();
    for r in &refs {
        assert!(
            model.is_known_ref(r),
            "conflict ref {r:?} unknown to the model"
        );
    }

    // CONFLICT in-window with a wrong base. Ref count and selection are free
    // variable 2 — only shape (validate_conflict) and membership are checked.
    let mid = INIT_FINALIZED + 5;
    let resp = client
        .stream(mid, Some("0xdeadbeef"), "zstd")
        .await
        .unwrap();
    assert_eq!(resp.status, 409);
    assert_eq!(model.query(mid, Some("0xdeadbeef")), QueryVerdict::Conflict);
    assert_eq!(
        v::validate_error(resp.status, &stream_headers(&resp), &resp.body).unwrap(),
        v::ErrorClass::Conflict
    );
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let refs = v::validate_conflict(&body).unwrap();
    for r in &refs {
        assert!(
            model.is_known_ref(r),
            "conflict ref {r:?} unknown to the model"
        );
    }

    // EMPTY: head+1 with the correct base → bounded wait, then the empty form
    // with watermarks (RP-4/RP-12).
    let resp = client
        .stream(head.number + 1, Some(&head.hash), "zstd")
        .await
        .unwrap();
    assert_eq!(resp.status, 204);
    // IB-5: the empty form carries watermarks — checked inside the validator.
    assert_eq!(
        v::validate_error(resp.status, &stream_headers(&resp), &resp.body).unwrap(),
        v::ErrorClass::Empty
    );
    let wm = v::validate_watermark(
        resp.finalized_number.as_deref(),
        resp.finalized_hash.as_deref(),
    )
    .unwrap();
    assert_eq!(wm, model.finalized());
    v::validate_watermark_bounds(&wm, &head).unwrap();
    assert!(resp.body.is_empty());
    assert_eq!(
        model.query(head.number + 1, Some(&head.hash)),
        QueryVerdict::Empty {
            head: model.head(),
            finalized: model.finalized()
        }
    );

    // RP-8 / FM-44: an ancient `from` is backfilled from the finalized stream
    // and spliced onto the snapshot. The eligible chain is that splice, so the
    // response must start at 0 and stay on it across the splice point.
    let resp = client.stream(0, None, "zstd").await.unwrap();
    assert_eq!(resp.status, 200);
    let wm = v::validate_data_headers(&stream_headers(&resp), "zstd").unwrap();
    assert_eq!(wm, model.finalized());
    // REQ-6 holds across the splice too: the backfill prefix is framed per
    // block, not re-framed as one stream (both encodings).
    let framed = v::validate_framing(&resp.body, "zstd").unwrap();
    let text = v::decode_body(&resp.body, "zstd").unwrap();
    assert_eq!(framed.concat(), text);
    let gz = client.stream(0, None, "identity").await.unwrap();
    let gz_wm = v::validate_data_headers(&stream_headers(&gz), "identity").unwrap();
    assert_eq!(gz_wm, wm);
    let gz_framed = v::validate_framing(&gz.body, "gzip").unwrap();
    let gz_text = v::decode_body(&gz.body, "gzip").unwrap();
    assert_eq!(gz_framed.concat(), gz_text);
    assert_eq!(gz_framed, framed, "REQ-6: backfill frame boundaries differ");
    assert_eq!(gz_text, text, "INV-25: backfill encodings differ");
    let eligible: Vec<_> = (0..CHAIN_LEN).map(|n| sim.header(n)).collect();
    let (records, gz_records) = {
        let lg = ledger.lock().unwrap();
        let oracle = v::DataOracle {
            ledger: Some(&lg),
            eligible: Some(&eligible),
        };
        let records = v::validate_data(&text, 0, v::sim_extract, &oracle).unwrap();
        let gz_records = v::validate_data(&gz_text, 0, v::sim_extract, &oracle).unwrap();
        (records, gz_records)
    };
    assert_eq!(gz_records, records);
    assert_eq!(records[0].number, 0);

    // The ledger now also holds the read-path backfill deliveries of the
    // `from = 0` query; replay must skip them (they are not ingest input) and
    // land on the same state regardless of when it runs.
    let model2 = ledger.lock().unwrap().replay(1000, false);
    assert_eq!(model2.head(), model.head());
    assert_eq!(model2.finalized(), model.finalized());

    // IB-1: wrong method on a known route. A transport outcome, not an RP-13
    // class — it must validate as the former and be rejected as the latter.
    let r = client.get_raw("/stream").await.unwrap();
    assert_eq!(r.status, 405);
    let rh = v::WireHeaders {
        content_type: r.content_type.as_deref(),
        ..v::WireHeaders::default()
    };
    assert_eq!(
        v::validate_outcome(r.status, &rh, &r.body).unwrap(),
        v::Outcome::MethodNotAllowed
    );
    assert!(v::validate_error(r.status, &rh, &r.body).is_err());

    // Unknown routes are a separate transport outcome: only 404 is bound,
    // while endpoint-level NOT_FOUND continues to require IB-7's text body.
    let r = client.get_raw("/unknown-route").await.unwrap();
    assert_eq!(
        v::validate_unknown_route(r.status).unwrap(),
        v::Outcome::UnknownRoute
    );

    // The other half of the split, driven against the service rather than
    // asserted in isolation: an unknown ingest-time height is RP-13's
    // NOT_FOUND and owes the diagnostic IB-7 binds.
    for path in ["/block-time/999999999", "/metrics/no-such-metric"] {
        let r = client.get_raw(path).await.unwrap();
        let rh = v::WireHeaders {
            content_type: r.content_type.as_deref(),
            ..v::WireHeaders::default()
        };
        assert_eq!(
            v::validate_error(r.status, &rh, &r.body).unwrap(),
            v::ErrorClass::NotFound,
            "{path}"
        );
    }

    // Readiness carries the exact text the binding pins (14 §operations).
    let r = client.get_raw("/readiness").await.unwrap();
    assert_eq!((r.status, r.body.as_ref()), (200, b"true".as_ref()));

    handle.shutdown().await;
}

#[tokio::test]
async fn ct1_gzip_and_zstd_decode_identically() {
    // REQ-6 acceptance: both negotiated encodings decode to identical bytes.
    let sim = SimChain::generate(SEED ^ 1, 30);
    let source = SimSource::new(&sim, 2, 3, 20);
    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: 1000,
        port: 0,
        auto_adjust_finalized_head: false,
    })
    .await
    .unwrap();
    let client = Client::new(handle.port);
    let tip = sim.block_ref(29);
    wait_until(&client, |h| *h == tip, "SUT to reach the chain tip").await;

    let base = sim.block_ref(20);
    let z = client.stream(21, Some(&base.hash), "zstd").await.unwrap();
    let g = client
        .stream(21, Some(&base.hash), "identity")
        .await
        .unwrap();
    assert_eq!(z.status, 200);
    assert_eq!(g.status, 200);
    assert_eq!(z.content_encoding.as_deref(), Some("zstd"));
    // No identity mode exists: anything without the zstd token gets gzip (IB-2).
    assert_eq!(g.content_encoding.as_deref(), Some("gzip"));
    v::validate_data_headers(&stream_headers(&z), "zstd").unwrap();
    v::validate_data_headers(&stream_headers(&g), "identity").unwrap();
    let zt = v::decode_body(&z.body, "zstd").unwrap();
    let gt = v::decode_body(&g.body, "gzip").unwrap();
    assert_eq!(zt, gt, "REQ-6: encodings decode to different bytes");
    // Both encodings frame per block: zstd passes stored frames through, gzip
    // re-encodes each block as its own member (IB-2).
    let zf = v::validate_framing(&z.body, "zstd").unwrap();
    let gf = v::validate_framing(&g.body, "gzip").unwrap();
    assert_eq!(zf, gf, "REQ-6: frame boundaries differ between encodings");
    v::validate_data(&zt, 21, v::sim_extract, &v::DataOracle::default()).unwrap();

    handle.shutdown().await;
}
