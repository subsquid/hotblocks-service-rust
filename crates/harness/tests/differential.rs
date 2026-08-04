//! SUT-vs-model differentials over hand-written pathological histories.
//!
//! CT-1 compares the two only on a happy path, so defects that surface while
//! rejecting bad input stay invisible there. Every expectation below comes
//! from the reference model, never from a recorded run.

use data_service_core::service::DataService;
use harness::model::{ApplyOutcome, ModelBlock};
use harness::script::{mb, replay, Event, ScriptedSource};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const CACHE_SIZE: usize = 10;

/// What the scripted source has done so far, so a test names the exact point
/// in the history it compares at.
struct Progress {
    /// Head-stream opens: > 1 means a session was torn down and retried.
    stream_calls: Arc<AtomicUsize>,
    /// Events the SUT has finished applying.
    delivered: Arc<AtomicUsize>,
}

impl Progress {
    fn stream_calls(&self) -> usize {
        self.stream_calls.load(Ordering::SeqCst)
    }
    fn delivered(&self) -> usize {
        self.delivered.load(Ordering::SeqCst)
    }
}

/// The SUT state a differential compares. The buffer itself is in it because
/// head and watermark are not enough: a substitution under an unchanged ref
/// (DEF-8) moves neither.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    head: data_service_core::types::BlockRef,
    finalized: data_service_core::types::BlockRef,
    buffer: Vec<ModelBlock>,
}

impl Observed {
    fn of_model(m: &harness::model::RefModel) -> Self {
        Observed {
            head: m.head(),
            finalized: m.finalized(),
            buffer: m.blocks().to_vec(),
        }
    }
}

/// Drive `events` through the SUT, wait for the condition the caller is
/// after, and read back everything the model can be compared against.
async fn drive_sut(
    seed: &harness::model::ModelBlock,
    events: Vec<Event>,
    settled: impl Fn(&Progress) -> bool,
) -> Observed {
    let source = ScriptedSource::new(seed, events);
    let progress = Progress {
        stream_calls: source.stream_calls(),
        delivered: source.delivered(),
    };
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let svc = Arc::new(DataService::new(source, CACHE_SIZE, false, cancel_rx));
    svc.init().await.unwrap();

    let runner = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run().await })
    };

    tokio::time::timeout(Duration::from_secs(2), async {
        while !settled(&progress) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the SUT never reached the state this test compares at");

    let tail = svc
        .query(seed.number, None)
        .await
        .expect("the buffer must stay servable")
        .tail
        .expect("a window query returns the snapshot");
    let observed = Observed {
        head: svc.get_head(),
        finalized: svc.get_finalized_head(),
        buffer: tail
            .iter()
            .map(|b| ModelBlock {
                number: b.number,
                hash: b.hash.clone(),
                parent_number: b.parent_number,
                parent_hash: b.parent_hash.clone(),
            })
            .collect(),
    };

    cancel_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("ingestion run must stop")
        .expect("ingestion task must not panic");

    observed
}

/// WP-5: a finality report the arriving blocks contradict rejects its batch
/// whole. Committing it would make the contradicted block the base every
/// later block descends from.
#[tokio::test]
async fn a_contradicted_finality_report_rejects_its_batch() {
    let seed = mb(1, "h1", 0, "h0");
    let events = vec![
        Event::blocks(vec![mb(2, "h2", 1, "h1")]).with_finality(
            data_service_core::types::BlockRef {
                number: 4,
                hash: "h4".into(),
            },
        ),
        Event::blocks(vec![mb(3, "h3", 2, "h2")]),
        // Height 4 is not what the obligation named.
        Event::blocks(vec![mb(4, "h4-forged", 3, "h3")]),
    ];

    let (model, stopped) = replay(&seed, &events, CACHE_SIZE, false);
    assert!(
        matches!(stopped, ApplyOutcome::IntegrityViolation(_)),
        "the history must violate integrity in the model, got {stopped:?}"
    );

    // The session dies and the ladder re-opens the stream.
    let observed = drive_sut(&seed, events, |p| p.stream_calls() >= 2).await;

    assert_eq!(
        observed,
        Observed::of_model(&model),
        "the SUT kept a block the model rejected"
    );
}

/// WP-14/INV-41: no input content ends the process. A block whose parent link
/// contradicts the buffer is rejected, the session restarts, and readers keep
/// being served from a buffer the bad batch never touched.
#[tokio::test]
async fn a_contradicting_parent_link_rejects_its_batch() {
    let seed = mb(1, "h1", 0, "h0");
    let events = vec![
        Event::blocks(vec![mb(2, "h2", 1, "h1")]),
        // Names height 2 as its parent, but under a hash the buffer does not
        // hold — neither an append nor a reorg the buffer can honour.
        Event::blocks(vec![mb(3, "h3", 2, "forged")]),
    ];

    let (model, stopped) = replay(&seed, &events, CACHE_SIZE, false);
    assert!(
        matches!(stopped, ApplyOutcome::IntegrityViolation(_)),
        "the history must violate integrity in the model, got {stopped:?}"
    );

    let observed = drive_sut(&seed, events, |p| p.stream_calls() >= 2).await;

    assert_eq!(
        observed,
        Observed::of_model(&model),
        "the SUT kept a block the model rejected"
    );
}

/// DEF-2/IB-4: hashes are non-empty and representable as HTTP field values.
/// The whole batch is rejected before either an empty or header-unsafe value
/// can become servable or finality metadata.
#[tokio::test]
async fn an_invalid_hash_rejects_its_batch() {
    let seed = mb(1, "h1", 0, "h0");
    let good = mb(2, "h2", 1, "h1");
    for (case, event) in [
        ("empty block hash", Event::blocks(vec![mb(3, "", 2, "h2")])),
        ("empty parent hash", Event::blocks(vec![mb(3, "h3", 2, "")])),
        (
            "empty finality hash",
            Event::blocks(vec![mb(3, "h3", 2, "h2")]).with_finality(
                data_service_core::types::BlockRef {
                    number: 9,
                    hash: String::new(),
                },
            ),
        ),
        (
            "header-unsafe block hash",
            Event::blocks(vec![mb(3, "bad\nhash", 2, "h2")]),
        ),
        (
            "header-unsafe parent hash",
            Event::blocks(vec![mb(3, "h3", 2, "bad\rparent")]),
        ),
        (
            "header-unsafe finality hash",
            Event::blocks(vec![mb(3, "h3", 2, "h2")]).with_finality(
                data_service_core::types::BlockRef {
                    number: 9,
                    hash: "bad\nfinality".into(),
                },
            ),
        ),
    ] {
        let events = vec![Event::blocks(vec![good.clone()]), event];
        let (model, stopped) = replay(&seed, &events, CACHE_SIZE, false);
        assert!(
            matches!(stopped, ApplyOutcome::SessionError(_)),
            "{case}: malformed source data is an adapter fault, got {stopped:?}"
        );

        let observed = drive_sut(&seed, events, |p| p.stream_calls() >= 2).await;
        assert_eq!(observed, Observed::of_model(&model), "{case}");
    }
}

/// DEF-8/WP-6: a ref that already names a buffered block, delivered under a
/// second ancestry, is neither a duplicate nor a reorg. Applying it as one
/// would rewrite history under an unchanged ref — invisible to every client.
#[tokio::test]
async fn a_second_ancestry_for_a_buffered_ref_rejects_its_batch() {
    let seed = mb(1, "h1", 0, "h0");
    let events = vec![
        Event::blocks(vec![mb(2, "h2", 1, "h1")]),
        Event::blocks(vec![mb(3, "h3", 2, "h2")]),
        // Same ref as the buffered block 3, reparented onto block 1 — which
        // *is* buffered, so nothing downstream rejects it: as a reorg it drops
        // block 2 and leaves the head ref exactly where it was.
        Event::blocks(vec![mb(3, "h3", 1, "h1")]),
    ];

    let (model, stopped) = replay(&seed, &events, CACHE_SIZE, false);
    assert!(
        matches!(stopped, ApplyOutcome::IntegrityViolation(_)),
        "the history must violate integrity in the model, got {stopped:?}"
    );

    let observed = drive_sut(&seed, events, |p| p.stream_calls() >= 2).await;

    assert_eq!(
        observed,
        Observed::of_model(&model),
        "the SUT kept a block the model rejected"
    );
}

/// DEF-20/WP-3: a batch that is not pairwise linked is rejected on shape,
/// before anything mutates. Applied block by block it would not even fail —
/// each block links to the buffer on its own, and the later one reorgs the
/// earlier one away, so the batch silently loses a block instead.
#[tokio::test]
async fn an_unordered_batch_is_rejected_whole() {
    let seed = mb(1, "h1", 0, "h0");
    let good = mb(2, "h2", 1, "h1");
    let events = vec![
        // A first block must land, or the rejection below is FM-31 startup
        // failure and the ladder never restarts to be compared against.
        Event::blocks(vec![good.clone()]),
        Event::blocks(vec![mb(4, "h4", 2, "h2"), mb(3, "h3", 2, "h2")]),
    ];

    let (model, stopped) = replay(&seed, &events, CACHE_SIZE, false);
    assert!(
        matches!(stopped, ApplyOutcome::SessionError(_)),
        "DEF-20 shape is an adapter fault, not an integrity violation, got {stopped:?}"
    );
    assert_eq!(
        model.head(),
        good.block_ref(),
        "the model mutated nothing on the bad batch"
    );

    let observed = drive_sut(&seed, events, |p| p.stream_calls() >= 2).await;

    assert_eq!(
        observed,
        Observed::of_model(&model),
        "the SUT applied part of a batch the model rejected whole"
    );
}

/// WP-6: an identical redelivery is a no-op. Treating it as a reorg
/// truncates blocks nothing reorged away, and readers pay for it with a
/// conflict recovery the chain never asked for.
#[tokio::test]
async fn an_identical_redelivery_changes_nothing() {
    let seed = mb(1, "h1", 0, "h0");
    let events = vec![
        Event::blocks(vec![mb(2, "h2", 1, "h1")]),
        Event::blocks(vec![mb(3, "h3", 2, "h2")]),
        Event::blocks(vec![mb(4, "h4", 3, "h3")]),
        // Redelivered byte-identical, below the head.
        Event::blocks(vec![mb(3, "h3", 2, "h2")]),
    ];

    let (model, stopped) = replay(&seed, &events, CACHE_SIZE, false);
    assert_eq!(
        stopped,
        ApplyOutcome::Applied,
        "redelivery is legal input, not a violation"
    );
    assert_eq!(model.head().number, 4, "the model absorbs the duplicate");

    // Nothing tears down here, so compare once the redelivery itself has
    // been applied — the head reaches 4 one event earlier.
    let n = events.len();
    let observed = drive_sut(&seed, events, |p| p.delivered() >= n).await;

    assert_eq!(
        observed,
        Observed::of_model(&model),
        "the SUT regressed its head on a redelivery"
    );
}
