//! Executable reference model (HC-5): the normative pseudocode of
//! `spec/13-conformance-tdd.md` §Reference model, verbatim in code.
//!
//! Pure and single-threaded. Free variables of the SUT (coverage end point,
//! conflict ref selection, batch grouping, timing) are NOT decided here; the
//! model returns the full information and comparators check the SUT's choice
//! against the contract instead of against one value.

use data_service_core::types::BlockRef;

/// The model's view of a block: linkage only, payload identified by ref.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelBlock {
    pub number: u64,
    pub hash: String,
    pub parent_number: u64,
    pub parent_hash: String,
}

impl ModelBlock {
    pub fn block_ref(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash.clone(),
        }
    }
    pub fn parent_ref(&self) -> BlockRef {
        BlockRef {
            number: self.parent_number,
            hash: self.parent_hash.clone(),
        }
    }
}

fn linked(a: &ModelBlock, b: &ModelBlock) -> bool {
    a.number == b.parent_number && a.hash == b.parent_hash
}

/// Alarms the model raises. Surfacing them is INV-31 — no comparator reads
/// this list yet (the matrix keeps INV-31 at U); tests may assert on it
/// directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Alarm {
    IntegrityViolation(&'static str),
    OverWindow,
    ForceAdvance,
    Terminal,
}

/// Outcome of applying one input event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    /// WP-5: batch rejected whole, session teardown — never process death.
    IntegrityViolation(&'static str),
    /// FM-30: divergence below finality.
    Terminal,
    /// WP-10 on empty `prev`: session error, not a rebase.
    SessionError(&'static str),
}

/// Model verdict for a stream query (RP-3 resolution).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryVerdict {
    /// Any non-empty prefix of `blocks` is conforming (free variable 1).
    Data {
        blocks: Vec<ModelBlock>,
        finalized: BlockRef,
    },
    Empty {
        head: BlockRef,
        finalized: BlockRef,
    },
    /// SUT's `prev` must be non-empty, ascending, and every ref a block-ref or
    /// parent-ref of the model chain (free variable 2 covers count/selection).
    Conflict,
    /// Below the window — RP-8; not modeled in Phase 0.
    Backfill,
}

/// The reference state machine over `C = (B, f)` plus session cursor.
pub struct RefModel {
    blocks: Vec<ModelBlock>,
    /// 0-based index of the finalized head within `blocks`.
    f: usize,
    cache_size: usize,
    auto_adjust: bool,
    /// WP-12: running maximum of finality reports for the current session —
    /// the only obligation held (ADR-16).
    fin_max: Option<BlockRef>,
    pub alarms: Vec<Alarm>,
}

impl RefModel {
    /// T1 INIT: seed a one-block buffer.
    pub fn init(seed: ModelBlock, cache_size: usize, auto_adjust: bool) -> Self {
        let m = RefModel {
            blocks: vec![seed],
            f: 0,
            cache_size,
            auto_adjust,
            fin_max: None,
            alarms: vec![],
        };
        m.assert_well_formed();
        m
    }

    pub fn head(&self) -> BlockRef {
        self.blocks.last().unwrap().block_ref()
    }

    pub fn finalized(&self) -> BlockRef {
        self.blocks[self.f].block_ref()
    }

    pub fn first(&self) -> BlockRef {
        self.blocks[0].block_ref()
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub fn blocks(&self) -> &[ModelBlock] {
        &self.blocks
    }

    /// INV-1..3, asserted after every transition (spec 13 `well_formed()`).
    pub fn assert_well_formed(&self) {
        assert!(!self.blocks.is_empty(), "INV-1: buffer empty");
        for w in self.blocks.windows(2) {
            assert!(
                linked(&w[0], &w[1]) && w[0].number < w[1].number,
                "INV-2: chain shape broken at {}..{}",
                w[0].number,
                w[1].number
            );
        }
        assert!(self.f < self.blocks.len(), "INV-3: finality index invalid");
    }

    /// One input batch: T2/T3 per block, then WP-12 + T4, then T5 — one
    /// atomic step (WP-3). A violating batch is rejected whole, including its
    /// finality report (WP-5).
    ///
    /// `blocks` is non-empty per DEF-20. Finality knowledge is attached
    /// opportunistically to a head batch per ADR-6; there is no standalone
    /// finality-only input event.
    pub fn apply_batch(
        &mut self,
        blocks: &[ModelBlock],
        finalized_report: Option<&BlockRef>,
    ) -> ApplyOutcome {
        if blocks.is_empty() {
            return ApplyOutcome::SessionError("empty input batch (DEF-20)");
        }
        // DEF-20 shape, before the checkpoint: applied block by block, an
        // append-then-reorg pair would pass as if it were well formed.
        if blocks
            .windows(2)
            .any(|w| w[0].number >= w[1].number || !linked(&w[0], &w[1]))
        {
            return ApplyOutcome::SessionError("malformed input batch (DEF-20)");
        }
        self.atomic_step(|m| {
            for x in blocks {
                match m.push(x) {
                    ApplyOutcome::Applied => {}
                    bad => return bad,
                }
            }
            // Settle before noting: the arriving blocks discharge the
            // obligation the previous batch left open.
            match m.settle_report() {
                ApplyOutcome::Applied => {}
                bad => return bad,
            }
            match finalized_report {
                Some(r) => match m.note_report(r) {
                    ApplyOutcome::Applied => m.settle_report(),
                    bad => bad,
                },
                None => ApplyOutcome::Applied,
            }
        })
    }

    /// WP-12: the running maximum is per session. A rebase (T6) stays inside
    /// one; a teardown ends it.
    pub fn begin_session(&mut self) {
        self.fin_max = None;
    }

    /// WP-3/WP-5: run `step` against a checkpoint, restore it whole on any
    /// non-`Applied` outcome, then T5 + INV-1..3.
    fn atomic_step(&mut self, step: impl FnOnce(&mut Self) -> ApplyOutcome) -> ApplyOutcome {
        let checkpoint = (self.blocks.clone(), self.f, self.fin_max.clone());
        match step(self) {
            ApplyOutcome::Applied => {}
            bad => {
                (self.blocks, self.f, self.fin_max) = checkpoint;
                return bad;
            }
        }
        self.compact();
        self.assert_well_formed();
        ApplyOutcome::Applied
    }

    /// WP-12: running maximum with contradiction detection. A report naming
    /// the maximum's height under a different hash is a violation; a higher
    /// report replaces the maximum, and the replaced obligation's own check is
    /// not owed (ADR-16).
    fn note_report(&mut self, r: &BlockRef) -> ApplyOutcome {
        if let Some(m) = &self.fin_max {
            if r.number == m.number && r.hash != m.hash {
                self.alarm(Alarm::IntegrityViolation("conflicting finality reports"));
                return ApplyOutcome::IntegrityViolation("conflicting finality reports");
            }
        }
        if self.fin_max.as_ref().is_none_or(|m| r.number > m.number) {
            self.fin_max = Some(r.clone());
        }
        ApplyOutcome::Applied
    }

    /// Validate the obligation once its named block has arrived; above the
    /// head it keeps the provisional whole-buffer finality (WP-23).
    fn settle_report(&mut self) -> ApplyOutcome {
        let head_num = self.blocks.last().unwrap().number;
        if let Some(m) = self.fin_max.clone() {
            if m.number <= head_num {
                if let bad @ ApplyOutcome::IntegrityViolation(_) = self.finalize(&m) {
                    return bad;
                }
            } else {
                self.f = self.blocks.len() - 1;
            }
        }
        ApplyOutcome::Applied
    }

    fn push(&mut self, x: &ModelBlock) -> ApplyOutcome {
        // WP-6: identical duplicate is a no-op. Checked before DEF-4, because
        // a redelivered root (parentNumber = number, DEF-4's sole exception)
        // must be absorbed here, not rejected below.
        if self.blocks.iter().any(|b| {
            b.number == x.number
                && b.hash == x.hash
                && b.parent_number == x.parent_number
                && b.parent_hash == x.parent_hash
        }) {
            return ApplyOutcome::Applied;
        }
        // WP-6/DEF-8 equivocation: one ref, two ancestries. As a reorg it
        // would rewrite history under an unchanged ref — unobservable.
        if self
            .blocks
            .iter()
            .any(|b| b.number == x.number && b.hash == x.hash)
        {
            self.alarm(Alarm::IntegrityViolation("ref equivocation"));
            return ApplyOutcome::IntegrityViolation("ref equivocation");
        }
        if x.parent_number >= x.number {
            self.alarm(Alarm::IntegrityViolation("descending height"));
            return ApplyOutcome::IntegrityViolation("descending height"); // DEF-4
        }
        let last = self.blocks.last().unwrap();
        if linked(last, x) {
            self.blocks.push(x.clone()); // T2 APPEND
            return ApplyOutcome::Applied;
        }
        // T3 REORG: unique buffered parent, at or above finality, hash match.
        let Some(i) = self.blocks.iter().position(|b| b.number == x.parent_number) else {
            self.alarm(Alarm::IntegrityViolation("gap: parent not buffered"));
            return ApplyOutcome::IntegrityViolation("gap: parent not buffered");
        };
        if i < self.f {
            self.alarm(Alarm::IntegrityViolation("write below finality"));
            return ApplyOutcome::IntegrityViolation("write below finality");
        }
        if !linked(&self.blocks[i], x) {
            self.alarm(Alarm::IntegrityViolation("parent hash mismatch"));
            return ApplyOutcome::IntegrityViolation("parent hash mismatch");
        }
        self.blocks.truncate(i + 1);
        self.blocks.push(x.clone());
        ApplyOutcome::Applied
    }

    /// T4 FINALIZE (WP-23).
    fn finalize(&mut self, r: &BlockRef) -> ApplyOutcome {
        if r.number < self.blocks[0].number {
            return ApplyOutcome::Applied; // stale — no-op
        }
        if r.number > self.blocks.last().unwrap().number {
            self.f = self.blocks.len() - 1; // whole buffer, re-validated later
            return ApplyOutcome::Applied;
        }
        let Some(i) = self.blocks.iter().position(|b| b.number == r.number) else {
            self.alarm(Alarm::IntegrityViolation(
                "finality names unbuffered height",
            ));
            return ApplyOutcome::IntegrityViolation("finality names unbuffered height");
        };
        if self.blocks[i].hash != r.hash {
            self.alarm(Alarm::IntegrityViolation("finality hash mismatch"));
            return ApplyOutcome::IntegrityViolation("finality hash mismatch");
        }
        self.f = self.f.max(i);
        ApplyOutcome::Applied
    }

    /// T5 COMPACT (WP-24).
    fn compact(&mut self) {
        let excess = self.blocks.len().saturating_sub(self.cache_size);
        if excess > self.f && self.auto_adjust {
            self.f = excess; // force-advance
            self.alarm(Alarm::ForceAdvance);
        }
        let k = excess.min(self.f);
        if k > 0 {
            self.blocks.drain(..k);
            self.f -= k;
        }
        if self.blocks.len() > self.cache_size {
            self.alarm(Alarm::OverWindow); // INV-4 standing alarm
        }
    }

    /// T6 REBASE (WP-10). Returns the new session base, or the terminal /
    /// session-error verdict.
    ///
    /// When no signalled ref matches a buffered block, the base is the newest
    /// buffered block strictly below the lowest signalled ref — stepwise
    /// descent (ADR-15): each signal moves the base at least one block down,
    /// so repeated signals either reach the true fork point or walk below
    /// finality, where divergence is terminal.
    pub fn fork_signal(&mut self, prev: &[BlockRef]) -> Result<BlockRef, ApplyOutcome> {
        if prev.is_empty() {
            return Err(ApplyOutcome::SessionError("empty fork signal")); // FM-13
        }
        let lowest = prev.iter().map(|r| r.number).min().unwrap();
        for b in self.blocks[self.f..].iter().rev() {
            if prev.iter().any(|r| *r == b.block_ref()) || b.number < lowest {
                return Ok(b.block_ref());
            }
        }
        self.alarm(Alarm::Terminal);
        Err(ApplyOutcome::Terminal) // FM-19 → FM-30
    }

    /// RP-3 range resolution against the current state (the snapshot).
    pub fn query(&self, from: u64, parent_hash: Option<&str>) -> QueryVerdict {
        // Below-window means below `first`'s parent link. A buffered root
        // (parentNumber = number, DEF-4's exception) has nothing below it:
        // every `from` up to its height is a window query served from it.
        let first = &self.blocks[0];
        let rooted = first.parent_number == first.number;
        if !rooted && from <= first.parent_number {
            return QueryVerdict::Backfill;
        }
        let last = self.blocks.last().unwrap();
        if from > last.number {
            if from == last.number + 1 {
                if let Some(ph) = parent_hash {
                    if ph != last.hash {
                        return QueryVerdict::Conflict; // RP-11 at head+1
                    }
                }
            }
            return QueryVerdict::Empty {
                head: self.head(),
                finalized: self.finalized(),
            };
        }
        let pos = self.blocks.iter().position(|b| b.number >= from).unwrap();
        let x = &self.blocks[pos];
        if let Some(ph) = parent_hash {
            if x.parent_hash != ph {
                return QueryVerdict::Conflict; // RP-11 in-window
            }
        }
        QueryVerdict::Data {
            blocks: self.blocks[pos..].to_vec(),
            finalized: self.finalized(),
        }
    }

    /// INV-22 membership: a conflict ref must be a block-ref or parent-ref of
    /// the model chain. A root's parent ref names no block (DEF-4), so it is
    /// not one of them.
    pub fn is_known_ref(&self, r: &BlockRef) -> bool {
        self.blocks
            .iter()
            .any(|b| b.block_ref() == *r || (b.parent_number != b.number && b.parent_ref() == *r))
    }

    fn alarm(&mut self, a: Alarm) {
        self.alarms.push(a);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blk(n: u64, hash: &str, parent: &str) -> ModelBlock {
        ModelBlock {
            number: n,
            hash: hash.into(),
            parent_number: n.saturating_sub(1),
            parent_hash: parent.into(),
        }
    }

    fn rf(n: u64, hash: &str) -> BlockRef {
        BlockRef {
            number: n,
            hash: hash.into(),
        }
    }

    /// A buffer seed: the first block is always linked to a parent outside the
    /// buffer (T1 seeds at the upstream finalized head — DEF-4 admits no root).
    fn seed() -> ModelBlock {
        blk(1, "h1", "h0")
    }

    fn chain(range: std::ops::RangeInclusive<u64>) -> Vec<ModelBlock> {
        range
            .map(|i| blk(i, &format!("h{i}"), &format!("h{}", i - 1)))
            .collect()
    }

    #[test]
    fn duplicate_redelivery_is_a_noop() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(m.apply_batch(&chain(2..=4), None), ApplyOutcome::Applied);
        // Redelivering block 3 must not truncate block 4 (WP-6 / GAP-29).
        assert_eq!(
            m.apply_batch(&[blk(3, "h3", "h2")], None),
            ApplyOutcome::Applied
        );
        assert_eq!(m.head(), rf(4, "h4"));
        assert_eq!(m.len(), 4);
    }

    #[test]
    fn seed_redelivery_is_a_noop() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(m.apply_batch(&chain(2..=3), None), ApplyOutcome::Applied);
        // WP-6 holds at every position, including the buffer's first block.
        assert_eq!(m.apply_batch(&[seed()], None), ApplyOutcome::Applied);
        assert_eq!(m.len(), 3);
        assert_eq!(m.head(), rf(3, "h3"));
    }

    #[test]
    fn the_same_ref_under_a_second_parent_is_equivocation() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(m.apply_batch(&chain(2..=3), None), ApplyOutcome::Applied);
        // Same ref, lower parent: absorbing hides it, reorging rewrites
        // block 3's ancestry under an unchanged ref. WP-6/DEF-8: violation.
        let forged = ModelBlock {
            number: 3,
            hash: "h3".into(),
            parent_number: 1,
            parent_hash: "h1".into(),
        };
        assert_eq!(
            m.apply_batch(&[forged], None),
            ApplyOutcome::IntegrityViolation("ref equivocation")
        );
        // WP-5: the rejected batch left the buffer whole.
        assert_eq!(m.len(), 3);
        assert_eq!(m.head(), rf(3, "h3"));
        assert_eq!(m.blocks()[2].parent_number, 2);
    }

    #[test]
    fn a_batch_that_is_not_pairwise_linked_is_rejected_whole() {
        let mut m = RefModel::init(seed(), 100, false);
        // Ascending but unlinked: block by block this appends 2 then gaps on
        // 4; DEF-20 rejects the batch before anything mutates.
        assert_eq!(
            m.apply_batch(&[blk(2, "h2", "h1"), blk(4, "h4", "h3")], None),
            ApplyOutcome::SessionError("malformed input batch (DEF-20)")
        );
        // Descending: an append-then-reorg pair that would otherwise "apply".
        let b3 = ModelBlock {
            number: 3,
            hash: "h3".into(),
            parent_number: 1,
            parent_hash: "h1".into(),
        };
        assert_eq!(
            m.apply_batch(&[b3, blk(2, "h2", "h1")], None),
            ApplyOutcome::SessionError("malformed input batch (DEF-20)")
        );
        assert_eq!(m.len(), 1);
        assert_eq!(m.head(), rf(1, "h1"));
    }

    fn root() -> ModelBlock {
        // DEF-4's root convention: parentNumber = number, parent hash names
        // no block — exactly what the simulator generates for block 0.
        ModelBlock {
            number: 0,
            hash: "h0".into(),
            parent_number: 0,
            parent_hash: "0x0".into(),
        }
    }

    #[test]
    fn root_redelivery_is_a_noop() {
        let mut m = RefModel::init(root(), 100, false);
        assert_eq!(m.apply_batch(&chain(1..=2), None), ApplyOutcome::Applied);
        // WP-6: an identical duplicate is a no-op even for the root, whose
        // self-parent convention must not trip the DEF-4 height check.
        assert_eq!(m.apply_batch(&[root()], None), ApplyOutcome::Applied);
        assert_eq!(m.len(), 3);
        assert_eq!(m.head(), rf(2, "h2"));
    }

    #[test]
    fn query_serves_data_at_a_buffered_root() {
        let mut m = RefModel::init(root(), 100, false);
        assert_eq!(m.apply_batch(&chain(1..=1), None), ApplyOutcome::Applied);
        // Block 0 is buffered and servable; nothing exists below a root, so
        // this is a window query, not a backfill.
        match m.query(0, None) {
            QueryVerdict::Data { blocks, .. } => assert_eq!(blocks[0].number, 0),
            other => panic!("buffered root must be served, got {other:?}"),
        }
    }

    #[test]
    fn query_below_a_non_root_base_is_backfill() {
        let m = RefModel::init(blk(10, "h10", "h9"), 100, false);
        assert_eq!(m.query(9, None), QueryVerdict::Backfill);
        assert!(matches!(m.query(10, None), QueryVerdict::Data { .. }));
    }

    #[test]
    fn fork_signal_exhausted_prev_descends_one_below_the_lowest_ref() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(m.apply_batch(&chain(2..=5), None), ApplyOutcome::Applied);
        // Upstream names height 4 under an unknown hash: no buffered ref
        // matches, so the base is the newest block strictly below the lowest
        // signalled ref — stepwise descent (WP-10), not terminal divergence.
        let base = m
            .fork_signal(&[rf(4, "evil")])
            .expect("stepwise descent, not terminal");
        assert_eq!(base, rf(3, "h3"));
    }

    #[test]
    fn fork_signal_matching_the_finalized_block_rebases() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(
            m.apply_batch(&chain(2..=5), Some(&rf(4, "h4"))),
            ApplyOutcome::Applied
        );
        assert_eq!(m.finalized(), rf(4, "h4"));
        // The finalized block itself is a legal base: the search is at or
        // above finality (WP-10 boundary), divergence starts strictly below.
        assert_eq!(m.fork_signal(&[rf(4, "h4")]), Ok(rf(4, "h4")));
    }

    #[test]
    fn fork_signal_entirely_below_finality_is_terminal() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(
            m.apply_batch(&chain(2..=5), Some(&rf(4, "h4"))),
            ApplyOutcome::Applied
        );
        // The only descent target would sit below the finalized block: FM-30.
        assert_eq!(m.fork_signal(&[rf(2, "evil")]), Err(ApplyOutcome::Terminal));
    }

    #[test]
    fn descending_height_is_rejected() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(m.apply_batch(&chain(2..=2), None), ApplyOutcome::Applied);
        let bad = ModelBlock {
            number: 3,
            hash: "hx".into(),
            parent_number: 5,
            parent_hash: "h5".into(),
        };
        assert_eq!(
            m.apply_batch(&[bad], None),
            ApplyOutcome::IntegrityViolation("descending height")
        );
    }

    #[test]
    fn empty_input_batch_is_an_adapter_fault() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(
            m.apply_batch(&[], Some(&rf(2, "A"))),
            ApplyOutcome::SessionError("empty input batch (DEF-20)")
        );
    }

    #[test]
    fn the_obligation_is_validated_when_its_block_arrives() {
        let mut m = RefModel::init(seed(), 100, false);
        // Report (3, A) rides on block 2 and stays open above the head.
        assert_eq!(
            m.apply_batch(&chain(2..=2), Some(&rf(3, "A"))),
            ApplyOutcome::Applied
        );
        // Block 3 arrives under a different hash: the open obligation is
        // checked before the batch's own report is noted, and fails (WP-23).
        assert_eq!(
            m.apply_batch(&[blk(3, "C", "h2")], Some(&rf(9, "B"))),
            ApplyOutcome::IntegrityViolation("finality hash mismatch")
        );
        // The rejected batch left no trace (WP-5).
        assert_eq!(m.head(), rf(2, "h2"));
    }

    #[test]
    fn a_higher_report_replaces_the_open_obligation() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(
            m.apply_batch(&chain(2..=2), Some(&rf(5, "A"))),
            ApplyOutcome::Applied
        );
        // A higher report supersedes (5, A) before block 5 ever arrives, so
        // (5, A)'s own hash check is no longer owed (ADR-16).
        assert_eq!(
            m.apply_batch(&chain(3..=3), Some(&rf(6, "B"))),
            ApplyOutcome::Applied
        );
        let mut tail = chain(4..=4);
        tail.push(blk(5, "C", "h4"));
        assert_eq!(m.apply_batch(&tail, None), ApplyOutcome::Applied);
        assert_eq!(m.head(), rf(5, "C"));
    }

    #[test]
    fn equal_height_contradictory_reports_are_a_violation() {
        let mut m = RefModel::init(seed(), 100, false);
        assert_eq!(
            m.apply_batch(&chain(2..=2), Some(&rf(4, "A"))),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply_batch(&chain(3..=3), Some(&rf(4, "B"))),
            ApplyOutcome::IntegrityViolation("conflicting finality reports")
        );
    }
}
