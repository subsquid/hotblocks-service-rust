//! In-memory chain buffer — exact port of `chain.ts`.

use crate::types::{
    Block, BlockHeader, BlockRef, DataResponse, IntegrityViolation, InvalidBaseBlock,
};

/// Returns true if `a` is the direct parent of `b`.
pub(crate) fn is_chain(a: &Block, b: &Block) -> bool {
    a.number == b.parent_number && a.hash == b.parent_hash
}

/// Bisect: return the index of the first block with `number >= target`.
/// Equivalent to the TS `bisect` utility used in chain.ts.
fn bisect(blocks: &[Block], target: u64) -> usize {
    let mut lo = 0usize;
    let mut hi = blocks.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if blocks[mid].number < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// What a `push` did to the buffer. An absorbed duplicate is a strict no-op
/// (WP-6), so its ingest-time observables must not be rewritten either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Push {
    Inserted,
    Absorbed,
}

/// What T5 could do about the window this time (WP-24). Both fields feed OB-6.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Compaction {
    /// Blocks still above `max_size` — non-zero only when a lagging finalized
    /// head blocks the trim and auto-adjust is off (INV-4).
    pub excess: usize,
    /// Height the finalized head was force-advanced past, if it was.
    pub force_advanced_past: Option<u64>,
}

impl Compaction {
    /// The buffer is inside its window bound.
    pub fn within_window(&self) -> bool {
        self.excess == 0
    }
}

/// Undo log for the batch currently being applied (WP-5).
///
/// Only the *originals* a truncation removed are saved — blocks the open batch
/// appended itself need no record — so an append-only batch costs nothing.
struct BatchUndo {
    /// Leading blocks the batch has not touched yet.
    intact: usize,
    /// The originals above `intact`, in buffer order.
    removed: Vec<Block>,
    finalized_head: usize,
}

/// An in-memory sliding window of recent blocks, with a finalized-head pointer.
///
/// Mirrors the TypeScript `Chain` class from `chain.ts`.
pub struct Chain {
    blocks: Vec<Block>,
    /// Index into `blocks` of the current finalized head.
    finalized_head: usize,
    max_size: usize,
    auto_adjust_finalized_head: bool,
    undo: Option<BatchUndo>,
}

impl Chain {
    pub fn new(base: Block, max_size: usize, auto_adjust_finalized_head: bool) -> Self {
        assert!(max_size > 0, "max_size must be > 0");
        Self {
            blocks: vec![base],
            finalized_head: 0,
            max_size,
            auto_adjust_finalized_head,
            undo: None,
        }
    }

    // ----- batch atomicity (WP-5) -----------------------------------------

    /// Open a batch. Everything until `commit_batch` can be undone whole, so a
    /// violation found late — a finality report the arriving blocks contradict
    /// — rejects the batch instead of leaving it half applied.
    ///
    /// Batches do not nest; opening one discards any log still open.
    pub fn begin_batch(&mut self) {
        debug_assert!(
            self.undo.is_none(),
            "an open batch was neither committed nor rolled back"
        );
        self.undo = Some(BatchUndo {
            intact: self.blocks.len(),
            removed: Vec::new(),
            finalized_head: self.finalized_head,
        });
    }

    /// Accept the open batch. Compaction must follow, never precede: it shifts
    /// the indices the log is written in.
    pub fn commit_batch(&mut self) {
        self.undo = None;
    }

    /// Restore the buffer to what it was at `begin_batch`.
    pub fn rollback_batch(&mut self) {
        let Some(undo) = self.undo.take() else { return };
        self.blocks.truncate(undo.intact);
        self.blocks.extend(undo.removed);
        self.finalized_head = undo.finalized_head;
    }

    /// Save the originals a truncation down to `from` is about to drop.
    fn record_removal(&mut self, from: usize) {
        let Chain { blocks, undo, .. } = self;
        let Some(undo) = undo.as_mut() else { return };
        if from >= undo.intact {
            return; // nothing but this batch's own appends
        }
        let mut originals = blocks[from..undo.intact].to_vec();
        originals.append(&mut undo.removed);
        undo.removed = originals;
        undo.intact = from;
    }

    // ----- mutation -------------------------------------------------------

    /// Push a new block onto the chain, potentially triggering a reorg.
    ///
    /// Mirrors `Chain.push` in chain.ts. Input the buffer contradicts is an
    /// integrity violation the caller resolves by rejecting the batch and
    /// tearing down the session (WP-5), never by dying (WP-14).
    pub fn push(&mut self, new_block: Block) -> Result<Push, IntegrityViolation> {
        // WP-6, decided against the block this height already holds — one per
        // height (INV-2), so bisect names the only candidate. Both outcomes
        // come before DEF-4 so a redelivered root, whose parent coordinate is
        // its own height (DEF-4's sole exception), is absorbed rather than
        // rejected below.
        let at = bisect(&self.blocks, new_block.number);
        if let Some(held) = self.blocks.get(at) {
            if held.number == new_block.number && held.hash == new_block.hash {
                // An identical redelivery is a strict no-op at every position,
                // including at or below the finalized head.
                if held.parent_number == new_block.parent_number
                    && held.parent_hash == new_block.parent_hash
                {
                    return Ok(Push::Absorbed);
                }
                // Same ref, another ancestry: neither duplicate nor reorg.
                return Err(IntegrityViolation::RefEquivocation {
                    number: new_block.number,
                    hash: new_block.hash,
                });
            }
        }

        // DEF-4; a descending block leaves `blocks` unsorted and `bisect` blind.
        if new_block.parent_number >= new_block.number {
            return Err(IntegrityViolation::DescendingHeight {
                number: new_block.number,
                parent_number: new_block.parent_number,
            });
        }

        if self.last_block().number == new_block.parent_number {
            if !is_chain(self.last_block(), &new_block) {
                return Err(IntegrityViolation::ParentHashMismatch {
                    number: new_block.number,
                    buffered: self.last_block().hash.clone(),
                    claimed: new_block.parent_hash,
                });
            }
            self.blocks.push(new_block);
            return Ok(Push::Inserted);
        }

        let pos = bisect(&self.blocks, new_block.parent_number);
        if pos < self.finalized_head {
            return Err(IntegrityViolation::WriteBelowFinality {
                number: new_block.number,
            });
        }
        // Above every buffered height, or DEF-1 height gap: either way the
        // named parent is not held, which is a gap and not a reorg.
        if self
            .blocks
            .get(pos)
            .is_none_or(|p| p.number != new_block.parent_number)
        {
            return Err(IntegrityViolation::ParentNotBuffered {
                number: new_block.number,
                parent_number: new_block.parent_number,
            });
        }
        if self.blocks[pos].hash != new_block.parent_hash {
            return Err(IntegrityViolation::ParentHashMismatch {
                number: new_block.number,
                buffered: self.blocks[pos].hash.clone(),
                claimed: new_block.parent_hash,
            });
        }

        self.record_removal(pos + 1);
        self.blocks.truncate(pos + 1);
        self.blocks.push(new_block);
        Ok(Push::Inserted)
    }

    /// Advance the finalized head pointer to `head`.
    ///
    /// Mirrors `Chain.finalize` in chain.ts.
    /// Returns `true` if the finalized head actually advanced. A report the
    /// buffer contradicts is an integrity violation the caller resolves by
    /// tearing down the session (WP-5), never by dying.
    pub fn finalize(&mut self, head: &BlockRef) -> Result<bool, IntegrityViolation> {
        // A regressive report is ignored before its hash is inspected: the
        // running session maximum already superseded this height (WP-12). This
        // also subsumes `head.number < self.first_block_number()`.
        if head.number < self.blocks[self.finalized_head].number {
            return Ok(false);
        }

        let current = self.finalized_head;

        if head.number > self.last_block_number() {
            // Finalize everything — safe per DataSource stream guarantees.
            self.finalized_head = self.blocks.len() - 1;
            return Ok(self.finalized_head > current);
        }

        let pos = if head.number == self.last_block_number() {
            self.blocks.len() - 1
        } else {
            bisect(&self.blocks, head.number)
        };

        // DEF-1 permits height gaps, so `bisect` can land above the report.
        if self.blocks[pos].number != head.number {
            return Err(IntegrityViolation::UnbufferedFinality {
                number: head.number,
            });
        }
        if self.blocks[pos].hash != head.hash {
            return Err(IntegrityViolation::FinalityHashMismatch {
                number: head.number,
                buffered: self.blocks[pos].hash.clone(),
                reported: head.hash.clone(),
            });
        }

        self.finalized_head = self.finalized_head.max(pos);
        Ok(self.finalized_head > current)
    }

    /// Trim old finalized blocks to keep the buffer at or below `max_size`.
    ///
    /// Mirrors `Chain.compact` in chain.ts.
    pub fn compact(&mut self) -> Compaction {
        debug_assert!(
            self.undo.is_none(),
            "compaction shifts the indices an open undo log is written in"
        );
        let extra = self.blocks.len().saturating_sub(self.max_size);
        if extra == 0 {
            return Compaction::default();
        }

        let mut force_advanced_past = None;
        if self.finalized_head < extra && self.auto_adjust_finalized_head {
            let new_last = &self.blocks[extra - 1];
            tracing::warn!(
                block_number = new_last.number,
                block_hash = %new_last.hash,
                "finalized head was adjusted automatically to block #{}",
                new_last.number
            );
            force_advanced_past = Some(new_last.number);
            self.finalized_head = extra;
        }

        let trim = extra.min(self.finalized_head);
        self.blocks.drain(..trim);
        self.finalized_head -= trim;
        Compaction {
            excess: self.blocks.len().saturating_sub(self.max_size),
            force_advanced_past,
        }
    }

    // ----- queries --------------------------------------------------------

    /// Query for blocks starting at `from`, optionally checking the parent
    /// hash of the first returned block against `base_block_hash`.
    ///
    /// Returns `InvalidBaseBlock` when the base hash doesn't match (with up
    /// to 100 previous block refs).
    ///
    /// Mirrors `Chain.query` in chain.ts (lines 91-134).
    pub fn query(
        &self,
        from: u64,
        base_block_hash: Option<&str>,
    ) -> Result<DataResponse, InvalidBaseBlock> {
        // Caller should do a below-query. A root has no below-window region:
        // its self-height parent coordinate names no block (DEF-4 / RP-3).
        let first = self.first_block();
        if first.parent_number != first.number && from <= first.parent_number {
            return Ok(DataResponse {
                finalized_head: None,
                head: None,
                tail: None,
            });
        }

        let pos = bisect(&self.blocks, from);

        if pos < self.blocks.len() {
            // The requested block is in our buffer.
            if let Some(bhash) = base_block_hash {
                if self.blocks[pos].parent_hash != bhash {
                    // Hash mismatch → return up to 100 previous block refs.
                    let start = pos.saturating_sub(100);
                    // A root's parent coordinate names no block (DEF-4), and
                    // it repeats the root's own height — emitting it breaks
                    // RP-7's ascending refs with a ref nothing resolves.
                    let mut prev: Vec<BlockRef> = self.blocks[start..=pos]
                        .iter()
                        .filter(|b| b.parent_number != b.number)
                        .map(|b| BlockRef {
                            number: b.parent_number,
                            hash: b.parent_hash.clone(),
                        })
                        .collect();
                    if prev.is_empty() {
                        // Only the root itself was in range: its own ref is the
                        // deepest thing a client can roll back to (RP-7 needs
                        // a non-empty list).
                        prev.push(self.first_block().block_ref());
                    }
                    return Err(InvalidBaseBlock { prev });
                }
            }
            Ok(DataResponse {
                finalized_head: Some(self.get_finalized_head()),
                head: None,
                tail: Some(self.blocks[pos..].to_vec()),
            })
        } else if let Some(bhash) = base_block_hash {
            // `from` is one past the last block — check parent hash of the
            // last block itself.
            let last = self.last_block();
            if from == last.number + 1 && bhash != last.hash {
                let start = self.blocks.len().saturating_sub(100);
                let prev: Vec<BlockRef> = self.blocks[start..]
                    .iter()
                    .map(|b| BlockRef {
                        number: b.number,
                        hash: b.hash.clone(),
                    })
                    .collect();
                return Err(InvalidBaseBlock { prev });
            }
            Ok(DataResponse {
                finalized_head: Some(self.get_finalized_head()),
                head: None,
                tail: None,
            })
        } else {
            Ok(DataResponse {
                finalized_head: Some(self.get_finalized_head()),
                head: None,
                tail: None,
            })
        }
    }

    /// Return a snapshot (clone) of the current blocks vec.
    /// Cheap because `Block.json_line_zstd` is `Bytes` (reference-counted).
    pub fn snapshot(&self) -> Vec<Block> {
        self.blocks.clone()
    }

    /// Walk our chain top-down (not below the finalized head) and find the
    /// highest block that appears in `prev`.
    ///
    /// Mirrors `Chain.getForkBase` in chain.ts (lines 202-220).
    pub fn get_fork_base(&self, prev: &[BlockRef]) -> Option<BlockRef> {
        // Work with a mutable copy so we can pop from the end.
        let mut prev: Vec<&BlockRef> = prev.iter().collect();
        let mut fh = prev.pop();
        let mut top = self.blocks.len() as isize - 1;

        while top >= self.finalized_head as isize {
            let b = &self.blocks[top as usize];
            let head = BlockRef {
                number: b.number,
                hash: b.hash.clone(),
            };

            // Advance `fh` past any upstream refs that are above `head`.
            while fh.map(|r| r.number > head.number).unwrap_or(false) {
                fh = prev.pop();
            }

            if fh.is_none() {
                return Some(head);
            }
            let fh_ref = fh.unwrap();
            if fh_ref.number == head.number && fh_ref.hash == head.hash {
                return Some(head);
            }
            top -= 1;
        }
        None
    }

    // ----- getters --------------------------------------------------------

    pub fn first_block(&self) -> &Block {
        &self.blocks[0]
    }

    /// The buffered block at `number` (unique by INV-2). Comparisons against
    /// input need the whole block: identity is ref *and* parent link (DEF-8).
    pub fn block_at(&self, number: u64) -> Option<&Block> {
        self.blocks.iter().find(|b| b.number == number)
    }

    pub fn last_block(&self) -> &Block {
        self.blocks.last().expect("chain is never empty")
    }

    pub fn first_block_number(&self) -> u64 {
        self.blocks[0].number
    }

    pub fn last_block_number(&self) -> u64 {
        self.last_block().number
    }

    /// Blocks held above the configured window (INV-4).
    pub fn window_excess(&self) -> usize {
        self.blocks.len().saturating_sub(self.max_size)
    }

    pub fn size(&self) -> usize {
        self.blocks.len()
    }

    pub fn get_finalized_head(&self) -> BlockRef {
        let b = &self.blocks[self.finalized_head];
        BlockRef {
            number: b.number,
            hash: b.hash.clone(),
        }
    }

    pub fn get_finalized_header(&self) -> BlockHeader {
        let b = &self.blocks[self.finalized_head];
        b.header()
    }

    pub fn get_head(&self) -> BlockRef {
        let b = self.last_block();
        BlockRef {
            number: b.number,
            hash: b.hash.clone(),
        }
    }

    pub fn get_header(&self) -> BlockHeader {
        self.last_block().header()
    }
}

#[cfg(test)]
mod root_conflict_tests {
    use super::*;

    fn blk(n: u64, h: &str, pn: u64, ph: &str) -> Block {
        Block {
            number: n,
            hash: h.into(),
            parent_number: pn,
            parent_hash: ph.into(),
            timestamp: None,
            json_line_zstd: bytes::Bytes::new(),
            timings: None,
        }
    }

    /// RP-7: refs must ascend and resolve to blocks. A root's parent
    /// coordinate does neither — it repeats the root's height and names
    /// nothing (DEF-4).
    #[test]
    fn conflict_prev_never_carries_the_root_sentinel() {
        let mut chain = Chain::new(blk(0, "h0", 0, "0x0"), 10, false);
        chain.push(blk(1, "h1", 0, "h0")).unwrap();

        let err = chain.query(1, Some("wrong")).unwrap_err();
        assert_eq!(
            err.prev,
            vec![BlockRef {
                number: 0,
                hash: "h0".into()
            }]
        );

        // Only the root in range: its own ref is the deepest rollback target,
        // and RP-7 still owes a non-empty list.
        let root_only = Chain::new(blk(0, "h0", 0, "0x0"), 10, false);
        let err = root_only.query(0, Some("wrong")).unwrap_err();
        assert_eq!(
            err.prev,
            vec![BlockRef {
                number: 0,
                hash: "h0".into()
            }]
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_block(number: u64, hash: &str, parent_number: u64, parent_hash: &str) -> Block {
        Block {
            number,
            hash: hash.to_string(),
            parent_number,
            parent_hash: parent_hash.to_string(),
            timestamp: Some(number * 1000),
            json_line_zstd: Bytes::new(),
            timings: None,
        }
    }

    fn genesis() -> Block {
        make_block(0, "h0", 0, "")
    }

    fn chain_of(n: u64) -> Chain {
        let mut c = Chain::new(genesis(), 1000, false);
        for i in 1..n {
            c.push(make_block(
                i,
                &format!("h{i}"),
                i - 1,
                &format!("h{}", i - 1),
            ))
            .unwrap();
        }
        c
    }

    // ---- push / sequential -----------------------------------------------

    #[test]
    fn push_sequential() {
        let c = chain_of(5);
        assert_eq!(c.size(), 5);
        assert_eq!(c.last_block_number(), 4);
    }

    /// WP-14: no input content ends the process. Each violation is reported,
    /// and the buffer it was rejected against is left exactly as it was.
    #[test]
    fn push_reports_violations_and_leaves_the_buffer_intact() {
        let mut c = Chain::new(genesis(), 100, false);
        assert!(matches!(
            c.push(make_block(1, "h1", 0, "wrong")),
            Err(IntegrityViolation::ParentHashMismatch { number: 1, .. })
        ));
        assert_eq!(c.size(), 1);

        // Parent ref names the buffered tip, but the block sits below it:
        // appending would leave `blocks` unsorted and `bisect` unusable.
        let mut c = chain_of(11);
        assert!(matches!(
            c.push(make_block(5, "h5b", 10, "h10")),
            Err(IntegrityViolation::DescendingHeight {
                number: 5,
                parent_number: 10
            })
        ));
        assert_eq!((c.size(), c.last_block_number()), (11, 10));

        // Above every buffered height — a gap, not a reorg.
        assert!(matches!(
            c.push(make_block(20, "h20", 19, "h19")),
            Err(IntegrityViolation::ParentNotBuffered {
                number: 20,
                parent_number: 19
            })
        ));
        assert_eq!((c.size(), c.last_block_number()), (11, 10));

        // A buffered parent height held under another hash.
        assert!(matches!(
            c.push(make_block(6, "h6b", 5, "forged")),
            Err(IntegrityViolation::ParentHashMismatch { number: 6, .. })
        ));
        assert_eq!((c.size(), c.last_block_number()), (11, 10));
    }

    // ---- push / reorg ----------------------------------------------------

    #[test]
    fn push_reorg() {
        let mut c = chain_of(5); // 0..=4
                                 // Reorg: replace blocks 3 and 4 with an alternate chain.
        c.push(make_block(3, "h3b", 2, "h2")).unwrap(); // reorg at pos 3
        assert_eq!(c.size(), 4);
        assert_eq!(c.last_block().hash, "h3b");
        // Now extend.
        c.push(make_block(4, "h4b", 3, "h3b")).unwrap();
        assert_eq!(c.size(), 5);
    }

    #[test]
    fn push_reorg_below_finalized() {
        let mut c = chain_of(5);
        c.finalize(&BlockRef {
            number: 3,
            hash: "h3".into(),
        })
        .unwrap();
        // Try to reorg back to block 2 (INV-11).
        assert!(matches!(
            c.push(make_block(3, "h3b", 2, "h2")),
            Err(IntegrityViolation::WriteBelowFinality { number: 3 })
        ));
        assert_eq!((c.size(), c.last_block_number()), (5, 4));
        assert_eq!(c.get_finalized_head().number, 3);
    }

    // ---- finalize --------------------------------------------------------

    #[test]
    fn finalize_advances() {
        let mut c = chain_of(5);
        assert!(c
            .finalize(&BlockRef {
                number: 3,
                hash: "h3".into()
            })
            .unwrap());
        assert_eq!(c.get_finalized_head().number, 3);
    }

    #[test]
    fn finalize_noop_below_the_finalized_head() {
        // The whole buffer sits above the report, so nothing it names exists.
        let mut c = Chain::new(make_block(10, "h10", 9, "h9"), 100, false);
        assert!(!c
            .finalize(&BlockRef {
                number: 5,
                hash: "h5".into()
            })
            .unwrap());
        assert_eq!(c.get_finalized_head().number, 10);
    }

    #[test]
    fn finalize_above_last_finalizes_all() {
        let mut c = chain_of(5); // blocks 0..=4
        assert!(c
            .finalize(&BlockRef {
                number: 100,
                hash: "nonexistent".into()
            })
            .unwrap());
        assert_eq!(c.get_finalized_head().number, 4);
    }

    #[test]
    fn finalize_monotonic() {
        let mut c = chain_of(10);
        c.finalize(&BlockRef {
            number: 5,
            hash: "h5".into(),
        })
        .unwrap();
        // A lower report is ignored before its hash is inspected: the running
        // session maximum already superseded this height (WP-12).
        assert!(!c
            .finalize(&BlockRef {
                number: 3,
                hash: "wrong-hash".into()
            })
            .unwrap());
        assert_eq!(c.get_finalized_head().number, 5);
    }

    // ---- duplicate absorption (WP-6) -------------------------------------

    #[test]
    fn identical_redelivery_leaves_the_buffer_alone() {
        let mut c = chain_of(5); // 0..=4
        c.finalize(&BlockRef {
            number: 2,
            hash: "h2".into(),
        })
        .unwrap();

        // Mid-buffer: the suffix above the duplicate must survive. Treating
        // this as a reorg is what made readers observe a head regression.
        c.push(make_block(3, "h3", 2, "h2")).unwrap();
        assert_eq!(c.last_block_number(), 4);
        assert_eq!(c.size(), 5);

        // At and below the finalized head: the finality guard must not fire.
        c.push(make_block(2, "h2", 1, "h1")).unwrap();
        c.push(make_block(1, "h1", 0, "h0")).unwrap();
        assert_eq!(c.last_block_number(), 4);
        assert_eq!(c.size(), 5);
        assert_eq!(c.get_finalized_head().number, 2);

        // The head itself.
        c.push(make_block(4, "h4", 3, "h3")).unwrap();
        assert_eq!(c.last_block_number(), 4);
        assert_eq!(c.size(), 5);
    }

    #[test]
    fn a_redelivered_root_is_absorbed_not_rejected() {
        // A root's parent coordinate repeats its own height (DEF-4's sole
        // exception), so absorption must be decided before the height check.
        let mut c = Chain::new(make_block(0, "h0", 0, "0x0"), 10, false);
        c.push(make_block(1, "h1", 0, "h0")).unwrap();
        c.push(make_block(0, "h0", 0, "0x0")).unwrap();
        assert_eq!(c.last_block_number(), 1);
        assert_eq!(c.size(), 2);
    }

    #[test]
    fn the_same_ref_under_a_second_parent_is_equivocation() {
        // DEF-8: absorbing it would hide the substitution, reorging it would
        // rewrite ancestry under an unchanged ref. Neither is observable.
        let mut c = chain_of(5);
        assert!(matches!(
            c.push(make_block(3, "h3", 2, "h2-other")),
            Err(IntegrityViolation::RefEquivocation { number: 3, .. })
        ));
        assert_eq!((c.size(), c.last_block_number()), (5, 4));
        assert_eq!(c.blocks[3].parent_hash, "h2");
    }

    #[test]
    fn a_genuine_reorg_is_still_a_reorg() {
        // Absorption keys on the full linkage tuple, so a different hash at a
        // buffered height must still truncate.
        let mut c = chain_of(5);
        c.push(make_block(3, "h3b", 2, "h2")).unwrap();
        assert_eq!(c.last_block_number(), 3);
        assert_eq!(c.size(), 4);
    }

    // ---- batch atomicity -------------------------------------------------

    fn refs(c: &Chain) -> Vec<(u64, String)> {
        c.blocks
            .iter()
            .map(|b| (b.number, b.hash.clone()))
            .collect()
    }

    #[test]
    fn rollback_restores_appends_reorgs_and_finality() {
        let mut c = chain_of(5); // 0..=4
        c.finalize(&BlockRef {
            number: 2,
            hash: "h2".into(),
        })
        .unwrap();
        let before = refs(&c);

        // Append-only batch: nothing is saved, everything is still undone.
        c.begin_batch();
        c.push(make_block(5, "h5", 4, "h4")).unwrap();
        c.push(make_block(6, "h6", 5, "h5")).unwrap();
        c.rollback_batch();
        assert_eq!(refs(&c), before);

        // A batch that appends, then reorgs below its own appends, then
        // reorgs again lower still — the originals come back in order.
        c.begin_batch();
        c.push(make_block(5, "h5", 4, "h4")).unwrap();
        c.push(make_block(4, "h4b", 3, "h3")).unwrap();
        c.push(make_block(3, "h3b", 2, "h2")).unwrap();
        c.finalize(&BlockRef {
            number: 3,
            hash: "h3b".into(),
        })
        .unwrap();
        assert_eq!(c.get_finalized_head().number, 3);
        c.rollback_batch();
        assert_eq!(refs(&c), before);
        assert_eq!(c.get_finalized_head().number, 2);

        // Committing keeps the batch and leaves no log behind.
        c.begin_batch();
        c.push(make_block(5, "h5", 4, "h4")).unwrap();
        c.commit_batch();
        c.rollback_batch();
        assert_eq!(c.last_block_number(), 5);
    }

    #[test]
    fn finalize_reports_a_contradicted_buffer_instead_of_dying() {
        // WP-23 revalidation: the height arrived under another hash.
        let mut c = chain_of(5);
        assert!(matches!(
            c.finalize(&BlockRef {
                number: 3,
                hash: "h3-forged".into()
            }),
            Err(IntegrityViolation::FinalityHashMismatch { number: 3, .. })
        ));
        // The buffer is untouched and still servable.
        assert_eq!(c.get_finalized_head().number, 0);
        assert_eq!(c.size(), 5);

        // DEF-1 height gaps: nothing is buffered at the named height.
        let mut sparse = Chain::new(make_block(10, "h10", 9, "h9"), 100, false);
        sparse.push(make_block(20, "h20", 10, "h10")).unwrap();
        assert!(matches!(
            sparse.finalize(&BlockRef {
                number: 15,
                hash: "h15".into()
            }),
            Err(IntegrityViolation::UnbufferedFinality { number: 15 })
        ));
    }

    // ---- compact ---------------------------------------------------------

    #[test]
    fn compact_trims_finalized() {
        let mut c = chain_of(10); // blocks 0..=9
        c.finalize(&BlockRef {
            number: 7,
            hash: "h7".into(),
        })
        .unwrap();
        // max_size was set to 1000 in chain_of, use small max_size.
        let mut c2 = Chain::new(genesis(), 5, false);
        for i in 1..10u64 {
            c2.push(make_block(
                i,
                &format!("h{i}"),
                i - 1,
                &format!("h{}", i - 1),
            ))
            .unwrap();
        }
        c2.finalize(&BlockRef {
            number: 7,
            hash: "h7".into(),
        })
        .unwrap();
        // size is 10, max_size is 5 → need to trim 5 entries.
        assert!(c2.compact().within_window());
        // finalized_head was at index 7; after trimming 5 it should be at 2.
        assert_eq!(c2.first_block_number(), 5);
        let _ = c;
    }

    #[test]
    fn compact_blocked_without_auto_adjust() {
        let mut c = Chain::new(genesis(), 3, false);
        for i in 1..5u64 {
            c.push(make_block(
                i,
                &format!("h{i}"),
                i - 1,
                &format!("h{}", i - 1),
            ))
            .unwrap();
        }
        // finalized head still at 0, size=5 > max=3 → cannot trim.
        let compaction = c.compact();
        assert!(!compaction.within_window());
        assert_eq!(compaction.excess, 2);
        assert_eq!(compaction.force_advanced_past, None);
    }

    #[test]
    fn compact_auto_adjust() {
        let mut c = Chain::new(genesis(), 3, true);
        for i in 1..5u64 {
            c.push(make_block(
                i,
                &format!("h{i}"),
                i - 1,
                &format!("h{}", i - 1),
            ))
            .unwrap();
        }
        let compaction = c.compact();
        assert!(compaction.within_window());
        // WP-24's force advance is an event OB-6 counts, naming what it moved past.
        assert_eq!(compaction.force_advanced_past, Some(1));
        // After auto-adjust, the buffer should be within max_size.
        assert!(c.size() <= 3);
    }

    // ---- query -----------------------------------------------------------

    #[test]
    fn query_in_range_hit() {
        let c = chain_of(5);
        let res = c.query(2, None).unwrap();
        assert!(res.tail.is_some());
        assert_eq!(res.tail.unwrap()[0].number, 2);
    }

    #[test]
    fn query_at_buffered_root_returns_data() {
        let c = chain_of(5);
        let res = c.query(0, None).unwrap();
        let tail = res.tail.expect("a root is served from the window");
        assert_eq!(tail[0].number, 0);
    }

    #[test]
    fn query_below_first_returns_empty() {
        let mut c = Chain::new(make_block(10, "h10", 9, "h9"), 100, false);
        // from=5 <= parent_number(9) of the first block
        // wait — first block is 10, parent_number is 9.  from=9 → empty.
        let res = c.query(9, None).unwrap();
        assert!(res.tail.is_none() && res.head.is_none() && res.finalized_head.is_none());
        // from=8 → also empty
        let res2 = c.query(8, None).unwrap();
        assert!(res2.tail.is_none());
        // finalize so we can test the second branch
        c.finalize(&BlockRef {
            number: 10,
            hash: "h10".into(),
        })
        .unwrap();
    }

    #[test]
    fn query_hash_mismatch_in_range() {
        let c = chain_of(5);
        let err = c.query(2, Some("wrong_hash")).unwrap_err();
        // prev should contain refs including the block at pos 2.
        assert!(!err.prev.is_empty());
    }

    #[test]
    fn query_hash_match_in_range() {
        let c = chain_of(5);
        // block 2 has parent_hash "h1"
        let res = c.query(2, Some("h1")).unwrap();
        assert!(res.tail.is_some());
    }

    #[test]
    fn query_from_past_end_wrong_hash() {
        let c = chain_of(5); // last block is 4, hash "h4"
        let err = c.query(5, Some("wrong")).unwrap_err();
        assert!(!err.prev.is_empty());
        // refs should be block refs (number, hash), not parent refs.
        assert_eq!(err.prev.last().unwrap().number, 4);
    }

    #[test]
    fn query_from_past_end_no_hash() {
        let c = chain_of(5);
        let res = c.query(5, None).unwrap();
        assert!(res.tail.is_none());
        assert!(res.finalized_head.is_some());
    }

    #[test]
    fn query_prev_block_window_max_100() {
        // Build a chain of 200 blocks.
        let mut c = Chain::new(genesis(), 1000, false);
        for i in 1..200u64 {
            c.push(make_block(
                i,
                &format!("h{i}"),
                i - 1,
                &format!("h{}", i - 1),
            ))
            .unwrap();
        }
        // Query at block 150 with wrong hash.
        let err = c.query(150, Some("bad")).unwrap_err();
        // Should return at most 101 refs (pos - 100 ..= pos).
        assert!(err.prev.len() <= 101);
    }

    // ---- get_fork_base ---------------------------------------------------

    #[test]
    fn fork_base_found() {
        let c = chain_of(5); // 0..=4 with hashes h0..h4
        let prev = vec![
            BlockRef {
                number: 3,
                hash: "h3".into(),
            },
            BlockRef {
                number: 4,
                hash: "h4_fork".into(),
            }, // diverges here
        ];
        let base = c.get_fork_base(&prev).unwrap();
        assert_eq!(base.number, 3);
        assert_eq!(base.hash, "h3");
    }

    #[test]
    fn fork_base_not_found_below_finalized() {
        let mut c = chain_of(5);
        c.finalize(&BlockRef {
            number: 4,
            hash: "h4".into(),
        })
        .unwrap();
        // All upstream blocks are unknown (different hashes).
        let prev = vec![
            BlockRef {
                number: 0,
                hash: "alien0".into(),
            },
            BlockRef {
                number: 1,
                hash: "alien1".into(),
            },
        ];
        assert!(c.get_fork_base(&prev).is_none());
    }

    #[test]
    fn fork_base_upstream_all_unknown_returns_our_head() {
        let c = chain_of(5);
        // When no upstream refs match but we pop them all, we return our head.
        let prev: Vec<BlockRef> = vec![];
        // empty prev means fh=None immediately → returns our top.
        let base = c.get_fork_base(&prev);
        // With empty prev, fh is None from the start → head = our top block.
        assert!(base.is_some());
        assert_eq!(base.unwrap().number, 4);
    }
}
