//! In-memory chain buffer — exact port of `chain.ts`.

use crate::types::{Block, BlockHeader, BlockRef, DataResponse, InvalidBaseBlock};

/// Returns true if `a` is the direct parent of `b`.
fn is_chain(a: &Block, b: &Block) -> bool {
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

/// An in-memory sliding window of recent blocks, with a finalized-head pointer.
///
/// Mirrors the TypeScript `Chain` class from `chain.ts`.
pub struct Chain {
    blocks: Vec<Block>,
    /// Index into `blocks` of the current finalized head.
    finalized_head: usize,
    max_size: usize,
    auto_adjust_finalized_head: bool,
}

impl Chain {
    pub fn new(base: Block, max_size: usize, auto_adjust_finalized_head: bool) -> Self {
        assert!(max_size > 0, "max_size must be > 0");
        Self {
            blocks: vec![base],
            finalized_head: 0,
            max_size,
            auto_adjust_finalized_head,
        }
    }

    // ----- mutation -------------------------------------------------------

    /// Push a new block onto the chain, potentially triggering a reorg.
    ///
    /// Mirrors `Chain.push` in chain.ts.
    pub fn push(&mut self, new_block: Block) {
        if self.last_block().number == new_block.parent_number {
            assert!(
                is_chain(self.last_block(), &new_block),
                "chain hash mismatch on sequential push"
            );
            self.blocks.push(new_block);
            return;
        }

        let pos = bisect(&self.blocks, new_block.parent_number);
        assert!(
            pos >= self.finalized_head,
            "attempt to revert finalized head"
        );
        assert!(
            pos < self.blocks.len(),
            "there is a gap between received block and the current head"
        );

        let prev = &self.blocks[pos];
        assert!(
            is_chain(prev, &new_block),
            "chain hash mismatch on reorg push"
        );
        self.blocks.truncate(pos + 1);
        self.blocks.push(new_block);
    }

    /// Advance the finalized head pointer to `head`.
    ///
    /// Mirrors `Chain.finalize` in chain.ts.
    /// Returns `true` if the finalized head actually advanced.
    pub fn finalize(&mut self, head: &BlockRef) -> bool {
        if head.number < self.first_block_number() {
            return false;
        }

        let current = self.finalized_head;

        if head.number > self.last_block_number() {
            // Finalize everything — safe per DataSource stream guarantees.
            self.finalized_head = self.blocks.len() - 1;
            return self.finalized_head > current;
        }

        let pos = if head.number == self.last_block_number() {
            self.blocks.len() - 1
        } else {
            bisect(&self.blocks, head.number)
        };

        assert!(
            self.blocks[pos].number == head.number && self.blocks[pos].hash == head.hash,
            "attempt to finalize a block that is not part of the current chain"
        );

        self.finalized_head = self.finalized_head.max(pos);
        self.finalized_head > current
    }

    /// Trim old finalized blocks to keep the buffer at or below `max_size`.
    ///
    /// Returns `true` if the buffer is within bounds (or was trimmed to fit).
    /// Returns `false` if trimming is blocked by a lagging finalized head and
    /// `auto_adjust_finalized_head` is disabled.
    ///
    /// Mirrors `Chain.compact` in chain.ts.
    pub fn compact(&mut self) -> bool {
        let extra = self.blocks.len().saturating_sub(self.max_size);
        if extra == 0 {
            return true;
        }

        let mut ok = self.finalized_head >= extra;
        if !ok && self.auto_adjust_finalized_head {
            let new_last = &self.blocks[extra - 1];
            tracing::warn!(
                block_number = new_last.number,
                block_hash = %new_last.hash,
                "finalized head was adjusted automatically to block #{}",
                new_last.number
            );
            self.finalized_head = extra;
            ok = true;
        }

        let trim = extra.min(self.finalized_head);
        self.blocks.drain(..trim);
        self.finalized_head -= trim;
        ok
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
        // Caller should do a below-query.
        if from <= self.first_block().parent_number {
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
                    let prev: Vec<BlockRef> = self.blocks[start..=pos]
                        .iter()
                        .map(|b| BlockRef {
                            number: b.parent_number,
                            hash: b.parent_hash.clone(),
                        })
                        .collect();
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

    pub fn last_block(&self) -> &Block {
        self.blocks.last().expect("chain is never empty")
    }

    pub fn first_block_number(&self) -> u64 {
        self.blocks[0].number
    }

    pub fn last_block_number(&self) -> u64 {
        self.last_block().number
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
            ));
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

    #[test]
    #[should_panic(expected = "chain hash mismatch on sequential push")]
    fn push_bad_hash() {
        let mut c = Chain::new(genesis(), 100, false);
        // Wrong parent hash for block 1.
        c.push(make_block(1, "h1", 0, "wrong"));
    }

    // ---- push / reorg ----------------------------------------------------

    #[test]
    fn push_reorg() {
        let mut c = chain_of(5); // 0..=4
                                 // Reorg: replace blocks 3 and 4 with an alternate chain.
        c.push(make_block(3, "h3b", 2, "h2")); // reorg at pos 3
        assert_eq!(c.size(), 4);
        assert_eq!(c.last_block().hash, "h3b");
        // Now extend.
        c.push(make_block(4, "h4b", 3, "h3b"));
        assert_eq!(c.size(), 5);
    }

    #[test]
    #[should_panic(expected = "attempt to revert finalized head")]
    fn push_reorg_below_finalized() {
        let mut c = chain_of(5);
        c.finalize(&BlockRef {
            number: 3,
            hash: "h3".into(),
        });
        // Try to reorg back to block 2.
        c.push(make_block(3, "h3b", 2, "h2"));
    }

    #[test]
    #[should_panic(expected = "there is a gap between received block and the current head")]
    fn push_gap() {
        let mut c = chain_of(5);
        // Block 10 is way ahead — gap.
        c.push(make_block(10, "h10", 9, "h9"));
    }

    // ---- finalize --------------------------------------------------------

    #[test]
    fn finalize_advances() {
        let mut c = chain_of(5);
        assert!(c.finalize(&BlockRef {
            number: 3,
            hash: "h3".into()
        }));
        assert_eq!(c.get_finalized_head().number, 3);
    }

    #[test]
    fn finalize_noop_below_first() {
        let c = chain_of(5);
        // Block 0's parent number is 0, which is the first block number.
        // A head.number < first_block_number (0) would be impossible here.
        // Test: finalized head number below first block number.
        let mut c2 = Chain::new(make_block(10, "h10", 9, "h9"), 100, false);
        // head.number = 5 < first_block_number (10) → no-op
        assert!(!c2.finalize(&BlockRef {
            number: 5,
            hash: "h5".into()
        }));
        assert_eq!(c2.get_finalized_head().number, 10);
        let _ = c;
    }

    #[test]
    fn finalize_above_last_finalizes_all() {
        let mut c = chain_of(5); // blocks 0..=4
        assert!(c.finalize(&BlockRef {
            number: 100,
            hash: "nonexistent".into()
        }));
        assert_eq!(c.get_finalized_head().number, 4);
    }

    #[test]
    fn finalize_monotonic() {
        let mut c = chain_of(10);
        c.finalize(&BlockRef {
            number: 5,
            hash: "h5".into(),
        });
        // Finalize at a lower number — must be a no-op (returns false).
        assert!(!c.finalize(&BlockRef {
            number: 3,
            hash: "h3".into()
        }));
        assert_eq!(c.get_finalized_head().number, 5);
    }

    // ---- compact ---------------------------------------------------------

    #[test]
    fn compact_trims_finalized() {
        let mut c = chain_of(10); // blocks 0..=9
        c.finalize(&BlockRef {
            number: 7,
            hash: "h7".into(),
        });
        // max_size was set to 1000 in chain_of, use small max_size.
        let mut c2 = Chain::new(genesis(), 5, false);
        for i in 1..10u64 {
            c2.push(make_block(
                i,
                &format!("h{i}"),
                i - 1,
                &format!("h{}", i - 1),
            ));
        }
        c2.finalize(&BlockRef {
            number: 7,
            hash: "h7".into(),
        });
        // size is 10, max_size is 5 → need to trim 5 entries.
        assert!(c2.compact());
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
            ));
        }
        // finalized head still at 0, size=5 > max=3 → cannot trim.
        assert!(!c.compact());
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
            ));
        }
        assert!(c.compact());
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
        });
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
            ));
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
        });
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
