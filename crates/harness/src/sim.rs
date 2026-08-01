//! Deterministic input simulator (HC-1) with a provenance ledger (HC-2).
//!
//! `SimChain` derives every hash and payload from a seed, so a failing run is
//! reproducible from its seed alone. `SimSource` implements the service's
//! `DataSource` and records every batch it actually delivers into the shared
//! [`Ledger`] at yield time — the ledger is the ground truth tests replay into
//! the reference model and diff served bytes against.

use crate::model::{ApplyOutcome, ModelBlock, RefModel};
use async_trait::async_trait;
use bytes::Bytes;
use data_service_core::source::{BlockBatch, DataSource, StreamError, StreamRequest};
use data_service_core::types::{Block, BlockRef};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn hash_at(seed: u64, number: u64, branch: u64) -> String {
    format!(
        "0x{:016x}",
        splitmix64(seed ^ number.wrapping_mul(0x0100_0000_01B3) ^ branch.rotate_left(17))
    )
}

/// What one delivered batch looked like, in delivery order.
#[derive(Debug, Clone)]
pub struct BatchEvent {
    /// Exact model-relevant linkage tuples as delivered in this event. Keeping
    /// only `BlockRef` would collapse two deliveries that share `(number,
    /// hash)` but disagree on the parent link — precisely the equivocation the
    /// model must see.
    pub blocks: Vec<ModelBlock>,
    pub finalized: Option<BlockRef>,
    /// True for batches of the finalized (init/backfill) stream.
    pub finalized_stream: bool,
}

/// Provenance record for one produced block.
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub header: ModelBlock,
    /// The exact newline-terminated JSON line the payload frame decodes to.
    pub json_line: String,
}

/// Everything the simulator produced, in order (HC-2).
#[derive(Default)]
pub struct Ledger {
    pub entries: HashMap<ModelBlock, LedgerEntry>,
    pub events: Vec<BatchEvent>,
}

impl Ledger {
    pub fn line_of(&self, header: &ModelBlock) -> Option<&str> {
        self.entries.get(header).map(|e| e.json_line.as_str())
    }

    /// Replay the recorded delivery history into a fresh reference model.
    ///
    /// The first event must be the one-block T1 seed. In Phase 0's one-session
    /// histories, later finalized-stream events are read-path backfill
    /// deliveries (RP-8) — not ingest input — and are skipped. A future
    /// multi-session runner must tag T1 re-INIT deliveries separately instead
    /// of using this helper. Panics if a supported event does not apply
    /// cleanly.
    pub fn replay(&self, cache_size: usize, auto_adjust: bool) -> RefModel {
        let mut events = self.events.iter();
        let seed = events.next().expect("ledger holds no events");
        assert!(
            seed.finalized_stream && seed.blocks.len() == 1,
            "first delivery must be the one-block T1 seed"
        );
        let mut model = RefModel::init(seed.blocks[0].clone(), cache_size, auto_adjust);
        for (i, ev) in events.enumerate() {
            if ev.finalized_stream {
                continue;
            }
            assert_eq!(
                model.apply_batch(&ev.blocks, ev.finalized.as_ref()),
                ApplyOutcome::Applied,
                "replay event {} did not apply cleanly",
                i + 1
            );
        }
        model
    }
}

/// A deterministic canonical chain derived from a seed.
pub struct SimChain {
    pub seed: u64,
    blocks: Vec<Block>,
}

impl SimChain {
    pub fn generate(seed: u64, len: u64) -> Self {
        let mut blocks = Vec::with_capacity(len as usize);
        for n in 0..len {
            // Block 0 is the root: parentNumber = number, parent hash names
            // no block (DEF-4's sole exception).
            let hash = hash_at(seed, n, 0);
            let parent_hash = if n == 0 {
                "0x0".to_string()
            } else {
                hash_at(seed, n - 1, 0)
            };
            let pn = if n == 0 { 0 } else { n - 1 };
            let line = format!(
                "{{\"number\":{n},\"parentNumber\":{pn},\"hash\":\"{hash}\",\"parentHash\":\"{parent_hash}\",\"seed\":{seed}}}\n"
            );
            blocks.push(Block {
                number: n,
                hash,
                parent_number: pn,
                parent_hash,
                timestamp: Some(n * 1000),
                json_line_zstd: Bytes::from(zstd::encode_all(line.as_bytes(), 1).unwrap()),
                timings: None,
            });
        }
        SimChain { seed, blocks }
    }

    pub fn header(&self, n: u64) -> ModelBlock {
        let b = &self.blocks[n as usize];
        ModelBlock {
            number: b.number,
            hash: b.hash.clone(),
            parent_number: b.parent_number,
            parent_hash: b.parent_hash.clone(),
        }
    }

    pub fn block_ref(&self, n: u64) -> BlockRef {
        self.blocks[n as usize].block_ref()
    }

    pub fn len(&self) -> u64 {
        self.blocks.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

/// Adapter-level simulator: serves one canonical chain, head batches of
/// `batch_size`, finality reports trailing the delivered head by `fin_lag`.
/// After the chain is drained the head stream stays pending (a healthy idle
/// head), so the smoke run has exactly one session.
pub struct SimSource {
    chain: Arc<Vec<Block>>,
    ledger: Arc<Mutex<Ledger>>,
    pub batch_size: usize,
    pub fin_lag: u64,
    /// Height reported by `get_finalized_head` (the T1 seed position).
    pub init_finalized: u64,
}

impl SimSource {
    pub fn new(sim: &SimChain, batch_size: usize, fin_lag: u64, init_finalized: u64) -> Self {
        SimSource {
            chain: Arc::new(sim.blocks.clone()),
            ledger: Arc::new(Mutex::new(Ledger::default())),
            batch_size,
            fin_lag,
            init_finalized,
        }
    }

    pub fn ledger(&self) -> Arc<Mutex<Ledger>> {
        Arc::clone(&self.ledger)
    }

    /// Record a batch at yield time: only what was actually delivered enters
    /// the ledger — the provenance oracle must not vouch for blocks the
    /// simulator never handed out. The vouched bytes come from the delivered
    /// block itself, so a fork-branch delivery vouches its own payload, not
    /// its canonical sibling's.
    fn record(ledger: &Mutex<Ledger>, batch: &BlockBatch, finalized_stream: bool) {
        let mut lg = ledger.lock().unwrap();
        let mut headers = Vec::with_capacity(batch.blocks.len());
        for b in &batch.blocks {
            let header = ModelBlock {
                number: b.number,
                hash: b.hash.clone(),
                parent_number: b.parent_number,
                parent_hash: b.parent_hash.clone(),
            };
            let json_line = String::from_utf8(
                zstd::decode_all(b.json_line_zstd.as_ref())
                    .expect("sim block payload is a zstd frame"),
            )
            .expect("sim block payload is UTF-8");
            match lg.entries.entry(header.clone()) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(LedgerEntry {
                        header: header.clone(),
                        json_line,
                    });
                }
                std::collections::hash_map::Entry::Occupied(slot) => {
                    assert_eq!(
                        slot.get().json_line,
                        json_line,
                        "simulator produced different bytes for the same model linkage tuple"
                    );
                }
            }
            headers.push(header);
        }
        lg.events.push(BatchEvent {
            blocks: headers,
            finalized: batch.finalized_head.clone(),
            finalized_stream,
        });
    }
}

#[async_trait]
impl DataSource for SimSource {
    async fn get_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain.last().unwrap().block_ref())
    }

    async fn get_finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self.chain[self.init_finalized as usize].block_ref())
    }

    fn get_finalized_stream(
        &self,
        req: StreamRequest,
    ) -> futures::stream::BoxStream<'static, Result<BlockBatch, StreamError>> {
        let chain = Arc::clone(&self.chain);
        let ledger = Arc::clone(&self.ledger);
        Box::pin(async_stream::stream! {
            let mut positioned = false;
            for block in chain.iter() {
                if block.number < req.from { continue; }
                if let Some(t) = req.to { if block.number > t { break; } }
                // DEF-21: with a parentHash given, the first block must link
                // to it; a mismatch is a fork signal naming the parent the
                // stream actually continues from (RP-8's base check).
                if !positioned {
                    positioned = true;
                    if let Some(ph) = &req.parent_hash {
                        if *ph != block.parent_hash {
                            yield Err(StreamError::Fork {
                                previous_blocks: vec![BlockRef {
                                    number: block.parent_number,
                                    hash: block.parent_hash.clone(),
                                }],
                            });
                            return;
                        }
                    }
                }
                let batch = BlockBatch {
                    blocks: vec![block.clone()],
                    finalized_head: Some(block.block_ref()),
                };
                Self::record(&ledger, &batch, true);
                yield Ok(batch);
            }
        })
    }

    fn get_stream(
        &self,
        req: StreamRequest,
    ) -> futures::stream::BoxStream<'static, Result<BlockBatch, StreamError>> {
        let chain = Arc::clone(&self.chain);
        let ledger = Arc::clone(&self.ledger);
        let batch_size = self.batch_size.max(1);
        let fin_lag = self.fin_lag;
        Box::pin(async_stream::stream! {
            let mut positioned = false;
            let mut batch: Vec<Block> = vec![];
            for block in chain.iter() {
                if block.number < req.from { continue; }
                if let Some(t) = req.to { if block.number > t { break; } }
                // WP-4: the SUT opens the head stream exactly one block above
                // a ref it holds. The canonical chain never forks, so a
                // mismatched base can only be a mis-positioned stream — a SUT
                // bug the harness must fail loudly on, not signal a fork for
                // (the SUT would "recover" and mask it).
                if !positioned {
                    positioned = true;
                    if let Some(ph) = &req.parent_hash {
                        assert_eq!(
                            *ph, block.parent_hash,
                            "sim: WP-4 positioning violated — stream from {} \
                             opened against a base that is not block {}'s parent",
                            req.from, block.number
                        );
                    }
                }
                batch.push(block.clone());
                if batch.len() == batch_size {
                    let last = batch.last().unwrap().number;
                    let fin = last.checked_sub(fin_lag)
                        .map(|n| chain[n as usize].block_ref());
                    let out = BlockBatch { blocks: std::mem::take(&mut batch), finalized_head: fin };
                    Self::record(&ledger, &out, false);
                    yield Ok(out);
                }
            }
            if !batch.is_empty() {
                let last = batch.last().unwrap().number;
                let fin = last.checked_sub(fin_lag).map(|n| chain[n as usize].block_ref());
                let out = BlockBatch { blocks: std::mem::take(&mut batch), finalized_head: fin };
                Self::record(&ledger, &out, false);
                yield Ok(out);
            }
            if req.to.is_some() {
                // A bounded request ends at `to`; only the unbounded head
                // stream idles to keep the session open.
                return;
            }
            // Chain drained: idle at the head, keep the session open.
            futures::future::pending::<()>().await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mb(n: u64, h: &str, pn: u64, ph: &str) -> ModelBlock {
        ModelBlock {
            number: n,
            hash: h.into(),
            parent_number: pn,
            parent_hash: ph.into(),
        }
    }

    fn rf(n: u64, h: &str) -> BlockRef {
        BlockRef {
            number: n,
            hash: h.into(),
        }
    }

    fn ledger_with(headers: &[ModelBlock], events: Vec<BatchEvent>) -> Ledger {
        let mut lg = Ledger::default();
        for h in headers {
            lg.entries.insert(
                h.clone(),
                LedgerEntry {
                    header: h.clone(),
                    json_line: String::new(),
                },
            );
        }
        lg.events = events;
        lg
    }

    fn ev(blocks: Vec<ModelBlock>, finalized: Option<BlockRef>, fs: bool) -> BatchEvent {
        BatchEvent {
            blocks,
            finalized,
            finalized_stream: fs,
        }
    }

    #[test]
    fn replay_uses_delivered_headers_not_canonical_ones() {
        // Height 51 was delivered twice: canonical `a`, then fork sibling `b`.
        // Replay keyed by height alone would apply `a` twice (a no-op) and
        // miss the reorg; preserving the delivered hash must land on `b`.
        let h50 = mb(50, "h50", 49, "h49");
        let a51 = mb(51, "a", 50, "h50");
        let b51 = mb(51, "b", 50, "h50");
        let lg = ledger_with(
            &[h50.clone(), a51.clone(), b51.clone()],
            vec![
                ev(vec![h50], Some(rf(50, "h50")), true),
                ev(vec![a51], None, false),
                ev(vec![b51], None, false),
            ],
        );
        let m = lg.replay(100, false);
        assert_eq!(m.head(), rf(51, "b"));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn replay_skips_read_path_backfill_deliveries() {
        // A below-window query records finalized-stream deliveries into the
        // same ledger; they are not ingest input and must not disturb replay.
        let h50 = mb(50, "h50", 49, "h49");
        let a51 = mb(51, "a", 50, "h50");
        let h0 = mb(0, "h0", 0, "0x0");
        let h1 = mb(1, "h1", 0, "h0");
        let lg = ledger_with(
            &[h50.clone(), a51.clone(), h0.clone(), h1.clone()],
            vec![
                ev(vec![h50], Some(rf(50, "h50")), true),
                ev(vec![a51], None, false),
                ev(vec![h0], Some(rf(0, "h0")), true),
                ev(vec![h1], Some(rf(1, "h1")), true),
            ],
        );
        let m = lg.replay(100, false);
        assert_eq!(m.head(), rf(51, "a"));
        assert_eq!(m.finalized(), rf(50, "h50"));
    }

    #[test]
    fn replay_preserves_parent_equivocation_for_the_same_block_ref() {
        fn block(h: &ModelBlock) -> Block {
            let line = format!(
                "{{\"number\":{},\"parentNumber\":{},\"hash\":\"{}\",\"parentHash\":\"{}\"}}\n",
                h.number, h.parent_number, h.hash, h.parent_hash
            );
            Block {
                number: h.number,
                hash: h.hash.clone(),
                parent_number: h.parent_number,
                parent_hash: h.parent_hash.clone(),
                timestamp: None,
                json_line_zstd: Bytes::from(zstd::encode_all(line.as_bytes(), 1).unwrap()),
                timings: None,
            }
        }

        let root = mb(0, "h0", 0, "0x0");
        let one = mb(1, "h1", 0, "h0");
        let two = mb(2, "h2", 1, "h1");
        let forged_two = mb(2, "h2", 0, "h0");
        let ledger = Mutex::new(Ledger::default());

        SimSource::record(
            &ledger,
            &BlockBatch {
                blocks: vec![block(&root)],
                finalized_head: Some(rf(0, "h0")),
            },
            true,
        );
        SimSource::record(
            &ledger,
            &BlockBatch {
                blocks: vec![block(&one), block(&two)],
                finalized_head: None,
            },
            false,
        );
        SimSource::record(
            &ledger,
            &BlockBatch {
                blocks: vec![block(&forged_two)],
                finalized_head: None,
            },
            false,
        );

        let model = ledger.into_inner().unwrap().replay(100, false);
        assert_eq!(model.len(), 2, "the forged parent must trigger a reorg");
        assert_eq!(model.head(), rf(2, "h2"));
        assert_eq!(model.blocks().last().unwrap().parent_number, 0);
    }
}
