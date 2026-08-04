# 04 — Mutations & transitions

Bands: WP-1..9 loop & discipline, WP-10..19 input handling & acquisition duties,
WP-20..29 transition catalog.

## The ingestion loop

**WP-1 — Single writer.** [MUST] Exactly one logical writer mutates the chain buffer.
Queries never mutate it; background/maintenance work never changes its logical content
(INV-10). All transitions below are steps of this one writer.

**WP-2 — The loop.** Conceptually:

```
C ← T1-INIT()                          // seed at upstream finalized head
S ← (base: ref(head(C)), stalled: 0)
loop:
  stream ← adapter.stream(from: S.base.number+1, parentHash: S.base.hash)
  for batch in stream:                 // until end, error, or fork signal
      apply(batch)                     // T2/T3 per block, then T4, T5 — one atomic step
      S.base ← ref(head(C))
  on fork signal(prev):   S.base ← rebase(prev)        // WP-10; stalled ← 0
  on error / end:         WP-9 ladder                  // maybe backoff, maybe T1 re-seed
```

**WP-3 — Batch atomicity & visibility.** [MUST] All effects of one input batch — its
appends/reorgs, its finality advance, and the resulting eviction — become visible to
readers as one committed state (INV-16). Readers never observe a half-applied batch.
A batch that violates DEF-20's shape (ascending, pairwise linked) is rejected before
anything mutates, not discovered block by block.
The commit point is the atomic publication of the new buffer state; there is no
acknowledgement to the adapter — redelivered blocks are absorbed idempotently (WP-13).

**WP-4 — Positioning.** [MUST] Every input stream is opened exactly one block above a
ref the service holds: `from = base.number + 1`, `parentHash = base.hash`, where `base`
is the current head — or, after a fork signal, the rebase target (WP-10). The service
never requests a range it cannot link to its buffer.

**WP-5 — Input validation at the buffer.** [MUST] A block enters the buffer only via
T2 or T3; every other shape of input relative to the current buffer — a gap (parent
height above head with no buffered parent), a parent-hash mismatch at a buffered
height, an attempt to modify at or below `finalized(C)` — is an **integrity violation**:
the batch is rejected whole (no partial application), the condition is alarmed (OB-7),
and the session is torn down per WP-9. It is never applied partially, never
process-fatal, and never silently dropped (INV-41, ADR-11 ⚠ pending ratification).

**WP-6 — Duplicate absorption & equivocation.** [MUST] Re-delivery of a block already
buffered with the same hash and same parent link (e.g. after session restart or fork
replay) is a strict no-op: the buffer — including everything above the duplicate — is
unchanged, and no reader observes any effect. This holds at every position, including
at or below `finalized(C)`. Treating a duplicate as a reorganization (truncating the
newer suffix, or tripping the finality guard) is non-conforming. Idempotency
is required under at-least-once delivery (DEF-20).

A delivery whose ref matches a buffered block but whose parent link differs is neither
a duplicate nor a reorganization: it is **equivocation** — one hash claiming two
ancestries (DEF-8) — and is an integrity violation (WP-5 handling). Applying
it as a reorg would leave the ref unchanged while its history silently changed, which
no client can detect: the conflict protocol compares parent hashes of *later* blocks
(RP-11), and those still link.

**WP-7 — No content-based rejection.** [MUST] The buffer layer judges blocks only by
linkage (DEF-5) and refs; payload content never causes rejection at this layer.
Content-level gates live in acquisition (WP-11) — by the time a block reaches the
buffer it is coherent or it does not arrive.

**WP-8 — Frame condition.** [MUST] The buffer changes only through T1–T5 in response
to input events or configuration-sanctioned eviction. No timer, query, metric scrape,
or maintenance activity changes `(B, f)` (INV-10).

**WP-9 — Session restart ladder.** [MUST] Errors reaching the ladder are
classified per FM-2. When a session ends in error or end-of-stream:
if the head advanced during the session, `stalled ← 0`, else `stalled ← stalled + 1`.
Restart delay: none for the first `P-STALL-FREE-RETRIES` consecutive stalled sessions,
then `P-SESSION-BACKOFF` between attempts. After `P-STALL-REINIT` consecutive stalled
sessions, the service abandons the buffer and re-seeds via T1 (self-healing reset),
alarming the reset (OB-7). A session that fails before the *first ever* block is
ingested is a startup failure (FM-31). The ladder never terminates silently: it either
converges (head advances) or alarms persistently (LIV-2).

## Input handling & acquisition duties

**WP-10 — Fork rebase.** [MUST] On a fork signal with refs `prev` (non-empty, DEF-21):
the service searches its buffer top-down, at or above `finalized(C)` — the finalized
block itself is a legal base; divergence starts strictly below it — for the newest
buffered block whose ref appears in `prev`; that ref becomes the new session base
(T6, WP-25). When no signalled ref matches any buffered block — an adapter may signal
refs the service never buffered — the base is the newest buffered block, still at or
above `finalized(C)`, strictly below the lowest signalled ref: **stepwise descent**
(ADR-15). Each such signal moves the base at least one block down, so repeated
signals either reach the true fork point or exhaust the unfinalized suffix.
Nothing is deleted at rebase time — truncation happens only when a replacement block
arrives (T3), so readers keep a consistent chain throughout. If neither a match nor a
descent target exists at or above `finalized(C)`, the divergence is **below
finality**: an unrecoverable contradiction handled per FM-30 (terminal alarm), never
by silently continuing.
A fork signal resets `stalled` to 0 and is never counted as an error. A fork signal
with empty `prev` is malformed input from the adapter: treated as a session error
(WP-9), not as a rebase — it MUST NOT produce a rebase to the current head (the
livelock of GAP-5).

**WP-11 — Acquisition duties.** The acquisition adapter, per block:
1. [MUST] acquire every selected component and establish coherence (DEF-15) before
   emitting the block;
2. [MUST] on incoherence or a missing/not-yet-available component, retry acquisition —
   re-fetching *all* components including the header (so a superseded block heals to
   its replacement) — at most `P-ENRICH-RETRIES` times with `P-ENRICH-DELAY` between
   attempts, in every acquisition mode (head-following *and* range backfill — GAP-11);
3. [MUST] on retry exhaustion, emit a stream error naming the block (fail-loud; the
   loop's WP-9 ladder takes over) — never emit the block with defaulted, emptied, or
   partial components (GAP-3);
4. [MUST] apply the enabled verification checks (DEF-25) as coherence gates — their
   failures take the same bounded retry-then-fail-loud path as any other incoherence,
   never an immediate session error (GAP-32);
5. [MUST] deliver batches in ascending order, pairwise linked, splitting at any
   discontinuity: blocks before an upstream-reported divergence are delivered, then the
   fork signal (a client of the adapter never receives an unlinked sequence);
6. [SHOULD] attach its newest finality knowledge to batches opportunistically (ADR-6)
   without ever delaying block delivery for it.

**WP-12 — Finality arbitration.** [MUST] The service maintains the maximum finality
report seen in the current session — one ref, no history — and applies T4 with it.
Regressive reports (lower than an already-applied finality) are ignored without error.
A report naming a buffered height with a different hash than the buffered block is an
integrity violation (WP-5 handling — alarm + session teardown, not process death).
So is a report naming the current maximum's height under a different hash: at
most one of the two can match the block when it arrives, so the contradiction is
decidable the moment the second report lands, held block or not. A report above the
buffered head finalizes the entire buffer and is re-validated when the named block
arrives (INV-12's check note); a higher report replaces it, and the replaced
obligation's own check is not owed (ADR-16 ⚠ pending ratification).

**WP-13 — Commit & redelivery.** [MUST] There is no partial visibility and no replay
journal: the committed buffer state *is* the only state. After any session restart the
service re-requests from its committed head (WP-4); redelivered blocks absorb per WP-6.

**WP-14 — Robustness.** [MUST] No input content — malformed, oversized, adversarial,
or absurd — may terminate the process or degrade it into a permanently failing state
(FM-1, INV-41). Every integrity violation path above ends in: alarm + session teardown
+ ladder, with the buffer intact or reset via T1.

**WP-15 — Restart continuity.** [MUST] The process recovers by construction: T1 seeds
from the upstream finalized head, which by INV-11's contract can never be reorged away
upstream. There is no local recovered state and therefore no recovery divergence
(INV-40). Anything a client held from before the restart remains valid under REQ-1/2
semantics (their next request either streams or conflicts).

## Transition catalog

Notation: state before `C = (⟨b₁…bₙ⟩, f)`; incoming block `x` (DEF-4).
DEF-33 tabulates the catalog.

**WP-20 — T1 INIT (and re-INIT).** *Pre:* upstream reachable; `fh = adapter.finalizedHead()`
resolves and the single block at `fh` is acquirable — and *is* `fh`: the acquired
block's ref MUST equal the reported one, since the two calls may land on different
nodes of a fleet and an unchecked block would become the buffer's finality anchor.
The acquired seed MUST also satisfy DEF-2 and DEF-4 before it enters the buffer.
A mismatch, a malformed seed, an empty seed batch, and a multi-block seed batch are
source faults — ladder (WP-9), or FM-31 at startup, never a process fault. *Post:*
`C = (⟨block(fh)⟩, f = 1)`; session `base = fh`, `stalled = 0`. Failure of T1 at
process start is a startup failure (exit, FM-31); failure of a re-INIT (WP-9 ladder)
re-enters the ladder. T1 discards any previous buffer entirely; it is the sanctioned
destructive reset and MUST be alarmed when it discards a non-trivial buffer (OB-7).
A re-INIT ends the current epoch and starts the next (INV-12, ADR-14): `fh` below the
previous epoch's finalized head is permitted — FM-26 lets upstream's finalized head
oscillate — and MUST be alarmed as a watermark regression. `fh` naming a height the
buffer being discarded holds under a different ref, or under the same ref with a
different parent link (DEF-8), is unrecoverable divergence (FM-30), not a re-seed;
outside that buffer the check has nothing to run against and is not owed.

**WP-21 — T2 APPEND.** *Pre:* `linked(bₙ, x)`. *Post:*
`B' = ⟨b₁…bₙ, x⟩`, `f' = f`. The only way the chain grows.

**WP-22 — T3 REORG.** *Pre:* ∃ unique `i` with `bᵢ.number = x.parentNumber`; require
`i ≥ f` (finalized protection — INV-11), `linked(bᵢ, x)`. *Post:*
`B' = ⟨b₁…bᵢ, x⟩`, `f' = f`. Blocks `b_{i+1}…bₙ` are discarded — this and T5/T1 are the
only paths by which buffered data leaves (INV-14). Violation of any precondition is an
integrity violation (WP-5).

**WP-23 — T4 FINALIZE.** Runs on a report attached opportunistically to a non-empty
input batch (DEF-20, ADR-6). The confirmation prober never emits a standalone
finality-only batch. With report `r` (post-arbitration, WP-12): if
`r.number < b₁.number` → no-op. If `r.number > bₙ.number` → `f' = n` (whole buffer;
see WP-12). Else require ∃ `i ≥ f`: `bᵢ.number = r.number ∧ bᵢ.hash = r.hash` →
`f' = max(f, i)`; a hash mismatch is an integrity violation (WP-5). `f` never
decreases (INV-12).

**WP-24 — T5 COMPACT.** Runs after T4 within the same atomic step (WP-3). Let
`excess = max(0, n − P-CACHE-SIZE)`. Evict `k = min(excess, f − 1)` oldest blocks:
`B' = ⟨b_{k+1}…bₙ⟩`, `f' = f − k`. If `excess > k` (finality lags): with
`autoAdjust = false`, the over-window state persists and MUST raise the standing alarm
OB-6; with `autoAdjust = true`, first `f ← excess + 1`, then evict as above, restoring
the bound. Eviction never removes an unfinalized block and never anything except the
oldest prefix (INV-14).

The force-advance is the sanctioned finality override (DEF-24, ADR-9): it declares
potentially-unfinalized blocks irreversible, trading rollback safety for the memory
bound, and MUST be alarmed per advance (OB-6). Afterwards INV-11 protects the advanced
position like any other, so a reorg deeper than the forced point becomes FM-19/FM-30 —
the accepted risk, made loud.

**WP-25 — T6 REBASE.** *Pre:* fork signal handled per WP-10, target `t` found. *Post:*
buffer unchanged; `S.base = t`, `stalled = 0`. (Included for completeness: T6 mutates
only session state.)
