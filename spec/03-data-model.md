# 03 — Data model & definitions

Bands: DEF-1..9 primitives, DEF-10..19 state, DEF-20..29 input events & policies,
DEF-30..39 derived/query concepts.

## Primitives

**DEF-1 — Height.** A block's position coordinate: a natural number ℕ. Heights ascend
along a chain but MAY skip values (chain families with gaps in the coordinate space are
admitted; see REQ-17). Arithmetic on heights is ordinary integer arithmetic.

**DEF-2 — Hash.** An opaque non-empty string identifying one block. Equality is exact
string equality; the core performs no normalization, decoding, or case folding.

**DEF-3 — BlockRef.** The pair `(number: Height, hash: Hash)`. Two refs are equal iff
both fields are equal.

**DEF-4 — Block.** The tuple
`(number: Height, hash: Hash, parentNumber: Height, parentHash: Hash, timestamp: ℕ|⊥, payload: Record)`
where `timestamp` is milliseconds since epoch (⊥ where the chain family has none) and
`payload` is the canonical record (DEF-6). `ref(b) = (b.number, b.hash)`,
`parentRef(b) = (b.parentNumber, b.parentHash)`. Required, without exception:
`b.parentNumber < b.number`. The buffer is seeded at the upstream *finalized* head
(T1), so a chain family's root block is never buffered and never an argument of any
transition; a chain younger than its own finality depth is out of scope, and a root
delivered as input is an integrity violation like any other malformed block (WP-5).

**DEF-5 — Parent link.** `linked(a, b) ≡ a.number = b.parentNumber ∧ a.hash = b.parentHash`.
This is the *only* adjacency relation in the core: contiguity of heights is NOT assumed
(DEF-1), linkage is.

**DEF-6 — Canonical record (payload).** The chain family's normalized serialization of
one block's data for the configured data selection (DEF-22): a single self-delimiting
textual record, deterministic per REQ-7. Stored and served in a per-block compressed
frame (REQ-6); the core treats it as opaque bytes. Field-level structure is bound per
chain family in 14 §payload.

**DEF-7 — Data component.** A named slice of a block's data acquired separately from
the header: the chain family defines the set (for EVM: transactions, receipts, event
logs, execution traces, state diffs). The data selection (DEF-22) picks a subset.

## State

**DEF-10 — Chain buffer.** The single principal entity. Abstract state:

```
C = (B, f)
B = ⟨b₁ … bₙ⟩        the buffered blocks
f ∈ 1..n             index of the finalized head within B
```

Well-formedness is defined by the structural invariants INV-1..INV-3 (single source of
truth in [07-invariants.md](07-invariants.md)): B non-empty, pairwise linked and
strictly ascending, f a valid index. The buffer is volatile: it exists only while the
process runs (NG1).

**DEF-11 — Watermarks.** Derived from C:
`head(C) = bₙ` — the newest buffered block (what "the chain head" means everywhere in
this suite); `first(C) = b₁` — the oldest buffered block; `finalized(C) = b_f` — the
newest block the service treats as irreversible. Each is exposed as its BlockRef.
`finalized(C)` asserts irreversibility, not retention: within the epoch no transition
*replaces* any of b₁..b_f. Finalized blocks leave the buffer only by T5's eviction of
the oldest finalized prefix (WP-24 — never b_f itself) or by T1, the sole alarmed
full reset, which is why T1 opens a new epoch (INV-11, WP-20, ADR-14).

**DEF-12 — Session.** The ingestion loop's (WP-2) cursor state, separate from C:
`S = (base: BlockRef, stalled: ℕ)` — `base` is the position above which the next input
stream is requested; `stalled` counts consecutive sessions that ended without advancing
the head (drives the restart ladder, WP-9).

**DEF-13 — Snapshot.** An immutable copy of `B` (or a suffix of it) plus
`finalized(C)`, taken atomically at query resolution: at admission, or — for a
request that waited (RP-4) — at its single post-wait re-resolution (INV-21). All of a
response's buffered blocks come from that one final snapshot.

**DEF-14 — Servable & response-eligible.** A block is *buffer-servable* when it is
an element of some committed buffer state (INV-16); blocks become buffer-servable only
via the transitions of 04, entire batches at a time (WP-3). A block is
*response-eligible* when it is buffer-servable **or** was acquired by the
window-underflow path (RP-8) and links gap-free into the response chain that ends at
the snapshot. Responses deliver only response-eligible blocks (RP-5); queries never
mutate the buffer, so backfilled blocks never become buffer-servable.

**DEF-15 — Component coherence.** The chain family's consistency predicate over one
block's acquired components. A block is *coherent* when every selected component is
present and all components describe the same block and agree on shared values (same
block identity in every component; counts match; per-block orderings/indices are
complete and consistent; cross-component sums/aggregates agree). The EVM instantiation
is bound in 14 §upstream (IB-15). Coherence is a gate for servability (WP-11), and it is
total: "component present but empty" satisfies coherence only if emptiness is itself
consistent with the other components.

## Input events & policies

**DEF-20 — Input batch.** The unit of ingestion the acquisition adapter delivers:
`(blocks: ⟨Block⟩, finalizedReport: BlockRef|⊥)`. `blocks` is non-empty, ascending, and
pairwise linked; `finalizedReport` is the adapter's newest knowledge of network
finality, attached opportunistically (ADR-6). Delivery is at-least-once in-order per
stream; a new stream may re-deliver blocks at or below a previous position.

**DEF-21 — Input stream & fork signal.** The adapter serves
`stream(from: Height, to: Height|⊥, parentHash: Hash|⊥)`: an ordered sequence of input
batches starting at the lowest available height ≥ `from`, each batch linked to the previous (and, when `parentHash`
is given, the first block's parentHash equal to it), terminated by end-of-range, an
error, or a **fork signal**. The fork signal reports that the upstream's canonical
chain diverges below `from`; it carries `prev: ⟨BlockRef⟩` — a non-empty ascending
sequence of refs on the upstream's canonical chain below the divergence, newest last.
The adapter also serves point reads `head()` and `finalizedHead()`.

**DEF-22 — Data selection policy.** The configured component subset (DEF-7) plus
acquisition-method choices the chain family exposes. Fixed at startup. Variants for EVM
are the configuration table of 14.

**DEF-23 — Finality policy.** Where `finalizedHead()` comes from: `network` (the
chain's own finality signal) or `offset(k)` (the newest block at height ≤ `head − k` —
exactly `head − k` on contiguous-height families, DEF-1) for chains without one.
Fixed at startup.

**DEF-24 — Retention policy.** `(P-CACHE-SIZE, autoAdjust: bool)` — the window size
(a target maximum buffered block count) and which of the two precedences applies when
lagging finality blocks eviction:

| autoAdjust | Precedence |
|---|---|
| off | finality dominates: the excess is retained and alarmed (ADR-9) |
| on | the bound dominates: finality is force-advanced to restore it (WP-24) |

Fixed at startup. The window is a sliding one — only the oldest finalized prefix is
ever evicted (WP-24, INV-14), and eviction is invisible to readers (INV-10). The size
trades memory against backfill traffic: a small window pushes resuming clients into
RP-8 constantly (HZ-5).

**DEF-25 — Verification policy.** The set of enabled optional integrity checks
(REQ-14). Fixed at startup.

Input event summary:

| Event | Content | Meaning | Handled by |
|---|---|---|---|
| batch | blocks + optional finalizedReport | extend/replace the chain above a linked position | T-APPEND / T-REORG, then T-FINALIZE, T-COMPACT |
| fork signal | prev refs | canonical chain diverges below the session base | WP-10 (rebase) |
| stream end / error | — | session over; input position may be stale | WP-9 (restart ladder) |
| finalizedReport (piggybacked) | BlockRef | network finality advanced | T-FINALIZE via WP-12 |

## Derived/query concepts

**DEF-30 — Coverage.** The block range a stream response delivers:
`[from, last]` where `last` is the last delivered block. A response's coverage is
always a contiguous-by-linkage prefix of what was requested, beginning at the lowest
available height ≥ `from`. This is the single rule everywhere a request names a start
(RP-3, RP-5, RP-11, INV-20): `from` itself need not name a block, since DEF-1 admits
gaps in the coordinate space — on families with contiguous heights the lowest such
height is `from` itself, which is a consequence, not a separate case. **Progress**: a successful
non-empty response advances the client by ≥ 1 block (INV-23); the client's next request
from `last.number + 1` with `last.hash` needs no other state — coverage is recoverable
purely from delivered blocks (REQ-1).

**DEF-31 — Conflict.** The query outcome "your claimed position is not on my chain",
carrying `prev: ⟨BlockRef⟩` per RP-7. A conflict is a normal protocol step, not a
server fault.

**DEF-32 — Readiness.** The predicate "the buffered head is at or above the upstream
head, as observed now" (RP-10). Oscillates by nature; it is a routing signal, not a
health signal.

**DEF-33 — Transition summary.** (semantics in [04-mutations.md](04-mutations.md)):

| Transition | One line |
|---|---|
| T1 INIT | seed a one-block buffer at the upstream finalized head |
| T2 APPEND | extend the head with a linked block |
| T3 REORG | truncate above a buffered parent, then append |
| T4 FINALIZE | advance f per a validated finality report |
| T5 COMPACT | evict a finalized prefix to keep the window bound |
| T6 REBASE | move the session cursor (fork/error); buffer unchanged |

## Terminology cross-reference

| Code / operational term | This suite |
|---|---|
| `Chain`, block cache, buffer | chain buffer (DEF-10) |
| `finalized_head` (index) | f in DEF-10 |
| `BlockRef` | DEF-3 |
| `jsonLineZstd` / stored frame | compressed canonical record (DEF-6, REQ-6) |
| `DataSource`, source | acquisition adapter (DEF-21) |
| `BlockBatch` | input batch (DEF-20) |
| `ForkException` / `StreamError::Fork`, `previousBlocks` | fork signal, `prev` (DEF-21, DEF-31) |
| `getForkBase` | rebase target search (WP-10) |
| `push` | T2/T3 |
| `finalize` | T4 |
| `compact` | T5 |
| `stacked` | `stalled` (DEF-12) |
| "below query", backfill | window-underflow query (RP-8) |
| `InvalidBaseBlock` | conflict (DEF-31) |
| stride | acquisition batch unit (unspecified internal; 14 §upstream) |
| commitment (`latest`/`finalized`) | acquisition target level (14 §upstream) |
| enrichment | component acquisition (WP-11) |
| `auto-adjust-finalized-head` | autoAdjust (DEF-24, WP-24) |
