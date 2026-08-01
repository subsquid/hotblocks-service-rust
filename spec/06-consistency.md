# 06 — Consistency & recovery

The buffer is volatile (NG1), so there is no durability tier here; what this document
fixes is the commit model readers rely on and the recovery contract restarts rely on.

**CN-1 — Commit order.** [MUST] The chain buffer moves through a single total order of
committed states `C₀, C₁, …` (one per applied batch/transition step, WP-3). Every read
(RP-1) observes exactly one committed state. There is no read that mixes two states.

Note the two orders at play: *version order* (the commit sequence) always advances;
*chain progress* (head height) may regress across versions when T3 truncates. Clients
observe monotone versions, not monotone heights.

**CN-2 — Atomic visibility.** [MUST] The unit of visibility is the input batch (WP-3):
appends, reorg truncation, finality advance, and eviction land as one version. No
reader observes a truncated-but-not-yet-extended chain or an evicted-but-unadjusted
finality index.

**CN-3 — Snapshot isolation.** [MUST] A stream response's buffered portion is served
from one snapshot (DEF-13) fixed at final resolution — admission, or the single
post-wait re-resolution of RP-3/RP-4. Concurrent commits (including reorgs
that orphan snapshot blocks) do not alter an in-flight response — a client may receive
blocks that were orphaned after admission and learns of it on its *next* request via
the conflict protocol (RP-7). This staleness is bounded by response duration
(`P-RESP-BUDGET`).

**CN-4 — Watermark coherence & freshness.** [MUST] Within any committed state:
`first ≤ finalized ≤ head` (by buffer position) and every watermark read reflects the
newest commit at read time (RP-9). A stream response's finalized-head metadata is the
*snapshot's* finalized head — it may trail the live value by design.

**CN-5 — Real-time monotonicity per reader.** [MUST] Against **one instance**, two
sequential reads by one client observe non-decreasing versions: a watermark read after
a stream response never reports a state older than that response's snapshot. (Height
may still regress — CN-1 note.) Scope is per instance and per epoch (INV-12): the wire
carries no version or epoch token and ADR-1 pins it that way, so a client fanned across
the two instances of FM-34, or reading across a T1 re-INIT, can observe an older
version. That flapping is sanctioned, never corruption — each response is one
instance's coherent snapshot. A client needing cross-instance monotonicity must pin an
instance itself; the service offers no affinity mechanism (ADR-14).

**CN-6 — Maintenance transparency.** [MUST] Eviction (T5) and any internal
housekeeping are invisible in query results except through the sanctioned observables:
`first(C)` advancing and window-underflow resolution (RP-8) taking over for evicted
heights. A metamorphic check — same query before/after an eviction that doesn't touch
its range — returns identical records (INV-10).

**CN-7 — Recovery contract.** [MUST] Post-restart state ≡ the result of T1 on current
upstream state (WP-15): a one-block buffer at the upstream finalized head, every
derived value (watermarks, metrics gauges, session state) computed freshly from it.
There is no recovered field that could disagree with a committed state, because
nothing is recovered. Recovery is idempotent (T1 twice ≡ T1 once, modulo upstream
advance) and requires no format-compatibility gate (no persisted format exists —
if OQ-3 introduces one, this clause gains the format gate).

**CN-8 — Clock independence.** [MUST] No consistency property above depends on wall
clocks. Timestamps in blocks are payload data; time appears only in bounds (waits,
budgets) — never in ordering or identity decisions.

**CN-9 — Subsystem non-interference.**

| Writer ↓ / concurrent → | Readers | Eviction | Watermark reads |
|---|---|---|---|
| Batch apply (T2–T5) | see only pre/post versions (CN-2) | same atomic step (WP-24) | pre/post only |
| Readers | independent snapshots; no reader blocks another | eviction never invalidates a held snapshot (snapshot owns its data) | — |
| Session restart / T6 | invisible (buffer unchanged) | — | invisible |
| T1 re-seed | next admission sees the fresh buffer; in-flight responses complete on their old snapshot | — | switch atomically |
