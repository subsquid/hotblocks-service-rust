# 07 — Safety invariants

Bands: 1–9 structural, 10–19 transition legality, 20–29 read/response, 30–34
reporting, 35–39 isolation, 40–49 recovery. Check strategies name test classes from
[13-conformance-tdd.md](13-conformance-tdd.md).

## Structural

**INV-1 — Non-empty buffer.** [state]
`|B| ≥ 1` in every committed state; the buffer is seeded before serving begins and
never drains below one block.
*Why:* watermarks and the base check are total functions only on a non-empty chain.
*Check:* CT-1 — well-formedness assertion after every reference-model transition.

**INV-2 — Chain shape.** [state]
`∀i ∈ 2..n: linked(b_{i−1}, bᵢ)` and numbers strictly ascend. Exactly one block per
buffered height.
*Why:* everything downstream (coverage, conflicts, rebase) assumes a single
well-formed chain.
*Check:* CT-1 — structural validator over every observable state and every response.

**INV-3 — Finality index validity.** [state]
`1 ≤ f ≤ |B|`.
*Why:* an out-of-range finality index makes eviction and reorg protection undefined.
*Check:* CT-1.

**INV-4 — Window bound (conditional).** [state]
With autoAdjust on: `|B| ≤ P-CACHE-SIZE` in every committed state — T5 restores the
bound within the same atomic step (WP-24), so no committed overshoot exists. With
autoAdjust off: whenever a committed state has `|B| > P-CACHE-SIZE`, the standing
over-window alarm (OB-6) is active for the entire excess period, with the excess
magnitude exposed (OB-1) — unbounded silent growth is non-conforming (REQ-12).
*Why:* memory must be a function of configuration or loudly not be (G4).
*Check:* CT-7 — soak with lagging finality, both settings; scrape OB-6.

**INV-5 — Clock independence.** [transition]
No ordering or identity decision reads a wall clock: transition legality, coverage,
conflict verdicts, and watermark values are functions of input and committed state
alone. Time enters only as bounds (waits, budgets, alarm thresholds). Block
timestamps are payload data (DEF-4), never an ordering key.
*Why:* a clock-dependent verdict is untestable and unreproducible — and on a chain
whose timestamps are adversary-influenced, unsafe.
*Check:* CT-1 — the reference model consumes no clock; the same history replayed at
different speeds must produce identical verdicts.

## Transition legality

**INV-10 — Frame condition & maintenance transparency.** [transition]
Per WP-8, `(B, f)` changes only via T1–T5 driven by input events or the retention policy; no
query, scrape, timer, or background task changes it. Metamorphic: any read repeated
across a quiescent period (no input) returns identical results, and a read whose range
an eviction did not touch returns identical records across that eviction. Eviction
leaves no reader-visible artifact: a request below the window switches to backfill
(RP-8), and nothing distinguishes "evicted" from "never buffered".
*Why:* catches whole classes of accidental mutation and "maintenance" corruption.
*Check:* CT-3 — concurrency swarm with input paused; diff all reads.

**INV-16 — Commit order & atomic visibility.** [transition]
The buffer moves through a single total order of committed states `C₀, C₁, …`, one per
applied batch (WP-3). Every read observes exactly one of them; no read mixes two, and
no reader observes a truncated-but-not-yet-extended chain or an evicted-but-unadjusted
finality index. Two orders are at play: *version order* always advances, while *chain
progress* (head height) may regress when T3 truncates — clients observe monotone
versions, not monotone heights.
*Why:* every read-side guarantee below is stated against "one committed state";
without a total order the phrase names nothing.
*Check:* CT-1 + CT-3 — swarm cannot observe an intermediate or mixed state (INV-15).

**INV-11 — Finalized immutability.** [transition]
No transition *replaces* any of `b₁..b_f`, and none removes `b_f` itself. T3 requires
its truncation point ≥ f; T5 evicts only the oldest finalized prefix strictly below
`b_f` (WP-24); T1 is the sole sanctioned full reset and is alarmed (WP-20).
*Why:* finality is the anchor clients and the backfill path rely on; violating it
invalidates every downstream consumer.
*Check:* CT-1 property test (reorg generator crossing f must be rejected) + CT-4
(adapter emitting sub-finality forks).

**INV-12 — Finality monotone & validated.** [transition]
Within one **epoch** — the buffer's lifetime between T1 INITs — `f` (as a block, not
index: `finalized(C)`) never moves to a lower height, and a finality report naming a
buffered height must match that block's hash to apply (WP-23); a mismatch is an
integrity violation, never a silent apply. Reports above the buffered head apply
provisionally to the whole buffer (WP-12) — the arriving blocks re-validate by
linkage. A T1 re-INIT opens a new epoch and MAY seed a lower watermark, because the
seed is upstream's finalized head and FM-26 permits that value to oscillate; the
reset is alarmed (WP-20, OB-7). Monotonicity across epochs is not offered and MUST NOT
be relied on (ADR-14). A seed that contradicts the buffer being discarded (same height,
different hash) is not a regression but unrecoverable divergence — FM-19/FM-30; beyond
that buffer's extent the comparison has no state to run against and is not claimed.
*Why:* a regressing or forged finality watermark breaks INV-11's meaning; scoping it
to the epoch keeps the guarantee honest instead of promising what a re-seed from an
oscillating upstream cannot deliver.
*Check:* adapter lifecycle regression for the retained maximum across a fork rebase and
a lower contradictory report; CT-4 — remaining contradictory-finality corpus; CT-2 —
re-INIT against an upstream whose finalized head oscillates, asserting the alarm and
the terminal path on contradiction.

**INV-13 — Admission by linkage only.** [transition]
A block enters B iff its parent link matches the block it lands on (the WP-21/22
preconditions); gaps, mismatches, and sub-finality parents are rejected whole with the
batch (WP-5); payload content is never judged at this layer (WP-7).
*Why:* one bad admit poisons every later linkage assumption.
*Check:* CT-1 + CT-4 (gap/mismatch corpus).

**INV-14 — Destructive paths are enumerated.** [transition]
Buffered blocks leave B only via: T3 truncation (above f), T5 eviction (oldest,
finalized prefix), T1 re-seed (alarmed). There is no other deletion path.
*Why:* "data left the buffer" must always have one of three auditable causes.
*Check:* CT-1 — ledger reconciliation: every simulator-produced block is either
buffered, evicted-after-finality, truncated-by-named-reorg, or discarded-by-named-reset.

**INV-15 — Single writer.** [transition]
All transitions are serialized through one writer (WP-1); no interleaving of two
concurrent transition steps is observable.
*Why:* the transition catalog's pre/post reasoning is meaningless without it.
*Check:* CT-3 — swarm cannot observe intermediate states (WP-3).

## Read / response semantics

**INV-20 — Response chain shape.** [response]
Every stream response's records, decoded, form an ascending pairwise-linked sequence
starting at the lowest response-eligible height ≥ the requested one (DEF-30), with no
duplicates and no gaps inside coverage.
*Why:* the client's entire recovery model rests on it.
*Check:* CT-5 structural validator on every response (also run inside CT-3/CT-4).

**INV-21 — Snapshot consistency.** [response]
All buffered records in one response come from one snapshot (DEF-13), fixed at final
resolution — admission, or the single post-wait re-resolution of RP-3/RP-4; no response
interleaves two versions. Commits concurrent with an in-flight response — including
reorgs that orphan snapshot blocks — do not alter it: a client may receive blocks
orphaned after admission and learns of it on its next request through the conflict
protocol (RP-7). This staleness is bounded by response duration (`P-RESP-BUDGET`).
*Why:* a mixed response can present a chain that never existed.
*Check:* CT-3 — reorg storm while streaming; validator checks cross-record linkage.

**INV-22 — Conflict honesty.** [response]
A conflict is returned iff the base check fails (RP-11), and always carries non-empty
`prev` conforming to RP-7. No other outcome reports a conflict; no conflict is
returned for positions the service cannot judge (RP-3 case 2).
*Why:* spurious/empty conflicts crash or livelock well-behaved clients (GAP-6).
*Check:* CT-1 (reference model decides conflict) + CT-5 (shape).

**INV-23 — Progress guarantee.** [response]
A successful non-empty response advances the client by ≥ 1 block; the empty form is
explicitly distinct (RP-12). A conforming client loop (poll → recover-on-conflict)
never enters a no-progress cycle while the service holds data it hasn't seen.
*Why:* the poll loop is the product; a zero-progress success starves it silently.
*Check:* CT-1 — model client driven against the SUT for every generated history.

**INV-24 — Watermark honesty.** [response]
Watermark reads and response metadata equal the corresponding committed state's values
(RP-9); within any committed state `first ≤ finalized ≤ head` by buffer position, so
finalized ≤ head within any single read. Every watermark read reflects the newest
commit at read time — no cache outlives a commit — while a response's finalized-head
metadata is the *snapshot's* and may trail the live value by design.
*Why:* clients schedule polling and rollback bounds off these values.
*Check:* CT-1 + CT-3 (interleaved watermark reads during storms).

**INV-29 — Reader monotonicity, scoped.** [response]
Against **one instance within one epoch** (INV-12), two sequential reads by one client
observe non-decreasing versions: a watermark read after a stream response never reports
a state older than that response's snapshot. Height may still regress (INV-16). The
wire carries no version or epoch token and ADR-1 pins it that way, so a client fanned
across the two instances of FM-34, or reading across a T1 re-INIT, can observe an older
version — sanctioned flapping, never corruption, since each response is one instance's
coherent snapshot. A client needing more must pin an instance itself; no affinity
mechanism is offered (ADR-14).
*Why:* stating monotonicity without the scope promises what a stateless, unpinned wire
protocol cannot deliver — and clients would build rollback logic on it.
*Check:* CT-3 — sequential reads per client during storms; cross-instance flapping
asserted only within one instance's history.

**INV-25 — Payload fidelity.** [response]
Served record bytes (after content-encoding removal) are exactly the stored canonical
record bytes, which are exactly the acquisition output for that block (no
re-serialization drift between encodings or over time).
*Why:* provenance: what the adapter produced is what every client gets, on every
encoding path.
*Check:* CT-5 — dual-encoding fetch + ledger byte-compare (HC-2).

**INV-26 — Payload determinism.** [response]
The canonical record is a deterministic function of (upstream data, data selection):
same inputs → same bytes, across runs, restarts, and concurrency (REQ-7). No
iteration-order, timing, or concurrency effect may reach the payload (GAP-13).
*Why:* differential testing, caching, and dedup all assume it; nondeterminism also
breaks REQ-24.
*Check:* CT-5 — repeated acquisition of recorded corpus, byte-diff (GAP-13 first
test).

**INV-27 — Error soundness.** [response]
Every terminal outcome is one taxonomy class (RP-13); no payload-plus-error hybrid;
after the first record only truncation exists and the underlying error is alarmed
server-side (RP-12). Conflict payloads always satisfy RP-7.
*Why:* clients branch on the closed set; a novel or hybrid outcome is undefined
behavior client-side.
*Check:* CT-4 + CT-9 — all injected faults; classify every observed outcome.

**INV-28 — Component completeness.** [response]
Every served block satisfies component coherence (DEF-15) for the active data
selection. No served block ever has a selected component absent, defaulted, emptied, or
thinned because acquisition failed, and none is filled from another block's answer;
absence of data is served only when emptiness is upstream-true and coherent.
*Why:* silently empty components are the worst failure class in this system's history
— undetectable downstream, unrecoverable once archived.
*Check:* CT-4 — per-component fault matrix asserting invalid-then-retry, never
empty-then-serve; on the trace and state-diff components today, the rest of the
components still uninjected. CT-5 golden corpus.

## Reporting

**INV-30 — Truthful signals.** [state]
Every exposed observable of 12 reflects the state it names (within OB freshness
bounds); a registered signal that structurally cannot change (dead gauge) or reports a
value known false is non-conforming.
*Why:* operators alarm on these; a lying metric converts an outage into a mystery
(GAP-24).
*Check:* CT-5 — scrape-vs-ledger comparison over scripted histories.

**INV-31 — Alarms are observable, not just logged.** [state]
Every condition this suite marks "alarm" (WP-5, WP-9, WP-20, OB-6, OB-7, FM-30) is
visible on the scrape surface as a level or counter — not only in the log stream.
*Why:* logs are for diagnosis; routing and paging read metrics (GAP-4).
*Check:* CT-4 — induce each alarm condition; scrape must show it.

## Isolation

**INV-35 — Reader/ingestion isolation.** [state]
Client behavior (count, speed, disconnects, malformed input) cannot: stall ingestion
beyond the declared shared upstream budget (RP-22), corrupt any other response, or
grow per-request memory beyond RP-21's bounds.
*Why:* the service sits in front of many independent consumers; one bad client must
cost only itself.
*Check:* CT-8 — noisy-neighbor scenarios (S5/S6).

**INV-36 — Configuration honesty.** [state]
Every accepted configuration option observably takes effect; an option the build
cannot honor fails startup (REQ-14, REQ-32). No silent no-op switches (GAP-20).
*Why:* an operator who enables verification and gets none is running with false
assurance.
*Check:* CT-5 — per-option effect probe: each flag flips at least one observable
behavior in a scripted scenario.

## Recovery

**INV-40 — Restart equivalence.** [recovery]
The post-restart state is exactly T1's result on current upstream state (WP-15): a
one-block buffer at the upstream finalized head, with every derived value (watermarks,
gauges, session state) computed freshly from it — no stale derived value, no residue
influencing behavior. Recovery is idempotent (T1 twice ≡ T1 once, modulo upstream
advance) and needs no format-compatibility gate, because nothing is persisted; if OQ-3
introduces persistence, this invariant gains that gate.
*Why:* the recovery story is "there is nothing to recover"; any residue falsifies it.
*Check:* CT-2 — kill-point matrix: kill at every transition boundary, restart,
compare against fresh reference model.

**INV-41 — No permanent internal failure.** [recovery]
No reachable input, timing, or internal error leaves the process running but
permanently unable to ingest or serve (a "bricked" state). Every failure path ends in:
recovery (ladder/T1), or explicit process exit (FM-30/FM-31). Applies to every shared
internal structure: a failure while holding one must not poison it for all future work.
*Why:* a process that answers liveness probes while dead inside defeats every
orchestrator; this is the single worst divergence found in the current implementation.
*Check:* CT-2 + CT-4 — inject integrity violations and panics mid-transition; assert
either full recovery or exit, never zombie.

Each entry's *Check:* line names the test classes that decide it; 13's traceability
matrix is the single record of what those classes cover today.
