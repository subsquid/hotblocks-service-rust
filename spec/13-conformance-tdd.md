# 13 — Conformance & TDD program

**Mutable doc #1.** Statuses dated **2026-08-05**, derived from the actual test
inventory of the current implementation (not aspiration).

## Harness architecture

```
                     ┌────────────────────────────────────────────┐
   scripted chain    │                  SUT (black box)           │
  ┌──────────────┐   │  ┌───────────┐   ┌────────┐   ┌─────────┐  │   ┌───────────────┐
  │  input       │──▶│  │acquisition│──▶│ chain  │──▶│ query   │──┼──▶│ client driver │
  │  simulator   │   │  │ adapter   │   │ buffer │   │ engine  │  │   │ + fuzzers     │
  │  (fake       │   │  └───────────┘   └────────┘   └─────────┘  │   │ + disconnector│
  │  upstream)   │   └────────────────────────────────────────────┘   └──────┬───────┘
  └──────┬───────┘        ▲ fault injectors (delay, error, equivocate,       │
         │                │ malformed, stall, kill)                          ▼
         │        ┌───────┴────────┐      ┌──────────────┐        ┌──────────────────┐
         └───────▶│    ledger      │◀────▶│  comparator  │◀───────│ structural       │
                  │ (provenance:   │      │  + reference │        │ validators       │
                  │ every produced │      │  model       │        └──────────────────┘
                  │ block/event)   │      └──────────────┘
                  └────────────────┘             ▲
                          observability scraper ─┘  (quiescence: no pending input,
                                                     no in-flight request, OB-2 stable
                                                     across two scrapes)
```

The simulator implements the acquisition adapter's *upstream* (14 §upstream binding)
for CT classes that exercise the real adapter, and the adapter contract itself
(DEF-20/21) for core-only classes. The ledger records every block, fork, and finality
event produced, making provenance checks (INV-14, INV-25, INV-30) decidable.

## Reference model (normative oracle)

Pure, single-threaded; the comparator runs it against the same scripted history.

```
state: B: list<Block>, f: int (1-based), base: Ref, stalled: int,
       fin_max: Ref|⊥    # WP-12 running maximum for the session — the only
                         # finality obligation held (ADR-16)

init(fh_block):            require P_CACHE_SIZE ≥ 1       else configuration_error
                           require hash(fh_block) ≠ "" and parentHash(fh_block) ≠ ""
                           require hash(fh_block), parentHash(fh_block) HTTP-field-safe
                           require parentNumber(fh_block) ≤ number(fh_block)
                                                            else source_error  # DEF-2/4
                           B ← [fh_block]; f ← 1; base ← ref(fh_block); stalled ← 0
                           new_session()

new_session():             fin_max ← ⊥      # WP-12 is per session; T6 rebase stays
                                            # inside one

apply_batch(blocks, finrep):
  require blocks ≠ []         else session_error       # DEF-20: batches are non-empty
  require ⟨blocks⟩ ascending ∧ pairwise linked  else session_error   # DEF-20 shape:
                                        # checked before any mutation, so a malformed
                                        # batch cannot half-apply (WP-11.5 owes the
                                        # split at every discontinuity)
  require ∀x ∈ blocks: hash(x) ≠ "" ∧ parentHash(x) ≠ ""   else session_error  # DEF-2:
                                        # linkage cannot catch it — an empty hash links
                                        # to an empty parent hash
  require ∀x ∈ blocks: hash(x), parentHash(x) HTTP-field-safe  else session_error
  require finrep = ⊥ ∨ hash(finrep) ≠ ""       else session_error              # DEF-2
  require finrep = ⊥ ∨ hash(finrep) HTTP-field-safe  else session_error            # IB-4
  checkpoint ← (B, f, fin_max)           # WP-5: a violating batch is rejected whole
  for x in blocks:
    if ∃ b ∈ B: b.number = x.number ∧ b.hash = x.hash ∧ parentRef(b) = parentRef(x):
        continue                        # WP-6: identical duplicate is a no-op —
                                        # checked first, so a redelivered root
                                        # (DEF-4's exception) is absorbed here
    if ∃ b ∈ B: ref(b) = ref(x) ∧ parentRef(b) ≠ parentRef(x):
        integrity_violation             # WP-6 equivocation: one ref, two ancestries
    require x.parentNumber < x.number     else integrity_violation        # DEF-4
    if linked(last(B), x):  B.append(x)                                   # T2
    else:
      i ← index of b in B with b.number = x.parentNumber   # unique by INV-2
      require i exists            else integrity_violation
      require i ≥ f               else integrity_violation                # INV-11
      require linked(B[i], x)     else integrity_violation
      B ← B[1..i] + [x]                                                   # T3
      f ← min(f, len(B))          # unchanged in value; index stays valid
  settle_report()             # the arriving blocks discharge the obligation first
  if finrep ≠ ⊥:
     note_report(finrep); settle_report()                                 # WP-12
  compact()                                                               # T5
  well_formed()               # INV-1..3 after every step
  base ← ref(last(B))

note_report(r):
  if r.number < finalized(B).number: return       # stale across session reset (WP-12)
  if fin_max ≠ ⊥ and r.number = fin_max.number and r.hash ≠ fin_max.hash:
     integrity_violation                # equal-height contradiction
  if fin_max = ⊥ or r.number > fin_max.number:
     fin_max ← r                        # a higher report replaces the obligation;
                                        # the replaced one's check is not owed

settle_report():
  if fin_max ≠ ⊥:
     if fin_max.number ≤ last(B).number: finalize(fin_max)   # validates hash
     else: f ← len(B)                   # WP-23 provisional, revalidated on arrival

finalize(r):
  if r.number < finalized(B).number: return                              # WP-12
  if r.number < B[1].number: return
  if r.number > last(B).number: f ← len(B); return                        # WP-23
  i ← index with B[i].number = r.number
  require B[i].hash = r.hash  else integrity_violation
  f ← max(f, i)

compact():
  excess ← max(0, len(B) − P_CACHE_SIZE)
  if excess > f−1 and autoAdjust: f ← excess + 1        # WP-24 force-advance, alarmed
  k ← min(excess, f−1)
  B ← B[k+1..]; f ← f − k
  if len(B) > P_CACHE_SIZE: alarm(OVER_WINDOW)          # INV-4

integrity_violation:  (B, f, fin_max) ← checkpoint; alarm; session_teardown
                                                        # WP-5 — never process death

fork_signal(prev):
  require prev non-empty      else session_error        # WP-10 / FM-13
  t ← newest b in B[f..] with ref(b) ∈ prev
        or b.number < min(r.number for r in prev)       # stepwise descent (ADR-15)
  if t exists: base ← ref(t); stalled ← 0               # T6
  else: terminal(FM_30)                                 # divergence at/below f

query(from, parentHash):                                # RP-3..RP-13
  snap ← (B, f)                                         # INV-21
  if snap.B[1] is not a root                            # DEF-4's exception: nothing
     and from ≤ snap.B[1].parentNumber:                 #  exists below a root
     return BACKFILL                                    # RP-8 (oracle: simulator
                                                        #  ledger supplies the range)
  if from > last(snap.B).number:
     if from = last(snap.B).number + 1 and parentHash ≠ ⊥
        and parentHash ≠ last(snap.B).hash:
        return CONFLICT(refs_of_last_upto(P_FORK_REFS_MAX))     # RP-11
     return EMPTY(watermarks(snap))                             # RP-12 (post-wait)
  pos ← min index in snap.B with snap.B[pos].number ≥ from  # DEF-30: from need
  x ← snap.B[pos]                                             # not exist
  if parentHash ≠ ⊥ and x.parentHash ≠ parentHash:
     return CONFLICT(parent_refs_window(x, P_FORK_REFS_MAX))    # RP-7
  return DATA(snap.B[pos..], finalized(snap))                   # any prefix ≥ 1 block
```

Input shape is part of the contract, not a convenience: `apply_batch` takes a
non-empty batch (DEF-20), with finality knowledge attached opportunistically per ADR-6.
The model deliberately exposes no standalone finality-only transition; a test that
feeds an empty batch is testing a shape no conforming adapter emits. The single
above-head obligation of WP-23 is `fin_max` itself (ADR-16), so an upstream reporting
finality faster than it delivers blocks buys no state in the model and none in the
service (PF-1).

**Free variables** (the SUT may vary; everything else must match the model):

1. Where DATA coverage ends (any record-boundary prefix with ≥ 1 block — RP-5).
2. The count/selection of refs inside CONFLICT, within RP-7's contract.
3. Batch grouping of ingested blocks (commit granularity), hence which intermediate
   versions are observable.
4. Timing within bounds (`P-WAIT-BLOCK`, budgets); whether a racing commit turns an
   EMPTY into DATA.
5. Transport chunking and compression bytes (only decoded record bytes are checked).

## Test-class taxonomy

| CT | Class | Primary properties | Needs |
|---|---|---|---|
| CT-1 | State-machine property tests: generated histories (appends, reorgs, finality, duplicates) driven through the SUT and model, all reads compared | INV-1..4, 10..15, 20..24, WP-*, RP-3..12, LIV-4, LIV-8 | HC-1, HC-5, HC-6 |
| CT-2 | Crash/restart & kill-point matrix: kill at transition boundaries and mid-response; restart; compare to fresh model; panic-injection for zombie detection | INV-40, INV-41, LIV-5, LIV-9, REQ-13, REQ-23; FM-32, FM-33 | HC-1, HC-7, kill harness |
| CT-3 | Concurrency swarms: parallel clients + reorg storms + watermark readers against a moving head | INV-10, 15, 16, 21, 24, 29, 35; LIV-3 | HC-1, HC-7, HC-9 |
| CT-4 | Input-fault corpus: FM-10..27 matrix (per-component faults × fault kinds), quirk corpora per network, finality contradiction corpus | INV-11..14, 27, 28, 31; WP-5, 10..12; LIV-2; REQ-9, 15, 16 | HC-3, HC-2 |
| CT-5 | Interface conformance: 14's binding table, structural validators on every response, dual-encoding fidelity, golden payload corpus, differential vs predecessor, config-honesty matrix, metrics-vs-ledger | IB-*, INV-25, 26, 30, 36; REQ-7, 24, 30, 32; FM-50..53 | HC-4, HC-6, HC-8 |
| CT-6 | Performance benchmarks: S1/S2 SLI measurement vs baselines | SLI-1..8, PF-10..12, HZ-8, LIV-1, LIV-6, LIV-7 | HC-10, HC-12 |
| CT-7 | Soak/endurance: multi-hour S1/S2 with lagging finality; memory, log volume, alarm levels | INV-4, LIV-2, LIV-11, REQ-31 | HC-1, HC-10 |
| CT-8 | Isolation/noisy-neighbor: S4/S5/S6 | INV-35, RP-21..23, LIV-10, LIV-12; FM-41..44 | HC-9 |
| CT-9 | Fuzz, both surfaces: HTTP requests (structure-aware) and upstream responses (schema-aware) | FM-1, FM-25, FM-40, INV-27, INV-41, REQ-22 | HC-3, HC-7, HC-11 seeds |

## Structural validators (kind-agnostic, every response)

decodable per negotiated encoding → **one frame per record**, with raw frame/member
boundaries enumerated once and each unit decoded independently (REQ-6/IB-2: this makes
every prefix ending at an enumerated boundary independently decodable in linear work;
whole-stream decoding cannot tell N frames from one frame holding N records) → records
self-delimiting → each block satisfies DEF-2/4 → ascending → pairwise
linked → coverage starts at the lowest response-eligible height ≥ the requested one
and never below it (DEF-30) → records follow the snapshot's branch → no
duplicates → watermark metadata parses and finalized ≤ head → conflict bodies
non-empty/ascending → endpoint error bodies match the RP-13 taxonomy shape → outcomes
outside that taxonomy (unknown-route 404, 405 wrong method, 503 readiness — IB-1 and
14's operation table) match their route-aware binding → empty form carries watermarks.

"Response-eligible" is decided by the **eligible chain** — for a window-underflow
query the backfill prefix RP-8 owes, spliced onto the snapshot; otherwise the snapshot
alone. Never by the cumulative provenance ledger: the ledger also holds orphaned and
evicted blocks, which no response may start at, and it records what the simulator
delivered rather than what the SUT owed. The ledger's job is byte provenance (INV-25).
Every record inside the eligible range is matched on hash *and* parentRef — the first
record has no predecessor for pairwise linkage to check, so a forged parent there is
otherwise invisible.

## Traceability matrix (2026-08-05)

Status: **C** covered (black-box, automated) / **P** partial / **U** unchecked.
`!` = known-violated today (see gap register). Current inventory: unit tests on the
buffer state machine; integration tests for a handful of endpoint shapes, one
fork-recovery scenario, backfill-error 500, shutdown; acquisition retry + finality
no-stall timing tests; payload golden fixtures (one network); upstream-client retry
classification and socket-transport suites. The Phase-0 harness (`crates/harness`)
adds an executable reference model, a seeded adapter-level simulator with a
linkage-tuple provenance ledger, a validator library, the CT-1 smoke run, and a set of
CT-1/CT-4 SUT-vs-model differentials over hand-written pathological histories. Phase 1
adds a fault-injecting JSON-RPC upstream, the CT-4 component-fault corpus that drives
the real acquisition adapter against it, and CT-7's deterministic catch-up entry point.
The fault matrix covers the trace and state-diff components only; no generated
differential corpus, no soak, no benchmarks yet.

| Property | CT | Status | Note |
|---|---|---|---|
| INV-1..3 | CT-1 | P | unit tests + model asserts on every applied event (CT-1 smoke); no generated histories yet |
| INV-4 | CT-7 | P ! | CT-7's deterministic entry point drives the real adapter through a catch-up that starts below the upstream finalized head and asserts the committed buffer ends inside the window with auto-adjust off. The over-window branch is unasserted while its alarm stays log-only (GAP-4); the auto-adjust setting and the soak are absent |
| INV-5 | CT-1 | P | the reference model consumes no clock; no replay-speed differential yet |
| INV-10 | CT-3 | U | |
| INV-11 | CT-1/4 | P | unit tests incl. rejection cases; a sub-finality write is reported, not fatal, and a redelivery is absorbed before the guard; no adapter-driven corpus |
| INV-12 | CT-2/4 | P | production and model retain the per-session maximum across fork rebase; a lifecycle regression covers a lower contradictory report and later settlement without a fresh report; the epoch-boundary contradiction check (ADR-14) has lifecycle coverage — full CT-2 exercise and the remaining CT-4 corpus are absent |
| INV-13 | CT-1/4 | P | unit-level only |
| INV-14 | CT-1 | U | no ledger reconciliation |
| INV-15 | CT-3 | U | |
| INV-16 | CT-1/3 | P | model asserts one committed state per applied batch (CT-1 smoke); no swarm |
| INV-20 | CT-5/3/4 | P | structural validator on live responses + dual-encoding equality (CT-1 smoke) |
| INV-21 | CT-3 | U | |
| INV-22 | CT-1/5 | P ! | conflict shape, `P-FORK-REFS-MAX` bound, and ref membership validated live (CT-1 smoke); GAP-6: empty-`prev` conflict emitted on empty backfill |
| INV-23 | CT-1 | P | progress guarantee + empty-form distinction checked (CT-1 smoke) |
| INV-24 | CT-1/3 | P ! | watermarks compared against the reference model (CT-1 smoke); GAP-30: post-wait empty form can lack them |
| INV-25 | CT-5 | P | golden fixtures (one network) + ledger byte-fidelity and dual-encoding compare (CT-1 smoke) |
| INV-26 | CT-5 | U ! | GAP-13: iteration-order nondeterminism on one trace path |
| INV-27 | CT-4/9 | U ! | GAP-6, GAP-22 |
| INV-28 | CT-4 | P | CT-4 component-fault corpus over the real adapter: error, null, wrong-block, partial-coverage and unparsable payloads on both trace-API methods, the debug frames path and the debug state-diff path, each asserting bounded re-acquisition then fail-loud, plus the happy-path cassette. The receipts and logs components have no injected faults yet, and the per-network quirk corpora are absent (GAP-9, GAP-14, GAP-16) |
| INV-29 | CT-3 | U | scoping is stated; no per-client sequential-read assertion exists |
| INV-30 | CT-5 | U ! | GAP-24: dead gauge exposed |
| INV-31 | CT-4 | U ! | GAP-4: alarm conditions are log-only |
| INV-35 | CT-8 | U | |
| INV-36 | CT-5 | U ! | GAP-8, GAP-20: inert options accepted |
| INV-40 | CT-2 | U | |
| INV-41 | CT-2/4 | P | no input content ends the process: buffer and finality violations are reported and their batch rolled back, with SUT-vs-model differentials over a contradicted parent link, a contradicted finality report, and a redelivery; the supervisor still drains and exits non-zero on a vanished writer (FM-32). Kill-point matrix absent |
| LIV-1 | CT-6 | P | finality-decoupling timing test only |
| LIV-2 | CT-4/7 | U ! | GAP-4: alarms are log-only; the silent terminal stop itself is closed (non-zero exit, 2026-08-02) |
| LIV-3 | CT-3/8 | U ! | GAP-31: unbounded internal waits bypass the budget |
| LIV-4 | CT-1 | U | |
| LIV-5 | CT-2 | U | |
| LIV-6 | CT-6 | U | |
| LIV-7 | CT-6/7 | P | CT-7 asserts the bound at two altitudes — the reports the adapter carries mid-catch-up, and the watermark the service commits — against a stub whose finalized head sits far above the buffer. No benchmark measures the lag under load, and `P-SLO-FINALITY-LAG` is still ⚠ |
| LIV-8 | CT-1/4 | P | single two-block reorg e2e test |
| LIV-9 | CT-2 | P | one prompt-shutdown test |
| LIV-10 | CT-8 | U | |
| LIV-11 | CT-7 | P ! | CT-7 asserts convergence — a buffer that outgrew its window while finality lagged is back inside it once the reports land. That the alarm clears with the excess is unasserted while it is log-only (GAP-4) |
| LIV-12 | CT-8 | U | |
| REQ-1..6 | CT-1/5 | P | endpoint smoke + one fork recovery; REQ-6 framing (one frame per record, every boundary a safe cut) validated on both encodings (CT-1 smoke) |
| REQ-7 | CT-5 | P | one-network golden corpus |
| REQ-8 | CT-5 | P | selection combinations exercised only via recorded-corpus configs |
| REQ-9 | CT-4 | P | the retry half of retry-then-alarm is asserted on the trace and state-diff components, bound included (`P-ENRICH-RETRIES` re-acquisitions, then a session error naming the block); the alarm half stays log-only (GAP-4) and the remaining components are uninjected |
| REQ-10..13 | CT-1/2/6 | P | |
| REQ-14 | CT-5 | U ! | GAP-8 |
| REQ-15 | CT-4 | U ! | GAP-16 (scope: OQ-6) |
| REQ-16 | CT-4 | P ! | retry classification unit-tested; GAP-10/20/21 |
| REQ-17 | CT-1 | U | synthetic non-EVM adapter run absent |
| REQ-20..23 | CT-6/2 | P | shutdown only |
| REQ-24 | CT-5 | U ! | GAP-27: no differential runner; GAP-13/18/19 known breaches |
| REQ-30..32 | CT-5 | U ! | GAP-24, GAP-25 |

## Gap register (2026-08-03)

Priorities: P0 active production risk · P1 correctness hole with plausible trigger ·
P2 bounded/rare · P3 polish. "First test" = cheapest failing-test-first entry point.
Retired rows stay in place — IDs are stable and ADRs cite them: GAP-1, GAP-2, GAP-3, GAP-7, GAP-17, GAP-29, GAP-33, GAP-35, GAP-36.

| GAP | Statement | Violates | Prio | First test |
|---|---|---|---|---|
| GAP-1 | **Retired 2026-08-04.** Buffer and finality violations are reported instead of asserted: the batch rolls back whole and the session re-enters the ladder, so no input content ends the process | — | retired | — |
| GAP-2 | **Retired 2026-08-05.** Stride acquisition is bounded by the finalized head on both streams, so every strided range is final and carries its own report; the prober skips what a report already settled and spaces only rounds that settle nothing. The over-window alarm remains log-only (GAP-4) | — | retired | — |
| GAP-3 | **Retired 2026-08-05.** On the execution-trace and state-diff paths an upstream error, a null result, a payload that is not the method's result type, an entry that fails to parse, a result labelled with another block's transaction, an entry carrying a frame of a different transaction, and a transaction left uncovered each mark the block incoherent, so WP-11.2 re-acquires it and WP-11.3 fails the session loud — the component is never emptied, thinned, or filled from another block | — | retired | — |
| GAP-4 | Alarms are log-only: no OB-7 (or OB-6) condition is visible on the scrape surface as a level or counter. The terminal-divergence exit path landed 2026-08-02 (rebase below finality and divergent re-seed end the run, drain within `P-SHUTDOWN-GRACE`, and exit non-zero per ADR-12), but an orchestrator still cannot distinguish alarm states before the exit | INV-31, OB-6, OB-7, LIV-2 | P2 | CT-4: induce each alarm condition (stall, over-window, integrity violation, terminal); scrape must show a level/counter change |
| GAP-5 | A fork signal with an empty ref list rebases to the current head and immediately reopens the stream: a malformed adapter can hot-spin the loop forever with no backoff or alarm | WP-10, LIV-8, FM-13 | P1 | CT-4: adapter emits empty-`prev` fork signal; assert session-error handling (ladder), not spin |
| GAP-6 | A window-underflow query whose upstream acquisition yields nothing returns a conflict with an **empty** ref list — a response that crashes the reference client and violates the conflict contract | INV-22, INV-27, RP-7 | P1 | CT-5: backfill against an empty simulator range; assert INTERNAL, never CONFLICT with empty refs |
| GAP-7 | **Retired 2026-08-04.** The per-session finality maximum is retained across fork rebase; lower reports are ignored and an above-head obligation settles when its block arrives | — | retired | — |
| GAP-8 | Three verification switches (sender recovery, transaction commitment, withdrawal commitment) are accepted and silently do nothing | INV-36, REQ-14, REQ-32 | P1 | CT-5: forged-field corpus per switch; assert rejection when on — fails today |
| GAP-9 | Two implemented verification checks lack the per-network exemptions (system/state-sync transactions) the baseline defines, producing false failures on those networks when enabled | REQ-14, REQ-15 | P2 | CT-4: quirk-network fixture with verification on; assert acceptance |
| GAP-10 | Upstream error-classification hooks are dropped for single-call batches — precisely the head path — so not-ready/absent-block conditions lose their mapping and surface as raw errors | REQ-16, FM-23, FM-25 | P1 | CT-4: single-block acquisition against a "not ready" upstream response; assert classified retry, not session error |
| GAP-11 | Range (backfill) acquisition has no bounded per-block retry: a persistently incoherent block causes a spurious-fork/restart hot loop instead of retry-then-alarm. The re-request is logged since 2026-08-05, and GAP-3's closure widened the trigger — component faults that used to be served as empty components now land here | WP-11.2, LIV-2, FM-15 | P1 | CT-4: one block persistently incoherent in a backfill range; assert bounded retries then alarmed session error (`range_acquisition_never_serves_an_emptied_component` asserts only the non-serving half today) |
| GAP-12 | Both trace-API acquisition methods (`trace_block` and `trace_replayBlockTransactions`) address blocks by hash, which some providers silently answer with incomplete data; the predecessor deliberately switched to by-number — regressed. Since GAP-3's closure the incomplete answer is *detected* (CT-4 covers reward-only frames), so what remains is cost and liveness: such a provider burns the retry budget and ends the session every block instead of being asked in the form it answers | REQ-9, LIV-2 | P1 | CT-4: reward-only answer to a hash-addressed trace call; assert the by-number form is used (detection is covered) |
| GAP-13 | Trace ordering on one acquisition path flows through an unordered map: payload bytes vary between runs of the same input — nondeterministic canonical records | INV-26, REQ-7, REQ-24 | P1 | CT-5: acquire the same recorded block twice in separate processes; byte-diff |
| GAP-14 | The upstream schema is stricter than the baseline tolerances: fields the predecessor treats as optional (log-removal marker, receipt status on pre-status-era blocks) are required, so affected networks fail to parse entire blocks | FM-25, REQ-15 | P2 | CT-4: corpus block lacking the optional fields; assert acceptance |
| GAP-15 | Structural validation of debug-trace frames (and the opt-in call-tree check) is absent; unmappable frames from buggy providers historically caused a week-long ingestion stall in the predecessor. Frames that fail to *deserialize* are bounded-rejected since 2026-08-05 (CT-4); the gap is now frames that parse but describe an impossible call tree | REQ-9, LIV-2, FM-25 | P2 | CT-4: corpus with a well-formed but structurally impossible frame; assert bounded rejection, not stall |
| GAP-16 | Per-network quirk handling for several networks of the predecessor's supported set is absent (phantom transactions, duplicated receipts, provider-specific trace addressing) — blocked on OQ-6 scope decision. One exemption is honored since 2026-08-05: polygon-based chains may leave transactions uncovered by debug traces (IB-16's quirk tolerance), so IB-15's coverage rule is enforced everywhere else but not there | REQ-15, FM-14 | P2 | CT-4: per-network quirk corpus (port the predecessor's fixtures), including the polygon coverage exemption |
| GAP-17 | **Retired 2026-08-05.** The trace API picks one method per selection, as the baseline does: the replay call carries traces only where it already runs for state diffs, otherwise `trace_block` answers alone. Forced by GAP-3's closure — once a component failure is fatal, a discarded second answer can end the session | — | retired | — |
| GAP-18 | The JSON metrics mode returns a string containing text exposition instead of the predecessor's structured array | REQ-24, IB | P3 (OQ-4) | CT-5 differential on the metrics route |
| GAP-19 | Oversized request bodies return the generic invalid-request status instead of the payload-too-large status the predecessor uses | REQ-24, IB | P3 (OQ-4) | CT-5: oversized body; assert status per 14 |
| GAP-20 | The upstream batch-size cap option is accepted but bypassed by the dominant call path | INV-36, REQ-16 | P2 | CT-5: metered upstream; assert max observed batch ≤ configured cap |
| GAP-21 | The upstream rate limiter admits concurrent callers past the budget (check-then-act race) | REQ-16, HZ-7 | P2 | CT-8: concurrent acquisition against a metered upstream; assert rate ≤ limit + tolerance |
| GAP-22 | A backfill continuity-check failure surfaces as a panic inside the streaming task: silent truncation with no server-side alarm | INV-27, INV-31, FM-32 | P2 | CT-4: adapter violates backfill continuity mid-response; assert truncation **and** OB-7 event |
| GAP-23 | Steady-state status logs are unthrottled (per-block head lines, per-batch over-window errors) — log flood regression against the predecessor's throttling | REQ-31, OB-8 | P3 | CT-7: S1 soak; assert log/block ratio |
| GAP-24 | A worker gauge that can never move is exposed, and runtime/process default metrics are absent | INV-30, REQ-30 | P3 | CT-5: metrics-vs-ledger sweep |
| GAP-25 | No upstream-interaction observability (request/error/retry counters, upstream head view) exists | OB-4, REQ-30 | P2 | CT-5: scrape after scripted upstream faults; assert counters moved |
| GAP-26 | Upstream endpoint credentials (URL userinfo/keys) are not redacted from error text and logs | IB-13 (operational security) | P2 | CT-4: inject failing upstream with a credentialed URL; scan logs |
| GAP-28 | Stream admission is unbounded: aggregate snapshot and response-buffer memory scales with client count, so the memory ceiling is not derivable from configuration | RP-23, PF-1, INV-35 | P2 | CT-8: connection flood against a small window; assert refusals beyond `P-MAX-CONCURRENT-STREAMS` and a bounded footprint |
| GAP-29 | **Retired 2026-08-04.** `push` absorbs a redelivery matching the buffered block on the full linkage tuple, decided before the DEF-4 height check | — | retired | — |
| GAP-30 | The post-wait re-resolution is not dispatched through full range resolution: a height evicted during the wait yields the empty form — without the mandatory watermark metadata — instead of a window-underflow response. The same watermark-less form also fires pre-wait: the below-window check and the query run under two separate lock acquisitions, so eviction racing admission hits it too | RP-3, RP-8, INV-24, IB-5 | P2 | CT-3: request just above head racing eviction across the wait (and across admission); assert backfill or a watermarked empty form |
| GAP-31 | The response budget and disconnect reap are bypassed by unbounded internal waits: the first backfill batch is awaited without a deadline, and a stalled consumer blocks the producer indefinitely between budget checks | RP-20, RP-21, LIV-3, LIV-10 | P2 | CT-8: stalled upstream during a backfill request, and a stalled reader mid-stream; assert termination within `P-RESP-BUDGET` + `P-DISCONNECT-REAP` |
| GAP-27 | No differential runner against the predecessor exists, though byte-compatibility (REQ-24) is the migration's acceptance criterion; the worst historical payload bug was found by a manual diff | REQ-24, HC-8 | P1 | build HC-8; run recorded-corpus diff in CI nightly |
| GAP-34 | Readiness probes upstream on every request (`is_ready` calls `get_head`), so probe frequency is upstream load an orchestrator controls, drawn from the budget ingestion shares. RP-10/RP-22 describe the local-view comparison ADR-17 proposes; until that lands, the coupling RP-22 no longer mentions is still real | RP-10, RP-22, REQ-16 | P2 | CT-8: probe `/readiness` at 1 Hz under S1; assert upstream call count is independent of probe rate |
| GAP-35 | **Retired 2026-08-04.** Batches are judged on DEF-20 shape — non-empty, ascending, pairwise linked — before anything mutates, so a malformed one is rejected whole instead of half-applying or silently dropping its out-of-order blocks | — | retired | — |
| GAP-36 | **Retired 2026-08-05.** Seed hashes, ordinary block and parent hashes, and finality-report hashes are checked for HTTP field-value safety before any chain mutation, so no committed finality hash can panic `/stream` header construction | — | retired | — |
| GAP-33 | **Retired 2026-08-04.** A ref that already names a buffered block, delivered under a second ancestry, is reported as an integrity violation instead of applied as a reorg | — | retired | — |
| GAP-32 | Enabled verification-check failures (DEF-25: receipts root, logs bloom) and the log-index/cumulative-gas coherence checks bypass WP-11.2's bounded per-block retry on the head path: they surface as an immediate session error the ladder retries forever, instead of bounded per-block retry then fail-loud | WP-11.2, WP-11.3, LIV-2 | P2 | CT-4: enable a verification switch against an upstream serving one persistently forged block; assert `P-ENRICH-RETRIES` bounded retries then an alarmed session error, not an unbounded restart loop |

## Build order

1. **Phase 0 — harness skeleton** *(done 2026-07-31, `crates/harness`; hardened
   2026-08-02: model absorbed the DEF-4 root convention and ADR-15's stepwise
   descent, one-session replay keyed by each delivered
   `(number, hash, parentNumber, parentHash)` tuple with read-path deliveries
   excluded, buffered-root resolution aligned between model and SUT,
   watermark bounds/strictness and IB-1/IB-2/IB-5/IB-6 transport rules added to
   HC-6, the free-variable-2 pin removed from CT-1; the suite's consolidation
   2026-08-02 dropped the pending-report list (ADR-16), made equivocation an
   integrity violation (WP-6), and added DEF-20 batch-shape and REQ-6 framing
   validation)*: HC-1
   simulator + HC-2 ledger + HC-5 reference model + HC-6 validators; wire CT-1 smoke
   (happy-path history) and the spec checker (MG-7). *Exit met:* CT-1 green on the
   happy path; INV-23 flipped U→P; INV-1..3, 20, 22, 24, 25 strengthened.
2. **Phase 1 — P0 gaps** *(complete 2026-08-05)*: failing tests for GAP-2 and GAP-3,
   then fixes. *GAP-3*: HC-3's upstream stub (`harness::upstream`) plus the CT-4 corpus
   in `crates/harness/tests/ct4_components.rs` — 24 cases, 22 of them failing before the
   fix, two healthy controls that must keep passing; INV-28 and REQ-9 lost their `!`,
   GAP-17 retired with it (one trace method per selection), GAP-11/12/15 narrowed.
   *GAP-2*: `crates/harness/tests/ct7_catchup_finality.rs` over the same stub — two
   cases, both failing before the fix, one on the reports the adapter carries and one
   on the buffer the service commits; LIV-7 and LIV-11 flipped U→P, INV-4 U→P.
3. **Phase 2 — correctness core**: full CT-1 generation (reorg/finality/duplicate
   histories), CT-2 kill-point matrix, CT-4 integrity corpus (GAP-5/6/11), CT-5
   golden + config-honesty (GAP-8), HC-8 differential (GAP-27, GAP-13). *Exit:* all
   INV rows ≥ P, INV-11/12/13/22/27/28 = C.
4. **Phase 3 — robustness**: CT-4 upstream-fault matrix (GAP-10/12/14/15), CT-9 fuzz
   both surfaces, CT-8 isolation, quirk corpora per OQ-6 (GAP-16), observability
   conformance (GAP-4/22/24/25). *Exit:* FM cross-reference fully exercised.
5. **Phase 4 — performance regime**: HC-10/HC-12, CT-6 baselines, CT-7 soak
   (GAP-2 regression guard, GAP-23), MG-5 armed. *Exit:* SLO table baselined; ⚠
   targets ratified via ADR-13.

Every phase ends by updating this document's matrix and register in the same
change. The post-migration redesign (OQ-2), once specified, re-plans phases 3-5.

## Merge gates

**Armed** — these block a merge today.

| MG | Gate | Threshold | When | Enforced by |
|---|---|---|---|---|
| MG-1 | Property-coverage ratchet: no INV/LIV/REQ row's status regresses (U → P → C is the only direction) and the count of rows at C never decreases; a PR adding a property adds its matrix row + CT class. Deliberately a count, not a fraction: a fraction floor would penalise adding the property this same gate mandates | `P-COV-PROP` (ratchet) | per-PR | HC-13 + review checklist |
| MG-3 | Failing test first: every GAP closure and bug fix lands with the test that fails without it, named in the register | — | per-PR | review checklist |
| MG-6 | Static gates: formatter check, linter at the pinned deny-set, dependency audit (audit advisory until added) | — | per-PR | existing CI |
| MG-7 | Spec integrity: `scripts/check-spec.py` zero error-severity findings | — | per-PR | HC-13 |

**Planned** — each arms in the phase that builds its capability, and blocks nothing
until then. The Phase-0 CT-1 smoke run blocks on its own, but it does not satisfy MG-4.

| MG | Gate | Threshold | When | Enforced by |
|---|---|---|---|---|
| MG-2 | Line coverage: changed-lines ≥ `P-COV-DIFF`, repo floor ≥ `P-COV-TOTAL`, both ratchet-only (arms with HC-11) | `P-COV-DIFF`, `P-COV-TOTAL` | per-PR | HC-11 |
| MG-4 | Fast conformance: CT-1 (bounded generation), CT-4 (corpus subset), CT-5 (binding + validators) green (arms in Phase 2) | `P-CI-PR-BUDGET` wall-clock | per-PR | CI + HC-1..6 |
| MG-5 | Performance regression: SLI-1..8 within `P-PERF-NOISE` of committed baselines (arms in Phase 4) | `P-PERF-NOISE` | nightly | HC-12 |
| MG-8 | Slow classes: CT-2 kill-point matrix, CT-3 swarms, CT-6 full benchmarks, CT-7 soak, CT-8 isolation, CT-9 fuzz (arms as each capability lands) | — | nightly / pre-release | HC-1, HC-3, HC-7, HC-9..12 + kill harness |

**Target flake policy (not yet automated)**: one automatic retry per test; a second
flake within `P-FLAKE-WINDOW` quarantines the test with a named owner and an expiry
date — quarantined tests are listed in this file and count as U in the matrix. Silent
skips and unconditional-pass shapes (assertions inside optional branches) are
forbidden. CI currently implements neither per-test retry/quarantine automation nor
the executed-test-count ratchet; the latter remains part of HC-11.

## Harness capability register

| HC | Capability | Needed by | Status | Note |
|---|---|---|---|---|
| HC-1 | Scripted input simulator (adapter-level and upstream-level), deterministic, seedable | CT-1..4, 7 | P | adapter-level built in `crates/harness` (seeded, ledger-backed); upstream-level built as `harness::upstream` — a linear scripted chain served over the real JSON-RPC surface, every hash a pure function of the script. Neither level generates histories yet, and the upstream one has no reorg or fork script |
| HC-2 | Provenance ledger + comparator | CT-1, 4, 5; INV-14/25/30 | P | events retain each delivered `(number, hash, parentNumber, parentHash)` tuple, including same-ref parent equivocation; one-session model replay + byte-fidelity comparator in `crates/harness`; replay skips read-path backfill deliveries; fork/re-INIT event tagging and metrics-vs-ledger remain pending |
| HC-3 | Fault-injecting upstream stub (per-method, per-component: error, null, wrong-block, malformed, delay, equivocate) | CT-4, CT-9 | P | `harness::upstream` injects error / null / wrong-block / truncated / non-result payload / unparsable entry per `(method, tracer, block)`, heals after a chosen number of calls, and counts calls so a test can assert the retry *bound*; a scripted source (`harness::script`) replays hand-written pathological histories through SUT and model. Delay and equivocate kinds are absent, and the corpus built on it covers the trace and state-diff components only |
| HC-4 | Recorded-corpus replay (real upstream captures) | CT-5 | C | cassette + golden fixtures wired in CI (one network) |
| HC-5 | Executable reference model (this doc's pseudocode) | CT-1..3 | P | core transitions + query verdicts in `crates/harness`; backfill/wait paths not yet modeled |
| HC-6 | Structural validators as a library | every CT | P | reusable core in `crates/harness`: linear independent zstd-frame/gzip-member splitting (REQ-6), DEF-2/4/5 block shape and linkage, snapshot-judged coverage start + branch, conflict shape and `P-FORK-REFS-MAX` bound, mandatory endpoint body content types, route-aware unknown-route 404, IB-2 DATA negotiation (`Content-Encoding` + `Vary`), RP-13 taxonomy, and watermark rules; the full route-by-route IB sweep remains CT-5 work |
| HC-7 | Client driver: poll loop, RP-7 recovery, fuzzer, disconnector | CT-1..3, 8, 9 | U | |
| HC-8 | Differential runner vs predecessor implementation | CT-5, REQ-24, GAP-27 | U | acceptance criterion of the migration; absent |
| HC-9 | Load/swarm driver (S3..S6) | CT-3, 8 | U | |
| HC-10 | Observability scraper + quiescence gate | CT-6, 7; INV-30/31 | U | |
| HC-11 | Coverage instrumentation in CI | MG-2 | U | |
| HC-12 | Benchmark runner, committed baselines, noise band | CT-6, MG-5 | U | |
| HC-13 | Matrix/register linter (part of the spec checker) | MG-1, MG-7 | C | `scripts/check-spec.py` |

A CT class whose capabilities are all U is *unbuildable today*, not merely unchecked —
that is why HC build order leads Phase 0.
