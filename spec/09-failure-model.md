# 09 — Failure model

Response verbs: **mask** (absorb; no client-visible effect), **degrade** (keep
serving with reduced service, marked), **fail-safe** (stop the affected activity in a
recoverable state), **alarm** (raise the condition on the observability surface,
INV-31). Verbs combine.

## Global requirements

**FM-1 — No externally-triggered termination or corruption.** [MUST]
No upstream response, client input, or their timing terminates the process or bricks
it (REQ-22, INV-41). Process exit is legal only via FM-30/FM-31 and the stop signal.

**FM-2 — Transient vs integrity classification.** [MUST] Every ingestion-path error is
classified: *transient* (retry within budgets: WP-11, REQ-16) or *integrity*
(contradiction with committed state or coherence rules: alarm + fail-safe per WP-5,
never silent retry-forever). Unclassifiable errors are treated as transient with the
session ladder as the backstop — but always counted (OB-7).

**FM-3 — Blast-radius containment.** [MUST] A fault in one activity (one request, one
acquisition, one probe) is contained to that activity plus declared shared budgets
(RP-22); shared internal structures survive the fault usable (INV-41).

## Input-side faults (acquisition adapter → service)

| FM | Fault | Required response |
|---|---|---|
| FM-10 | Batch with parent-hash mismatch / gap / sub-finality parent | fail-safe (reject batch whole) + alarm + session ladder (WP-5) |
| FM-11 | Duplicate or already-buffered blocks (redelivery) | mask (WP-6) |
| FM-12 | Fork signal, well-formed | mask (normal path: WP-10 rebase) |
| FM-13 | Fork signal with empty `prev` | fail-safe + alarm as session error (WP-10); never rebase-to-self (GAP-5) |
| FM-14 | Network-quirk block (phantom txs, duplicated receipts, system txs) on a supported network | mask via the network's quirk normalization (REQ-15); on an unsupported network: fail-safe + alarm (retry exhaustion, WP-11) |
| FM-15 | Component incoherence (counts, indices, cross-refs disagree) | retry per WP-11; on exhaustion fail-safe + alarm; never serve (INV-28) |
| FM-16 | Block not yet available / partially propagated | mask (bounded retry, WP-11.2) |
| FM-17 | Finality report regressive | mask (ignore, WP-12) |
| FM-18 | Finality report contradicting a buffered hash | fail-safe + alarm (integrity, WP-12) |
| FM-19 | Divergence below finality (rebase target under f) | FM-30 |

## Upstream faults (node ↔ adapter)

| FM | Fault | Required response |
|---|---|---|
| FM-20 | Down / unreachable / timeout | retry per REQ-16 schedule; alarm at `P-STALL-ALARM` (LIV-2); readiness reflects staleness (RP-10) |
| FM-21 | Slow (latency ≫ cadence) | degrade (lag grows, visible via OB-3); no unbounded queueing |
| FM-22 | Rate-limiting / throttling responses | mask (classified retryable; budget respected) |
| FM-23 | Erroring (5xx-class / internal errors) | retry only per configured classification (REQ-16, 14 §upstream); else surface to ladder |
| FM-24 | Equivocating (load-balanced fleet disagrees between calls) | mask via whole-block re-acquisition (WP-11.2); persistent equivocation → retry exhaustion → ladder |
| FM-25 | Oversized / malformed / schema-violating response | fail the call (transient class); tolerate documented optional-field absence (IB-16, GAP-14); never panic (FM-1) |
| FM-26 | Lying finality (finalized > head, or oscillating) | WP-12 arbitration; oscillation masked by the monotone max *within an epoch* — a T1 re-seed may adopt a lower value, alarmed (INV-12, ADR-14); the above-head obligation is the maximum alone, so report rate buys upstream no state (ADR-16); alarmed if contradiction (FM-18) |
| FM-27 | Stale head (head report behind delivered blocks) | mask (head reads are advisory; readiness may flap) |

## Process faults

| FM | Fault | Required response |
|---|---|---|
| FM-30 | **Unrecoverable divergence** (FM-19) or equivalent contradiction that no retry can heal | terminal alarm: raise OB-7 terminal state, drain in-flight responses, then exit non-zero (ADR-12); MUST NOT continue serving while appearing healthy with ingestion silently dead (GAP-4) |
| FM-31 | Startup failure (T1 impossible: bad config, unreachable upstream) | exit non-zero with diagnostic (REQ-32) |
| FM-32 | Internal panic/defect in one activity | contain (FM-3); if the writer is affected: recover via ladder/T1 or exit — never zombie (INV-41) |
| FM-33 | Crash / kill at any point | restart per REQ-13; recovery contract INV-40 |
| FM-34 | Dual instance behind one address | tolerated: the protocol is stateless per request (RP-5); clients may see version flapping between instances but never corruption — each response is one instance's snapshot. INV-29's monotonicity is scoped per instance for exactly this reason (ADR-14); no affinity or epoch token is offered (ADR-1 pins the wire) |

## Client faults

| FM | Fault | Required response |
|---|---|---|
| FM-40 | Malformed / oversized request | INVALID_REQUEST (RP-2); mask for everyone else |
| FM-41 | Slow reader | backpressure within RP-21 bounds; truncation at budget; isolation per INV-35 |
| FM-42 | Mid-response disconnect | reap within `P-DISCONNECT-REAP` (RP-21); mask |
| FM-43 | Request flood | degrade within declared limits (RP-23, HZ-6); ingestion isolation holds (INV-35) |
| FM-44 | Absurd positions (far-future `from`, ancient `from`) | far-future: empty result (RP-3); ancient: backfill or INTERNAL (RP-8); never process harm |

## Operator faults

| FM | Fault | Required response |
|---|---|---|
| FM-50 | Invalid option value / combination | FM-31 (reject at startup, REQ-32) |
| FM-51 | Option the build cannot honor | FM-31 (INV-36) |
| FM-52 | Wrong network endpoint (chain mismatch mid-run) | manifests as FM-10/FM-19 → containment + FM-30 path; SHOULD be detected at startup by a network-identity check (14 §upstream) |
| FM-53 | Undersized window for the chain's finality lag | over-window alarm (OB-6) or autoAdjust per WP-24; never OOM-by-design without alarm |

Which test class exercises which fault family is 13's business: CT-4 owns the input
and upstream families, CT-2 the process families, CT-8/CT-9 the client families, and
CT-5 the operator families. The traceability matrix there is the single record of what
each class covers today.
