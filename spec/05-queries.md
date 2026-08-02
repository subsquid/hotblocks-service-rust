# 05 — Queries & the result contract

Bands: RP-1..4 operations & admission, RP-5..19 result/conflict/error contracts,
RP-20..29 resource rules.

## Operations

**RP-1 — Operation table.**

| Operation | Kind | Result |
|---|---|---|
| stream(from, parentHash?) | range read | block records ∪ empty ∪ conflict ∪ error |
| head() | watermark read | BlockRef |
| finalizedHead() | watermark read | BlockRef |
| readiness() | status read | boolean |
| liveness() | status read | constant |
| metrics(), metric(name) | status read | metric exposition |
| blockIngestTime(height) | status read | timestamp ∪ not-found |

All operations are reads of committed state (INV-16); none mutates the buffer (WP-1).
Every operation is naturally idempotent and safe to retry.

## Admission

**RP-2 — Validation.** [MUST] A stream request is admitted only if: the body is
well-formed per 14 (IB-1, IB-3), `from` is a valid Height, the optional `parentHash` is a string,
and the body does not exceed `P-REQ-BODY-MAX`. Violations map to the INVALID_REQUEST
error class (RP-13); unknown request fields are ignored (14 pins this for
compatibility). There is no authentication in scope (deployment perimeter concern).

**RP-3 — Range resolution.** [MUST] Resolution cost is bounded per PF-3.
Against the resolution snapshot (DEF-13 — fresh at admission, and once more after a
wait per RP-4),
`from` resolves to exactly one of:
1. **window** — `first(C).parentNumber < from ≤ head(C).number`: serve the buffered
   suffix starting at the lowest buffered height ≥ `from` (DEF-30, after the base
   check, RP-11). The lower bound is the parent link, not `first(C).number`, so a
   `from` strictly between them still resolves here.
2. **above head** — `from > head(C).number`: wait per RP-4, then re-resolve once —
   a full resolution against a fresh snapshot that may land in *any* of the three
   cases, including window-underflow when eviction crossed `from` during the wait
   (GAP-30 marks the current miss); if still above head, complete empty (RP-12's
   empty form). Note: the base check
   (RP-11) applies only when `from = head.number + 1` — a request farther ahead cannot
   be validated against any buffered block and completes empty even if its
   `parentHash` is nonsense (explicitly tolerated; the client discovers the mismatch
   when the height enters the window).
3. **below window** — `from ≤ first(C).parentNumber`: window-underflow (RP-8). Void
   when `first(C)` is a root (DEF-4's exception, `parentNumber = number`): nothing
   exists below a root, so every `from ≤ head` resolves to the window and is served
   from the root up.

**RP-4 — Bounded waiting.** [MUST] The above-head wait lasts at most `P-WAIT-BLOCK`;
it is released early by any commit that reaches `from` (LIV-4). At most one
re-resolution happens per request (no unbounded internal retry).

## The result contract

**RP-5 — Coverage.** [MUST] A successful (non-empty) response delivers blocks
`x_from … x_last` such that: the sequence is ascending and pairwise linked (DEF-5);
the first delivered block is the lowest response-eligible height ≥ `from` (DEF-30);
every delivered block is response-eligible (DEF-14);
and `last ≥ from` (≥ 1 block — INV-23). **Early stop is normal**: the
service may end the response at any record boundary (budget `P-RESP-BUDGET`, internal
limits, or the snapshot's end). The client recovers coverage exclusively from delivered
records (DEF-30); zero server-side session state exists.

**RP-6 — Emission.** [MUST] Records are emitted in ascending order, complete within
coverage (no block skipped inside `[from, last]`), each framed independently per
REQ-6. Truncation granularity is the record: a response never ends inside a record.
Emission is deterministic modulo the declared free variables (13 §free-variables):
where coverage ends, and transport chunking — the records themselves, given the same
snapshot and request, are byte-identical.

**RP-7 — Conflict protocol.** [MUST] A conflict (DEF-31) is returned instead of data
when the client's claimed base fails the base check (RP-11) — and only then
(INV-22). The conflict carries `prev: ⟨BlockRef⟩` with: `prev` non-empty; at most
`P-FORK-REFS-MAX` refs (a size bound, not a selection rule — the choice within it
stays free, 13 §free-variables); ascending by number; every ref on (or an
ancestor-ref of) the chain the service currently holds; the newest ref last. The **client recovery algorithm** is normative:

```
recover(client_chain, prev):
  i ← |client_chain|                       // newest own block
  for r in prev descending:
      if ∃ c ∈ client_chain: c.number = r.number ∧ c.hash = r.hash:
          rollback client state above c
          return resume(from: c.number+1, parentHash: c.hash)
  // no overlap with prev:
  return resume(from: oldest(prev).number, parentHash: ⊥)   // or fail if client
                                                            // cannot roll back that far
```

Each conflict round strictly lowers the resume point or converges; the service MUST
choose `prev` such that repeated application terminates — a conflict response that
could reproduce the identical conflict forever is non-conforming. Deeper-than-window
divergence surfaces as the client exhausting `prev` below the window and switching to
RP-8 territory or giving up; the service side of that condition is FM-30.

**RP-8 — Window-underflow (backfill) queries.** [MUST] For `from` below the window:
the service acquires the missing range `[from, first(C).parentNumber]` from upstream
(bounded to the acquisition target that can no longer reorg — the finalized level) and
serves it followed by the snapshot suffix, as one response obeying RP-5/RP-6 across
the splice point (gap-free, linked). The base check applies at `from` against the
acquired chain. Failures: a fork signal during backfill → conflict with the signal's
refs; any other acquisition failure → INTERNAL error if no record has been sent yet,
else truncation (RP-12). An empty acquisition result where blocks were expected is an
INTERNAL error, never an empty conflict (INV-27, GAP-6).

**RP-9 — Watermark reads (DEF-11).** [MUST] `head()`/`finalizedHead()` return refs from one
committed state (INV-24); freshness: any commit is reflected by all subsequent
watermark reads (INV-24 — no caching that outlives a commit). Successful and empty
stream responses carry the snapshot's finalized-head ref as metadata (14 §headers);
conflicts and errors need not.

**RP-10 — Readiness.** [MUST] Readiness (DEF-32) compares the *last observed* upstream
head (OB-4) to the buffered head and reports true iff `buffered ≥ observed` and the
observation is younger than `P-STALL-ALARM`; a stale view reports not-ready. The probe
is a local read — it never calls upstream and never reports an internal error, since
its consumers are routers (ADR-17 ⚠ pending ratification; it lands with OB-4's gauge,
GAP-25, and until then the service still probes per request — GAP-34). Liveness is
unconditional while the process serves at all.

**RP-11 — The base check.** [MUST] When the client supplies `parentHash` and `from`
resolves to the window or to backfill, let `x` be the lowest response-eligible block
at height ≥ `from` (DEF-30): admit iff
`x.parentHash = parentHash` (exact equality per DEF-2) — a resuming client's
`parentHash` names its last block, which is `x`'s parent whether or not the
intervening heights exist. When `from = head.number + 1`: admit iff
`parentHash = head.hash`. Omitted `parentHash` always admits (the client opts out of
fork detection for this request).

## Empty results, truncation, errors

**RP-12 — Empty result & truncation.** [MUST] The empty result — no block reached
`from` within the wait, or no record was produced within the response budget (RP-20)
— is a distinct successful form (14 binds it) carrying watermarks, letting the
client re-poll. **Truncation caveat**: an already-sent record
prefix is always valid data regardless of why the response ended (budget, internal
error after first record, disconnect); truncation is indistinguishable from a chosen
early stop *by design* — clients MUST treat any record-boundary end as normal
(ADR-10). Consequence: after the first record is sent, errors can no longer be
signaled in-band; they surface only as truncation (INV-27 requires the error still be
alarmed server-side, OB-7).

**RP-13 — Error taxonomy.** [MUST] Closed set; concrete codes in 14 §errors.

| Class | Trigger | Retryable? |
|---|---|---|
| INVALID_REQUEST | RP-2 violation | no (fix the request) |
| CONFLICT | base check failure (RP-11) | yes, via RP-7 recovery |
| EMPTY | RP-12 empty result | yes (poll) |
| NOT_FOUND | unknown metric / unknown ingest-time height / unknown route | no |
| INTERNAL | acquisition or service failure before first record | yes (backoff) |

No other terminal outcome exists; in particular a data payload is never accompanied by
an in-band error marker (INV-27), and CONFLICT always carries a usable `prev`
(RP-7 — GAP-6).

## Resource rules

**RP-20 — Response budget.** [MUST] Record production per request stops at
`P-RESP-BUDGET` from admission (REQ-21). The wait of RP-4 counts against the budget.
A budget that expires before the first record is sent completes as the empty form
(RP-12) — expiry is a bound, not a failure, so INTERNAL is never owed for it; after
the first record it surfaces as truncation.

**RP-21 — Slow clients & disconnect.** [MUST] A client that stops reading receives
backpressure, not unbounded server-side buffering: per-request buffered-but-unsent
data is bounded by `P-RESP-BUFFER`. A disconnect releases all request-scoped resources
(snapshot, backfill acquisition, buffers) within `P-DISCONNECT-REAP` (LIV-10). Neither a slow
client nor a disconnect affects ingestion or other requests beyond the declared shared
budgets (INV-35).

**RP-22 — Query-side upstream budget.** [MUST] Window-underflow acquisitions (RP-8)
draw from the *same* configured upstream budget as ingestion
(REQ-16, ADR-3). This coupling is deliberate and declared; fairness between ingestion
and query-side acquisition is a known hazard (HZ-1) until a priority mechanism exists.

**RP-23 — Admission control.** [MUST] Concurrent stream requests are bounded by
`P-MAX-CONCURRENT-STREAMS`, making aggregate response-side memory derivable from
configuration (PF-1, G4). A request beyond the bound is refused promptly with an
INTERNAL-class error (retryable, RP-13), never queued unboundedly. The current
implementation admits without bound — GAP-28; until it closes, HZ-2/HZ-6 stand as
the operative risks.
