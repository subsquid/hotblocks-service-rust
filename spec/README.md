# Hotblocks service — specification suite

The hotblocks service follows a blockchain's head through an upstream node, buffers the
most recent blocks in memory, and serves them to clients as a resumable, fork-aware
stream of canonical per-block records. It is the low-latency "hot" end of the Subsquid
data pipeline; clients recover from chain reorganizations through an explicit conflict
protocol.

This suite is a **conformance-tier, stateful-service** specification. It states what the
service MUST do (independently of any implementation), records why it is that way (the
decision log), and defines the machinery that keeps it true: a reference model to test
against, a test-class taxonomy, a traceability matrix, a prioritized gap register, and
merge gates. The service core is chain-agnostic by design; chain families (EVM today,
others later) plug in as acquisition adapters whose upstream binding lives in
[14-interface-binding.md](14-interface-binding.md).

## Document map

| Doc | Contents | Normative? |
|---|---|---|
| [01-overview.md](01-overview.md) | context, actors, goals, non-goals, trust model, lifecycle | yes |
| [02-requirements.md](02-requirements.md) | product requirements `REQ-n` with acceptance criteria | yes |
| [03-data-model.md](03-data-model.md) | definitions `DEF-n`: state tuple, events, policies | yes |
| [04-mutations.md](04-mutations.md) | ingestion loop and transition catalog `WP-n`, incl. eviction (T5) | yes |
| [05-queries.md](05-queries.md) | query & conflict contract `RP-n` | yes |
| [07-invariants.md](07-invariants.md) | safety catalog `INV-n`, incl. commit model and recovery | yes |
| [08-liveness.md](08-liveness.md) | progress properties `LIV-n` | yes |
| [09-failure-model.md](09-failure-model.md) | fault families and required responses `FM-n` | yes |
| [11-performance.md](11-performance.md) | SLIs/SLOs, workload model, hazards `PF/SLI/HZ-n` | yes |
| [12-observability.md](12-observability.md) | required signals `OB-n` | yes |
| [13-conformance-tdd.md](13-conformance-tdd.md) | reference model, `CT-n`, matrix, `GAP-n`, `MG-n`, `HC-n` | **mutable** |
| [14-interface-binding.md](14-interface-binding.md) | HTTP wire contract, CLI, upstream binding `IB-n` | yes |
| [15-parameters.md](15-parameters.md) | parameter registry (`P-*`, `W-*` values) | **mutable** |
| decisions/ | ADR log, one file per decision | append-only |

## Conventions

- **RFC 2119**: MUST / MUST NOT / SHOULD / MAY are normative keywords.
- **IDs** are stable and never renumbered. Prefixes: `REQ` requirements, `DEF`
  definitions, `WP` write/mutation properties, `RP` read/query properties, `INV` safety
  invariants, `LIV` liveness, `FM` failure-model rows, `PF` performance requirements,
  `SLI` indicators, `HZ` hazards, `OB` observables, `CT` test classes, `MG` merge gates,
  `HC` harness capabilities, `GAP` gaps, `IB` binding rules, `ADR` decisions. Numbering
  is **banded** per category so insertions never renumber (bands are declared at the top
  of each catalog); gaps in a band, and in the document numbering, are intentional.
- **One fact, one home.** Every rule is stated once, where it is decided, and cited
  everywhere else. A restatement that drifts is worse than a link.
- **Symbolic parameters**: every constant appears in normative text only as a `P-NAME`
  (or workload `W-NAME`) symbol. Concrete values, observed vs target, live only in
  [15-parameters.md](15-parameters.md). ⚠ marks a proposed target awaiting ratification
  by ADR.
- **Math**: ℕ = natural numbers, ⊥ = absent/undefined, `⟨…⟩` = ordered sequence,
  `|s|` = length.
- **Invariant scopes**: `[state]` holds in every observable state; `[transition]`
  relates consecutive states; `[response]` holds of every response; `[recovery]` holds
  across restart.
- **Mutability rule**: only two documents ever change without a change of intent —
  13 (statuses, matrix, gap register) and 15 (observed/target values). `decisions/` only
  gains files; an accepted ADR is never edited except to mark it superseded. Every other
  document changes only when *intended behavior* changes.

## How to use this suite

1. **Ratify**: review `Proposed` ADRs and ⚠ parameter targets; accept or amend them
   (each acceptance is an ADR edit from `Proposed` → `Accepted` plus a registry update).
2. **Build the harness**: follow the build order in
   [13-conformance-tdd.md](13-conformance-tdd.md) — input simulator and reference model
   first, then the P0 gaps, each closed by a failing test first.
3. **Keep it honest**: changes land only through the merge gates (MG-1..MG-8); the
   traceability matrix and gap register are updated in the same
   change as the code they describe; `scripts/check-spec.py` gates spec integrity in CI.
