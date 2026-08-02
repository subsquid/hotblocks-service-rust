# 02 — Product requirements

Bands: 1–9 core serving, 10–19 ingestion & data management, 20–29 quality, 30–39
operations. Acceptance *status* lives in [13-conformance-tdd.md](13-conformance-tdd.md).

## Core serving

**REQ-1 — Resumable block stream.** [MUST]
A client that names a starting height (and optionally the hash of that height's parent)
receives a contiguous, ascending, parent-linked sequence of canonical block records
starting at the lowest available height at or above the named one (DEF-30). Any
response prefix is independently valid: the client
may resume later from the last block it received, with no server-side session state.
*Acceptance:* for every successful stream response, blocks decode, ascend,
parent-link, and start at the lowest available height ≥ the requested one (DEF-30);
issuing a follow-up request from `last+1` with
`last.hash` succeeds or conflicts, never gaps.
*Trace:* RP-5, RP-6, INV-20, INV-23.

**REQ-2 — Explicit fork recovery.** [MUST]
When the client's claimed position is not on the chain the service currently holds, the
service refuses with a conflict result carrying a non-empty, ascending list of block
references from its own recent chain, sufficient for the client to locate a common
ancestor and re-request from there. Following the recovery algorithm of RP-7 always
either converges to streaming or reports the fork as deeper than the service's window.
*Acceptance:* every conflict response carries ≥ 1 reference; replaying RP-7's client
algorithm against the service terminates, each round strictly lowering the resume
point (RP-7's termination obligation).
*Trace:* RP-7, INV-22; ADR-1.

**REQ-3 — Watermark reads.** [MUST]
The service exposes its current head and finalized head as point reads, and attaches the
finalized head to every successful stream response. Reported watermarks always come from
one committed buffer state.
*Acceptance:* watermark reads parse as (number, hash) and equal some committed state's
head/finalized head; finalized ≤ head at that state.
*Trace:* RP-9, INV-24.

**REQ-4 — Bounded waiting.** [MUST]
A request for blocks just above the current head is held open at most `P-WAIT-BLOCK`
waiting for the next block, then completes as an explicit empty result (with watermarks)
rather than an error. Clients distinguish "no data yet" from every failure mode.
*Acceptance:* a request at head+1 completes within `P-WAIT-BLOCK` + `P-SLO-QUERY-OVERHEAD`
with either ≥ 1 block or the empty-result form of 14.
*Trace:* RP-4, LIV-4.

**REQ-5 — Backfill below the window.** [MUST]
A request starting below the buffered window is served by re-acquiring the missing range
from upstream and splicing it, gap-free and parent-linked, onto the buffered chain in
one response. Failure to backfill is an explicit error, never a silent gap.
*Acceptance:* a request `P-CACHE-SIZE` below head yields a contiguous stream crossing
the window boundary with no discontinuity, or a taxonomy error (RP-11).
*Trace:* RP-8, INV-20.

**REQ-6 — Self-contained per-block framing.** [MUST]
Each block travels as an independently decodable compressed record; a response is a
plain concatenation of such records. Truncating a response at any record boundary leaves
every delivered record valid. Stored compression is reused verbatim for clients that
accept it; otherwise records are re-encoded per record, never re-framed as one stream.
*Acceptance:* cutting a response after any record and decoding what was received
succeeds; both negotiated encodings decode to identical bytes.
*Trace:* RP-6, RP-12, INV-25; ADR-7.

**REQ-7 — Canonical deterministic payload.** [MUST]
The per-block record is the chain family's canonical normalized form: a deterministic
function of the upstream block data and the data-selection configuration. Two
acquisitions of the same upstream data yield byte-identical records.
*Acceptance:* repeated acquisition of a fixed recorded upstream corpus is byte-stable
across runs and process restarts; golden fixtures match.
*Trace:* INV-25, INV-26; ADR-1.

**REQ-8 — Data selection.** [MUST]
The operator selects which data components (beyond header and transactions) are
acquired and served — e.g. receipts or logs, execution traces, state diffs — via
configuration; every selected component appears in every served block, and unselected
components are absent.
*Acceptance:* for each selection configuration, served records contain exactly the
selected component set.
*Trace:* REQ-9, INV-28, 14 §configuration.

**REQ-9 — Component coherence.** [MUST]
A block becomes servable only when all selected components are present and mutually
consistent (they describe the same block, agree on counts, indices, and cross-component
identities per DEF-15). A block that cannot be made coherent within the acquisition
retry budget is an ingestion error (WP-11), never a served block with silently emptied
or partial components.
*Acceptance:* under every injected component fault of FM §input, no served block ever
lacks a selected component or fails DEF-15; the fault surfaces as retry-then-alarm.
*Trace:* INV-28, WP-11, FM-10..; GAP-3.

## Ingestion & data management

**REQ-10 — Head following.** [MUST]
The service tracks the chain head through the upstream node, tolerating
reorganizations of any depth above the finalized head: on any reorganization it
converges to the upstream's canonical chain without operator action.
*Acceptance:* under simulated reorganizations of depth 1..(window−1) that stay above
the finalized head (LIV-8's precondition — deeper divergence is FM-30 territory, not
convergence), the buffer converges to the new canonical branch within
`P-SLO-REORG-CONVERGE`; INV-11 never violated.
*Trace:* WP-5, WP-10, LIV-8.

**REQ-11 — Finality tracking.** [MUST]
The finalized head is obtained from the network's finality signal or, where the network
provides none, as a configured depth offset from the head. Within an epoch it advances
monotonically (INV-12; a T1 re-INIT opens a new epoch and MAY seed lower, alarmed —
ADR-14), never above the buffered head's height without validation, and contradictory
or regressive finality reports are tolerated without state corruption.
*Acceptance:* under regressive/contradictory injected finality reports, the finalized
watermark is monotone within each epoch and the process stays healthy.
*Trace:* WP-12, WP-20, INV-12; GAP-7; ADR-6, ADR-14.

**REQ-12 — Bounded window.** [MUST]
The buffer holds at most `P-CACHE-SIZE` blocks plus transient batch overshoot
(INV-4). Eviction removes only finalized blocks (oldest first). When lagging finality
blocks eviction, the operator chooses by configuration between (a) exceeding the window
with a continuous alarm and (b) force-advancing finality to keep the bound
(`auto-adjust`, WP-24).
*Acceptance:* with auto-adjust on, no committed state's buffer exceeds `P-CACHE-SIZE`
(INV-4); with it off, every over-window state is accompanied by an active alarm
observable (OB-6).
*Trace:* DEF-24, WP-24, INV-4, LIV-11; GAP-2; ADR-9.

**REQ-13 — Unattended restart.** [MUST]
After any process exit, a fresh start reaches serving state from configuration and
upstream state alone: it reseeds from the upstream finalized head and catches up. No
local state, operator action, or client cooperation is required.
*Acceptance:* kill-and-restart under load reaches readiness within `P-SLO-STARTUP` +
catch-up time; clients recover using only REQ-1/REQ-2 semantics.
*Trace:* WP-15, INV-40, LIV-5.

**REQ-14 — Optional verification.** [SHOULD]
The operator can enable independent verification of upstream-supplied integrity claims
(e.g. block hash, transaction/receipt commitments, sender recovery), each as a separate
switch. An enabled check that fails makes the block incoherent (REQ-9 path).
[MUST] Every accepted verification switch is enforced; a switch the build cannot honor
is rejected at startup (INV-36).
*Acceptance:* for each switch: a corpus block with a forged field is rejected when the
switch is on and accepted when off; enabling an unimplemented switch fails startup.
*Trace:* INV-36; GAP-8, GAP-9.

**REQ-15 — Supported-network quirks.** [MUST]
For every network in the supported set (an operational decision, ADR-8), known
deviations of that network from the chain family's baseline (phantom transactions,
duplicated receipts, system transactions exempt from verification, non-standard trace
responses) are normalized or tolerated such that REQ-9 and REQ-10 hold on that network.
*Acceptance:* the per-network quirk corpus (13 §CT-4) passes for every supported
network.
*Trace:* FM-14, ADR-8; GAP-16.

**REQ-16 — Upstream protection.** [MUST]
Requests to the upstream node respect a configured rate limit and batch-size cap across
*all* internal activities combined (head following, finality probing, backfill), with
bounded, classified retries: transient upstream errors are retried on a bounded
schedule; non-transient errors surface immediately.
*Acceptance:* under a metered fake upstream, aggregate request rate never exceeds the
configured limit by more than `P-RATE-TOLERANCE`; retry counts per call are bounded by
`P-RPC-RETRY-ATTEMPTS`.
*Trace:* WP-11, FM-20..; ADR-3; GAP-10, GAP-20, GAP-21.

**REQ-17 — Multi-VM extensibility.** [MUST]
The core state machine, query contract, and HTTP surface are chain-agnostic: block
height is an opaque ascending coordinate with an explicit parent coordinate (heights
MAY skip), hashes are opaque strings, and payloads are opaque records. Adding a chain
family requires only a new acquisition adapter and an upstream binding section in 14.
*Acceptance:* the conformance suite's input simulator (13, HC-1) exercises the full
core contract with a synthetic non-EVM chain (including skipped heights) and passes.
*Trace:* DEF-1, ADR-2; open question OQ-1.

## Quality

**REQ-20 — Head latency.** [SHOULD]
Under the steady workload (11 §S1) with a healthy upstream, a new canonical block is
servable within `P-SLO-HEAD-LATENCY` of its availability upstream.
*Acceptance:* SLI-1 measured under S1 meets the SLO table of 11.
*Trace:* SLI-1, PF-2; ADR-4, ADR-5.

**REQ-21 — Query termination.** [MUST]
Every request completes — with data, an empty result, or a taxonomy error — within
`P-RESP-BUDGET` plus transport time of the data actually sent. Early termination at the
budget is a valid, truncated response (REQ-6), not an error.
*Acceptance:* no request observed to exceed `P-RESP-BUDGET` + `P-SLO-QUERY-OVERHEAD`
before its last byte or termination, under all of 11's scenarios.
*Trace:* RP-13, LIV-3.

**REQ-22 — No externally-triggered death or corruption.** [MUST]
No upstream response, client request, or timing of either may terminate the process —
the sole exceptions are the explicit terminal exits of FM-30/FM-31 (IB-11, ADR-12) —
or leave it in a state where any endpoint or ingestion permanently fails while the
process continues to run.
*Acceptance:* fault corpus (CT-4) and fuzz (CT-9) never produce process exit or a
permanently failing endpoint; after every injected fault the service either serves
correctly or has exited by an explicit FM-30/FM-31 path.
*Trace:* FM-1, FM-30, FM-31, INV-41; GAP-1.

**REQ-23 — Graceful shutdown.** [MUST]
On the stop signal the service stops accepting work and exits within
`P-SHUTDOWN-GRACE`, regardless of upstream or client state. A repeated stop signal
forces immediate exit. Shutdown is crash-equivalent or better: nothing it does can make
the subsequent restart (REQ-13) worse than after a crash.
*Acceptance:* stop under each of: idle, mid-stream response, upstream stalled — exits
within `P-SHUTDOWN-GRACE`; restart then satisfies REQ-13.
*Trace:* LIV-9.

**REQ-24 — Predecessor wire compatibility.** [MUST — sunset per OQ-4]
Until the migration cutover is declared complete, the service is byte-compatible with
its predecessor implementation on: the HTTP surface of 14, the per-block payload bytes
(REQ-7), and configuration names/defaults. Deliberate divergences are enumerated in 14
and each cites an ADR.
*Acceptance:* differential run against the predecessor on a recorded corpus (HC-8)
shows no diffs outside the enumerated list.
*Trace:* ADR-1; GAP-18, GAP-19, GAP-27.

## Operations

**REQ-30 — Observability surface.** [MUST]
The service exposes machine-readable metrics, a liveness probe, and a readiness probe
that reflects "caught up to the upstream head". All signals of 12 are present and
truthful (a registered signal that cannot move is a defect).
*Acceptance:* 12's property→observable table is fully decidable from the scrape
surface; OB conformance checks (CT-5) pass.
*Trace:* OB-1..; INV-30; GAP-24, GAP-25.

**REQ-31 — Bounded log volume.** [SHOULD]
Steady-state operation emits at most `P-LOG-RATE-STEADY` log records per ingested
block at the default level, and repeated identical conditions are throttled.
*Acceptance:* S1 soak: log records / blocks ≤ `P-LOG-RATE-STEADY`; a persistent
alarm condition emits at a throttled rate, not per event.
*Trace:* OB-8; GAP-23.

**REQ-32 — Configuration honesty.** [MUST]
The full configuration surface is the one in 14: every accepted option takes effect;
invalid values, unsupported combinations, and malformed endpoints are rejected at
startup with a diagnostic, not at first use.
*Acceptance:* startup matrix over invalid configs exits non-zero with a diagnostic
naming the option; INV-36 checks pass for every accepted option.
*Trace:* INV-36, 14 §configuration; GAP-8, GAP-20.

## Explicitly unspecified

The following are deliberately left open; conformance tests MUST NOT pin them:

- Exact log text, levels, and format.
- Internal batch grouping of ingested blocks (only atomic visibility per batch, WP-3).
- Poll cadence, prediction, and scheduling internals of the acquisition adapter.
- The count and choice of references inside a conflict response beyond RP-7's contract.
- Response chunking at the transport layer.
- Compression level and encoder identity (only decoded bytes are contracted, REQ-6).
- Additional metrics beyond 12's required set.
- Behavior of `HEAD`/`OPTIONS` and unknown routes beyond 14's table.

## Open questions

| ID | Question | Owner | Blocking |
|---|---|---|---|
| OQ-1 | Non-EVM adapters (Solana first): mapping of slots→height/parent coordinates, finality/commitment semantics, payload canonical form — what, if anything, does the core contract miss? | product/eng | REQ-17 acceptance beyond the synthetic adapter |
| OQ-2 | The intended post-migration redesign ("change how it works") is not yet specified; which parts of this suite does it supersede? | product | roadmap ADRs |
| OQ-3 | Should the buffer survive restarts (warm restart), or is NG1 permanent? | product/eng | NG1, INV-40 |
| OQ-4 | When is REQ-24 (predecessor byte-compatibility) sunset, releasing GAP-18/19 and the `?json` shape to be fixed forward? | product | REQ-24, GAP-18, GAP-19 |
| OQ-5 | One process per chain (NG3) — does multi-dataset serving enter the roadmap? | product | NG3 |
| OQ-6 | The supported-network set for REQ-15 (is Cronos/Hedera/Tac in scope?) | ops | GAP-16 priority |
