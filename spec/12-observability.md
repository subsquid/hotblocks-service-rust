# 12 — Observability

Required signals. Concrete metric names/routes are bound in 14 (IB-12); here each
signal is a numbered obligation. "Level" = a gauge readable at any time; "event" = a counter.

**OB-1 — State gauges.** [MUST] Levels for: first buffered height, head height,
finalized height, buffered block count, and window excess (INV-4). Fresh per commit
(INV-24).

**OB-2 — Progress heartbeat.** [MUST] A signal that distinguishes *idle input* (no new
upstream block) from *stalled service* (upstream advanced, nothing committed): the
height and wall-time of the last commit, alongside OB-4. An observer with one scrape
pair must be able to compute "commits are happening" independently of block content —
so the heartbeat counts *commits*, not blocks: a batch that inserts nothing because
WP-6 absorbed it is still ingestion working, and a head height alone would read a
redelivery stretch as a dead service.

**OB-3 — Lag.** [MUST] Levels/distributions for block arrival lag (block timestamp →
commit) where the chain family has timestamps; ⊥-timestamp chains expose the
distribution as absent, never zero. While REQ-24 stands, the pinned wire
representation of absence is the −1 sentinel of IB-12 (ADR-1; revisited at OQ-4
sunset).

[SHOULD] Where the adapter discovers the head by polling, that lag is decomposable
without a second time source: the wait preceding a poll that discovered a block —
the interval the block may have gone unnoticed in, less any part of it the adapter
spent parked on its consumer, which no cadence would have shortened — and the
observed arrival interval feeding the poll cadence are both distributions. Neither
needs the true production time, and together they separate a poller mispredicting
the cadence from a chain whose arrivals are simply spread out.

[SHOULD] Where the adapter acquires a block's data separately from its header, the
re-acquisitions that costs are counted by reason, so lag sitting after detection is
separable from lag sitting before it.

**OB-4 — Upstream view.** [MUST] The upstream head height *and upstream finalized
height* as last observed (LIV-7/SLI-6 are decidable only with the latter), and
upstream interaction health: request/error/retry counts by class (REQ-16
visibility). Staleness of the upstream view is itself visible (last-observed
timestamps). The head timestamp records the last successful explicit head read used
for readiness. A committed source batch may advance the displayed height without
refreshing that timestamp; the core supplies the bounded periodic read, while
adapter-specific acquisition may improve the number between reads (ADR-6/17, HZ-1).

**OB-5 — Operation metrics.** [MUST] Per query class (window / wait-empty / backfill /
conflict / error): counts and duration distributions; truncations (RP-12) counted
separately by cause (budget, error-after-first-record, disconnect). Ingestion:
per-batch commit duration distribution.

**OB-6 — Retention alarms.** [MUST] Over-window condition as a level (active/inactive
+ magnitude) and force-advance events (WP-24) as a counter with the advanced-past
height. (INV-31: level + event, not log-only.)

**OB-7 — Ingestion alarms.** [MUST] Reason-coded events + a current-state level for:
integrity violations (WP-5), session restarts by cause (error / fork / stall-reset
T1), acquisition retry exhaustion (WP-11.3), fork rebases, and the terminal FM-30
state. The stall alarm (LIV-2) is a level that flips at `P-STALL-ALARM`.

**OB-8 — Bounded cardinality & volume.** [MUST] All label sets are closed and
enumerable at startup (no per-block/per-client labels), and every value is
registered at zero before its first event — a series that appears when it first
moves cannot be alerted on beforehand. Log volume per REQ-31.

**OB-9 — Lifecycle.** [MUST] Timestamps (as levels) for: process start, first
acceptance, first commit, last commit (OB-2), shutdown start. Readiness semantics per
RP-10; liveness unconditional (RP-1).

**OB-10 — Capture-on-stall.** [SHOULD] When the stall alarm (OB-7) flips on, the
service captures a diagnostic snapshot (session state, last error, ladder position) to
the log stream — once per flip, not per retry.

## Property → observable mapping

| Property | Decided by |
|---|---|
| LIV-1/LIV-2 head progress & stall | OB-2 vs OB-4 + OB-7 stall level |
| LIV-4 waiter release | OB-5 wait-path durations (released waiters complete in the window class; expiries in wait-empty) |
| LIV-5 startup bounds | OB-9 timestamps |
| LIV-6 catch-up | OB-2 rate vs OB-4 |
| LIV-7 finality keep-up | OB-1 finalized vs OB-4 upstream-finalized |
| LIV-8 reorg convergence | OB-7 fork events + OB-2 |
| LIV-9 shutdown | OB-9 + process exit |
| LIV-11 eviction convergence | OB-1 stored/excess + OB-6 level |
| INV-4 window bound | OB-1 excess + OB-6 |
| REQ-16 upstream budget | OB-4 counters |
| RP-12 truncation visibility | OB-5 truncation counters |
| FM-30 terminal state | OB-7 terminal level |

**The harness rule**: lying metrics are failures — during conformance runs every OB
signal is cross-checked against the simulator ledger (HC-2); a signal that disagrees
with ledger truth fails the run exactly like a wrong response (INV-30).
