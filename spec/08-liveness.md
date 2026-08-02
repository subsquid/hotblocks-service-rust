# 08 — Liveness

## §0 — Environmental definitions

Liveness claims hold only under these premises; outside them the obligation reduces to
"alarm or keep trying, boundedly" (FM verbs).

- **Healthy upstream**: the upstream node answers within `P-RPC-TIMEOUT`, error rate
  below `W-UPSTREAM-ERR`, produces blocks at the workload cadence `W-BLOCK-INTERVAL`,
  and its reported head/finality are self-consistent.
- **Adequate resources**: CPU, memory, and network per 11's resource-bound
  requirements; no starvation by co-tenants.
- **Quiescent**: no input event pending and no in-flight request.
- Bounds are end-to-end observable (each names its witness observable OB-n), and
  none of them participates in ordering or identity decisions (INV-5).

## Properties

**LIV-1 — Head progress.** Precondition: healthy upstream, adequate resources.
Bound: every new canonical block is committed (servable) within `P-SLO-COMMIT-LATENCY`
of upstream availability (SLI-2; the end-to-end client-visible bound is REQ-20's
`P-SLO-HEAD-LATENCY`, SLI-1). Witness: OB-2 heartbeat + OB-3 lag. Check: CT-6 (S1).

**LIV-2 — Bounded stall / convergence-or-alarm.** Precondition: process alive.
Bound: zero commit progress while the upstream head advances lasts at most
`P-STALL-ALARM` before the stall alarm (OB-7) is active; the ladder (WP-9) keeps
attempting recovery forever (with T1 re-seed after `P-STALL-REINIT` stalled sessions)
— there is no silent terminal idle state (GAP-4). Witness: OB-2 vs OB-4 divergence,
OB-7, with OB-10 capture on the flip. Check: CT-4 (persistent-fault corpus) + CT-7.

**LIV-3 — Query termination.** Precondition: none (holds under overload).
Bound: every admitted request reaches a terminal outcome (last byte, empty, error, or
truncation) within `P-RESP-BUDGET` + `P-SLO-QUERY-OVERHEAD` of admission, plus
transport drain time bounded by RP-21. Witness: OB-5 duration metrics. Check: CT-3,
CT-8.

**LIV-4 — Waiter release.** Precondition: a commit reaches the awaited height.
Bound: the waiting request unblocks within `P-WAKE-LATENCY` of the commit; absent a
commit, within `P-WAIT-BLOCK` (RP-4). Witness: OB-5 wait-path latency. Check: CT-1
timing assertions.

**LIV-5 — Startup bounds, decoupled.** Precondition: healthy upstream at start.
Bound: the service *accepts* connections within `P-STARTUP-ACCEPT` of process start
(liveness endpoint answers even before catch-up); it reaches *readiness* within
`P-SLO-STARTUP` + catch-up time proportional to the gap. Acceptance never waits on
catch-up (PF-5). Witness: OB-9 lifecycle timestamps. Check: CT-2.

**LIV-6 — Catch-up.** Precondition: healthy upstream, gap ≤ `W-CATCHUP-GAP`.
Bound: sustained ingestion throughput during catch-up ≥ `P-SLO-CATCHUP-RATE` ×
head production rate (i.e. the gap strictly shrinks). Witness: OB-2 rate vs OB-4.
Check: CT-6 (S2).

**LIV-7 — Finality keep-up.** Precondition: healthy upstream with advancing finality.
Bound: `finalized(C)` trails the upstream finalized head by at most `P-SLO-FINALITY-LAG`
blocks in steady state — including during catch-up, so that eviction (T5) can keep the
window bound (GAP-2). Witness: OB-1 finalized gauge vs upstream. Check: CT-6 (S2),
CT-7.

**LIV-8 — Reorg convergence.** Precondition: healthy upstream that settles on one
branch. Bound: a depth-d reorganization (d < window above finality) converges — buffer
head on the new branch — within `P-SLO-REORG-CONVERGE(d)`; the fork/rebase cycle
cannot loop without progress (each rebase strictly lowers the base or the session
errors into the ladder; the empty-`prev` livelock of GAP-5 is excluded by WP-10).
Witness: OB-7 fork events + OB-2 resumption. Check: CT-1 (reorg histories), CT-4.

**LIV-9 — Shutdown.** Precondition: stop signal. Bound: exit within
`P-SHUTDOWN-GRACE` regardless of upstream/client state; a second signal exits
immediately (REQ-23). Witness: process exit + OB-9. Check: CT-2.

**LIV-10 — Backfill termination.** Precondition: healthy upstream. Bound: a
window-underflow request (RP-8) reaches a terminal outcome within LIV-3's bound; the
acquisition it spawns is abandoned within `P-DISCONNECT-REAP` of the response ending
for any reason. Witness: OB-5 backfill counters, resource gauges. Check: CT-8.

**LIV-11 — Eviction convergence.** Precondition: finality advancing again after a lag.
Bound: the commit that finalizes past the excess also evicts it (T5 runs in the same
atomic step, WP-24), so the buffer is back within the window bound at that commit and
over-window alarms clear with it. Witness: OB-1 stored gauge, OB-6. Check: CT-7.

**LIV-12 — No cross-client starvation.** Precondition: adequate resources. Bound: a
conforming client's poll loop achieves ≥ 1 block of progress per
`P-STARVATION-WINDOW` while any set of other clients (slow, greedy, disconnecting)
operates. Witness: per-request metrics OB-5. Check: CT-8 (S5/S6).
