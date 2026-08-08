# 11 — Performance

## SLI definitions

Black-box measurable, one line each.

- **SLI-1 — Head-to-served latency**: time from a block being retrievable upstream to
  a waiting client's request completing with it (measured by a probe client at the
  head).
- **SLI-2 — Commit latency**: time from upstream availability to the block appearing
  in watermark reads (server-side component of SLI-1).
- **SLI-3 — Query latency**: admission → first byte, and admission → terminal outcome,
  per resolution path (window / wait / backfill), p50/p99.
- **SLI-4 — Stream throughput**: decompressed payload bytes per second delivered to a
  single fast client on the window path.
- **SLI-5 — Catch-up rate**: blocks committed per second while > `W-CATCHUP-GAP`
  behind head.
- **SLI-6 — Finality lag**: buffered-finalized height behind upstream-finalized
  height.
- **SLI-7 — Readiness availability**: fraction of probe intervals reporting ready
  while the upstream is healthy and the service is caught up.
- **SLI-8 — Memory footprint**: peak resident memory as a multiple of
  `P-MEM-PER-BLOCK` × window occupancy.

## SLO table

All targets ⚠ provisional pending ADR-13 ratification; baselines are the only
committed numbers the corpus provides (compression benchmark and retry budgets — see
15 for values and provenance).

| SLI | Scenario | Target | Known baseline |
|---|---|---|---|
| SLI-1 | S1 steady | ≤ `P-SLO-HEAD-LATENCY` ⚠ | the incoherent case is bounded by `P-ENRICH-RETRIES × P-ENRICH-DELAY`; a legitimately lagging component (not-ready, GAP-40) is bounded by `P-NOT-READY-BUDGET` |
| SLI-2 | S1 | ≤ `P-SLO-COMMIT-LATENCY` ⚠ | per-block encode cost baseline (15 §compression) |
| SLI-3 | S1 / S4 | ≤ `P-SLO-QUERY-OVERHEAD` beyond data time ⚠ | a missing/incoherent historic block below the polled finalized head now terminates after ≤ ≈ 500 ms adapter delay plus RPC-client retries (ADR-19); unmeasured end-to-end |
| SLI-4 | S1 | ≥ `P-SLO-THROUGHPUT` ⚠ | stored-frame passthrough ≈ 0 re-encode cost; fallback encoding pays per-block re-encode (15 §compression) |
| SLI-5 | S2 cold start | ≥ `P-SLO-CATCHUP-RATE` × head rate ⚠ | — |
| SLI-6 | S1, S2 | ≤ `P-SLO-FINALITY-LAG` ⚠ | tracks upstream during S2 since GAP-2's closure; unmeasured under load |
| SLI-7 | S1 | ≥ `P-SLO-READY-AVAIL` ⚠ | — |
| SLI-8 | all | ≤ `P-SLO-MEM-FACTOR` × window ⚠ | bounded under S2 since GAP-2's closure; excess now tracks the upstream head-to-finality distance, which exceeds the window on chains whose finality lags it |

## Resource-bound requirements

**PF-1 — Memory ceiling from configuration.** [MUST] Peak resident memory is
derivable: `P-MEM-BASE` + `P-MEM-PER-BLOCK` × (window + INV-4 excess) +
`P-RESP-BUFFER` × `W-BLOCK-SIZE` × `P-MAX-CONCURRENT-STREAMS` (RP-23; `P-RESP-BUFFER`
counts whole record frames, so this term's ceiling is workload-dependent through the
chain's record sizes) + acquisition in-flight bounded
by pipeline and batch parameters. No unbounded internal queue exists on any path
(ingestion, finality tracking — the session holds one obligation, the maximum report
(WP-12) — serving).

**PF-2 — End-to-end backpressure.** [MUST] Every producer/consumer seam (adapter →
loop, loop → buffer, buffer → response, response → socket) is bounded; a stalled
consumer stalls its producer rather than queueing unboundedly.

**PF-3 — Admission overhead.** [MUST] Request admission (validation + resolution) is
O(log window) against the snapshot; it never scans payloads.

**PF-5 — Startup work scheduling.** [MUST] Catch-up acquisition must not starve
serving of already-committed blocks; readiness gating (LIV-5) is the mechanism —
serving capacity exists from acceptance, not from readiness.

## Workload model

| W-param | Meaning |
|---|---|
| `W-BLOCK-INTERVAL` | median inter-block time of the target network |
| `W-BLOCK-SIZE` | median canonical record size (compressed) |
| `W-REORG-RATE`, `W-REORG-DEPTH` | reorganization frequency and depth distribution |
| `W-FINALITY-LAG` | network finality delay in blocks |
| `W-CLIENTS` | concurrent streaming clients |
| `W-CATCHUP-GAP` | blocks behind head at cold start |
| `W-UPSTREAM-ERR` | upstream transient error rate |

Reference scenarios:

- **S1 steady** — head-following at `W-BLOCK-INTERVAL`, `W-CLIENTS` pollers, no
  faults.
- **S2 cold start / catch-up** — start `W-CATCHUP-GAP` behind; measures SLI-5/6/8
  (the scenario GAP-2 broke).
- **S3 conflict storm** — reorgs at elevated `W-REORG-RATE`/depth; clients recovering
  via RP-7 continuously.
- **S4 backfill storm** — many clients starting below the window simultaneously
  (shared-budget stress, RP-22).
- **S5 slow-reader storm** — `W-CLIENTS` readers at near-zero drain rate +
  disconnects (RP-21).
- **S6 noisy neighbor** — S1 head-following concurrent with S4 backfill on one
  upstream budget (HZ-1).

## Hazard register

Mechanism → threatened property → probe. (Dated defects live in 13's gap register;
these are standing risks.)

- **HZ-1 — Shared upstream budget without priority.** Backfill traffic can starve head
  acquisition (RP-22): head-following ought to win that contention, and no mechanism
  makes it. → LIV-1, SLI-1. Probe: S6 with budget saturation.
- **HZ-2 — Snapshot cost under the commit lock.** Snapshot-taking that scales with
  window size can convoy the writer and other readers. → LIV-3, SLI-3, INV-35.
  Probe: S5 with maximal window and high admission rate.
- **HZ-3 — Catch-up finality stall.** Acquisition modes that outpace finality
  tracking inflate the window (INV-4). → SLI-6, SLI-8, LIV-7. Probe: S2 with
  `W-FINALITY-LAG` ≫ window.
- **HZ-4 — Per-request re-encode cost.** The fallback content encoding pays a
  per-block re-encode per client; N clients multiply CPU. → SLI-3/4 under S4/S5.
  Probe: S4 all-fallback-encoding.
- **HZ-5 — Window-size / backfill-frequency trade.** Small windows push clients into
  RP-8 constantly, converting memory savings into upstream load. → REQ-16, HZ-1.
  Probe: S4 with minimal `P-CACHE-SIZE`.
- **HZ-6 — Unbounded request concurrency.** While GAP-28 is open (no admission cap,
  RP-23): aggregate snapshot + buffer cost scales with client count. → PF-1, INV-35.
  Probe: S5 connection flood.
- **HZ-7 — Retired 2026-08-07.** Rate admission and reservation are one state
  transition; deterministic queued-waiter and metered concurrent-call regressions
  close the overshoot race (GAP-21). Full S6 saturation remains part of CT-8, not a
  known limiter defect.
- **HZ-8 — Finality-probe amplification.** Finality confirmation that probes
  per-block can multiply upstream calls at high block rates. → REQ-16, SLI-6. Probe:
  S1 at `W-BLOCK-INTERVAL` ≤ 250 ms, count upstream calls per block.

## Benchmarking requirements

- **PF-10 — Baselines.** [MUST] Every SLI has a committed baseline measured on S1
  (and S2 for SLI-5/6/8) on pinned hardware/workload descriptors; regressions gate per
  MG-5 with noise band `P-PERF-NOISE`.
- **PF-11 — Saturation knee.** [MUST] Characterize client count and block rate at
  which SLI-3 p99 departs linearly (the knee); re-measure when the serving path
  changes.
- **PF-12 — Overload phases.** [MUST] Benchmarks include past-knee phases and verify
  degrade-not-collapse (LIV-3 holds; error taxonomy only, no zombie states).
