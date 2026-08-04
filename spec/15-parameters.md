# 15 — Parameter registry

**Mutable doc #2.** Every `P-*`/`W-*` symbol used anywhere in the suite has a row.
"Observed" is the current implementation's value (or behavior) as of 2026-08-04;
"Target" is the intended value — ⚠ marks a proposal awaiting ratification (ADR-13
covers the batch; individual ratifications link their ADR). `—` = no distinct target
(observed is intended). ⊥ = not implemented/measured.

## Window & retention

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-CACHE-SIZE | window size (DEF-24, WP-24) | 1000 (default; configurable integer ≥ 1) | — |
| P-BLOCKTIME-CACHE | ingest-time cache capacity (IB table) | 1000 | — |
| P-BLOCKTIME-TTL | ingest-time cache TTL | 30 min | — |
| P-MEM-PER-BLOCK | memory model coefficient (PF-1) | ⊥ unmeasured | ⚠ measure in CT-6 |
| P-MEM-BASE | memory model constant (PF-1) | ⊥ unmeasured | ⚠ measure in CT-6 |

## Ingestion & sessions

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-ENRICH-RETRIES | component re-acquisition attempts (WP-11.2) | 10 (head path); **violated: 0 on backfill path** (GAP-11) | 10 ⚠, both paths |
| P-ENRICH-DELAY | delay between re-acquisitions | 50 ms | — |
| P-STALL-FREE-RETRIES | stalled-session restarts before backoff (WP-9) | 1 | — |
| P-SESSION-BACKOFF | delay between stalled sessions | 30 s | — |
| P-STALL-REINIT | stalled sessions before T1 re-seed | 6 (code gate `stalled > 5`) | — |
| P-STALL-ALARM | zero-progress time before the stall alarm level (LIV-2); also the readiness staleness floor (RP-10) | ⊥ not implemented (GAP-4) | ⚠ 60 s |
| P-STRIDE-SIZE | acquisition range-batch size (IB-10) | 5 (default) | — |
| P-STRIDE-CONCURRENCY | concurrent range batches | 5 (default) | — |

## Upstream client

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-RPC-TIMEOUT | per-call timeout (IB-13) | 10 000 ms (default) | — |
| P-RPC-RETRY-ATTEMPTS | per-call retry cap (REQ-16) | 5 (fixed) | — |
| P-RPC-RETRY-SCHEDULE | retry pauses, indexed by attempt with the last entry repeated (IB-13) | 10, 100, 500, 2000, 10000, 20000 ms (the 6th entry is reachable only via per-call attempt overrides above the P-RPC-RETRY-ATTEMPTS default) | — |
| P-RATE-TOLERANCE | allowed budget overshoot (REQ-16 acceptance) | **violated: unbounded overshoot race** (GAP-21) | ⚠ 10 % over 1 s windows |
| P-DEBUG-TIMEOUT | debug-trace call timeout (IB-14) | 60 s (fixed) | — |

## Serving

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-PORT | listen port | 3000 (default) | — |
| P-REQ-BODY-MAX | stream request body cap (RP-2) | 1024 B | — |
| P-WAIT-BLOCK | above-head wait (RP-4) | 5 s | — |
| P-RESP-BUDGET | response production budget (RP-20) | 60 s | — |
| P-RESP-BUFFER | per-request buffered-unsent bound (RP-21) | 32 frames | — |
| P-FORK-REFS-MAX | max refs in a conflict (RP-7) | 101 — the window path emits an inclusive span of 101 refs, the head path up to 100; the bound is the larger, and the count within it is a free variable (13) | — |
| P-DISCONNECT-REAP | resource release after disconnect (RP-21) | ⊥ unmeasured | ⚠ 2 s |
| P-MAX-CONCURRENT-STREAMS | admission cap for concurrent stream requests (RP-23) | ⊥ unbounded today (GAP-28) | ⚠ 64 |
| P-WAKE-LATENCY | commit→waiter wakeup (LIV-4) | ⊥ unmeasured | ⚠ 100 ms |
| P-STARVATION-WINDOW | per-client progress window (LIV-12) | ⊥ unmeasured | ⚠ 10 × W-BLOCK-INTERVAL |

## Lifecycle

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-STARTUP-ACCEPT | start→accepting bound (LIV-5) | ⊥ unmeasured | ⚠ 5 s |
| P-SLO-STARTUP | start→ready bound before catch-up (REQ-13) | ⊥ unmeasured | ⚠ 30 s |
| P-SHUTDOWN-GRACE | stop→exit bound (REQ-23) | drain bounded by the ⚠ target in the binary (2026-08-02); < 2 s in the one existing test | ⚠ 5 s |

## SLO targets (11) — all ⚠ pending ADR-13

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-SLO-HEAD-LATENCY | SLI-1 bound | ⊥ unmeasured; worst conforming acquisition retry ≈ P-ENRICH-RETRIES × P-ENRICH-DELAY = 500 ms | ⚠ |
| P-SLO-COMMIT-LATENCY | SLI-2 bound | ⊥ | ⚠ |
| P-SLO-QUERY-OVERHEAD | SLI-3 overhead bound | ⊥ | ⚠ |
| P-SLO-THROUGHPUT | SLI-4 floor | ⊥ | ⚠ |
| P-SLO-CATCHUP-RATE | SLI-5 multiple of head rate | ⊥ | ⚠ |
| P-SLO-FINALITY-LAG | SLI-6 bound | **violated during catch-up** (GAP-2) | ⚠ |
| P-SLO-READY-AVAIL | SLI-7 floor | ⊥ | ⚠ |
| P-SLO-MEM-FACTOR | SLI-8 bound | **violated during catch-up** (GAP-2) | ⚠ |
| P-SLO-REORG-CONVERGE | LIV-8 bound (function of depth) | one fork signal + session per depth unit observed | ⚠ |
| P-PERF-NOISE | benchmark noise band (MG-5) | ⊥ | ⚠ |
| P-LOG-RATE-STEADY | log records per ingested block (REQ-31) | **violated: ≥ 1 status line per block, unthrottled** (GAP-23) | ⚠ 0.1 |

## Process gates (13)

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-COV-PROP | property-coverage ratchet: per-row rank and the count of rows at C, both upward-only (MG-1) | 0 rows C today | ⚠ ratchet from first Phase-0 measurement |
| P-COV-DIFF | changed-lines coverage (MG-2) | ⊥ no instrumentation (HC-11) | ⚠ 80 % |
| P-COV-TOTAL | repo coverage floor (MG-2) | ⊥ | ⚠ ratchet from first measurement |
| P-CI-PR-BUDGET | per-PR gate wall-clock (MG-4) | full suite ≈ minutes today | ⚠ 10 min |
| P-FLAKE-WINDOW | flake-quarantine window (13 §flake) | ⊥ | ⚠ 7 days |

## Workload descriptors (11)

| Parameter | Role | Observed | Target |
|---|---|---|---|
| W-BLOCK-INTERVAL | network cadence | per-network (250 ms – 12 s across deployed networks) | scenario input |
| W-BLOCK-SIZE | median compressed record size | per-network | scenario input |
| W-REORG-RATE / W-REORG-DEPTH | reorg profile | per-network | scenario input |
| W-FINALITY-LAG | network finality delay | per-network (up to minutes/epochs) | scenario input |
| W-CLIENTS | concurrent streaming clients | ⊥ | scenario input |
| W-CATCHUP-GAP | cold-start gap | ⊥ | scenario input |
| W-UPSTREAM-ERR | upstream transient error rate | ⊥ | scenario input |

## Committed baselines (provenance notes)

- **Compression** (predecessor benchmark, 1001 blocks ≈ 63 MB raw): store-encode
  cost ≈ 72 ms and ≈ 5.85 MB at the chosen at-rest setting vs ≈ 2583 ms / 8.91 MB at
  the strongest legacy alternative; fallback re-encode ≈ 387 ms per equivalent
  serve. Basis of ADR-7 and the SLI-2/SLI-4 baselines.
- **Acquisition retry budget**: P-ENRICH-RETRIES × P-ENRICH-DELAY = 500 ms — chosen
  to match the predecessor's backfill retry wall-clock (5 × 100 ms) at finer
  granularity (ADR-5).
- **Historical stall**: an unbounded silent retry once froze head ingestion for ~2
  days on a public testnet — the incident behind ADR-5's fail-loud bound and OB-7's
  alarm requirements.
