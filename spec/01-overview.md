# 01 — Overview

## What it is

The hotblocks service closes the gap between a chain's head and the durable archive.
It continuously acquires the newest blocks from an upstream node, holds a bounded window
of recent blocks in memory as a single well-formed chain, and serves them over a simple
request/response API as concatenated canonical block records. Clients poll it in a loop:
each response advances them by at least one block or tells them, via an explicit
conflict, that the chain they were following has been reorganized and where to roll back
to. Finality reported by the network bounds how deep a reorganization can reach.

**The hot path** the system exists for: a new canonical block appears at the chain head →
the service acquires and validates all requested data components for it → the block
becomes visible to queries as one atomic extension of the buffered chain → a waiting
client's request completes with that block, within `P-SLO-HEAD-LATENCY` of the block's
network arrival.

The service core is chain-agnostic. A chain family (EVM today; other VMs planned)
integrates by providing an **acquisition adapter** that implements the input contract of
[03-data-model.md](03-data-model.md) §Input events; the core state machine, query
contract, and HTTP surface do not change per chain (see ADR-2).

## Actors

| Actor | Role | Interface |
|---|---|---|
| Upstream node | Source of truth for blocks, head, and finality; typically a load-balanced fleet of chain nodes | acquisition adapter's upstream binding (14 §upstream) |
| Streaming client | Consumes the block stream, tracks its own chain copy, performs rollback on conflict (Subsquid portal, indexers) | HTTP API (14) |
| Monitoring client | Scrapes metrics, probes readiness/liveness | HTTP API (14) |
| Operator | Configures, deploys, restarts; owns the supported-network set | CLI/config surface (14), signals |
| Orchestrator | Restarts the process, routes traffic on readiness | liveness/readiness endpoints, exit codes |

## Design goals

- **G1 — Freshness.** Serve a new head block within a bounded latency of its network
  arrival. → REQ-10, REQ-20, SLI-1, LIV-1.
- **G2 — Fork-consistent resumability.** A client can always make progress or learn
  exactly where to roll back; it can never be silently fed blocks from two incompatible
  chains in one logical stream. → REQ-1, REQ-2, RP-5, RP-7, INV-20..27.
- **G3 — Correctness of served data.** Every served block is complete and internally
  coherent for the configured data selection; data is never silently dropped or emptied.
  → REQ-7, REQ-9, INV-25, INV-26, INV-28.
- **G4 — Bounded footprint.** Memory is a function of configuration, not of chain or
  client behavior. → REQ-12, INV-4, PF-1.
- **G5 — Self-healing operation.** Transient upstream trouble is retried within bounds;
  persistent trouble surfaces as an alarm and a restartable condition — never a silent
  stall and never a corrupted process. → REQ-13, REQ-22, LIV-2, FM-1, INV-41.
- **G6 — Multi-VM extensibility.** Adding a chain family touches only an acquisition
  adapter and its upstream binding, not the core or the client contract. → REQ-17, ADR-2.
- **G7 — Wire continuity.** During the migration period the service is a drop-in
  replacement for its predecessor implementation: same API bytes, same payload format,
  same configuration names. → REQ-24, ADR-1.

## Non-goals

- **NG1 — Durable storage.** The buffer is a window, not an archive; blocks older than
  the window are re-acquired from upstream on demand, and nothing survives a restart.
  Rationale: the durable archive is a separate system; duplicating it here buys nothing
  (ADR-9 fixes the retention precedence this implies).
- **NG2 — Chain-data interpretation.** The service does not filter, index, or interpret
  block contents; it serves whole canonical blocks. Selection is by data component only
  (REQ-8), never by content.
- **NG3 — Multi-chain multiplexing.** One process serves one chain/dataset. Serving
  several chains means several processes (open question OQ-5 in 13 tracks whether this
  changes).
- **NG4 — Trust-minimized verification by default.** Cryptographic re-verification of
  upstream data is opt-in per check (REQ-14); by default the upstream node is trusted
  for content, and only structural coherence is enforced (REQ-9).
- **NG5 — Subscription push.** Clients poll; the service does not push. Bounded waiting
  (RP-4) exists to make polling cheap, not to provide a push channel.

## Trust model

| Actor | Verified | Trusted | Must never be able to cause |
|---|---|---|---|
| Upstream node | structural coherence of each block's components (REQ-9); parent-hash linkage of the delivered sequence; optional cryptographic checks per REQ-14 | content of block data (unless a REQ-14 check covers it); head and finality reports (bounded by INV-11/INV-12 sanity rules) | process termination outside the sanctioned terminal exits of FM-30/FM-31 (FM-1, ADR-12), or a permanently unservable state (INV-41); violation of buffer well-formedness (INV-1..3); silent data loss (INV-28) |
| Streaming client | request syntax and bounds (RP-2) | nothing | ingestion stall or another client's starvation beyond declared coupling (INV-35); process termination; unbounded memory (INV-4) |
| Monitoring client | request syntax | nothing | anything beyond its own response |
| Operator | configuration validity at startup (REQ-32) | intent of accepted configuration | — (an accepted flag that does nothing is a defect: INV-36) |

## Lifecycle at a glance

Dataflow:

```
 upstream node ──▶ acquisition adapter ──▶ ingestion loop ──▶ chain buffer ──▶ query engine ──▶ clients
      ▲                (per-block           (batches,          (window,          (snapshot,
      │                 completeness,        transitions        finality,         wait, conflict,
      └── backfill ◀──  bounded retry)       T1–T6)             eviction)         backfill)
```

Entity lifecycle (one block): acquired → validated/coherent → appended (visible) →
finalized → evicted (still servable via backfill) — or truncated away by a
reorganization at any point before finalization.

Process lifecycle: start → seed buffer from upstream finalized head → accepting
(liveness up) → caught-up (readiness up, oscillates with head) → draining on shutdown
signal → exit; on unrecoverable divergence: terminal alarm, then non-zero exit
(FM-30, IB-11).
