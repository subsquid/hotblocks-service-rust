# ADR-17 — Readiness compares against the local upstream view, not a fresh probe

Status: Accepted

## Context

RP-10 defines readiness as a comparison against the *current* upstream head, read
live from the adapter on every probe. Orchestrators probe readiness on their own
cadence, unaware of the chain's; every probe therefore spends a call from the single
upstream budget that ingestion and backfill share (ADR-3, RP-22). The hazard register
carried this as a standing risk, and it is one the service creates for itself: at 1 Hz
probing the readiness path can out-call the head path on a 12-second chain.

The service already tracks the upstream head — that is how it follows the chain —
and OB-4 requires exposing it together with the age of the observation. A probe that
re-asks upstream for a value the ingestion loop just fetched adds latency and load
without adding information.

## Decision

Readiness reports true iff the buffered head is at or above the **last observed**
upstream head (OB-4) and that observation is younger than `P-STALL-ALARM`. A stale
view reports not-ready, which is the same verdict the old probe produced when the
adapter failed — a probe never returns an internal error to a router.

Readiness becomes a pure local read: no upstream call, no share of the upstream
budget, no failure mode of its own.

The core owns the observation contract. It refreshes `DataSource::get_head` every
`P-STALL-ALARM / 2`, with each read bounded to one quarter of that period. The tail of
every committed source batch may advance the local head number between reads, but it
does not refresh the successful-read timestamp used for readiness. Adapter-specific
acquisition may publish more precise numbers, but readiness does not depend on that
optional integration. A failed refresh is logged and leaves the previous successful
read to age out normally, even while batches continue to arrive.

## Consequences

The readiness-probe hazard is retired from 11's register: probe frequency no longer
affects upstream load, and readiness cost
becomes independent of orchestrator configuration. SLI-7 measures the freshness of
the ingestion loop's own view rather than the adapter's probe latency.

Readiness staleness is now bounded by the head-following cadence and the core refresh
instead of being zero-by-construction. Under a healthy upstream the number moves with
committed source batches and its successful-read timestamp is refreshed before
`P-STALL-ALARM`; under an unhealthy one that timestamp ages out regardless of local
commit progress, which is the behavior a router wants anyway.

This binds readiness to the observable exposed since GAP-25 closed: the upstream head
view carries its last successful-read Prometheus timestamp plus typed monotonic times
for read freshness and numeric progress. GAP-34 closed with the probe's switch away
from a per-request upstream call on 2026-08-07.

Rejected: caching the probe result with a short TTL. It keeps the upstream call, keeps
the failure mode, and adds a second staleness parameter to reason about.

Shapes RP-10, DEF-32, SLI-7, OB-4, RP-22; retires the readiness-probe hazard row.
