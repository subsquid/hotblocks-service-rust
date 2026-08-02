# ADR-17 — Readiness compares against the local upstream view, not a fresh probe

Status: Proposed

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

## Consequences

The readiness-probe hazard is retired from 11's register: probe frequency no longer
affects upstream load, and readiness cost
becomes independent of orchestrator configuration. SLI-7 measures the freshness of
the ingestion loop's own view rather than the adapter's probe latency.

Readiness staleness is now bounded by the head-following cadence instead of being
zero-by-construction. Under a healthy upstream the loop observes the head at least
once per block, so the view is fresher than a block time; under an unhealthy one the
`P-STALL-ALARM` floor flips readiness off before the stall alarm's own threshold
elapses, which is the behavior a router wants anyway.

This binds readiness to an observable the implementation does not expose yet: GAP-25
tracks the missing upstream-head view, so the change lands with OB-4's gauge or not
at all.

Rejected: caching the probe result with a short TTL. It keeps the upstream call, keeps
the failure mode, and adds a second staleness parameter to reason about.

Shapes RP-10, DEF-32, SLI-7, OB-4, RP-22; retires the readiness-probe hazard row.
