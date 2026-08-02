# ADR-3 — Single runtime, no worker threads, one shared upstream budget

Status: Accepted (historical)

## Context

The TS service offloads CPU-bound work (mapping, hashing, sender recovery,
compression) to worker threads — including a fresh worker, with its own RPC client
and rate-limit budget, per backfill request. That design protected Node's event loop
but amplified two production leaks (workers leaked on early stream termination and on
undetected client disconnects) and made N concurrent backfills consume N× the
configured upstream budget. Rust has no event loop to protect. A rayon pool was the
considered alternative for CPU work.

## Decision

Run IO on the async runtime and CPU-bound block processing via blocking-pool tasks;
no worker threads. Use **one** shared upstream RPC client for head-following and all
backfills, so a single configured rate limit bounds the service's total upstream
load. The planning document and README call the shared budget "intended".

## Consequences

The configured upstream budget is actually enforceable (REQ-16, IB-17); resource
teardown is by structured cancellation instead of a worker-close protocol. Trade-off
accepted: backfill and head-following now compete for one budget with no priority
mechanism — the head path can be starved by query-driven acquisition (HZ-1);
a priority lever remains future work. The predecessor's `active_workers` gauge became
meaningless (GAP-24).
