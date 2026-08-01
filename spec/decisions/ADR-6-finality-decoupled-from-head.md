# ADR-6 — Finality tracking decoupled from head ingestion

Status: Accepted (historical)

## Context

Finality confirmation requires upstream probes (re-fetching candidate blocks at the
finalized level). An earlier design emitted a confirmed finalized head as its own
batch through the same queue as head blocks; a slow probe round-trip therefore
delayed fresh-block delivery, measurably stalling ingestion during finalization
epochs (an incident tracked in the team's issue tracker; fixed in both
implementations).

## Decision

Probe finality in a background task, rate-limited (rounds of at most 5 probes spaced
at least 500 ms apart), never blocking block delivery; a confirmed finalized head is
attached opportunistically to the *next* head batch instead of being emitted alone.
Deferring finality by one batch is safe because finality trails the head by design
and the consumer applies reports through a monotone maximum (WP-12).

## Consequences

Head latency is independent of probe RTT (LIV-1; guarded by a timing regression
test). Finality advance becomes lazy — it can trail when head batches are sparse or
when acquisition modes don't carry reports, which is exactly the mechanism GAP-2
exploits during catch-up; LIV-7 exists to bound it. Shapes DEF-20's piggyback field,
WP-11.6, WP-12.
