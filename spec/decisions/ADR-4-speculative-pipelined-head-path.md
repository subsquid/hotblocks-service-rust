# ADR-4 — Speculative, pipelined head acquisition with cadence prediction

Status: Accepted (historical)

## Context

The TS head path fetches whole-block strides (header + all components in one round)
and truncates at the first not-ready block, polling on a fixed 100 ms interval with a
head check per round. Head latency therefore serializes: wait for block, then fetch
everything, then repeat. Component data (receipts/logs) typically becomes available
slightly after the header.

## Decision

At the head, poll the *next block's existence* with a single header fetch; on a hit,
acquire its components in a spawned task while immediately polling for the block
after (bounded in-order pipeline, depth 3, emission strictly ordered —
a stuck block holds emission rather than being skipped). Predict block cadence and
sleep until shortly before the predicted arrival, then poll on a tight interval —
because arrival jitter (propagation, load-balanced fleets) makes a single long sleep
risk missing an early block.

**Amended 2026-08-09.** The predictor was an EMA of inter-block intervals. It averaged
in every long interval and then over-predicted until it decayed, sleeping through
arrivals: on a chain skipping 2.5% of slots this cost 200–600 ms on each of the six
blocks after a gap, and jitter alone exceeded the hot window besides. It is now the
floor of the last N intervals, which cannot be dragged upward by a late arrival.
Under-prediction costs polls, over-prediction costs latency; the estimator now errs in
the cheap direction. Candidates — including the rejected ones — are scored by
`crates/latency-bench` (`cadence`) against arrival traces collected with
`trace-collect`.

## Consequences

Removes the serial stall from the hot path (G1, REQ-20, SLI-1); adds pipeline
machinery and the not-ready retry interplay of ADR-5. Poll cadence and pipeline
internals are explicitly unspecified for clients (02 §unspecified) — only the latency
outcome is contracted. Introduced the head/backfill mode boundary whose finality
side-effect became GAP-2.
