# ADR-10 — Truncation is a normal response outcome, not an error

Status: Accepted (historical)

## Context

Stream responses can be large (a full window, or an arbitrarily long backfill).
Bounding response duration requires ending responses early; signaling an in-band
error after data has been sent would require a framing protocol with trailers or
error records, which the plain concatenated-frame format (ADR-7) deliberately does
not have. The client already must handle "response ended, resume from the last
block" for the success case.

## Decision

A response may end at any frame boundary for any reason — production budget, internal
limit, error after the first frame, disconnect — and the client treats every
frame-boundary end identically: the received prefix is valid, resume from the last
received block. There is no in-band error signaling after the first frame; server-side
causes are alarmed on the observability surface instead.

## Consequences

Client logic stays a single resume loop (REQ-1, RP-5, RP-12); the serving path needs
no trailer/framing protocol. Cost: a client cannot distinguish a healthy short
response from a server-side failure mid-stream — acceptable because the resume loop
self-heals, but it makes server-side truncation observability mandatory. OB-5's
counters split it by cause (budget, error, disconnect) since GAP-22 closed.
