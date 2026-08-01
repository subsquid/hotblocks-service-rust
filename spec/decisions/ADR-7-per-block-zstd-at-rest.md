# ADR-7 — Per-block zstd at rest, passthrough serving, gzip fallback

Status: Accepted (historical)

## Context

Blocks are stored compressed and served compressed. The predecessor benchmarked
gzip levels 1/3/6/9 against zstd 1/3/6/9 on a 1001-block real corpus (~63 MB raw):
zstd-1 encoded ~13× cheaper than gzip-9 with an ~18 % smaller result
(15 §baselines). Serving cost is zero when the stored frame can pass through
unchanged.

## Decision

Store each block as an independent zstd frame (level 1). Serve stored frames
verbatim to clients that accept zstd; for others, re-encode per block to gzip
(level 1) at serve time. Responses are concatenations of independent frames/members —
never one continuous stream — so truncation at any frame boundary leaves valid data
and clients decode incrementally.

## Consequences

Ingest CPU and cache size dominated by the cheap encoder (SLI-2/SLI-8 baselines);
zstd clients cost ~nothing to serve; gzip clients pay a per-block re-encode per
request (HZ-4). The per-frame independence property becomes load-bearing contract
(REQ-6, IB-2, RP-12 truncation semantics, ADR-10).
