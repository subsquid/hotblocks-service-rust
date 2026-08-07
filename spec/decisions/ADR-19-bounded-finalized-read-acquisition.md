# ADR-19 — Bounded acquisition below a polled finalized head

Status: Accepted

## Context

`below_query` serves a window underflow through `get_finalized_stream`. Its short and
tail ranges use finalized poll mode rather than concurrent strides. Before GAP-11 was
closed, a missing or incoherent block in that mode was truncated out of the batch and
polled forever at 100 ms intervals. A request waiting for its first batch never
responded; after an earlier prefix the response never ended.

The polled finalized head and the block body may come from different members of a
load-balanced upstream pool. A replica can therefore report finality that another
replica has not made available yet. Applying ADR-5's whole-block retry rule here changes
the client-facing historic-read tolerance: its budget was motivated by component lag at
the speculative head, while this path can be waiting for replica convergence below
finality.

## Decision

Once the polled finalized head is at or above a requested number, that number is a
finalized acquisition obligation. A missing or incoherent answer is re-acquired as a
whole block up to `P-ENRICH-RETRIES` times with `P-ENRICH-DELAY`, in addition to any
retries performed inside the RPC client. The existing budget is reused provisionally
instead of introducing an unmeasured query-only parameter; its target and the query SLO
remain ⚠ under ADR-13.

Exhaustion names the block and ends the stream. Before the first response frame,
`below_query` maps that failure to INTERNAL (HTTP 500). After a valid prefix has already
been emitted, the wire format cannot carry an in-band error: HTTP remains 200 and ends
at a frame boundary, which clients resume from per ADR-10. Closing GAP-11 widened that
path from continuity failures to bounded acquisition failures after a prefix; both now
raise OB-5's `error` truncation counter, which is the server-side alarm INV-27 owes
once in-band signalling is gone (GAP-22).

When the polled finalized head is still below the requested start, no acquisition
obligation exists. Finalized poll mode keeps waiting at its 100 ms cadence and does not
call the range helper with an empty number set. The helper asserts that non-empty input
precondition itself, so future call sites cannot silently turn an empty range into a
spin or a synthetic acquisition error.

That precondition is a hard internal assertion, not a recoverable stream error. If a
future call site violates it, unwinding ends the task polling the stream instead of
producing the HTTP 500 described above: before the first frame the client loses the
request without that response, and after a prefix it observes a truncated 200 that no
counter reaches, since the counting path unwound with it. The assertion is reserved for
a caller bug; ordinary missing or incoherent upstream data follows the named-error path.

Rejected: preserve indefinite polling below a reported finalized head. It tolerates an
arbitrarily lagging replica but recreates GAP-11's unobservable request hang. Also
rejected for now: a separate, longer historic-read budget. There is no measured value
to ratify; CT-6/CT-8 and ADR-13 are the place to split the parameter if the shared bound
proves too short.

## Consequences

Persistently bad historic data now has a bounded terminal outcome instead of holding a
request forever. A transiently lagging pool has at most roughly
`P-ENRICH-RETRIES × P-ENRICH-DELAY` (currently 500 ms, less the skipped first delay
after a short batch) of adapter delay, plus RPC-client retries, before the query fails
or truncates. This is an explicit SLI-3 behavior change, not only an ingestion change.

Shapes WP-11, RP-8, SLI-3, `P-SLO-QUERY-OVERHEAD`, GAP-11, and GAP-22.
