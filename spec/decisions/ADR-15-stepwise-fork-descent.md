# ADR-15 — Fork rebase descends stepwise when no signalled ref is buffered

Status: Accepted (historical)

## Context

WP-10's rebase searches the unfinalized buffer suffix for a block whose ref appears
in the fork signal's `prev`. As originally written, finding none was terminal
divergence (FM-30). That reading makes routine reorganizations fatal: an adapter is
free to signal only the immediate parent of the diverging block (the EVM adapter
does exactly that — a single-ref `prev`), so any reorganization deeper than one
block names a ref the service has under a different hash, matches nothing, and the
text prescribed process death for an event LIV-8 requires the service to converge
through.

Both implementations (the predecessor and the port) never behaved that way: when the
signalled refs are exhausted below the scan point, the rebase falls back to the
newest buffered block strictly below the lowest signalled ref, and the next session
re-approaches the head from there. The parameter registry has recorded the resulting
convergence ("one fork signal + session per depth unit") all along — the load-bearing
mechanism was simply absent from the normative text and from the reference model,
which would have called a correctly converging service terminally divergent.

## Decision

Make the fallback normative (WP-10): if no signalled ref matches a buffered block at
or above `finalized(C)`, the new session base is the newest buffered block, still at
or above `finalized(C)`, strictly below the lowest signalled ref — **stepwise
descent**. Each fork signal then moves the base at least one block down, so repeated
signals either reach the true fork point (rebase, T6) or exhaust the unfinalized
suffix, at which point divergence is at or below finality and FM-30 applies.

The search boundary is pinned inclusive: the finalized block itself is a legal base
(divergence starts strictly below it), matching both the reference model's `B[f..]`
scan and the implementation.

## Consequences

Deep reorganizations converge in at most `depth` sessions (LIV-8's bound absorbs the
per-session cost); a signal-poor adapter costs sessions, not correctness. Termination
is preserved: descent is strictly downward and floored at `finalized(C)`, so the
fork/rebase cycle cannot loop without progress, and sub-finality divergence still
terminates per FM-30. The reference model implements the same rule, so a conforming
service no longer diffs against a model that would have killed it.

Rejected: requiring adapters to signal a ref the service is guaranteed to hold (e.g.
a full window of ancestors). It moves the burden onto every adapter for every chain
family, and the service cannot verify the guarantee anyway — a defensive fallback in
one place is strictly cheaper than a fragile obligation in N adapters.

Rejected: keeping the terminal reading and treating single-ref signals as malformed.
It contradicts both shipped implementations and turns the most common reorganization
shape into an outage.

Shapes WP-10, LIV-8, WP-25; the reference model's `fork_signal` in 13.
