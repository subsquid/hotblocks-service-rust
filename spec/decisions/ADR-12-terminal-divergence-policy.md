# ADR-12 — Terminal divergence: alarm, then exit non-zero

Status: Proposed

## Context

When the upstream's canonical chain diverges below the service's finalized head
(FM-19), no retry can heal it: the service's irreversibility promise (INV-11) and
the upstream's reality contradict. The predecessor exits the process (via an
unhandled rejection — exit 1), letting the orchestrator restart it into a fresh T1
seed; the current implementation logs an error and silently stops ingesting while
continuing to serve stale data, with only the readiness probe hinting (GAP-4's
worst case). Neither behavior was ever chosen deliberately. A third option — keep
serving the stale window indefinitely as a degraded read-only mode — was considered
and rejected: stale-serving without a hard signal misleads every downstream consumer.

## Decision

On terminal divergence the service: (1) raises the terminal alarm level on the
observability surface (OB-7), (2) completes in-flight responses, and (3) exits
non-zero. The orchestrator's restart produces a fresh seed at the *new* upstream
finality (REQ-13), which is the only correct recovery. The same policy applies to
any future condition proven unhealable by retry.

## Consequences

Divergence becomes loud, bounded, and self-recovering under any standard
orchestrator (FM-30, LIV-2). Cost: a restart-loop if the upstream itself is sick —
acceptable because the alarm level plus exit reason make the loop diagnosable, and
the alternative (silent zombie or silent stale-serving) converts an upstream problem
into a trust problem. Closes the policy half of GAP-4; shapes FM-30, IB-11.
