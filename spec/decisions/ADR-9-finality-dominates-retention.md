# ADR-9 — Finality dominates retention; auto-adjust is the explicit override

Status: Accepted (historical)

## Context

The buffer targets a fixed window size, but eviction of an unfinalized block would
break the rollback guarantee (a reorg could then need blocks the service no longer
holds, and the conflict protocol's anchor would be gone). On chains whose finality
signal stalls or doesn't exist, a strict "evict only finalized" rule grows the buffer
without bound. The predecessor faced exactly this ("chains that never publish
finalization") and introduced an opt-in force-advance.

## Decision

By default, finality strictly dominates the window bound: only finalized blocks are
evictable, and when finality lags, the buffer exceeds its target size with a standing
alarm rather than evicting unfinalized data. The `auto-adjust` option inverts the
precedence explicitly: when eviction is blocked, finality is force-advanced exactly
far enough to restore the bound, each advance alarmed — the operator consciously
trades rollback safety on stalled-finality chains for a hard memory bound.

## Consequences

Rollback safety is never silently sacrificed; memory excess is never silent either
(DEF-24, WP-24, INV-4, OB-6). With auto-adjust on, a reorg deeper than the forced
finality becomes an unrecoverable divergence (FM-19/FM-30) — accepted and loud.
LIV-7 (finality keep-up) is what keeps the default mode's excess bounded in practice;
its current violation during catch-up is GAP-2.
