# ADR-16 — Above-head finality obligations collapse to the running maximum

Status: Proposed

## Context

WP-12 arbitrates finality reports by keeping the session's running maximum. Reports
naming a height above the buffered head cannot be hash-checked when they arrive, so
the original text held *every* such report in a list with its own registry-bound cap,
validated each one when its named height was ingested, and made overflow a session
error. The reference model carried the same list.

The list buys one thing: a report at an intermediate height is hash-checked when its
block arrives, catching a forged watermark the maximum would not catch. It costs a
parameter, an overflow-as-session-error path, an equal-height contradiction scan over
the whole list, a clause in PF-1's memory ceiling, and roughly a third of the model's
finality logic — all for a case that only exists while upstream reports finality
faster than it delivers blocks.

The cost is not paid back, because WP-23 already finalizes the *entire* buffer
provisionally on an above-head report. Every block below the maximum is therefore
already declared irreversible without an individual hash check; validating the
intermediate reports afterwards does not undo that, it only reports the contradiction
slightly earlier than the maximum's own check would.

## Decision

Keep exactly one obligation per session: the running maximum `fin_max`. A report is a
contradiction iff it names `fin_max`'s height under a different hash. A new maximum
above the buffered head replaces the old one and is validated when its height is
ingested; a report below the current maximum is ignored (already WP-12). No list, no
cap parameter, no overflow path.

## Consequences

An adversarial upstream can no longer make the service hold state proportional to its
own report rate, so PF-1 loses a term and FM-26 loses a bound instead of gaining one —
the memory sink the list was capped against cannot form at all.

Detection of a forged intermediate report is deferred to the maximum's own validation:
a fleet that reports `(100, A)` then `(105, B)` while delivering a chain whose block
100 has hash `A'` is caught at 105, not at 100. Both are integrity violations on the
same session with the same handling (WP-5), so the observable difference is which
block number appears in the alarm.

Rejected: dropping the provisional whole-buffer finalization instead and validating
every report against a held block. It is the stricter rule, but it is what starves
eviction during catch-up (GAP-2) — finality would trail acquisition by construction.

Rejected: keeping the list with a smaller cap. The cap is not the complexity; the
list, its overflow verdict, and its cross-scan are.

Shapes WP-12, WP-23, INV-12, FM-26, PF-1; the reference model's `note_report` /
`settle_report` in 13; retires the pending-report cap from the registry in 15.
