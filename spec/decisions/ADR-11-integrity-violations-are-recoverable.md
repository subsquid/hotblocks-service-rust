# ADR-11 — Integrity violations are recoverable session errors, never process corruption

Status: Proposed

## Context

The buffer's integrity guards (parent-hash mismatch at a buffered height, gapped
input, writes at or below finality, contradictory finality reports) fire on *input*
that contradicts committed state — conditions caused by upstream fleets, adapter
bugs, or reorg races, all external to the process. The predecessor treats them as
thrown errors: the ingestion session restarts through the standard ladder. The
current implementation treats them as programming-error assertions: the failure
poisons shared state, permanently killing ingestion *and* every subsequent request
while the process keeps answering its liveness probe (GAP-1's zombie mode). The
choice between "assert" and "recoverable error" was never made deliberately — it
fell out of the port.

## Decision

Integrity violations are classified as *environmental*, not programming errors: each
one is alarmed (OB-7), rejects its batch whole, tears down the session, and enters
the restart ladder (WP-5, WP-9) — exactly like any other session error. Panics and
assertions are reserved for genuine internal impossibilities, and even those must
not leave shared state unusable (INV-41): a defect that escapes containment ends the
process, never zombifies it.

## Consequences

The service survives adversarial or buggy upstreams (FM-10, REQ-22); the ladder plus
the T1 re-seed gives self-healing up to full buffer reset. Requires reworking the
current lock-and-assert scheme around the buffer. Closes GAP-1; shapes WP-5, WP-14,
INV-41, FM-32.
