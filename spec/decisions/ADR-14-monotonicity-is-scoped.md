# ADR-14 — Monotonicity is scoped to an epoch and an instance

Status: Accepted

## Context

Two monotonicity guarantees were written as if they were absolute, and neither can be
met as stated.

INV-12 says the finalized watermark never moves to a lower height. But T1 re-INIT
(WP-20) unconditionally seeds the buffer from upstream's finalized head, and FM-26
explicitly permits that value to oscillate — a load-balanced upstream pool with one
lagging node is enough. So the ladder's own self-healing reset can lower the
watermark, contradicting the invariant it is supposed to preserve.

INV-29 says two sequential reads by one client observe non-decreasing versions. FM-34
explicitly tolerates two instances behind one address and says clients "may see version
flapping". Nothing on the wire distinguishes the instances: the response carries a
finalized-head watermark but no version, epoch, or instance token, and ADR-1 pins the
wire to the predecessor's bytes. A client fanned across instances therefore violates
that monotonicity in normal operation.

Left unresolved, both clauses are untestable: a conformance run can produce a legal
trace that fails them.

## Decision

Scope the guarantees to the domain in which they hold, rather than strengthening the
system to meet the absolute reading.

**Epoch** — the buffer's lifetime between T1 INITs — is the unit for INV-12. Within an
epoch the watermark is monotone and every report is hash-validated. A re-INIT opens a
new epoch and may seed lower; the reset is alarmed (OB-7, WP-20).

Contradiction is still terminal, but only where it is decidable. The buffer is the
only state (WP-13), so the seed is compared against the buffer the re-INIT is about to
discard: a seed naming a height that buffer holds under a different hash is
unrecoverable divergence (FM-19 → FM-30). Below the buffer's first block, and after
any eviction, there is nothing left to compare against and the service MUST NOT claim
to detect it — an obligation over discarded state is not a guarantee.

**Instance** is the unit for INV-29, and it is additionally per epoch. Cross-instance
monotonicity is not offered; a client that needs it pins an instance.

## Consequences

The invariants become checkable: CT-2 can drive a re-INIT against an oscillating
upstream and assert alarm-not-violation, and CT-3's monotonicity checks bind to one
instance and one epoch. Clients relying on a global watermark ordering must handle
regression across a restart — the same handling REQ-1/2 already require after a
restart (INV-40), so no new client burden appears in practice.

Rejected: adding an epoch or version token to the response so the ordering becomes
globally decidable. It changes the wire, which ADR-1 and REQ-24 pin to the
predecessor's bytes for the duration of the migration; the differential oracle (HC-8)
would fail on every response. Revisit only under OQ-4, which owns wire divergences.

Rejected: keeping a monotonic floor across re-INIT (refuse to seed below the previous
epoch's finalized head). It converts an ordinary upstream oscillation — one lagging
node in a pool — into either a permanent stall or a terminal exit, trading a bounded,
alarmed watermark regression for an availability failure. The contradiction case
already gets the terminal treatment, which is where it belongs.

Shapes REQ-11, INV-12, INV-29, WP-20, FM-26, FM-34.
