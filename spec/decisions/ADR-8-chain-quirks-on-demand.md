# ADR-8 — Per-network quirk handling is ported on demand, not wholesale

Status: Accepted (historical)

## Context

The predecessor accumulated years of per-network workarounds: Cronos/Ethermint
phantom transactions (with receipt recovery and bloom-leak tolerance, gated below a
block cutoff), Hedera duplicated receipts, Tac's receipt/tracer deviations, Polygon
Amoy state-sync classification, and others. Each is dead weight on networks that
never hit it, and porting all of them up front would have delayed the migration.
The planning document: "isolate in quirk modules; port only if the network is a
target — confirm with owner before spending time." The alternative — port everything
for parity regardless of deployment targets — was explicitly declined.

## Decision

Quirk handling is ported per network, when that network enters the supported set
(an operational decision, OQ-6). Until then, running an unported network through the
service is an explicit operational precondition violation, documented per network.
The predecessor's cutoff discipline is kept where applicable: quirk fixes activate
only in their known-bad block ranges so they cannot paper over new, unrelated
provider bugs.

## Consequences

Faster migration; smaller, auditable quirk surface. Risk: an unported network stalls
(coherence checks fail permanently) or, worse, mis-serves — REQ-15 confines the
obligation to the supported set and GAP-16 tracks the porting backlog with its
per-network fixture corpus. The supported set must be written down and enforced at
deployment, or this ADR silently becomes "quirks are handled nowhere".
