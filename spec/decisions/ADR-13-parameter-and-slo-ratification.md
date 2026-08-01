# ADR-13 — Ratification of proposed parameter targets and SLOs

Status: Proposed

## Context

The parameter registry (15) carries targets marked ⚠: SLO bounds (head latency,
finality lag, memory factor, catch-up rate, availability), operational bounds
(stall alarm threshold, disconnect reap, startup/shutdown), and process gates
(coverage ratchets, CI budget, flake window). They were proposed by the spec author
from observed behavior, incident history, and the benchmark baselines — not yet
agreed by the owners. Several have no observed value at all until the Phase-4
benchmark harness (HC-12) exists.

## Decision

This ADR is the ratification vehicle: when the owners accept the ⚠ targets (as a
batch or per row), its status flips to Accepted, the registry drops the ⚠ marks and
links here. Rows that need measurement first are ratified after CT-6 baselining;
until then their gates (MG-5) run advisory. Any later change to a ratified target is
a new ADR superseding this one for that row.

## Consequences

Keeps proposed numbers clearly separated from agreed contract (no silent
normativity); gives MG-1/MG-2/MG-5 unambiguous thresholds once accepted. Until
acceptance, performance conformance is informative, not gating.
