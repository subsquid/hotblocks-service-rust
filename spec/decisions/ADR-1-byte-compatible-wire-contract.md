# ADR-1 — Byte-compatible wire contract with the TypeScript predecessor

Status: Accepted (historical)

## Context

The service is a Rust reimplementation of `@subsquid/evm-data-service` (and its
internal dependencies `util-internal-data-service`, `evm-rpc`, `evm-normalization`,
`rpc-client`). Consumers — the portal, HotblocksDB, SDK clients — were built against
the TS service's exact HTTP surface, per-block JSON payload, and CLI flag names.
A migration that changed any of those would require coordinated client releases.
The recovered planning document states the goal: "byte-compatible HTTP API and data
format, same operational behavior".

## Decision

The HTTP API, the per-block JSON data format, and the CLI flag names/defaults are a
byte-compatible contract with the TS service: a client cannot tell which
implementation it is talking to. The TS source remains the authoritative spec for
behavior not stated elsewhere. Mechanisms: JSON key order preserved to match JS
object insertion order; serde field order matched to TS output; per-block independent
compression frames kept because clients rely on them. Deliberate divergences are
enumerated (welcome text fixed, `--http-retry-internal-server-errors` actually
honored) and anything else that differs is a defect.

## Consequences

Enables silent cutover and differential testing (the predecessor is the oracle —
HC-8, REQ-24). Freezes known predecessor warts (409-status conventions, substring
encoding negotiation, oversized-body status) until REQ-24 is sunset (OQ-4). The
temporary pinned differential runner retired GAP-18, GAP-19 and GAP-27; it is removed
with this compatibility requirement. Withdrawal keys are currently pinned to the
recorded geth emission order (`index, validatorIndex, address, amount`), not a
canonical order; because the predecessor passes those objects through, another
provider order needs its own fixture and may differ. Shapes REQ-7, REQ-24, IB-2..IB-9,
INV-25/26.
