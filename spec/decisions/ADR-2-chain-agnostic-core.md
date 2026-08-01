# ADR-2 — Chain-agnostic core with per-family acquisition adapters

Status: Accepted (historical)

## Context

The TS service is EVM-specific throughout. The product roadmap adds Solana and other
VMs. Duplicating the buffer/serving machinery per chain family would multiply every
future fix. The planning document names "architecture that generalizes to non-EVM
chains later" as a goal.

## Decision

Split the system into a chain-agnostic core (block buffer, ingestion state machine,
HTTP serving, metrics — knowing only heights, hashes, parent links, and opaque
payloads) and chain-family acquisition adapters behind a single trait-shaped contract
(`DataSource`: streams of linked block batches plus head/finality reads). A new VM
integrates by implementing the adapter contract; the core and client contract do not
change. Concretely: workspace crates `data-service-core` / `rpc-client` /
`evm-source` / `evm-data-service`.

## Consequences

REQ-17 and DEF-1/DEF-20/DEF-21 exist because of this decision; heights are opaque
ascending coordinates with explicit parent coordinates (accommodating families with
skipped heights). The core's conformance suite runs against a synthetic adapter
(HC-1), so most of the spec is testable without any real chain. Adapter authors
inherit precise obligations (WP-11). Open question OQ-1 tracks whether Solana's
commitment model fits DEF-23 as-is.
