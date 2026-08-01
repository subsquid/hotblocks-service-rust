# ADR-5 — Bounded whole-block re-acquisition retry, fail-loud on exhaustion

Status: Accepted (historical)

## Context

A block's components can be transiently incoherent at the head: receipts lag the
header, or a load-balanced fleet serves a header and receipts from different forks.
The original retry reused the already-fetched header on every attempt and was
unbounded — a persistent mismatch could never heal, and the loop spun silently. This
froze a public-testnet deployment's head for ~2 days behind a healthy-looking
process. Alternatives considered and rejected: keep the unbounded retry and rely on
the backfill-mode switch as a backstop (the TS head behavior); exponential backoff
(the not-ready window after a header appears is short, so a flat tight poll beats it
on tail latency); retrying components only (made a fork mismatch permanent).

## Decision

On incoherence, re-fetch the **whole block** — header and components — up to
`P-ENRICH-RETRIES` times at a flat `P-ENRICH-DELAY`, sized so the total budget
comfortably exceeds a provider's normal component lag; the first attempt may reuse
the speculative header (hot-path economy). On exhaustion, fail loud: return an error
that tears down the ingestion session (the ladder takes over), never emit the block
incomplete. Per-retry logging is at debug level (retries are routine at the head);
the *bound plus the session-level alarm*, not log level, carries the observability
duty.

## Consequences

Heals reorg/fleet mismatches once the canonical data arrives; converts silent stalls
into observable restarts (WP-11, LIV-2, OB-7). The same discipline is normative for
*every* acquisition mode — the current backfill path lacks it (GAP-11). Shapes
REQ-9, INV-28, FM-15/16/24, and the 15 §baselines retry-budget note.
