# ADR-18 — Parallel stride acquisition stays below the finalized head

Status: Accepted

## Context

ADR-4 split acquisition into a strided range mode and an ordered head mode, and left
the boundary between them at the head of the stream's own commitment level. On the head
stream that meant strides ran up to the *latest* block, and two things followed.

A strided range is fetched as several concurrent requests, each addressing blocks by
number, each observing the chain at its own instant and possibly on its own node of a
fleet. Below the finalized head that is harmless — no reorg can change what those
numbers name. Above it, a reorg mid-range yields a set assembled from two branches.
Continuity checking catches it, so the outcome is a spurious fork signal and a rebase
cycle rather than corruption, but the exposure opens on every process start: T1 seeds
at the finalized head, so a fresh head stream begins exactly one block below it and the
distance to the latest block — the chain's finality distance — is wide enough to select
stride mode immediately.

The second consequence was GAP-2, HZ-3 realized. A range that may still reorg cannot
carry a finality report about itself, so the head stream's stride batches carried none
at all, and the consumer's watermark was left to the confirmation prober alone. The
prober is rate limited by construction (ADR-6), so during catch-up acquisition outran
finality, eviction starved, and the buffer grew by the whole gap.

## Decision

Strides are bounded by the upstream finalized head on every stream, whatever its
commitment level, and every stride batch carries that head as its finality report.
Blocks above it are acquired by the ordered paths — the speculative pipeline on the head
stream, polled strides on the finalized one — which deliver in order and hand finality
to the prober.

The finalized head is read from a short-lived observation refreshed in the background
(`P-FINALITY-VIEW-TTL`), never awaited on the ingestion path: with no observation yet
there is simply no stride range, and the ordered path carries the stream. This is what
keeps ADR-6's guarantee intact — a finality read must never delay a block.

Not awaiting it is not sufficient on its own. The refresh draws on the same upstream
budget as every block fetch, and that budget has no priority (ADR-3, HZ-1), so a call
issued ahead of the first fetch competes for the permit that fetch needs — the stall in
a second guise, and measurably so at a capacity of one. The read is therefore also
ordered: none goes out before a stream has delivered a block. Past that point the
contention is HZ-1's, unchanged by this decision and smaller than before it, since the
prober no longer resolves the head every round.

The observation is scoped to the epoch, and monotone within one — the same rule WP-12
applies to the reports it feeds. Both fetch paths run when it expires, so answers can
land in either order, and a fleet does not speak with one voice; taking the last would
let one lagging replica pull the stride bound and the probe filter back for a whole TTL.
T1's read replaces it outright, and a fetch still in flight from the epoch that ended is
discarded rather than stored. Upstream
finality may oscillate (FM-26) and a re-seed may legitimately open below the previous
epoch (ADR-14), so an observation that outlived one would bound strides over blocks
that can still reorg *and* report above the fresh buffer — finalizing and evicting what
upstream no longer calls final, and leaving the next reorg to land under INV-11 as
FM-30. Nothing else bounds the hint's age, and nothing else needs to: within an epoch
the consumer's watermark is a maximum (WP-12), so a stale-high report cannot move it
anywhere a report already applied has not.

## Consequences

Out-of-order acquisition is confined to a range that cannot change under it, so the
reorg exposure and the spurious-fork cycle it produced are gone, and the report a
stride batch carries is true of every block in it by construction (WP-11.6, LIV-7,
INV-4). Restores the boundary the predecessor draws.

The cost lands on providers that under-report finality: their stride window is narrower
than the truth, so a catch-up through blocks they wrongly call unfinalized runs on the
ordered path and is slower. That configuration is already degenerate — the buffer must
hold every block above the reported watermark, so an under-report wider than
`P-CACHE-SIZE` means a permanent over-window state under ADR-9 regardless of this
decision. `finality_confirmation` (REQ-11's depth offset from the head) is the standing
answer for such providers, and it repairs the stride window in the same move.

Rejected: keeping the bound at the commitment head and attaching the report only when
it sits above the batch. It preserves fast catch-up on under-reporting providers
without the offset, but it keeps out-of-order acquisition in reorg range on every
process start, and it splits the report rule into two cases where one suffices.

Shapes WP-11.6, DEF-20; bounds SLI-6/SLI-8 during S2; retires GAP-2.
