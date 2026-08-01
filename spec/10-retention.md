# 10 — Retention & the window

The buffer is a sliding window over the chain's newest blocks. This document fixes the
policy semantics, precedences, and bounds. Physical deletion is trivial here (memory
release with the buffer version, CN-9); the substance is *what may be evicted when*.

**RS-1 — Policy semantics.**

| Policy element | Semantics |
|---|---|
| `P-CACHE-SIZE` | target maximum buffered block count |
| autoAdjust = off | finality strictly dominates the bound: excess is retained + alarmed |
| autoAdjust = on | the bound dominates: finality is force-advanced to restore it |

**RS-2 — Finality dominates eviction.** [MUST] Only blocks strictly below
`finalized(C)` are evictable (WP-24, INV-11/INV-14). With autoAdjust off, this
precedence is absolute: the window bound yields (ADR-9). Availability floor: the
buffer always retains at least the finalized block itself — `|B| ≥ 1` and the
finalized head is always servable from the window.

**RS-3 — Excess bound.** [MUST] Define excess = `max(0, |B| − P-CACHE-SIZE)`.
With autoAdjust on: excess = 0 in every committed state (INV-4 — T5 restores the
bound within the same atomic step, WP-24).
With autoAdjust off: excess is bounded only by finality lag; the service MUST keep the
over-window alarm (OB-6) active for the entire excess period and expose the excess
magnitude (OB-1). Unbounded silent growth is non-conforming — the current
implementation's catch-up behavior violates this (GAP-2, HZ-3). The buffer's
contribution to resident memory is PF-1's window term,
`P-MEM-PER-BLOCK` × (`P-CACHE-SIZE` + excess); the full ceiling is PF-1's.

**RS-4 — autoAdjust (sanctioned finality override).** [MUST] When enabled and eviction
is blocked by lagging finality, the service advances `f` exactly far enough to restore
the bound, then evicts (WP-24). Each force-advance is alarmed (OB-6 variant) — it
declares potentially-unfinalized blocks irreversible, trading rollback safety for the
memory bound (ADR-9 records why this exists: chains that never report finality).
After a force-advance, INV-11 protects the advanced position like any other: a deeper
reorg then becomes FM-19/FM-30. This is the accepted risk, made loud.

**RS-5 — Reclamation invisibility.** [MUST] Eviction is invisible to in-flight reads
(snapshots own their data, CN-6, CN-9) and to clients generally: a request below the window
transparently switches to backfill (RP-8). No reader-visible artifact distinguishes
"evicted" from "never buffered".

**RS-6 — Residue convergence.** [MUST] Abandoned request-scoped resources (snapshots,
backfill acquisitions) converge to zero within `P-DISCONNECT-REAP` of their request
ending (LIV-10); nothing a dead request held pins buffer memory across versions
indefinitely.

**RS-7 — Eviction cost.** [SHOULD] Eviction is amortized O(evicted) per commit and
never blocks reads beyond the commit's atomic step (CN-2).

## Interactions

- **× finality**: RS-2/RS-4 are the two precedence modes; WP-12 arbitration feeds T4
  which gates T5.
- **× forks**: T3 truncation is not retention — it can remove any unfinalized suffix
  regardless of window pressure; retention only ever removes the oldest finalized
  prefix.
- **× queries**: below-window requests are the retention-visible seam (RP-8); the
  window size trades memory against backfill frequency (HZ-5).
- **× recovery**: restart empties the window entirely (CN-7); clients ride through via
  RP-8 backfill after re-seed.
- **× liveness**: LIV-7 (finality keep-up) is what keeps RS-3's excess bounded in
  practice; LIV-11 closes the loop after a lag.
