# Release notes template

GitHub title is always `vX.Y.Z` — set with `--title "vX.Y.Z"`. The body holds
the prose headline.

## Single-change release (most common)

```markdown
## <Headline>

<1-2 sentence lead — what the operator or client sees now that they didn't
before. No implementation details.>

See [`spec/X.md`](https://github.com/subsquid/hotblocks-service-rust/blob/vNEW/spec/X.md) for the full contract. (#PR)

**Full Changelog**: https://github.com/subsquid/hotblocks-service-rust/compare/vPREV...vNEW
```

## Multi-change release

```markdown
## <Headline>

<1-2 sentence lead.>

- **Bold lede.** Short explanation of change 1.
- **Bold lede.** Short explanation of change 2.

See [`spec/X.md`](...) for full details. (#PR)

**Full Changelog**: ...
```

## Style rules

- **English, always.** Translate a Russian draft before it goes anywhere.
- **Headline is a concept, not a version.** "Faster head delivery",
  "Stricter trace validation". No emoji. No leading verb.
- **Lead with observable impact**, not internal mechanism. "Blocks reach
  clients sooner at the chain head" beats "moved sender recovery to rayon".
- **No deployment / ops instructions.** No flag recipes, no kubectl, no
  rollout order. If a release requires operator action, say that it does and
  point at the doc — don't repeat the recipe.
- **No CI / internal changes.** Skip workflow tweaks, clippy fixes,
  refactors. Release notes are for behavior someone outside observes.
- **No internal detail.** No upstream provider names or endpoints, no cluster
  or environment names, no CLI flag names or parameter values, no benchmark
  numbers, no description of what was broken in enough detail to reconstruct
  it. Generalize: a specific tracer bug → "trace data is now validated before
  it is served".
- **Never link `planning/`** — it is gitignored, so the link 404s publicly and
  the content is internal working notes. `spec/` and `README.md` are the only
  committed docs.
- **Doc + PR ref** as a single line at the end of the prose, before the
  compare link. Anchor on the tag (`/blob/vNEW/`) so the link doesn't rot
  when `master` evolves.
- **Compare link always last.** Resolve `vPREV` with
  `git merge-base vPREV vNEW` when the previous-tag-in-sort-order isn't the
  real parent — branches that forked off an earlier point break the naive sort.

## What to skip entirely

- Install / upgrade commands (`docker pull`, `kubectl apply`) — live in deploy docs.
- The full commit message — `git log` is the engineer's-eye view; notes are for the user's.
- A "Tests" section on patch releases unless the headline is about test coverage.
- Internal cluster names, account ids, environment labels, dashboards, ticket ids.
