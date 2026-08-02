#!/usr/bin/env python3
"""Standing quality gate for the spec suite in spec/.

Line-oriented, stdlib-only. Enforces the conventions the suite itself declares
(README §Conventions): stable banded IDs, one home doc per prefix, symbolic
parameters, two mutable docs, append-only ADRs, full traceability.

Usage: check-spec.py [SPEC_DIR] [--format text|json|github] [--severity E|W|I]
                     [--only CHECK] [--strict] [--no-ignore] [--list-checks]
                     [--base REF]   # arms the history checks (default:
                                    # SPEC_CHECK_BASE or origin/$GITHUB_BASE_REF)
Exit: 1 on surviving error-severity findings (or warnings with --strict),
      2 on bad usage, 0 otherwise.
"""

import json
import os
import re
import subprocess
import sys
from collections import defaultdict

# ---------------------------------------------------------------- configuration
# Home documents: prefix -> file that defines it (everything else references).
HOME = {
    "REQ": "02-requirements.md",
    "OQ": "02-requirements.md",
    "DEF": "03-data-model.md",
    "WP": "04-mutations.md",
    "RP": "05-queries.md",
    "INV": "07-invariants.md",
    "LIV": "08-liveness.md",
    "FM": "09-failure-model.md",
    "PF": "11-performance.md",
    "SLI": "11-performance.md",
    "HZ": "11-performance.md",
    "OB": "12-observability.md",
    "CT": "13-conformance-tdd.md",
    "MG": "13-conformance-tdd.md",
    "HC": "13-conformance-tdd.md",
    "GAP": "13-conformance-tdd.md",
    "IB": "14-interface-binding.md",
}
PARAM_HOME = "15-parameters.md"          # P-* and W-* registry
ADR_DIR = "decisions"
ADR_STATUSES = ("Proposed", "Accepted", "Superseded")
MUTABLE_DOCS = {"13-conformance-tdd.md", "15-parameters.md"}
# Matrix-tracked prefixes and the section of 13 that must contain their rows
# (MG-1 gates property coverage over exactly these).
MATRIX_PREFIXES = ("REQ", "INV", "LIV")
MATRIX_SECTION = "Traceability matrix"
GATE_SECTION = "Merge gates"
HC_SECTION = "Harness capability register"
CT_SECTION = "Test-class taxonomy"
GAP_SECTION = "Gap register"
# Non-HC enforcers a merge gate may legitimately name (process, not machinery).
GATE_ENFORCER_EXEMPT = ("review checklist", "existing CI", "CI")
# Docs where naming the concrete surface is allowed (impl-leakage skip).
LEAKAGE_EXEMPT = {"14-interface-binding.md"}
LEAKAGE_TERMS = [
    "tokio", "axum", "serde", "reqwest", "tungstenite", "hyper", "clap",
    "rayon", "TypeScript", "JavaScript", "Node.js", "worker thread",
    "RwLock", "mpsc", "cargo", "crates/", "src/", ".rs`", "V8",
]
# Sections whose prose legitimately holds concrete values / unrouted markers.
BARE_CONST_EXEMPT_HEADINGS = (
    "Explicitly unspecified", "Open questions", "Terminology cross-reference",
    "Committed baselines",
)
ID_RE = re.compile(r"\b([A-Z]{2,4})-(\d+)((?:/\d+)*)\b")
RANGE_RE = re.compile(r"\b([A-Z]{2,4})-(\d+)\.\.(?:([A-Z]{2,4})-)?(\d+)\b")
PARAM_RE = re.compile(r"\b([PW])-([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\b")
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)#]+)(#[^)]*)?\)")
BARE_CONST_RE = re.compile(
    r"(?<![\w`/.-])\d+(?:\.\d+)?\s?(?:ms|s\b|sec|seconds|minutes|min\b|h\b|"
    r"KiB|MiB|GiB|KB|MB|GB|%)"
)

CHECKS = {
    "id-undefined": "E", "id-duplicate": "E", "param-undefined": "E",
    "link-missing-file": "E", "link-missing-anchor": "E",
    "doc-unlisted": "E", "doc-missing": "E",
    "adr-undefined": "E", "adr-id-mismatch": "E", "adr-bad-status": "E",
    "trace-missing": "E", "trace-unknown": "E", "inv-check-unknown": "E",
    "gate-threshold-literal": "E", "gate-no-capability": "E",
    "gate-unknown-class": "E",
    "req-missing-tag": "E", "req-missing-acceptance": "E",
    "inv-missing-scope": "E", "inv-missing-check": "E",
    "id-orphan": "W", "param-unused": "W", "hc-orphan": "W",
    "ct-unbuildable": "W", "inv-check-matrix-mismatch": "W",
    "param-bare-constant": "W", "impl-leakage": "W", "heading-duplicate": "W",
    "warn-unratified": "W", "stale-matrix": "W",
    "ct-ungated": "I",
    "matrix-bad-status": "E", "matrix-bad-ct": "E", "matrix-duplicate-row": "E",
    # History checks (need --base / GITHUB_BASE_REF; skipped without one —
    # but an explicitly requested base that cannot be resolved is exit 2):
    "id-removed": "E", "adr-append-only": "E", "matrix-regression": "E",
}

findings = []


def emit(check, path, line, msg):
    findings.append({"check": check, "severity": CHECKS[check],
                     "file": path, "line": line, "message": msg})


def strip_fences(lines):
    """Yield (lineno, text) outside fenced code blocks."""
    fenced = False
    for i, ln in enumerate(lines, 1):
        if ln.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if not fenced:
            yield i, ln


def heading_of(lines, idx):
    for j in range(idx - 1, -1, -1):
        if lines[j].startswith("#"):
            return lines[j].lstrip("#").strip()
    return ""


def expand_ids(text):
    """All (prefix, num, hard) references in a text fragment.

    Slash lists (INV-20/21) -> hard refs; ranges (CT-2..CT-9, REQ-1..6):
    endpoints hard, interior soft (satisfy orphan, never raise undefined).
    'Bands:' declarations describe the numbering convention, not references."""
    if text.lstrip().startswith("Bands:"):
        return []
    out = []
    for m in RANGE_RE.finditer(text):
        pfx, lo, pfx2, hi = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        if pfx2 and pfx2 != pfx:
            continue
        out.append((pfx, lo, True))
        out.append((pfx, hi, True))
        for n in range(lo + 1, hi):
            out.append((pfx, n, False))
    stripped = RANGE_RE.sub(" ", text)
    for m in ID_RE.finditer(stripped):
        pfx, first, extra = m.group(1), int(m.group(2)), m.group(3)
        out.append((pfx, first, True))
        for part in filter(None, extra.split("/")):
            out.append((pfx, int(part), True))
    return out


def scan_defs(docs):
    """Line-anchored ID definitions in their home docs (strong/heading/row)."""
    strong = re.compile(r"\*\*([A-Z]{2,4})-(\d+)(?:\s+—[^*]*)?\.?\*\*")
    head = re.compile(r"^#{1,6}\s+([A-Z]{2,4})-(\d+)\s+—")
    row = re.compile(r"^\|\s*([A-Z]{2,4})-(\d+)\s*\|")
    out = set()
    for path, lines in docs.items():
        for _i, ln in strip_fences(lines):
            for rx in (strong, head, row):
                m = rx.search(ln) if rx is strong else rx.match(ln)
                if m and HOME.get(m.group(1)) == path:
                    out.add((m.group(1), int(m.group(2))))
    return out


def parse_matrix_ranks(lines):
    """Property -> coverage rank (U=0, P=1, C=2) from the traceability matrix."""
    ranks, insec = {}, False
    for ln in lines:
        if ln.startswith("#"):
            insec = MATRIX_SECTION.lower() in ln.lower()
            continue
        if insec and ln.startswith("|") and "---" not in ln:
            cells = [c.strip() for c in ln.strip("|").split("|")]
            if len(cells) < 3 or cells[0] in ("Property", ""):
                continue
            status = cells[2].replace("!", "").strip()[:1]
            rank = {"U": 0, "P": 1, "C": 2}.get(status)
            if rank is None:
                continue
            for pfx, num, _hard in expand_ids(cells[0]):
                if pfx in MATRIX_PREFIXES:
                    ranks[(pfx, num)] = max(ranks.get((pfx, num), 0), rank)
    return ranks


def git_base_docs(spec_dir, base):
    """{relpath: lines} of the spec tree at `base`, or None if unresolvable."""
    def git(*args):
        r = subprocess.run(["git", *args], capture_output=True, text=True, cwd=spec_dir)
        return r.stdout if r.returncode == 0 else None
    if git("rev-parse", "--verify", base + "^{commit}") is None:
        return None
    prefix = (git("rev-parse", "--show-prefix") or "").strip()
    # --full-tree: paths (and the pathspec) are repo-root-relative regardless
    # of the cwd, which sits inside the spec dir itself.
    listing = git("ls-tree", "-r", "--name-only", "--full-tree", base,
                  "--", prefix or ".")
    if listing is None:
        return None
    docs = {}
    for full in listing.splitlines():
        if not full.endswith(".md"):
            continue
        rel = full[len(prefix):] if prefix and full.startswith(prefix) else full
        content = git("show", f"{base}:{full}")
        if content is not None:
            docs[rel] = content.splitlines()
    return docs


def run_history_checks(spec_dir, base, docs, defined):
    base_docs = git_base_docs(spec_dir, base)
    if base_docs is None:
        return  # no base to compare against — checks are skipped, not failed
    # id-removed: IDs are never renumbered or deleted, only retired in place.
    current = {(p, n) for (p, n) in defined}
    for pfx, num in sorted(scan_defs(base_docs) - current):
        emit("id-removed", HOME[pfx], 1,
             f"{pfx}-{num} was defined at {base} but is gone — IDs are stable; "
             "retire in place, never renumber or delete")
    # adr-append-only: an accepted ADR changes only Status -> Superseded.
    for rel, base_lines in base_docs.items():
        if not rel.startswith(ADR_DIR + os.sep):
            continue
        cur_lines = docs.get(rel)
        if cur_lines is None:
            emit("adr-append-only", rel, 1,
                 f"ADR file present at {base} was deleted — the log is append-only")
            continue
        b_status = next((l for l in base_lines if l.startswith("Status:")), "")
        c_status = next((l for l in cur_lines if l.startswith("Status:")), "")
        if not b_status.startswith("Status: Accepted"):
            continue  # proposals may be edited freely until accepted
        b_rest = [l for l in base_lines if not l.startswith("Status:")]
        c_rest = [l for l in cur_lines if not l.startswith("Status:")]
        if b_rest != c_rest:
            emit("adr-append-only", rel, 1,
                 "accepted ADR body edited — supersede with a new ADR instead")
        elif b_status != c_status and "Superseded" not in c_status:
            emit("adr-append-only", rel, 1,
                 "accepted ADR status may only change to 'Superseded by ADR-n'")
    # matrix-regression: no row's rank decreases and the C count never shrinks
    # (MG-1 ratchet, per-row and in aggregate — a row that vanishes or loses its
    # status cell is caught by trace-missing / matrix-bad-status).
    base_13 = base_docs.get("13-conformance-tdd.md")
    cur_13 = docs.get("13-conformance-tdd.md")
    if base_13 and cur_13:
        b_ranks = parse_matrix_ranks(base_13)
        c_ranks = parse_matrix_ranks(cur_13)
        names = {0: "U", 1: "P", 2: "C"}
        for key, b in sorted(b_ranks.items()):
            c = c_ranks.get(key)
            if c is not None and c < b:
                emit("matrix-regression", "13-conformance-tdd.md", 1,
                     f"{key[0]}-{key[1]} coverage regressed {names[b]} -> {names[c]} "
                     f"vs {base} — the matrix ratchets upward only (MG-1)")
        b_covered = sum(1 for r in b_ranks.values() if r == 2)
        c_covered = sum(1 for r in c_ranks.values() if r == 2)
        if c_covered < b_covered:
            emit("matrix-regression", "13-conformance-tdd.md", 1,
                 f"{b_covered} properties were at C at {base}, {c_covered} now — "
                 "MG-1 ratchets the covered count upward only")


def main():
    args = sys.argv[1:]
    spec_dir, fmt, min_sev, only, strict, use_ignore = None, "text", None, None, False, True
    base_ref = os.environ.get("SPEC_CHECK_BASE") or (
        f"origin/{os.environ['GITHUB_BASE_REF']}"
        if os.environ.get("GITHUB_BASE_REF") else None)
    it = iter(args)
    for a in it:
        if a == "--base":
            base_ref = next(it, None)
            if not base_ref:
                print("--base requires a ref", file=sys.stderr)
                return 2
        elif a == "--format":
            fmt = next(it, "text")
        elif a == "--severity":
            min_sev = next(it, None)
        elif a == "--only":
            only = next(it, None)
        elif a == "--strict":
            strict = True
        elif a == "--no-ignore":
            use_ignore = False
        elif a == "--list-checks":
            for c, s in sorted(CHECKS.items()):
                print(f"{s}  {c}")
            return 0
        elif a.startswith("-"):
            print(f"unknown option {a}", file=sys.stderr)
            return 2
        else:
            spec_dir = a
    spec_dir = spec_dir or "spec"
    if fmt not in ("text", "json", "github") or (only and only not in CHECKS):
        print("bad usage", file=sys.stderr)
        return 2
    if not os.path.isdir(spec_dir):
        print(f"not a directory: {spec_dir}", file=sys.stderr)
        return 2
    if base_ref is not None:
        # Fail closed: a comparison that was asked for and cannot run must
        # not look like a pass.
        probe = subprocess.run(
            ["git", "rev-parse", "--verify", base_ref + "^{commit}"],
            capture_output=True, text=True, cwd=spec_dir)
        if probe.returncode != 0:
            print(f"--base {base_ref} does not resolve to a commit; "
                  "history checks cannot run", file=sys.stderr)
            return 2

    docs = {}   # relpath -> list of lines
    for root, _dirs, files in os.walk(spec_dir):
        for f in sorted(files):
            if f.endswith(".md"):
                rel = os.path.relpath(os.path.join(root, f), spec_dir)
                with open(os.path.join(root, f), encoding="utf-8") as fh:
                    docs[rel] = fh.read().splitlines()

    # ---- definitions -------------------------------------------------------
    defined = {}          # (pfx, num) -> (file, line)
    strong_def = re.compile(r"\*\*([A-Z]{2,4})-(\d+)(?:\s+—[^*]*)?\.?\*\*")
    head_def = re.compile(r"^#{1,6}\s+([A-Z]{2,4})-(\d+)\s+—")
    row_def = re.compile(r"^\|\s*([A-Z]{2,4})-(\d+)\s*\|")
    for path, lines in docs.items():
        for i, ln in strip_fences(lines):
            for m in strong_def.finditer(ln):
                pfx, num = m.group(1), int(m.group(2))
                if HOME.get(pfx) == path:
                    key = (pfx, num)
                    if key in defined:
                        emit("id-duplicate", path, i,
                             f"{pfx}-{num} already defined at "
                             f"{defined[key][0]}:{defined[key][1]} — one ID, one meaning")
                    else:
                        defined[key] = (path, i)
            m = head_def.match(ln)
            if m and HOME.get(m.group(1)) == path:
                defined.setdefault((m.group(1), int(m.group(2))), (path, i))
            m = row_def.match(ln)
            if m and HOME.get(m.group(1)) == path:
                defined.setdefault((m.group(1), int(m.group(2))), (path, i))

    # ADR definitions from decisions/ filenames + headings
    adrs = {}
    for path, lines in docs.items():
        if not path.startswith(ADR_DIR + os.sep):
            continue
        fn = os.path.basename(path)
        m = re.match(r"ADR-(\d+)-", fn)
        if not m:
            emit("adr-id-mismatch", path, 1, "filename does not start with ADR-<n>-")
            continue
        n = int(m.group(1))
        adrs[n] = path
        h = next((ln for ln in lines if ln.startswith("#")), "")
        hm = re.match(r"#\s+ADR-(\d+)\s+—", h)
        if not hm or int(hm.group(1)) != n:
            emit("adr-id-mismatch", path, 1,
                 f"heading disagrees with filename number {n}")
        st = next((ln for ln in lines if ln.startswith("Status:")), None)
        if not st or not any(s in st for s in ADR_STATUSES):
            emit("adr-bad-status", path, 1,
                 f"Status line missing or not one of {ADR_STATUSES}")

    # ---- references --------------------------------------------------------
    refs = defaultdict(list)      # (pfx, num) -> [(file, line, hard)]
    for path, lines in docs.items():
        in_bands = False
        for i, raw in enumerate(lines, 1):   # fenced blocks included: pseudocode cites IDs
            if raw.lstrip().startswith("Bands:"):
                in_bands = True
                continue
            if in_bands:                     # band declarations wrap; skip the paragraph
                if raw.strip():
                    continue
                in_bands = False
            for pfx, num, hard in expand_ids(raw):
                if pfx in HOME or pfx == "ADR":
                    refs[(pfx, num)].append((path, i, hard))

    for (pfx, num), sites in sorted(refs.items()):
        if pfx == "ADR":
            if num not in adrs and any(h for _, _, h in sites):
                f0, l0, _ = sites[0]
                emit("adr-undefined", f0, l0,
                     f"ADR-{num} referenced but no decisions/ADR-{num}-*.md exists")
            continue
        if (pfx, num) not in defined and any(h for _, _, h in sites):
            f0, l0, _ = next(s for s in sites if s[2])
            emit("id-undefined", f0, l0,
                 f"{pfx}-{num} referenced but not defined in {HOME[pfx]}")

    for (pfx, num), (dpath, dline) in sorted(defined.items()):
        others = [s for s in refs.get((pfx, num), [])
                  if not (s[0] == dpath and s[1] == dline)]
        if not others:
            emit("id-orphan", dpath, dline,
                 f"{pfx}-{num} is defined but nothing references it — "
                 "cite it where it applies or retire it explicitly")

    # ---- parameters --------------------------------------------------------
    registry, param_use = {}, defaultdict(list)
    for path, lines in docs.items():
        if path == "README.md":      # conventions doc: P-NAME is the naming scheme
            continue
        for i, ln in strip_fences(lines):
            for m in PARAM_RE.finditer(ln):
                sym = f"{m.group(1)}-{m.group(2)}"
                if path == PARAM_HOME and ln.lstrip().startswith("|"):
                    registry.setdefault(sym, (path, i))
                param_use[sym].append((path, i))
    for sym, sites in sorted(param_use.items()):
        ext = [s for s in sites if s[0] != PARAM_HOME]
        if sym not in registry and ext:
            emit("param-undefined", ext[0][0], ext[0][1],
                 f"{sym} used but has no row in {PARAM_HOME}")
        if sym in registry and not ext and not sym.startswith("W-"):
            p, l = registry[sym]
            emit("param-unused", p, l,
                 f"{sym} has a registry row but is cited nowhere else")

    # ---- links & document map ---------------------------------------------
    slugs = {}
    for path, lines in docs.items():
        seen = defaultdict(int)
        out = set()
        for i, ln in strip_fences(lines):
            if ln.startswith("#"):
                t = ln.lstrip("#").strip()
                slug = re.sub(r"[^\w\- ]", "", t.lower()).strip().replace(" ", "-")
                seen[slug] += 1
                if seen[slug] > 1:
                    emit("heading-duplicate", path, i,
                         f"duplicate heading slug '{slug}' makes anchors ambiguous")
                out.add(slug)
        slugs[path] = out
    for path, lines in docs.items():
        base = os.path.dirname(path)
        for i, ln in strip_fences(lines):
            for m in LINK_RE.finditer(ln):
                target, frag = m.group(1), m.group(2)
                if "://" in target or target.startswith("mailto:"):
                    continue
                rel = os.path.normpath(os.path.join(base, target))
                if rel not in docs and not os.path.exists(os.path.join(spec_dir, rel)):
                    emit("link-missing-file", path, i, f"broken link: {target}")
                elif frag and rel in docs:
                    if frag[1:].lower() not in slugs[rel]:
                        emit("link-missing-anchor", path, i,
                             f"anchor {frag} not found in {rel}")

    readme = docs.get("README.md", [])
    mapped = set()
    for i, ln in strip_fences(readme):
        for m in LINK_RE.finditer(ln):
            mapped.add(os.path.normpath(m.group(1)))
    for i, ln in strip_fences(readme):
        if "decisions/" in ln:
            mapped.add(ADR_DIR)
    for path in docs:
        if path == "README.md" or path.startswith(ADR_DIR + os.sep):
            if path != "README.md" and ADR_DIR not in mapped:
                emit("doc-unlisted", "README.md", 1,
                     "decisions/ folder not mentioned in the document map")
            continue
        if path not in mapped:
            emit("doc-unlisted", "README.md", 1,
                 f"{path} exists but is not in the README document map")
    for target in mapped:
        if target != ADR_DIR and target not in docs:
            emit("doc-missing", "README.md", 1,
                 f"document map lists {target} but the file does not exist")

    # ---- normative shape ---------------------------------------------------
    req_doc = docs.get(HOME["REQ"], [])
    req_lines = {n: l for (p, n), (f, l) in defined.items()
                 if p == "REQ" and f == HOME["REQ"]}
    tag_re = re.compile(r"\[(MUST|SHOULD|MAY)[^\]]*\]")
    def entry_block(doc, l):
        """Lines of one entry: from its definition to the next strong def/heading."""
        out = [doc[l - 1]]
        for x in doc[l:]:
            if strong_def.match(x.strip()) or x.startswith("#"):
                break
            out.append(x)
        return out
    for n, l in sorted(req_lines.items()):
        block = entry_block(req_doc, l)
        if not any(tag_re.search(x) for x in block[:2]):
            emit("req-missing-tag", HOME["REQ"], l,
                 f"REQ-{n} lacks an RFC 2119 tag ([MUST]/[SHOULD]/[MAY])")
        if not any(x.startswith("*Acceptance:*") for x in block):
            emit("req-missing-acceptance", HOME["REQ"], l,
                 f"REQ-{n} lacks an *Acceptance:* line")

    inv_doc = docs.get(HOME["INV"], [])
    inv_lines = {n: l for (p, n), (f, l) in defined.items()
                 if p == "INV" and f == HOME["INV"]}
    inv_check_cts = {}
    for n, l in sorted(inv_lines.items()):
        block = entry_block(inv_doc, l)
        if not re.search(r"\[(state|transition|response|recovery)\]", block[0]):
            emit("inv-missing-scope", HOME["INV"], l,
                 f"INV-{n} lacks a scope tag [state]/[transition]/[response]/[recovery]")
        chk = next((x for x in block if x.startswith("*Check:*")), None)
        if not chk:
            emit("inv-missing-check", HOME["INV"], l,
                 f"INV-{n} lacks a *Check:* strategy line")
        else:
            cts = {f"CT-{num}" for pfx, num, _ in expand_ids(chk) if pfx == "CT"}
            inv_check_cts[n] = cts
            for ct in sorted(cts):
                num = int(ct.split("-")[1])
                if ("CT", num) not in defined:
                    emit("inv-check-unknown", HOME["INV"], l,
                         f"INV-{n} Check: names {ct}, not in the taxonomy")

    # ---- 13: matrix, gates, capabilities -----------------------------------
    conf = docs.get("13-conformance-tdd.md", [])

    def section_rows(title):
        rows, insec = [], False
        for i, ln in enumerate(conf, 1):
            if ln.startswith("#"):
                insec = title.lower() in ln.lower()
                continue
            if insec and ln.startswith("|") and "---" not in ln:
                cells = [c.strip() for c in ln.strip("|").split("|")]
                rows.append((i, cells))
        return rows

    matrix_ids = {}
    status_re = re.compile(r"^[CPU](\s*!)?$")
    for i, cells in section_rows(MATRIX_SECTION):
        if not cells or cells[0] in ("Property", ""):
            continue
        ids = expand_ids(cells[0])
        # A row short of a Status cell must not pass as coverage: it satisfies
        # trace-missing while stating nothing (fail-open).
        if ids and not status_re.match(cells[2].strip() if len(cells) >= 3 else ""):
            emit("matrix-bad-status", "13-conformance-tdd.md", i,
                 f"matrix row '{cells[0]}' states no status in the closed set "
                 "C / P / U (optionally suffixed '!') — columns are "
                 "Property | CT | Status | Note")
        ct_cell = cells[1] if len(cells) > 1 else ""
        cts = [(n, hard) for p, n, hard in expand_ids(ct_cell) if p == "CT"]
        if ids and not cts:
            emit("matrix-bad-ct", "13-conformance-tdd.md", i,
                 f"matrix row '{cells[0]}' names no CT class in '{ct_cell}' — "
                 "a status nothing produces")
        for n, hard in cts:
            if hard and ("CT", n) not in defined:
                emit("matrix-bad-ct", "13-conformance-tdd.md", i,
                     f"matrix row '{cells[0]}' names CT-{n}, not in the taxonomy")
        for pfx, num, _hard in ids:
            if (pfx, num) in matrix_ids and matrix_ids[(pfx, num)][0] != i:
                emit("matrix-duplicate-row", "13-conformance-tdd.md", i,
                     f"{pfx}-{num} already has a row at line "
                     f"{matrix_ids[(pfx, num)][0]} — two rows can disagree and "
                     "the ratchet reads only the best of them")
            matrix_ids[(pfx, num)] = (i, cells)
        for pfx, num, hard in ids:
            if hard and (pfx, num) not in defined:
                emit("trace-unknown", "13-conformance-tdd.md", i,
                     f"matrix row names {pfx}-{num}, which is not defined")
    for (pfx, num) in sorted(defined):
        if pfx in MATRIX_PREFIXES and (pfx, num) not in matrix_ids:
            emit("trace-missing", "13-conformance-tdd.md", 1,
                 f"{pfx}-{num} has no row in the {MATRIX_SECTION} of 13 — "
                 "a property nobody decided how to test")
    for n, cts in sorted(inv_check_cts.items()):
        row = matrix_ids.get(("INV", n))
        if row and cts:
            row_cts = {f"CT-{num}" for pfx, num, _ in expand_ids(row[1][1])
                       if pfx == "CT"}
            missing = cts - row_cts
            if missing:
                emit("inv-check-matrix-mismatch", "13-conformance-tdd.md", row[0],
                     f"INV-{n} matrix row lacks {sorted(missing)} named by its "
                     "Check: line (CI reads only the matrix)")

    ct_defined = {n for (p, n) in defined if p == "CT"}
    hc_status = {}
    for i, cells in section_rows(HC_SECTION):
        if len(cells) >= 4 and re.match(r"HC-\d+$", cells[0]):
            hc_status[int(cells[0].split("-")[1])] = cells[3]
    hc_needed = set()
    gated_cts = set()
    for i, cells in section_rows(GATE_SECTION):
        if not cells or not re.match(r"MG-\d+$", cells[0]):
            continue
        mg, gate, threshold, _when, enforced = cells[0], cells[1], cells[2], cells[3], cells[4]
        if threshold not in ("—", "-", "") and not PARAM_RE.search(threshold):
            emit("gate-threshold-literal", "13-conformance-tdd.md", i,
                 f"{mg} threshold '{threshold}' is a literal — register a P-* symbol")
        hcs = [num for pfx, num, _ in expand_ids(enforced) if pfx == "HC"]
        hc_needed.update(hcs)
        if not hcs and not any(x.lower() in enforced.lower()
                               for x in GATE_ENFORCER_EXEMPT):
            emit("gate-no-capability", "13-conformance-tdd.md", i,
                 f"{mg} names no HC-n capability — a gate nothing enforces is a wish")
        for pfx, num, hard in expand_ids(gate + " " + enforced):
            if pfx == "CT":
                gated_cts.add(num)
                if hard and num not in ct_defined:
                    emit("gate-unknown-class", "13-conformance-tdd.md", i,
                         f"{mg} names CT-{num}, not in the taxonomy")

    ct_claims_covered = set()      # CT classes some matrix row marks C
    for (pfx, num), (i, cells) in matrix_ids.items():
        if len(cells) >= 3 and cells[2].strip().startswith("C"):
            for p2, n2, _ in expand_ids(cells[1]):
                if p2 == "CT":
                    ct_claims_covered.add(n2)
    for i, cells in section_rows(CT_SECTION):
        if not cells or not re.match(r"CT-\d+$", cells[0]):
            continue
        ct = int(cells[0].split("-")[1])
        needs = [num for pfx, num, _ in expand_ids(cells[-1]) if pfx == "HC"]
        for h in needs:
            hc_needed.add(h)
        if needs and ct in ct_claims_covered \
           and all(hc_status.get(h, "U").startswith("U") for h in needs):
            emit("ct-unbuildable", "13-conformance-tdd.md", i,
                 f"CT-{ct}: a matrix row claims C coverage but every capability "
                 "it needs is status U — unbuildable today")
        if ct not in gated_cts:
            emit("ct-ungated", "13-conformance-tdd.md", i,
                 f"CT-{ct} is reachable from no merge gate")
    for h, (dpath, dline) in sorted((k[1], v) for k, v in defined.items()
                                    if k[0] == "HC"):
        if h not in hc_needed:
            emit("hc-orphan", dpath, dline,
                 f"HC-{h} is needed by no CT class or merge gate")

    # ---- bare constants, leakage, unratified markers -----------------------
    for path, lines in docs.items():
        if path in MUTABLE_DOCS or path.startswith(ADR_DIR + os.sep) \
           or path == "README.md":
            continue
        for i, ln in strip_fences(lines):
            hd = heading_of(lines, i - 1)
            if any(x.lower() in hd.lower() for x in BARE_CONST_EXEMPT_HEADINGS):
                continue
            if BARE_CONST_RE.search(ln) and not PARAM_RE.search(ln):
                emit("param-bare-constant", path, i,
                     "bare dimensioned constant in normative text — "
                     "extract a P-* symbol (or cite the one that covers it)")
    for path, lines in docs.items():
        # Mutable status docs record the actual current state, which is
        # inherently implementation-bound (HC notes, observed values).
        if path in LEAKAGE_EXEMPT or path in MUTABLE_DOCS \
           or path.startswith(ADR_DIR + os.sep):
            continue
        for i, ln in strip_fences(lines):
            for term in LEAKAGE_TERMS:
                if term in ln:
                    emit("impl-leakage", path, i,
                         f"implementation term '{term}' in a normative doc — "
                         "rewrite in abstract mechanism language")
    route_re = re.compile(r"\b(ADR|OQ|GAP)-\d+")
    routed_params = {s for s in registry
                     if any(route_re.search(docs[PARAM_HOME][l - 1])
                            or "ratif" in docs[PARAM_HOME][l - 1].lower()
                            for (p2, l) in [registry[s]])}
    for path, lines in docs.items():
        # 15's header declares the route for its own ⚠ rows; README defines the
        # marker; ADRs discuss the marker system legitimately.
        if path in (PARAM_HOME, "README.md") or path.startswith(ADR_DIR + os.sep):
            continue
        for i, ln in strip_fences(lines):
            if "⚠" not in ln:
                continue
            para = [ln]
            for j in range(i, min(i + 2, len(lines))):
                if lines[j].strip():
                    para.append(lines[j])
            blob = " ".join(para)
            if route_re.search(blob):
                continue
            if any(f"{p[0]}-{p[1]}" in {s for s in routed_params}
                   or f"{p[0]}-{p[1]}" in registry
                   for p in [(m.group(1), m.group(2))
                             for m in PARAM_RE.finditer(blob)]):
                continue
            emit("warn-unratified", path, i,
                 "⚠ marker routes nowhere — cite the ADR/OQ/GAP or a registered "
                 "parameter that owns the decision")

    # ---- history (vs --base) ----------------------------------------------
    if base_ref:
        run_history_checks(spec_dir, base_ref, docs, defined)

    # ---- freshness ---------------------------------------------------------
    try:
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                             capture_output=True, text=True, cwd=spec_dir)
        if top.returncode == 0:
            asof = None
            for ln in conf:
                m = re.search(r"(\d{4}-\d{2}-\d{2})", ln)
                if m:
                    asof = max(asof or "", m.group(1))
            dirty = subprocess.run(
                ["git", "status", "--porcelain", "--", "."],
                capture_output=True, text=True, cwd=spec_dir).stdout
            dirty_files = {ln[3:].split("/")[-1] for ln in dirty.splitlines()}
            if asof:
                for path in docs:
                    if path in MUTABLE_DOCS or path.startswith(ADR_DIR + os.sep):
                        continue
                    r = subprocess.run(
                        ["git", "log", "-1", "--format=%cs", "--", path],
                        capture_output=True, text=True, cwd=spec_dir)
                    d = r.stdout.strip()
                    if d and d > asof and \
                       "13-conformance-tdd.md" not in dirty_files:
                        emit("stale-matrix", path, 1,
                             f"committed {d}, newer than 13's as-of date {asof} — "
                             "update the matrix/register in the same change")
    except FileNotFoundError:
        pass

    # ---- ignore file, output ----------------------------------------------
    ignores = []
    ipath = os.path.join(spec_dir, ".spec-check-ignore")
    if use_ignore and os.path.exists(ipath):
        with open(ipath, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                parts = [p.strip() for p in ln.split("|")]
                if len(parts) == 3:
                    ignores.append(parts)
    import fnmatch
    kept = []
    for f in findings:
        if only and f["check"] != only:
            continue
        if min_sev and f["severity"] != min_sev:
            continue
        if any(c == f["check"] and fnmatch.fnmatch(f["file"], g)
               and re.search(rx, f["message"]) for c, g, rx in ignores):
            continue
        kept.append(f)

    order = {"E": 0, "W": 1, "I": 2}
    kept.sort(key=lambda f: (order[f["severity"]], f["file"], f["line"]))
    if fmt == "json":
        print(json.dumps(kept, indent=2, ensure_ascii=False))
    elif fmt == "github":
        for f in kept:
            kind = {"E": "error", "W": "warning", "I": "notice"}[f["severity"]]
            print(f"::{kind} file={spec_dir}/{f['file']},line={f['line']},"
                  f"title={f['check']}::{f['message']}")
    else:
        for f in kept:
            print(f"[{f['severity']}] {f['check']}  {f['file']}:{f['line']}  "
                  f"{f['message']}")
        by = defaultdict(int)
        for f in kept:
            by[f["severity"]] += 1
        print(f"\n{len(kept)} finding(s): "
              f"{by['E']} error, {by['W']} warning, {by['I']} info")
    errors = sum(1 for f in kept if f["severity"] == "E"
                 or (strict and f["severity"] == "W"))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
