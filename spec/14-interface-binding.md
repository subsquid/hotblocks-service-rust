# 14 — Interface binding

The only normative document that names the concrete surface. Everything here is
*observable contract*; internals stay out. Anything not specified here is unspecified —
clients and tests MUST NOT pin it. Binding changes update this file and CT-5 in the
same change (IB-20).

## General transport rules

**IB-1.** The API is HTTP/1.1+ on the configured port. Request bodies are JSON.
Unknown routes → 404; only that status is bound (headers and body are unspecified),
and this transport outcome is not RP-13's endpoint-level NOT_FOUND. Wrong method on a
known route → 405. Text bodies bound below are `text/plain`; JSON bodies
`application/json`.

**IB-2 — Compression negotiation.** Non-empty successful responses (200) of
`POST /stream` are always content-encoded: `zstd` when the request's
`Accept-Encoding` contains the token
`zstd` (substring match — pinned for predecessor compatibility, ADR-1), else `gzip`.
There is no identity mode. `Content-Encoding` and `Vary: Accept-Encoding` are set.
The body is a concatenation of **independent per-block frames**: zstd frames
pass through from storage; gzip mode re-encodes each block as its own gzip member.
Clients MUST use a multi-frame/multi-member decoder.

## Operation → endpoint table

| Operation (05) | Method & path | Success | Notes |
|---|---|---|---|
| liveness | `GET /` | 200 text `Welcome to hot block data service!` | exact bytes per ADR-1 (predecessor's text was wrong; corrected — deliberate divergence) |
| head | `GET /head` | 200 JSON `{"number": u64, "hash": string}` | |
| finalizedHead | `GET /finalized-head` | 200 JSON, same shape | |
| readiness | `GET /readiness` | 200 text `true` / 503 text `false` | 503 also on probe failure (RP-10) |
| stream | `POST /stream` | 200 block frames / 204 empty | see IB-3..IB-8 |
| metrics | `GET /metrics` | 200 Prometheus text v0.0.4 | `?json=true`: structured JSON array of Prometheus metric families |
| metric | `GET /metrics/{name}` | 200 single-metric text | 404 text `requested metric not found` |
| blockIngestTime | `GET /block-time/{height}` | 200 text decimal ms timestamp | 404 text `Timestamp not found for the specified block`; entries expire per `P-BLOCKTIME-TTL`, capacity `P-BLOCKTIME-CACHE` |

## POST /stream

**IB-3 — Request.**

```jsonc
{
  "fromBlock": 12345678,        // required, natural number (DEF-1)
  "parentBlockHash": "0x…"      // optional; null ≡ absent
}
```

Body ≤ `P-REQ-BODY-MAX` bytes. Unknown fields ignored (pinned — ADR-1).

**IB-4 — Success (200).** Concatenated per-block frames (IB-2); decompressed, each
frame is one JSON record terminated by `\n` (the canonical record, DEF-6). Chunked
transfer; no `Content-Length`. Headers on 200 and 204:
`x-sqd-finalized-head-number: <decimal>`, `x-sqd-finalized-head-hash: <hash>` —
the snapshot's finalized head (RP-9). DEF-2's HTTP field-value constraint is checked
on the seed, every ordinary block and parent hash, and every finality report before
the corresponding input can mutate the buffer.

**IB-5 — Empty (204).** No body; the finalized-head headers are present. Meaning:
RP-12's empty form.

**IB-6 — Conflict (409).** JSON body `{"previousBlocks": [{"number": u64, "hash":
string}, …]}` — RP-7's `prev`, non-empty (GAP-6 marks the current empty-list
breach), ascending, newest last. No finalized-head headers.

**IB-7 — Errors.** Status/error mapping (RP-13 classes):

| Class | Status | Body |
|---|---|---|
| INVALID_REQUEST | 400; 413 when the body exceeds `P-REQ-BODY-MAX` | text diagnostic |
| CONFLICT | 409 | IB-6 |
| EMPTY | 204 | IB-5 |
| NOT_FOUND | 404 | text diagnostic |
| INTERNAL | 500 | text starting `Internal server error` |

After the first frame is sent, the status is committed: any later failure ends the
body at a frame boundary (RP-12 truncation; the client resumes from the last frame).

**IB-8 — Response limits.** Production stops at `P-RESP-BUDGET`; the above-head wait
is `P-WAIT-BLOCK`. Both count from admission.

## Payload

**IB-9.** The canonical record's field-level schema is chain-family-specific. For EVM
it is the predecessor's normalized block form (ADR-1 pins it as the migration
contract): a single JSON object
`{header, transactions, logs?, traces?, stateDiffs?}` with camelCase keys throughout,
lower-cased addresses, decimal JS-safe integers for small quantities, hex strings for
large ones, receipt fields flattened onto transactions, and per-selection presence
per REQ-8. The authoritative definition is the golden corpus + differential oracle
(HC-4/HC-8), not prose: any byte deviation from the predecessor's output for the same
upstream data is a defect until OQ-4 sunsets REQ-24.

## Configuration surface

**IB-10.** CLI flags (names and defaults pinned — ADR-1). Values are bound to `P-*`
symbols in 15.

| Flag | Default | Binds |
|---|---|---|
| `--http-rpc <url>` | required | upstream endpoint (http/https/ws/wss) |
| `--port` | `P-PORT` | listen port |
| `--block-cache-size` | `P-CACHE-SIZE` | window (DEF-24) |
| `--http-rpc-stride-size` | `P-STRIDE-SIZE` | acquisition range-batch size |
| `--http-rpc-stride-concurrency` | `P-STRIDE-CONCURRENCY` | concurrent range batches |
| `--http-rpc-rate-limit` | unset | upstream budget, items/s (REQ-16) |
| `--http-rpc-timeout` | `P-RPC-TIMEOUT` | per-call timeout |
| `--http-rpc-max-batch-call-size` | unset | upstream call-batch cap (GAP-20) |
| `--http-retry-internal-server-errors` | off | widen retryable classification (FM-23) |
| `--finality-confirmation <n>` | unset | finality policy `offset(n)` (DEF-23) |
| `--auto-adjust-finalized-head` | off | DEF-24, WP-24 |
| `--with-receipts` / `--with-traces` / `--with-statediffs` | off | data selection (DEF-22); receipts and logs are mutually exclusive acquisitions — receipts off ⇒ logs on |
| `--use-trace-api`, `--use-debug-api-for-statediffs`, `--use-debug-trace-block-by-number` | off | EVM acquisition method choices |
| `--verify-block-hash`, `--verify-tx-sender`, `--verify-tx-root`, `--verify-receipts-root`, `--verify-withdrawals-root`, `--verify-logs-bloom` | off | verification policy (DEF-25); each is applied per block, and its failure is incoherence (WP-11.4) |
| `--skip-log-index-check`, `--skip-cumulative-gas-used-check`, `--use-gas-used-for-receipts-root` | off | coherence-check tuning (DEF-15 instantiation) |
| `--profile-block-timings` | off | opt-in per-block timing telemetry (ADR-5 family; not in the predecessor) |

Receipt-dependent policy is validated at startup: `--verify-receipts-root` and
`--skip-cumulative-gas-used-check` require `--with-receipts`, while
`--use-gas-used-for-receipts-root` additionally requires
`--verify-receipts-root`. The other five verification switches operate on the
header/transactions or on logs available through either acquisition path.

Environment: `RUST_LOG`-style filter takes precedence; `SQD_TRACE/DEBUG/INFO/WARN/
ERROR/FATAL` set the global level for chart compatibility (values are not
namespace-interpreted — divergence from the predecessor's namespace globs, accepted).
Startup validation per REQ-32/FM-50.

**IB-11 — Process contract.** Exit 0 on clean shutdown; non-zero on startup failure
(FM-31) and terminal divergence (FM-30, per ADR-12). `SIGINT`/`SIGTERM` → graceful
drain within `P-SHUTDOWN-GRACE`; a second signal → immediate exit 130.

## Metrics binding

**IB-12.** Required series (OB obligations in parentheses; names pinned by ADR-1):
`sqd_hotblocks_first_block`, `sqd_hotblocks_last_block`,
`sqd_hotblocks_finalized_block`, `sqd_hotblocks_stored_blocks` (OB-1);
`sqd_hotblocks_last_block_lag_ms`, `sqd_hotblocks_block_lag_ms` (OB-3; −1 is the
pinned wire sentinel for absence on ⊥ timestamps — ADR-1, revisited at OQ-4);
`sqd_hotblocks_processing_time_ms` (OB-5);
`sqd_hotblocks_queries_total{type=cache|backfill|error}` (OB-5; all label values
pre-registered). Signals this suite additionally requires but the surface lacks today:
the OB-1 window-excess gauge, OB-2 heartbeat, OB-4 upstream head/finalized views and
interaction counters, OB-5 wait-empty/conflict query classes and truncation counters,
OB-6/OB-7 alarm levels, and OB-9 lifecycle timestamps — tracked by
GAP-4/GAP-24/GAP-25. `sqd_hotblocks_active_workers` exists for
predecessor compatibility; a series that cannot move violates INV-30 (GAP-24) and is
removed or implemented per OQ-4.

## Upstream binding (EVM) — the input-side contract

What a simulator/stub must implement (HC-1/HC-3), and what the adapter may assume.

**IB-13 — Protocol.** JSON-RPC 2.0 over HTTP(S) or WebSocket, batch calls supported;
per-call timeout `P-RPC-TIMEOUT`; retryable classes per FM-22/FM-23: transport
errors, timeouts, HTTP 408/429/5xx-gateway, RPC codes −32005/429, rate-limit and
timeout message patterns; internal-error codes only when the widening flag is on.
Retries: `P-RPC-RETRY-ATTEMPTS` on schedule `P-RPC-RETRY-SCHEDULE` (indexed by
attempt, last entry repeated past its end); batches split on
too-large responses. Within one logical batch operation, an item whose successful
response was observed is complete and MUST NOT be submitted again by RPC-level retry
or reduction; only unresolved or retryable failed items may be retried. If a
request-level failure leaves every item outcome unknown, the request may be split and
retried. This does not weaken WP-11.2: later whole-block re-acquisition is a new
coherence attempt and intentionally re-fetches its header and selected components.
Diagnostics and error text MUST NOT leak endpoint credentials (GAP-26).

**IB-14 — Method matrix** (per data selection):

| Data | Method |
|---|---|
| header + transactions | `eth_getBlockByNumber(qty, true)` |
| head / finality watermark | `eth_getBlockByNumber("latest"\|"finalized"\|qty, false)`; offset policy uses `eth_blockNumber` + by-number fetch |
| logs (receipts off) | `eth_getLogs({fromBlock, toBlock})` |
| receipts | `eth_getBlockReceipts` (probed once; per-tx `eth_getTransactionReceipt` fallback) |
| traces (debug) | `debug_traceBlockByHash\|ByNumber` with `callTracer {onlyTopCall:false, withLog:true}`, timeout `P-DEBUG-TIMEOUT` |
| traces (trace API) | exactly one per selection (GAP-17): `trace_block(qty)` or `trace_replayBlockTransactions(hash, [trace])` |
| state diffs (trace API) | `trace_replayBlockTransactions(hash, [stateDiff])` |
| state diffs (debug) | `debug_traceBlock*` with `prestateTracer {diffMode:true}` |
| identity | `eth_chainId` (once; SHOULD gate FM-52) |

**IB-15 — Coherence instantiation (DEF-15 for EVM).** Per block: every receipt's
block hash equals the header hash; receipt count equals transaction count; log
indices are block-wise continuous from 0 (unless skipped by configuration);
cumulative gas is non-decreasing and consistent per transaction (unless skipped);
an empty log set is coherent only with an empty logs-bloom; trace/state-diff results
must reference the requested block and cover its transactions. Enabled verification
checks (DEF-25) join this predicate.

**IB-16 — Tolerances.** The adapter MUST tolerate (as the predecessor does):
absent optional fields per the baseline schema (log-removal marker, pre-status-era
receipts — GAP-14); null entries in receipt arrays — stripped with alarm, which by
construction fails IB-15's receipt-count check for the batch method, so coherence is
re-established via the per-tx receipt fallback (IB-14) or WP-11.2 retry, never by
relaxing the count check or serving the stripped set; providers
that answer not-yet-indexed blocks with null/absent results (retry class, FM-16);
and the documented per-network quirks of the supported set (REQ-15, GAP-16).

**IB-17 — Budget.** All methods above share the single configured budget (rate
limit × batch cap × concurrency) — REQ-16/RP-22/ADR-3.

## Versioning

**IB-20.** Any change to this file is a contract change: it lands with the CT-5
update in the same change, and — while REQ-24 stands — with a differential-run
justification or an ADR recording the deliberate divergence.
