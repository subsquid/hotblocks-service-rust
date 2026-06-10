# Rust rewrite plan: hot-block data service

Reimplementation of `sdk/evm/evm-data-service` (+ its internal deps) in Rust.
Goal: byte-compatible HTTP API and data format, same operational behavior,
architecture that generalizes to non-EVM chains later.

Reference TS sources (read them when implementing — they are the spec):
- Service entry: `sdk/evm/evm-data-service/src/main.ts`
- EVM data source setup/mapping: `sdk/evm/evm-data-service/src/data-source/`
- Generic service: `sdk/util/util-internal-data-service/src/` (data-service.ts, chain.ts, http-app.ts, metrics.ts, types.ts, util.ts)
- DataSource abstraction: `sdk/util/util-internal-data-source/src/index.ts`
- EVM RPC ingestion: `sdk/evm/evm-rpc/src/` (rpc.ts, rpc-data.ts, data-source/{rpc-data-source,ingest,poll-stream,finalizer}.ts, verification code, chain-utils.ts)
- Normalization: `sdk/evm/evm-normalization/src/` (mapping.ts, data.ts)
- RPC client: `sdk/util/rpc-client/src/` (client.ts, rate.ts, transport/http.ts)
- JSON conventions: `sdk/util/util-internal-json/src/json.ts`

## Architectural decisions (differences from TS)

1. **No worker threads.** TS uses worker threads to keep CPU work (mapping,
   zstd, verification) off the HTTP event loop, and spawns a fresh worker per
   backfill request. In Rust: tokio for IO; CPU-bound block processing
   (keccak/MPT/sender recovery/zstd) runs via `tokio::task::spawn_blocking`
   (or a rayon pool) inside the mapping stage. One shared RPC client for both
   head-following and backfill streams (shared rate-limit budget — intended).
2. **Generic core / chain-specific edge.** Workspace split so non-EVM chains
   plug in by implementing one trait:
   - `data-service-core`: `Block` struct (number/hash/parent/timestamp/zstd
     payload), `DataSource` trait, `Chain` buffer, `DataService` ingestion
     loop, HTTP app, metrics. Zero EVM knowledge.
   - `evm-source`: everything EVM (RPC client usage, validation, verification,
     normalization, mapping to core `Block`).
   - `bin/evm-data-service`: CLI gluing them.
3. Fix the `/` welcome text ("Solana" copy-paste in TS).
4. The `sqd:*:trace` per-block instrumentation log lines in current working
   tree become `tracing::debug!`/`trace!` events, not always-on info.

Everything below must match TS behavior exactly unless flagged.

## Workspace layout (`rust-data-service/`)

```
Cargo.toml                  # workspace
crates/
  data-service-core/        # chain-agnostic service
  rpc-client/               # JSON-RPC client (batching, rate limit, retry)
  evm-source/               # EVM DataSource impl (fetch, validate, verify, normalize)
  evm-data-service/         # binary: CLI + wiring
```

Suggested crates: tokio, hyper or axum (HTTP server — streaming responses
required), reqwest (RPC transport), serde/serde_json, zstd, flate2 (gzip),
prometheus or metrics+exporter, clap (CLI; flags must mirror TS names),
tracing + tracing-subscriber, alloy-primitives + alloy-rlp + alloy-trie
(keccak/RLP/MPT), k256 (sender recovery), thiserror, anyhow, futures.

## Phase 1 — `data-service-core`

### Types (`types.ts`)
```rust
struct Block { number: u64, hash: String, parent_number: u64, parent_hash: String,
               timestamp: Option<u64> /* ms */, json_line_zstd: Bytes }
struct BlockRef { number: u64, hash: String }
struct DataResponse { finalized_head: Option<BlockRef>,
                      head: Option<BlockStream>, tail: Option<Vec<Block>> }
struct InvalidBaseBlock { prev: Vec<BlockRef> }
```

### DataSource trait (mirror `util-internal-data-source`)
```rust
struct StreamRequest { from: u64, to: Option<u64>, parent_hash: Option<String> }
struct BlockBatch { blocks: Vec<Block>, finalized_head: Option<BlockRef> }
trait DataSource {
    async fn get_head(&self) -> Result<BlockRef>;
    async fn get_finalized_head(&self) -> Result<BlockRef>;
    fn get_stream(&self, req: StreamRequest) -> BoxStream<Result<BlockBatch, StreamError>>;
    fn get_finalized_stream(&self, req: StreamRequest) -> BoxStream<...>;
}
```
Fork signaling: `StreamError::Fork { previous_blocks: Vec<BlockRef> }`
(TS `ForkException` with blockNumber, expectedParentHash, previousBlocks).
A stream yields verified blocks up to the fork point, then the fork error.

### Chain buffer (`chain.ts` — port exactly)
Sorted `Vec<Block>` + `finalized_head: usize` index.
- `push`: append if `last.number == new.parent_number` (assert hash chain);
  otherwise bisect to parent position, assert `pos >= finalized_head`
  ("attempt to revert finalized head") and `pos < len` (gap), truncate and
  append (reorg).
- `finalize(head_ref)`: no-op if below first block; if above last block,
  finalize everything; else bisect, assert number+hash match, advance index
  monotonically. Returns whether it advanced.
- `compact(max_size, auto_adjust)`: trim up to `finalized_head` entries when
  over `max_size`; if cannot trim enough and `auto_adjust_finalized_head` is
  set, force-advance finalized head with a warning. Returns ok flag (false →
  service logs "block finalization lags behind and prevents cache purging").
- `query(from, base_block_hash)`: exact TS semantics (`chain.ts:91-134`):
  - `from <= first.parent_number` → empty response (caller does below-query).
  - in-range, parent hash mismatch → `InvalidBaseBlock` with up to 100
    previous block refs (parent refs of blocks `[pos-100ish..=pos]`).
  - in-range, match → `{finalized_head, tail: blocks[pos..]}`.
  - `from == last.number + 1` with mismatched base hash → `InvalidBaseBlock`
    with refs of last ≤100 blocks.
  - else → `{finalized_head}` only.
- `get_fork_base(prev: Vec<BlockRef>)`: walk own chain top-down (not below
  finalized head) against upstream refs, return highest common ref
  (`chain.ts:202-220`).
- `snapshot()`: clone of blocks vec (cheap if `Block` payload is `Bytes`/Arc).

### DataService (`data-service.ts` — port exactly)
- `init()`: `get_finalized_head()`, then fetch exactly that one block via
  `get_finalized_stream{from:h, to:h}` → seed `Chain`.
- `run()` loop: `ingest_session(base)` consuming `get_stream{from: base+1,
  parent_hash: base.hash}`; per batch: push blocks, track max finalized head,
  `chain.finalize`, `chain.compact`, update metrics, notify waiters.
  Error handling: on fork error → `get_fork_base`; if none → fatal
  "rollback behind finalized head". On other errors: if head didn't advance
  since last restart increment `stacked`; pause 30s when `stacked > 1`; after
  `stacked > 5` re-`init()` from scratch. First-block-ingested future gates
  service readiness; ingestion death before first block is fatal.
- `query(from, parent_hash)`:
  - if `from <= chain.first().parent_number` → **below-query**: snapshot tail
    + finalized head first, then open `get_finalized_stream{from,
    to: first.parent_number, parent_hash}`; first batch awaited eagerly
    (fork error → `InvalidBaseBlock(prev)`); response streams source batches
    (asserting per-block continuity, incl. continuity with snapshot tail)
    then the snapshot tail. Continuity violation = panic/500.
  - else `chain.query`; if block not yet available, wait up to **5s** for it
    (`wait_for_block` via watch/notify), then re-query.
  - metrics: count query as cache/backfill/error.
- `is_ready()`: `source.get_head().number <= chain.head().number`.

### HTTP app (`http-app.ts` — port exactly; API is the contract)
- `GET /` → 200 text (fix wording: "Welcome to hot block data service!" or
  EVM-specific).
- `POST /stream`, body ≤ 1024 bytes, JSON `{fromBlock: nat,
  parentBlockHash?: string}`; 400 on validation failure.
  - `InvalidBaseBlock` → 409 JSON `{"previousBlocks": [{number, hash}...]}`.
  - headers: `x-sqd-finalized-head-number`, `x-sqd-finalized-head-hash`.
  - no head and no tail → 204.
  - body: concatenated JSON lines; `content-type: text/plain; charset=UTF-8`.
    If client `Accept-Encoding` contains `zstd` → write stored zstd frames
    as-is with `content-encoding: zstd`; else decompress and re-encode gzip
    level 1 per block. `vary: Accept-Encoding`. NOTE: each block is an
    independent zstd frame / gzip member concatenated — clients rely on this.
  - stream head batches then tail; stop early when response has been running
    > 60s and the socket needs backpressure (TS checks elapsed both on drain
    waits and after each batch). Mirror this: respect backpressure, cap ~60s.
- `GET /head`, `GET /finalized-head` → 200 JSON BlockRef.
- `GET /readiness` → 200 "true" / 503 "false".
- `GET /metrics` (prometheus text; `?json=true` → JSON), `GET /metrics/{name}`,
  404 if unknown.
- `GET /block-time/{height}` → ingestion timestamp (ms) as text, 404 if absent.

### Metrics (`metrics.ts` — same metric names/labels)
Port the registry: first/last/finalized block gauges, stored blocks, last
block timestamp, block lag observation, processing time, query counter with
result label (cache/backfill/error), active workers gauge (rename or keep for
dashboard compat — keep), block ingestion timestamp map backing `/block-time`
(bounded LRU; check TS impl for retention).

## Phase 2 — `rpc-client` crate (port `util/rpc-client`)

- JSON-RPC 2.0 over HTTP (reqwest, keep-alive). WS optional/later — the TS
  service accepts ws urls but http covers production; flag this as a TODO.
- Batch calls: split by `max_batch_call_size` (default: unlimited, or
  `max(1, rate_limit/5)` when rate-limited); match responses by id.
- Concurrency `capacity` (service passes MAX — effectively unlimited),
  priority queue, sliding-window rate limiter (10 × 100ms slots) counting
  batch items.
- Retry: schedule `[10,100,500,2000,10000,20000]` ms (repeat last), retry
  attempts configurable (service uses 5).
- Retryability (`client.ts isConnectionError` + evm `isRetryableError`):
  - transport/connection errors, timeouts
  - HTTP 408, 429, 502, 503, 504, **529**
  - message `/rate limit|too many requests/i`, `/execution timeout/i`,
    `/request .* timed out/i`
  - RPC error code -32005; code 429
  - with `retry_internal_server_errors`: code -32000/-32603,
    `/internal( server)? error/i`
  - `RetryError` thrown by per-call validators (model as an error variant
    validators can return)
  - batch-level (`reduceBatchOnRetry`): also RpcProtocolError and
    "response too large" → split batch in half and retry recursively.
- Per-call `validate_result` / `validate_error` hooks (needed by evm quirks).

## Phase 3 — `evm-source` crate

### 3a. RPC data model + validation (`evm-rpc/src/rpc-data.ts`)
Define serde structs for GetBlock, Transaction, Receipt, Log, debug frames,
trace replays, with the same leniency the TS validation DSL has:
- preserve unknown handling semantics: TS service *casts* into a normalized
  shape (the data service maps, it does not store raw), so plain serde
  structs are fine; but be tolerant exactly where TS is:
  - quirk fields: optional `to` (Sei deployments), snake_case access lists
    (Frontier `oneOf`), nullable-vs-missing distinctions (`option(nullable())`).
- hex quantity type (`Qty`) kept as string where TS keeps strings; convert
  with the same `qty2Int` (u64 with safe-int check where TS uses
  `safeQty2Int`, e.g. chainId).

### 3b. Rpc fetch layer (`evm-rpc/src/rpc.ts`)
- `eth_getBlockByNumber` (with txs), latest head via tag; finalized head:
  `finality_confirmation` set → `eth_blockNumber - confirmation`, else
  `finalized` tag.
- logs mode (default): `eth_getLogs` per range; receipts mode:
  `eth_getBlockReceipts` with auto-detect + per-tx
  `eth_getTransactionReceipt` fallback (validate receipt.blockHash).
- traces: debug `callTracer` (`debug_traceBlockByHash`, or ByNumber under
  `--use-debug-trace-block-by-number`, timeout param "60s") or
  `trace_replayBlockTransactions` under `--use-trace-api`.
- statediffs: `trace_replayBlockTransactions` stateDiff, or debug
  prestateTracer under `--use-debug-api-for-statediffs`.
- per-method transient-error quirks (Hyperliquid 'invalid block height',
  Avalanche 'cannot query unfinalized data', Alchemy/Sei -32000) → retry via
  validator hooks. Port the table from rpc.ts.
- height-mismatch / hash-mismatch responses mark block `_isInvalid` → stride
  stops and retries (check `ingest.ts` semantics).

### 3c. Verification (each behind its CLI flag, same defaults)
- block hash: RLP header encode + keccak256 (Tempo nested-header variant for
  chain ids 0x1079/0xa5bf/0xa5bd).
- tx root / receipts root / withdrawals root: ordered MPT (alloy-trie),
  RLP-encoded items keyed by rlp(index); receipts root honors
  `--use-gas-used-for-receipts-root` (Cosmos chains); exclude Polygon
  (0x89)/Shibarium state-sync tx type 0x7f and Hyperliquid system txs.
- logs bloom: standard 3×11-bit keccak bloom check.
- tx sender: k256 ECDSA recovery over the per-type signing hash (legacy,
  2930, 1559, 4844, 7702), compare lowercased.
- log index sequential check (`--skip-log-index-check` disables),
  cumulativeGasUsed monotonicity check (`--skip-cumulative-gas-used-check`).
- Cronos phantom-tx handling (rpc.ts:510-851): isolate in `quirks/cronos.rs`;
  port only if Cronos is a target — confirm with owner before spending time.

### 3d. Ingestion / streaming (`data-source/*.ts`)
- `EvmRpcDataSource::get_finalized_stream`: stride-splitting ingest with
  `stride_size` (default 5) and `stride_concurrency` (default 5) parallel
  in-flight strides, ordered emission; `ensure_continuity` checks
  parentHash linkage (against `req.parent_hash` for the first block) and
  converts mismatch into the Fork error carrying previously emitted refs.
- `get_stream`: same + `finalizer` stage: passes batches through, probes
  unfinalized blocks (≤5 per probe, 500ms between probes, ≤50 queued) against
  the finalized chain and attaches `finalized_head` to batches.
- poll-stream: near-head polling, 100ms sleep when caught up.
- Read `ingest.ts`/`poll-stream.ts`/`finalizer.ts` carefully — exact
  batching/ordering matters for fork semantics.

### 3e. Normalization + mapping (`evm-normalization/mapping.ts` + service `mapping.ts`)
Output JSON must be **field-for-field identical** to the TS service (clients
parse these lines):
- header/tx/log field conversions per mapping.ts (hex→int for
  number/timestamp/size/indices/nonce/status/type/yParity/gas
  used fields; keep Qty hex strings where TS keeps them; lowercase all
  addresses; `l1FeeScalar` → float).
- traces: flatten debug call tree depth-first with `traceAddress` paths and
  `subtraces` counts; type mapping create/call/selfdestruct; callType
  lowercased; revert reason extraction from ABI-encoded output. Trace-API
  replay path as alternative.
- statediffs: kinds `+ - * =`, `=` filtered for replay source; iteration
  order: TS iterates JS object key order (insertion order of the JSON) —
  use a JSON parser preserving order (serde_json `preserve_order`) to match.
- serialization: serde to the same JSON shape TS `toJSON` produces (bigints
  as decimal strings — check which fields are bigint in TS output; hex bytes
  with 0x). **Key order in JSON output should match TS** so byte-level diffs
  are possible during validation; serde struct field order handles this.
- final per-block record: `JSON.stringify(...) + "\n"` then zstd level 1 →
  `Block{number, hash: block.hash, parent_number: number-1, parent_hash,
  timestamp: parse(ts)*1000, json_line_zstd}`.

## Phase 4 — binary + CLI

Mirror every flag from `main.ts` (same names/defaults): `--http-rpc`
(required), `--http-rpc-max-batch-call-size`, `--http-rpc-stride-size` (5),
`--http-rpc-stride-concurrency` (5), `--http-rpc-rate-limit`,
`--http-rpc-timeout` (10000), `--http-retry-internal-server-errors`,
`--block-cache-size` (1000), `-p/--port` (3000), `--finality-confirmation`,
`--with-receipts/--with-traces/--with-statediffs`, `--use-trace-api`,
`--use-debug-api-for-statediffs`, `--use-debug-trace-block-by-number`,
all `--verify-*`, `--skip-log-index-check`,
`--skip-cumulative-gas-used-check`, `--use-gas-used-for-receipts-root`,
`--auto-adjust-finalized-head`.
Note TS bug: `httpRetryInternalServerErrors` is parsed but **not** forwarded
into `DataSourceOptions` in main.ts — in Rust, forward it (intentional fix).
Logging: JSON logs to stderr comparable to @subsquid/logger (level via
`SQD_LOG`/RUST_LOG; nice-to-have, not contract). Graceful shutdown on
SIGINT/SIGTERM.

## Phase 5 — verification & parity testing

1. Unit tests: Chain buffer (push/reorg/finalize/compact/query edge cases —
   port scenarios from chain.ts semantics), retry classification table,
   verification primitives against known mainnet blocks (hash/txRoot/
   receiptsRoot/bloom/sender recovery fixtures).
2. **Differential test**: run TS service and Rust service against the same
   RPC endpoint; diff `/head`, `/finalized-head`, and decompressed `/stream`
   JSON lines per block (jq-normalized and ideally byte-identical). Script it.
3. Fork simulation: mock DataSource emitting reorgs; assert 409 protocol,
   getForkBase behavior, finalized-head protection.
4. Load: confirm streaming backpressure + 60s cap; memory stays bounded by
   block-cache-size.

## Suggested execution order for subagents

1. Workspace scaffolding + core types + Chain buffer with tests.
2. rpc-client crate.
3. DataService loop + HTTP app + metrics (against a mock DataSource) — the
   service is testable end-to-end here with a fake chain.
4. evm rpc-data model + fetch layer + ingest/poll/finalizer streams.
5. Normalization/mapping with JSON-parity fixtures (capture fixtures from a
   real chain via the TS service).
6. Verification suite.
7. CLI wiring + differential testing vs TS.

Each agent must read the corresponding TS files before implementing; this
plan lists semantics but the TS code is authoritative for edge cases.
