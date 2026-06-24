# rust-data-service

Hot-block data service for EVM chains — a Rust reimplementation of
`sdk/evm/evm-data-service` (and its internal deps). It follows the chain head
over JSON-RPC, buffers recent blocks, and serves them over HTTP as concatenated
JSON lines.

The HTTP API, the per-block JSON data format, and the CLI flag names are a
byte-compatible contract with the TypeScript service: a client cannot tell which
implementation it is talking to. The TS source remains the authoritative spec
for behavior not stated here.

## Workspace layout

| Crate | Responsibility |
|-------|----------------|
| `data-service-core` | Chain-agnostic: `Block`, the `DataSource` trait, the chain buffer, the ingestion loop, HTTP, metrics. No EVM knowledge. |
| `rpc-client` | JSON-RPC client: batching, rate limiting, retry, HTTP and WebSocket transports. |
| `evm-source` | Everything EVM: RPC fetch, validation, verification, normalization, mapping to core `Block`. |
| `evm-data-service` | The binary: CLI and wiring. |

A non-EVM chain plugs in by implementing the `DataSource` trait against
`data-service-core`.

## Build and run

```sh
cargo build --release -p evm-data-service
./target/release/evm-data-service --http-rpc <RPC_URL> --port 3000
```

The `Dockerfile` builds the same binary and runs it as the entrypoint.

## HTTP API

| Method | Path | Returns |
|--------|------|---------|
| GET | `/` | Liveness text |
| GET | `/head` | Chain head `{number, hash}` (JSON) |
| GET | `/finalized-head` | Finalized head `{number, hash}` (JSON) |
| GET | `/readiness` | `200 "true"` when caught up, else `503 "false"` |
| POST | `/stream` | Block stream (see below) |
| GET | `/metrics` | Prometheus text (`?json=true` for JSON) |
| GET | `/metrics/{name}` | One metric, `404` if unknown |
| GET | `/block-time/{height}` | Ingestion timestamp in ms, `404` if absent |

`POST /stream` takes a JSON body `{fromBlock: number, parentBlockHash?: string}`
(≤1024 bytes). It responds with concatenated per-block JSON lines and the
`x-sqd-finalized-head-*` headers. Each block is an independent compressed frame:
stored zstd frames pass through when the client sends `Accept-Encoding: zstd`,
otherwise each block is re-encoded as a gzip member. A base block that no longer
matches the chain returns `409` with `{"previousBlocks": [...]}` so the client
can roll back.

## CLI flags

`--http-rpc <url>` is required. Names and defaults mirror the TS service.

| Flag | Default | Purpose |
|------|---------|---------|
| `--port` | 3000 | Listen port |
| `--block-cache-size` | 1000 | Blocks buffered in memory |
| `--http-rpc-stride-size` | 5 | Blocks per backfill stride |
| `--http-rpc-stride-concurrency` | 5 | Concurrent in-flight strides |
| `--http-rpc-rate-limit` | — | Max requests/sec to the RPC |
| `--http-rpc-timeout` | 10000 | RPC request timeout (ms) |
| `--http-rpc-max-batch-call-size` | — | Cap JSON-RPC batch size |
| `--http-retry-internal-server-errors` | off | Treat RPC internal errors as retryable |
| `--finality-confirmation <n>` | — | Use `head - n` as finalized instead of the `finalized` tag |
| `--auto-adjust-finalized-head` | off | Force-advance the finalized head when the cache fills |

Data selection: `--with-receipts`, `--with-traces`, `--with-statediffs`,
`--use-trace-api`, `--use-debug-api-for-statediffs`,
`--use-debug-trace-block-by-number`.

Verification (all off by default): `--verify-block-hash`, `--verify-tx-sender`,
`--verify-tx-root`, `--verify-receipts-root`, `--verify-withdrawals-root`,
`--verify-logs-bloom`. Tune consistency checks with `--skip-log-index-check`,
`--skip-cumulative-gas-used-check`, `--use-gas-used-for-receipts-root`.

`--profile-block-timings` emits per-block pipeline timing logs (target
`block_timing`).

## Differences from the TypeScript service

The API and data format match. The items below are where it deliberately
diverges — in structure, in runtime behavior, or to fix a TS bug.

### Architecture

- **No worker threads.** TS offloads CPU work (mapping, keccak/MPT, sender
  recovery, zstd) to worker threads, spawning a fresh worker per backfill
  request. Rust runs IO on tokio and CPU-bound block processing via
  `tokio::task::spawn_blocking`. One shared RPC client serves both head-following
  and backfill, so they share a single rate-limit budget — intended.
- **Generic core, chain-specific edge.** The TS service is EVM-specific
  throughout; here the chain-agnostic machinery lives in `data-service-core` and
  EVM specifics in `evm-source` (see the layout above).

### Head-following and enrichment

- **Speculative, pipelined head path.** At the head, Rust polls block N's
  existence with a single `eth_getBlockByNumber`, then enriches N in a spawned
  task while polling N+1 (pipeline depth 3). TS has no header/enrich split: its
  `PollStream` fetches whole-block strides and truncates at the first not-ready
  block.
- **The enrichment retry is bounded at the head.** A block whose receipts/logs
  stay inconsistent is retried by re-fetching the whole block (header + data) up
  to 10 times at 50 ms, then it returns an error that restarts the ingestion
  session. TS bounds this retry only in backfill (5 × 100 ms, then throw); at the
  head its poll loop retries indefinitely and relies on the switch to backfill
  mode as the backstop. On a head block that cannot be made consistent, Rust
  fails loud and restarts where TS keeps polling. The retry mirrors TS
  `getBlocks` — re-fetching the whole block heals a reorg / load-balanced hash
  mismatch once the canonical header arrives; the first attempt reuses the
  already-fetched header to avoid a redundant call on the ready path.
- **Per-retry logging.** Rust emits a `warn!` on every enrichment retry; TS
  retries silently. A silent retry loop once hid a multi-day head stall.
- **Trace instrumentation is opt-in.** The per-block `sqd:*:trace` lines are
  `tracing` debug/trace events, not always-on info logs.

### Functional gaps

- **Cronos Ethermint phantom transactions are not handled.** TS reconciles
  phantom-tx receipts for the Cronos bug window (`rpc.ts`); the Rust fetch layer
  omits this (see the note atop `crates/evm-source/src/fetch.rs`). Port it before
  running Cronos through this service.

### Fixes to TS behavior

- **`--http-retry-internal-server-errors` is honored.** Rust forwards the flag
  into the RPC client. TS parses it but never forwards it into
  `DataSourceOptions`, so it is a no-op there.
- **Correct welcome text.** `GET /` returns "Welcome to hot block data
  service!"; the TS service shipped copy-pasted "Solana" wording.
