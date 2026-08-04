use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use data_service_core::service::{run_data_service, DataServiceOptions, RunEnd};
use evm_source::fetch::RpcOptions;
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use rpc_client::{RpcClient, RpcClientConfig};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

enum ShutdownReason {
    Clean,
    Failure(anyhow::Error),
}

fn classify_run_end(end: Result<RunEnd, tokio::sync::oneshot::error::RecvError>) -> ShutdownReason {
    match end {
        Ok(RunEnd::Stopped) => ShutdownReason::Clean,
        Ok(RunEnd::StartupFailure(error)) => {
            ShutdownReason::Failure(error.context("startup failure (FM-31)"))
        }
        Ok(RunEnd::Terminal(error)) => {
            ShutdownReason::Failure(error.context("terminal divergence (FM-30)"))
        }
        Err(error) => ShutdownReason::Failure(
            anyhow::Error::new(error)
                .context("ingestion task vanished before reporting how it ended (FM-32)"),
        ),
    }
}

async fn finish_shutdown<F>(shutdown: F, reason: ShutdownReason) -> anyhow::Result<()>
where
    F: Future<Output = ()>,
{
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, shutdown).await;
    match (drained, reason) {
        (Ok(()), ShutdownReason::Clean) => Ok(()),
        (Ok(()), ShutdownReason::Failure(error)) => Err(error),
        (Err(_), ShutdownReason::Clean) => {
            anyhow::bail!("service drain exceeded the shutdown grace")
        }
        (Err(_), ShutdownReason::Failure(error)) => {
            Err(error.context("service drain also exceeded the shutdown grace"))
        }
    }
}

/// Hot block data service for EVM
#[derive(Parser, Debug)]
#[command(name = "evm-data-service")]
struct Args {
    /// HTTP RPC url
    #[arg(long, value_name = "url")]
    http_rpc: String,

    /// Maximum size of RPC batch call
    #[arg(long, value_name = "number")]
    http_rpc_max_batch_call_size: Option<usize>,

    /// The size of ingestion stride
    #[arg(long, value_name = "number", default_value_t = 5)]
    http_rpc_stride_size: usize,

    /// Max number of concurrent ingestion strides
    #[arg(long, value_name = "number", default_value_t = 5)]
    http_rpc_stride_concurrency: usize,

    /// Maximum RPC rate in requests per second
    #[arg(long, value_name = "rps")]
    http_rpc_rate_limit: Option<f64>,

    /// RPC client request timeout in ms
    #[arg(long, value_name = "ms", default_value_t = 10000)]
    http_rpc_timeout: u64,

    /// If set, the internal server errors from the RPC endpoint will be treated as retryable
    #[arg(long)]
    http_retry_internal_server_errors: bool,

    /// Max number of blocks to buffer
    #[arg(long, value_name = "number", default_value = "1000")]
    block_cache_size: NonZeroUsize,

    /// Port to listen on
    #[arg(short, long, value_name = "number", default_value_t = 3000)]
    port: u16,

    /// Finality offset from the head of a chain
    #[arg(long, value_name = "number")]
    finality_confirmation: Option<u64>,

    /// Fetch transaction receipt data
    #[arg(long)]
    with_receipts: bool,

    /// Fetch EVM call traces
    #[arg(long)]
    with_traces: bool,

    /// Fetch EVM state updates
    #[arg(long)]
    with_statediffs: bool,

    /// Use trace_* API for statediffs and call traces
    #[arg(long)]
    use_trace_api: bool,

    /// Use debug prestateTracer to fetch statediffs (by default will use trace_* api)
    #[arg(long)]
    use_debug_api_for_statediffs: bool,

    /// Use debug_traceBlockByNumber instead of debug_traceBlockByHash
    #[arg(long)]
    use_debug_trace_block_by_number: bool,

    /// Verify block header against block hash
    #[arg(long)]
    verify_block_hash: bool,

    /// Check if transaction sender matches sender recovered from signature
    #[arg(long)]
    verify_tx_sender: bool,

    /// Verify block transactions against transactions root
    #[arg(long)]
    verify_tx_root: bool,

    /// Verify block receipts against receipts root
    #[arg(long)]
    verify_receipts_root: bool,

    /// Verify block withdrawals against withdrawals root
    #[arg(long)]
    verify_withdrawals_root: bool,

    /// Verify block logs against logs bloom
    #[arg(long)]
    verify_logs_bloom: bool,

    /// Do not check log indices within a block are sequential
    #[arg(long)]
    skip_log_index_check: bool,

    /// Do not check cumulativeGasUsed consistency across transactions
    #[arg(long)]
    skip_cumulative_gas_used_check: bool,

    /// Use gasUsed instead of cumulativeGasUsed for receipts root calculation
    #[arg(long)]
    use_gas_used_for_receipts_root: bool,

    /// Automatically adjust finalized head when block cache is full
    /// and finalized head is not in the new range
    #[arg(long)]
    auto_adjust_finalized_head: bool,

    /// Emit per-block pipeline timing logs (target=block_timing) for latency profiling
    #[arg(long)]
    profile_block_timings: bool,
}

fn sqd_log_filter() -> tracing_subscriber::EnvFilter {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        return filter;
    }

    // SQD_<LEVEL> env vars (set to a non-empty value, typically "*").
    // Pick the most verbose level that is set.
    let levels = [
        ("SQD_TRACE", "trace"),
        ("SQD_DEBUG", "debug"),
        ("SQD_INFO", "info"),
        ("SQD_WARN", "warn"),
        ("SQD_ERROR", "error"),
        ("SQD_FATAL", "error"),
    ];
    for (var, level) in &levels {
        if std::env::var(var).map(|v| !v.is_empty()).unwrap_or(false) {
            return tracing_subscriber::EnvFilter::new(*level);
        }
    }

    tracing_subscriber::EnvFilter::new("info")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(sqd_log_filter())
        .json()
        .init();

    let args = Args::parse();

    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: args.http_rpc,
        max_batch_call_size: args.http_rpc_max_batch_call_size,
        capacity: usize::MAX,
        rate_limit: args.http_rpc_rate_limit,
        request_timeout: Duration::from_millis(args.http_rpc_timeout),
        retry_attempts: 5,
        retry_schedule: vec![10, 100, 500, 2000, 10000, 20000]
            .into_iter()
            .map(Duration::from_millis)
            .collect(),
        retry_internal_server_errors: args.http_retry_internal_server_errors,
        ws_pool_size: None,
    }));

    let source = EvmRpcDataSource::new(
        client,
        EvmRpcDataSourceOptions {
            rpc_options: RpcOptions {
                finality_confirmation: args.finality_confirmation,
                verify_block_hash: args.verify_block_hash,
                verify_tx_sender: args.verify_tx_sender,
                verify_tx_root: args.verify_tx_root,
                verify_receipts_root: args.verify_receipts_root,
                verify_withdrawals_root: args.verify_withdrawals_root,
                verify_logs_bloom: args.verify_logs_bloom,
                check_log_index: !args.skip_log_index_check,
                check_cumulative_gas_used: !args.skip_cumulative_gas_used_check,
                use_gas_used_for_receipts_root: args.use_gas_used_for_receipts_root,
            },
            data_request: DataRequest {
                logs: !args.with_receipts,
                receipts: args.with_receipts,
                traces: args.with_traces,
                state_diffs: args.with_statediffs,
                use_trace_api: args.use_trace_api,
                use_debug_api_for_state_diffs: args.use_debug_api_for_statediffs,
                use_debug_trace_block_by_number: args.use_debug_trace_block_by_number,
                debug_trace_timeout: Some("60s".to_string()),
            },
            stride_size: args.http_rpc_stride_size,
            stride_concurrency: args.http_rpc_stride_concurrency,
            profile_block_timings: args.profile_block_timings,
        },
    );

    let mut handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: args.block_cache_size.get(),
        port: args.port,
        auto_adjust_finalized_head: args.auto_adjust_finalized_head,
    })
    .await?;

    tracing::info!("listening on port {}", handle.port);

    // Serve until a signal arrives — but exit non-zero if ingestion dies
    // before its first block (WP-9/FM-31) or ends in terminal divergence
    // (FM-30): the process must never keep serving with ingestion dead.
    // One listener for the whole process lifetime: re-registering per phase
    // leaves a window where tokio's global handler consumes a signal with
    // nobody listening, losing IB-11's second-signal exit.
    let mut signals = {
        use tokio::signal::unix::{signal, SignalKind};
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(4);
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sigint.recv() => {}
                    _ = sigterm.recv() => {}
                }
                if tx.send(()).await.is_err() {
                    return;
                }
            }
        });
        rx
    };

    let shutdown_reason = {
        let mut started_done = false;
        loop {
            tokio::select! {
                _ = signals.recv() => break ShutdownReason::Clean,
                res = &mut handle.started, if !started_done => {
                    started_done = true;
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            break ShutdownReason::Failure(
                                error.context("startup failure (FM-31)")
                            );
                        }
                        Err(error) => {
                            break ShutdownReason::Failure(
                                anyhow::Error::new(error)
                                    .context("startup supervisor vanished (FM-32)")
                            );
                        }
                    }
                }
                end = &mut handle.ended => {
                    break classify_run_end(end);
                }
            }
        }
    };
    tracing::info!("shutting down");

    // Hard-exit on a second signal (either kind) while we drain (IB-11). The
    // channel buffers one that lands before this task starts.
    tokio::spawn(async move {
        if signals.recv().await.is_some() {
            tracing::warn!("second signal — forcing exit");
            std::process::exit(130);
        }
    });

    // Every reason, including terminal failure, goes through the same bounded
    // drain. The original failure is returned only after in-flight responses
    // have had their grace period (ADR-12 / IB-11).
    finish_shutdown(handle.shutdown(), shutdown_reason).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn zero_block_cache_size_is_rejected_by_the_cli() {
        let args = [
            "evm-data-service",
            "--http-rpc",
            "http://localhost:8545",
            "--block-cache-size",
            "0",
        ];
        assert!(Args::try_parse_from(args).is_err());
    }

    #[tokio::test]
    async fn vanished_ingestion_task_is_a_failure() {
        let (tx, rx) = tokio::sync::oneshot::channel::<RunEnd>();
        drop(tx);

        let reason = classify_run_end(rx.await);
        let ShutdownReason::Failure(error) = reason else {
            panic!("a vanished ingestion task must not be a clean stop");
        };
        assert!(error.to_string().contains("ingestion task vanished"));
    }

    #[tokio::test]
    async fn terminal_failure_is_returned_after_drain() {
        let drained = Arc::new(AtomicBool::new(false));
        let drained_by_shutdown = Arc::clone(&drained);
        let reason = classify_run_end(Ok(RunEnd::Terminal(anyhow::anyhow!("diverged"))));

        let error = finish_shutdown(
            async move {
                drained_by_shutdown.store(true, Ordering::SeqCst);
            },
            reason,
        )
        .await
        .expect_err("terminal divergence remains a non-zero result");

        assert!(drained.load(Ordering::SeqCst));
        assert!(error.to_string().contains("terminal divergence"));
    }
}
