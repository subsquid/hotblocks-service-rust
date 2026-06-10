use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use data_service_core::service::{run_data_service, DataServiceOptions};
use evm_source::fetch::RpcOptions;
use evm_source::source::{EvmRpcDataSource, EvmRpcDataSourceOptions};
use evm_source::types::DataRequest;
use rpc_client::{RpcClient, RpcClientConfig};

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
    #[arg(long, value_name = "number", default_value_t = 1000)]
    block_cache_size: usize,

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
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
        },
    );

    let handle = run_data_service(DataServiceOptions {
        source,
        block_cache_size: args.block_cache_size,
        port: args.port,
        auto_adjust_finalized_head: args.auto_adjust_finalized_head,
    })
    .await?;

    tracing::info!("listening on port {}", handle.port);

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    handle.shutdown().await;
    Ok(())
}
