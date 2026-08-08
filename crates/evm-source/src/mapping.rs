/// Final mapping: normalized block → core Block with zstd-compressed JSON line.
/// Mirrors evm-data-service/src/data-source/mapping.ts.
use anyhow::Result;
use bytes::Bytes;
use data_service_core::{Block, BlockTimings};

use crate::normalization::{map_rpc_block, MappingOptions};
use crate::rpc_data::RawRpcBlock;
use crate::types::qty2_u64;

/// Map a raw RPC block to a core Block with zstd-compressed JSON line.
///
/// If `timings_in` is `Some`, the returned `Block` will have its `timings`
/// field populated with `enrich_done` and `normalize_done`/`compress_duration`
/// filled from within this function.
pub fn map_raw_block(
    raw: &RawRpcBlock,
    options: &MappingOptions,
    timings_in: Option<(std::time::Instant, std::time::Instant)>,
) -> Result<Block> {
    let normalized = map_rpc_block(raw, options);

    let normalize_done = timings_in.map(|_| std::time::Instant::now());

    // Serialize straight into the zstd stream: the uncompressed line (up to
    // several MB) never exists as one buffer. Buffered, so serde's small
    // writes don't each cross into the compressor. The zstd frame bytes are
    // identical to the two-pass encode's — the compressor is agnostic to
    // write granularity. Serialization time moves from the normalize bucket
    // to the compress bucket of the block_timing log.
    let compress_start = std::time::Instant::now();
    let mut writer =
        std::io::BufWriter::with_capacity(64 * 1024, zstd::Encoder::new(Vec::new(), 1)?);
    serde_json::to_writer(&mut writer, &normalized)?;
    std::io::Write::write_all(&mut writer, b"\n")?;
    let compressed = writer
        .into_inner()
        .map_err(|e| anyhow::anyhow!("flush into zstd encoder: {e}"))?
        .finish()?;
    let compress_duration = compress_start.elapsed();
    let json_line_zstd = Bytes::from(compressed);

    let number = qty2_u64(&raw.block.number);
    let timestamp = qty2_u64(&raw.block.timestamp) * 1000;

    let timings = timings_in.and_then(|(body_received, enrich_done)| {
        Some(BlockTimings {
            body_received,
            enrich_done,
            normalize_done: normalize_done?,
            compress_duration,
        })
    });

    Ok(Block {
        number,
        hash: raw.block.hash.clone(),
        parent_number: number.saturating_sub(1),
        parent_hash: raw.block.parent_hash.clone(),
        timestamp: Some(timestamp),
        json_line_zstd,
        timings,
    })
}
