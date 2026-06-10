/// Final mapping: normalized block → core Block with zstd-compressed JSON line.
/// Mirrors evm-data-service/src/data-source/mapping.ts.
use anyhow::Result;
use bytes::Bytes;
use data_service_core::Block;

use crate::normalization::{map_rpc_block, MappingOptions};
use crate::rpc_data::RawRpcBlock;
use crate::types::qty2_u64;

/// Map a raw RPC block to a core Block with zstd-compressed JSON line.
pub fn map_raw_block(raw: &RawRpcBlock, options: &MappingOptions) -> Result<Block> {
    let normalized = map_rpc_block(raw, options);
    let json_line = serde_json::to_string(&normalized)? + "\n";
    let json_line_bytes = json_line.into_bytes();

    let compressed = zstd::encode_all(std::io::Cursor::new(&json_line_bytes), 1)?;
    let json_line_zstd = Bytes::from(compressed);

    let number = qty2_u64(&raw.block.number);
    let timestamp = qty2_u64(&raw.block.timestamp) * 1000;

    Ok(Block {
        number,
        hash: raw.block.hash.clone(),
        parent_number: number.saturating_sub(1),
        parent_hash: raw.block.parent_hash.clone(),
        timestamp: Some(timestamp),
        json_line_zstd,
    })
}
