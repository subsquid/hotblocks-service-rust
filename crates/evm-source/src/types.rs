//! Types shared across the evm-source crate.

/// Data request flags controlling what gets fetched from RPC.
#[derive(Debug, Clone, Default)]
pub struct DataRequest {
    pub logs: bool,
    pub receipts: bool,
    pub traces: bool,
    pub state_diffs: bool,
    pub use_trace_api: bool,
    pub use_debug_api_for_state_diffs: bool,
    pub use_debug_trace_block_by_number: bool,
    pub debug_trace_timeout: Option<String>,
}

/// Convert hex quantity string to u64.
pub fn qty2_u64(qty: &str) -> u64 {
    let s = qty.strip_prefix("0x").unwrap_or(qty);
    u64::from_str_radix(s, 16).unwrap_or(0)
}

/// Convert hex quantity string to u64, returning None if it would overflow a safe JS integer.
pub fn safe_qty2_u64(qty: &str) -> Option<u64> {
    let s = qty.strip_prefix("0x").unwrap_or(qty);
    let v = u64::from_str_radix(s, 16).ok()?;
    // JS safe integer: <= 2^53 - 1
    if v <= 9_007_199_254_740_991 {
        Some(v)
    } else {
        None
    }
}

/// Convert u64 to 0x-prefixed hex string.
pub fn to_qty(n: u64) -> String {
    format!("0x{n:x}")
}

/// Parse a hex quantity as u128 for BigInt fields.
pub fn qty2_u128(qty: &str) -> u128 {
    let s = qty.strip_prefix("0x").unwrap_or(qty);
    u128::from_str_radix(s, 16).unwrap_or(0)
}
