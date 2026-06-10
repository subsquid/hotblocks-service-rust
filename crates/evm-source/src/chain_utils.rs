/// Chain-specific utils — mirrors chain-utils.ts.

#[derive(Debug, Clone)]
pub struct ChainUtils {
    pub chain_id: u64,
    pub is_polygon_mainnet: bool,
    pub is_hyperliquid_mainnet: bool,
    pub is_hyperliquid_testnet: bool,
    pub is_stable: bool,
    pub is_tempo: bool,
    pub is_cronos_mainnet: bool,
    pub is_shibarium_mainnet: bool,
    pub is_polygon_based: bool,
    pub use_gas_used_for_receipts_root: bool,
}

impl ChainUtils {
    pub fn new(chain_id: u64, use_gas_used_for_receipts_root: bool) -> Self {
        let is_polygon_mainnet = chain_id == 0x89;
        let is_hyperliquid_mainnet = chain_id == 0x3e7;
        let is_hyperliquid_testnet = chain_id == 0x3e6;
        let is_stable = chain_id == 0x3dc || chain_id == 0x899;
        let is_tempo = chain_id == 0x1079 || chain_id == 0xa5bf || chain_id == 0xa5bd;
        let is_cronos_mainnet = chain_id == 0x19;
        let is_shibarium_mainnet = chain_id == 0x6d;
        let is_polygon_based = is_polygon_mainnet || is_shibarium_mainnet;

        ChainUtils {
            chain_id,
            is_polygon_mainnet,
            is_hyperliquid_mainnet,
            is_hyperliquid_testnet,
            is_stable,
            is_tempo,
            is_cronos_mainnet,
            is_shibarium_mainnet,
            is_polygon_based,
            use_gas_used_for_receipts_root,
        }
    }

    pub fn is_hyperliquid(&self) -> bool {
        self.is_hyperliquid_mainnet || self.is_hyperliquid_testnet
    }
}

/// True if a transaction is a Hyperliquid system tx (gasPrice == 0x0).
pub fn is_hyperliquid_system_tx(gas_price: Option<&str>) -> bool {
    gas_price == Some("0x0")
}

/// True if a receipt is a Hyperliquid system receipt (cumulativeGasUsed == 0x0).
pub fn is_hyperliquid_system_receipt(cumulative_gas_used: &str) -> bool {
    cumulative_gas_used == "0x0"
}

/// True if a transaction is a Tempo system tx (type 0x0, r == 0x0, s == 0x0).
pub fn is_tempo_system_tx(tx_type: Option<&str>, r: Option<&str>, s: Option<&str>) -> bool {
    tx_type == Some("0x0") && r == Some("0x0") && s == Some("0x0")
}

/// True if a transaction is a Stable system tx (r == 0x0, s == 0x0).
pub fn is_stable_system_tx(r: Option<&str>, s: Option<&str>) -> bool {
    r == Some("0x0") && s == Some("0x0")
}
