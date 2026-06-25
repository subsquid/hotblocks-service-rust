/// Raw RPC data types mirroring the TypeScript rpc-data.ts validators.
/// Serde is lenient exactly where TS is lenient.
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── Access lists ─────────────────────────────────────────────────────────────

/// Standard camelCase access list item (EIP-2930).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessItem {
    pub address: String,
    pub storage_keys: Vec<String>,
}

/// Frontier snake_case access list item (Bittensor, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierAccessItem {
    pub address: String,
    pub storage_keys: Vec<String>,
}

/// Unified access list item supporting both camelCase and snake_case.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AccessListItem {
    Standard(AccessItemBoth),
    Frontier(FrontierAccessItem),
}

impl AccessListItem {
    pub fn address(&self) -> &str {
        match self {
            AccessListItem::Standard(a) => &a.address,
            AccessListItem::Frontier(a) => &a.address,
        }
    }
    pub fn storage_keys(&self) -> &[String] {
        match self {
            AccessListItem::Standard(a) => &a.storage_keys,
            AccessListItem::Frontier(a) => &a.storage_keys,
        }
    }
}

/// Handles both `storageKeys` (camelCase) and `storage_keys` (snake_case).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessItemBoth {
    pub address: String,
    #[serde(rename = "storageKeys", alias = "storage_keys")]
    pub storage_keys: Vec<String>,
}

// ─── EIP-7702 authorization ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EIP7702Authorization {
    pub chain_id: String,
    pub address: String,
    pub nonce: String,
    pub y_parity: String,
    pub r: String,
    pub s: String,
}

/// Frontier/Bittensor snake_case + nested signature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierEIP7702Authorization {
    pub chain_id: u64,
    pub address: String,
    pub nonce: String,
    pub signature: FrontierSignature,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierSignature {
    pub odd_y_parity: bool,
    pub r: String,
    pub s: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EIP7702AuthorizationItem {
    Standard(EIP7702Authorization),
    Frontier(FrontierEIP7702Authorization),
}

// ─── Tempo transaction types ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TempoCall {
    pub to: Option<String>,
    pub value: String,
    pub input: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TempoPrimitiveSignature {
    #[serde(rename = "secp256k1")]
    Secp256k1 {
        r: String,
        s: String,
        #[serde(rename = "yParity", skip_serializing_if = "Option::is_none")]
        y_parity: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        v: Option<String>,
    },
    #[serde(rename = "p256")]
    P256 {
        r: String,
        s: String,
        #[serde(rename = "pubKeyX")]
        pub_key_x: String,
        #[serde(rename = "pubKeyY")]
        pub_key_y: String,
        #[serde(rename = "preHash")]
        pre_hash: bool,
    },
    #[serde(rename = "webAuthn")]
    WebAuthn {
        r: String,
        s: String,
        #[serde(rename = "pubKeyX")]
        pub_key_x: String,
        #[serde(rename = "pubKeyY")]
        pub_key_y: String,
        #[serde(rename = "webauthnData")]
        webauthn_data: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoKeychainSignature {
    pub user_address: String,
    pub signature: TempoPrimitiveSignature,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TempoSignatureObject {
    Keychain(TempoKeychainSignature),
    Primitive(TempoPrimitiveSignature),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoSignedAuthorization {
    pub chain_id: String,
    pub address: String,
    pub nonce: String,
    pub signature: TempoSignatureObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TempoTokenLimit {
    pub token: String,
    pub limit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoSignedKeyAuthorization {
    pub chain_id: String,
    pub key_type: String,
    pub key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<Vec<TempoTokenLimit>>,
    pub signature: TempoPrimitiveSignature,
}

// ─── Transaction ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransaction {
    pub block_number: String,
    pub block_hash: String,
    pub hash: String,
    pub transaction_index: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<String>,
    pub from: String,
    /// Optional: Sei omits this for contract deployment
    pub to: Option<String>,
    pub gas: String,
    pub gas_price: Option<String>,
    /// Optional for Tempo 0x76 transactions
    pub input: Option<String>,
    pub max_fee_per_gas: Option<String>,
    pub max_priority_fee_per_gas: Option<String>,
    pub nonce: String,
    pub v: Option<String>,
    pub r: Option<String>,
    pub s: Option<String>,
    #[serde(rename = "type")]
    pub tx_type: Option<String>,
    /// Optional for Tempo 0x76 transactions
    pub value: Option<String>,
    pub y_parity: Option<String>,
    pub access_list: Option<Vec<AccessListItem>>,
    pub max_fee_per_blob_gas: Option<String>,
    pub blob_versioned_hashes: Option<Vec<String>>,
    pub authorization_list: Option<Vec<EIP7702AuthorizationItem>>,
    pub request_id: Option<String>,
    pub ticket_id: Option<String>,
    pub refund_to: Option<String>,
    pub max_refund: Option<String>,
    pub submission_fee_refund: Option<String>,
    pub l1_base_fee: Option<String>,
    pub deposit_value: Option<String>,
    pub retry_to: Option<String>,
    pub retry_value: Option<String>,
    pub beneficiary: Option<String>,
    pub max_submission_fee: Option<String>,
    pub retry_data: Option<String>,
    pub source_hash: Option<String>,
    pub mint: Option<String>,
    // Tempo 0x76 fields
    pub calls: Option<Vec<TempoCall>>,
    pub nonce_key: Option<String>,
    pub signature: Option<TempoSignatureObject>,
    pub fee_token: Option<String>,
    pub fee_payer_signature: Option<FeePayerSignature>,
    pub valid_before: Option<String>,
    pub valid_after: Option<String>,
    pub aa_authorization_list: Option<Vec<TempoSignedAuthorization>>,
    pub key_authorization: Option<TempoSignedKeyAuthorization>,
    // Extra fields not in the strict schema — ignore silently
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeePayerSignature {
    pub v: String,
    pub r: String,
    pub s: String,
}

// ─── Withdrawal ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcWithdrawal {
    pub address: String,
    pub amount: String,
    pub index: String,
    pub validator_index: String,
}

// ─── Log ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    pub block_hash: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub log_index: String,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub removed: bool,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

// ─── Receipt ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcReceipt {
    pub block_hash: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub contract_address: Option<String>,
    pub cumulative_gas_used: String,
    pub from: String,
    pub gas_used: String,
    pub effective_gas_price: Option<String>,
    pub logs: Vec<RpcLog>,
    pub logs_bloom: String,
    pub status: String,
    pub to: Option<String>,
    #[serde(rename = "type")]
    pub receipt_type: String,
    pub blob_gas_used: Option<String>,
    pub blob_gas_price: Option<String>,
    pub l1_fee: Option<String>,
    pub l1_fee_scalar: Option<String>,
    pub l1_base_fee_scalar: Option<String>,
    pub l1_blob_base_fee: Option<String>,
    pub l1_blob_base_fee_scalar: Option<String>,
    pub l1_gas_price: Option<String>,
    pub l1_gas_used: Option<String>,
    pub deposit_nonce: Option<String>,
    pub deposit_receipt_version: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

// ─── Block ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlock {
    pub number: String,
    pub hash: String,
    pub parent_hash: String,
    pub difficulty: Option<String>,
    pub total_difficulty: Option<String>,
    pub excess_blob_gas: Option<String>,
    pub extra_data: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub sha3_uncles: String,
    pub logs_bloom: String,
    pub transactions_root: String,
    pub receipts_root: String,
    pub state_root: String,
    pub miner: String,
    pub mix_hash: Option<String>,
    pub nonce: Option<String>,
    pub base_fee_per_gas: Option<String>,
    pub blob_gas_used: Option<String>,
    pub parent_beacon_block_root: Option<String>,
    pub size: String,
    pub timestamp: String,
    pub transactions: Vec<RpcTransaction>,
    pub uncles: Vec<String>,
    pub withdrawals: Option<Vec<RpcWithdrawal>>,
    pub withdrawals_root: Option<String>,
    pub requests_hash: Option<String>,
    pub l1_block_number: Option<String>,
    // Tempo-specific
    pub main_block_general_gas_limit: Option<String>,
    pub shared_gas_limit: Option<String>,
    pub timestamp_millis_part: Option<String>,
}

// ─── Trace data types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceActionCreate {
    pub from: String,
    pub value: String,
    pub gas: String,
    pub init: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionCall {
    pub call_type: String,
    pub from: String,
    pub gas: String,
    pub input: String,
    pub to: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionReward {
    pub author: String,
    pub reward_type: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionSelfdestruct {
    pub address: String,
    pub balance: String,
    pub refund_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TraceAction {
    Create(TraceActionCreate),
    Call(TraceActionCall),
    Reward(TraceActionReward),
    Selfdestruct(TraceActionSelfdestruct),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResultCreate {
    pub gas_used: String,
    pub code: String,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResultCall {
    pub gas_used: String,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TraceResult {
    Create(TraceResultCreate),
    Call(TraceResultCall),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceFrame {
    pub action: TraceAction,
    pub block_hash: Option<String>,
    pub block_number: Option<u64>,
    pub result: Option<TraceResult>,
    pub subtraces: u64,
    pub trace_address: Vec<u64>,
    pub transaction_hash: Option<String>,
    pub transaction_position: Option<u64>,
    #[serde(rename = "type")]
    pub frame_type: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceTransactionReplay {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    // `trace_replayBlockTransactions` returns these keys camelCase (`stateDiff`,
    // `transactionHash`). Without the rename they silently stayed None, dropping
    // all trace-API statediffs (and breaking the transaction_hash association).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_diff: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<Vec<TraceFrame>>,
    pub transaction_hash: Option<String>,
}

// ─── Debug frames ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugFrame {
    #[serde(rename = "type")]
    pub frame_type: String,
    pub from: String,
    pub to: Option<String>,
    pub input: Option<String>,
    pub output: Option<String>,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    pub value: Option<String>,
    pub gas: String,
    pub gas_used: Option<String>,
    pub calls: Option<Vec<DebugFrame>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugFrameResult {
    pub result: DebugFrame,
    pub tx_hash: Option<String>,
}

/// State diff types for debug_traceBlock with prestateTracer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugStateMap {
    pub balance: Option<String>,
    pub code: Option<String>,
    pub nonce: Option<u64>,
    pub storage: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugStateDiff {
    pub pre: serde_json::Map<String, Value>,
    pub post: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugStateDiffResult {
    pub result: DebugStateDiff,
    pub tx_hash: Option<String>,
}

// ─── RpcBlock with attached data ──────────────────────────────────────────────

fn default_false() -> bool {
    false
}

/// The full raw block as produced by the fetch layer,
/// analogous to TS `Block` type in types.ts.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RawRpcBlock {
    pub number: u64,
    pub hash: String,
    pub block: RpcBlock,
    #[serde(default)]
    pub logs: Option<Vec<RpcLog>>,
    #[serde(default)]
    pub receipts: Option<Vec<RpcReceipt>>,
    #[serde(default, rename = "debugFrames")]
    pub debug_frames: Option<Vec<Option<DebugFrameResult>>>,
    #[serde(default, rename = "traceReplays")]
    pub trace_replays: Option<Vec<TraceTransactionReplay>>,
    #[serde(default, rename = "debugStateDiffs")]
    pub debug_state_diffs: Option<Vec<Option<DebugStateDiffResult>>>,
    #[serde(default = "default_false")]
    pub is_invalid: bool,
    #[serde(default)]
    pub error_message: Option<String>,
}

impl RawRpcBlock {
    pub fn new(number: u64, hash: String, block: RpcBlock) -> Self {
        RawRpcBlock {
            number,
            hash,
            block,
            logs: None,
            receipts: None,
            debug_frames: None,
            trace_replays: None,
            debug_state_diffs: None,
            is_invalid: false,
            error_message: None,
        }
    }

    pub fn mark_invalid(&mut self, msg: impl Into<String>) {
        self.is_invalid = true;
        self.error_message = Some(msg.into());
    }
}
