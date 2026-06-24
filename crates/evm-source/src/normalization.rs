/// Normalization layer — ports evm-normalization/src/mapping.ts.
/// Converts raw RPC data into the normalized output JSON structure.
use serde::Serialize;
use serde_json::Value;

use crate::rpc_data::{
    AccessListItem, DebugFrame, DebugFrameResult, DebugStateDiffResult, EIP7702AuthorizationItem,
    RawRpcBlock, RpcBlock, RpcLog, RpcReceipt, RpcTransaction, TempoPrimitiveSignature,
    TempoSignatureObject, TraceAction, TraceFrame, TraceResult,
};
use crate::types::{qty2_u64, safe_qty2_u64, to_qty};

// ─── Output types (normalized JSON structures) ────────────────────────────────

/// Normalized block output — matches the TypeScript Block data.ts interface exactly.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedBlock {
    pub header: NormalizedHeader,
    pub transactions: Vec<NormalizedTransaction>,
    pub logs: Vec<NormalizedLog>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traces: Option<Vec<NormalizedTrace>>,
    // TS emits this key as camelCase `stateDiffs`; without rename_all serde
    // would serialize the snake_case field name and silently break consumers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_diffs: Option<Vec<NormalizedStateDiff>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedWithdrawal {
    pub address: String,
    pub amount: String,
    pub index: String,
    pub validator_index: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedHeader {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
    pub transactions_root: String,
    pub receipts_root: String,
    pub state_root: String,
    pub logs_bloom: String,
    pub sha3_uncles: String,
    pub extra_data: String,
    pub miner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mix_hash: Option<String>,
    pub size: u64,
    pub gas_limit: String,
    pub gas_used: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difficulty: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_difficulty: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawals: Option<Vec<NormalizedWithdrawal>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawals_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_gas_used: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excess_blob_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_beacon_block_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_block_number: Option<u64>,
    // Tempo-specific
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main_block_general_gas_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_gas_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_millis_part: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedAccessListItem {
    pub address: String,
    pub storage_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedEIP7702Authorization {
    pub chain_id: String,
    pub address: String,
    /// bigint → decimal string
    pub nonce: String,
    pub y_parity: u64,
    pub r: String,
    pub s: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NormalizedTempoCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    pub value: String,
    pub input: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NormalizedTempoPrimitiveSig {
    #[serde(rename = "secp256k1")]
    Secp256k1 {
        r: String,
        s: String,
        #[serde(skip_serializing_if = "Option::is_none", rename = "yParity")]
        y_parity: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        v: Option<u64>,
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

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum NormalizedTempoSig {
    Keychain(NormalizedTempoKeychainSig),
    Primitive(NormalizedTempoPrimitiveSig),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTempoKeychainSig {
    pub user_address: String,
    pub signature: NormalizedTempoPrimitiveSig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTempoSignedAuth {
    pub chain_id: String,
    pub address: String,
    pub nonce: u64,
    pub signature: NormalizedTempoSig,
}

#[derive(Debug, Clone, Serialize)]
pub struct NormalizedTempoTokenLimit {
    pub token: String,
    pub limit: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTempoKeyAuth {
    pub chain_id: String,
    pub key_type: String,
    pub key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<Vec<NormalizedTempoTokenLimit>>,
    pub signature: NormalizedTempoPrimitiveSig,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedFeePayerSig {
    pub v: u64,
    pub r: String,
    pub s: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTransaction {
    pub transaction_index: u64,
    pub hash: String,
    pub nonce: u64,
    pub from: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub tx_type: Option<u64>,
    pub gas: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_priority_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub y_parity: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_list: Option<Vec<NormalizedAccessListItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_blob_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_versioned_hashes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_list: Option<Vec<NormalizedEIP7702Authorization>>,
    // Tempo 0x76
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calls: Option<Vec<NormalizedTempoCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<NormalizedTempoSig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_payer_signature: Option<NormalizedFeePayerSig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aa_authorization_list: Option<Vec<NormalizedTempoSignedAuth>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_authorization: Option<NormalizedTempoKeyAuth>,
    // Receipt fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cumulative_gas_used: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_gas_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_bloom: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_gas_used: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_gas_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_base_fee_scalar: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_blob_base_fee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_blob_base_fee_scalar: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_fee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_fee_scalar: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_gas_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_gas_used: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedLog {
    pub log_index: u64,
    pub transaction_index: u64,
    pub transaction_hash: String,
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTraceCreateAction {
    pub from: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub gas: String,
    pub init: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_method: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTraceCallAction {
    pub from: String,
    pub to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub gas: String,
    pub input: String,
    pub call_type: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTraceRewardAction {
    pub author: String,
    pub value: String,
    pub reward_type: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTraceSelfdestructAction {
    pub address: String,
    pub refund_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum NormalizedTraceAction {
    Create(NormalizedTraceCreateAction),
    Call(NormalizedTraceCallAction),
    Reward(NormalizedTraceRewardAction),
    Selfdestruct(NormalizedTraceSelfdestructAction),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTraceCreateResult {
    pub gas_used: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTraceCallResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum NormalizedTraceResult {
    Create(NormalizedTraceCreateResult),
    Call(NormalizedTraceCallResult),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTrace {
    pub transaction_index: u64,
    pub trace_address: Vec<u64>,
    #[serde(rename = "type")]
    pub trace_type: String,
    pub subtraces: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revert_reason: Option<String>,
    pub action: NormalizedTraceAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<NormalizedTraceResult>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedStateDiff {
    pub transaction_index: u64,
    pub address: String,
    pub key: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

// ─── Mapping functions ────────────────────────────────────────────────────────

fn map_access_list_item(src: &AccessListItem) -> NormalizedAccessListItem {
    NormalizedAccessListItem {
        address: src.address().to_string(),
        storage_keys: src.storage_keys().to_vec(),
    }
}

fn map_eip7702_auth(src: &EIP7702AuthorizationItem) -> NormalizedEIP7702Authorization {
    match src {
        EIP7702AuthorizationItem::Frontier(a) => NormalizedEIP7702Authorization {
            chain_id: to_qty(a.chain_id),
            address: a.address.clone(),
            nonce: {
                let n = parse_qty_u128_str(&a.nonce);
                n.to_string()
            },
            y_parity: if a.signature.odd_y_parity { 1 } else { 0 },
            r: a.signature.r.clone(),
            s: a.signature.s.clone(),
        },
        EIP7702AuthorizationItem::Standard(a) => NormalizedEIP7702Authorization {
            chain_id: a.chain_id.clone(),
            address: a.address.clone(),
            nonce: {
                let n = parse_qty_u128_str(&a.nonce);
                n.to_string()
            },
            y_parity: qty2_u64(&a.y_parity),
            r: a.r.clone(),
            s: a.s.clone(),
        },
    }
}

fn map_tempo_primitive_sig(src: &TempoPrimitiveSignature) -> NormalizedTempoPrimitiveSig {
    match src {
        TempoPrimitiveSignature::Secp256k1 { r, s, y_parity, v } => {
            NormalizedTempoPrimitiveSig::Secp256k1 {
                r: r.clone(),
                s: s.clone(),
                y_parity: y_parity.as_deref().map(qty2_u64),
                v: v.as_deref().map(qty2_u64),
            }
        }
        TempoPrimitiveSignature::P256 {
            r,
            s,
            pub_key_x,
            pub_key_y,
            pre_hash,
        } => NormalizedTempoPrimitiveSig::P256 {
            r: r.clone(),
            s: s.clone(),
            pub_key_x: pub_key_x.clone(),
            pub_key_y: pub_key_y.clone(),
            pre_hash: *pre_hash,
        },
        TempoPrimitiveSignature::WebAuthn {
            r,
            s,
            pub_key_x,
            pub_key_y,
            webauthn_data,
        } => NormalizedTempoPrimitiveSig::WebAuthn {
            r: r.clone(),
            s: s.clone(),
            pub_key_x: pub_key_x.clone(),
            pub_key_y: pub_key_y.clone(),
            webauthn_data: webauthn_data.clone(),
        },
    }
}

fn map_tempo_sig(src: &TempoSignatureObject) -> NormalizedTempoSig {
    match src {
        TempoSignatureObject::Keychain(k) => {
            NormalizedTempoSig::Keychain(NormalizedTempoKeychainSig {
                user_address: k.user_address.to_lowercase(),
                signature: map_tempo_primitive_sig(&k.signature),
                version: k.version.clone(),
            })
        }
        TempoSignatureObject::Primitive(p) => {
            NormalizedTempoSig::Primitive(map_tempo_primitive_sig(p))
        }
    }
}

pub fn map_transaction(
    src: &RpcTransaction,
    receipt: Option<&RpcReceipt>,
) -> NormalizedTransaction {
    let tx_type = src.tx_type.as_deref().map(qty2_u64);

    NormalizedTransaction {
        transaction_index: qty2_u64(&src.transaction_index),
        hash: src.hash.clone(),
        nonce: qty2_u64(&src.nonce),
        from: src.from.to_lowercase(),
        to: src.to.as_deref().map(|s| s.to_lowercase()),
        input: src.input.clone(),
        value: src.value.clone(),
        tx_type,
        gas: src.gas.clone(),
        gas_price: src.gas_price.clone(),
        max_fee_per_gas: src.max_fee_per_gas.clone(),
        max_priority_fee_per_gas: src.max_priority_fee_per_gas.clone(),
        v: src.v.clone(),
        r: src.r.clone(),
        s: src.s.clone(),
        y_parity: src.y_parity.as_deref().map(qty2_u64),
        access_list: src
            .access_list
            .as_deref()
            .map(|al| al.iter().map(map_access_list_item).collect()),
        chain_id: src.chain_id.as_deref().and_then(safe_qty2_u64),
        max_fee_per_blob_gas: src.max_fee_per_blob_gas.clone(),
        blob_versioned_hashes: src.blob_versioned_hashes.clone(),
        authorization_list: src
            .authorization_list
            .as_deref()
            .map(|al| al.iter().map(map_eip7702_auth).collect()),
        calls: src.calls.as_deref().map(|calls| {
            calls
                .iter()
                .map(|c| NormalizedTempoCall {
                    to: c.to.as_deref().map(|s| s.to_lowercase()),
                    value: c.value.clone(),
                    input: c.input.clone(),
                })
                .collect()
        }),
        nonce_key: src.nonce_key.clone(),
        signature: src.signature.as_ref().map(map_tempo_sig),
        fee_token: src.fee_token.as_deref().map(|s| s.to_lowercase()),
        fee_payer_signature: src
            .fee_payer_signature
            .as_ref()
            .map(|fp| NormalizedFeePayerSig {
                v: qty2_u64(&fp.v),
                r: fp.r.clone(),
                s: fp.s.clone(),
            }),
        valid_before: src.valid_before.clone(),
        valid_after: src.valid_after.clone(),
        aa_authorization_list: src.aa_authorization_list.as_deref().map(|al| {
            al.iter()
                .map(|a| NormalizedTempoSignedAuth {
                    chain_id: a.chain_id.clone(),
                    address: a.address.to_lowercase(),
                    nonce: qty2_u64(&a.nonce),
                    signature: map_tempo_sig(&a.signature),
                })
                .collect()
        }),
        key_authorization: src
            .key_authorization
            .as_ref()
            .map(|ka| NormalizedTempoKeyAuth {
                chain_id: ka.chain_id.clone(),
                key_type: ka.key_type.clone(),
                key_id: ka.key_id.to_lowercase(),
                expiry: ka.expiry.clone(),
                limits: ka.limits.as_deref().map(|lims| {
                    lims.iter()
                        .map(|l| NormalizedTempoTokenLimit {
                            token: l.token.to_lowercase(),
                            limit: l.limit.clone(),
                        })
                        .collect()
                }),
                signature: map_tempo_primitive_sig(&ka.signature),
            }),
        // Receipt fields
        contract_address: receipt
            .and_then(|r| r.contract_address.as_deref())
            .map(|s| s.to_lowercase()),
        cumulative_gas_used: receipt.map(|r| r.cumulative_gas_used.clone()),
        effective_gas_price: receipt.and_then(|r| r.effective_gas_price.clone()),
        gas_used: receipt.map(|r| r.gas_used.clone()),
        logs_bloom: receipt.map(|r| r.logs_bloom.clone()),
        status: receipt.map(|r| qty2_u64(&r.status)),
        blob_gas_used: receipt.and_then(|r| r.blob_gas_used.clone()),
        blob_gas_price: receipt.and_then(|r| r.blob_gas_price.clone()),
        l1_base_fee_scalar: receipt
            .and_then(|r| r.l1_base_fee_scalar.as_deref())
            .map(qty2_u64),
        l1_blob_base_fee: receipt.and_then(|r| r.l1_blob_base_fee.clone()),
        l1_blob_base_fee_scalar: receipt
            .and_then(|r| r.l1_blob_base_fee_scalar.as_deref())
            .map(qty2_u64),
        l1_fee: receipt.and_then(|r| r.l1_fee.clone()),
        l1_fee_scalar: receipt
            .and_then(|r| r.l1_fee_scalar.as_deref())
            .and_then(|s| s.parse::<f64>().ok()),
        l1_gas_price: receipt.and_then(|r| r.l1_gas_price.clone()),
        l1_gas_used: receipt.and_then(|r| r.l1_gas_used.clone()),
    }
}

pub fn map_log(src: &RpcLog) -> NormalizedLog {
    NormalizedLog {
        log_index: qty2_u64(&src.log_index),
        transaction_index: qty2_u64(&src.transaction_index),
        transaction_hash: src.transaction_hash.clone(),
        address: src.address.to_lowercase(),
        data: src.data.clone(),
        topics: src.topics.clone(),
    }
}

fn map_trace_action(src: &TraceAction) -> NormalizedTraceAction {
    match src {
        TraceAction::Create(c) => NormalizedTraceAction::Create(NormalizedTraceCreateAction {
            from: c.from.to_lowercase(),
            value: Some(c.value.clone()),
            gas: c.gas.clone(),
            init: c.init.clone(),
            creation_method: c.creation_method.clone(),
        }),
        TraceAction::Call(c) => NormalizedTraceAction::Call(NormalizedTraceCallAction {
            from: c.from.to_lowercase(),
            to: c.to.to_lowercase(),
            value: Some(c.value.clone()),
            gas: c.gas.clone(),
            input: c.input.clone(),
            call_type: c.call_type.clone(),
        }),
        TraceAction::Reward(r) => NormalizedTraceAction::Reward(NormalizedTraceRewardAction {
            author: r.author.to_lowercase(),
            value: r.value.clone(),
            reward_type: r.reward_type.clone(),
        }),
        TraceAction::Selfdestruct(s) => {
            NormalizedTraceAction::Selfdestruct(NormalizedTraceSelfdestructAction {
                address: s.address.to_lowercase(),
                refund_address: s.refund_address.to_lowercase(),
                balance: Some(s.balance.clone()),
            })
        }
    }
}

fn map_trace_result(src: Option<&TraceResult>) -> Option<NormalizedTraceResult> {
    let src = src?;
    match src {
        TraceResult::Call(c) => Some(NormalizedTraceResult::Call(NormalizedTraceCallResult {
            gas_used: Some(c.gas_used.clone()),
            output: Some(c.output.clone()),
        })),
        TraceResult::Create(c) => {
            Some(NormalizedTraceResult::Create(NormalizedTraceCreateResult {
                gas_used: c.gas_used.clone(),
                code: Some(c.code.clone()),
                address: Some(c.address.to_lowercase()),
            }))
        }
    }
}

fn extract_revert_reason(result: Option<&TraceResult>) -> Option<String> {
    let result = result?;
    let output = match result {
        TraceResult::Call(c) => &c.output,
        TraceResult::Create(_) => return None,
    };
    // ABI-encoded revert reason: keccak4("Error(string)") + offset + length + string
    // Output starts with 0x (offset 136 chars for hex with 0x prefix, or 134 without)
    let output_str = output.strip_prefix("0x").unwrap_or(output);
    // minimum: 4 bytes selector + 32 bytes offset + 32 bytes length = 68 bytes = 136 hex chars
    if output_str.len() < 136 {
        return None;
    }
    // skip 4-byte selector + 32-byte offset = 36 bytes = 72 hex chars
    let after_selector = &output_str[72..];
    // read length (32 bytes = 64 hex chars)
    if after_selector.len() < 64 {
        return None;
    }
    let len = u32::from_str_radix(&after_selector[..64], 16).ok()? as usize;
    let data_hex = &after_selector[64..];
    if data_hex.len() < len * 2 {
        return None;
    }
    let bytes = hex::decode(&data_hex[..len * 2]).ok()?;
    String::from_utf8(bytes).ok()
}

pub fn map_trace_frame(src: &TraceFrame, transaction_index: u64) -> NormalizedTrace {
    let revert_reason = if src.error.is_some() {
        extract_revert_reason(src.result.as_ref())
    } else {
        None
    };

    NormalizedTrace {
        transaction_index,
        trace_address: src.trace_address.clone(),
        trace_type: if src.frame_type == "suicide" {
            "selfdestruct".to_string()
        } else {
            src.frame_type.clone()
        },
        subtraces: src.subtraces,
        error: src.error.clone(),
        revert_reason,
        action: map_trace_action(&src.action),
        result: map_trace_result(src.result.as_ref()),
    }
}

// ─── Debug frame traversal ────────────────────────────────────────────────────

struct DebugFrameItem<'a> {
    trace_address: Vec<u64>,
    subtraces: u64,
    frame: &'a DebugFrame,
}

fn traverse_debug_frame<'a>(
    frame: &'a DebugFrame,
    trace_address: Vec<u64>,
) -> Vec<DebugFrameItem<'a>> {
    let subcalls = frame.calls.as_deref().unwrap_or(&[]);
    let mut result = vec![DebugFrameItem {
        trace_address: trace_address.clone(),
        subtraces: subcalls.len() as u64,
        frame,
    }];
    for (i, sub) in subcalls.iter().enumerate() {
        let mut child_addr = trace_address.clone();
        child_addr.push(i as u64);
        result.extend(traverse_debug_frame(sub, child_addr));
    }
    result
}

pub fn map_debug_frame(
    transaction_index: u64,
    frame_result: &DebugFrameResult,
) -> Vec<NormalizedTrace> {
    // STOP type with no subcalls produces no traces
    let frame_type = frame_result.result.frame_type.as_str();
    if frame_type == "STOP"
        && frame_result
            .result
            .calls
            .as_deref()
            .unwrap_or(&[])
            .is_empty()
    {
        return vec![];
    }

    let mut traces = Vec::new();
    for item in traverse_debug_frame(&frame_result.result, vec![]) {
        let frame = item.frame;
        let base_error = frame.error.clone();
        let base_revert = frame.revert_reason.clone();

        let trace_opt = match frame_type_to_normalized(frame.frame_type.as_str()) {
            "create" => {
                let input = frame.input.clone().unwrap_or_else(|| "0x".to_string());
                let action = NormalizedTraceAction::Create(NormalizedTraceCreateAction {
                    from: frame.from.to_lowercase(),
                    value: frame.value.clone(),
                    gas: frame.gas.clone(),
                    init: input,
                    creation_method: None,
                });

                let result =
                    if frame.gas_used.is_some() || frame.output.is_some() || frame.to.is_some() {
                        Some(NormalizedTraceResult::Create(NormalizedTraceCreateResult {
                            gas_used: frame.gas_used.clone().unwrap_or_default(),
                            code: frame.output.clone(),
                            address: frame.to.as_deref().map(|s| s.to_lowercase()),
                        }))
                    } else {
                        None
                    };

                Some(NormalizedTrace {
                    transaction_index,
                    trace_address: item.trace_address,
                    trace_type: "create".to_string(),
                    subtraces: item.subtraces,
                    error: base_error,
                    revert_reason: base_revert,
                    action,
                    result,
                })
            }
            "call" => {
                let to = match &frame.to {
                    Some(t) => t.to_lowercase(),
                    None => continue,
                };
                let input = frame.input.clone().unwrap_or_else(|| "0x".to_string());
                let call_type = frame.frame_type.to_lowercase();
                let action = NormalizedTraceAction::Call(NormalizedTraceCallAction {
                    from: frame.from.to_lowercase(),
                    to,
                    value: frame.value.clone(),
                    gas: frame.gas.clone(),
                    input,
                    call_type,
                });

                let result = if frame.gas_used.is_some() || frame.output.is_some() {
                    Some(NormalizedTraceResult::Call(NormalizedTraceCallResult {
                        gas_used: frame.gas_used.clone(),
                        output: frame.output.clone(),
                    }))
                } else {
                    None
                };

                Some(NormalizedTrace {
                    transaction_index,
                    trace_address: item.trace_address,
                    trace_type: "call".to_string(),
                    subtraces: item.subtraces,
                    error: base_error,
                    revert_reason: base_revert,
                    action,
                    result,
                })
            }
            "selfdestruct" => {
                let to = match &frame.to {
                    Some(t) => t.to_lowercase(),
                    None => continue,
                };
                let action =
                    NormalizedTraceAction::Selfdestruct(NormalizedTraceSelfdestructAction {
                        address: frame.from.to_lowercase(),
                        refund_address: to,
                        balance: frame.value.clone(),
                    });
                Some(NormalizedTrace {
                    transaction_index,
                    trace_address: item.trace_address,
                    trace_type: "selfdestruct".to_string(),
                    subtraces: item.subtraces,
                    error: base_error,
                    revert_reason: base_revert,
                    action,
                    result: None,
                })
            }
            _ => continue,
        };

        if let Some(t) = trace_opt {
            traces.push(t);
        }
    }
    traces
}

fn frame_type_to_normalized(t: &str) -> &'static str {
    match t.to_uppercase().as_str() {
        "CREATE" | "CREATE2" => "create",
        "CALL" | "CALLCODE" | "DELEGATECALL" | "STATICCALL" | "INVALID" => "call",
        "SELFDESTRUCT" => "selfdestruct",
        _ => "unknown",
    }
}

// ─── State diff normalization ─────────────────────────────────────────────────

pub fn map_debug_state_diff(
    transaction_index: u64,
    diff_result: &DebugStateDiffResult,
) -> Vec<NormalizedStateDiff> {
    let pre = &diff_result.result.pre;
    let post = &diff_result.result.post;
    let mut diffs = Vec::new();

    // Process all addresses in pre
    for (address, pre_val) in pre {
        let empty_map = serde_json::Map::new();
        let post_val = post
            .get(address)
            .and_then(|v| v.as_object())
            .unwrap_or(&empty_map);
        let pre_obj = pre_val.as_object().unwrap_or(&empty_map);
        diffs.extend(map_debug_state_map(
            transaction_index,
            address,
            pre_obj,
            post_val,
        ));
    }

    // Process addresses only in post
    for (address, post_val) in post {
        if !pre.contains_key(address) {
            let empty_map = serde_json::Map::new();
            let post_obj = post_val.as_object().unwrap_or(&empty_map);
            diffs.extend(map_debug_state_map(
                transaction_index,
                address,
                &empty_map,
                post_obj,
            ));
        }
    }

    diffs
}

fn map_debug_state_map(
    transaction_index: u64,
    address: &str,
    prev: &serde_json::Map<String, Value>,
    next: &serde_json::Map<String, Value>,
) -> Vec<NormalizedStateDiff> {
    let mut diffs = Vec::new();

    // code
    if let Some(next_code) = next.get("code").and_then(|v| v.as_str()) {
        let prev_code = prev.get("code").and_then(|v| v.as_str());
        diffs.push(make_debug_state_diff_record(
            transaction_index,
            address,
            "code",
            prev_code,
            Some(next_code),
        ));
    }

    // balance
    if let Some(next_bal) = next.get("balance").and_then(|v| v.as_str()) {
        let prev_bal = prev.get("balance").and_then(|v| v.as_str());
        diffs.push(make_debug_state_diff_record(
            transaction_index,
            address,
            "balance",
            prev_bal,
            Some(next_bal),
        ));
    }

    // nonce
    if let Some(next_nonce) = next.get("nonce") {
        let next_nonce_val = match next_nonce {
            Value::Number(n) => to_qty(n.as_u64().unwrap_or(0)),
            Value::String(s) => s.clone(),
            _ => "0x0".to_string(),
        };
        let prev_nonce_val = match prev.get("nonce") {
            Some(Value::Number(n)) => to_qty(n.as_u64().unwrap_or(0)),
            Some(Value::String(s)) => s.clone(),
            _ => "0x0".to_string(),
        };
        diffs.push(make_debug_state_diff_record(
            transaction_index,
            address,
            "nonce",
            Some(prev_nonce_val.as_str()),
            Some(next_nonce_val.as_str()),
        ));
    }

    // storage
    if let Some(prev_storage) = prev.get("storage").and_then(|v| v.as_object()) {
        for (key, val) in prev_storage {
            let prev_val = val.as_str();
            let next_storage = next.get("storage").and_then(|v| v.as_object());
            let next_val = next_storage
                .and_then(|s| s.get(key))
                .and_then(|v| v.as_str());
            diffs.push(make_debug_state_diff_record(
                transaction_index,
                address,
                key,
                prev_val,
                next_val,
            ));
        }
    }
    if let Some(next_storage) = next.get("storage").and_then(|v| v.as_object()) {
        let prev_storage = prev.get("storage").and_then(|v| v.as_object());
        for (key, val) in next_storage {
            let has_in_prev = prev_storage.is_some_and(|s| s.contains_key(key));
            if !has_in_prev {
                let next_val = val.as_str();
                diffs.push(make_debug_state_diff_record(
                    transaction_index,
                    address,
                    key,
                    None,
                    next_val,
                ));
            }
        }
    }

    diffs
}

fn make_debug_state_diff_record(
    transaction_index: u64,
    address: &str,
    key: &str,
    prev: Option<&str>,
    next: Option<&str>,
) -> NormalizedStateDiff {
    let base = NormalizedStateDiff {
        transaction_index,
        address: address.to_lowercase(),
        key: key.to_string(),
        kind: String::new(),
        prev: None,
        next: None,
    };

    match (prev, next) {
        (None, Some(n)) => NormalizedStateDiff {
            kind: "+".to_string(),
            next: Some(n.to_string()),
            ..base
        },
        (Some(p), None) => NormalizedStateDiff {
            kind: "-".to_string(),
            prev: Some(p.to_string()),
            ..base
        },
        (Some(p), Some(n)) => NormalizedStateDiff {
            kind: "*".to_string(),
            prev: Some(p.to_string()),
            next: Some(n.to_string()),
            ..base
        },
        (None, None) => NormalizedStateDiff {
            kind: "=".to_string(),
            ..base
        },
    }
}

pub fn map_replay_state_diff(
    src: &serde_json::Map<String, Value>,
    transaction_index: u64,
) -> Vec<NormalizedStateDiff> {
    let mut diffs = Vec::new();

    for (address, diffs_val) in src {
        let diffs_obj = match diffs_val.as_object() {
            Some(o) => o,
            None => continue,
        };

        for key in ["code", "balance", "nonce"] {
            if let Some(diff) = diffs_obj.get(key) {
                if let Some(d) = make_state_diff_from_replay(transaction_index, address, key, diff)
                {
                    if d.kind != "=" {
                        diffs.push(d);
                    }
                }
            }
        }

        if let Some(storage) = diffs_obj.get("storage").and_then(|v| v.as_object()) {
            for (slot_key, diff) in storage {
                if let Some(d) =
                    make_state_diff_from_replay(transaction_index, address, slot_key, diff)
                {
                    if d.kind != "=" {
                        diffs.push(d);
                    }
                }
            }
        }
    }

    diffs
}

fn make_state_diff_from_replay(
    transaction_index: u64,
    address: &str,
    key: &str,
    diff: &Value,
) -> Option<NormalizedStateDiff> {
    let base = NormalizedStateDiff {
        transaction_index,
        address: address.to_lowercase(),
        key: key.to_string(),
        kind: String::new(),
        prev: None,
        next: None,
    };

    if diff.as_str() == Some("=") {
        return Some(NormalizedStateDiff {
            kind: "=".to_string(),
            ..base
        });
    }

    if let Some(obj) = diff.as_object() {
        if let Some(plus) = obj.get("+") {
            return Some(NormalizedStateDiff {
                kind: "+".to_string(),
                next: Some(plus.as_str()?.to_string()),
                ..base
            });
        }
        if let Some(change) = obj.get("*") {
            let from = change.get("from")?.as_str()?.to_string();
            let to = change.get("to")?.as_str()?.to_string();
            return Some(NormalizedStateDiff {
                kind: "*".to_string(),
                prev: Some(from),
                next: Some(to),
                ..base
            });
        }
        if let Some(minus) = obj.get("-") {
            return Some(NormalizedStateDiff {
                kind: "-".to_string(),
                prev: Some(minus.as_str()?.to_string()),
                ..base
            });
        }
    }

    None
}

// ─── Block header mapping ─────────────────────────────────────────────────────

pub fn map_block_header(src: &RpcBlock) -> NormalizedHeader {
    NormalizedHeader {
        number: qty2_u64(&src.number),
        hash: src.hash.clone(),
        parent_hash: src.parent_hash.clone(),
        timestamp: qty2_u64(&src.timestamp),
        transactions_root: src.transactions_root.clone(),
        receipts_root: src.receipts_root.clone(),
        state_root: src.state_root.clone(),
        logs_bloom: src.logs_bloom.clone(),
        sha3_uncles: src.sha3_uncles.clone(),
        extra_data: src.extra_data.clone(),
        miner: src.miner.clone(),
        nonce: src.nonce.clone(),
        mix_hash: src.mix_hash.clone(),
        size: qty2_u64(&src.size),
        gas_limit: src.gas_limit.clone(),
        gas_used: src.gas_used.clone(),
        difficulty: src.difficulty.clone(),
        total_difficulty: src.total_difficulty.clone(),
        base_fee_per_gas: src.base_fee_per_gas.clone(),
        uncles: Some(src.uncles.clone()),
        withdrawals: src.withdrawals.as_deref().map(|ws| {
            ws.iter()
                .map(|w| NormalizedWithdrawal {
                    address: w.address.clone(),
                    amount: w.amount.clone(),
                    index: w.index.clone(),
                    validator_index: w.validator_index.clone(),
                })
                .collect()
        }),
        withdrawals_root: src.withdrawals_root.clone(),
        blob_gas_used: src.blob_gas_used.clone(),
        excess_blob_gas: src.excess_blob_gas.clone(),
        parent_beacon_block_root: src.parent_beacon_block_root.clone(),
        requests_hash: src.requests_hash.clone(),
        l1_block_number: src.l1_block_number.as_deref().map(qty2_u64),
        main_block_general_gas_limit: src.main_block_general_gas_limit.clone(),
        shared_gas_limit: src.shared_gas_limit.clone(),
        timestamp_millis_part: src.timestamp_millis_part.clone(),
    }
}

// ─── Main block normalization function ───────────────────────────────────────

pub struct MappingOptions {
    pub with_traces: bool,
    pub with_state_diffs: bool,
}

pub fn map_rpc_block(raw: &RawRpcBlock, options: &MappingOptions) -> NormalizedBlock {
    let block = &raw.block;

    let mut normalized = NormalizedBlock {
        header: map_block_header(block),
        transactions: Vec::new(),
        logs: Vec::new(),
        traces: if options.with_traces {
            Some(Vec::new())
        } else {
            None
        },
        state_diffs: if options.with_state_diffs {
            Some(Vec::new())
        } else {
            None
        },
    };

    // Build tx index: hash → position
    let mut tx_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, tx) in block.transactions.iter().enumerate() {
        tx_index.insert(tx.hash.clone(), i);
    }

    // Map transactions (with optional receipt)
    for (i, tx) in block.transactions.iter().enumerate() {
        let receipt = raw.receipts.as_deref().and_then(|r| r.get(i));
        normalized.transactions.push(map_transaction(tx, receipt));
    }

    // Map logs
    if let Some(logs) = &raw.logs {
        // logs from eth_getLogs
        for log in logs {
            normalized.logs.push(map_log(log));
        }
    } else if let Some(receipts) = &raw.receipts {
        // logs from receipts
        for receipt in receipts {
            for log in &receipt.logs {
                normalized.logs.push(map_log(log));
            }
        }
    }

    // Traces
    if options.with_traces {
        let traces = normalized.traces.as_mut().unwrap();
        if let Some(debug_frames) = &raw.debug_frames {
            for (i, frame_opt) in debug_frames.iter().enumerate() {
                if let Some(frame) = frame_opt {
                    let frame_traces = map_debug_frame(i as u64, frame);
                    traces.extend(frame_traces);
                }
            }
        } else if let Some(replays) = &raw.trace_replays {
            for replay in replays {
                if let Some(tx_hash) = &replay.transaction_hash {
                    if let Some(&tx_idx) = tx_index.get(tx_hash) {
                        if let Some(trace_frames) = &replay.trace {
                            for frame in trace_frames {
                                traces.push(map_trace_frame(frame, tx_idx as u64));
                            }
                        }
                    }
                }
            }
        }
    }

    // State diffs
    if options.with_state_diffs {
        let state_diffs = normalized.state_diffs.as_mut().unwrap();
        if let Some(debug_diffs) = &raw.debug_state_diffs {
            for (i, diff_opt) in debug_diffs.iter().enumerate() {
                if let Some(diff) = diff_opt {
                    state_diffs.extend(map_debug_state_diff(i as u64, diff));
                }
            }
        } else if let Some(replays) = &raw.trace_replays {
            for replay in replays {
                if let Some(tx_hash) = &replay.transaction_hash {
                    if let Some(&tx_idx) = tx_index.get(tx_hash) {
                        if let Some(state_diff) = &replay.state_diff {
                            let diffs = map_replay_state_diff(state_diff, tx_idx as u64);
                            state_diffs.extend(diffs);
                        }
                    }
                }
            }
        }
    }

    normalized
}

fn parse_qty_u128_str(s: &str) -> u128 {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(s, 16).unwrap_or(0)
}
