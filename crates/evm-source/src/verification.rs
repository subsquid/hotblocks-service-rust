//! Verification routines porting verification.ts.
#![allow(clippy::vec_init_then_push)]
/// Each verification is behind its own flag and returns a Result.
use alloy_primitives::keccak256;
use anyhow::{anyhow, bail};
use sha3::{Digest, Keccak256};

use crate::rpc_data::{RpcBlock, RpcLog, RpcReceipt, RpcTransaction, RpcWithdrawal, AccessListItem, EIP7702AuthorizationItem};

// ─── Helper: decode 0x-hex to bytes ──────────────────────────────────────────

pub fn decode_hex(s: &str) -> anyhow::Result<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    Ok(hex::decode(s)?)
}

fn decode_hex_or_empty(s: &str) -> Vec<u8> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s).unwrap_or_default()
}

fn to_hex(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

// ─── RLP encoding helpers ─────────────────────────────────────────────────────

fn rlp_encode_uint(n: u128) -> Vec<u8> {
    alloy_rlp::encode(alloy_primitives::U256::from(n))
}

/// Encode a big integer (u128) as minimal RLP.
fn rlp_uint(n: u128) -> Vec<u8> {
    alloy_rlp::encode(alloy_primitives::U256::from(n))
}

/// Encode a u64 as minimal RLP integer.
fn rlp_u64(n: u64) -> Vec<u8> {
    alloy_rlp::encode(alloy_primitives::U64::from(n))
}

/// RLP-encode an index (usize) for trie key.
fn rlp_index(idx: usize) -> Vec<u8> {
    alloy_rlp::encode(alloy_primitives::U64::from(idx as u64))
}

// ─── Block hash ───────────────────────────────────────────────────────────────

pub fn block_hash(block: &RpcBlock) -> anyhow::Result<String> {
    let fields = ethereum_header_fields(block)?;
    Ok(hash_block_header_fields(&fields))
}

pub fn tempo_block_hash(block: &RpcBlock) -> anyhow::Result<String> {
    let main_gas_limit = block
        .main_block_general_gas_limit
        .as_deref()
        .ok_or_else(|| anyhow!("block.mainBlockGeneralGasLimit is missing"))?;
    let shared_gas_limit = block
        .shared_gas_limit
        .as_deref()
        .ok_or_else(|| anyhow!("block.sharedGasLimit is missing"))?;
    let ts_millis_part = block
        .timestamp_millis_part
        .as_deref()
        .ok_or_else(|| anyhow!("block.timestampMillisPart is missing"))?;

    // Outer list: [generalGasLimit, sharedGasLimit, timestampMillisPart, innerHeader]
    // where innerHeader is itself an RLP list (nested)
    let inner_fields = ethereum_header_fields(block)?;
    let inner_encoded = rlp_encode_list(&inner_fields);

    let outer: Vec<RlpField> = vec![
        RlpField::Uint(parse_qty_u128(main_gas_limit)),
        RlpField::Uint(parse_qty_u128(shared_gas_limit)),
        RlpField::Uint(parse_qty_u128(ts_millis_part)),
        RlpField::RawList(inner_encoded),
    ];

    Ok(hash_block_header_raw(&outer))
}

// ─── RLP field abstraction ────────────────────────────────────────────────────

enum RlpField {
    Bytes(Vec<u8>),
    Uint(u128),
    // A pre-encoded RLP list embedded as a raw item (used for nested Tempo header)
    RawList(Vec<u8>),
}

fn rlp_encode_list(fields: &[RlpField]) -> Vec<u8> {
    // Build each item, then wrap as a list
    let mut items: Vec<Vec<u8>> = Vec::new();
    for f in fields {
        items.push(encode_rlp_field(f));
    }
    let payload: Vec<u8> = items.into_iter().flatten().collect();
    let mut out = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut out);
    out.extend_from_slice(&payload);
    out
}

fn hash_block_header_fields(fields: &[RlpField]) -> String {
    let encoded = rlp_encode_list(fields);
    let hash = keccak256(&encoded);
    format!("0x{}", hex::encode(hash.as_slice()))
}

fn hash_block_header_raw(fields: &[RlpField]) -> String {
    let encoded = rlp_encode_list(fields);
    let hash = keccak256(&encoded);
    format!("0x{}", hex::encode(hash.as_slice()))
}

fn encode_rlp_field(f: &RlpField) -> Vec<u8> {
    match f {
        RlpField::Bytes(b) => encode_rlp_bytes(b),
        RlpField::Uint(n) => encode_rlp_uint(*n),
        RlpField::RawList(raw) => raw.clone(), // already encoded list
    }
}

fn encode_rlp_bytes(b: &[u8]) -> Vec<u8> {
    if b.len() == 1 && b[0] < 0x80 {
        return vec![b[0]];
    }
    let mut out = Vec::new();
    encode_rlp_length(b.len(), 0x80, &mut out);
    out.extend_from_slice(b);
    out
}

fn encode_rlp_uint(n: u128) -> Vec<u8> {
    if n == 0 {
        return vec![0x80]; // empty string
    }
    // big-endian minimal bytes
    let bytes = n.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(15);
    encode_rlp_bytes(&bytes[start..])
}

fn encode_rlp_length(len: usize, offset: u8, out: &mut Vec<u8>) {
    if len < 56 {
        out.push(offset + len as u8);
    } else {
        let len_bytes = encode_uint_bytes(len);
        out.push(offset + 55 + len_bytes.len() as u8);
        out.extend_from_slice(&len_bytes);
    }
}

fn encode_uint_bytes(n: usize) -> Vec<u8> {
    let bytes = n.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bytes[start..].to_vec()
}

fn parse_qty_u128(s: &str) -> u128 {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(s, 16).unwrap_or(0)
}

fn ethereum_header_fields(block: &RpcBlock) -> anyhow::Result<Vec<RlpField>> {
    let mut fields = vec![
        RlpField::Bytes(decode_hex(&block.parent_hash)?),
        RlpField::Bytes(decode_hex(&block.sha3_uncles)?),
        RlpField::Bytes(decode_hex(&block.miner)?),
        RlpField::Bytes(decode_hex(&block.state_root)?),
        RlpField::Bytes(decode_hex(&block.transactions_root)?),
        RlpField::Bytes(decode_hex(&block.receipts_root)?),
        RlpField::Bytes(decode_hex(&block.logs_bloom)?),
        RlpField::Uint(parse_qty_u128(
            block
                .difficulty
                .as_deref()
                .ok_or_else(|| anyhow!("block.difficulty is missing"))?,
        )),
        RlpField::Uint(parse_qty_u128(&block.number)),
        RlpField::Uint(parse_qty_u128(&block.gas_limit)),
        RlpField::Uint(parse_qty_u128(&block.gas_used)),
        RlpField::Uint(parse_qty_u128(&block.timestamp)),
        RlpField::Bytes(decode_hex(&block.extra_data)?),
        RlpField::Bytes(decode_hex(
            block
                .mix_hash
                .as_deref()
                .ok_or_else(|| anyhow!("block.mixHash is missing"))?,
        )?),
        RlpField::Bytes(decode_hex(
            block
                .nonce
                .as_deref()
                .ok_or_else(|| anyhow!("block.nonce is missing"))?,
        )?),
    ];

    if let Some(base_fee) = &block.base_fee_per_gas {
        fields.push(RlpField::Uint(parse_qty_u128(base_fee)));
    }
    if let Some(wr) = &block.withdrawals_root {
        fields.push(RlpField::Bytes(decode_hex(wr)?));
    }
    if block.blob_gas_used.is_some() && block.excess_blob_gas.is_some() {
        fields.push(RlpField::Uint(parse_qty_u128(
            block.blob_gas_used.as_deref().unwrap(),
        )));
        fields.push(RlpField::Uint(parse_qty_u128(
            block.excess_blob_gas.as_deref().unwrap(),
        )));
    }
    if let Some(pbr) = &block.parent_beacon_block_root {
        fields.push(RlpField::Bytes(decode_hex(pbr)?));
    }
    if let Some(rh) = &block.requests_hash {
        fields.push(RlpField::Bytes(decode_hex(rh)?));
    }

    Ok(fields)
}

// ─── Logs bloom ───────────────────────────────────────────────────────────────

pub fn logs_bloom(logs: &[&RpcLog]) -> String {
    let mut bloom = [0u8; 256];
    for log in logs {
        add_to_bloom(&mut bloom, decode_hex_or_empty(&log.address).as_slice());
        for topic in &log.topics {
            add_to_bloom(&mut bloom, decode_hex_or_empty(topic).as_slice());
        }
    }
    to_hex(&bloom)
}

fn add_to_bloom(bloom: &mut [u8; 256], entry: &[u8]) {
    let mut hasher = Keccak256::new();
    hasher.update(entry);
    let hash = hasher.finalize();
    for idx in [0usize, 2, 4] {
        let bit_to_set = (((hash[idx] as u16) << 8) | hash[idx + 1] as u16) & 0x07FF;
        let bit_index = 0x07FF - bit_to_set as usize;
        let byte_index = bit_index / 8;
        let bit_value = 1u8 << (7 - (bit_index % 8));
        bloom[byte_index] |= bit_value;
    }
}

pub fn is_bloom_superset(superset: &str, subset: &str) -> bool {
    let sup = decode_hex_or_empty(superset);
    let sub = decode_hex_or_empty(subset);
    if sup.len() != 256 || sub.len() != 256 {
        return false;
    }
    for i in 0..256 {
        if (sup[i] & sub[i]) != sub[i] {
            return false;
        }
    }
    true
}

pub fn verify_logs_bloom(block: &RpcBlock, logs: &[&RpcLog]) -> anyhow::Result<()> {
    let computed = logs_bloom(logs);
    if block.logs_bloom != computed {
        bail!(
            "failed to verify logs bloom: expected {} got {}",
            block.logs_bloom,
            computed
        );
    }
    Ok(())
}

// ─── Transactions root ────────────────────────────────────────────────────────

pub fn transactions_root(txs: &[&RpcTransaction]) -> anyhow::Result<String> {
    let trie = alloy_trie::HashBuilder::default();
    let mut keys_values: Vec<(Vec<u8>, Vec<u8>)> = txs
        .iter()
        .enumerate()
        .map(|(idx, tx)| {
            let key = rlp_index(idx);
            let value = encode_transaction(tx).unwrap_or_default();
            (key, value)
        })
        .collect();
    keys_values.sort_by(|a, b| a.0.cmp(&b.0));

    let mut trie = alloy_trie::HashBuilder::default();
    for (k, v) in &keys_values {
        let nibbles = alloy_trie::Nibbles::unpack(k);
        trie.add_leaf(nibbles, v.as_slice());
    }

    let root = trie.root();
    Ok(format!("0x{}", hex::encode(root.as_slice())))
}

pub fn receipts_root(
    receipts: &[&RpcReceipt],
    use_gas_used: bool,
) -> anyhow::Result<String> {
    let mut keys_values: Vec<(Vec<u8>, Vec<u8>)> = receipts
        .iter()
        .enumerate()
        .map(|(idx, r)| {
            let key = rlp_index(idx);
            let value = encode_receipt(r, use_gas_used).unwrap_or_default();
            (key, value)
        })
        .collect();
    keys_values.sort_by(|a, b| a.0.cmp(&b.0));

    let mut trie = alloy_trie::HashBuilder::default();
    for (k, v) in &keys_values {
        let nibbles = alloy_trie::Nibbles::unpack(k);
        trie.add_leaf(nibbles, v.as_slice());
    }

    let root = trie.root();
    Ok(format!("0x{}", hex::encode(root.as_slice())))
}

pub fn withdrawals_root(withdrawals: &[&RpcWithdrawal]) -> anyhow::Result<String> {
    let mut keys_values: Vec<(Vec<u8>, Vec<u8>)> = withdrawals
        .iter()
        .enumerate()
        .map(|(idx, w)| {
            let key = rlp_index(idx);
            let value = encode_withdrawal(w);
            (key, value)
        })
        .collect();
    keys_values.sort_by(|a, b| a.0.cmp(&b.0));

    let mut trie = alloy_trie::HashBuilder::default();
    for (k, v) in &keys_values {
        let nibbles = alloy_trie::Nibbles::unpack(k);
        trie.add_leaf(nibbles, v.as_slice());
    }

    let root = trie.root();
    Ok(format!("0x{}", hex::encode(root.as_slice())))
}

fn encode_withdrawal(w: &RpcWithdrawal) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(&w.index)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&w.validator_index)));
    payload.extend(encode_rlp_bytes(&decode_hex_or_empty(&w.address)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&w.amount)));
    let mut out = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut out);
    out.extend(payload);
    out
}

fn decode_access_list(access_list: &[AccessListItem]) -> Vec<u8> {
    let mut items_encoded = Vec::new();
    for item in access_list {
        let addr = decode_hex_or_empty(item.address());
        let keys: Vec<Vec<u8>> = item
            .storage_keys()
            .iter()
            .map(|k| decode_hex_or_empty(k))
            .collect();

        let mut item_payload = Vec::new();
        item_payload.extend(encode_rlp_bytes(&addr));
        // encode keys as list
        let keys_encoded: Vec<u8> = keys.iter().flat_map(|k| encode_rlp_bytes(k)).collect();
        let mut keys_list = Vec::new();
        encode_rlp_length(keys_encoded.len(), 0xc0, &mut keys_list);
        keys_list.extend(keys_encoded);
        item_payload.extend(keys_list);

        let mut item_out = Vec::new();
        encode_rlp_length(item_payload.len(), 0xc0, &mut item_out);
        item_out.extend(item_payload);
        items_encoded.extend(item_out);
    }
    let mut out = Vec::new();
    encode_rlp_length(items_encoded.len(), 0xc0, &mut out);
    out.extend(items_encoded);
    out
}

fn decode_authorization_list(auth_list: &[EIP7702AuthorizationItem]) -> Vec<u8> {
    let mut items_encoded = Vec::new();
    for item in auth_list {
        let (chain_id, address, nonce, y_parity, r, s) = match item {
            EIP7702AuthorizationItem::Standard(a) => (
                parse_qty_u128(&a.chain_id),
                decode_hex_or_empty(&a.address),
                parse_qty_u128(&a.nonce),
                parse_qty_u128(&a.y_parity),
                decode_hex_or_empty(&a.r),
                decode_hex_or_empty(&a.s),
            ),
            EIP7702AuthorizationItem::Frontier(a) => (
                a.chain_id as u128,
                decode_hex_or_empty(&a.address),
                parse_qty_u128(&a.nonce),
                if a.signature.odd_y_parity { 1u128 } else { 0u128 },
                decode_hex_or_empty(&a.signature.r),
                decode_hex_or_empty(&a.signature.s),
            ),
        };

        let mut item_payload = Vec::new();
        item_payload.extend(encode_rlp_uint(chain_id));
        item_payload.extend(encode_rlp_bytes(&address));
        item_payload.extend(encode_rlp_uint(nonce));
        item_payload.extend(encode_rlp_uint(y_parity));
        // r and s are decoded as big integers
        let r_int = if r.is_empty() {
            0u128
        } else {
            let mut n = 0u128;
            for b in &r {
                n = n << 8 | *b as u128;
            }
            n
        };
        let s_int = if s.is_empty() {
            0u128
        } else {
            let mut n = 0u128;
            for b in &s {
                n = n << 8 | *b as u128;
            }
            n
        };
        item_payload.extend(encode_rlp_uint(r_int));
        item_payload.extend(encode_rlp_uint(s_int));

        let mut item_out = Vec::new();
        encode_rlp_length(item_payload.len(), 0xc0, &mut item_out);
        item_out.extend(item_payload);
        items_encoded.extend(item_out);
    }
    let mut out = Vec::new();
    encode_rlp_length(items_encoded.len(), 0xc0, &mut out);
    out.extend(items_encoded);
    out
}

/// Encode a transaction as per the Ethereum wire format (type-prefix + RLP).
pub fn encode_transaction(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let tx_type = tx.tx_type.as_deref().unwrap_or("0x0");
    match tx_type {
        "0x0" => encode_legacy_tx(tx),
        "0x1" => encode_eip2930_tx(tx),
        "0x2" => encode_eip1559_tx(tx),
        "0x3" => encode_eip4844_tx(tx),
        "0x4" => encode_eip7702_tx(tx),
        // Arbitrum and others — unsupported for root verification (TS also skips sender recovery for these)
        "0x64" | "0x65" | "0x66" | "0x68" | "0x69" | "0x6a" => {
            encode_arbitrum_tx(tx, tx_type)
        }
        // Optimism deposit
        "0x7e" => encode_optimism_deposit_tx(tx),
        // Tempo
        "0x76" => bail!("Tempo 0x76 tx encoding not yet supported for root verification"),
        // Polygon state-sync 0x7f — not used in trie; caller should exclude
        "0x7f" => bail!("Polygon 0x7f state-sync tx should be excluded from tx root"),
        _ => bail!("unsupported tx type: {tx_type}"),
    }
}

fn encode_legacy_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let gas_price = tx
        .gas_price
        .as_deref()
        .ok_or_else(|| anyhow!("tx.gasPrice is missing"))?;
    let value = tx
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("tx.value is missing"))?;
    let input = tx.input.as_deref().unwrap_or("0x");
    let v = tx.v.as_deref().ok_or_else(|| anyhow!("tx.v is missing"))?;
    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let to_bytes = if let Some(to) = &tx.to {
        decode_hex(to)?
    } else {
        vec![]
    };

    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
    payload.extend(encode_rlp_uint(parse_qty_u128(gas_price)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_bytes(&to_bytes));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_bytes(&decode_hex(input)?));

    let v_int = parse_qty_u128(v);
    payload.extend(encode_rlp_uint(v_int));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(r)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(s)));

    let mut out = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut out);
    out.extend(payload);
    Ok(out)
}

fn encode_eip2930_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let chain_id = tx
        .chain_id
        .as_deref()
        .ok_or_else(|| anyhow!("tx.chainId is missing"))?;
    let gas_price = tx
        .gas_price
        .as_deref()
        .ok_or_else(|| anyhow!("tx.gasPrice is missing"))?;
    let value = tx
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("tx.value is missing"))?;
    let input = tx.input.as_deref().unwrap_or("0x");
    let v = tx.v.as_deref().ok_or_else(|| anyhow!("tx.v is missing"))?;
    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let to_bytes = if let Some(to) = &tx.to {
        decode_hex(to)?
    } else {
        vec![]
    };
    let access_list = tx.access_list.as_deref().unwrap_or(&[]);

    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
    payload.extend(encode_rlp_uint(parse_qty_u128(gas_price)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_bytes(&to_bytes));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_bytes(&decode_hex(input)?));
    payload.extend(decode_access_list(access_list));
    payload.extend(encode_rlp_uint(parse_qty_u128(v)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(r)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(s)));

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![0x01];
    out.extend(list);
    Ok(out)
}

fn encode_eip1559_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let chain_id = tx
        .chain_id
        .as_deref()
        .ok_or_else(|| anyhow!("tx.chainId is missing"))?;
    let max_prio = tx
        .max_priority_fee_per_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxPriorityFeePerGas is missing"))?;
    let max_fee = tx
        .max_fee_per_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxFeePerGas is missing"))?;
    let value = tx
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("tx.value is missing"))?;
    let input = tx.input.as_deref().unwrap_or("0x");
    let v = tx.v.as_deref().ok_or_else(|| anyhow!("tx.v is missing"))?;
    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let to_bytes = if let Some(to) = &tx.to {
        decode_hex(to)?
    } else {
        vec![]
    };
    let access_list = tx.access_list.as_deref().unwrap_or(&[]);

    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_bytes(&to_bytes));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_bytes(&decode_hex(input)?));
    payload.extend(decode_access_list(access_list));
    payload.extend(encode_rlp_uint(parse_qty_u128(v)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(r)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(s)));

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![0x02];
    out.extend(list);
    Ok(out)
}

fn encode_eip4844_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let chain_id = tx
        .chain_id
        .as_deref()
        .ok_or_else(|| anyhow!("tx.chainId is missing"))?;
    let max_prio = tx
        .max_priority_fee_per_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxPriorityFeePerGas is missing"))?;
    let max_fee = tx
        .max_fee_per_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxFeePerGas is missing"))?;
    let value = tx
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("tx.value is missing"))?;
    let input = tx.input.as_deref().unwrap_or("0x");
    let max_fee_blob = tx
        .max_fee_per_blob_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxFeePerBlobGas is missing"))?;
    let blob_hashes = tx
        .blob_versioned_hashes
        .as_ref()
        .ok_or_else(|| anyhow!("tx.blobVersionedHashes is missing"))?;

    let y_parity = if let Some(yp) = &tx.y_parity {
        parse_qty_u128(yp)
    } else {
        parse_qty_u128(tx.v.as_deref().ok_or_else(|| anyhow!("tx.v is missing"))?)
    };
    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let to_bytes = if let Some(to) = &tx.to {
        decode_hex(to)?
    } else {
        vec![]
    };
    let access_list = tx.access_list.as_deref().unwrap_or(&[]);

    // blob hashes as list
    let blob_encoded: Vec<u8> = blob_hashes
        .iter()
        .flat_map(|h| encode_rlp_bytes(&decode_hex_or_empty(h)))
        .collect();
    let mut blob_list = Vec::new();
    encode_rlp_length(blob_encoded.len(), 0xc0, &mut blob_list);
    blob_list.extend(blob_encoded);

    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_bytes(&to_bytes));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_bytes(&decode_hex(input)?));
    payload.extend(decode_access_list(access_list));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_fee_blob)));
    payload.extend(blob_list);
    payload.extend(encode_rlp_uint(y_parity));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(r)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(s)));

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![0x03];
    out.extend(list);
    Ok(out)
}

fn encode_eip7702_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let chain_id = tx
        .chain_id
        .as_deref()
        .ok_or_else(|| anyhow!("tx.chainId is missing"))?;
    let max_prio = tx
        .max_priority_fee_per_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxPriorityFeePerGas is missing"))?;
    let max_fee = tx
        .max_fee_per_gas
        .as_deref()
        .ok_or_else(|| anyhow!("tx.maxFeePerGas is missing"))?;
    let value = tx
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("tx.value is missing"))?;
    let input = tx.input.as_deref().unwrap_or("0x");

    let y_parity = if let Some(yp) = &tx.y_parity {
        parse_qty_u128(yp)
    } else {
        parse_qty_u128(tx.v.as_deref().ok_or_else(|| anyhow!("tx.v is missing"))?)
    };
    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let to_bytes = if let Some(to) = &tx.to {
        decode_hex(to)?
    } else {
        vec![]
    };
    let access_list = tx.access_list.as_deref().unwrap_or(&[]);
    let auth_list = tx.authorization_list.as_deref().unwrap_or(&[]);

    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_bytes(&to_bytes));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_bytes(&decode_hex(input)?));
    payload.extend(decode_access_list(access_list));
    payload.extend(decode_authorization_list(auth_list));
    payload.extend(encode_rlp_uint(y_parity));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(r)));
    payload.extend(encode_rlp_bytes(&decode_hex_big_int(s)));

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![0x04];
    out.extend(list);
    Ok(out)
}

fn encode_arbitrum_tx(tx: &RpcTransaction, tx_type: &str) -> anyhow::Result<Vec<u8>> {
    let type_byte = u8::from_str_radix(tx_type.strip_prefix("0x").unwrap_or(tx_type), 16)
        .map_err(|_| anyhow!("invalid tx type: {tx_type}"))?;

    let payload = match tx_type {
        "0x64" => {
            let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
            let request_id = tx.request_id.as_deref().ok_or_else(|| anyhow!("requestId missing"))?;
            let to = tx.to.as_deref().ok_or_else(|| anyhow!("to missing"))?;
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("value missing"))?;

            let mut p = Vec::new();
            p.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            p.extend(encode_rlp_bytes(&decode_hex(request_id)?));
            p.extend(encode_rlp_bytes(&decode_hex_or_empty(&tx.from)));
            p.extend(encode_rlp_bytes(&decode_hex(to)?));
            p.extend(encode_rlp_uint(parse_qty_u128(value)));
            p
        }
        "0x65" => {
            let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().ok_or_else(|| anyhow!("input missing"))?;
            let to_bytes = if let Some(to) = &tx.to { decode_hex(to)? } else { vec![] };
            let gas_price = tx.gas_price.as_deref().unwrap_or("0x0");

            let mut p = Vec::new();
            p.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            p.extend(encode_rlp_bytes(&decode_hex_or_empty(&tx.from)));
            p.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            p.extend(encode_rlp_uint(parse_qty_u128(gas_price)));
            p.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            p.extend(encode_rlp_bytes(&to_bytes));
            p.extend(encode_rlp_uint(parse_qty_u128(value)));
            p.extend(encode_rlp_bytes(&decode_hex(input)?));
            p
        }
        _ => bail!("unsupported Arbitrum tx type: {tx_type}"),
    };

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![type_byte];
    out.extend(list);
    Ok(out)
}

fn encode_optimism_deposit_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let source_hash = tx
        .source_hash
        .as_deref()
        .ok_or_else(|| anyhow!("sourceHash missing"))?;
    let value = tx
        .value
        .as_deref()
        .ok_or_else(|| anyhow!("value missing"))?;
    let input = tx
        .input
        .as_deref()
        .ok_or_else(|| anyhow!("input missing"))?;

    let to_bytes = if let Some(to) = &tx.to {
        decode_hex(to)?
    } else {
        vec![]
    };

    let mut payload = Vec::new();
    payload.extend(encode_rlp_bytes(&decode_hex(source_hash)?));
    payload.extend(encode_rlp_bytes(&decode_hex_or_empty(&tx.from)));
    payload.extend(encode_rlp_bytes(&to_bytes));
    payload.extend(encode_rlp_uint(parse_qty_u128(
        tx.mint.as_deref().unwrap_or("0x0"),
    )));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_uint(0)); // isSystemTx = false
    payload.extend(encode_rlp_bytes(&decode_hex(input)?));

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![0x7e];
    out.extend(list);
    Ok(out)
}

/// Decode a hex big integer to minimal bytes (strips leading zeros).
fn decode_hex_big_int(s: &str) -> Vec<u8> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() || s == "0" {
        return vec![];
    }
    let bytes = hex::decode(s).unwrap_or_default();
    // strip leading zeros
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(0);
    bytes[start..].to_vec()
}

// ─── Receipt encoding ─────────────────────────────────────────────────────────

fn encode_receipt(receipt: &RpcReceipt, use_gas_used: bool) -> anyhow::Result<Vec<u8>> {
    let status = parse_qty_u128(&receipt.status);
    let gas_field = if use_gas_used {
        &receipt.gas_used
    } else {
        &receipt.cumulative_gas_used
    };

    let logs_encoded = encode_logs(&receipt.logs);

    let tx_type = receipt.receipt_type.as_str();

    if tx_type == "0x7e" {
        // Optimism deposit receipt
        let deposit_nonce = receipt
            .deposit_nonce
            .as_deref()
            .ok_or_else(|| anyhow!("receipt.depositNonce is missing for 0x7e receipt"))?;
        let has_receipt_version = receipt.deposit_receipt_version.is_some();

        let mut payload = Vec::new();
        payload.extend(encode_rlp_uint(status));
        payload.extend(encode_rlp_uint(parse_qty_u128(gas_field)));
        payload.extend(encode_rlp_bytes(&decode_hex_or_empty(&receipt.logs_bloom)));
        payload.extend(logs_encoded);
        payload.extend(encode_rlp_uint(parse_qty_u128(deposit_nonce)));
        payload.extend(encode_rlp_uint(if has_receipt_version { 1 } else { 0 }));

        let mut list = Vec::new();
        encode_rlp_length(payload.len(), 0xc0, &mut list);
        list.extend(payload);

        let mut out = vec![0x7e];
        out.extend(list);
        return Ok(out);
    }

    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(status));
    payload.extend(encode_rlp_uint(parse_qty_u128(gas_field)));
    payload.extend(encode_rlp_bytes(&decode_hex_or_empty(&receipt.logs_bloom)));
    payload.extend(logs_encoded);

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    if tx_type == "0x0" {
        return Ok(list);
    }

    let type_byte = u8::from_str_radix(tx_type.strip_prefix("0x").unwrap_or(tx_type), 16)
        .map_err(|_| anyhow!("invalid receipt type: {tx_type}"))?;

    let mut out = vec![type_byte];
    out.extend(list);
    Ok(out)
}

fn encode_logs(logs: &[RpcLog]) -> Vec<u8> {
    let mut items = Vec::new();
    for log in logs {
        let addr = decode_hex_or_empty(&log.address);
        let topics: Vec<Vec<u8>> = log
            .topics
            .iter()
            .map(|t| decode_hex_or_empty(t))
            .collect();
        let data = decode_hex_or_empty(&log.data);

        let topics_encoded: Vec<u8> = topics.iter().flat_map(|t| encode_rlp_bytes(t)).collect();
        let mut topics_list = Vec::new();
        encode_rlp_length(topics_encoded.len(), 0xc0, &mut topics_list);
        topics_list.extend(topics_encoded);

        let mut item_payload = Vec::new();
        item_payload.extend(encode_rlp_bytes(&addr));
        item_payload.extend(topics_list);
        item_payload.extend(encode_rlp_bytes(&data));

        let mut item_out = Vec::new();
        encode_rlp_length(item_payload.len(), 0xc0, &mut item_out);
        item_out.extend(item_payload);
        items.extend(item_out);
    }
    let mut out = Vec::new();
    encode_rlp_length(items.len(), 0xc0, &mut out);
    out.extend(items);
    out
}

// ─── Polygon state-sync tx hash ───────────────────────────────────────────────

pub fn calculate_state_sync_tx_hash(block_num: &str, block_hash: &str) -> String {
    let num = parse_qty_u128(block_num) as u64;
    let num_bytes = num.to_be_bytes();

    let prefix = b"matic-bor-receipt-";
    let hash_bytes = decode_hex_or_empty(block_hash);

    let mut input = Vec::new();
    input.extend_from_slice(prefix);
    input.extend_from_slice(&num_bytes);
    input.extend_from_slice(&hash_bytes);

    let hash = keccak256(&input);
    format!("0x{}", hex::encode(hash.as_slice()))
}

// ─── Transaction sender recovery ─────────────────────────────────────────────

pub fn recover_tx_sender(tx: &RpcTransaction) -> anyhow::Result<Option<String>> {
    let msg = match serialize_transaction_for_signing(tx) {
        Ok(Some(b)) => b,
        Ok(None) => return Ok(None), // unsupported type
        Err(e) => return Err(e),
    };

    let mut hasher = Keccak256::new();
    hasher.update(&msg);
    let msg_hash: [u8; 32] = hasher.finalize().into();

    // For Tempo 0x76, use tx.signature object — not supported here, skip
    let tx_type = tx.tx_type.as_deref().unwrap_or("0x0");
    if tx_type == "0x76" {
        return Ok(None);
    }

    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let r_bytes = decode_hex_big_int(r);
    let s_bytes = decode_hex_big_int(s);

    let mut r32 = [0u8; 32];
    let mut s32 = [0u8; 32];
    let r_start = 32usize.saturating_sub(r_bytes.len());
    let s_start = 32usize.saturating_sub(s_bytes.len());
    r32[r_start..].copy_from_slice(&r_bytes);
    s32[s_start..].copy_from_slice(&s_bytes);

    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(&r32);
    sig[32..].copy_from_slice(&s32);

    let recovery_id = calculate_sig_recovery(tx)?;

    use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
    let sig_obj = Signature::from_bytes((&sig).into()).map_err(|e| anyhow!("sig parse: {e}"))?;
    let rec_id =
        RecoveryId::try_from(recovery_id as u8).map_err(|e| anyhow!("recovery_id: {e}"))?;
    let vk = VerifyingKey::recover_from_prehash(&msg_hash, &sig_obj, rec_id)
        .map_err(|e| anyhow!("recovery failed: {e}"))?;

    let encoded_point = vk.to_encoded_point(false);
    let uncompressed = encoded_point.as_bytes();
    // Skip the 0x04 prefix
    let pub_key_bytes = &uncompressed[1..];

    let mut hasher = Keccak256::new();
    hasher.update(pub_key_bytes);
    let hash = hasher.finalize();
    let addr = &hash[12..];
    Ok(Some(format!("0x{}", hex::encode(addr))))
}

fn calculate_sig_recovery(tx: &RpcTransaction) -> anyhow::Result<u64> {
    let v = tx.v.as_deref().unwrap_or("0x0");
    let v_int = parse_qty_u128(v);

    if v_int == 0 || v_int == 1 {
        return Ok(v_int as u64);
    }
    if v_int == 27 || v_int == 28 {
        return Ok(v_int as u64 - 27);
    }

    // Legacy with EIP-155 replay protection
    let chain_id = tx
        .chain_id
        .as_deref()
        .ok_or_else(|| anyhow!("tx.chainId is missing for EIP-155 recovery"))?;
    let chain_id_int = parse_qty_u128(chain_id);
    let recovery = v_int as u64 - (chain_id_int as u64 * 2 + 35);
    Ok(recovery)
}

fn serialize_transaction_for_signing(tx: &RpcTransaction) -> anyhow::Result<Option<Vec<u8>>> {
    let tx_type = tx.tx_type.as_deref().unwrap_or("0x0");
    match tx_type {
        "0x0" => {
            let gas_price = tx.gas_price.as_deref().unwrap_or("0x0");
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("tx.value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let v = tx.v.as_deref().unwrap_or("0x0");
            let v_int = parse_qty_u128(v);

            let to_bytes = if let Some(to) = &tx.to { decode_hex(to)? } else { vec![] };

            let mut fields = Vec::new();
            fields.push(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            fields.push(encode_rlp_uint(parse_qty_u128(gas_price)));
            fields.push(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            fields.push(encode_rlp_bytes(&to_bytes));
            fields.push(encode_rlp_uint(parse_qty_u128(value)));
            fields.push(encode_rlp_bytes(&decode_hex(input)?));

            // EIP-155
            if v_int != 27 && v_int != 28 && v_int != 0 && v_int != 1 {
                let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
                fields.push(encode_rlp_uint(parse_qty_u128(chain_id)));
                fields.push(encode_rlp_uint(0));
                fields.push(encode_rlp_uint(0));
            }

            let payload: Vec<u8> = fields.into_iter().flatten().collect();
            let mut out = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut out);
            out.extend(payload);
            Ok(Some(out))
        }
        "0x1" => {
            let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
            let gas_price = tx.gas_price.as_deref().unwrap_or("0x0");
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let to_bytes = if let Some(to) = &tx.to { decode_hex(to)? } else { vec![] };
            let access_list = tx.access_list.as_deref().unwrap_or(&[]);

            let mut payload = Vec::new();
            payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            payload.extend(encode_rlp_uint(parse_qty_u128(gas_price)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            payload.extend(encode_rlp_bytes(&to_bytes));
            payload.extend(encode_rlp_uint(parse_qty_u128(value)));
            payload.extend(encode_rlp_bytes(&decode_hex(input)?));
            payload.extend(decode_access_list(access_list));

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x01];
            out.extend(list);
            Ok(Some(out))
        }
        "0x2" => {
            let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx.max_priority_fee_per_gas.as_deref().ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx.max_fee_per_gas.as_deref().ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let to_bytes = if let Some(to) = &tx.to { decode_hex(to)? } else { vec![] };
            let access_list = tx.access_list.as_deref().unwrap_or(&[]);

            let mut payload = Vec::new();
            payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            payload.extend(encode_rlp_bytes(&to_bytes));
            payload.extend(encode_rlp_uint(parse_qty_u128(value)));
            payload.extend(encode_rlp_bytes(&decode_hex(input)?));
            payload.extend(decode_access_list(access_list));

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x02];
            out.extend(list);
            Ok(Some(out))
        }
        "0x3" => {
            let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx.max_priority_fee_per_gas.as_deref().ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx.max_fee_per_gas.as_deref().ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let max_fee_blob = tx.max_fee_per_blob_gas.as_deref().ok_or_else(|| anyhow!("maxFeePerBlobGas missing"))?;
            let blob_hashes = tx.blob_versioned_hashes.as_ref().ok_or_else(|| anyhow!("blobVersionedHashes missing"))?;
            let to_bytes = if let Some(to) = &tx.to { decode_hex(to)? } else { vec![] };
            let access_list = tx.access_list.as_deref().unwrap_or(&[]);

            let blob_encoded: Vec<u8> = blob_hashes
                .iter()
                .flat_map(|h| encode_rlp_bytes(&decode_hex_or_empty(h)))
                .collect();
            let mut blob_list = Vec::new();
            encode_rlp_length(blob_encoded.len(), 0xc0, &mut blob_list);
            blob_list.extend(blob_encoded);

            let mut payload = Vec::new();
            payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            payload.extend(encode_rlp_bytes(&to_bytes));
            payload.extend(encode_rlp_uint(parse_qty_u128(value)));
            payload.extend(encode_rlp_bytes(&decode_hex(input)?));
            payload.extend(decode_access_list(access_list));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_fee_blob)));
            payload.extend(blob_list);

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x03];
            out.extend(list);
            Ok(Some(out))
        }
        "0x4" => {
            let chain_id = tx.chain_id.as_deref().ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx.max_priority_fee_per_gas.as_deref().ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx.max_fee_per_gas.as_deref().ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx.value.as_deref().ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let to_bytes = if let Some(to) = &tx.to { decode_hex(to)? } else { vec![] };
            let access_list = tx.access_list.as_deref().unwrap_or(&[]);
            let auth_list = tx.authorization_list.as_deref().unwrap_or(&[]);

            let mut payload = Vec::new();
            payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
            payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
            payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            payload.extend(encode_rlp_bytes(&to_bytes));
            payload.extend(encode_rlp_uint(parse_qty_u128(value)));
            payload.extend(encode_rlp_bytes(&decode_hex(input)?));
            payload.extend(decode_access_list(access_list));
            payload.extend(decode_authorization_list(auth_list));

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x04];
            out.extend(list);
            Ok(Some(out))
        }
        // Arbitrum, Optimism deposit, Polygon state-sync: no sender recovery
        "0x64" | "0x65" | "0x66" | "0x68" | "0x69" | "0x6a" | "0x7e" | "0x7f" | "0x76" => {
            Ok(None)
        }
        _ => Ok(None),
    }
}
