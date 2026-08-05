//! Verification routines porting verification.ts.
#![allow(clippy::vec_init_then_push)]
/// Each verification is behind its own flag and returns a Result.
use alloy_primitives::keccak256;
use anyhow::{anyhow, bail};
use sha3::{Digest, Keccak256};

use crate::rpc_data::{
    AccessListItem, EIP7702AuthorizationItem, RpcBlock, RpcLog, RpcReceipt, RpcTransaction,
    RpcWithdrawal, TempoCall, TempoCallScope, TempoPrimitiveSignature, TempoSelectorRule,
    TempoSignatureObject, TempoSignedAuthorization, TempoSignedKeyAuthorization, TempoTokenLimit,
};

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

fn parse_qty_u128_checked(s: &str, field: &str) -> anyhow::Result<u128> {
    let digits = s.strip_prefix("0x").unwrap_or(s);
    if digits.is_empty() {
        bail!("{field} is an empty hexadecimal quantity");
    }
    u128::from_str_radix(digits, 16)
        .map_err(|e| anyhow!("{field} is not a valid u128 hexadecimal quantity: {e}"))
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
    let mut keys_values: Vec<(Vec<u8>, Vec<u8>)> = txs
        .iter()
        .enumerate()
        .map(|(idx, tx)| {
            // A transaction that will not encode makes the root unknowable; an
            // empty leaf in its place would compute a root that verifies
            // against nothing and read as a forgery.
            let value = encode_transaction(tx)
                .map_err(|e| anyhow!("transaction {} ({idx}): {e}", tx.hash))?;
            Ok((rlp_index(idx), value))
        })
        .collect::<anyhow::Result<_>>()?;
    keys_values.sort_by(|a, b| a.0.cmp(&b.0));

    let mut trie = alloy_trie::HashBuilder::default();
    for (k, v) in &keys_values {
        let nibbles = alloy_trie::Nibbles::unpack(k);
        trie.add_leaf(nibbles, v.as_slice());
    }

    let root = trie.root();
    Ok(format!("0x{}", hex::encode(root.as_slice())))
}

pub fn receipts_root(receipts: &[&RpcReceipt], use_gas_used: bool) -> anyhow::Result<String> {
    let mut keys_values: Vec<(Vec<u8>, Vec<u8>)> = receipts
        .iter()
        .enumerate()
        .map(|(idx, r)| {
            let value = encode_receipt(r, use_gas_used)
                .map_err(|e| anyhow!("receipt {} ({idx}): {e}", r.transaction_hash))?;
            Ok((rlp_index(idx), value))
        })
        .collect::<anyhow::Result<_>>()?;
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

fn decode_authorization_list(auth_list: &[EIP7702AuthorizationItem]) -> anyhow::Result<Vec<u8>> {
    let mut items_encoded = Vec::new();
    for item in auth_list {
        let (chain_id, address, nonce, y_parity, r, s) = match item {
            EIP7702AuthorizationItem::Standard(a) => (
                parse_qty_u128(&a.chain_id),
                decode_hex(&a.address)?,
                parse_qty_u128(&a.nonce),
                parse_qty_u128(&a.y_parity),
                a.r.as_str(),
                a.s.as_str(),
            ),
            EIP7702AuthorizationItem::Frontier(a) => (
                a.chain_id as u128,
                decode_hex(&a.address)?,
                parse_qty_u128(&a.nonce),
                if a.signature.odd_y_parity {
                    1u128
                } else {
                    0u128
                },
                a.signature.r.as_str(),
                a.signature.s.as_str(),
            ),
        };

        let mut item_payload = Vec::new();
        item_payload.extend(encode_rlp_uint(chain_id));
        item_payload.extend(encode_rlp_bytes(&address));
        item_payload.extend(encode_rlp_uint(nonce));
        item_payload.extend(encode_rlp_uint(y_parity));
        item_payload.extend(encode_rlp_big_uint(r)?);
        item_payload.extend(encode_rlp_big_uint(s)?);

        let mut item_out = Vec::new();
        encode_rlp_length(item_payload.len(), 0xc0, &mut item_out);
        item_out.extend(item_payload);
        items_encoded.extend(item_out);
    }
    let mut out = Vec::new();
    encode_rlp_length(items_encoded.len(), 0xc0, &mut out);
    out.extend(items_encoded);
    Ok(out)
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
        "0x3f" => encode_stable_tx(tx),
        // Arbitrum and others — unsupported for root verification (TS also skips sender recovery for these)
        "0x64" | "0x65" | "0x66" | "0x68" | "0x69" | "0x6a" => encode_arbitrum_tx(tx, tx_type),
        // Optimism deposit
        "0x7e" => encode_optimism_deposit_tx(tx),
        // Tempo native account-abstraction transaction
        "0x76" => encode_tempo_tx(tx),
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
    payload.extend(encode_rlp_big_uint(r)?);
    payload.extend(encode_rlp_big_uint(s)?);

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
    payload.extend(encode_rlp_big_uint(r)?);
    payload.extend(encode_rlp_big_uint(s)?);

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
    payload.extend(encode_rlp_big_uint(r)?);
    payload.extend(encode_rlp_big_uint(s)?);

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
    payload.extend(encode_rlp_big_uint(r)?);
    payload.extend(encode_rlp_big_uint(s)?);

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
    payload.extend(decode_authorization_list(auth_list)?);
    payload.extend(encode_rlp_uint(y_parity));
    payload.extend(encode_rlp_big_uint(r)?);
    payload.extend(encode_rlp_big_uint(s)?);

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);

    let mut out = vec![0x04];
    out.extend(list);
    Ok(out)
}

fn encode_stable_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
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
    let nonce_key = tx
        .nonce_key
        .as_deref()
        .ok_or_else(|| anyhow!("tx.nonceKey is missing"))?;
    let y_parity = tx
        .y_parity
        .as_deref()
        .or(tx.v.as_deref())
        .ok_or_else(|| anyhow!("tx.yParity and tx.v are missing"))?;
    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;
    let to = tx
        .to
        .as_deref()
        .map(decode_hex)
        .transpose()?
        .unwrap_or_default();

    let mut payload = stable_tx_base_payload(tx, chain_id, max_prio, max_fee, value, &to)?;
    payload.extend(encode_rlp_uint(parse_qty_u128(y_parity)));
    payload.extend(encode_rlp_big_uint(r)?);
    payload.extend(encode_rlp_big_uint(s)?);
    payload.extend(encode_rlp_uint(parse_qty_u128(nonce_key)));
    payload.extend(encode_rlp_uint(parse_qty_u128(
        tx.timeout_timestamp.as_deref().unwrap_or("0x0"),
    )));

    let mut list = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut list);
    list.extend(payload);
    let mut out = vec![0x3f];
    out.extend(list);
    Ok(out)
}

fn stable_tx_base_payload(
    tx: &RpcTransaction,
    chain_id: &str,
    max_prio: &str,
    max_fee: &str,
    value: &str,
    to: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_prio)));
    payload.extend(encode_rlp_uint(parse_qty_u128(max_fee)));
    payload.extend(encode_rlp_uint(parse_qty_u128(&tx.gas)));
    payload.extend(encode_rlp_bytes(to));
    payload.extend(encode_rlp_uint(parse_qty_u128(value)));
    payload.extend(encode_rlp_bytes(&decode_hex(
        tx.input.as_deref().unwrap_or("0x"),
    )?));
    payload.extend(decode_access_list(tx.access_list.as_deref().unwrap_or(&[])));
    Ok(payload)
}

fn encode_rlp_items(items: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
    let payload: Vec<u8> = items.into_iter().flatten().collect();
    let mut out = Vec::new();
    encode_rlp_length(payload.len(), 0xc0, &mut out);
    out.extend(payload);
    out
}

fn decode_hex_with_len(s: &str, expected: usize, field: &str) -> anyhow::Result<Vec<u8>> {
    let bytes = decode_hex(s).map_err(|e| anyhow!("{field}: {e}"))?;
    if bytes.len() != expected {
        bail!("{field} must be {expected} bytes, got {}", bytes.len());
    }
    Ok(bytes)
}

fn decode_signature_scalar(s: &str, field: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = decode_hex_big_int(s).map_err(|e| anyhow!("{field}: {e}"))?;
    let mut out = [0u8; 32];
    let start = out
        .len()
        .checked_sub(bytes.len())
        .ok_or_else(|| anyhow!("{field} exceeds 256 bits"))?;
    out[start..].copy_from_slice(&bytes);
    Ok(out)
}

fn encode_tempo_call(call: &TempoCall) -> anyhow::Result<Vec<u8>> {
    let to = call
        .to
        .as_deref()
        .map(decode_hex)
        .transpose()?
        .unwrap_or_default();
    Ok(encode_rlp_items([
        encode_rlp_bytes(&to),
        encode_rlp_big_uint(&call.value)?,
        encode_rlp_bytes(&decode_hex(&call.input)?),
    ]))
}

fn encode_tempo_calls(calls: &[TempoCall]) -> anyhow::Result<Vec<u8>> {
    let items = calls
        .iter()
        .map(encode_tempo_call)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(encode_rlp_items(items))
}

fn encode_tempo_primitive_signature_bytes(
    signature: &TempoPrimitiveSignature,
) -> anyhow::Result<Vec<u8>> {
    match signature {
        TempoPrimitiveSignature::Secp256k1 { r, s, y_parity, v } => {
            let parity = y_parity
                .as_deref()
                .or(v.as_deref())
                .ok_or_else(|| anyhow!("Tempo secp256k1 signature is missing yParity/v"))?;
            let parity = parse_qty_u128_checked(parity, "Tempo signature yParity/v")?;
            if parity > 1 {
                bail!("Tempo signature yParity/v must be 0 or 1");
            }

            let mut out = Vec::with_capacity(65);
            out.extend_from_slice(&decode_signature_scalar(r, "Tempo signature r")?);
            out.extend_from_slice(&decode_signature_scalar(s, "Tempo signature s")?);
            out.push(u8::try_from(parity)? + 27);
            Ok(out)
        }
        TempoPrimitiveSignature::P256 {
            r,
            s,
            pub_key_x,
            pub_key_y,
            pre_hash,
        } => {
            let mut out = Vec::with_capacity(130);
            out.push(0x01);
            out.extend_from_slice(&decode_signature_scalar(r, "Tempo P256 signature r")?);
            out.extend_from_slice(&decode_signature_scalar(s, "Tempo P256 signature s")?);
            out.extend(decode_hex_with_len(pub_key_x, 32, "Tempo P256 pubKeyX")?);
            out.extend(decode_hex_with_len(pub_key_y, 32, "Tempo P256 pubKeyY")?);
            out.push(u8::from(*pre_hash));
            Ok(out)
        }
        TempoPrimitiveSignature::WebAuthn {
            r,
            s,
            pub_key_x,
            pub_key_y,
            webauthn_data,
        } => {
            let data = decode_hex(webauthn_data)?;
            let mut out = Vec::with_capacity(129 + data.len());
            out.push(0x02);
            out.extend(data);
            out.extend_from_slice(&decode_signature_scalar(r, "Tempo WebAuthn signature r")?);
            out.extend_from_slice(&decode_signature_scalar(s, "Tempo WebAuthn signature s")?);
            out.extend(decode_hex_with_len(
                pub_key_x,
                32,
                "Tempo WebAuthn pubKeyX",
            )?);
            out.extend(decode_hex_with_len(
                pub_key_y,
                32,
                "Tempo WebAuthn pubKeyY",
            )?);
            Ok(out)
        }
    }
}

fn encode_tempo_signature_bytes(signature: &TempoSignatureObject) -> anyhow::Result<Vec<u8>> {
    match signature {
        TempoSignatureObject::Primitive(signature) => {
            encode_tempo_primitive_signature_bytes(signature)
        }
        TempoSignatureObject::Keychain(signature) => {
            let mut out = Vec::new();
            out.push(if signature.version.as_deref() == Some("v2") {
                0x04
            } else {
                0x03
            });
            out.extend(decode_hex_with_len(
                &signature.user_address,
                20,
                "Tempo keychain userAddress",
            )?);
            out.extend(encode_tempo_primitive_signature_bytes(
                &signature.signature,
            )?);
            Ok(out)
        }
    }
}

fn encode_tempo_authorization(authorization: &TempoSignedAuthorization) -> anyhow::Result<Vec<u8>> {
    Ok(encode_rlp_items([
        encode_rlp_big_uint(&authorization.chain_id)?,
        encode_rlp_bytes(&decode_hex(&authorization.address)?),
        encode_rlp_big_uint(&authorization.nonce)?,
        encode_rlp_bytes(&encode_tempo_signature_bytes(&authorization.signature)?),
    ]))
}

fn encode_tempo_authorizations(
    authorizations: &[TempoSignedAuthorization],
) -> anyhow::Result<Vec<u8>> {
    let items = authorizations
        .iter()
        .map(encode_tempo_authorization)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(encode_rlp_items(items))
}

fn tempo_signature_type(key_type: &str) -> anyhow::Result<u128> {
    match key_type {
        "secp256k1" => Ok(0),
        "p256" => Ok(1),
        "webAuthn" => Ok(2),
        _ => bail!("unsupported Tempo key type: {key_type}"),
    }
}

fn encode_tempo_selector_rule(rule: &TempoSelectorRule) -> anyhow::Result<Vec<u8>> {
    let recipients = rule
        .recipients
        .iter()
        .map(|recipient| Ok(encode_rlp_bytes(&decode_hex(recipient)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(encode_rlp_items([
        encode_rlp_bytes(&decode_hex(&rule.selector)?),
        encode_rlp_items(recipients),
    ]))
}

fn encode_tempo_call_scope(scope: &TempoCallScope) -> anyhow::Result<Vec<u8>> {
    let rules = scope
        .selector_rules
        .iter()
        .map(encode_tempo_selector_rule)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(encode_rlp_items([
        encode_rlp_bytes(&decode_hex(&scope.target)?),
        encode_rlp_items(rules),
    ]))
}

fn encode_tempo_token_limit(limit: &TempoTokenLimit) -> anyhow::Result<Vec<u8>> {
    let mut fields = vec![
        encode_rlp_bytes(&decode_hex(&limit.token)?),
        encode_rlp_big_uint(&limit.limit)?,
    ];
    if let Some(period) = &limit.period {
        if parse_qty_u128_checked(period, "Tempo token-limit period")? != 0 {
            fields.push(encode_rlp_big_uint(period)?);
        }
    }
    Ok(encode_rlp_items(fields))
}

fn encode_trailing_optional(fields: Vec<Option<Vec<u8>>>) -> Vec<Vec<u8>> {
    let len = fields
        .iter()
        .rposition(Option::is_some)
        .map_or(0, |index| index + 1);
    fields
        .into_iter()
        .take(len)
        .map(|field| field.unwrap_or_else(|| encode_rlp_bytes(&[])))
        .collect()
}

fn encode_tempo_key_authorization(
    authorization: &TempoSignedKeyAuthorization,
) -> anyhow::Result<Vec<u8>> {
    let limits = authorization
        .limits
        .as_ref()
        .map(|limits| {
            limits
                .iter()
                .map(encode_tempo_token_limit)
                .collect::<anyhow::Result<Vec<_>>>()
                .map(encode_rlp_items)
        })
        .transpose()?;
    let allowed_calls = authorization
        .allowed_calls
        .as_ref()
        .map(|scopes| {
            scopes
                .iter()
                .map(encode_tempo_call_scope)
                .collect::<anyhow::Result<Vec<_>>>()
                .map(encode_rlp_items)
        })
        .transpose()?;

    let mut fields = vec![
        encode_rlp_big_uint(&authorization.chain_id)?,
        encode_rlp_uint(tempo_signature_type(&authorization.key_type)?),
        encode_rlp_bytes(&decode_hex(&authorization.key_id)?),
    ];
    fields.extend(encode_trailing_optional(vec![
        authorization
            .expiry
            .as_deref()
            .map(encode_rlp_big_uint)
            .transpose()?,
        limits,
        allowed_calls,
        authorization
            .witness
            .as_deref()
            .map(|witness| decode_hex(witness).map(|bytes| encode_rlp_bytes(&bytes)))
            .transpose()?,
        authorization
            .is_admin
            .is_some_and(|is_admin| is_admin)
            .then(|| encode_rlp_uint(1)),
        authorization
            .account
            .as_deref()
            .map(|account| decode_hex(account).map(|bytes| encode_rlp_bytes(&bytes)))
            .transpose()?,
    ]));

    Ok(encode_rlp_items([
        encode_rlp_items(fields),
        encode_rlp_bytes(&encode_tempo_primitive_signature_bytes(
            &authorization.signature,
        )?),
    ]))
}

fn encode_tempo_transaction_fields(
    tx: &RpcTransaction,
    for_signing: bool,
) -> anyhow::Result<Vec<Vec<u8>>> {
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
    let calls = tx
        .calls
        .as_deref()
        .ok_or_else(|| anyhow!("tx.calls is missing for 0x76 tx"))?;
    let nonce_key = tx
        .nonce_key
        .as_deref()
        .ok_or_else(|| anyhow!("tx.nonceKey is missing for 0x76 tx"))?;
    let has_fee_payer = tx.fee_payer_signature.is_some();

    let fee_token = if for_signing && has_fee_payer {
        encode_rlp_bytes(&[])
    } else {
        tx.fee_token
            .as_deref()
            .map(|token| decode_hex(token).map(|bytes| encode_rlp_bytes(&bytes)))
            .transpose()?
            .unwrap_or_else(|| encode_rlp_bytes(&[]))
    };
    let fee_payer = if for_signing {
        if has_fee_payer {
            encode_rlp_bytes(&[0])
        } else {
            encode_rlp_bytes(&[])
        }
    } else if let Some(signature) = &tx.fee_payer_signature {
        encode_rlp_items([
            encode_rlp_big_uint(&signature.v)?,
            encode_rlp_big_uint(&signature.r)?,
            encode_rlp_big_uint(&signature.s)?,
        ])
    } else {
        encode_rlp_bytes(&[])
    };

    let mut fields = vec![
        encode_rlp_big_uint(chain_id)?,
        encode_rlp_big_uint(max_prio)?,
        encode_rlp_big_uint(max_fee)?,
        encode_rlp_big_uint(&tx.gas)?,
        encode_tempo_calls(calls)?,
        decode_access_list(tx.access_list.as_deref().unwrap_or(&[])),
        encode_rlp_big_uint(nonce_key)?,
        encode_rlp_big_uint(&tx.nonce)?,
        tx.valid_before
            .as_deref()
            .map(encode_rlp_big_uint)
            .transpose()?
            .unwrap_or_else(|| encode_rlp_bytes(&[])),
        tx.valid_after
            .as_deref()
            .map(encode_rlp_big_uint)
            .transpose()?
            .unwrap_or_else(|| encode_rlp_bytes(&[])),
        fee_token,
        fee_payer,
        encode_tempo_authorizations(tx.aa_authorization_list.as_deref().unwrap_or(&[]))?,
    ];
    if let Some(authorization) = &tx.key_authorization {
        fields.push(encode_tempo_key_authorization(authorization)?);
    }
    Ok(fields)
}

fn encode_tempo_tx(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let signature = tx
        .signature
        .as_ref()
        .ok_or_else(|| anyhow!("tx.signature is missing for 0x76 tx"))?;
    let mut fields = encode_tempo_transaction_fields(tx, false)?;
    fields.push(encode_rlp_bytes(&encode_tempo_signature_bytes(signature)?));

    let mut out = vec![0x76];
    out.extend(encode_rlp_items(fields));
    Ok(out)
}

fn encode_tempo_tx_for_signing(tx: &RpcTransaction) -> anyhow::Result<Vec<u8>> {
    let mut out = vec![0x76];
    out.extend(encode_rlp_items(encode_tempo_transaction_fields(tx, true)?));
    Ok(out)
}

fn encode_arbitrum_tx(tx: &RpcTransaction, tx_type: &str) -> anyhow::Result<Vec<u8>> {
    let type_byte = u8::from_str_radix(tx_type.strip_prefix("0x").unwrap_or(tx_type), 16)
        .map_err(|_| anyhow!("invalid tx type: {tx_type}"))?;

    let payload = match tx_type {
        "0x64" => {
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let request_id = tx
                .request_id
                .as_deref()
                .ok_or_else(|| anyhow!("requestId missing"))?;
            let to = tx.to.as_deref().ok_or_else(|| anyhow!("to missing"))?;
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("value missing"))?;

            let mut p = Vec::new();
            p.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
            p.extend(encode_rlp_bytes(&decode_hex(request_id)?));
            p.extend(encode_rlp_bytes(&decode_hex_or_empty(&tx.from)));
            p.extend(encode_rlp_bytes(&decode_hex(to)?));
            p.extend(encode_rlp_uint(parse_qty_u128(value)));
            p
        }
        "0x65" => {
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
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
        // Submit-retryable: https://github.com/OffchainLabs/go-ethereum/blob/7503143/core/types/arb_types.go#L387
        "0x6a" => {
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let input = tx
                .input
                .as_deref()
                .ok_or_else(|| anyhow!("input missing"))?;

            let mut p = Vec::new();
            p.extend(encode_rlp_uint(parse_qty_u128(chain_id)));
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
///
/// RPC quantities are minimal-length, so an odd digit count is ordinary —
/// `hex::decode` rejects it, and signatures whose r or s starts with a zero
/// nibble would decode to nothing.
fn decode_hex_big_int(s: &str) -> anyhow::Result<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() || s == "0" {
        return Ok(vec![]);
    }
    let bytes = if s.len() % 2 == 1 {
        hex::decode(format!("0{s}"))
    } else {
        hex::decode(s)
    }
    .map_err(|e| anyhow!("invalid hexadecimal integer: {e}"))?;
    // strip leading zeros
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    let bytes = bytes[start..].to_vec();
    if bytes.len() > 32 {
        bail!("integer exceeds 256 bits");
    }
    Ok(bytes)
}

fn encode_rlp_big_uint(s: &str) -> anyhow::Result<Vec<u8>> {
    Ok(encode_rlp_bytes(&decode_hex_big_int(s)?))
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
        let topics: Vec<Vec<u8>> = log.topics.iter().map(|t| decode_hex_or_empty(t)).collect();
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

    let tx_type = tx.tx_type.as_deref().unwrap_or("0x0");
    if tx_type == "0x76" {
        let signature = tx
            .signature
            .as_ref()
            .ok_or_else(|| anyhow!("tx.signature is missing for 0x76 tx"))?;
        return recover_tempo_sender(signature, &msg_hash).map(Some);
    }

    let r = tx.r.as_deref().ok_or_else(|| anyhow!("tx.r is missing"))?;
    let s = tx.s.as_deref().ok_or_else(|| anyhow!("tx.s is missing"))?;

    let r = decode_signature_scalar(r, "tx.r")?;
    let s = decode_signature_scalar(s, "tx.s")?;
    let recovery_id = u8::try_from(calculate_sig_recovery(tx)?)?;
    recover_secp256k1_sender(&msg_hash, &r, &s, recovery_id).map(Some)
}

fn recover_secp256k1_sender(
    msg_hash: &[u8; 32],
    r: &[u8; 32],
    s: &[u8; 32],
    recovery_id: u8,
) -> anyhow::Result<String> {
    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(r);
    sig[32..].copy_from_slice(s);

    use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
    let sig_obj = Signature::from_bytes((&sig).into()).map_err(|e| anyhow!("sig parse: {e}"))?;
    let rec_id = RecoveryId::try_from(recovery_id).map_err(|e| anyhow!("recovery_id: {e}"))?;
    let vk = VerifyingKey::recover_from_prehash(msg_hash, &sig_obj, rec_id)
        .map_err(|e| anyhow!("recovery failed: {e}"))?;

    let encoded_point = vk.to_encoded_point(false);
    let uncompressed = encoded_point.as_bytes();
    // Skip the 0x04 prefix
    let pub_key_bytes = &uncompressed[1..];

    let mut hasher = Keccak256::new();
    hasher.update(pub_key_bytes);
    let hash = hasher.finalize();
    let addr = &hash[12..];
    Ok(format!("0x{}", hex::encode(addr)))
}

fn recover_tempo_sender(
    signature: &TempoSignatureObject,
    msg_hash: &[u8; 32],
) -> anyhow::Result<String> {
    match signature {
        TempoSignatureObject::Keychain(signature) => Ok(to_hex(&decode_hex_with_len(
            &signature.user_address,
            20,
            "Tempo keychain userAddress",
        )?)),
        TempoSignatureObject::Primitive(signature) => {
            recover_tempo_primitive_sender(signature, msg_hash)
        }
    }
}

fn recover_tempo_primitive_sender(
    signature: &TempoPrimitiveSignature,
    msg_hash: &[u8; 32],
) -> anyhow::Result<String> {
    match signature {
        TempoPrimitiveSignature::Secp256k1 { r, s, y_parity, v } => {
            let parity = y_parity
                .as_deref()
                .or(v.as_deref())
                .ok_or_else(|| anyhow!("Tempo secp256k1 signature is missing yParity/v"))?;
            let parity = parse_qty_u128_checked(parity, "Tempo signature yParity/v")?;
            if parity > 1 {
                bail!("Tempo signature yParity/v must be 0 or 1");
            }
            recover_secp256k1_sender(
                msg_hash,
                &decode_signature_scalar(r, "Tempo signature r")?,
                &decode_signature_scalar(s, "Tempo signature s")?,
                u8::try_from(parity)?,
            )
        }
        TempoPrimitiveSignature::P256 {
            pub_key_x,
            pub_key_y,
            ..
        }
        | TempoPrimitiveSignature::WebAuthn {
            pub_key_x,
            pub_key_y,
            ..
        } => {
            let mut public_key = decode_hex_with_len(pub_key_x, 32, "Tempo pubKeyX")?;
            public_key.extend(decode_hex_with_len(pub_key_y, 32, "Tempo pubKeyY")?);
            let hash = keccak256(&public_key);
            Ok(to_hex(&hash[12..]))
        }
    }
}

fn calculate_sig_recovery(tx: &RpcTransaction) -> anyhow::Result<u64> {
    let v = tx.v.as_deref().unwrap_or("0x0");
    let v_int = parse_qty_u128_checked(v, "tx.v")?;

    if v_int == 0 || v_int == 1 {
        return u64::try_from(v_int).map_err(|_| anyhow!("tx.v exceeds u64"));
    }
    if v_int == 27 || v_int == 28 {
        return Ok(u64::try_from(v_int).map_err(|_| anyhow!("tx.v exceeds u64"))? - 27);
    }

    // Legacy with EIP-155 replay protection
    let chain_id = tx
        .chain_id
        .as_deref()
        .ok_or_else(|| anyhow!("tx.chainId is missing for EIP-155 recovery"))?;
    let chain_id_int = parse_qty_u128_checked(chain_id, "tx.chainId")?;
    let chain_id = u64::try_from(chain_id_int).map_err(|_| anyhow!("tx.chainId exceeds u64"))?;
    let v = u64::try_from(v_int).map_err(|_| anyhow!("tx.v exceeds u64"))?;
    let eip155_base = chain_id
        .checked_mul(2)
        .and_then(|value| value.checked_add(35))
        .ok_or_else(|| anyhow!("EIP-155 recovery value overflows u64"))?;
    let recovery = v
        .checked_sub(eip155_base)
        .ok_or_else(|| anyhow!("tx.v is invalid for chainId"))?;
    if recovery > 1 {
        bail!("tx.v yields invalid recovery id {recovery}");
    }
    Ok(recovery)
}

fn serialize_transaction_for_signing(tx: &RpcTransaction) -> anyhow::Result<Option<Vec<u8>>> {
    let tx_type = tx.tx_type.as_deref().unwrap_or("0x0");
    match tx_type {
        "0x0" => {
            let gas_price = tx.gas_price.as_deref().unwrap_or("0x0");
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("tx.value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let v = tx.v.as_deref().unwrap_or("0x0");
            let v_int = parse_qty_u128(v);

            let to_bytes = if let Some(to) = &tx.to {
                decode_hex(to)?
            } else {
                vec![]
            };

            let mut fields = Vec::new();
            fields.push(encode_rlp_uint(parse_qty_u128(&tx.nonce)));
            fields.push(encode_rlp_uint(parse_qty_u128(gas_price)));
            fields.push(encode_rlp_uint(parse_qty_u128(&tx.gas)));
            fields.push(encode_rlp_bytes(&to_bytes));
            fields.push(encode_rlp_uint(parse_qty_u128(value)));
            fields.push(encode_rlp_bytes(&decode_hex(input)?));

            // EIP-155
            if v_int != 27 && v_int != 28 && v_int != 0 && v_int != 1 {
                let chain_id = tx
                    .chain_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("chainId missing"))?;
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
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let gas_price = tx.gas_price.as_deref().unwrap_or("0x0");
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
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

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x01];
            out.extend(list);
            Ok(Some(out))
        }
        "0x2" => {
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx
                .max_priority_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx
                .max_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
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

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x02];
            out.extend(list);
            Ok(Some(out))
        }
        "0x3" => {
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx
                .max_priority_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx
                .max_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
            let max_fee_blob = tx
                .max_fee_per_blob_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxFeePerBlobGas missing"))?;
            let blob_hashes = tx
                .blob_versioned_hashes
                .as_ref()
                .ok_or_else(|| anyhow!("blobVersionedHashes missing"))?;
            let to_bytes = if let Some(to) = &tx.to {
                decode_hex(to)?
            } else {
                vec![]
            };
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
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx
                .max_priority_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx
                .max_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("value missing"))?;
            let input = tx.input.as_deref().unwrap_or("0x");
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
            payload.extend(decode_authorization_list(auth_list)?);

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);

            let mut out = vec![0x04];
            out.extend(list);
            Ok(Some(out))
        }
        "0x3f" => {
            let chain_id = tx
                .chain_id
                .as_deref()
                .ok_or_else(|| anyhow!("chainId missing"))?;
            let max_prio = tx
                .max_priority_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxPriorityFeePerGas missing"))?;
            let max_fee = tx
                .max_fee_per_gas
                .as_deref()
                .ok_or_else(|| anyhow!("maxFeePerGas missing"))?;
            let value = tx
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("value missing"))?;
            let nonce_key = tx
                .nonce_key
                .as_deref()
                .ok_or_else(|| anyhow!("nonceKey missing"))?;
            let to = tx
                .to
                .as_deref()
                .map(decode_hex)
                .transpose()?
                .unwrap_or_default();
            let mut payload = stable_tx_base_payload(tx, chain_id, max_prio, max_fee, value, &to)?;
            payload.extend(encode_rlp_uint(parse_qty_u128(nonce_key)));
            payload.extend(encode_rlp_uint(parse_qty_u128(
                tx.timeout_timestamp.as_deref().unwrap_or("0x0"),
            )));

            let mut list = Vec::new();
            encode_rlp_length(payload.len(), 0xc0, &mut list);
            list.extend(payload);
            let mut out = vec![0x3f];
            out.extend(list);
            Ok(Some(out))
        }
        // These transaction families carry no ECDSA sender signature.
        "0x64" | "0x65" | "0x66" | "0x68" | "0x69" | "0x6a" | "0x7e" | "0x7f" => Ok(None),
        // Tempo recovery uses the signature object after hashing this payload.
        "0x76" => encode_tempo_tx_for_signing(tx).map(Some),
        _ => bail!("unsupported tx type for sender recovery: {tx_type}"),
    }
}
