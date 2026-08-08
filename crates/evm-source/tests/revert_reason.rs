//! Byte-compat tests for trace-API revertReason extraction against the
//! predecessor's `extractRevertReason` (evm-normalization mapping.ts). Expected
//! values were produced by running the TS algorithm under Node.
use evm_source::normalization::{map_trace_frame, NormalizedTrace};
use evm_source::rpc_data::{
    TraceAction, TraceActionCall, TraceFrame, TraceResult, TraceResultCall, TraceResultCreate,
};

fn word(n: u64) -> String {
    format!("{n:064x}")
}

fn str_hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

fn pad32(hex: &str) -> String {
    let rem = hex.len() % 64;
    let pad = if rem == 0 { 0 } else { 64 - rem };
    format!("{hex}{}", "0".repeat(pad))
}

/// Standard `Error(string)` ABI encoding: selector + offset + length + data.
fn error_string(reason_hex: &str, byte_len: u64) -> String {
    format!(
        "0x08c379a0{}{}{}",
        word(0x20),
        word(byte_len),
        pad32(reason_hex)
    )
}

fn frame(output: &str, error: Option<&str>, result_is_create: bool) -> TraceFrame {
    TraceFrame {
        action: TraceAction::Call(TraceActionCall {
            call_type: "call".to_string(),
            from: "0x0000000000000000000000000000000000000001".to_string(),
            gas: "0x0".to_string(),
            input: "0x".to_string(),
            to: "0x0000000000000000000000000000000000000002".to_string(),
            value: "0x0".to_string(),
        }),
        block_hash: None,
        block_number: None,
        result: Some(if result_is_create {
            TraceResult::Create(TraceResultCreate {
                gas_used: "0x0".to_string(),
                code: output.to_string(),
                address: "0x0000000000000000000000000000000000000003".to_string(),
            })
        } else {
            TraceResult::Call(TraceResultCall {
                gas_used: "0x0".to_string(),
                output: output.to_string(),
            })
        }),
        subtraces: 0,
        trace_address: vec![],
        transaction_hash: None,
        transaction_position: None,
        frame_type: "call".to_string(),
        error: error.map(str::to_string),
    }
}

fn revert_reason_of(f: &TraceFrame) -> Option<String> {
    match map_trace_frame(f, 0) {
        NormalizedTrace::TraceApi(t) => t.revert_reason,
        NormalizedTrace::Debug(_) => panic!("expected trace-API frame"),
    }
}

fn reason(output: &str) -> Option<String> {
    revert_reason_of(&frame(output, Some("Reverted"), false))
}

#[test]
fn well_formed_error_string() {
    let output = error_string(&str_hex("revert msg"), 10);
    assert_eq!(reason(&output), Some("revert msg".to_string()));
}

#[test]
fn empty_output_yields_empty_reason() {
    assert_eq!(reason("0x"), Some(String::new()));
}

#[test]
fn custom_error_selector_yields_empty_reason() {
    assert_eq!(reason("0xdeadbeef"), Some(String::new()));
}

#[test]
fn panic_yields_empty_reason() {
    let output = format!("0x4e487b71{}", word(0x11));
    assert_eq!(reason(&output), Some(String::new()));
}

#[test]
fn reason_of_256_bytes_truncates_to_empty() {
    // 0x100 mod 256 = 0: the predecessor reads only the low byte of the length word
    let output = error_string(&str_hex(&"A".repeat(256)), 256);
    assert_eq!(reason(&output), Some(String::new()));
}

#[test]
fn reason_of_300_bytes_truncates_mod_256() {
    // 0x12c mod 256 = 44: first 44 bytes of the 300-byte string survive
    let output = error_string(&str_hex(&"0123456789".repeat(30)), 300);
    assert_eq!(
        reason(&output),
        Some("01234567890123456789012345678901234567890123".to_string())
    );
}

#[test]
fn invalid_utf8_is_lossy() {
    let output = error_string("ff6869", 3);
    assert_eq!(reason(&output), Some("\u{FFFD}hi".to_string()));
}

#[test]
fn invalid_utf8_maximal_subpart_replacement() {
    let output = error_string("e228a1", 3);
    assert_eq!(reason(&output), Some("\u{FFFD}(\u{FFFD}".to_string()));
}

#[test]
fn garbage_hex_tail_truncates_node_style() {
    // "zz" stops Node's pairwise hex decode; len byte 10 clamps to what decoded
    let output = format!("0x08c379a0{}{}68656c6czz6f21", word(0x20), word(10));
    assert_eq!(reason(&output), Some("hell".to_string()));
}

#[test]
fn odd_length_hex_tail_ignores_trailing_nibble() {
    let output = format!("0x08c379a0{}{}68656c6c6", word(0x20), word(10));
    assert_eq!(reason(&output), Some("hell".to_string()));
}

#[test]
fn no_error_omits_reason() {
    let output = error_string(&str_hex("revert msg"), 10);
    assert_eq!(revert_reason_of(&frame(&output, None, false)), None);
}

#[test]
fn empty_error_omits_reason() {
    // TS truthiness: `if (frame.error)` treats "" as absent, so the key is
    // omitted even over revert-shaped output.
    let output = error_string(&str_hex("revert msg"), 10);
    assert_eq!(revert_reason_of(&frame(&output, Some(""), false)), None);
    // Control: a non-empty error still extracts.
    assert_eq!(
        revert_reason_of(&frame(&output, Some("Reverted"), false)),
        Some("revert msg".to_string())
    );
}

#[test]
fn create_result_omits_reason() {
    let output = error_string(&str_hex("revert msg"), 10);
    assert_eq!(
        revert_reason_of(&frame(&output, Some("Reverted"), true)),
        None
    );
}
