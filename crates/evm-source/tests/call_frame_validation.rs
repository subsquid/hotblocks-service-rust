use evm_source::normalization::{map_debug_frame, NormalizedTrace};
use evm_source::rpc_data::{DebugFrame, DebugFrameResult, RpcTransaction};
use evm_source::verification::{check_call_frame_tree, check_debug_frame_structure};
use serde_json::json;

const SENDER: &str = "0xb31fb3fd1b61e571a9709bc59413950e1abc9926";
const CONTRACT: &str = "0xe22a1e72591acb61ec32a9a1d2a1d0818c2f53e0";
const LIBRARY: &str = "0x1111111111111111111111111111111111111111";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

fn call(from: &str, to: &str) -> DebugFrame {
    DebugFrame {
        frame_type: "CALL".to_string(),
        from: from.to_string(),
        to: Some(to.to_string()),
        input: Some("0x".to_string()),
        output: None,
        error: None,
        revert_reason: None,
        value: None,
        gas: "0x5208".to_string(),
        gas_used: None,
        calls: None,
        extra: serde_json::Map::new(),
    }
}

fn transaction(from: &str, to: Option<&str>) -> RpcTransaction {
    serde_json::from_value(json!({
        "blockNumber": "0x1",
        "blockHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
        "hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
        "transactionIndex": "0x0",
        "from": from,
        "to": to,
        "gas": "0x5208",
        "input": "0x",
        "nonce": "0x0"
    }))
    .expect("transaction fixture")
}

fn frame_result(result: DebugFrame) -> DebugFrameResult {
    DebugFrameResult {
        result,
        tx_hash: None,
    }
}

#[test]
fn structural_validation_accepts_every_normalizable_frame_type_casing() {
    let cases = [
        ("CALL", Some("call")),
        ("call", Some("call")),
        ("Call", Some("call")),
        ("CALLCODE", Some("call")),
        ("callcode", Some("call")),
        ("callCode", Some("call")),
        ("DELEGATECALL", Some("call")),
        ("delegatecall", Some("call")),
        ("delegateCall", Some("call")),
        ("STATICCALL", Some("call")),
        ("staticcall", Some("call")),
        ("staticCall", Some("call")),
        ("INVALID", Some("call")),
        ("invalid", Some("call")),
        ("CREATE", Some("create")),
        ("create", Some("create")),
        ("CREATE2", Some("create")),
        ("create2", Some("create")),
        ("SELFDESTRUCT", Some("selfdestruct")),
        ("selfdestruct", Some("selfdestruct")),
        ("STOP", None),
        ("stop", None),
    ];

    for (frame_type, normalized_type) in cases {
        let mut frame = call(SENDER, CONTRACT);
        frame.frame_type = frame_type.to_string();
        if normalized_type == Some("create") {
            frame.gas_used = Some("0x0".to_string());
        } else if normalized_type == Some("selfdestruct") {
            frame.input = None;
        } else if normalized_type.is_none() {
            frame.to = None;
            frame.input = None;
        }

        assert_eq!(
            check_debug_frame_structure(&frame),
            None,
            "{frame_type} must be structurally accepted"
        );

        let traces = map_debug_frame(0, &frame_result(frame));
        match normalized_type {
            Some(expected) => {
                let [NormalizedTrace::Debug(trace)] = traces.as_slice() else {
                    panic!("{frame_type} must map to one debug trace");
                };
                assert_eq!(trace.trace_type, expected, "{frame_type}");
            }
            None => assert!(traces.is_empty(), "{frame_type}"),
        }
    }
}

#[test]
fn failed_create_may_use_empty_result_fields_without_inventing_a_result() {
    let mut frame = call(SENDER, CONTRACT);
    frame.frame_type = "create".to_string();
    frame.to = Some(String::new());
    frame.output = Some(String::new());
    frame.gas_used = Some(String::new());

    assert_eq!(check_debug_frame_structure(&frame), None);
    let traces = map_debug_frame(0, &frame_result(frame));
    let [NormalizedTrace::Debug(trace)] = traces.as_slice() else {
        panic!("failed create must map to one debug trace");
    };
    assert!(trace.result.is_none());
}

#[test]
fn nested_stop_has_no_lossless_normalized_representation() {
    let mut frame = call(SENDER, CONTRACT);
    let mut stop = call(CONTRACT, LIBRARY);
    stop.frame_type = "stop".to_string();
    stop.to = None;
    stop.input = None;
    frame.calls = Some(vec![stop]);

    assert_eq!(
        check_debug_frame_structure(&frame).as_deref(),
        Some("frame 0 has unsupported type stop")
    );
}

#[test]
fn structural_validation_pins_every_normalization_precondition() {
    let intact = call(SENDER, CONTRACT);
    assert_eq!(check_debug_frame_structure(&intact), None);

    let mut missing_target = intact.clone();
    missing_target.to = None;
    assert_eq!(
        check_debug_frame_structure(&missing_target).as_deref(),
        Some("root call frame has no target")
    );

    let mut missing_input = intact.clone();
    missing_input.input = None;
    assert_eq!(
        check_debug_frame_structure(&missing_input).as_deref(),
        Some("root call frame has no input")
    );

    let mut incomplete_create = intact.clone();
    incomplete_create.frame_type = "CREATE".to_string();
    incomplete_create.input = Some("0x6000".to_string());
    incomplete_create.gas_used = None;
    assert_eq!(
        check_debug_frame_structure(&incomplete_create).as_deref(),
        Some("root create frame has a result but no gas used")
    );

    let mut missing_beneficiary = intact.clone();
    missing_beneficiary.calls = Some(vec![DebugFrame {
        frame_type: "SELFDESTRUCT".to_string(),
        from: CONTRACT.to_string(),
        to: None,
        input: None,
        output: None,
        error: None,
        revert_reason: None,
        value: None,
        gas: "0x0".to_string(),
        gas_used: None,
        calls: None,
        extra: serde_json::Map::new(),
    }]);
    assert_eq!(
        check_debug_frame_structure(&missing_beneficiary).as_deref(),
        Some("selfdestruct frame 0 has no beneficiary")
    );

    let mut invalid_address = intact.clone();
    invalid_address.from = "0x1234".to_string();
    assert_eq!(
        check_debug_frame_structure(&invalid_address).as_deref(),
        Some("root frame has invalid from address 0x1234")
    );

    let mut stop = intact;
    stop.frame_type = "STOP".to_string();
    stop.from = "0x".to_string();
    stop.to = None;
    stop.input = None;
    assert_eq!(check_debug_frame_structure(&stop), None);
    stop.calls = Some(vec![call(SENDER, CONTRACT)]);
    assert_eq!(
        check_debug_frame_structure(&stop).as_deref(),
        Some("root STOP frame has subcalls")
    );
}

#[test]
fn semantic_validation_binds_the_root_to_its_transaction() {
    let tx = transaction(SENDER, Some(CONTRACT));
    let intact = call(SENDER, CONTRACT);
    assert_eq!(check_call_frame_tree(&tx, &intact), None);

    let wrong_sender = transaction(CONTRACT, Some(CONTRACT));
    assert_eq!(
        check_call_frame_tree(&wrong_sender, &intact).as_deref(),
        Some(
            "root frame is executed by 0xb31fb3fd1b61e571a9709bc59413950e1abc9926, but the transaction is sent by 0xe22a1e72591acb61ec32a9a1d2a1d0818c2f53e0"
        )
    );

    let create_tx = transaction(SENDER, None);
    assert_eq!(
        check_call_frame_tree(&create_tx, &intact).as_deref(),
        Some("root frame has type CALL, but the transaction creates a contract")
    );
}

#[test]
fn semantic_validation_follows_call_stack_context() {
    let tx = transaction(SENDER, Some(CONTRACT));
    let mut root = call(SENDER, CONTRACT);
    let mut delegate = call(CONTRACT, LIBRARY);
    delegate.frame_type = "DELEGATECALL".to_string();
    delegate.calls = Some(vec![call(CONTRACT, LIBRARY)]);
    root.calls = Some(vec![delegate]);

    assert_eq!(check_call_frame_tree(&tx, &root), None);

    root.calls.as_mut().expect("delegate")[0]
        .calls
        .as_mut()
        .expect("nested")[0]
        .from = ZERO_ADDRESS.to_string();
    assert_eq!(
        check_call_frame_tree(&tx, &root).as_deref(),
        Some(
            "frame 0/0 is executed by 0x0000000000000000000000000000000000000000, but 0xe22a1e72591acb61ec32a9a1d2a1d0818c2f53e0 is on top of the call stack"
        )
    );
}
