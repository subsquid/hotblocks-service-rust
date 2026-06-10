/// Parity tests: deserialize fixture raw blocks, normalize, assert JSON output matches expected.
use std::fs;
use std::path::Path;

use evm_source::normalization::{map_rpc_block, MappingOptions};
use evm_source::rpc_data::RawRpcBlock;

#[derive(serde::Deserialize)]
struct Fixture {
    raw: RawRpcBlock,
    #[serde(rename = "normalizedJsonLine")]
    normalized_json_line: String,
}

fn run_fixture_file(path: &Path, options: MappingOptions) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));

    let fixtures: Vec<Fixture> = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {e}", path.display()));

    for (idx, fixture) in fixtures.iter().enumerate() {
        let normalized = map_rpc_block(&fixture.raw, &options);
        let got = serde_json::to_string(&normalized).expect("serialize normalized block");

        // Compare as JSON values (ignore whitespace/formatting differences)
        let got_val: serde_json::Value = serde_json::from_str(&got)
            .unwrap_or_else(|e| panic!("fixture {idx}: got is not valid JSON: {e}\ngot: {got}"));
        let expected_val: serde_json::Value =
            serde_json::from_str(fixture.normalized_json_line.trim_end_matches('\n'))
                .unwrap_or_else(|e| {
                    panic!(
                        "fixture {idx}: expected is not valid JSON: {e}\nexpected: {}",
                        fixture.normalized_json_line
                    )
                });

        if got_val != expected_val {
            // Show diff-friendly output
            let got_pretty = serde_json::to_string_pretty(&got_val).unwrap();
            let expected_pretty = serde_json::to_string_pretty(&expected_val).unwrap();
            panic!(
                "fixture {idx} (block #{block}): JSON mismatch\n--- expected ---\n{expected_pretty}\n--- got ---\n{got_pretty}",
                block = fixture.raw.block.number
            );
        }
    }
}

#[test]
fn parity_base_logs() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/base-logs.json");
    run_fixture_file(
        &path,
        MappingOptions {
            with_traces: false,
            with_state_diffs: false,
        },
    );
}

#[test]
fn parity_base_receipts() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/base-receipts.json");
    run_fixture_file(
        &path,
        MappingOptions {
            with_traces: false,
            with_state_diffs: false,
        },
    );
}

#[test]
fn parity_base_traces() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/base-traces.json");
    run_fixture_file(
        &path,
        MappingOptions {
            with_traces: true,
            with_state_diffs: false,
        },
    );
}
