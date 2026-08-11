//! Provider affinity: a block's components are served by the backend that
//! answered the poll which found it.
//!
//! The mock is a load balancer over a fleet whose members do not share a head.
//! As measured on a production endpoint: a request naming no backend is
//! assigned one and told which, a request naming a live one is served there
//! and told nothing, and receipts are never behind the header on the backend
//! that showed it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use evm_source::fetch::{Rpc, RpcOptions, NOT_READY_DELAY};
use evm_source::rpc_data::{RawRpcBlock, RpcBlock};
use evm_source::types::DataRequest;
use rpc_client::{RpcClient, RpcClientConfig};

// ─── The fleet ────────────────────────────────────────────────────────────────

/// Deliberately not any real provider's: the name is the mock's business.
const COOKIE: &str = "FLEETNODE";

/// One request as the balancer saw it.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    /// The block number the call addressed, where it named one.
    number: Option<u64>,
    /// The backend the request pinned, if any.
    pinned: Option<String>,
    /// Which backend served it.
    served_by: String,
    /// The client port: one TCP connection, one port.
    connection: u16,
}

struct Fleet {
    /// Backend id → the highest block it has imported.
    heads: HashMap<String, u64>,
    /// Assignment order for requests that name no backend.
    rotation: Vec<String>,
    next: usize,
    /// Off = an upstream with no such notion, e.g. an aggregating router.
    names_backends: bool,
    log: Vec<Seen>,
}

impl Fleet {
    fn assign(&mut self) -> String {
        let id = self.rotation[self.next % self.rotation.len()].clone();
        self.next += 1;
        id
    }
}

#[derive(Clone)]
struct FleetState(Arc<Mutex<Fleet>>);

fn block_hash(number: u64) -> String {
    format!("0x{number:064x}")
}

const TX_HASH: &str = "0xdead0000000000000000000000000000000000000000000000000000000000ee";
const ZERO_BLOOM: &str = "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

fn block_json(number: u64) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": block_hash(number),
        "parentHash": block_hash(number.wrapping_sub(1)),
        "difficulty": "0x0",
        "totalDifficulty": "0x0",
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "logsBloom": ZERO_BLOOM,
        "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "miner": "0x0000000000000000000000000000000000000000",
        "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x1",
        "size": "0x220",
        "timestamp": format!("0x{:x}", 1_700_000_000u64 + number * 5),
        "transactions": [{
            "hash": TX_HASH,
            "nonce": "0x0",
            "blockHash": block_hash(number),
            "blockNumber": format!("0x{number:x}"),
            "transactionIndex": "0x0",
            "from": "0x0000000000000000000000000000000000000001",
            "to": "0x0000000000000000000000000000000000000002",
            "value": "0x0",
            "gas": "0x5208",
            "gasPrice": "0x1",
            "input": "0x",
            "type": "0x2",
            "chainId": "0x1",
            "maxFeePerGas": "0x1",
            "maxPriorityFeePerGas": "0x1",
            "accessList": [],
            "v": "0x0",
            "r": "0x0",
            "s": "0x0"
        }],
        "uncles": [],
        "withdrawals": []
    })
}

fn receipt_json(number: u64) -> Value {
    json!({
        "blockHash": block_hash(number),
        "blockNumber": format!("0x{number:x}"),
        "transactionHash": TX_HASH,
        "transactionIndex": "0x0",
        "contractAddress": null,
        "cumulativeGasUsed": "0x5208",
        "from": "0x0000000000000000000000000000000000000001",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "logs": [],
        "logsBloom": ZERO_BLOOM,
        "status": "0x1",
        "to": "0x0000000000000000000000000000000000000002",
        "type": "0x2"
    })
}

fn requested_number(params: &Value) -> Option<u64> {
    let tag = params.get(0)?.as_str()?;
    u64::from_str_radix(tag.strip_prefix("0x")?, 16).ok()
}

/// Chain id, head tag, capability probe. These must not consume the rotation,
/// or a scripted fleet would depend on how many probes the client issued.
fn addresses_no_block(call: &Value) -> bool {
    let method = call
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let tag = call
        .get("params")
        .and_then(|p| p.get(0))
        .and_then(Value::as_str);
    matches!(method, "eth_chainId" | "eth_blockNumber")
        || matches!(tag, Some("latest") | Some("finalized"))
}

async fn balancer(
    State(state): State<FleetState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let pinned = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(';')
                .filter_map(|pair| pair.trim().split_once('='))
                .find(|(name, _)| *name == COOKIE)
                .map(|(_, id)| id.to_string())
        });

    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let batched = request.is_array();
    let calls = request.as_array().cloned().unwrap_or_else(|| vec![request]);

    let infrastructural = calls.iter().all(addresses_no_block);
    let (served_by, assigned) = {
        let mut fleet = state.0.lock().expect("fleet");
        if infrastructural {
            let id = fleet.rotation[0].clone();
            (id, None)
        } else if !fleet.names_backends {
            // Lands wherever it lands, and the answer says nothing.
            (fleet.assign(), None)
        } else {
            match pinned.as_ref().filter(|id| fleet.heads.contains_key(*id)) {
                // Served there, and silence is what says "still yours".
                Some(id) => (id.clone(), None),
                // No id, or an unknown one: assigned afresh and told which.
                None => {
                    let id = fleet.assign();
                    (id.clone(), Some(id))
                }
            }
        }
    };

    let head = *state
        .0
        .lock()
        .expect("fleet")
        .heads
        .get(&served_by)
        .expect("a backend serves");

    let mut responses = Vec::with_capacity(calls.len());
    for call in &calls {
        let id = call.get("id").cloned().unwrap_or(json!(1));
        let method = call
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let params = call.get("params").cloned().unwrap_or(json!([]));
        let number = requested_number(&params);

        let result = match method.as_str() {
            "eth_chainId" => json!("0x1"),
            "eth_blockNumber" => json!(format!("0x{head:x}")),
            "eth_getLogs" => json!([]),
            // The capability probe, which is not block-addressed.
            "eth_getBlockReceipts" if params.get(0).and_then(Value::as_str) == Some("latest") => {
                json!([])
            }
            "eth_getBlockByNumber" => match number {
                Some(n) if n <= head => block_json(n),
                _ => Value::Null,
            },
            // Stored in one step, so a header it has is receipts it has.
            "eth_getBlockReceipts" => match number {
                Some(n) if n <= head => json!([receipt_json(n)]),
                _ => Value::Null,
            },
            _ => Value::Null,
        };

        state.0.lock().expect("fleet").log.push(Seen {
            method: method.clone(),
            number,
            pinned: pinned.clone(),
            served_by: served_by.clone(),
            connection: peer.port(),
        });

        responses.push(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    let payload = if batched {
        serde_json::to_vec(&responses)
    } else {
        serde_json::to_vec(&responses[0])
    }
    .expect("serialize");

    let mut out = axum::http::HeaderMap::new();
    out.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    if let Some(id) = assigned {
        out.insert(
            axum::http::header::SET_COOKIE,
            format!("{COOKIE}={id}; Path=/; HttpOnly").parse().unwrap(),
        );
    }
    (axum::http::StatusCode::OK, out, payload)
}

/// Backends holding the listed heads, assigned in `rotation` order.
async fn serve_fleet(
    heads: &[(&str, u64)],
    rotation: &[&str],
    names_backends: bool,
) -> (String, Arc<Mutex<Fleet>>) {
    let fleet = Arc::new(Mutex::new(Fleet {
        heads: heads.iter().map(|(id, h)| (id.to_string(), *h)).collect(),
        rotation: rotation.iter().map(|id| id.to_string()).collect(),
        next: 0,
        names_backends,
        log: Vec::new(),
    }));

    let app = Router::new()
        .route("/", post(balancer))
        .with_state(FleetState(Arc::clone(&fleet)));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("serve");
    });
    (format!("http://127.0.0.1:{}", addr.port()), fleet)
}

fn rpc_with(url: &str, affinity: Option<bool>) -> Arc<Rpc> {
    let client = Arc::new(RpcClient::new(RpcClientConfig {
        url: url.to_string(),
        // Above the concurrent legs, so the binding is never connection reuse.
        capacity: 8,
        retry_attempts: 0,
        ..Default::default()
    }));
    Arc::new(Rpc::new(
        client,
        RpcOptions {
            provider_affinity: affinity,
            ..RpcOptions::default()
        },
    ))
}

fn body_of(block: &Value) -> RawRpcBlock {
    let parsed: RpcBlock = serde_json::from_value(block.clone()).expect("parse block");
    let number = u64::from_str_radix(parsed.number.strip_prefix("0x").expect("hex number"), 16)
        .expect("number");
    RawRpcBlock::new(number, parsed.hash.clone(), parsed)
}

/// `LEGS_ONLY_RETRIES` in the fetch layer.
const LEGS_ONLY_ATTEMPTS: u32 = 3;

/// Past the legs-only streak, well short of the 30 s not-ready budget.
const LADDER_WINDOW: Duration = Duration::from_millis(600);

fn logs_and_receipts() -> DataRequest {
    DataRequest {
        receipts: true,
        ..Default::default()
    }
}

fn log(fleet: &Arc<Mutex<Fleet>>) -> Vec<Seen> {
    fleet.lock().expect("fleet").log.clone()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// The winner of the poll that found a block serves that block's legs, and no
/// other block's.
#[tokio::test]
async fn the_backend_that_showed_the_header_serves_its_legs() {
    // Rotation gives 100 to `a` and 101 to `b`: two blocks in flight, two
    // winners, as in production.
    let (url, fleet) = serve_fleet(&[("a", 101), ("b", 101)], &["a", "b"], true).await;
    let rpc = rpc_with(&url, None);
    let req = logs_and_receipts();

    for number in [100u64, 101] {
        let poll = rpc.poll_head_block(number).await.expect("poll");
        let body = poll.block.expect("the block exists");
        rpc.enrich_block_with_retry(body, &req, None, poll.session)
            .await
            .expect("enrichment succeeds");
    }

    let seen = log(&fleet);
    for number in [100u64, 101] {
        let legs: Vec<&Seen> = seen
            .iter()
            .filter(|s| s.method == "eth_getBlockReceipts" && s.number == Some(number))
            .collect();
        assert!(!legs.is_empty(), "block {number} fetched receipts");

        let winner = seen
            .iter()
            .find(|s| s.method == "eth_getBlockByNumber" && s.number == Some(number))
            .map(|s| s.served_by.clone())
            .expect("the poll that found the block");

        for leg in legs {
            assert_eq!(
                leg.pinned.as_deref(),
                Some(winner.as_str()),
                "block {number}'s legs must replay the backend that showed its header"
            );
        }
    }

    // Different blocks, different backends — why the binding cannot live on
    // the client.
    let winners: Vec<String> = [100u64, 101]
        .iter()
        .map(|n| {
            seen.iter()
                .find(|s| s.method == "eth_getBlockByNumber" && s.number == Some(*n))
                .expect("a poll per block")
                .served_by
                .clone()
        })
        .collect();
    assert_eq!(winners, vec!["a".to_string(), "b".to_string()]);
}

/// The legs run concurrently and so cannot all share one connection. The
/// binding must therefore travel in the request.
#[tokio::test]
async fn one_binding_reaches_over_independent_connections() {
    let (url, fleet) = serve_fleet(&[("a", 200)], &["a"], true).await;
    let rpc = rpc_with(&url, None);
    let req = DataRequest {
        logs: true,
        receipts: true,
        ..Default::default()
    };

    let poll = rpc.poll_head_block(200).await.expect("poll");
    let body = poll.block.expect("the block exists");
    rpc.enrich_block_with_retry(body, &req, None, poll.session)
        .await
        .expect("enrichment succeeds");

    let seen = log(&fleet);
    let pinned: Vec<&Seen> = seen.iter().filter(|s| s.pinned.is_some()).collect();
    let connections: std::collections::HashSet<u16> = pinned.iter().map(|s| s.connection).collect();
    assert!(
        connections.len() > 1,
        "the concurrent legs must prove the binding is not connection reuse, saw {connections:?}"
    );
    assert!(
        pinned.iter().all(|s| s.pinned.as_deref() == Some("a")),
        "every pinned request names the same backend"
    );
}

/// The win, stated as traffic: bound to the backend that showed the header,
/// the block's receipts are there, so the not-ready ladder never runs and the
/// block costs exactly one round trip per leg.
#[tokio::test]
async fn the_binding_removes_the_ladder_rather_than_hurrying_it() {
    // `b` lags, and rotation offers it next: an unbound receipts ask lands
    // there, a bound one does not.
    let (url, fleet) = serve_fleet(&[("a", 300), ("b", 299)], &["a", "b"], true).await;
    let rpc = rpc_with(&url, None);
    let req = logs_and_receipts();

    let poll = rpc.poll_head_block(300).await.expect("poll");
    let body = poll.block.expect("the block exists");
    let (enriched, profile) = rpc
        .enrich_block_with_retry(body, &req, None, poll.session)
        .await
        .expect("enrichment succeeds first time");

    assert!(!enriched.is_invalid);
    assert_eq!(profile.attempts, 1, "no re-acquisition at all");
    let receipts = log(&fleet)
        .iter()
        .filter(|s| s.method == "eth_getBlockReceipts" && s.number == Some(300))
        .count();
    assert_eq!(receipts, 1, "and so exactly one receipts round trip");
}

/// Nothing is asked twice to change backend. Where the ask does move it is the
/// whole-block escalation the ladder was going to make anyway.
#[tokio::test]
async fn moving_the_ask_costs_no_extra_round_trip() {
    // Every backend lags, so the ladder runs and its round trips are countable.
    let bound = {
        let (url, fleet) = serve_fleet(&[("a", 400), ("b", 400)], &["a", "b"], true).await;
        let rpc = rpc_with(&url, None);
        let poll = rpc.poll_head_block(400).await.expect("poll");
        let body = poll.block.expect("the block exists");
        {
            let mut guard = fleet.lock().expect("fleet");
            guard.heads.insert("a".to_string(), 399);
            guard.heads.insert("b".to_string(), 399);
        }
        let req = logs_and_receipts();
        let enrich = rpc.enrich_block_with_retry(body, &req, None, poll.session);
        let _ = tokio::time::timeout(LADDER_WINDOW, enrich).await;
        log(&fleet)
    };

    let unbound = {
        let (url, fleet) = serve_fleet(&[("a", 400), ("b", 400)], &["a", "b"], true).await;
        let rpc = rpc_with(&url, Some(false));
        let poll = rpc.poll_head_block(400).await.expect("poll");
        let body = poll.block.expect("the block exists");
        {
            let mut guard = fleet.lock().expect("fleet");
            guard.heads.insert("a".to_string(), 399);
            guard.heads.insert("b".to_string(), 399);
        }
        let req = logs_and_receipts();
        let enrich = rpc.enrich_block_with_retry(body, &req, None, poll.session);
        let _ = tokio::time::timeout(LADDER_WINDOW, enrich).await;
        log(&fleet)
    };

    let count =
        |seen: &[Seen], method: &str| seen.iter().filter(|s| s.method == method).count() as i64;
    for method in ["eth_getBlockByNumber", "eth_getBlockReceipts"] {
        let bound_calls = count(&bound, method);
        let unbound_calls = count(&unbound, method);
        assert!(
            (bound_calls - unbound_calls).abs() <= 1,
            "{method}: the binding must not buy round trips — {bound_calls} bound vs \
             {unbound_calls} unbound"
        );
    }
}

/// A lagging backend is escaped by the escalation the ladder already makes:
/// three legs-only retries, then a whole-block re-acquisition that redraws.
#[tokio::test]
async fn the_whole_block_escalation_escapes_a_lagging_backend() {
    // `a` answers the poll then falls behind; rotation hands `b` the escalation.
    let (url, fleet) = serve_fleet(&[("a", 500), ("b", 500)], &["a", "b"], true).await;
    let rpc = rpc_with(&url, None);
    let req = logs_and_receipts();

    let poll = rpc.poll_head_block(500).await.expect("poll");
    let body = poll.block.expect("the block exists");
    fleet
        .lock()
        .expect("fleet")
        .heads
        .insert("a".to_string(), 499);

    let (enriched, profile) = tokio::time::timeout(
        Duration::from_secs(5),
        rpc.enrich_block_with_retry(body, &req, None, poll.session),
    )
    .await
    .expect("the escalation is reached well inside the budget")
    .expect("enrichment succeeds on another backend");

    assert!(!enriched.is_invalid);
    // The first acquisition, the legs-only streak, then the whole-block round.
    assert_eq!(profile.attempts, 1 + LEGS_ONLY_ATTEMPTS + 1);

    let seen = log(&fleet);
    let legs_only: Vec<&Seen> = seen
        .iter()
        .filter(|s| s.method == "eth_getBlockReceipts" && s.number == Some(500))
        .collect();
    assert!(
        legs_only[..=LEGS_ONLY_ATTEMPTS as usize]
            .iter()
            .all(|s| s.pinned.as_deref() == Some("a")),
        "the ladder stays where the header came from until it gives up on it"
    );
    let escalation = seen
        .iter()
        .rposition(|s| s.method == "eth_getBlockByNumber" && s.number == Some(500))
        .map(|i| &seen[i])
        .expect("a whole-block re-acquisition");
    assert_eq!(
        escalation.pinned, None,
        "the escalation names no backend, so the upstream picks a new one"
    );
    assert_eq!(escalation.served_by, "b");
}

/// An upstream with no notion of backends is untouched: no header is ever sent,
/// and the not-ready ladder is the one it has always had.
#[tokio::test]
async fn an_upstream_that_names_no_backend_is_unchanged() {
    let (url, fleet) = serve_fleet(&[("a", 550), ("b", 550)], &["a", "b"], false).await;
    let rpc = rpc_with(&url, None);
    let req = logs_and_receipts();

    let poll = rpc.poll_head_block(550).await.expect("poll");
    let body = poll.block.expect("the block exists");
    {
        let mut guard = fleet.lock().expect("fleet");
        guard.heads.insert("a".to_string(), 549);
        guard.heads.insert("b".to_string(), 549);
    }

    let started = Instant::now();
    let enrich = rpc.enrich_block_with_retry(body, &req, None, poll.session);
    let _ = tokio::time::timeout(Duration::from_millis(250), enrich).await;

    assert!(
        log(&fleet).iter().all(|s| s.pinned.is_none()),
        "nothing may be sent that the upstream never named"
    );
    assert!(
        started.elapsed() >= NOT_READY_DELAY,
        "the ladder is the one it always had"
    );
}

/// The switch forces the whole mechanism off, against an upstream that does
/// name backends.
#[tokio::test]
async fn the_switch_forces_it_off() {
    let (url, fleet) = serve_fleet(&[("a", 600), ("b", 600)], &["a", "b"], true).await;
    let rpc = rpc_with(&url, Some(false));
    let req = logs_and_receipts();

    let poll = rpc.poll_head_block(600).await.expect("poll");
    assert!(poll.session.is_none(), "no binding is created at all");
    let body = poll.block.expect("the block exists");
    {
        let mut guard = fleet.lock().expect("fleet");
        guard.heads.insert("a".to_string(), 599);
        guard.heads.insert("b".to_string(), 599);
    }

    let started = Instant::now();
    let enrich = rpc.enrich_block_with_retry(body, &req, None, poll.session);
    let _ = tokio::time::timeout(Duration::from_millis(250), enrich).await;

    assert!(
        log(&fleet).iter().all(|s| s.pinned.is_none()),
        "with the switch off no request may carry an assignment back"
    );
    assert!(
        started.elapsed() >= NOT_READY_DELAY,
        "and the ladder is exactly the current one"
    );
}

/// A whole-block re-acquisition draws afresh: it re-fetches the header, so
/// keeping the old backend would bind legs to a node that never showed it.
#[tokio::test]
async fn a_whole_block_reacquisition_draws_a_new_backend() {
    let (url, fleet) = serve_fleet(&[("a", 700), ("b", 700)], &["a", "b"], true).await;
    let rpc = rpc_with(&url, None);
    let req = logs_and_receipts();

    // A header from nowhere in particular: a poll whose winner was dropped.
    let body = body_of(&block_json(700));
    let (enriched, _) = rpc
        .enrich_block_with_retry(body, &req, None, rpc.new_session())
        .await
        .expect("enrichment succeeds");
    assert!(!enriched.is_invalid);

    let seen = log(&fleet);
    let first = seen
        .iter()
        .find(|s| s.method == "eth_getBlockReceipts" && s.number == Some(700))
        .expect("a receipts round trip");
    assert_eq!(
        first.pinned, None,
        "an unbound acquisition names nothing until the upstream does"
    );
}
