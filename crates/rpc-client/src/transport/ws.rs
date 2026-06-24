//! WebSocket request/response transport for `RpcClient`.
//!
//! Mirrors the TS `@subsquid/rpc-client` WS connection (request/response only —
//! no subscriptions). A small round-robin pool of independent connections, each
//! with its own reader task, writer task, pending map, and lazy reconnect.
//!
//! Key invariants:
//! - The global request-id counter lives in `RpcClient` and NEVER resets, so a
//!   stale frame from a dropped socket can never collide with a reused id.
//! - The pending map is keyed on the NUMERIC id only; a response whose id is not
//!   a `u64` is rejected/logged, never collapsed to 0.
//! - Connection loss / reset rejects all pending oneshots with
//!   `RpcError::Disconnected` (retryable) so the `RpcClient` retry layer
//!   re-sends on a fresh connection.
//! - Single-flight lazy dial: concurrent first callers (and post-reset callers)
//!   join ONE dial.
//! - Unknown-id frame → protocol corruption → reset the connection.
//! - Whole-batch error / non-array response → fail all N pending of that batch
//!   promptly (don't wait for the per-request timeout).
//! - After `split()`, tokio-tungstenite does NOT auto-pong; the reader forwards
//!   incoming `Ping` to the writer to send `Pong`. `Binary` frames → reset.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::future::{try_join_all, Shared};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, warn};

use super::{OwnedRpcRequest, RpcResponse, RpcTransport};
use crate::error::RpcError;

/// TCP keepalive on the underlying WS socket. Matches the HTTP client's 30s.
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// Bounded reconnect backoff schedule (index = consecutive-failure count).
const RECONNECT_BACKOFF: &[Duration] = &[
    Duration::from_millis(0),
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
    Duration::from_secs(5),
];

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsSource = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Shared routing state for a single connection generation.
///
/// `responders` maps a numeric request id → the oneshot that completes that
/// request. `batches` groups multi-call requests so that an incoming array
/// frame can be matched to the batch that issued it and any ids missing from
/// the array can be failed promptly (C2 short-array handling) instead of
/// hanging until the per-request timeout.
#[derive(Default)]
struct PendingState {
    responders: HashMap<u64, oneshot::Sender<RpcResponse>>,
    /// All ids belonging to a registered batch, keyed by the batch's first id.
    batches: HashMap<u64, HashSet<u64>>,
    /// Reverse index: any id of a batch → that batch's key (first id), so an
    /// arriving array frame can locate the owning batch from any member id.
    id_to_batch: HashMap<u64, u64>,
}

type Pending = Arc<StdMutex<PendingState>>;

/// A live connection's send-side handle, shared by all callers of one
/// connection generation.
struct Active {
    /// Outgoing text frames to the writer task.
    outgoing: mpsc::UnboundedSender<String>,
    /// Pending requests keyed on numeric id (+ batch grouping).
    pending: Pending,
}

impl Active {
    /// Register a single request id, returning its response receiver.
    fn register(&self, id: u64) -> oneshot::Receiver<RpcResponse> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().responders.insert(id, tx);
        rx
    }

    /// Register a batch of request ids as one group, returning a receiver per
    /// id (in `ids` order). The group lets the reader fail the whole batch
    /// promptly if the server's array reply is short / non-array.
    fn register_batch(&self, ids: &[u64]) -> Vec<oneshot::Receiver<RpcResponse>> {
        let mut st = self.pending.lock().unwrap();
        let key = ids[0];
        let set: HashSet<u64> = ids.iter().copied().collect();
        for &id in ids {
            st.id_to_batch.insert(id, key);
        }
        st.batches.insert(key, set);
        ids.iter()
            .map(|&id| {
                let (tx, rx) = oneshot::channel();
                st.responders.insert(id, tx);
                rx
            })
            .collect()
    }

    /// Drop registrations for these ids (e.g. on send failure / timeout) so the
    /// pending map doesn't leak entries that will never be answered.
    fn unregister(&self, ids: &[u64]) {
        let mut st = self.pending.lock().unwrap();
        for id in ids {
            st.responders.remove(id);
            if let Some(key) = st.id_to_batch.remove(id) {
                if let Some(set) = st.batches.get_mut(&key) {
                    set.remove(id);
                    if set.is_empty() {
                        st.batches.remove(&key);
                    }
                }
            }
        }
    }

    fn send_frame(&self, frame: String) -> Result<(), RpcError> {
        self.outgoing
            .send(frame)
            .map_err(|_| RpcError::Disconnected("writer task gone".into()))
    }
}

/// Shared in-flight dial future (single-flight). Cloneable; all joiners await
/// the same connect. The error is carried as a `String` because `Shared`
/// requires a `Clone` output and `RpcError` is not `Clone`; callers wrap it
/// back into `RpcError::Disconnected`.
type DialResult = Result<Arc<Active>, String>;
type DialFuture = Shared<Pin<Box<dyn Future<Output = DialResult> + Send>>>;

/// State guarding one connection's lifecycle.
struct ConnState {
    /// The current live connection, if any.
    active: Option<Arc<Active>>,
    /// In-flight dial shared by concurrent first/post-reset callers.
    dialing: Option<DialFuture>,
    /// Consecutive dial failures, for bounded backoff.
    failures: usize,
}

/// One independent WS connection (reader + writer + pending + lazy reconnect).
struct Connection {
    url: String,
    state: AsyncMutex<ConnState>,
    /// Monotonic connection generation; bumped on every reset so a late reset
    /// from an old socket can't clobber a newer connection.
    generation: AtomicU64,
}

impl Connection {
    fn new(url: String) -> Self {
        Connection {
            url,
            state: AsyncMutex::new(ConnState {
                active: None,
                dialing: None,
                failures: 0,
            }),
            generation: AtomicU64::new(0),
        }
    }

    /// Get the live connection, dialing if needed. Single-flight: concurrent
    /// callers join one dial.
    async fn get_active(self: &Arc<Self>) -> Result<Arc<Active>, RpcError> {
        // Fast path + obtain (or start) the shared dial future without holding
        // the lock across the await.
        let fut = {
            let mut st = self.state.lock().await;
            if let Some(active) = &st.active {
                return Ok(active.clone());
            }
            if let Some(d) = &st.dialing {
                d.clone()
            } else {
                let backoff = backoff_for(st.failures);
                let conn = self.clone();
                let fut: Pin<Box<dyn Future<Output = DialResult> + Send>> = Box::pin(async move {
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    conn.dial().await.map_err(|e| e.to_string())
                });
                let shared = fut.shared();
                st.dialing = Some(shared.clone());
                shared
            }
        };

        let result = fut.await;

        // Settle the dial outcome into shared state. Clearing the in-flight
        // marker is idempotent across joiners (all see the same `result`).
        let mut st = self.state.lock().await;
        st.dialing = None;
        match &result {
            Ok(active) => {
                st.active = Some(active.clone());
                st.failures = 0;
            }
            Err(_) => {
                st.failures = st.failures.saturating_add(1);
            }
        }
        result.map_err(RpcError::Disconnected)
    }

    /// Establish a fresh connection and spawn its reader/writer tasks.
    async fn dial(self: &Arc<Self>) -> Result<Arc<Active>, RpcError> {
        let (ws, _resp) = tokio_tungstenite::connect_async(&self.url)
            .await
            .map_err(|e| RpcError::Disconnected(format!("WS connect failed: {e}")))?;

        // Set TCP keepalive on the underlying socket (provider LBs drop idle
        // sockets; a single long-lived WS conn is exposed on quiet chains).
        set_tcp_keepalive(&ws);

        let generation = self.generation.load(Ordering::SeqCst);
        let (sink, source) = ws.split();
        let pending: Pending = Arc::new(StdMutex::new(PendingState::default()));
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<String>();

        // Writer task: owns the sink, forwards outgoing text frames and pongs.
        let (pong_tx, pong_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(writer_task(sink, outgoing_rx, pong_rx));

        // Reader task: owns the source, routes responses, forwards pings.
        let conn = self.clone();
        let pending_for_reader = pending.clone();
        tokio::spawn(async move {
            let reason = reader_task(source, pending_for_reader.clone(), pong_tx).await;
            conn.reset(generation, reason).await;
        });

        Ok(Arc::new(Active {
            outgoing: outgoing_tx,
            pending,
        }))
    }

    /// Tear down the current connection (if it's still this generation) and
    /// reject all pending requests with a retryable disconnect error.
    async fn reset(self: &Arc<Self>, generation: u64, reason: String) {
        // Only reset if no newer generation has already taken over.
        if self
            .generation
            .compare_exchange(
                generation,
                generation + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }

        let active = {
            let mut st = self.state.lock().await;
            st.active.take()
        };

        if let Some(active) = active {
            // Reject every pending request so the retry layer re-sends.
            let drained: Vec<oneshot::Sender<RpcResponse>> = {
                let mut st = active.pending.lock().unwrap();
                st.batches.clear();
                st.id_to_batch.clear();
                st.responders.drain().map(|(_, tx)| tx).collect()
            };
            let n = drained.len();
            for tx in drained {
                // The receiver translates a closed channel into Disconnected.
                drop(tx);
            }
            if n > 0 {
                warn!(reason, pending = n, "WS connection reset, rejected pending");
            } else {
                debug!(reason, "WS connection reset");
            }
            // Dropping `active` drops the outgoing sender, stopping the writer.
        }
    }

    /// Force a reset triggered by a request-level fault (timeout / protocol),
    /// using the connection's current generation.
    ///
    /// Best-effort by design: it reads the generation, then `reset()` CASes on
    /// it. If another reset (e.g. the reader task on socket close) raced in
    /// between, this call's CAS fails and it no-ops — the other reset already
    /// drained the pending map. The "no dangling oneshot" guarantee therefore
    /// relies on *some* reset of this generation draining the map, not on this
    /// particular call winning the race. Every pending sender is dropped by
    /// whichever reset succeeds, so each waiting receiver resolves promptly.
    async fn reset_current(self: &Arc<Self>, reason: String) {
        let generation = self.generation.load(Ordering::SeqCst);
        self.reset(generation, reason).await;
    }
}

fn backoff_for(failures: usize) -> Duration {
    let idx = failures.min(RECONNECT_BACKOFF.len() - 1);
    RECONNECT_BACKOFF[idx]
}

/// Writer task: drains outgoing frames and pong replies onto the sink.
async fn writer_task(
    mut sink: WsSink,
    mut outgoing: mpsc::UnboundedReceiver<String>,
    mut pongs: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    loop {
        tokio::select! {
            frame = outgoing.recv() => {
                match frame {
                    Some(text) => {
                        if sink.send(Message::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    None => break, // Active dropped → connection closing.
                }
            }
            pong = pongs.recv() => {
                match pong {
                    Some(payload) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // Pong channel closed (reader gone); keep serving
                        // outgoing until that side closes too.
                    }
                }
            }
        }
    }
    let _ = sink.close().await;
}

/// Reader task: routes incoming frames to pending oneshots. Returns the reason
/// string when the connection should be torn down.
async fn reader_task(
    mut source: WsSource,
    pending: Pending,
    pong_tx: mpsc::UnboundedSender<Vec<u8>>,
) -> String {
    while let Some(msg) = source.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => return format!("socket error: {e}"),
        };
        match msg {
            Message::Text(text) => {
                if let Some(reason) = route_text(&text, &pending) {
                    return reason;
                }
            }
            Message::Binary(_) => {
                // Mirrors ws.ts: a non-text frame is a protocol error → reset.
                return "received non-text (binary) frame".into();
            }
            Message::Ping(payload) => {
                // Split streams do not auto-pong; forward to the writer.
                let _ = pong_tx.send(payload);
            }
            Message::Pong(_) => {}
            Message::Close(_) => return "socket closed by peer".into(),
            Message::Frame(_) => {}
        }
    }
    "socket stream ended".into()
}

/// Parse and route a text frame. Returns `Some(reason)` if the connection must
/// be reset (protocol corruption), `None` otherwise.
fn route_text(text: &str, pending: &Pending) -> Option<String> {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Some("received invalid JSON message".into()),
    };

    match parsed {
        Value::Array(items) => route_array(items, pending),
        obj @ Value::Object(_) => route_one(obj, pending),
        _ => Some("received non-object JSON frame".into()),
    }
}

/// Route an array frame — the server's reply to a batch. After routing every
/// item, locate the batch that owns the responses and, if the array was short
/// (missing ids), fail those missing ids promptly with a protocol error rather
/// than letting them hang to the per-request timeout (C2). Returns `Some(reason)`
/// only on genuine protocol corruption (unknown id, malformed item).
fn route_array(items: Vec<Value>, pending: &Pending) -> Option<String> {
    // Snapshot the numeric ids present in this array up front. We must resolve
    // the owning batch(es) BEFORE routing items, because `route_one` removes
    // each routed id from the batch index — looking up afterwards would miss the
    // batch entirely.
    let present: HashSet<u64> = items
        .iter()
        .filter_map(|item| item.get("id").and_then(|v| v.as_u64()))
        .collect();
    let batch_keys = owning_batches(&present, pending);

    for item in items {
        if let Some(reason) = route_one(item, pending) {
            return Some(reason);
        }
    }
    fail_short_batches(batch_keys, &present, pending);
    None
}

/// Resolve the distinct batch keys owning any of the just-arrived ids, before
/// routing mutates the batch index.
fn owning_batches(present: &HashSet<u64>, pending: &Pending) -> HashSet<u64> {
    let st = pending.lock().unwrap();
    present
        .iter()
        .filter_map(|id| st.id_to_batch.get(id).copied())
        .collect()
}

/// Fail any ids of the given batches that did NOT arrive in the array reply, so
/// a short array reply fails the affected batch immediately (C2).
fn fail_short_batches(batch_keys: HashSet<u64>, present: &HashSet<u64>, pending: &Pending) {
    let mut st = pending.lock().unwrap();

    for key in batch_keys {
        let Some(expected) = st.batches.remove(&key) else {
            continue;
        };
        for id in &expected {
            st.id_to_batch.remove(id);
        }
        // Any expected id not present in this array reply is missing. Drop its
        // sender: the waiting receiver resolves immediately with a recv error,
        // which `send_batch` surfaces as a prompt batch (`Protocol`) failure.
        for id in expected.difference(present) {
            st.responders.remove(id);
        }
    }
}

/// Route a single response object. Returns `Some(reason)` to reset.
fn route_one(value: Value, pending: &Pending) -> Option<String> {
    let obj = match value.as_object() {
        Some(o) => o,
        None => return Some("response frame is not an object".into()),
    };

    // Notifications (have a `method` field) and id-less / id:null frames
    // (including whole-batch error objects) are not routed by id.
    if obj.contains_key("method") {
        debug!("ignoring WS notification frame");
        return None;
    }

    let id_value = obj.get("id");
    let numeric_id = match id_value {
        None | Some(Value::Null) => {
            // No id / id:null. A whole-batch error object (`{error, id:null}`)
            // must fail the in-flight batch promptly — we can't attribute it to
            // a specific batch on a multiplexed socket, so reset the connection,
            // which rejects all pending as retryable. A bare notification (no
            // error) is simply dropped.
            if obj.contains_key("error") {
                return Some("received whole-batch error frame (id:null)".into());
            }
            debug!("ignoring WS frame without numeric id");
            return None;
        }
        Some(v) => match v.as_u64() {
            Some(id) => id,
            None => {
                // Non-numeric id: reject/log, never collapse to 0.
                warn!(id = %v, "ignoring WS response with non-numeric id");
                return None;
            }
        },
    };

    let resp: RpcResponse = match serde_json::from_value(value) {
        Ok(r) => r,
        Err(_) => return Some(format!("malformed response for id {numeric_id}")),
    };

    let tx = {
        let mut st = pending.lock().unwrap();
        // Clear this id's batch bookkeeping if it belonged to one (interleaved
        // per-item replies route here without ever hitting `fail_short_batches`,
        // so keep the batch index from leaking).
        if let Some(key) = st.id_to_batch.remove(&numeric_id) {
            if let Some(set) = st.batches.get_mut(&key) {
                set.remove(&numeric_id);
                if set.is_empty() {
                    st.batches.remove(&key);
                }
            }
        }
        st.responders.remove(&numeric_id)
    };
    match tx {
        Some(tx) => {
            let _ = tx.send(resp);
            None
        }
        None => {
            // Unknown id → protocol corruption → reset (faithful to ws.ts:147).
            Some(format!("got response for unknown request {numeric_id}"))
        }
    }
}

// ─── WsTransport ──────────────────────────────────────────────────────────────

/// Default pool size for the WS transport.
pub(crate) const DEFAULT_POOL_SIZE: usize = 4;

/// WebSocket transport: a round-robin pool of independent connections.
pub(crate) struct WsTransport {
    pool: Vec<Arc<Connection>>,
    next: AtomicU64,
}

impl WsTransport {
    pub fn new(url: String, pool_size: usize) -> Self {
        let pool_size = pool_size.max(1);
        let pool = (0..pool_size)
            .map(|_| Arc::new(Connection::new(url.clone())))
            .collect();
        WsTransport {
            pool,
            next: AtomicU64::new(0),
        }
    }

    fn pick(&self) -> &Arc<Connection> {
        let i = self.next.fetch_add(1, Ordering::Relaxed) as usize % self.pool.len();
        &self.pool[i]
    }
}

/// Resolve the per-request timeout. `Duration::ZERO` means "no per-request
/// timeout", consistent with the HTTP transport (`transport.rs`, which only sets
/// `req.timeout()` when the configured timeout is non-zero). `None` here →
/// await the response without a deadline; the connection's own health (socket
/// close / reset) still fails the request promptly. A finite timeout keeps the
/// timeout-resets-the-connection behavior.
fn effective_timeout(timeout: Duration) -> Option<Duration> {
    if timeout.is_zero() {
        None
    } else {
        Some(timeout)
    }
}

/// Await a oneshot under an optional deadline. `None` → wait indefinitely (the
/// connection reset still resolves the receiver). `Some` → on elapse the caller
/// resets the connection (a late response can't be cancelled mid-flight).
async fn await_with_timeout<T>(
    deadline: Option<Duration>,
    fut: impl Future<Output = T>,
) -> Result<T, ()> {
    match deadline {
        Some(d) => tokio::time::timeout(d, fut).await.map_err(|_| ()),
        None => Ok(fut.await),
    }
}

#[async_trait]
impl RpcTransport for WsTransport {
    async fn send_single(
        &self,
        req: OwnedRpcRequest,
        timeout: Duration,
    ) -> Result<RpcResponse, RpcError> {
        let conn = self.pick();
        let active = conn.get_active().await?;

        let id = req.id;
        let frame = serde_json::to_string(&req.as_wire()).expect("serialize");
        let rx = active.register(id);

        if let Err(e) = active.send_frame(frame) {
            active.unregister(&[id]);
            // The writer task is gone (dead/raced connection); drop it so the
            // next caller dials fresh instead of reusing a dead handle.
            conn.reset_current("writer task gone".into()).await;
            return Err(e);
        }

        match await_with_timeout(effective_timeout(timeout), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            // Channel closed without a value → connection was reset.
            Ok(Err(_)) => Err(RpcError::Disconnected("connection reset".into())),
            Err(()) => {
                // Per-request timeout: tear down the connection so a late
                // response can't desync the id map; siblings retry.
                conn.reset_current("request timed out".into()).await;
                Err(RpcError::Timeout)
            }
        }
    }

    async fn send_batch(
        &self,
        reqs: Vec<OwnedRpcRequest>,
        timeout: Duration,
    ) -> Result<Vec<RpcResponse>, RpcError> {
        if reqs.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.pick();
        let active = conn.get_active().await?;

        let ids: Vec<u64> = reqs.iter().map(|r| r.id).collect();
        let wire: Vec<_> = reqs.iter().map(|r| r.as_wire()).collect();
        let frame = serde_json::to_string(&wire).expect("serialize");

        // Snapshot the generation BEFORE registering/sending so the post-await
        // comparison reliably tells a connection reset (whole-batch error frame /
        // socket loss → generation bumped, C1) apart from a short-array per-batch
        // failure (same generation, C2). Loading it AFTER registration opens a
        // race: a reset in the window between `register_batch` and the load would
        // leave `gen_after == gen_before`, misclassifying a genuine disconnect as
        // `Protocol` — which is NOT retryable on the single-call path that
        // single-element chunks travel through.
        let gen_before = conn.generation.load(Ordering::SeqCst);

        // Register the whole batch as one group so the reader can fail it
        // promptly if the array reply is short / non-array (C2). `register_batch`
        // returns receivers in request order, so `try_join_all` yields results
        // in request order.
        let receivers = active.register_batch(&ids);

        if let Err(e) = active.send_frame(frame) {
            active.unregister(&ids);
            conn.reset_current("writer task gone".into()).await;
            return Err(e);
        }

        // Await ALL receivers together, deterministically (C1): `try_join_all`
        // resolves as soon as any receiver fails. A connection reset drops every
        // sender, so all receivers fail promptly; a short array drops only the
        // missing ids' senders, failing this batch promptly. Neither waits for
        // the per-request timeout.
        let deadline = effective_timeout(timeout);
        let joined = await_with_timeout(deadline, try_join_all(receivers)).await;

        match joined {
            // `try_join_all` preserves input (request) order.
            Ok(Ok(out)) => Ok(out),
            Ok(Err(_recv_err)) => {
                active.unregister(&ids);
                // `gen_before` was sampled before registration, so any reset that
                // tore down this connection — at any point from registration
                // onward — bumps the generation and is seen here. A reset drops
                // every sender (the receiver observes a recv error with no routed
                // value), which means the connection was torn down → Disconnected
                // (retryable on BOTH the batch and the single-call path, so a
                // single-element chunk routed through `call` still retries).
                let gen_after = conn.generation.load(Ordering::SeqCst);
                if gen_after != gen_before {
                    // The connection was reset (whole-batch error / disconnect):
                    // retryable so the retry layer re-sends on a fresh socket.
                    Err(RpcError::Disconnected("connection reset".into()))
                } else {
                    // Same generation, sender dropped without a reset: the only
                    // path that does this is `fail_short_batches` — the server
                    // stayed connected but its array reply omitted one or more
                    // ids for this batch → genuine protocol fault, fail fast.
                    Err(RpcError::Protocol(
                        "short batch response: server omitted one or more ids".into(),
                    ))
                }
            }
            Err(()) => {
                conn.reset_current("batch request timed out".into()).await;
                Err(RpcError::Timeout)
            }
        }
    }
}

// ─── TCP keepalive helper ─────────────────────────────────────────────────────

fn set_tcp_keepalive(ws: &WebSocketStream<MaybeTlsStream<TcpStream>>) {
    // The deref chain below is coupled to tokio-tungstenite 0.24 + native-tls:
    // `MaybeTlsStream::NativeTls(tokio_native_tls::TlsStream)` derefs through the
    // native-tls and tokio adapters down to the raw `tokio::net::TcpStream`. Only
    // the `Plain` and `NativeTls` variants are handled; any other `MaybeTlsStream`
    // variant (e.g. a rustls stack, which this crate does not enable) silently
    // skips keepalive. If the TLS feature set changes, revisit this match.
    let tcp = match ws.get_ref() {
        MaybeTlsStream::Plain(s) => s,
        MaybeTlsStream::NativeTls(s) => s.get_ref().get_ref().get_ref(),
        _ => return,
    };
    let sock = socket2::SockRef::from(tcp);
    // Set both the idle time before the first probe and the inter-probe interval
    // so a half-open socket on a quiet chain is detected and torn down.
    let ka = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE)
        .with_interval(TCP_KEEPALIVE);
    if let Err(e) = sock.set_tcp_keepalive(&ka) {
        debug!(error = %e, "failed to set TCP keepalive on WS socket");
    }
}
