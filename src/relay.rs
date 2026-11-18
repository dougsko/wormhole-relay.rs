//! Transit relay state machine.
//!
//! Ported from the Go implementation (`internal/relay/relay.go` in
//! <https://github.com/dougsko/wormhole-relay>); the wire protocol is
//! specified by
//! <https://github.com/magic-wormhole/magic-wormhole-protocols> (`transit.md`,
//! "Relay Handshake"), which is authoritative. The relay is deliberately
//! dumb: it pairs two connections that present the same token (with
//! different, or legacy/empty, side tokens), sends each `ok\n`, and glues
//! the byte streams together until one side disappears, then closes the
//! other (no half-close).
//!
//! Notes on transport asymmetry (kept from the reference):
//! - TCP is line-oriented during the handshake (`\n`-terminated) and raw
//!   afterwards; anything received before the partner shows up that isn't a
//!   valid handshake or that arrives ahead of the pairing gets `impatient\n`.
//! - WebSocket messages are opaque frames, but the payload is treated as the
//!   same byte stream. The first message carries the handshake; trailing
//!   line-ending whitespace in that message is tolerated (the Python and
//!   Rust clients send `please relay ...\n`, which is the protocol-literal
//!   form). Subsequent binary (or text) messages are relayed verbatim.
//! - `bad handshake\n` vs `bad handshake`: kept per transport for wire
//!   compatibility with the reference's clients.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{FutureExt, SinkExt, StreamExt};
use regex::Regex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};
use tokio_tungstenite::tungstenite::{Bytes, Utf8Bytes};
use tokio_tungstenite::WebSocketStream;

// Timeouts are constants so both transports and tests share them.
// preHandshakeReadTimeout bounds how long a client may take to deliver its
// handshake line: idle or trickling clients can't park a task and fd
// forever. It does not apply after the handshake, when a paired transfer may
// be idle between messages. writeTimeout bounds a single socket write so a
// stalled peer can't leak the relay's tasks; exceeded writes tear the pipe
// down.
pub const PRE_HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(30);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bounds the best-effort Close frame sent when a WebSocket tears down; a
/// wedged peer must not hold the task.
pub const CLOSE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub const MAX_HANDSHAKE_BYTES: usize = 1024;
/// Caps a single WebSocket message the relay will buffer. Transit frames
/// are far smaller; this exists so a hostile client can't exhaust memory.
pub const MAX_WS_MESSAGE_BYTES: usize = 4 << 20;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// Connection result mood, recorded in the usage log/database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mood {
    /// connected, but never sent a handshake
    Empty,
    /// both sides gave a correct handshake
    Happy,
    /// good handshake, but the other side never showed up
    Lonely,
    /// good handshake, abandoned in favor of a different connection
    Redundant,
    Impatient,
    /// one side gave the wrong handshake
    Errory,
    Jilted,
}

impl Mood {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mood::Empty => "empty",
            Mood::Happy => "happy",
            Mood::Lonely => "lonely",
            Mood::Redundant => "redundant",
            Mood::Impatient => "impatient",
            Mood::Errory => "errory",
            Mood::Jilted => "jilted",
        }
    }
}

/// State of a single transit connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitServerState {
    #[allow(dead_code)]
    Listening,
    WaitRelay,
    WaitPartner,
    Relaying,
    Done,
}

/// Callback used to compute the current time (injectable for tests).
pub type GetTimestamp = Box<dyn Fn() -> SystemTime + Send + Sync>;

/// Usage recorder interface for recording usage statistics. Implemented by
/// [`crate::usage::UsageTracker`]; tests may substitute a mock.
pub trait UsageRecorder: Send + Sync {
    fn record(
        &self,
        started: SystemTime,
        buddy_started: Option<SystemTime>,
        mood: Mood,
        bytes_sent: u64,
        buddy_bytes: u64,
    );
    fn update_stats(
        &self,
        rebooted: SystemTime,
        updated: SystemTime,
        connected: i64,
        waiting: i64,
        incomplete_bytes: i64,
    );
}

/// A handle to either kind of transit connection, used as the "pending
/// client" in the registry. Cheap to clone (`Arc` under the hood).
#[derive(Clone)]
pub enum PendingClientRef {
    Tcp(Arc<TcpTransitConnection>),
    Ws(Arc<WebSocketTransitConnection>),
}

impl std::fmt::Debug for PendingClientRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PendingClientRef::Tcp(c) => write!(f, "TcpTransitConnection({})", c.id()),
            PendingClientRef::Ws(c) => write!(f, "WebSocketTransitConnection({})", c.id()),
        }
    }
}

impl PendingClientRef {
    pub fn id(&self) -> u64 {
        match self {
            PendingClientRef::Tcp(c) => c.id,
            PendingClientRef::Ws(c) => c.id,
        }
    }

    pub fn mood(&self) -> Mood {
        match self {
            PendingClientRef::Tcp(c) => c.state.lock().unwrap().mood,
            PendingClientRef::Ws(c) => c.state.lock().unwrap().mood,
        }
    }

    pub fn total_sent(&self) -> u64 {
        match self {
            PendingClientRef::Tcp(c) => c.state.lock().unwrap().total_sent,
            PendingClientRef::Ws(c) => c.state.lock().unwrap().total_sent,
        }
    }

    /// Snapshot the fields `record_usage` needs from the partner (under the
    /// partner's mutex; the buddy may be mutating total_sent).
    pub fn buddy_stats(&self) -> (SystemTime, u64) {
        match self {
            PendingClientRef::Tcp(c) => {
                let s = c.state.lock().unwrap();
                (s.started, s.total_sent)
            }
            PendingClientRef::Ws(c) => {
                let s = c.state.lock().unwrap();
                (s.started, s.total_sent)
            }
        }
    }

    pub async fn send_bytes(&self, data: &[u8]) {
        match self {
            PendingClientRef::Tcp(c) => c.send_bytes(data).await,
            PendingClientRef::Ws(c) => c.send_bytes(data).await,
        }
    }

    pub async fn close(&self) {
        match self {
            PendingClientRef::Tcp(c) => c.close().await,
            PendingClientRef::Ws(c) => c.close().await,
        }
    }

    async fn got_partner(&self, other: PendingClientRef) {
        match self {
            PendingClientRef::Tcp(c) => c.got_partner(other).await,
            PendingClientRef::Ws(c) => c.got_partner(other).await,
        }
    }

    async fn partner_connection_lost(&self) {
        match self {
            PendingClientRef::Tcp(c) => c.partner_connection_lost().await,
            PendingClientRef::Ws(c) => c.partner_connection_lost().await,
        }
    }
}

/// Tracks active (paired) connections.
#[derive(Default)]
pub struct ActiveConnections {
    connections: Mutex<HashMap<u64, PendingClientRef>>,
}

impl ActiveConnections {
    pub fn register(&self, side0: PendingClientRef, side1: PendingClientRef) {
        let mut g = self.connections.lock().unwrap();
        g.insert(side0.id(), side0);
        g.insert(side1.id(), side1);
    }

    pub fn unregister(&self, id: u64) {
        self.connections.lock().unwrap().remove(&id);
    }

    pub fn count(&self) -> i64 {
        self.connections.lock().unwrap().len() as i64
    }

    pub fn incomplete_bytes(&self) -> i64 {
        self.connections
            .lock()
            .unwrap()
            .values()
            .map(|c| c.total_sent() as i64)
            .sum()
    }
}

/// Tracks outstanding (not yet paired) requests, keyed by token and side.
pub struct PendingRequests {
    requests: Mutex<HashMap<String, HashMap<String, PendingClientRef>>>,
    active: Arc<ActiveConnections>,
}

impl PendingRequests {
    pub fn new(active: Arc<ActiveConnections>) -> Self {
        PendingRequests {
            requests: Mutex::new(HashMap::new()),
            active,
        }
    }

    pub fn unregister(&self, token: &str, side: &str, id: u64) {
        {
            let mut g = self.requests.lock().unwrap();
            if let Some(sides) = g.get_mut(token) {
                sides.remove(side);
                if sides.is_empty() {
                    g.remove(token);
                }
            }
        }
        self.active.unregister(id);
    }

    /// Register a pending request. Returns true if this is the first side to
    /// arrive (the caller waits for a partner); false if it was paired with
    /// (or evicted as) an existing request.
    pub async fn register(&self, token: &str, side: &str, tc: PendingClientRef) -> bool {
        // Find a matching partner under the lock; call them outside it.
        let pair: Option<(
            Arc<ActiveConnections>,
            PendingClientRef,
            Vec<PendingClientRef>,
        )> = {
            let mut g = self.requests.lock().unwrap();
            let sides = g.entry(token.to_string()).or_default();

            // Check for a matching side: legacy ("") matches anything, and
            // any side matches a different side. Identical non-empty sides
            // do NOT match (they deduplicate a reconnecting session).
            let matched = sides
                .iter()
                .find(|(existing_side, _)| {
                    existing_side.is_empty() || side.is_empty() || *existing_side != side
                })
                .map(|(existing_side, existing_tc)| (existing_side.clone(), existing_tc.clone()));

            match matched {
                Some((existing_side, existing_tc)) => {
                    sides.remove(&existing_side);
                    // Drop any other sides (redundant)
                    let leftovers: Vec<PendingClientRef> = sides.values().cloned().collect();
                    g.remove(token);
                    Some((self.active.clone(), existing_tc, leftovers))
                }
                None => {
                    sides.insert(side.to_string(), tc.clone());
                    None
                }
            }
        };

        match pair {
            Some((active, existing_tc, leftovers)) => {
                active.register(tc.clone(), existing_tc.clone());
                // glue the two ends together (both `ok\n`s are sent here,
                // synchronously — before the caller's Register returns, as
                // in the reference)
                tc.got_partner(existing_tc.clone()).await;
                existing_tc.got_partner(tc).await;
                for leftover in leftovers {
                    leftover.partner_connection_lost().await;
                }
                false
            }
            None => true,
        }
    }

    pub fn count(&self) -> i64 {
        self.requests
            .lock()
            .unwrap()
            .values()
            .map(|sides| sides.len() as i64)
            .sum()
    }
}

/// The transit server: owns the pending/active registries and usage sink.
pub struct TransitServer {
    pub active: Arc<ActiveConnections>,
    pending: PendingRequests,
    usage: Arc<dyn UsageRecorder>,
    get_timestamp: GetTimestamp,
    rebooted: SystemTime,
    stats_mu: Mutex<()>,
}

impl TransitServer {
    pub fn new(
        usage: Arc<dyn UsageRecorder>,
        get_timestamp: impl Fn() -> SystemTime + Send + Sync + 'static,
    ) -> Arc<TransitServer> {
        let active = Arc::new(ActiveConnections::default());
        Arc::new(TransitServer {
            active: active.clone(),
            pending: PendingRequests::new(active),
            usage,
            rebooted: get_timestamp(),
            get_timestamp: Box::new(get_timestamp),
            stats_mu: Mutex::new(()),
        })
    }

    pub fn now(&self) -> SystemTime {
        (self.get_timestamp)()
    }

    /// Update global statistics (mirrors `TimerService(5*60, transit.update_stats)`).
    pub fn update_stats(&self) {
        let _guard = self.stats_mu.lock().unwrap();
        let connected = self.active.count();
        let waiting = self.pending.count();
        let incomplete_bytes = self.active.incomplete_bytes();
        self.usage.update_stats(
            self.rebooted,
            self.now(),
            connected,
            waiting,
            incomplete_bytes,
        );
    }

    pub async fn pending_register(&self, token: &str, side: &str, tc: PendingClientRef) -> bool {
        self.pending.register(token, side, tc).await
    }

    pub fn pending_unregister(&self, token: &str, side: &str, id: u64) {
        self.pending.unregister(token, side, id);
    }

    /// Number of not-yet-paired connections (useful for tests and stats).
    pub fn pending_count(&self) -> i64 {
        self.pending.count()
    }

    /// Number of paired (active) connections.
    pub fn active_count(&self) -> i64 {
        self.active.count()
    }

    fn record_usage(
        &self,
        started: SystemTime,
        buddy: Option<&PendingClientRef>,
        mood: Mood,
        total_sent: u64,
    ) {
        // Like the reference's _record_usage: skip the "jilted" event when
        // the partner ended happy, so each relayed transfer produces
        // exactly one usage record.
        if mood == Mood::Jilted {
            if let Some(b) = buddy {
                if b.mood() == Mood::Happy {
                    return;
                }
            }
        }

        let (buddy_started, buddy_bytes) = match buddy {
            Some(b) => {
                let (s, bytes) = b.buddy_stats();
                (Some(s), bytes)
            }
            None => (None, 0),
        };

        self.usage
            .record(started, buddy_started, mood, total_sent, buddy_bytes);
    }
}

#[derive(Debug)]
struct CoreState {
    state: TransitServerState,
    token: String,
    side: String,
    first: bool,
    mood: Mood,
    buddy: Option<PendingClientRef>,
    total_sent: u64,
    started: SystemTime,
    raw_mode: bool,
    first_msg: bool,
}

impl CoreState {
    fn new(started: SystemTime) -> Self {
        CoreState {
            state: TransitServerState::WaitRelay,
            token: String::new(),
            side: String::new(),
            first: false,
            mood: Mood::Empty,
            buddy: None,
            total_sent: 0,
            started,
            raw_mode: false,
            first_msg: true,
        }
    }
}

fn handshake_regexes() -> (&'static Regex, &'static Regex) {
    static OLD: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    static NEW: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let old = OLD.get_or_init(|| Regex::new(r"^please relay ([0-9A-Za-z_]{64})$").unwrap());
    let new = NEW.get_or_init(|| {
        Regex::new(r"^please relay ([0-9A-Za-z_]{64}) for side ([0-9A-Za-z_]{16})$").unwrap()
    });
    (old, new)
}

/// Outcome of parsing one handshake line/message.
#[derive(Debug, PartialEq, Eq)]
enum Handshake {
    /// `please relay <64hex>\n` — legacy form
    Legacy(String),
    /// `please relay <64hex> for side <16chars>\n`
    Sided(String, String),
    Invalid,
}

fn parse_handshake(line: &[u8]) -> Handshake {
    let (old_re, new_re) = handshake_regexes();
    if let Some(m) = old_re.captures(std::str::from_utf8(line).unwrap_or("")) {
        return Handshake::Legacy(m[1].to_string());
    }
    if let Some(m) = new_re.captures(std::str::from_utf8(line).unwrap_or("")) {
        return Handshake::Sided(m[1].to_string(), m[2].to_string());
    }
    Handshake::Invalid
}

/// Trim leading/trailing ASCII whitespace, like bytes.TrimSpace.
fn trim_ascii(data: &[u8]) -> &[u8] {
    let start = data
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(data.len());
    let end = data
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(start);
    &data[start..end]
}

// ---------------------------------------------------------------------------
// TCP transport
// ---------------------------------------------------------------------------

pub struct TcpTransitConnection {
    id: u64,
    server: Arc<TransitServer>,
    state: Mutex<CoreState>,
    read_half: Mutex<Option<OwnedReadHalf>>,
    write_half: Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
    close_notify: Arc<Notify>,
    closed: AtomicBool,
    weak_self: Mutex<std::sync::Weak<TcpTransitConnection>>,
}

impl TcpTransitConnection {
    pub fn new(stream: TcpStream, server: Arc<TransitServer>) -> Arc<Self> {
        let id = next_conn_id();
        let (read_half, write_half) = stream.into_split();
        let conn = Arc::new(TcpTransitConnection {
            id,
            server: server.clone(),
            state: Mutex::new(CoreState::new(server.now())),
            read_half: Mutex::new(Some(read_half)),
            write_half: Arc::new(AsyncMutex::new(Some(write_half))),
            close_notify: Arc::new(Notify::new()),
            closed: AtomicBool::new(false),
            weak_self: Mutex::new(std::sync::Weak::new()),
        });
        *conn.weak_self.lock().unwrap() = Arc::downgrade(&conn);
        conn
    }

    fn arc_self(&self) -> Arc<TcpTransitConnection> {
        self.weak_self
            .lock()
            .unwrap()
            .upgrade()
            .expect("connection handled through its Arc")
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn send_bytes(&self, data: &[u8]) {
        let mut guard = self.write_half.lock().await;
        let Some(wh) = guard.as_mut() else {
            return; // already closed
        };
        match timeout(WRITE_TIMEOUT, wh.write_all(data)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("transit: write to peer failed: {}", e);
                // the peer is wedged; close our side so the read loop exits
                // and connection_lost tears down both ends
                drop(guard);
                self.close().await;
            }
            Err(_) => {
                eprintln!("transit: write to peer timed out");
                drop(guard);
                self.close().await;
            }
        }
    }

    /// Close our side of the connection exactly once (FIN) and wake the
    /// read loop so it can tear down.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close_notify.notify_waiters();
        let mut guard = self.write_half.lock().await;
        if let Some(wh) = guard.as_mut() {
            let _ = wh.shutdown().await;
        }
        *guard = None;
    }

    /// Entry point for the connection's task: run the state machine, then
    /// record usage / tear down (the equivalent of the reference's
    /// `defer c.connectionLost()`).
    pub async fn handle(self: Arc<Self>) {
        // A panic must never take down the whole relay; log and fall
        // through to connection_lost.
        let result = std::panic::AssertUnwindSafe(self.clone().run())
            .catch_unwind()
            .await;
        if result.is_err() {
            eprintln!("relay: panic in TCP connection handler (connection torn down)");
        }
        self.connection_lost().await;
    }

    async fn run(self: Arc<Self>) {
        let mut read_half = self
            .read_half
            .lock()
            .unwrap()
            .take()
            .expect("handle called twice");
        let mut buf = vec![0u8; 65536];
        let mut handshake_buf: Vec<u8> = Vec::with_capacity(128);

        loop {
            let (state_done, raw) = {
                let s = self.state.lock().unwrap();
                (s.state == TransitServerState::Done, s.raw_mode)
            };
            if state_done || self.is_closed() {
                return;
            }

            let n = if raw {
                tokio::select! {
                    res = read_half.read(&mut buf) => match res {
                        Ok(0) => return,       // EOF
                        Ok(n) => n,
                        Err(_) => return,
                    },
                    _ = self.close_notify.notified() => return,
                }
            } else {
                tokio::select! {
                    res = timeout(PRE_HANDSHAKE_READ_TIMEOUT, read_half.read(&mut buf)) => match res {
                        Ok(Ok(0)) => return,   // EOF
                        Ok(Ok(n)) => n,
                        Ok(Err(_)) => return,
                        Err(_) => return,       // pre-handshake read timeout
                    },
                    _ = self.close_notify.notified() => return,
                }
            };

            let chunk = &buf[..n];

            // Re-check raw mode now that we have data, because got_partner
            // may have flipped it since last iteration.
            let raw = self.state.lock().unwrap().raw_mode;
            if raw {
                self.handle_data(chunk).await;
                continue;
            }

            // Pre-handshake: accumulate bytes until we see a newline (or
            // until raw_mode flips). A handshake line is ~105 bytes;
            // anything beyond MAX_HANDSHAKE_BYTES is rejected.
            handshake_buf.extend_from_slice(chunk);
            if handshake_buf.len() > MAX_HANDSHAKE_BYTES {
                self.bad_token().await;
                return;
            }
            while let Some(idx) = handshake_buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = handshake_buf.drain(..=idx).collect();
                let line = trim_ascii(&line[..line.len() - 1]).to_vec();
                self.handle_line(&line).await;

                let (state_done, raw, rest) = {
                    let s = self.state.lock().unwrap();
                    let done = s.state == TransitServerState::Done;
                    let raw = s.raw_mode;
                    let rest = std::mem::take(&mut handshake_buf);
                    (done, raw, rest)
                };
                if raw && !rest.is_empty() {
                    self.handle_data(&rest).await;
                }
                if state_done {
                    return;
                }
            }
        }
    }

    async fn handle_line(&self, line: &[u8]) {
        match parse_handshake(line) {
            Handshake::Legacy(token) => self.please_relay(token, String::new()).await,
            Handshake::Sided(token, side) => self.please_relay(token, side).await,
            Handshake::Invalid => self.bad_token().await,
        }
    }

    async fn please_relay(&self, token: String, side: String) {
        {
            let mut s = self.state.lock().unwrap();
            s.token = token.clone();
            s.side = side.clone();
            s.mood = Mood::Lonely;
            // Valid handshake -> switch to raw byte reading immediately,
            // like the reference's lineReceived() calling setRawMode()
            // before the partner is found. This is what makes
            // "got_bytes" -> "impatient" work.
            s.raw_mode = true;
        }

        let me = PendingClientRef::Tcp(self.arc_self());
        let first = self.server.pending_register(&token, &side, me).await;
        {
            let mut s = self.state.lock().unwrap();
            s.first = first;
            if first && s.state != TransitServerState::Relaying {
                s.state = TransitServerState::WaitPartner;
            }
            // else we stay in Relaying (partner already set it)
        }
    }

    async fn bad_token(&self) {
        {
            let mut s = self.state.lock().unwrap();
            s.mood = Mood::Errory;
            s.state = TransitServerState::Done;
        }
        self.send_bytes(b"bad handshake\n").await;
        self.close().await;
        self.record_usage();
    }

    async fn got_partner(&self, other: PendingClientRef) {
        {
            let mut s = self.state.lock().unwrap();
            s.buddy = Some(other);
            s.mood = Mood::Happy;
            s.state = TransitServerState::Relaying;
            s.raw_mode = true;
        }
        self.send_bytes(b"ok\n").await;
    }

    async fn partner_connection_lost(&self) {
        let was_waiting = {
            let mut s = self.state.lock().unwrap();
            if s.state == TransitServerState::WaitPartner {
                s.mood = Mood::Redundant;
                s.state = TransitServerState::Done;
                true
            } else {
                false
            }
        };
        if !was_waiting {
            return;
        }
        self.close().await;
        self.record_usage();
    }

    async fn handle_data(&self, data: &[u8]) {
        enum Outcome {
            Forward(Option<PendingClientRef>),
            Impatient { token: String, side: String },
        }
        let outcome = {
            let mut s = self.state.lock().unwrap();
            if s.state != TransitServerState::Relaying {
                let token = s.token.clone();
                let side = s.side.clone();
                s.mood = Mood::Impatient;
                s.state = TransitServerState::Done;
                Outcome::Impatient { token, side }
            } else {
                if s.buddy.is_some() {
                    s.total_sent += data.len() as u64;
                }
                Outcome::Forward(s.buddy.clone())
            }
        };
        match outcome {
            Outcome::Forward(Some(b)) => b.send_bytes(data).await,
            Outcome::Forward(None) => {}
            Outcome::Impatient { token, side } => {
                self.send_bytes(b"impatient\n").await;
                self.close().await;
                self.server.pending_unregister(&token, &side, self.id);
                self.record_usage();
            }
        }
    }

    async fn connection_lost(&self) {
        // Close our own side exactly once; nothing else does this on
        // teardown.
        self.close().await;

        let (record, unregister, buddy_to_close) = {
            let mut s = self.state.lock().unwrap();
            if s.state == TransitServerState::Done {
                return;
            }
            match s.state {
                TransitServerState::Listening => {
                    // the reference records errory for a connection lost
                    // before the state machine ever saw the client, and
                    // records no usage
                    s.mood = Mood::Errory;
                    s.state = TransitServerState::Done;
                    (false, None, None)
                }
                TransitServerState::WaitRelay => {
                    // connected, but never sent a handshake: the reference
                    // keeps the initial mood ("empty") and records the usage
                    s.state = TransitServerState::Done;
                    (true, None, None)
                }
                TransitServerState::WaitPartner => {
                    s.mood = Mood::Lonely;
                    let t = (s.token.clone(), s.side.clone());
                    s.state = TransitServerState::Done;
                    (true, Some(t), None)
                }
                TransitServerState::Relaying => {
                    s.mood = if s.first { Mood::Happy } else { Mood::Jilted };
                    let t = (s.token.clone(), s.side.clone());
                    let buddy = s.buddy.clone();
                    s.state = TransitServerState::Done;
                    (true, Some(t), buddy)
                }
                TransitServerState::Done => return,
            }
        };

        if let Some((token, side)) = &unregister {
            self.server.pending_unregister(token, side, self.id);
        }
        if let Some(b) = buddy_to_close {
            b.close().await;
        }
        if record {
            self.record_usage();
        }
    }

    fn record_usage(&self) {
        let s = self.state.lock().unwrap();
        let started = s.started;
        let mood = s.mood;
        let total_sent = s.total_sent;
        let buddy = s.buddy.clone();
        drop(s);
        self.server
            .record_usage(started, buddy.as_ref(), mood, total_sent);
    }
}

// ---------------------------------------------------------------------------
// WebSocket transport
// ---------------------------------------------------------------------------

pub struct WebSocketTransitConnection {
    id: u64,
    server: Arc<TransitServer>,
    state: Mutex<CoreState>,
    read_stream: Mutex<Option<SplitStream<WebSocketStream<TcpStream>>>>,
    sink: Arc<AsyncMutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    close_notify: Arc<Notify>,
    closed: AtomicBool,
    weak_self: Mutex<std::sync::Weak<WebSocketTransitConnection>>,
}

impl WebSocketTransitConnection {
    pub fn new(stream: WebSocketStream<TcpStream>, server: Arc<TransitServer>) -> Arc<Self> {
        let id = next_conn_id();
        let (sink, read_stream) = stream.split();
        let conn = Arc::new(WebSocketTransitConnection {
            id,
            server: server.clone(),
            state: Mutex::new(CoreState::new(server.now())),
            read_stream: Mutex::new(Some(read_stream)),
            sink: Arc::new(AsyncMutex::new(sink)),
            close_notify: Arc::new(Notify::new()),
            closed: AtomicBool::new(false),
            weak_self: Mutex::new(std::sync::Weak::new()),
        });
        *conn.weak_self.lock().unwrap() = Arc::downgrade(&conn);
        conn
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn arc_self(&self) -> Arc<WebSocketTransitConnection> {
        self.weak_self
            .lock()
            .unwrap()
            .upgrade()
            .expect("connection handled through its Arc")
    }

    async fn send_bytes(&self, data: &[u8]) {
        let mut guard = self.sink.lock().await;
        let msg = Message::Binary(Bytes::copy_from_slice(data));
        match timeout(WRITE_TIMEOUT, guard.send(msg)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("transit: write to websocket peer failed: {}", e);
                drop(guard);
                self.close().await;
            }
            Err(_) => {
                eprintln!("transit: write to websocket peer timed out");
                drop(guard);
                self.close().await;
            }
        }
    }

    /// Close our side of the connection exactly once (best-effort Close
    /// frame, then FIN) and wake the read loop so it can tear down.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close_notify.notify_waiters();
        let mut guard = self.sink.lock().await;
        let close = Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: Utf8Bytes::from_static(""),
        }));
        let _ = timeout(CLOSE_WRITE_TIMEOUT, guard.send(close)).await;
        let _ = timeout(CLOSE_WRITE_TIMEOUT, guard.close()).await;
    }

    /// Entry point for the connection's task: run the state machine, then
    /// record usage / tear down (the equivalent of the reference's
    /// `defer c.connectionLost()`).
    pub async fn handle(self: Arc<Self>) {
        let result = std::panic::AssertUnwindSafe(self.clone().run())
            .catch_unwind()
            .await;
        if result.is_err() {
            eprintln!("relay: panic in WebSocket connection handler (connection torn down)");
        }
        self.connection_lost().await;
    }

    async fn run(self: Arc<Self>) {
        let mut stream = self
            .read_stream
            .lock()
            .unwrap()
            .take()
            .expect("handle called twice");

        loop {
            let (state_done, first) = {
                let s = self.state.lock().unwrap();
                (s.state == TransitServerState::Done, s.first_msg)
            };
            if state_done || self.is_closed() {
                return;
            }

            // Deadline applies only until the handshake message arrives; a
            // paired transfer may be idle between messages.
            let msg = if first {
                tokio::select! {
                    res = timeout(PRE_HANDSHAKE_READ_TIMEOUT, stream.next()) => match res {
                        Ok(Some(Ok(msg))) => msg,
                        Ok(Some(Err(_))) => return,
                        Ok(None) => return,     // peer hung up
                        Err(_) => return,       // pre-handshake read timeout
                    },
                    _ = self.close_notify.notified() => return,
                }
            } else {
                tokio::select! {
                    res = stream.next() => match res {
                        Some(Ok(msg)) => msg,
                        Some(Err(_)) => return,
                        None => return,
                    },
                    _ = self.close_notify.notified() => return,
                }
            };

            match msg {
                Message::Binary(_) | Message::Text(_) => {
                    let data = match &msg {
                        Message::Binary(d) => d.as_ref(),
                        Message::Text(d) => d.as_bytes(),
                        _ => unreachable!(),
                    };
                    self.handle_message(data, first).await;
                }
                Message::Ping(payload) => {
                    let mut guard = self.sink.lock().await;
                    let _ = timeout(WRITE_TIMEOUT, guard.send(Message::Pong(payload))).await;
                }
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Close(_) => return,
            }
        }
    }

    async fn handle_message(&self, data: &[u8], first: bool) {
        if first {
            // The first message carries the handshake. Trailing line-ending
            // whitespace is tolerated: the Python and Rust clients send the
            // `please relay ...\n` literal, which is the protocol-specified
            // form of the relay handshake.
            let line = trim_ascii(data);
            match parse_handshake(line) {
                Handshake::Legacy(token) => self.please_relay(token, String::new()).await,
                Handshake::Sided(token, side) => self.please_relay(token, side).await,
                Handshake::Invalid => self.bad_token().await,
            }
            // first handshake consumed
            self.state.lock().unwrap().first_msg = false;
            return;
        }
        self.handle_data(data).await;
    }

    async fn please_relay(&self, token: String, side: String) {
        {
            let mut s = self.state.lock().unwrap();
            s.token = token.clone();
            s.side = side.clone();
            s.mood = Mood::Lonely;
        }

        let me = PendingClientRef::Ws(self.arc_self());
        let first = self.server.pending_register(&token, &side, me).await;
        let mut s = self.state.lock().unwrap();
        s.first = first;
        if first {
            s.state = TransitServerState::WaitPartner;
        }
        // else we stay in Relaying (partner already set it)
    }

    async fn bad_token(&self) {
        {
            let mut s = self.state.lock().unwrap();
            s.mood = Mood::Errory;
            s.state = TransitServerState::Done;
        }
        self.send_bytes(b"bad handshake").await;
        self.close().await;
        self.record_usage();
    }

    async fn got_partner(&self, other: PendingClientRef) {
        {
            let mut s = self.state.lock().unwrap();
            s.buddy = Some(other);
            s.mood = Mood::Happy;
            s.state = TransitServerState::Relaying;
        }
        self.send_bytes(b"ok\n").await;
    }

    async fn partner_connection_lost(&self) {
        let was_waiting = {
            let mut s = self.state.lock().unwrap();
            if s.state == TransitServerState::WaitPartner {
                s.mood = Mood::Redundant;
                s.state = TransitServerState::Done;
                true
            } else {
                false
            }
        };
        if !was_waiting {
            return;
        }
        self.close().await;
        self.record_usage();
    }

    async fn handle_data(&self, data: &[u8]) {
        enum Outcome {
            Forward(Option<PendingClientRef>),
            Impatient { token: String, side: String },
        }
        let outcome = {
            let mut s = self.state.lock().unwrap();
            if s.state != TransitServerState::Relaying {
                let token = s.token.clone();
                let side = s.side.clone();
                s.mood = Mood::Impatient;
                s.state = TransitServerState::Done;
                Outcome::Impatient { token, side }
            } else {
                if s.buddy.is_some() {
                    s.total_sent += data.len() as u64;
                }
                Outcome::Forward(s.buddy.clone())
            }
        };
        match outcome {
            Outcome::Forward(Some(b)) => b.send_bytes(data).await,
            Outcome::Forward(None) => {}
            Outcome::Impatient { token, side } => {
                self.send_bytes(b"impatient\n").await;
                self.close().await;
                self.server.pending_unregister(&token, &side, self.id);
                self.record_usage();
            }
        }
    }

    async fn connection_lost(&self) {
        self.close().await;

        let (record, unregister, buddy_to_close) = {
            let mut s = self.state.lock().unwrap();
            if s.state == TransitServerState::Done {
                return;
            }
            match s.state {
                TransitServerState::WaitRelay => {
                    // connected, but never sent a handshake: the reference
                    // keeps the initial mood ("empty")
                    s.state = TransitServerState::Done;
                    (true, None, None)
                }
                TransitServerState::WaitPartner => {
                    s.mood = Mood::Lonely;
                    let t = (s.token.clone(), s.side.clone());
                    s.state = TransitServerState::Done;
                    (true, Some(t), None)
                }
                TransitServerState::Relaying => {
                    s.mood = if s.first { Mood::Happy } else { Mood::Jilted };
                    let t = (s.token.clone(), s.side.clone());
                    let buddy = s.buddy.clone();
                    s.state = TransitServerState::Done;
                    (true, Some(t), buddy)
                }
                TransitServerState::Listening | TransitServerState::Done => {
                    s.mood = Mood::Errory;
                    s.state = TransitServerState::Done;
                    (true, None, None)
                }
            }
        };

        if let Some((token, side)) = &unregister {
            self.server.pending_unregister(token, side, self.id);
        }
        if let Some(b) = buddy_to_close {
            b.close().await;
        }
        if record {
            self.record_usage();
        }
    }

    fn record_usage(&self) {
        let s = self.state.lock().unwrap();
        let started = s.started;
        let mood = s.mood;
        let total_sent = s.total_sent;
        let buddy = s.buddy.clone();
        drop(s);
        self.server
            .record_usage(started, buddy.as_ref(), mood, total_sent);
    }
}

/// Handle a WebSocket connection.
pub async fn handle_websocket(ws: WebSocketStream<TcpStream>, transit: Arc<TransitServer>) {
    let conn = WebSocketTransitConnection::new(ws, transit);
    conn.handle().await;
}

/// Handle a TCP connection.
pub async fn handle_tcp_connection(stream: TcpStream, transit: Arc<TransitServer>) {
    let conn = TcpTransitConnection::new(stream, transit);
    conn.handle().await;
}
