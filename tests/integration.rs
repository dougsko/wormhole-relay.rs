//! Integration tests for the transit relay over real TCP and WebSocket
//! connections on loopback, mirroring the Go test suite.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

use wormhole_relay::relay::{handle_websocket, Mood, TransitServer, UsageRecorder};
use wormhole_relay::usage::UsageTracker;
use wormhole_relay::usage_db::open_usage_db;

// ---------- mock usage recorder ----------

#[derive(Debug, Clone)]
struct MockEvent {
    #[allow(dead_code)]
    started: SystemTime,
    buddy_started: Option<SystemTime>,
    mood: Mood,
    bytes_sent: u64,
    buddy_bytes: u64,
}

#[derive(Default)]
struct MockUsage {
    events: Mutex<Vec<MockEvent>>,
    updates: Mutex<Vec<(i64, i64, i64)>>,
}

impl UsageRecorder for MockUsage {
    fn record(
        &self,
        started: SystemTime,
        buddy_started: Option<SystemTime>,
        mood: Mood,
        bytes_sent: u64,
        buddy_bytes: u64,
    ) {
        self.events.lock().unwrap().push(MockEvent {
            started,
            buddy_started,
            mood,
            bytes_sent,
            buddy_bytes,
        });
    }

    fn update_stats(
        &self,
        _rebooted: SystemTime,
        _updated: SystemTime,
        connected: i64,
        waiting: i64,
        incomplete_bytes: i64,
    ) {
        self.updates
            .lock()
            .unwrap()
            .push((connected, waiting, incomplete_bytes));
    }
}

struct TestHarness {
    addr: std::net::SocketAddr,
    server: Arc<TransitServer>,
    usage: Arc<MockUsage>,
    _tcp_task: tokio::task::JoinHandle<()>,
}

/// Start a relay on an ephemeral loopback port with a mock usage recorder.
async fn start_relay() -> TestHarness {
    let usage = Arc::new(MockUsage::default());
    let server = TransitServer::new(usage.clone(), SystemTime::now);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tcp_server = server.clone();
    let tcp_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            tokio::spawn(wormhole_relay::relay::handle_tcp_connection(
                stream,
                tcp_server.clone(),
            ));
        }
    });
    TestHarness {
        addr,
        server,
        usage,
        _tcp_task: tcp_task,
    }
}

/// Start a relay with both TCP and WebSocket listeners.
async fn start_ws_relay() -> (TestHarness, std::net::SocketAddr) {
    let usage = Arc::new(MockUsage::default());
    let server = TransitServer::new(usage.clone(), SystemTime::now);

    // TCP side
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();
    let tcp_server = server.clone();
    let tcp_task = tokio::spawn(async move {
        while let Ok((stream, _)) = tcp_listener.accept().await {
            let _ = stream.set_nodelay(true);
            tokio::spawn(wormhole_relay::relay::handle_tcp_connection(
                stream,
                tcp_server.clone(),
            ));
        }
    });

    // WebSocket side
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();
    let ws_server = server.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = ws_listener.accept().await {
            let _ = stream.set_nodelay(true);
            let ws_server = ws_server.clone();
            tokio::spawn(async move {
                let mut cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
                cfg.max_message_size = Some(wormhole_relay::relay::MAX_WS_MESSAGE_BYTES);
                cfg.max_frame_size = Some(wormhole_relay::relay::MAX_WS_MESSAGE_BYTES);
                match tokio_tungstenite::accept_async_with_config(stream, Some(cfg)).await {
                    Ok(ws) => handle_websocket(ws, ws_server).await,
                    Err(e) => eprintln!("ws upgrade failed: {}", e),
                }
            });
        }
    });

    (
        TestHarness {
            addr,
            server,
            usage,
            _tcp_task: tcp_task,
        },
        ws_addr,
    )
}

async fn read_exact_timeout(
    stream: &mut TcpStream,
    buf: &mut [u8],
    dur: Duration,
) -> std::io::Result<()> {
    tokio::time::timeout(dur, stream.read_exact(buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timeout"))??;
    Ok(())
}

async fn connect(addr: std::net::SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

async fn handshake_both(
    a: &mut TcpStream,
    b: &mut TcpStream,
    token: &str,
    side_a: &str,
    side_b: &str,
) {
    let line_a = format!("please relay {} for side {}\n", token, side_a);
    let line_b = format!("please relay {} for side {}\n", token, side_b);
    a.write_all(line_a.as_bytes()).await.unwrap();
    b.write_all(line_b.as_bytes()).await.unwrap();
    let mut ok_a = [0u8; 3];
    let mut ok_b = [0u8; 3];
    read_exact_timeout(a, &mut ok_a, Duration::from_secs(2))
        .await
        .unwrap();
    read_exact_timeout(b, &mut ok_b, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&ok_a, b"ok\n");
    assert_eq!(&ok_b, b"ok\n");
}

async fn wait_for_events(usage: &MockUsage, n: usize, dur: Duration) -> Vec<MockEvent> {
    let deadline = Instant::now() + dur;
    loop {
        let events = usage.events.lock().unwrap().clone();
        if events.len() >= n {
            return events;
        }
        if Instant::now() > deadline {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn token() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{:02x}", b)).collect()
}

// ---------- tests ----------

#[tokio::test]
async fn pairs_and_relays_bytes_bidirectionally() {
    let h = start_relay().await;
    let t = token();
    let mut a = connect(h.addr).await;
    let mut b = connect(h.addr).await;
    handshake_both(&mut a, &mut b, &t, &"1".repeat(16), &"2".repeat(16)).await;

    a.write_all(b"hello b").await.unwrap();
    let mut buf = [0u8; 7];
    read_exact_timeout(&mut b, &mut buf, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&buf, b"hello b");

    b.write_all(b"hello a").await.unwrap();
    read_exact_timeout(&mut a, &mut buf, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&buf, b"hello a");

    drop(a);
    drop(b);

    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert!(!events.is_empty(), "expected usage events");
    let happy = events.iter().filter(|e| e.mood == Mood::Happy).count();
    assert_eq!(
        happy,
        1,
        "exactly one happy usage record expected: {:?}",
        events.iter().map(|e| e.mood).collect::<Vec<_>>()
    );
    let ev = events.iter().find(|e| e.mood == Mood::Happy).unwrap();
    assert_eq!(ev.bytes_sent + ev.buddy_bytes, 14);
    assert!(
        ev.buddy_started.is_some(),
        "paired connections carry a waiting time"
    );
}

#[tokio::test]
async fn empty_connection_records_empty_mood() {
    let h = start_relay().await;
    let a = connect(h.addr).await;
    drop(a); // closed without ever sending a handshake

    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mood, Mood::Empty);
    assert!(events[0].buddy_started.is_none());
}

#[tokio::test]
async fn bad_handshake_records_errory() {
    let h = start_relay().await;
    let mut a = connect(h.addr).await;
    a.write_all(b"this is not a relay handshake\n")
        .await
        .unwrap();
    let mut resp = vec![0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut resp))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&resp[..n], b"bad handshake\n");

    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mood, Mood::Errory);
}

#[tokio::test]
async fn oversized_handshake_is_rejected() {
    let h = start_relay().await;
    let mut a = connect(h.addr).await;
    // > 1024 bytes without a newline
    a.write_all(&vec![b'x'; 1100]).await.unwrap();
    let mut resp = vec![0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut resp))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&resp[..n], b"bad handshake\n");
    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mood, Mood::Errory);
}

#[tokio::test]
async fn lonely_connection_records_lonely_without_waiting_time() {
    let h = start_relay().await;
    let t = token();
    let mut a = connect(h.addr).await;
    let line = format!("please relay {}\n", t); // legacy handshake form
    a.write_all(line.as_bytes()).await.unwrap();
    // close before any partner shows up
    drop(a);

    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mood, Mood::Lonely);
    assert!(
        events[0].buddy_started.is_none(),
        "unpaired connection has no waiting time"
    );
}

#[tokio::test]
async fn impatient_when_data_before_partner() {
    let h = start_relay().await;
    let t = token();
    let mut a = connect(h.addr).await;
    let line = format!("please relay {} for side {}\n", t, "3".repeat(16));
    a.write_all(line.as_bytes()).await.unwrap();
    // data before a partner arrives -> "impatient\n" and the relay hangs up
    a.write_all(b"too soon").await.unwrap();
    let mut resp = vec![0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut resp))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&resp[..n], b"impatient\n");

    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mood, Mood::Impatient);
}

#[tokio::test]
async fn closing_one_side_tears_down_the_other() {
    let h = start_relay().await;
    let t = token();
    let mut a = connect(h.addr).await;
    let mut b = connect(h.addr).await;
    handshake_both(&mut a, &mut b, &t, &"4".repeat(16), &"5".repeat(16)).await;

    // a closes; b's socket must be closed by the relay shortly after
    drop(a);
    let mut buf = [0u8; 1];
    let res = tokio::time::timeout(Duration::from_secs(2), b.read(&mut buf)).await;
    match res {
        Ok(Ok(0)) => {}  // clean FIN
        Ok(Err(_)) => {} // reset; also fine
        Ok(Ok(n)) => panic!("expected EOF from relay, got {} bytes", n),
        Err(_) => panic!("relay did not close the partner's connection in time"),
    }

    // exactly one usage record: the first side's happy event; the jilted
    // event is suppressed
    let _events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let events = h.usage.events.lock().unwrap().clone();
    assert_eq!(
        events.len(),
        1,
        "jilted event should be suppressed: {:?}",
        events.iter().map(|e| e.mood).collect::<Vec<_>>()
    );
    assert_eq!(events[0].mood, Mood::Happy);
}

#[tokio::test]
async fn legacy_handshake_pairs_two_clients() {
    let h = start_relay().await;
    let t = token();
    let mut a = connect(h.addr).await;
    let mut b = connect(h.addr).await;
    // legacy form has no side; empty side matches everything
    let line = format!("please relay {}\n", t);
    a.write_all(line.as_bytes()).await.unwrap();
    b.write_all(line.as_bytes()).await.unwrap();
    let mut ok = [0u8; 3];
    read_exact_timeout(&mut a, &mut ok, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&ok, b"ok\n");
    read_exact_timeout(&mut b, &mut ok, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&ok, b"ok\n");
}

#[tokio::test]
async fn pending_count_and_active_count_track_lifecycle() {
    let h = start_relay().await;
    let t = token();
    let mut a = connect(h.addr).await;
    a.write_all(format!("please relay {}\n", t).as_bytes())
        .await
        .unwrap();
    // wait for the handshake to register
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.server.update_stats();
    let waiting = h.server.pending_count();
    assert_eq!(waiting, 1);
    drop(a);
    let events = wait_for_events(&h.usage, 1, Duration::from_secs(2)).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].mood, Mood::Lonely);
}

// ---------- websocket tests ----------

#[tokio::test]
async fn websocket_pairs_and_relays_binary_messages() {
    let (_h, ws_addr) = start_ws_relay().await;
    let url = format!("ws://{}/", ws_addr);
    let t = token();

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    let (mut ws_b, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();

    // protocol-literal handshake with trailing newline
    ws_a.send(Message::Binary(
        format!("please relay {} for side {}\n", t, "a".repeat(16)).into(),
    ))
    .await
    .unwrap();
    ws_b.send(Message::Binary(
        format!("please relay {} for side {}\n", t, "b".repeat(16)).into(),
    ))
    .await
    .unwrap();

    // both sides get "ok\n"
    let ok_a = ws_a.next().await;
    let ok_b = ws_b.next().await;
    let is_ok = |m: &Option<Result<Message, _>>| matches!(m, Some(Ok(Message::Binary(d))) if d.as_ref() == b"ok\n");
    assert!(is_ok(&ok_a), "side a did not get ok: {:?}", ok_a);
    assert!(is_ok(&ok_b), "side b did not get ok: {:?}", ok_b);

    // relay binary payloads both ways
    ws_a.send(Message::Binary(b"ping".as_ref().into()))
        .await
        .unwrap();
    let msg = ws_b.next().await.unwrap().unwrap();
    assert!(matches!(msg, Message::Binary(d) if d.as_ref() == b"ping"));
    ws_b.send(Message::Binary(b"pong".as_ref().into()))
        .await
        .unwrap();
    let msg = ws_a.next().await.unwrap().unwrap();
    assert!(matches!(msg, Message::Binary(d) if d.as_ref() == b"pong"));
}

#[tokio::test]
async fn websocket_handshake_without_newline_is_accepted() {
    let (_h, ws_addr) = start_ws_relay().await;
    let url = format!("ws://{}/", ws_addr);
    let t = token();

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    let (mut ws_b, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    ws_a.send(Message::Binary(
        format!("please relay {} for side {}", t, "c".repeat(16)).into(),
    ))
    .await
    .unwrap();
    ws_b.send(Message::Binary(
        format!("please relay {} for side {}", t, "d".repeat(16)).into(),
    ))
    .await
    .unwrap();

    let ok_a = ws_a.next().await;
    let ok_b = ws_b.next().await;
    assert!(matches!(ok_a, Some(Ok(Message::Binary(d))) if d.as_ref() == b"ok\n"));
    assert!(matches!(ok_b, Some(Ok(Message::Binary(d))) if d.as_ref() == b"ok\n"));
}

#[tokio::test]
async fn websocket_bad_handshake_gets_error_and_close() {
    let (_h, ws_addr) = start_ws_relay().await;
    let url = format!("ws://{}/", ws_addr);

    let (mut ws_a, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    ws_a.send(Message::Binary(b"nonsense".as_ref().into()))
        .await
        .unwrap();
    let msg = ws_a.next().await.unwrap().unwrap();
    assert!(matches!(msg, Message::Binary(d) if d.as_ref() == b"bad handshake"));
}

#[tokio::test]
async fn tcp_and_websocket_clients_can_be_paired_together() {
    let (_h, ws_addr) = start_ws_relay().await;
    let t = token();

    // TCP client on 6380? no — need the TCP addr; start_ws_relay returns it in TestHarness
    // (the harness addr is the TCP listener)
    let _ = ws_addr;
    // pair a TCP client and a WS client through the same relay
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/", ws_addr).as_str())
        .await
        .unwrap();
    let mut tcp = TcpStream::connect(_h.addr).await.unwrap();
    tcp.write_all(format!("please relay {} for side {}\n", t, "e".repeat(16)).as_bytes())
        .await
        .unwrap();
    ws.send(Message::Binary(
        format!("please relay {} for side {}\n", t, "f".repeat(16)).into(),
    ))
    .await
    .unwrap();

    let mut ok = [0u8; 3];
    read_exact_timeout(&mut tcp, &mut ok, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&ok, b"ok\n");
    let ok_ws = ws.next().await;
    assert!(matches!(ok_ws, Some(Ok(Message::Binary(d))) if d.as_ref() == b"ok\n"));

    // TCP -> WS and WS -> TCP byte flow
    tcp.write_all(b"to websockets").await.unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    assert!(matches!(msg, Message::Binary(d) if d.as_ref() == b"to websockets"));
    ws.send(Message::Binary(b"to tcp".as_ref().into()))
        .await
        .unwrap();
    let mut buf = [0u8; 6];
    read_exact_timeout(&mut tcp, &mut buf, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(&buf, b"to tcp");
}

// ---------- usage tracker / database tests ----------

#[tokio::test]
async fn json_log_writer_emits_parseable_records() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("usage.log");
    let tracker = UsageTracker::create(0, log_path.to_str().unwrap(), "").unwrap();
    tracker.record(SystemTime::now(), None, Mood::Lonely, 0, 0);
    tracker.flush().await;
    let content = std::fs::read_to_string(&log_path).unwrap();
    let line = content.lines().next().expect("one JSON line");
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(v["mood"], "lonely");
    assert_eq!(v["total_bytes"], 0);
    assert!(v["waiting_time"].is_null());
    assert!(v["started"].as_f64().unwrap() > 0.0);
    tracker.close().await.unwrap();
}

#[tokio::test]
async fn usage_database_records_happy_pairing() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("usage.sqlite");
    let tracker = UsageTracker::create(0, "", db_path.to_str().unwrap()).unwrap();
    let started = SystemTime::now();
    tracker.record(
        started,
        Some(started + Duration::from_millis(50)),
        Mood::Happy,
        123,
        456,
    );
    tracker.flush().await;

    let conn = open_usage_db(db_path.to_str().unwrap()).unwrap();
    {
        let conn = conn.lock().unwrap();
        let row: (f64, Option<f64>, i64, String) = conn
            .query_row(
                "SELECT total_time, waiting_time, total_bytes, result FROM usage",
                [],
                |r| {
                    Ok((
                        r.get::<_, f64>(0)?,
                        r.get::<_, Option<f64>>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row.2, 579);
        assert_eq!(row.3, "happy");
        assert!(row.1.is_some());
        let version: i64 = conn
            .query_row("SELECT version FROM version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 1);
        let current_rows: i64 = conn
            .query_row("SELECT count(*) FROM current", [], |r| r.get(0))
            .unwrap();
        assert_eq!(current_rows, 0); // no UpdateStats called yet
    }
    tracker.close().await.unwrap();
}

#[tokio::test]
async fn update_stats_writes_current_row() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("usage.sqlite");
    let db = open_usage_db(db_path.to_str().unwrap()).unwrap();
    let tracker = UsageTracker::create(0, "", db_path.to_str().unwrap()).unwrap();
    tracker.update_stats(SystemTime::now(), SystemTime::now(), 3, 2, 987);
    {
        let conn = db.lock().unwrap();
        let (connected, waiting, incomplete): (i64, i64, i64) = conn
            .query_row(
                "SELECT connected, waiting, incomplete_bytes FROM current",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((connected, waiting, incomplete), (3, 2, 987));
    }
    tracker.close().await.unwrap();
}

#[tokio::test]
async fn existing_database_with_wrong_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("usage.sqlite");
    // initialize a v1 database, then bump the version to simulate an
    // incompatible future schema
    {
        let _db = open_usage_db(db_path.to_str().unwrap()).unwrap();
    }
    {
        let conn = open_usage_db(db_path.to_str().unwrap()).unwrap();
        let conn = conn.lock().unwrap();
        conn.execute("UPDATE version SET version = 999", [])
            .unwrap();
    }
    let err = open_usage_db(db_path.to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("version"),
        "unexpected error: {}",
        err
    );
}
