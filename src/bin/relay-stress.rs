//! relay-stress: a load generator for transit relays. It speaks the wire
//! protocol directly (handshake + byte pipe), so it can exercise any
//! implementation — the Rust relay in this repository, the Go relay
//! (github.com/dougsko/wormhole-relay), the Python reference
//! (magic-wormhole-transit-relay), or a production relay.
//!
//! Modes:
//!   throughput  open --pairs concurrent pairs, push --bytes through each,
//!               report aggregate MB/s and per-pair latency percentiles
//!   churn       connect/handshake/close as fast as possible for --duration
//!               seconds, report completed pairs/sec (pairing-churn load)
//!   stall       one pair; the receiver never reads; observe whether the
//!               relay applies backpressure / closes the stuck pipe
//!               (behavior shaped by the write-deadline fixes)
//!
//! Example:
//!   relay-stress --addr 127.0.0.1:4001 --mode throughput --pairs 8 --bytes 16777216
//!
//! Exit code is non-zero if any pair errors out.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use rand::RngCore;

#[derive(Parser)]
#[command(
    name = "relay-stress",
    version,
    about = "Load generator for magic-wormhole transit relays"
)]
struct Args {
    /// relay address (host:port)
    #[arg(long, default_value = "127.0.0.1:4001")]
    addr: String,
    /// throughput | churn | stall
    #[arg(long, default_value = "throughput")]
    mode: String,
    /// concurrent pairs (throughput, churn)
    #[arg(long, default_value_t = 8)]
    pairs: usize,
    /// bytes to relay per pair (throughput)
    #[arg(long, default_value_t = 16 << 20)]
    bytes: u64,
    /// write chunk size (throughput)
    #[arg(long, default_value_t = 64 << 10)]
    chunk: usize,
    /// run duration (churn), e.g. "10s", "500ms"
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    duration: Duration,
    /// how long to watch the stalled pipe (stall), e.g. "15s"
    #[arg(long, default_value = "15s", value_parser = parse_duration)]
    stall_timeout: Duration,
    /// TCP dial/handshake timeout, e.g. "5s"
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    dial_timeout: Duration,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1000u64)
    } else if let Some(n) = s.strip_suffix("us") {
        (n, 0) // handled specially below
    } else {
        (s, 1000u64) // bare number = seconds
    };
    if s.ends_with("us") {
        let v: u64 = num
            .parse()
            .map_err(|_| format!("invalid duration {:?}", s))?;
        return Ok(Duration::from_micros(v));
    }
    let v: f64 = num
        .parse()
        .map_err(|_| format!("invalid duration {:?}", s))?;
    Ok(Duration::from_millis((v * mult as f64) as u64))
}

fn random_token() -> String {
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Dial both ends of a pair, perform the token handshake on each, and wait
/// for "ok\n". Returns the two connections.
fn handshake(
    addr: &str,
    token: &str,
    dial_timeout: Duration,
) -> Result<(TcpStream, TcpStream), String> {
    let sock_addr = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve {}: {}", addr, e))?
        .next()
        .ok_or_else(|| format!("resolve {}: no addresses", addr))?;

    let c1 = TcpStream::connect_timeout(&sock_addr, dial_timeout)
        .map_err(|e| format!("dial side 1: {}", e))?;
    let c2 = TcpStream::connect_timeout(&sock_addr, dial_timeout).map_err(|e| {
        let _ = c1.shutdown(std::net::Shutdown::Both);
        format!("dial side 2: {}", e)
    })?;

    let line = format!("please relay {}\n", token);
    for c in [&c1, &c2] {
        c.set_read_timeout(Some(dial_timeout)).ok();
        c.set_write_timeout(Some(dial_timeout)).ok();
        let mut c = c;
        c.write_all(line.as_bytes())
            .map_err(|e| format!("handshake write: {}", e))?;
    }
    for c in [&c1, &c2] {
        let mut ok = [0u8; 3];
        if let Err(e) = (&*c).read_exact(&mut ok) {
            let _ = c1.shutdown(std::net::Shutdown::Both);
            let _ = c2.shutdown(std::net::Shutdown::Both);
            return Err(format!("reading ok: {}", e));
        }
        if &ok != b"ok\n" {
            let _ = c1.shutdown(std::net::Shutdown::Both);
            let _ = c2.shutdown(std::net::Shutdown::Both);
            return Err(format!("unexpected handshake response {:?}", ok));
        }
    }
    for c in [&c1, &c2] {
        c.set_read_timeout(None).ok();
        c.set_write_timeout(None).ok();
    }
    Ok((c1, c2))
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = (p * (sorted.len() - 1) as f64) as usize;
    sorted[idx]
}

fn run_throughput(args: &Args) -> i32 {
    let mut payload = vec![0u8; args.chunk];
    rand::thread_rng().fill_bytes(&mut payload);

    let total_bytes = Arc::new(AtomicI64::new(0));
    let err_count = Arc::new(AtomicI64::new(0));
    let durations = Arc::new(Mutex::new(Vec::with_capacity(args.pairs)));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.pairs);
    for id in 0..args.pairs {
        let payload = payload.clone();
        let addr = args.addr.clone();
        let bytes_per = args.bytes;
        let dial_timeout = args.dial_timeout;
        let total_bytes = total_bytes.clone();
        let err_count = err_count.clone();
        let durations = durations.clone();
        handles.push(std::thread::spawn(move || {
            let pair_start = Instant::now();
            let token = random_token();
            let (c1, c2) = match handshake(&addr, &token, dial_timeout) {
                Ok(p) => p,
                Err(e) => {
                    err_count.fetch_add(1, Ordering::Relaxed);
                    eprintln!("pair {}: {}", id, e);
                    return;
                }
            };

            let bytes_wanted = bytes_per;
            let writer = std::thread::spawn(move || -> Result<(), String> {
                let mut c1 = c1;
                let mut remaining = bytes_wanted;
                while remaining > 0 {
                    let n = (payload.len() as u64).min(remaining);
                    c1.set_write_timeout(Some(Duration::from_secs(120))).ok();
                    c1.write_all(&payload[..n as usize])
                        .map_err(|e| format!("write: {}", e))?;
                    remaining -= n;
                }
                Ok(())
            });
            let reader = std::thread::spawn(move || -> Result<(), String> {
                let mut c2 = c2;
                let mut remaining = bytes_per;
                let mut sink = vec![0u8; 128 << 10];
                while remaining > 0 {
                    c2.set_read_timeout(Some(Duration::from_secs(120))).ok();
                    let n = c2.read(&mut sink).map_err(|e| format!("read: {}", e))?;
                    if n == 0 {
                        return Err("unexpected EOF".to_string());
                    }
                    remaining -= n as u64;
                }
                Ok(())
            });

            let write_res = writer
                .join()
                .unwrap_or_else(|_| Err("writer panicked".to_string()));
            let read_res = reader
                .join()
                .unwrap_or_else(|_| Err("reader panicked".to_string()));
            match (write_res, read_res) {
                (Ok(()), Ok(())) => {
                    durations.lock().unwrap().push(pair_start.elapsed());
                    total_bytes.fetch_add(bytes_per as i64, Ordering::Relaxed);
                }
                (Err(e), _) => {
                    err_count.fetch_add(1, Ordering::Relaxed);
                    eprintln!("pair {} write: {}", id, e);
                }
                (_, Err(e)) => {
                    err_count.fetch_add(1, Ordering::Relaxed);
                    eprintln!("pair {} read: {}", id, e);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let wall = start.elapsed();

    let total = total_bytes.load(Ordering::Relaxed) as f64;
    let errs = err_count.load(Ordering::Relaxed);
    let mut ds = durations.lock().unwrap().clone();
    ds.sort();

    println!(
        "mode=throughput addr={} pairs={} bytes_per_pair={} wall={}ms",
        args.addr,
        args.pairs,
        args.bytes,
        wall.as_millis()
    );
    println!(
        "aggregate: {:.1} MB/s ({} bytes in {}ms), errors={}",
        total / wall.as_secs_f64() / (1 << 20) as f64,
        total as i64,
        wall.as_millis(),
        errs
    );
    if !ds.is_empty() {
        println!(
            "pair latency: min={}ms p50={}ms p99={}ms max={}ms",
            ds[0].as_millis(),
            percentile(&ds, 0.50).as_millis(),
            percentile(&ds, 0.99).as_millis(),
            ds[ds.len() - 1].as_millis()
        );
    }
    if errs > 0 {
        1
    } else {
        0
    }
}

fn run_churn(args: &Args) -> i32 {
    let stop = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicI64::new(0));
    let err_count = Arc::new(AtomicI64::new(0));

    // wall-clock deadline, checked by each worker
    let deadline = Instant::now() + args.duration;
    let mut handles = Vec::with_capacity(args.pairs);
    for _ in 0..args.pairs {
        let addr = args.addr.clone();
        let dial_timeout = args.dial_timeout;
        let completed = completed.clone();
        let err_count = err_count.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                let token = random_token();
                match handshake(&addr, &token, dial_timeout) {
                    Ok((c1, c2)) => {
                        let _ = c1.shutdown(std::net::Shutdown::Both);
                        let _ = c2.shutdown(std::net::Shutdown::Both);
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        err_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let completed = completed.load(Ordering::Relaxed);
    let errs = err_count.load(Ordering::Relaxed);
    let rate = completed as f64 / args.duration.as_secs_f64();
    println!(
        "mode=churn addr={} workers={} duration={}ms",
        args.addr,
        args.pairs,
        args.duration.as_millis()
    );
    println!(
        "completed pairs={} rate={:.1} pairs/s errors={}",
        completed, rate, errs
    );
    if errs > 0 {
        1
    } else {
        0
    }
}

fn run_stall(args: &Args) -> i32 {
    let (c1, c2) = match handshake(&args.addr, &random_token(), args.dial_timeout) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("handshake: {}", e);
            return 1;
        }
    };
    // c2 deliberately never reads: this is the stuck-receiver scenario.
    let _keep_c2_open = c2;

    let mut payload = vec![0u8; 64 << 10];
    rand::thread_rng().fill_bytes(&mut payload);

    let start = Instant::now();
    let mut written: u64 = 0;
    let mut blocked_at: Option<Duration> = None;

    while start.elapsed() < args.stall_timeout {
        let mut c1 = c1.try_clone().expect("try_clone");
        c1.set_write_timeout(Some(Duration::from_millis(500))).ok();
        match c1.write(&payload) {
            Ok(n) => {
                written += n as u64;
            }
            Err(e) => {
                let is_timeout = e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut;
                if is_timeout {
                    // Write-side stalled: are we connected to a dead-but-open
                    // pipe, or did the relay close on us? A real remote close
                    // yields EOF/ECONNRESET; our own read timeout expiring
                    // means the pipe is still open.
                    let mut probe = match c1.try_clone() {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    probe.set_read_timeout(Some(Duration::from_secs(2))).ok();
                    let mut one = [0u8; 1];
                    let rerr = probe.read(&mut one);
                    blocked_at = Some(start.elapsed());
                    match rerr {
                        Ok(0) | Err(_) => {
                            // EOF/reset => relay closed; or timeout (error
                            // kind WouldBlock/TimedOut) => pipe still open.
                            match rerr {
                                Err(ref e)
                                    if e.kind() == std::io::ErrorKind::WouldBlock
                                        || e.kind() == std::io::ErrorKind::TimedOut =>
                                {
                                    break
                                }
                                _ => {
                                    println!("mode=stall addr={}", args.addr);
                                    println!(
                                        "relay closed the stuck pipe after ~{} bytes (blocked after {}ms, read err: {:?})",
                                        written,
                                        blocked_at.map(|d| d.as_millis()).unwrap_or(0),
                                        rerr.err()
                                    );
                                    return 0;
                                }
                            }
                        }
                        Ok(_) => {}
                    }
                    break;
                }
                // hard error: relay closed or reset the connection
                println!("mode=stall addr={}", args.addr);
                println!(
                    "relay closed/reset the pipe after ~{} bytes (write err: {}, after {}ms)",
                    written,
                    e,
                    start.elapsed().as_millis()
                );
                return 0;
            }
        }
    }

    let blocked_at = blocked_at.unwrap_or_else(|| start.elapsed());
    println!("mode=stall addr={}", args.addr);
    println!(
        "sender still blocked after {}ms with ~{} bytes written; relay did not close the pipe within {}ms",
        blocked_at.as_millis(),
        written,
        args.stall_timeout.as_millis()
    );
    println!("(this is the expected outcome for a relay without write deadlines / backpressure handling)");
    0
}

fn main() {
    let args = Args::parse();
    let code = match args.mode.as_str() {
        "throughput" => run_throughput(&args),
        "churn" => run_churn(&args),
        "stall" => run_stall(&args),
        other => {
            eprintln!("unknown mode {:?}", other);
            2
        }
    };
    std::process::exit(code);
}
