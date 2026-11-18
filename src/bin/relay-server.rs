//! relay-server: the magic-wormhole transit relay server.
//!
//! Option names and short flags match the Python `twist transitrelay` plugin
//! (and the Go implementation), and the endpoints use the same
//! `tcp:PORT[:interface=ADDR]` syntax as twisted endpoint strings.

use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::net::TcpListener;
use tokio::signal;
use tokio_tungstenite::accept_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use wormhole_relay::endpoint::parse_endpoint;
use wormhole_relay::relay::{handle_websocket, TransitServer, MAX_WS_MESSAGE_BYTES};
use wormhole_relay::usage::UsageTracker;

const DEFAULT_PORT: &str = "tcp:4001:interface=::";

const HELP: &str = "\
Usage: relay-server [options]

Options:
  -p, --port <endpoint>          endpoint to listen on [tcp:4001:interface=::]
  -w, --websocket <endpoint>     endpoint to listen for WebSocket connections
  -u, --websocket-url <url>      WebSocket URL (derived from endpoint if not provided)
      --blur-usage <seconds>     blur timestamps and data sizes in logs (0 = off)
      --log-fd <fd-or-path>      write JSON usage logs to this file descriptor (a number, or a path)
      --usage-db <path>          record usage data (SQLite)
  -h, --help                     show this help and exit
";

#[derive(Default)]
struct Config {
    port: Option<String>,
    websocket: String,
    websocket_url: String,
    blur_usage: i64,
    log_fd: String,
    usage_db: String,
}

fn take_value(
    flag: &str,
    inline: Option<String>,
    args: &mut std::vec::IntoIter<String>,
) -> Result<String, String> {
    match inline {
        Some(v) => Ok(v),
        None => args
            .next()
            .ok_or_else(|| format!("flag -{} needs a value", flag)),
    }
}

fn parse_args() -> Result<Config, String> {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if !arg.starts_with('-') {
            return Err(format!("unexpected argument {:?}", arg));
        }
        let stripped = arg.trim_start_matches('-');
        let (name, inline) = match stripped.split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (stripped.to_string(), None),
        };
        match name.as_str() {
            "port" | "p" => cfg.port = Some(take_value("port", inline, &mut args)?),
            "websocket" | "w" => cfg.websocket = take_value("websocket", inline, &mut args)?,
            "websocket-url" | "u" => {
                cfg.websocket_url = take_value("websocket-url", inline, &mut args)?
            }
            "blur-usage" => {
                let v = take_value("blur-usage", inline, &mut args)?;
                cfg.blur_usage = v
                    .parse::<i64>()
                    .map_err(|_| format!("flag --blur-usage needs an integer, got {:?}", v))?;
            }
            "log-fd" => cfg.log_fd = take_value("log-fd", inline, &mut args)?,
            "usage-db" => cfg.usage_db = take_value("usage-db", inline, &mut args)?,
            "help" | "h" => {
                print!("{}", HELP);
                exit(0);
            }
            _ => return Err(format!("unknown flag -{}; use --help", name)),
        }
    }
    Ok(cfg)
}

/// Back off on repeated accept failures (e.g. fd exhaustion) instead of
/// busy-spinning; returns the delay to sleep before the next attempt.
fn accept_backoff(consecutive_errors: u32) -> Duration {
    let shift = consecutive_errors.saturating_sub(1).min(8);
    let backoff = Duration::from_millis(5u64 << shift);
    backoff.min(Duration::from_secs(1))
}

fn increase_rlimits() {
    // Try to increase RLIMIT_NOFILE so the server can handle many
    // connections. Linux defaults are often (1024, 1048576); macOS reports
    // a huge hard limit but caps the soft limit at 10240.
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 {
            eprintln!("warning: getrlimit(RLIMIT_NOFILE) failed");
            return;
        }
        let (soft, hard) = (rl.rlim_cur, rl.rlim_max);
        if soft >= 10000 {
            eprintln!("RLIMIT_NOFILE soft={} already >= 10000, leaving it", soft);
            return;
        }
        for new_limit in [hard, 10000, 3200, 1024] {
            let new_rl = libc::rlimit {
                rlim_cur: new_limit,
                rlim_max: hard,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &new_rl) == 0 {
                eprintln!(
                    "RLIMIT_NOFILE raised from {} to {} (hard={})",
                    soft, new_limit, hard
                );
                return;
            }
        }
        eprintln!(
            "warning: unable to raise RLIMIT_NOFILE, leaving it at {}",
            soft
        );
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {}", e);
            eprintln!("Try --help.");
            exit(2);
        }
    };
    let mut cfg = cfg;
    if cfg.port.is_none() {
        cfg.port = Some(DEFAULT_PORT.to_string());
    }

    // Create usage tracker
    let usage = match UsageTracker::create(cfg.blur_usage, &cfg.log_fd, &cfg.usage_db) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("Failed to create usage tracker: {}", e);
            exit(1);
        }
    };
    let usage = Arc::new(usage);

    // Create transit server
    let transit = TransitServer::new(usage.clone(), SystemTime::now);

    // Increase rlimits if possible
    increase_rlimits();

    // Setup TCP listener
    let tcp_endpoint = match parse_endpoint(cfg.port.as_deref().unwrap()) {
        Ok(ep) => ep,
        Err(e) => {
            eprintln!("bad --port: {}", e);
            exit(1);
        }
    };
    let tcp_listener = match TcpListener::bind(&tcp_endpoint.addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to listen on TCP {}: {}", tcp_endpoint.addr, e);
            exit(1);
        }
    };

    // Setup WebSocket listener if configured
    let mut ws_listen = None;
    if !cfg.websocket.is_empty() {
        match parse_endpoint(&cfg.websocket) {
            Ok(ep) => match TcpListener::bind(&ep.addr).await {
                Ok(l) => {
                    if cfg.websocket_url.is_empty() {
                        // derive the WebSocket URL like the Python plugin does
                        cfg.websocket_url = format!("ws://localhost:{}/", ep.port);
                        println!("Using WebSocket URL {:?}", cfg.websocket_url);
                    }
                    ws_listen = Some((l, ep.addr));
                }
                Err(e) => {
                    eprintln!("Failed to listen on WebSocket {}: {}", ep.addr, e);
                    exit(1);
                }
            },
            Err(e) => {
                eprintln!("bad --websocket: {}", e);
                exit(1);
            }
        }
    }

    eprintln!(
        "Transit relay server listening on TCP {}",
        tcp_listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| tcp_endpoint.addr.clone())
    );
    if let Some((l, addr)) = &ws_listen {
        let _ = l;
        eprintln!("Transit relay server listening on WebSocket {}", addr);
    }

    // Start WebSocket accept loop
    if let Some((ws_listener, _)) = ws_listen {
        let transit = transit.clone();
        tokio::spawn(async move {
            let mut accept_errors: u32 = 0;
            loop {
                match ws_listener.accept().await {
                    Ok((stream, _)) => {
                        accept_errors = 0;
                        let _ = stream.set_nodelay(true);
                        let transit = transit.clone();
                        let mut ws_config = WebSocketConfig::default();
                        ws_config.max_message_size = Some(MAX_WS_MESSAGE_BYTES);
                        ws_config.max_frame_size = Some(MAX_WS_MESSAGE_BYTES);
                        tokio::spawn(async move {
                            match accept_async_with_config(stream, Some(ws_config)).await {
                                Ok(ws) => handle_websocket(ws, transit).await,
                                Err(e) => eprintln!("WebSocket upgrade failed: {}", e),
                            }
                        });
                    }
                    Err(e) => {
                        accept_errors += 1;
                        let backoff = accept_backoff(accept_errors);
                        eprintln!(
                            "WebSocket accept error ({} consecutive): {}; retrying in {:?}",
                            accept_errors, e, backoff
                        );
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        });
    }

    // Update stats immediately at startup, then every 5 minutes; mirrors the
    // reference's TimerService(5*60, transit.update_stats), which twisted
    // runs immediately when the service starts. tokio's first interval tick
    // fires immediately, so the startup update happens there.
    {
        let transit = transit.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5 * 60));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                transit.update_stats();
            }
        });
    }

    // Flush usage records on Ctrl-C / SIGTERM.
    {
        let usage = usage.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal as unix_signal, SignalKind};
                let mut term =
                    unix_signal(SignalKind::terminate()).expect("install SIGTERM handler");
                tokio::select! {
                    _ = signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = signal::ctrl_c().await;
            }
            eprintln!("shutting down: flushing usage records");
            let _ = usage.close().await;
            exit(0);
        });
    }

    // Accept TCP connections with backoff on repeated failures; only fatal
    // if the listener itself is gone.
    let mut accept_errors: u32 = 0;
    loop {
        match tcp_listener.accept().await {
            Ok((stream, _)) => {
                accept_errors = 0;
                let _ = stream.set_nodelay(true);
                let transit = transit.clone();
                tokio::spawn(wormhole_relay::relay::handle_tcp_connection(
                    stream, transit,
                ));
            }
            Err(e) => {
                accept_errors += 1;
                if accept_errors > 1000000 {
                    eprintln!(
                        "TCP accept error ({} consecutive): {}; giving up",
                        accept_errors, e
                    );
                    exit(1);
                }
                let backoff = accept_backoff(accept_errors);
                eprintln!(
                    "TCP accept error ({} consecutive): {}; retrying in {:?}",
                    accept_errors, e, backoff
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}
