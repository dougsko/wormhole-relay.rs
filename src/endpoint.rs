//! Parsing of twisted-style listen endpoints, matching the `tcp:PORT[:interface=ADDR]`
//! strings the Python `twist transitrelay` plugin accepts for `--port`/`--websocket`.
//!
//! Supported forms:
//!
//! ```text
//! tcp:PORT
//! tcp:PORT:interface=ADDR
//! PORT
//! HOST:PORT / [HOST]:PORT
//! ```
//!
//! Bare ports and `host:port` are also accepted, as they were by the Go port.
//!
//! Unlike the Go implementation — which split the whole remainder of the endpoint
//! on `:` and therefore mangled IPv6 `interface=` values (everything after the
//! first colon was silently dropped) — the parser here keeps the full remainder
//! after `interface=`, so `tcp:4001:interface=::1` binds `[::1]:4001`.

use std::fmt;
use std::net::IpAddr;

/// A resolved listen endpoint: the address to bind and the numeric port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenEndpoint {
    /// Address string suitable for `std::net::TcpListener::bind` /
    /// `tokio::net::TcpListener::bind`.
    pub addr: String,
    /// The numeric port.
    pub port: u16,
}

/// Error returned when an endpoint string cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointError(pub String);

impl fmt::Display for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for EndpointError {}

fn join_host_port(host: &str, port: u16) -> String {
    // Numeric IPv6 hosts must be bracketed; everything else joins plainly.
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{}]:{}", host, port),
        _ => format!("{}:{}", host, port),
    }
}

fn parse_port(s: &str, whole: &str) -> Result<u16, EndpointError> {
    s.parse::<u16>().map_err(|_| {
        EndpointError(format!(
            "unrecognized endpoint {:?}: bad port {:?}",
            whole, s
        ))
    })
}

/// Parse an endpoint string into a listen address and port.
pub fn parse_endpoint(ep: &str) -> Result<ListenEndpoint, EndpointError> {
    let ep = ep.trim();
    if ep.is_empty() {
        return Err(EndpointError("unrecognized endpoint \"\"".to_string()));
    }

    if !ep.starts_with("tcp:") {
        // Bare port or host:port.
        if ep.chars().all(|c| c.is_ascii_digit()) {
            let port = parse_port(ep, ep)?;
            return Ok(ListenEndpoint {
                addr: format!("[::]:{}", port),
                port,
            });
        }
        let (host, port_str) = split_host_port(ep).ok_or_else(|| {
            EndpointError(format!(
                "unrecognized endpoint {:?} (want tcp:PORT[:interface=ADDR] or host:port)",
                ep
            ))
        })?;
        let port = parse_port(port_str, ep)?;
        Ok(ListenEndpoint {
            addr: join_host_port(host, port),
            port,
        })
    } else {
        let rest = &ep["tcp:".len()..];
        let (port_part, remainder) = match rest.split_once(':') {
            Some((p, r)) => (p, Some(r)),
            None => (rest, None),
        };
        if port_part.is_empty() {
            return Err(EndpointError(format!("unrecognized endpoint {:?}", ep)));
        }
        let port = parse_port(port_part, ep)?;

        let mut iface = String::new();
        if let Some(rem) = remainder {
            // Everything after "interface=" is the interface, verbatim; this
            // keeps IPv6 addresses intact (they contain colons).
            if let Some(idx) = rem.find("interface=") {
                iface = rem[idx + "interface=".len()..].to_string();
            }
        }

        if iface.is_empty() {
            // dual-stack wildcard; see note above
            return Ok(ListenEndpoint {
                addr: format!("[::]:{}", port),
                port,
            });
        }
        let iface = iface.trim_start_matches('[').trim_end_matches(']');
        Ok(ListenEndpoint {
            addr: join_host_port(iface, port),
            port,
        })
    }
}

/// Split `host:port` / `[host]:port`, returning None when the host portion
/// contains an unbracketed colon (i.e. it is not a valid host:port pair).
fn split_host_port(ep: &str) -> Option<(&str, &str)> {
    if let Some(rest) = ep.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let after = rest[end + 1..].strip_prefix(':')?;
        return Some((host, after));
    }
    let (host, port) = ep.rsplit_once(':')?;
    if host.contains(':') || host.is_empty() {
        // Either an unbracketed IPv6 literal ("::1:4001" — rejected, like
        // Go's net.SplitHostPort) or a missing host.
        return None;
    }
    Some((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(ep: &str, addr: &str, port: u16) {
        let got = parse_endpoint(ep).unwrap();
        assert_eq!(
            got,
            ListenEndpoint {
                addr: addr.into(),
                port
            },
            "ep={}",
            ep
        );
    }

    #[test]
    fn bare_port() {
        ok("4001", "[::]:4001", 4001);
    }

    #[test]
    fn twisted_port_only() {
        ok("tcp:4001", "[::]:4001", 4001);
    }

    #[test]
    fn twisted_default_interface() {
        // the Python default endpoint; binds the dual-stack wildcard
        ok("tcp:4001:interface=::", "[::]:4001", 4001);
    }

    #[test]
    fn twisted_ipv4_interface() {
        ok("tcp:4001:interface=127.0.0.1", "127.0.0.1:4001", 4001);
    }

    #[test]
    fn twisted_ipv6_interface() {
        ok("tcp:4001:interface=::1", "[::1]:4001", 4001);
    }

    #[test]
    fn host_port() {
        ok("localhost:4001", "localhost:4001", 4001);
        ok("127.0.0.1:4001", "127.0.0.1:4001", 4001);
    }

    #[test]
    fn bracketed_ipv6_host_port() {
        ok("[::1]:4001", "[::1]:4001", 4001);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_endpoint("").is_err());
        assert!(parse_endpoint("tcp:").is_err());
        assert!(parse_endpoint("tcp:notaport").is_err());
        assert!(parse_endpoint("tcp:99999").is_err());
        assert!(parse_endpoint("::1:4001").is_err()); // unbracketed IPv6, like Go
        assert!(parse_endpoint("host:").is_err());
    }

    #[test]
    fn colon_port_alone_rejected() {
        assert!(parse_endpoint(":4001").is_err());
    }
}
