//! # wormhole-relay
//!
//! A Rust implementation of the [Magic Wormhole](https://github.com/magic-wormhole)
//! transit relay server: a small, protocol-neutral service that pairs two
//! clients which present the same shared token and glues their TCP or
//! WebSocket streams together.
//!
//! The behavior is modeled on the Go implementation
//! (<https://github.com/dougsko/wormhole-relay>); the wire protocol is
//! specified by <https://github.com/magic-wormhole/magic-wormhole-protocols>
//! (see [`transit.md` in that repo](https://github.com/magic-wormhole/magic-wormhole-protocols/blob/master/transit.md),
//! section "Relay Handshake"), which is authoritative for the protocol.
//!
//! ## Layout
//!
//! - [`endpoint`] — twisted-style listen endpoint parsing (`tcp:PORT[:interface=ADDR]`)
//! - [`relay`] — the transit state machine, pairing registry, TCP/WebSocket transports
//! - [`usage`] — asynchronous usage accounting (JSON log and SQLite backends)
//! - [`usage_db`] — usage SQLite database creation and schema management

pub mod endpoint;
pub mod relay;
pub mod usage;
pub mod usage_db;

pub use endpoint::{parse_endpoint, EndpointError, ListenEndpoint};
pub use relay::{TransitServer, UsageRecorder};
pub use usage::{UsageEvent, UsageTracker};
