# Performance notes

The repository ships a protocol-level load generator, `relay-stress`
(`src/bin/relay-stress.rs`), which speaks the transit wire protocol directly
(handshake + byte pipe). It can exercise this Rust relay, the Go relay
(github.com/dougsko/wormhole-relay), the Python reference
(`magic-wormhole-transit-relay`), or a production relay.

## Running

```sh
cargo build --release

# throughput: open N concurrent pairs, push M bytes through each
./target/release/relay-stress --addr 127.0.0.1:4001 --mode throughput --pairs 8 --bytes 16777216

# churn: connect/handshake/close as fast as possible for a duration
./target/release/relay-stress --addr 127.0.0.1:4001 --mode churn --pairs 16 --duration 10s

# stall: one pair; the receiver never reads; observe the relay's reaction
./target/release/relay-stress --addr 127.0.0.1:4001 --mode stall
```

## Baseline (2026-11-18, loopback, release build)

Measured against the release build on this machine (Linux, x86_64), with the
Go relay (`relay-server` from dougsko/wormhole-relay) measured the same way
as a control. Throughput runs pushed 8 pairs × 8 MiB; churn ran for 3 s with
16 workers. Numbers are indicative, not absolute — re-measure on your own
hardware.

| Benchmark | Rust relay | Go relay |
|-----------|-----------:|---------:|
| throughput (8 pairs × 8 MiB) | 567 MB/s | 578 MB/s |
| pair latency p50 | 104 ms | 69 ms |
| churn (pairs completed in 3 s) | 5255 (1752 pairs/s) | 2689 (896 pairs/s) |

This puts the Rust relay at parity or better on throughput and roughly 2× on
pairing churn. The churn path stresses the pairing registry, usage-event
dispatch and connection teardown, which are also the paths that matter for
relay load under wormhole's many-small-connections usage.

## Method notes

- Both relays were run with no `--usage-db` on loopback.
- The stress client is identical for both; it exercises only the public wire
  protocol.
- To compare against the Python reference, run
  `twist transitrelay --port 4211` from
  [magic-wormhole-transit-relay](https://github.com/magic-wormhole/magic-wormhole-transit-relay)
  as the target instead. (The Python relay is single-threaded by design.)
