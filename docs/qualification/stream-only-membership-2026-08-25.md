# Stream-only conversation membership qualification

Date: 2026-08-25
Qualification revision: `d79ff5521d191b72d0bffa8da4d6d4cae4da021f`

## Claim

One addressable `stream_only` participant and one `inbox` worker can share the
authoritative channel log without creating false human work. A managed worker
invocation can publish a causal result to the passive participant, and that
result remains available through native live delivery and durable cursor replay
after daemon restart.

## Composition

The qualification provisions all agents and mixed channel membership through
the public authenticated API. It then:

1. opens the human participant's bearer-authenticated native WebSocket;
2. appends an opaque human-to-worker request and verifies its only SQLite
   delivery recipient is the worker;
3. reserves, arms, and completes the worker invocation through the public API;
4. receives the causal opaque result live and verifies SQLite contains no
   delivery row for it;
5. restarts the daemon and replays the result from the request cursor;
6. proves the operator can see a worker-to-peer direct message that the human
   participant cannot; and
7. broadcasts from the human, receives it on the human live stream and peer
   history, and verifies only the `inbox` worker receives a delivery row.

The test queries `channel_members` and `agent_deliveries` directly by exact
channel and message sequence. An empty inbox claim is not used as evidence for
delivery-row absence. Unknown kinds and nested payload fields are compared
through append responses, WebSocket frames, and replay without rewriting.

## Commands and environment

```sh
cargo test --test live_conversation_membership
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The exact qualification revision passed all commands. Runtime versions were:

- macOS 26.5.2 build 25F84 on arm64;
- `rustc 1.95.0 (59807616e 2026-04-14)`;
- `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`; and
- SQLite 3.51.0.

No external browser, harness, worker, or model runtime participated in this
slice. The Rust test owns and terminates every loopback daemon it starts.
