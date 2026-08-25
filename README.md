# fleetd

`fleetd` is a local-first coordination plane for software agents that need to
talk to one another and keep working across process restarts.

The foundation is deliberately small: durable agent identities, bounded
channels, immutable messages, cursor replay, live WebSocket streams, and leased
agent inboxes. Local worker commits send only a content-free wake to the daemon;
streams still read their exact envelopes and ordering from SQLite. Fleetd does
not host Git, interpret workflows, define domain semantics, or require a
federated identity protocol.

## Run it

```sh
cargo run -- serve --db .fleetd/fleetd.db
```

The daemon creates `.fleetd/operator.token` with owner-only permissions. In
another terminal, register two agents and save their credentials directly to
private files:

```sh
cargo run -- agent add --name piler --metadata '{"harness":"opencode"}' \
  --credential-file .fleetd/piler.token
cargo run -- agent add --name weaver --metadata '{"harness":"codex"}' \
  --credential-file .fleetd/weaver.token
```

Create a channel with both agent IDs:

```sh
cargo run -- channel create --name project-001 \
  --member PILER_ID --member WEAVER_ID
```

Watch the durable conversation:

```sh
cargo run -- --token-file .fleetd/weaver.token message watch \
  --channel CHANNEL_ID
```

Send an addressed message:

```sh
cargo run -- --token-file .fleetd/piler.token message send \
  --channel CHANNEL_ID --to WEAVER_ID --text 'review commit 5fe343f' \
  --idempotency-key invocation/example/result
```

An identical idempotent retry returns the original message. Reusing the key
with different content fails with `409 Conflict`.

## Durable inbox and invocation fence

An adapter can lease ordinary idempotent work through `inbox claim`. A managed
harness worker instead reserves a delivery and its invocation record in one
transaction:

```sh
cargo run -- --token-file .fleetd/weaver.token invocation reserve \
  --agent WEAVER_ID --limit 1 --lease-ms 300000
```

Immediately before an effectful prompt, the controller commits the write-ahead
dispatch fence:

```sh
cargo run -- --token-file .fleetd/weaver.token invocation arm \
  --agent WEAVER_ID --invocation INVOCATION_ID \
  --lease LEASE_TOKEN --fence FENCE_TOKEN
```

The effect is never sent before arming succeeds. A pre-arm crash is provably
unstarted and recoverable. An armed attempt whose outcome cannot be proven is
parked rather than repeated. Known completion publishes the correlated result,
acknowledges the input, and terminalizes the invocation atomically:

```sh
cargo run -- --token-file .fleetd/weaver.token invocation complete \
  --agent WEAVER_ID --invocation INVOCATION_ID \
  --lease LEASE_TOKEN --fence FENCE_TOKEN \
  --kind work.result/v1 --payload '{"status":"done"}'
```

Operators resolve ambiguous work explicitly:

```sh
cargo run -- inbox blocked --agent WEAVER_ID
cargo run -- inbox resolve --block BLOCK_ID --resolution requeue \
  --note 'verified the external effect did not occur'
```

## Harness plugins

Harness integrations run as child-process plugins. Fleetd launches an absolute
executable directly with an empty environment, owns its complete process group,
and gives it neither Fleetd credentials nor ambient secrets.

Lifecycle initialization returns plugin identity plus exact operational
interfaces. Current harness integrations implement
`fleetd.harness-acp@0.1.0`; that interface identifies the typed wire contract
for session open/resume, fenced turns, permission resolution, events, and close.
It makes no claim about what semantic work a model or agent can perform.

The workspace contains:

- a policy-free ACP host library built on the authoritative Rust SDK;
- independently identified OpenCode and Codex harness plugins;
- a development ACP reference plugin;
- a continuous worker that composes inbox reservation, durable session
  acquisition, owner epochs, dispatch fencing, turn drain, atomic completion,
  conservative parking, and supervised restart.

Build and run one worker seat:

```sh
cargo build --workspace
cp examples/worker.opencode.example.json .fleetd/worker.json
# Fill in the agent ID, executable paths, model route, and working directory.
cargo run --bin fleetd -- worker run --db .fleetd/fleetd.db \
  --config .fleetd/worker.json
```

One seat is serialized and defaults to one native harness session per channel
and working-directory identity. Compatible restarts adopt that session under a
higher owner epoch; incompatible profiles rotate the binding generation;
uncertain sessions fail closed. See [the worker guide](docs/WORKER.md) and
[the harness architecture](docs/HARNESS_EXECUTION.md).

Ready plugin generations and managed turns leave bounded durable operational
evidence. Operator-only read models expose exact generation identity and
liveness, session ownership, and per-invocation event counters and chain
digests at `/v1/plugin-generations`, `/v1/session-bindings`, and
`/v1/invocation-observations`. Fleetd does not duplicate raw harness
transcripts in SQLite.

## Agent-to-agent loop

A worker may receive the narrow `fleet.messaging.send` invocation grant. The
controller resolves it to an ephemeral loopback MCP endpoint, derives sender,
channel, correlation, causation, and idempotency from the armed invocation, and
never hands the harness an agent bearer credential.

A real OpenCode turn has committed an attributed peer message through this
grant. A bounded two-seat run composed A → B → A while preserving lineage and
resumed A's native session after restart. Worker schema 2 requires explicit
inbound message kinds, preventing generic result messages from becoming new
work accidentally. See the
[single-hop qualification](docs/qualification/message-grant-opencode-2026-08-24.md)
and [continuous two-seat qualification](docs/qualification/continuous-two-seat-opencode-loop-2026-08-24.md).
The later [local Qwen restart qualification](docs/qualification/qwen-restart-resumption-2026-08-24.md)
proves generation replacement and native-session adoption while preserving a
typed-payload mismatch as application evidence rather than normalizing it in
Fleetd.

For reproducible unattended runs, the standalone
[`fleetd-soak`](tools/soak/README.md) tool appends exact opaque workloads through
the public API, correlates bounded observations through message causation, and
captures credential-free loopback observer JSON without interpreting provider
fields. The daemon remains unaware of workload or model-server semantics. See
the [first real Qwen runner qualification](docs/qualification/qwen-unattended-soak-runner-2026-08-25.md).
An exploratory [one-versus-two sequence matrix](docs/qualification/qwen-max-num-seqs-matrix-2026-08-25.md)
keeps one server sequence as the measured operating point for the causal
two-seat loop; independent parallel work still needs its own matrix.

## Human-to-agent product loop

Fleetd now has a presentation-free product qualification for a human
`stream_only` participant talking to a real continuous OpenCode/Qwen worker.
Four causal turns passed through the production WebKit browser stream across a
fresh browser connection, daemon replacement, and worker plus harness
replacement. The durable binding retained its native session while advancing
the owner epoch, and the human accumulated no leased inbox work. See the
[qualification record](docs/qualification/live-human-agent-conversation-2026-08-25.md)
and [exact machine evidence](docs/qualification/live-human-agent-conversation-2026-08-25.json).

Fleetd also serves a usable same-origin presentation at `/conversation/`. It
uses the shared headless TypeScript session, the public browser stream, and no
polling fallback. Open it in an ordinary browser and connect explicitly, or
build the [Electrobun desktop host](apps/conversation-desktop/README.md), which
loads the same page in a native system webview and sources its two authorities
from separate owner-only files. Neither target adds conversation semantics to
the daemon.

## Semantic boundary

Fleetd message kinds and JSON payloads are opaque transport data. Fleetd does
not import semantic IR, choose implementations, validate domain documents, or
produce semantic candidates.

An external compiler integration may lift Fleetd's public API, plugin
observations, or generated artifacts; explicitly bridge that evidence into its
own semantics; and lower a linked deployment to Fleetd's public API or worker
configuration. That integration is a separately versioned package outside this
repository. Fleetd does not know that the lift or lowering occurred.

This keeps the relationship composable: Fleetd coordinates agents working on
any project, while external compilers can target Fleetd without becoming part
of its daemon. See [the integration boundary](docs/INTEGRATION_BOUNDARY.md).

## Security boundary

Every versioned HTTP operation except the dedicated browser WebSocket upgrade
requires an operator or agent-bound bearer token. That upgrade accepts only the
configured same-origin loopback surface and releases no application data until
it redeems a bearer-authenticated, single-use channel grant. Administrative
actions require the operator. Agent delivery and invocation operations are
bound to the authenticated agent; sender attribution is always
constructed server-side. Raw tokens are returned once and SQLite stores only
cryptographic digests.

Listeners remain loopback-only. Authentication is not encrypted transport, so
remote workers remain unsupported until TLS and enrollment are designed.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

See [the vision](VISION.md), [architecture](docs/ARCHITECTURE.md),
[API contract](docs/API_CONTRACT.md), [protocol](docs/PROTOCOL.md), and
[milestones](docs/MILESTONES.md).
