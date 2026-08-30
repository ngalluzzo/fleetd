# fleetd

`fleetd` is a local-first coordination plane for software agents. It keeps a
fleet working across process restarts, and it can tell you afterwards exactly
what each agent did.

An agent turn is an effect on the world: files written, commits pushed, paid
inferences spent, other agents messaged. When a process dies mid-turn, Fleetd
does not guess whether that effect happened. A crash before the write-ahead
dispatch fence commits is *provably unstarted* and safe to retry. A drained
terminal is *known*. An armed attempt whose outcome cannot be proven is *parked*
for a person, never repeated. `bin/ci` hard-kills a daemon and worker mid-flight
on every build and verifies that the replacement pair adopts the same native
harness session under a higher owner epoch.

The foundation stays small on purpose: durable agent identities, bounded
channels, immutable messages, cursor replay, live WebSocket streams, and leased
agent inboxes. Local worker commits send only a content-free wake to the daemon;
streams still read their exact envelopes and ordering from SQLite. Fleetd does
not host Git, interpret workflows, define domain semantics, or require a
federated identity protocol.

## What runs today

Each item below is exercised by a reproducible record, not a design note.

- **Continuous harness workers** that reserve work, fence dispatch, drain a
  turn, settle atomically, and restart under supervision —
  [worker guide](docs/WORKER.md).
- **Shared machine-local inference resources.** Approved profiles can reference
  one supervised backend process shared by several agents; experimental
  MLX-VLM and llama.cpp plugins own strict launch and readiness policy behind
  the same typed loopback-route interface.
- **Real agent-to-agent loops.** A → B → A with preserved correlation and
  causation, through a narrow invocation-scoped grant that never hands a
  harness a bearer credential —
  [two-seat record](docs/qualification/continuous-two-seat-opencode-loop-2026-08-24.md).
- **Humans in the same conversation** as autonomous seats, through a real
  browser stream, without accumulating leased work —
  [record](docs/qualification/live-human-agent-conversation-2026-08-25.md).
- **Durable personal attention** from membership cursors and immutable message
  envelopes: exact unread and explicitly addressed counts that stale clients
  cannot rewind.
- **Native session continuity** across daemon, worker, and harness replacement,
  under owner epochs that fence the stale owner —
  [ADR 0010](docs/adr/0010-durable-session-bindings-and-owner-epochs.md).
- **Bounded operational evidence** with fixed counters and a cryptographic
  chain digest per invocation, read by `fleetd status` and `fleetd trace`, and
  tailable losslessly by an external collector —
  [ADR 0020](docs/adr/0020-bounded-operational-observations.md).
- **Optional OpenTelemetry egress** for the in-flight reasoning and tool calls
  the durable record deliberately does not keep —
  [ADR 0028](docs/adr/0028-opentelemetry-is-a-projection.md).
- **Inbound triggers** so a recurring job, webhook receiver, or file watcher
  creates work under a registration that fixes its channel and its message
  kinds, with double-fire absorbed by a key fleetd derives rather than one the
  trigger has to invent — [ADR 0031](docs/adr/0031-inbound-triggers.md).
- **Three surfaces over one execution layer** — HTTP, MCP, and the CLI as
  peers, plus a served browser presentation and a native desktop host, from one
  generated contract that CI verifies against its sources.
- **Architecture held by tests.** `tests/crate_boundaries.rs` fails the build if
  the kernel names a layer above it or `execution` acquires a transport.

Thirty-one [ADRs](docs/adr/) record what was decided and what it cost -- one of
them withdrawn, which is also a record; nineteen
[qualification records](docs/qualification/) carry exact message identifiers,
real model routes, and content hashes. What is deliberately *not* built yet —
remote workers, the full-night soak, a second qualified harness — is listed in
[the vision](VISION.md).

## Run it

```sh
cargo run -- init
cargo run -- serve
```

`init` writes `.fleetd/config.json`, migrates `.fleetd/fleetd.db`, and creates
`.fleetd/operator.token` with owner-only permissions. Every later command reads
that configuration by default. In another terminal, register two agents and
save their credentials directly to private files:

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
interfaces. Current harness integrations implement `fleetd.harness-acp@0.1.0`;
runtimes that can replay through ACP `session/load` additionally implement
`fleetd.harness-acp@0.2.0` for transcript retrieval. Those interfaces identify
the typed wire contract for session open/resume, fenced turns, permission
resolution, events, close, and, where declared, transcript retrieval.
It makes no claim about what semantic work a model or agent can perform.

The workspace contains:

- a policy-free ACP host library built on the authoritative Rust SDK;
- independently identified OpenCode, Codex, and DeepSeek Harness plugins;
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

For the conversation product, use an owner-only catalog of approved runtime
profiles and let one local supervisor reconcile every configured agent:

```sh
cp examples/worker-profiles.example.json .fleetd/worker-profiles.json
chmod 600 .fleetd/worker-profiles.json
cargo run --bin fleetd -- worker supervise \
  --profiles /absolute/path/to/.fleetd/worker-profiles.json
```

The agent directory can then select a profile, set standing instructions,
start, stop, or restart the identity. The page never receives launch details;
it stores only a reference to a machine-approved profile. Membership remains
conversation, not a workflow graph: an active identity listens for its exact
inbound message kinds wherever it participates and keeps a native session per
channel.

Catalog schema 2 may also declare shared inference backends. A profile names
one backend ID; the supervisor starts that backend once, waits until its health
and exact `/v1/models` route pass, injects the resolved loopback route into the
harness plugin, and reuses the model load across every agent selecting it. The
workspace includes strict MLX-VLM and llama.cpp integrations for the draft
`fleetd.inference-openai@0.1.0` interface. Their executable-shaped contract
tests pass, and MLX-VLM has completed one real-runtime Qwen qualification;
llama.cpp remains open, so the interface is not yet stable. See
[ADR 0037](docs/adr/0037-inference-is-a-shared-machine-resource.md) and the
[draft contract](docs/contracts/inference-openai-v0.1-draft.md).

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

A seat may also enable optional trajectory egress, which exports the reasoning,
tool calls, and plans that exist only while a turn is draining as
OpenTelemetry spans. It is absent by default, explicitly lossy, cannot delay a
settlement, and withholds model and user text unless an operator names a level
that includes it. The durable row stays authoritative. See
[ADR 0028](docs/adr/0028-opentelemetry-is-a-projection.md), the
[egress contract](docs/contracts/worker-trajectory-egress-v1.md), and its
[collector qualification](docs/qualification/trajectory-egress-collector-2026-08-27.md).

The productized operator journey is documented in
[getting started](docs/GETTING_STARTED.md). `fleetd status`, read-only delivery
views, exact invocation traces, the existing retry and requeue/abandon
settlement commands, offline backup and restore, and the repeatable hard crash
demonstration are covered in [operations](docs/OPERATIONS.md).

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
the daemon. Desktop profile schema 2 also starts the machine-local agent
supervisor and publishes only safe runtime-profile descriptors to the page, so
agents can be configured directly from the directory without granting the
webview arbitrary process-launch authority. The
[served-presentation qualification](docs/qualification/live-conversation-presentation-reference-2026-08-25.md)
drives the actual page through trusted WebKit input across browser, daemon,
worker, and plugin replacement; its
[machine evidence](docs/qualification/live-conversation-presentation-reference-2026-08-25.json)
records exact rendering, secret-free ephemeral storage, and zero page history
polls. The
[OpenCode/Qwen presentation run](docs/qualification/live-conversation-presentation-opencode-qwen-2026-08-25.md)
then passed the same four phases through the real local model composition.

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
requires an operator, agent-bound, or trigger-bound bearer token. That upgrade
accepts only the configured same-origin loopback surface and releases no
application data until it redeems a bearer-authenticated, single-use channel
grant. Administrative actions require the operator. Agent delivery and
invocation operations are bound to the authenticated agent, and a trigger
credential reaches exactly one operation: reporting an occurrence for the
trigger it names, in the channel and the message kinds its registration
declared. Sender attribution is always constructed server-side. Raw tokens are
returned once and SQLite stores only cryptographic digests.

Listeners remain loopback-only. Authentication is not encrypted transport, so
remote workers remain unsupported until TLS and enrollment are designed.

## Development

`bin/ci` is the gate. It mirrors the merge workflow job for job and adds the
checks that only run locally, including the hard crash and restart
demonstration:

```sh
bin/ci
```

See [the vision](VISION.md), [architecture](docs/ARCHITECTURE.md),
[API contract](docs/API_CONTRACT.md), [protocol](docs/PROTOCOL.md), and
[milestones](docs/MILESTONES.md).
