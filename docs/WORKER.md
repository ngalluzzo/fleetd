# Continuous worker operations

`fleetd worker run` is the first dogfood worker seat. It is a trusted local
controller process that opens the same SQLite database as `fleetd serve`, owns
one harness plugin generation, and serially consumes one agent inbox. It does
not add harness concepts to the public API or messaging kernel.

## Start one seat

Build the daemon and the selected harness plugin, copy its desired-state
example, then fill in every placeholder with an absolute path or exact observed
identity. For an OpenCode seat:

```sh
cargo build --workspace
cp examples/worker.opencode.example.json .fleetd/worker.json
cargo run --bin fleetd -- worker run --db .fleetd/fleetd.db \
  --config .fleetd/worker.json
```

Use `--once` to wait for and settle one invocation, which is useful for a
qualification run. Without it, the process polls until `Ctrl-C`. Run only one
seat for a given agent during this first slice; durable session fencing remains
safe with competing processes, but they will produce avoidable retries.

Worker desired-state schema 2 requires an explicit adapter. The envelope
adapter's `inbound` block declares schema 1 and a non-empty exact
`message_kinds` set. The worker atomically reserves only matching kinds;
non-matches remain pending without an attempt increment. Matching establishes
eligibility, not semantic validity; the payload remains opaque through
dispatch. The acceptance contract participates in session
compatibility and changing it rotates the binding generation. See the
[v1 contract](contracts/worker-inbound-acceptance-v1.md).

The worker configuration schema is versioned and rejects unknown fields. Each
harness plugin then validates its own opaque configuration with a strict
plugin-owned schema. Never put a fleetd bearer credential, provider key, or
other secret in this file. The plugin process starts with an empty environment;
the selected plugin constructs only the native settings its integration owns.

Harness configuration is plugin-owned. The OpenCode plugin accepts a typed
model route and exact executable/version, constructs native configuration
internally, and computes the effective profile digest. It rejects arbitrary
environment and credential fields. The development-only ACP reference plugin
retains an operator-supplied profile digest for protocol qualification; do not
use it as a production vendor adapter. By default the worker resumes only an
exact observed profile digest. Set `compatibility_digest` only after a
separately qualified set of profiles has proven native-session compatibility.

For a local OpenAI-compatible model server, the OpenCode plugin also accepts a
typed `openai_compatible` block. It requires a credential-free explicit
loopback HTTP address, constructs the native
`@ai-sdk/openai-compatible` provider entry, and requires `model` to equal the
exact `provider_id/model_id` route. The provider configuration and plugin policy
version participate in the effective profile digest. The plugin denies
OpenCode's nested `task` permission: nested subagent activity is not admitted
until its tools, progress, cancellation, and budgets are visible through the
same typed Fleetd evidence boundary.

`mcp_grants` is an allowlist of invocation-scoped runtime grant names, not MCP
commands, URLs, credentials, or semantic claims. The current worker accepts
either no grants or exactly
`fleet.messaging.send`. When enabled, it starts a random-port `127.0.0.1`
Streamable HTTP endpoint and supplies its ephemeral token only in the resolved
ACP session setup. The desired-state file, SQLite catalog, effective-config
evidence, and logs never receive that token. Unknown or duplicate grant names
fail configuration validation before a plugin starts.

The exposed `publish_durable_message` tool requires `operation_id`,
`recipient_id`, `kind`, and `payload`. It is addressed-message only, rejects
self-send, permits at most eight committed messages per invocation, and caps
the encoded payload at 64 KiB. Exact retries reuse the operation ID and return
the same committed message. The agent cannot choose sender, channel,
correlation, causation, or the durable idempotency key. See the
[OpenCode qualification](qualification/message-grant-opencode-2026-08-24.md).

## Turn and lane behavior

The built-in envelope adapter passes the full immutable Fleetd message envelope
and its invocation attempt to the harness as JSON. It adds no task, Git, review,
UI, workflow, or compiler semantics. Configured exact kinds decide only
reservation eligibility. One durable native session lane is maintained per
channel, so conversation context stays scoped to the channel.

Any external system may define message kinds consumed by a seat. Fleetd does
not recognize those contracts; their owners remain responsible for semantic
validation, implementation selection, prompt construction, result parsing, and
conformance. See the [integration boundary](INTEGRATION_BOUNDARY.md).

The lease must cover the configured wall timeout, cancellation drain timeout,
and a 60-second settlement margin. Permission requests are denied by the
controller, tool use is observed and cancelled at the configured budget, token
budgets are not claimed, output capture is capped at 512 KiB, and all policy
bounds are validated before a process starts.

## Failure semantics

- Before durable dispatch arming, adapter, session, or harness-start failures
  release the invocation with `not_started` evidence and a bounded retry delay.
- After arming, unknown protocol, persistence, timeout, or process state is
  never converted into permission to replay. The controller drains a known
  terminal outcome or durably blocks the delivery and marks the session
  uncertain.
- After a completed or blocked turn, result settlement is already committed in
  SQLite before the next reservation.
- A newly committed result or invocation-scoped peer message sends one
  content-free best-effort local wake to the daemon. Open streams then reconcile
  SQLite; notification failure never changes the committed outcome or removes
  cursor replay as the recovery path.
- A known, quiescent terminal is safe to settle but is not automatically a
  successful semantic result. Final-JSON adapters report failure unless one
  complete protocol-bounded JSON value was captured. Host cancellation
  overrides a runtime `end_turn` and preserves that runtime claim separately as
  evidence.
- Invocation grants are activated only after dispatch arming. Revocation
  serializes behind any accepted durable append and completes before result or
  block settlement.
- Plugin generations restart with bounded backoff. Their in-memory session
  cache is discarded; compatible ready bindings are reacquired under a higher
  owner epoch and natively resumed.
- A generation that reaches readiness is durably identified before it can
  receive work, heartbeats while owned, and records its stop and process-group
  shutdown outcome. Arming a turn atomically creates one fixed-size
  observation; ordered updates fold into counters, byte totals, and a chain
  digest rather than a second transcript store.
- `Ctrl-C` is observed between turns. An armed turn is allowed to finish or
  block before the child process group is shut down.

The final JSON report includes generations, restarts, reservations,
completions, blocks, safe pre-arm retries, and idle polls. Inspect durable
ambiguity with `fleetd inbox blocked`; only an operator can requeue or abandon
it. Operator credentials can inspect generation, session, and invocation
evidence at `/v1/plugin-generations`, `/v1/session-bindings`, and
`/v1/invocation-observations`.

For ordinary operation, `fleetd status --agent AGENT_ID` composes those read
models with current delivery state. `fleetd trace --invocation INVOCATION_ID`
then reads the exact source/result, generation, session owner epoch, and bounded
turn evidence for one attempt. The self-contained
[`examples/restart-demo`](../examples/restart-demo/run.sh) performs a hard
daemon and worker replacement and verifies that composition end to end.
