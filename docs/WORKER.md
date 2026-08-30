# Continuous worker operations

`fleetd worker run` is the first dogfood worker seat. It is a trusted local
controller process that opens the same SQLite database as `fleetd serve`, owns
one harness plugin generation, and serially consumes one agent inbox. It does
not add harness concepts to the public API or messaging kernel.

For the conversation product, `fleetd worker supervise` is the normal host. It
reconciles every durable agent-seat configuration against a private local
catalog of approved profiles. Adding an agent to a channel does not prescribe a
sequence or create a task: it gives that stable identity a place to converse.
Selecting a profile makes the identity executable wherever it participates,
while its native harness session remains separate per channel.

## Supervise configured agents

Copy the approved-profile example, replace every placeholder, and keep it
owner-only because it controls which local executables and tools may run:

```sh
cp examples/worker-profiles.example.json .fleetd/worker-profiles.json
chmod 600 .fleetd/worker-profiles.json
fleetd worker supervise \
  --profiles /absolute/path/to/.fleetd/worker-profiles.json
```

The catalog owns executable paths, plugin configuration, model selection,
working directories, inbound message kinds, and tool grants. The operator API
and conversation page can only store and send a profile ID, standing
instructions, `running` or `stopped`, and a restart revision. Consequently a
browser-held operator credential cannot turn a request body into an arbitrary
process launch. A configured profile absent from the host catalog does not run
and is reported in the supervisor log.

Schema 2 can separate inference from the harness profile. Its private
`inference_backends` list contains strict backend plugin configuration; a
profile selects one by `inference_backend` ID. The supervisor starts the model
server before any dependent harness, waits for the configured loopback health
and exact OpenAI-compatible model route, and injects only the typed description
into the harness plugin. It starts one backend per ID, so several agent
identities can share an expensive model load without sharing a native harness
session. When that backend fails, dependent workers stop before bounded backend
restart.

The first integrations are `fleetd.inference.mlx-vlm` and
`fleetd.inference.llama-cpp`. They share
`fleetd.inference-openai@0.1.0`, while each owns its native flags, version
probe, environment allowlist, profile digest, health URL, and metrics format.
The MLX-VLM integration additionally owns explicit `enable_thinking` and
`thinking_budget` launch settings. They are backend defaults and therefore
apply to every profile sharing that backend ID.
No generic argument or environment field exists. The interface remains a draft
until both pass fresh real-runtime qualification. MLX-VLM's first Qwen proof is
[recorded](qualification/inference-mlx-vlm-qwen-2026-08-28.md); see the
[contract](contracts/inference-openai-v0.1-draft.md) for the remaining bar.

The supervisor owns at most one reconciler per database on a machine. A real
configuration change or explicit restart advances the durable revision; the
supervisor drains the old worker safely and starts the selected profile with
the same agent identity. Stopping cancels between turns, preserving the
worker's existing conservative settlement rules. The manual `worker run`
command remains useful for qualification and transcript operations.

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
other secret in this file. The plugin process starts with an empty environment.
A vendor plugin may point its harness at an explicit private state directory;
that harness, not Fleetd desired state, owns any provider credentials stored
there.

An optional `instructions` string supplies standing guidance to the envelope
adapter. It is bounded to 32 KiB, remains outside the kernel and immutable
message history, and is presented alongside the adapter's fixed safety and
addressing preamble. In the supervised path it comes from the durable seat
configuration; a profile may not preselect either the agent ID or instructions.

Harness configuration is plugin-owned. The OpenCode plugin accepts a typed
model route and exact executable/version, constructs native configuration
internally, and computes the effective profile digest. It rejects arbitrary
environment and credential fields. The development-only ACP reference plugin
retains an operator-supplied profile digest for protocol qualification; do not
use it as a production vendor adapter. By default the worker resumes only an
exact observed profile digest. Set `compatibility_digest` only after a
separately qualified set of profiles has proven native-session compatibility.

The DeepSeek Harness plugin has two mutually exclusive model routes. For a
DSH-owned remote provider, configure the exact native pair directly:

```json
{
  "provider": "zai",
  "model": "glm-5.3"
}
```

The private `dsh_home` must already contain the DSH-managed provider settings
and credential created through DSH. Fleetd preserves `settings.yaml` and
`.credentials.yaml`, passes no raw provider key in plugin configuration or the
child environment, and pins the selected provider/model in the generated ACP
profile. Provider protocol, model metadata, reasoning levels, and credential
resolution remain DSH-owned. Changing the selected provider/model changes the
Fleetd profile identity.

The other DSH route consumes a supervisor-injected `inference` descriptor for a
Fleetd-managed local model server. In that mode, `provider` and `model` are
absent, DSH settings and credentials are disabled, and the profile must supply
the exact local `reasoning_effort`, `max_output_tokens`, `context_window`, and
`stream_idle_timeout_ms`. The two modes cannot be combined. This keeps remote
provider credentials in their native harness while retaining one shared local
model load for Qwen and other machine-hosted models.

For a local OpenAI-compatible model server, the OpenCode plugin also accepts a
typed `openai_compatible` block. It requires a credential-free explicit
loopback HTTP address, constructs the native
`@ai-sdk/openai-compatible` provider entry, and requires `model` to equal the
exact `provider_id/model_id` route. The provider configuration and plugin policy
version participate in the effective profile digest. The plugin denies
OpenCode's nested `task` permission: nested subagent activity is not admitted
until its tools, progress, cancellation, and budgets are visible through the
same typed Fleetd evidence boundary.

For one of those compatible routes, optional `reasoning_effort` is a typed
OpenCode harness setting with the exact values `none`, `minimal`, `low`,
`medium`, `high`, or `xhigh`. OpenCode sends it on every model request. The
backend still owns its concrete behavior: for MLX-VLM, an effort other than
`none` enables thinking, while `thinking_budget` supplies the enforceable
thinking-token ceiling. Both settings participate in their owning plugin's
profile digest; changing either conservatively rotates the native session.
The qualified Qwen3.8 MLX template accepts `low`, `medium`, or `xhigh`; it
rejects `high`, so the MLX Qwen example uses `xhigh`.
MLX-VLM 0.6.15 rejects a hard `thinking_budget` when a speculative draft model
is active. The MLX integration rejects that combination during initialization;
choose the budget ceiling or speculative decoding rather than silently losing
either control.

In a supervised schema-2 profile, do not write that block. A referenced backend
plugin supplies `inference` only after readiness, and OpenCode maps it to the
same native provider mechanism under the fixed `fleetd-inference` provider ID.
The resolved backend identity and profile digest participate in OpenCode's
effective profile digest, so changing backend composition follows the existing
conservative session-generation rules.

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

## Sandbox and permission policy

Mutation-capable seats can declare a macOS Seatbelt boundary in private worker
desired state:

```json
{
  "working_directory": "/absolute/isolated/checkout",
  "additional_directories": [],
  "sandbox": {
    "kind": "macos_seatbelt",
    "posture": "strict",
    "read_access": "declared_and_system",
    "read_only_directories": [
      "/absolute/path/to/pinned/runtime"
    ],
    "writable_directories": [],
    "network": "allow_outbound"
  },
  "turn": {
    "permission_policy": "allow_once"
  }
}
```

Omitting `posture` and `read_access` preserves this existing `strict` shape.
Strict remains deny-by-default: declared writable roots are read/write,
explicit runtime roots are read-only, and `network` is `deny` or
`allow_outbound`. Strict also permits writes to the literal device
`/dev/null`; Git opens it read/write while sanitizing inherited standard file
descriptors, and granting that single sink does not admit another filesystem
path.

Some local harnesses need ambient dependency reads and a private localhost
listener before their ACP adapter can initialize. An operator may instead
declare the narrower write-only claim explicitly:

```json
{
  "sandbox": {
    "kind": "macos_seatbelt",
    "posture": "write_scoped",
    "read_access": "unrestricted",
    "network": "unrestricted",
    "private_state_directory": "/absolute/seat-private/state",
    "private_temp_directory": "/absolute/seat-private/tmp",
    "writable_directories": []
  }
}
```

`write_scoped` is not hermetic and provides no read or network confidentiality.
It starts from allow-default, denies every file write, then restores writes only
to `/dev/null`, the working and additional directories, explicit writable
directories, and the two private per-seat roots. The posture name and effective
profile digest participate in session compatibility. Use it only when that
write-confinement-only boundary is intentional; see
[ADR 0039](adr/0039-write-scoped-seatbelt-is-write-confinement-only.md).

`working_directory`, every `additional_directories` entry, and every explicit
`writable_directories` entry are writable. `read_only_directories` exists for
pinned runtimes and other exact launch dependencies. All paths must already be
absolute directories; the filesystem root is refused. The plugin and every
descendant it launches share the selected process-group sandbox. Only `strict`
is deny-by-default; `write_scoped` is allow-default with deny-by-default writes.

Under `strict`, `network` is either `deny` or `allow_outbound`. The latter is needed by a
hosted model provider but currently permits every outbound destination; it is
not a provider-domain allowlist. Do not claim network isolation for such a
profile. `write_scoped` requires the honest value `unrestricted`, because its
allow-default profile does not constrain inbound or outbound operations.
Destination-filtered egress and non-macOS sandbox implementations are not yet
available.

The permission policy defaults to `deny`. `allow_once` is accepted only for an
OS-sandboxed profile and selects exactly one ACP option whose typed kind is
`allow_once`. Fleetd does not parse tool names, command text, paths, URLs, or
adapter-specific option IDs, and never selects `allow_always`. Missing or
ambiguous one-shot options are cancelled. The sandbox digest and permission
policy participate in session compatibility, so changing either prevents
silent native-session reuse. See
[ADR 0038](adr/0038-one-shot-acp-permission-requires-an-os-boundary.md).

Use a fully isolated clone for a Git-writing seat. A linked Git worktree keeps
its object store, refs, and administrative files in the parent repository;
granting enough access to commit would therefore widen the write boundary
beyond the seat checkout.

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
and a 60-second settlement margin. Permission requests are denied by default;
an explicitly sandboxed seat may instead select only typed one-shot options.
Tool use is observed and cancelled at the configured budget, token budgets are
not claimed, output capture is capped at 512 KiB, and all policy bounds are
validated before a process starts.

Turns are interruptible by default. `turn.interrupt_on_new_message` controls
whether a newer claimable message of an accepted kind, committed in the same
channel after the active turn began, requests cancellation.
`turn.interrupt_poll_interval_ms` controls the authoritative SQLite
reconciliation cadence and defaults to 250 milliseconds. Existing backlog is
not treated as an interruption: messages already present when a turn begins
continue to drain in durable order. A clean cancellation settles the old turn
with `status: "interrupted"`, `stop_reason: "host_newer_message"`, and the
`interrupted_by_message_id`; the worker then retires that native session and
opens a fresh generation for the newer message with refreshed shared history.
The durable channel, rather than cancellation-tainted private session state,
carries conversational continuity. Set the boolean to `false` for adapters
whose accepted messages are independent queued jobs rather than conversational
follow-ups.

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
- `Ctrl-C` and `SIGTERM` interrupt an armed turn through the same bounded
  cancel-and-drain path. A known quiescent cancellation settles with
  `host_worker_shutdown`; ambiguous cancellation blocks before the child
  process group is shut down.

The final JSON report includes generations, restarts, reservations,
completions, interruptions, blocks, safe pre-arm retries, and idle polls. Inspect durable
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

## Trajectory egress

The bounded observation a turn already records keeps counters, byte totals, and
a chain digest, not content. Reasoning, tool arguments, and intermediate plans
exist only while a turn is draining. Egress is the optional per-seat sink that
exports them as OpenTelemetry spans before that evidence is folded away. It is
absent by default: with no `egress` block there is no exporter and no queue. The
decision is [ADR 0028](adr/0028-opentelemetry-is-a-projection.md), the field
rules are the [v1 contract](contracts/worker-trajectory-egress-v1.md), and one
real turn per content level against a live collector is recorded in the
[collector qualification](qualification/trajectory-egress-collector-2026-08-27.md).

```sh
cp examples/worker.acp.egress.example.json .fleetd/worker.json
```

That example differs from the ACP reference one by exactly the `egress` block,
and points at a loopback OTLP collector on the default `4318`.

`content` decides what may leave the process. `none` exports timing and
ordering only; `metadata`, the default, adds the tool kind, call id, status,
plan size, and stop reason but never model or user text and never a tool's
agent-authored title; `full` adds assistant text, reasoning, the title, and
tool arguments, each bounded by `max_attribute_bytes`. `classifications`
selects whole event classes by the names `fleetd trace` already reports, so
`reasoning` can be dropped rather than merely redacted. A non-loopback
`endpoint` must be `https`, and a collector requiring authorization reads it
from a `headers_file` whose mode is verified owner-only before it is read. As
everywhere else in this file, no credential belongs in the desired-state
document.

The sink is lossy deliberately. A full queue drops the event and counts it, an
unreachable collector cannot delay a settlement or influence a fence, and the
counters surface in the log stream rather than in the final JSON report: one
warning on a generation's first drop, and one summary of accepted, exported, and
dropped totals when that generation retires. Nothing an operator is promised
depends on it, and neither `fleetd status` nor `fleetd trace` consults it. For a
lossless reader, tail `/v1/invocation-observations` and
`/v1/plugin-generations` through their cursors instead; that path needs no
collector and no egress block.

Enabling egress does not rotate a binding generation. Unlike inbound
acceptance, it changes nothing the harness sees, so it must not discard native
conversational state.
