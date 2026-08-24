# fleetd

`fleetd` is a local-first control plane for software agents that need to talk to
each other and keep working across process restarts.

The foundation is deliberately small: durable agent identities, bounded
channels, immutable messages, cursor-based replay, live WebSocket streams, and
leased agent inboxes that resume after worker crashes. It does not host Git,
understand a particular model harness, or require a federated identity protocol.

## Run it

```sh
cargo run -- serve --db .fleetd/fleetd.db
```

The daemon creates `.fleetd/operator.token` with owner-only permissions. The CLI
uses it automatically for administrative commands. In another terminal,
register two agents and save their credentials directly to private files:

```sh
cargo run -- agent add --name piler --metadata '{"harness":"dsh"}' \
  --credential-file .fleetd/piler.token
cargo run -- agent add --name weaver --metadata '{"harness":"codex"}' \
  --credential-file .fleetd/weaver.token
```

Each response contains `agent.id` but omits the raw token when a credential file
is requested. fleetd never overwrites an existing credential file.

Create a channel containing both IDs:

```sh
cargo run -- channel create --name gooir-001 \
  --member Piler_ID --member Weaver_ID
```

Watch it from one process:

```sh
cargo run -- --token-file .fleetd/weaver.token message watch \
  --channel CHANNEL_ID
```

Then send a durable message from another:

```sh
cargo run -- --token-file .fleetd/piler.token message send \
  --channel CHANNEL_ID --to Weaver_ID --text 'review commit 5fe343f' \
  --idempotency-key invocation/example/result
```

The key is optional. An identical retry returns the original message instead
of creating another delivery; conflicting reuse fails with `409 Conflict`.

Weaver's adapter can atomically lease the message:

```sh
cargo run -- --token-file .fleetd/weaver.token inbox claim \
  --agent Weaver_ID --limit 1 --lease-ms 300000
```

That low-level claim is appropriate for non-effectful or independently
idempotent consumers. A managed harness controller uses an invocation
reservation instead, which commits the lease and its durable execution record
in one transaction:

```sh
cargo run -- --token-file .fleetd/weaver.token invocation reserve \
  --agent Weaver_ID --limit 1 --lease-ms 300000
```

Immediately before sending an effectful prompt or tool request, the controller
commits the returned write-ahead fence:

```sh
cargo run -- --token-file .fleetd/weaver.token invocation arm \
  --agent Weaver_ID --invocation INVOCATION_ID \
  --lease LEASE_TOKEN --fence FENCE_TOKEN
```

The effect must never be sent before `arm` succeeds. If the controller crashes
before arming, fleetd proves that attempt `not_started` and may reclaim it. If
it crashes after arming, lease recovery records `outcome_unknown` and blocks
the delivery instead of executing it again. Operators can audit the ledger
with `cargo run -- invocation list`.

After a known successful turn, completion publishes the correlated result and
acknowledges the input in one commit:

```sh
cargo run -- --token-file .fleetd/weaver.token invocation complete \
  --agent Weaver_ID --invocation INVOCATION_ID \
  --lease LEASE_TOKEN --fence FENCE_TOKEN \
  --kind work.result/v1 --payload '{"status":"done"}'
```

The result is addressed to the input sender in the same channel. Its causation
is the input message, its correlation is preserved, and an identical completion
retry returns the original result without publishing or delivering it twice.

For the low-level claim path, successful work is acknowledged with the returned
message and lease IDs:

```sh
cargo run -- --token-file .fleetd/weaver.token inbox ack --agent Weaver_ID \
  --message MESSAGE_ID --lease LEASE_TOKEN
```

If processing fails, `inbox retry` releases the delivery with a delay and
diagnostic message. If the worker disappears, the lease expires and another
worker can claim the delivery with an incremented attempt count.

If a worker cannot prove whether an external effect happened, it parks the
delivery instead of blindly retrying it:

```sh
cargo run -- --token-file .fleetd/weaver.token inbox block \
  --agent Weaver_ID --message MESSAGE_ID --lease LEASE_TOKEN \
  --reason 'tool connection closed after request write'
```

Blocked work remains unclaimable after the lease expires. The operator can
inspect it and make an explicit decision:

```sh
cargo run -- inbox blocked --agent Weaver_ID
cargo run -- inbox resolve --block BLOCK_ID --resolution requeue \
  --note 'verified that the side effect did not occur'
```

`--resolution abandon` permanently ends that delivery instead. Blocking and
resolution are both safely replayable after a lost HTTP response; changing the
evidence or decision on replay fails with `409 Conflict`.

Every message also accepts a machine-readable JSON payload, a semantic `kind`,
and optional correlation and causation IDs.

## Security boundary

Every versioned API request requires an operator or agent-bound bearer
credential. Administrative actions require the operator. Claim, acknowledge,
retry, block, reservation, dispatch arming, and completion are restricted to
the credential's agent; only the operator can inspect invocations or resolve
blocked work. Message attribution is constructed by the server rather than
accepted from request data. Raw tokens are returned once and stored only in
owner-readable files; SQLite contains cryptographic digests.

The daemon still rejects non-loopback listen addresses. Authentication is not a
substitute for encrypted transport, so remote workers remain unsupported until
TLS and enrollment are designed.

## Plugin boundary

Harnesses and other domain behavior run outside the daemon as child-process
plugins. fleetd provides a small, versioned lifecycle over newline-framed
JSON-RPC: initialize, negotiate exact capabilities, check readiness, and shut
down within a deadline. It launches absolute executables directly with an empty
environment, owns their complete process group, and does not give plugins
fleetd credentials.

The workspace now includes an experimental typed `harness.acp` host client, a
generic ACP v1 driver built on the official Rust SDK, and a one-turn controller
that composes reservation, write-ahead arming, prompt drain, atomic completion,
and conservative unknown-outcome parking. The underlying JSON-RPC call surface
remains private; this is not a generic `execute` contract.

The driver has completed a real Codex turn. DSH initialization and identity
verification pass, but local session qualification is currently blocked by an
unconfigured DSH credential, so the capability remains experimental. Durable
session bindings, persisted event evidence, and the continuous inbox worker are
the next boundary. See
[the harness execution architecture](docs/HARNESS_EXECUTION.md),
[ADR 0004](docs/adr/0004-out-of-process-capability-plugins.md),
[ADR 0005](docs/adr/0005-acp-harness-boundary.md),
[ADR 0008](docs/adr/0008-write-ahead-invocation-fence.md),
[ADR 0009](docs/adr/0009-typed-acp-driver-and-process-ownership.md), and the
[qualification record](docs/qualification/acp-driver-2026-08-24.md).

See [the vision](VISION.md), [architecture](docs/ARCHITECTURE.md),
[protocol](docs/PROTOCOL.md), and [milestones](docs/MILESTONES.md) for the
intended boundaries and next slices.
