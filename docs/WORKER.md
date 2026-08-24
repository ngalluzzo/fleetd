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

## Turn and lane behavior

The built-in adapter passes the full immutable fleetd message envelope and its
invocation attempt to the harness as JSON. It adds no task, Git, review, or UI
semantics. One durable native session lane is maintained per channel, so
conversation context stays scoped to the channel. A future versioned adapter
or workflow plugin can choose a different lane policy without changing the
kernel.

Set `adapter.kind` to `capability_work_v1` to use the first such adapter; see
[`worker.capability.opencode.example.json`](../examples/worker.capability.opencode.example.json).
Its `providers` list gives each semantic provider an exact identity,
capability, and implementation digest; no name-only or version-range match is
permitted. This identity is distinct from the selected harness plugin. The
adapter validates `work.capability.request/v1`, requires message correlation to
equal the request identity, rejects partial facts where the capability requires
completeness, and uses one lane per work contract. It persists the provider
descriptor beside raw terminal evidence so a later strict lift does not trust
an agent to report its own identity. The extracted candidate still does not
establish that the named conformance suite passed.

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
- Plugin generations restart with bounded backoff. Their in-memory session
  cache is discarded; compatible ready bindings are reacquired under a higher
  owner epoch and natively resumed.
- `Ctrl-C` is observed between turns. An armed turn is allowed to finish or
  block before the child process group is shut down.

The final JSON report includes generations, restarts, reservations,
completions, blocks, safe pre-arm retries, and idle polls. Inspect durable
ambiguity with `fleetd inbox blocked`; only an operator can requeue or abandon
it.
