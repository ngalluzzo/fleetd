# fleetd-soak

`fleetd-soak` drives exact predeclared workloads through Fleetd's public API and
writes one evidence artifact. It is an operator tool, not part of the daemon or
messaging kernel.

The runner proves transport facts: the seed was appended, a terminal message
was causally descended from it, and the expected ordered agent invocations
became terminal. Message kinds and payloads remain opaque. It does not decide
whether a JSON payload satisfies an application contract.

## Plan

Start with [`plan.example.json`](plan.example.json). Every workload has its own
stable idempotency key and exact payload. `invocation_agents` is the expected
causal execution order, including repeated seats. Runs are sequential so each
workload receives bounded before/after evidence.

Credential paths must name private regular files. Observer URLs must be
credential-free `http` URLs with an explicit loopback IP and port. Their JSON
documents are preserved without field interpretation. A required observer that
is unavailable before dispatch fails closed. `max_bytes` bounds each opaque
capture (1 MiB by default and 16 MiB at most).

```sh
cargo run -p fleetd-soak -- \
  --plan .fleetd/soak-plan.json \
  --output .fleetd/reports/night-2026-08-25.json
```

The output path must not exist. The tool atomically publishes a synced report
on Fleetd, observer, dispatch, polling, timeout, and completion failures, then
exits nonzero for a failed run. Invalid plans, unsafe credential files, and
unusable endpoint configuration fail before execution.

## Evidence boundary

Each report contains:

- the SHA-256 digest of the exact plan bytes;
- exact seed, completion, and causally descended message envelopes;
- bounded invocation observations tied to source and result message IDs;
- Fleetd plugin-generation, session-binding, observation, and block snapshots;
- opaque external observer documents and capture errors.

It contains no credential values or credential-file paths. Model-specific
aggregation belongs in a separate analyzer consuming this artifact.
