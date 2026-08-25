# ADR 0021: Soak workloads and external telemetry remain outside the daemon

- Status: accepted for experimental dogfood
- Date: 2026-08-25

## Context

An unattended fleet must be exercised with reproducible workloads and retain
enough evidence to distinguish transport, harness, model-server, and
application-contract failures. The daemon's bounded operational records cover
Fleetd-owned control state, but model servers and harnesses own additional
telemetry such as token counts, queue depth, and decode rate.

Adding provider metric fields to Fleetd would make the coordination plane know
about one backend and create a prematurely universal analytics schema. Letting
a workload driver invent or normalize application payloads would similarly
move semantic contract ownership into Fleetd. Timing-window correlation alone
is also insufficient when several seats are working concurrently.

## Decision

The repository includes `fleetd-soak`, a standalone operator tool that uses
only Fleetd's authenticated public HTTP API. A versioned plan declares exact
opaque seed messages, exact causally ordered invocation agents, a terminal
message kind, and a timeout. The runner executes workloads sequentially and
does not generate, interpret, repair, or validate application payloads.

Invocation observations expose the source message ID and optional result
message ID already represented by the invocation record. This additive public
read-model data lets an external collector correlate observations through
immutable message causation rather than infer ownership from timestamps.

External telemetry sources are explicit credential-free loopback HTTP
observers. The runner captures each JSON document opaquely before and after a
run and each workload under an explicit byte limit. It does not assign shared
meanings to provider fields. A separately versioned backend-specific analyzer
may consume the resulting report later.

Plans reference owner-only credential files. Reports include neither token
values nor credential-file paths. Required observer failure prevents dispatch;
Fleetd and observer failures after dispatch are retained in the atomically
published report before the process returns a failure status.

## Consequences

- The daemon remains a provider-neutral coordination plane and does not become
  a workload generator or metrics warehouse.
- A run can prove which exact message caused each observed invocation, even
  when unrelated work occurs during the same time window.
- Raw observer documents preserve backend-native evidence and can evolve
  without a Fleetd migration or API change.
- Reports qualify transport and operational behavior only. Application payload
  conformance requires its own contract-aware validator.
- Observer snapshots are bounded points, not continuous time-series storage.
  Higher-frequency collection and backend interpretation belong in external
  observers or analyzers.
