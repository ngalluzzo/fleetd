# ADR 0004: Integrations use out-of-process plugins

- Status: accepted
- Date: 2026-08-24

## Context

Fleetd must adopt new harnesses and external systems without growing those
domains into its coordination kernel. Rust dynamic libraries would bind
integrations to Fleetd's compiler and ABI while allowing a plugin fault to
corrupt the daemon.

## Decision

Integrations run in supervised child processes. Plugins speak JSON-RPC 2.0 over
newline-framed stdin/stdout and advertise narrow, independently versioned
operational interfaces during initialization. An interface identifies a typed
wire contract; it is not a semantic capability claim.

Fleetd launches an absolute executable directly, never through a shell. The
child starts with an empty environment and receives only explicit opaque
configuration. Fleetd credentials are never passed to plugins; narrow
invocation grants mediate any controller-authorized service.

Standard output is protocol traffic only. Standard error is discarded by
default because arbitrary output may contain secrets. A child process provides
crash isolation, not a security sandbox.

The lifecycle methods are initialization, health, notifications, and shutdown.
There is no generic `execute` method. Typed clients invoke exact operational
interfaces over the lifecycle transport.

## Consequences

Plugins can use any language and release cadence. Exact interface negotiation
prevents silent fallback when a required wire contract is absent. The
supervisor bounds frames, startup, calls, shutdown, and complete process-group
cleanup without interpreting message or task semantics.
