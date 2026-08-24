# ADR 0004: Domain behavior uses out-of-process capability plugins

- Status: accepted
- Date: 2026-08-24

## Context

fleetd must adopt new harnesses, forges, model runtimes, communication systems,
and policies without growing those domains into its coordination kernel. A Rust
dynamic-library interface would bind plugins to fleetd's compiler and ABI while
letting a plugin crash or corrupt the daemon.

## Decision

Domain-specific behavior runs in supervised child processes. Plugins speak
JSON-RPC 2.0 over newline-framed stdin and stdout and advertise narrow,
independently versioned capabilities during initialization. The lifecycle
protocol is versioned separately from capability contracts.

fleetd launches an absolute executable directly, never through a shell. The
child starts with an empty environment and receives only explicitly granted
configuration. fleetd credentials are not passed to plugins; future host
capabilities mediate inbox, storage, and network operations.

Standard output is exclusively protocol traffic. Standard error is discarded
by default because arbitrary plugin output may contain secrets; structured
diagnostics will use an explicit capability contract. A plugin process provides
crash isolation, not a security sandbox. OS-level sandboxing remains a future
deployment boundary.

The stable lifecycle methods are initialization, health, and shutdown. No
generic `execute` method exists. Codex and DSH will be implemented together
before a harness capability contract is stabilized.

## Consequences

Plugins may be written in any language and upgraded independently. Capability
negotiation prevents silent fallback when a required behavior is absent. The
supervisor can bound framing, startup time, and shutdown time and can attribute
process exits without interpreting domain payloads.

Process startup and JSON serialization add overhead, but agent operations are
coarse enough that isolation and interoperability dominate. High-frequency
streaming contracts may add binary side channels later without changing the
lifecycle protocol.
