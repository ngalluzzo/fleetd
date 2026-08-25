# ADR 0020: GOOIR capabilities, Fleetd execution host

Status: accepted

## Context

The first dogfood path duplicated capability semantics inside Fleetd. It added
a local provider descriptor, a capability-work protocol, implementation
selection inside the worker, and repository-specific adapters. In the opposite
direction, GOOIR temporarily acquired Fleetd dialect crates and a process
supervisor. That made both systems less composable: protocol, capability,
plugin, provider, and execution host were becoming aliases for one another.

## Decision

GOOIR owns capability and fact meaning. A plugin is an installable package that
offers one or more exact GOOIR capability implementations. A transport protocol
is only one way to invoke those implementations.

Fleetd consumes and produces the neutral GOOIR offer, invocation, result,
candidate, and evidence documents. Fleetd owns durable runtime state and
process policy. Its generic worker selects message kinds, not semantic
implementations, and passes an immutable Fleetd envelope to the selected
harness. It never constructs a domain prompt or parses a domain response.

Fleetd may implement Fleetd-owned capabilities such as durable messaging, but
it advertises them through GOOIR identities and keeps their runtime grants
separate from capability meaning. Product and repository semantics remain in
external packages and plugins.

## Consequences

- Git, GitHub, and GitLab can offer overlapping and independent capabilities
  without inheriting one repository-provider interface.
- OpenCode, Codex, DSH, and future runtimes can implement the same agent-session
  capabilities over ACP or another protocol.
- One plugin can offer several capabilities; one capability can have several
  implementations; composition happens over exact identities.
- Fleetd can be replaced by another execution host without changing GOOIR
  semantics, and GOOIR can evolve without absorbing Fleetd lifecycle state.
- Domain-specific prompt construction, parsing, and conformance must live in
  capability packages or their plugins, even when Fleetd is the dogfood host.

This supersedes the removed capability-work and repository-adapter experiments.
