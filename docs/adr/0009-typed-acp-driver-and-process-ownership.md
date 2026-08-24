# ADR 0009: The ACP driver is typed and shares process ownership with its runtime

- Status: accepted
- Date: 2026-08-24

## Context

ADR 0005 selects ACP as the inner harness protocol, but leaves two dangerous
implementation choices open. Exposing generic JSON-RPC calls would let every
controller invent an unversioned capability surface. Supervising only the outer
driver process would also leave its ACP adapter, model gateway, or tool
descendants alive after a driver crash or host drop.

The ACP ecosystem changes quickly enough that fleetd should use an
authoritative SDK and preserve extension data, while retaining strict control
over which operations cross the trusted boundary.

## Decision

The host exports `HarnessAcpClient` as the only domain-call surface for
`harness.acp` v1. Generic capability calls remain crate-private. The client
validates local bounds before dispatch and admits notifications only when their
complete fence and contiguous event sequence match its active turn.

The generic driver is a separate workspace executable. Its outer transport is
fleetd lifecycle JSON-RPC; its inner transport uses the exactly pinned official
Rust ACP SDK. Raw typed SDK wrappers retain unknown initialize results, session
updates, permission requests, and prompt responses within explicit bounds. The
driver verifies observed runtime name, version, adapter digest, protocol
version, and effective capabilities against a strict immutable profile. It
receives no ambient environment or fleet credential and grants the runtime only
an allowlist of non-secret environment settings.

The supervisor creates one operating-system process group for the plugin. The
driver's launcher joins the inner ACP adapter to that same group without a
shell. Startup failure, observed exit, forced shutdown, and object drop kill the
complete group. A future Windows implementation must provide the equivalent
job-object ownership before that platform is supported.

The initial managed controller is deliberately one-turn and deny-by-default. It
requires a reserved invocation and a previously persisted native session
reference, commits the write-ahead arm before prompt dispatch, denies bridged
permissions, drains ordered evidence, atomically completes known quiescent
output, and parks all post-arm ambiguity.

## Consequences

Adding a new fleet harness operation requires an explicit contract and typed
host method. Fast-moving ACP parsing stays outside the daemon, while runtime
identity and unknown extension data remain observable. A dead driver cannot
leave its inner adapter running as an unowned agent.

This is process containment, not an OS security sandbox. The capability remains
experimental until Codex and DSH pass the full common matrix. Durable session
bindings, event persistence, generation adoption, approved MCP brokering, and a
continuous inbox loop remain separate work.
