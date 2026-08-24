# ADR 0005: Use ACP inside the harness execution boundary

- Status: proposed
- Date: 2026-08-24

## Context

fleetd needs Codex, DSH, and future harnesses to occupy replaceable worker
seats. Inventing a fleet-specific prompt, session, tool-update, permission, and
cancellation protocol would duplicate a mature boundary and make each harness
integration bespoke.

Buzz already operates multiple harnesses through the Agent Client Protocol
(ACP). The local DSH qualification demonstrated ACP v1 session creation,
streaming, tools, cancellation, and persistent-session continuation with
`@deepseek-ai/dsh` `0.1.1-rc.2`,
`@openma/deepseek-harness-acp` `0.4.22`, and
`@agentclientprotocol/sdk` `1.4.0`.

Buzz also demonstrates the failure mode to avoid: durable delivery, session
ownership, retry state, and dead lettering can accumulate inside a large
in-memory ACP bridge even though the bridge itself is not persistent.

## Decision

ACP v1 is the inner harness interoperability protocol. fleetd will build one
generic ACP driver plugin and qualify it against both `codex-acp` and
`dsh-acp`. Harness-specific shims are added only for observed incompatibilities
or optional capabilities.

The outer boundary remains fleetd's plugin lifecycle plus a narrow,
independently versioned `harness.acp` capability. That capability adds only the
semantics ACP cannot own for fleetd:

- durable invocation identity;
- session binding generations and owner fencing;
- effective profile identity;
- host-enforced deadlines and declared budget strength;
- bounded evidence attribution;
- conservative terminal certainty.

It does not expose arbitrary ACP or JSON-RPC methods. ACP content and extension
data pass through typed fields, and the driver uses an authoritative ACP SDK
rather than reimplementing the wire schema.

fleetd owns delivery, leases, invocation state, session ownership, retry
policy, result admission, and correlated output messages. The harness owns its
native transcript, compaction, model loop, tools, sandbox, and resume
mechanics. fleetd stores only an opaque native session reference plus bounded
control evidence.

ACP v2 is not selected by this decision. It remains unstable and uses a
different asynchronous prompt lifecycle. Supporting it requires an explicit
qualification and may require a new driver capability revision.

## Consequences

Codex and DSH can exercise one real capability without pretending their
internal architectures are identical. A driver or harness crash cannot erase
the fleet inbox or current invocation fence. DSH can retain its stronger
append-only session semantics without fleetd duplicating its transcript.

The driver adds one process hop and must translate fleet cancellation and
evidence rules carefully. ACP observations alone cannot hard-limit tools that
the harness admits internally, so tool budgets must report their real
enforcement strength. Process loss after prompt acceptance remains an unknown
outcome unless harness-specific evidence proves otherwise.

The capability stays experimental until the acceptance matrix in
[`HARNESS_EXECUTION.md`](../HARNESS_EXECUTION.md) passes through Codex and DSH.
Its current wire draft is
[`harness-acp-v1-draft.md`](../contracts/harness-acp-v1-draft.md).

## Alternatives considered

### Embed an ACP client directly in the Rust daemon

This removes one process hop, but binds fast-moving ACP SDK and adapter behavior
to fleetd releases and puts protocol/parser failures in the trusted daemon. The
workload is coarse enough that independent upgrade and crash isolation are
worth the hop.

### Treat a raw ACP harness as the fleetd lifecycle plugin

ACP initializes an agent session, but it does not provide fleetd's desired
profile identity, process generation, health, fencing, or terminal certainty.
Adding those as ad hoc ACP extensions would couple every harness adapter to
fleetd. The generic outer driver adds them once.

### Implement one fleetd plugin per harness

This duplicates ACP client, cancellation, permission, observation, and
supervision logic. A harness-specific shim remains possible after a measured
gap, but is not the starting architecture.

### Use DSH Agent Teams as the fleet scheduler

That creates a second, process-local identity and scheduling plane whose
messages and ownership do not share fleetd's durable inbox. DSH subagents may
remain an internal optimization behind one fleet agent identity.
