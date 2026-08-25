# ADR 0005: ACP is an inner harness protocol

- Status: accepted
- Date: 2026-08-24

## Context

Fleetd needs replaceable agent harnesses without teaching the messaging kernel
about sessions, prompts, model providers, tools, or vendor process layouts.
Raw protocol tunneling would let controller code call unreviewed methods and
would leave fencing, cancellation, and evidence ambiguous.

## Decision

ACP v1 remains inside vendor-owned harness plugins. Those plugins negotiate the
exact Fleetd operational interface `fleetd.harness-acp@0.1.0`. The interface
exposes typed description, session open/resume, fenced turn start, permission
resolution, cancellation, ordered events, terminal evidence, and close.

ACP itself is not a Fleetd plugin interface: the shared host translates between
Fleetd's bounded controller contract and the authoritative ACP SDK. A future
non-ACP harness can implement the same Fleetd interface through a different
inner protocol, while another Fleetd interface could use ACP without changing
the lifecycle protocol.

The worker requires only the exact operational interface. Vendor launch
arguments, environments, model routing, and native session persistence remain
plugin-owned. Fleetd owns leases, invocation fences, session bindings, owner
epochs, deadlines, settlement, and ambiguity policy.

## Consequences

- The messaging kernel has no harness concepts.
- The controller cannot issue arbitrary JSON-RPC or ACP methods.
- Plugin compatibility is exact and operational, never inferred from a vendor
  name.
- Speaking the interface makes no claim about the semantic tasks an agent can
  perform.
- The interface remains experimental until two independent vendor plugins pass
  the same real-runtime qualification matrix.
