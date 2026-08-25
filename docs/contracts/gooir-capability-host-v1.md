# GOOIR capability host boundary v1

Status: active

fleetd is an execution host and evidence producer for GOOIR documents. It does
not define capability meaning, implementation selection, or conformance policy.
The canonical semantic contract lives in GOOIR; this document records only how
Fleetd transports those documents and binds its runtime evidence to them.

## Message mapping

| Fleetd message kind | GOOIR payload |
| --- | --- |
| `gooir.capability.invocation/v1` | `org.gooi.capability.invocation/v1` |
| `gooir.capability.result/v1` | `org.gooi.capability.result/v1` |

The payload is carried unchanged. The message correlation ID for an invocation
is its exact `invocation_id`. Addressing an invocation to an agent is a Fleetd
routing decision, not a claim that the agent or its harness implements the
capability.

## Package offers

During lifecycle initialization, every plugin returns one
`org.gooi.capability.offers/v1` document. The package may offer any non-empty
set of exact capability implementations. Fleetd uses exact identities for
runtime admission; it does not collapse the package into a provider type or
infer capabilities from the transport protocol.

ACP is one transport used by current agent-session plugins. The typed client
requires exact `open`, `turn_execute`, `permission_resolve`, and `close`
capabilities from `org.gooi.capability.agent_session`. Another protocol can
implement the same capabilities, and ACP can transport other capabilities,
without changing Fleetd's kernel.

## Candidate evidence

Given an exact invocation, implementation offer, and immutable Fleetd result
message, Fleetd can produce a GOOIR candidate. It validates the GOOIR result and
adds an evidence reference with type
`dev.fleetd.evidence/durable_message@1.0.0`, the canonical message digest,
message ID, and global sequence.

The candidate remains unverified. Fleetd does not select a conformance suite,
run a domain verifier, or admit facts into a GOOIR graph.

## Runtime separation

Fleetd alone owns agent addresses, channels, message sequence, deliveries,
leases, session references, binding generations, owner epochs, process groups,
deadlines, write-ahead fences, and settlement. None of those fields are added
to GOOIR invocation or result bodies. They may appear only as typed evidence
attached by the host.

Fleetd core contains no Git, GitHub, GitLab, repository inspection, repository
patch, UI, workflow, or model capability adapter. Those meanings belong in
separately versioned capability packages whose plugins consume and produce
GOOIR.
