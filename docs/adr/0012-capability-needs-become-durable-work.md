# ADR 0012: Capability needs become durable work above the kernel

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

GOOIR can now plan typed derivations and emit a `CapabilityNeed` when no local
provider exists. The first live need asks for a runnable Fleetd web artifact
from an exact web target IR fact. Sending that need as prose would discard the
fact identity, expected output, coverage requirement, and conformance suite.

Adding capability columns and scheduling policy to fleetd's message tables
would instead make the coordination kernel understand one orchestration
domain. Treating an OpenCode session as the capability would also couple
semantic work to one transport and harness.

## Decision

GOOIR binds a need to exact input fact instances as a provider-neutral
capability request. fleetd carries that request in the existing immutable open
message envelope. The authenticated sender supplies requester authority, the
explicit recipient supplies initial assignment, the invocation references the
exact message, and the session turn records binding generation and owner epoch.
No new persistence table is required.

A versioned `CapabilityWorkTurnAdapter` validates the request and admits only
an exact configured capability set. It uses one session lane per request
identity and projects the request into the existing typed harness capability.
The worker and controller remain free of OpenCode, Qwen, Codex, or other vendor
conditionals.

Request identity uses RFC 8785 canonical JSON before SHA-256 so producer and
consumer do not depend on Rust field serialization order. Registration or
configuration as eligible means only that a provider may attempt the work. The
initial correlated response is explicitly an attempt, not an accepted output.

## Consequences

The exact GOOIR request survives daemon, worker, and harness restarts under the
already-proven invocation and owner-epoch fences. Replacing the harness plugin
does not change capability meaning. A wrong capability, changed body, partial
complete-only input, or mismatched correlation fails before dispatch.

Provider discovery, conformance execution, review, and acceptance remain
visible next steps instead of being hidden in an agent prompt. Strict
candidate-result extraction was added by
[ADR 0013](0013-raw-attempts-lift-to-unverified-candidates.md); it does not
retroactively make an attempt or candidate trusted.
