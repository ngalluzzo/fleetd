# ADR 0013: Raw attempts lift to unverified candidates

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

The first capability-work slice durably carried an exact GOOIR request through
a fenced harness turn, but its result was only terminal evidence. Treating the
assistant's JSON as accepted output would conflate four identities: semantic
provider, harness plugin, protocol, and conformance authority. It would also
make an agent's statement that tests passed sufficient to enter semantic facts.

## Decision

Capability-worker configuration now names exact semantic providers with an
implementation digest. The adapter, not the agent, selects this descriptor and
the controller stores it as immutable result context beside the untouched
terminal evidence. Harness and protocol identities remain separate.

A strict lift consumes the exact request and immutable attempt message. It
validates message kind, correlation, and causation, and derives attempt identity
and provider-agent authority from the durable envelope. Inside the raw payload
it accepts only one complete text assistant message containing one raw JSON
object. The object must bind the exact request and suite, mark conformance
`unverified`, and contain either the complete requested output set or an
explicit unable result. It does not parse Markdown or recover JSON
heuristically from prose.

The resulting `CapabilityCandidate` binds request, provider, implementation,
outputs, attempt message and invocation identities, and an RFC 8785/SHA-256
digest of the complete attempt message. Fleetd can reconstruct it
deterministically with `fleetd work extract`; it does not declare it trusted.

[ADR 0015](0015-protocol-bounded-structured-results.md) later introduced
attempt v2. It preserves this semantic lift while allowing progress and result
to occupy distinct protocol-identified assistant messages. Attempt v1 keeps
the exact one-message rule recorded here.

## Consequences

- Raw terminal evidence remains the durable source and can be re-lifted.
- A changed prompt protocol does not silently change semantic-provider identity.
- Missing, duplicated, partial, malformed, or prose-wrapped outputs fail closed.
- An explicit unable result is observable without fabricating candidate facts.
- GOOIR and Fleetd now agree on exact candidate identity across repositories.
- The independently implemented named conformance suite and accepted-result
  publication remain the next product boundary.
