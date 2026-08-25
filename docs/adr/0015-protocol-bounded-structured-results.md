# ADR 0015: Structured results use protocol message boundaries

- Status: accepted
- Date: 2026-08-24

## Context

An agent may narrate progress, use tools, and end with a machine-readable final
message. Concatenating every assistant chunk into one string destroys the
protocol boundary. Searching that text for a JSON-looking suffix would be
ambiguous and attacker-controlled.

ACP already supplies the missing primitive: chunks may carry `messageId`, and
a changed ID denotes a new assistant message. Fleetd was discarding it.

## Decision

The typed ACP host preserves message IDs and assembles adjacent chunks into
ordered assistant messages. It does not invent boundaries. Reappearing IDs are
protocol violations, and several un-ID'd messages cannot establish a final
result boundary.

Turn adapters may choose transcript capture or final-assistant-JSON capture.
The controller performs only boundary selection and whole-message JSON parsing.
It retains the complete assistant transcript and records which protocol message
supplied the structured value. It does not search prose, interpret domain
meaning, or establish domain conformance.

The generic Fleetd envelope worker uses transcript capture. An external adapter
that needs structured domain output owns its semantic parsing outside Fleetd.

## Consequences

- Productive progress narration can coexist with a strict machine result.
- Raw assistant evidence remains inspectable and can be bound into host
  evidence.
- Runtimes that omit optional IDs still work for one JSON-only response but
  cannot claim a boundary across multiple messages.
- ACP is one implementation of the message-boundary mechanism. Other
  harnesses may satisfy the same result capture with their own trustworthy
  final-message primitive.
- Durable tool, reasoning, permission, and plan event fragments remain future
  evidence work.
