# ADR 0015: Structured results use protocol message boundaries

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

The first model-backed runnable-web attempt demonstrated a real mismatch. The
agent productively narrated progress, used tools, and ended with a valid JSON
response, but Fleetd's ACP host concatenated every assistant chunk into one
message. Strict extraction correctly rejected the mixed prose. Searching that
text for a JSON-looking suffix would be ambiguous and attacker-controlled.

ACP already supplies the missing primitive: chunks may carry `messageId`, and
a changed ID denotes a new assistant message. Fleetd was discarding it.

## Decision

The typed ACP host preserves message IDs and assembles adjacent chunks into
ordered assistant messages. It does not invent boundaries. Reappearing IDs are
protocol violations, and several un-ID'd messages cannot establish a final
result boundary.

Turn adapters now choose a result-capture mode. Ordinary envelope turns retain
the existing transcript-only payload. Capability work requests
`FinalAssistantJson` and publishes `work.capability.attempt/v2`, containing the
captured assistant transcript plus a structured result sourced from either the
only assistant message or the last of several distinctly identified messages.

The controller performs only boundary selection and whole-message JSON
parsing. The capability-work lift independently recomputes that selection and
parsing before applying semantic output checks. Neither layer searches prose,
interprets capability meaning in the messaging kernel, or establishes
conformance.

## Consequences

- Productive progress narration can coexist with a strict machine result.
- Raw assistant evidence remains inspectable and bound into candidate identity.
- Runtimes that omit optional IDs still work for one JSON-only response but
  cannot claim a boundary across multiple messages.
- Attempt v1 remains immutable and extractable; new workers emit v2.
- ACP is one implementation of the boundary, not the capability itself. Other
  harnesses may satisfy the same result capture with their own trustworthy
  final-message primitive.
- Durable tool, reasoning, permission, and plan event fragments remain future
  evidence work.
