# Capability work attempt v2

Status: experimental cross-project dogfood contract

The request remains `work.capability.request/v1`. Version 2 changes only the
provider attempt boundary: progress and the semantic response no longer have
to occupy one assistant message.

## Attempt envelope

The capability-work adapter publishes `work.capability.attempt/v2`. The
immutable Fleetd message keeps its ordinary correlation and causation:

- correlation ID: the exact capability request ID;
- causation ID: the exact request message ID;
- sender: the authenticated provider agent; and
- payload context: the adapter-selected semantic provider and implementation.

The controller retains every captured assistant message and separately records
the JSON value parsed from one protocol-bounded final message:

```json
{
  "status": "completed",
  "invocation_id": "uuid",
  "stop_reason": "end_turn",
  "transcript_complete": true,
  "assistant_messages": [
    {
      "message_id": "progress-1",
      "content": [{"type": "text", "text": "Checking the exact revision."}],
      "complete": true,
      "first_event_seq": 4,
      "last_event_seq": 12
    },
    {
      "message_id": "result-1",
      "content": [{"type": "text", "text": "{\"request_id\":\"sha256:...\"}"}],
      "complete": true,
      "first_event_seq": 28,
      "last_event_seq": 31
    }
  ],
  "structured_result": {
    "status": "captured",
    "source": {
      "selection": "last_identified_assistant_message",
      "message_id": "result-1",
      "first_event_seq": 28,
      "last_event_seq": 31
    },
    "value": {"request_id": "sha256:..."}
  },
  "usage": {},
  "session_persistence": "runtime_claimed",
  "result_context": {
    "request_id": "sha256:...",
    "provider": {}
  }
}
```

`status` describes successful result transport, not semantic conformance. It is
`completed` only when the runtime reports `end_turn`, every retained assistant
message is complete, and the protocol-bounded final JSON value is captured.
Malformed, absent, incomplete, or ambiguous structured output produces
`status: failed` while retaining one closed `structured_result` reason such as
`ambiguous_message_boundary` or `malformed_final_json`.

When a host layer cancels a turn whose terminal state later becomes known and
quiescent, the attempt is durably settled but cannot inherit the runtime's
success claim. Its effective `stop_reason` is the enforcing layer's reason,
such as `wall_deadline`, `idle_deadline`, or the outer controller's
`host_wall_deadline`, and the runtime's terminal claim is retained separately:

```json
{
  "status": "failed",
  "stop_reason": "host_wall_deadline",
  "runtime_stop_reason": "end_turn",
  "structured_result": {
    "status": "unavailable",
    "reason": "malformed_final_json"
  }
}
```

`runtime_stop_reason` is absent on turns the host did not stop. The strict lift
rejects every failed attempt and any payload carrying host-stop provenance,
even if a runtime claimed `end_turn`. A provider may still return the semantic
response status `unable` inside successfully captured JSON; that is a complete,
explicit negative result rather than a transport failure.

## Boundary rules

ACP `messageId` is the authority for assistant-message boundaries. The host
groups adjacent chunks with the same ID. A changed ID starts a new message; an
ID that reappears after another message is a protocol violation.

The controller selects a result only under one of two deterministic rules:

1. one assistant message is the `only_assistant_message`, even when the ACP
   runtime omitted its optional ID; or
2. when there are several messages, every message has a distinct ID and the
   result is the `last_identified_assistant_message`.

The selected message must be completely captured, contain only text content,
and consist entirely of one raw JSON value. Fleetd never searches arbitrary
prose, strips Markdown fences, or scans earlier messages for JSON.

The strict lift recomputes the selection from `assistant_messages`, reparses
the final text, and requires exact equality with `structured_result.value`.
It then applies the unchanged v1 semantic checks: exact request and suite,
exact requested output set, unverified conformance status, bounded unable
diagnostics, and adapter-owned provider identity. A captured result is still
only a candidate; independent conformance remains mandatory.

## Compatibility and limits

Historical `work.capability.attempt/v1` messages remain extractable under their
original one-message JSON-only rule. New capability workers emit v2.

The durable attempt retains the complete captured assistant transcript. Tool,
reasoning, permission, and plan event persistence remains a separate runtime
evidence slice; v2 does not falsely claim that terminal assistant messages are
the complete execution trace.
