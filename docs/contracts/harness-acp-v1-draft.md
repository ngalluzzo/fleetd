# Fleetd ACP harness interface v1 draft

Status: experimental. The adapter is not stable until the Codex and DSH
acceptance matrix passes.

This typed protocol adapter is carried over Fleetd's plugin lifecycle JSON-RPC
transport. It wraps one ACP client connection with Fleetd ownership, fencing,
deadlines, and evidence. Plugins advertise the exact operational interfaces
`fleetd.harness-acp@0.1.0` and `fleetd.harness-acp@0.2.0`; the `harness.acp.*`
names below are their typed methods. Interface identity is matched by equality
rather than by SemVer range, so both are declared and a host requiring either
negotiates. `0.2.0` adds transcript retrieval and nothing else; it stays
unstable until two independent integrations have qualified against it.
The adapter is deliberately smaller than ACP and is not an arbitrary protocol
tunnel. It makes no semantic claim about the work an agent can perform.

## Common rules

- Every request is a JSON-RPC object below the one-MiB lifecycle frame bound.
- Fleet-owned IDs are opaque non-empty strings with a maximum of 256 bytes.
  Native `session_ref` values are opaque strings bounded to 4,096 bytes and
  preserved exactly.
- `binding_generation`, `owner_epoch`, and `event_seq` are positive integers.
  They never decrease for one binding.
- The plugin processes at most the negotiated number of turns concurrently;
  v1 qualification requires a valid single-turn mode.
- A turn event is admissible only when its complete fence matches the current
  durable invocation.
- ACP `_meta`, unknown session updates, and unknown stop reasons are preserved
  as opaque JSON within the evidence bound. An oversized value is represented
  by an explicit truncation record with its observed byte count and digest.
- Inbox lease tokens, agent bearer credentials, and raw provider credentials
  are forbidden in every request, response, event, and initialization config.

The common fence shape is:

```json
{
  "binding_id": "uuid",
  "binding_generation": 1,
  "owner_epoch": 3,
  "invocation_id": "uuid",
  "fence_token": "opaque-random-value"
}
```

## `harness.acp.describe`

Returns the initialized inner runtime and its effective abilities. It does not
repeat desired configuration as if it were observed truth.

Request:

```json
{}
```

Result:

```json
{
  "driver": {
    "version": "0.1.0",
    "acp_sdk_version": "2.0.0",
    "acp_protocol_version": 1
  },
  "runtime": {
    "name": "dsh-acp",
    "version": "0.4.22",
    "executable_digest": "sha256:..."
  },
  "agent_capabilities": {},
  "limits": {
    "max_concurrent_turns": 1,
    "max_frame_bytes": 1048576
  },
  "profile_digest": "sha256:...",
  "raw_initialize_result": {}
}
```

`raw_initialize_result` preserves extension data but is subject to the frame
bound and redaction policy. Readiness fails when observed runtime identity or
ACP feature observations do not match the immutable profile.

## `harness.acp.session.open`

Creates or resumes a native session before an effectful turn begins.

Resuming sends ACP `session/resume`, which must not replay the conversation,
and falls back to `session/load` only for a runtime that advertises no resume
capability. Adoption wants the session back rather than its transcript, and a
replayed entry belongs to no invocation.

Request:

```json
{
  "binding": {
    "binding_id": "uuid",
    "binding_generation": 1,
    "owner_epoch": 3
  },
  "mode": {
    "kind": "create"
  },
  "working_directory": "/absolute/worktree",
  "additional_directories": [],
  "mcp_grants": ["fleet.messaging.send"],
  "resolved_mcp_grants": [
    {
      "name": "fleet.messaging.send",
      "endpoint": {
        "type": "http",
        "url": "http://127.0.0.1:49152/mcp",
        "headers": [
          {
            "name": "x-fleetd-grant-token",
            "value": "ephemeral-redacted-value"
          }
        ]
      }
    }
  ],
  "profile_digest": "sha256:..."
}
```

Resume replaces `mode` with:

```json
{
  "kind": "resume",
  "session_ref": "opaque-native-session-reference"
}
```

Result:

```json
{
  "session_ref": "opaque-native-session-reference",
  "profile_digest": "sha256:...",
  "resumed": false,
  "effective_config": {},
  "raw_session_result": {}
}
```

The host persists `session_ref` before starting a prompt. A create response
lost after ACP `session/new` may leave an orphan session. It must never cause
the host to assume that a prompt was executed.

Resume fails closed when the runtime lacks a qualified load/resume path, the
profile compatibility key differs, or the driver already owns conflicting
local state. The controller is responsible for ensuring that no older process
has live ownership before it grants a higher owner epoch. The driver does not
invent resume by replaying a text transcript into a new session.

Resume also requires the same effective working directory and ordered
additional-directory set. ACP history updates produced by `session/load` are
reported as session-replay evidence during open; they are not invocation
events or candidate output for the next turn.

`mcp_grants` contains names, not credentials or arbitrary child commands. The
effective immutable profile resolves each name to one controller-approved MCP
definition in `resolved_mcp_grants`. This second field is derived
controller-to-driver data, never operator-authored desired state. The current
host accepts only explicit `127.0.0.1` HTTP endpoints, rejects missing,
duplicate, or unrequested resolutions, and redacts header values from debug
output. Ephemeral grant tokens are not fleet bearer credentials and are
not persisted in session-binding or effective-config evidence.

## `harness.acp.turn.start`

Accepts one typed ACP prompt under a durable fleet fence. The response is an
acceptance boundary, not a completion response; progress and terminal evidence
are notifications.

Request:

```json
{
  "fence": {
    "binding_id": "uuid",
    "binding_generation": 1,
    "owner_epoch": 3,
    "invocation_id": "uuid",
    "fence_token": "opaque-random-value"
  },
  "session_ref": "opaque-native-session-reference",
  "source": {
    "agent_id": "recipient-agent-id",
    "message_id": "input-message-id",
    "channel_id": "channel-id",
    "sender_id": "sender-agent-id",
    "correlation_id": "optional-correlation-id",
    "causation_id": "optional-causation-id"
  },
  "prompt": [
    {
      "type": "text",
      "text": "Review the bounded evidence packet."
    }
  ],
  "policy": {
    "idle_timeout_ms": 120000,
    "wall_timeout_ms": 600000,
    "cancel_drain_timeout_ms": 15000,
    "max_captured_output_bytes": 1048576,
    "permission_policy": "controller",
    "tool_budget": {
      "limit": 8,
      "required_enforcement": "observe_then_cancel"
    },
    "token_budget": null
  }
}
```

Result:

```json
{
  "accepted": true,
  "effective_enforcement": {
    "wall_timeout": "hard",
    "idle_timeout": "hard",
    "cancel_drain_timeout": "hard",
    "captured_output_bytes": "hard",
    "tool_budget": "observe_then_cancel",
    "token_budget": "unavailable"
  }
}
```

The driver rejects a fence older than its current adopted owner epoch, a second
active turn on the same session, a profile mismatch, unsupported ACP content,
or an enforcement requirement it cannot meet.

`source` contains immutable fleet envelope identity, not authority. The driver
uses it for evidence attribution and may project it into ACP `_meta`; it never
uses a caller-supplied sender identity to authorize an outbound message.

`accepted: true` means the ACP prompt request has crossed the driver's write
boundary and may execute. Loss after this response is therefore conservatively
`outcome_unknown` until runtime-specific reconciliation proves otherwise.

The host independently enforces the wall deadline. Plugin enforcement is a
second safety boundary, not the authority for extending a lease.

## `harness.acp.turn.event`

Plugin notification:

```json
{
  "jsonrpc": "2.0",
  "method": "harness.acp.turn.event",
  "params": {
    "fence": {
      "binding_id": "uuid",
      "binding_generation": 1,
      "owner_epoch": 3,
      "invocation_id": "uuid",
      "fence_token": "opaque-random-value"
    },
    "event_seq": 7,
    "observed_at_ms": 1787533200000,
    "classification": "tool_call_update",
    "raw_update": {}
  }
}
```

`event_seq` is strictly increasing for an invocation and starts at one. A
duplicate `(invocation_id, event_seq)` with identical content is idempotent; a
different payload is a protocol violation. Unknown classifications are
preserved and do not count as idle-resetting activity unless the contract
explicitly recognizes them.

The controller derives activity from a recognized classification and validated
raw shape; the plugin cannot extend an idle deadline with an arbitrary boolean.
The initial activity set is ACP agent message content, reasoning content, tool
state changes, permission requests, and plan state changes. Metadata-only,
usage-only, keepalive, and unknown updates do not reset the idle deadline.

The driver may coalesce adjacent token fragments into bounded ordered batches.
Coalescing must preserve content order, original ACP update kinds, message/tool
identities, and the first and last observation times. It may not merge across a
tool, plan, permission, or terminal boundary.

## `harness.acp.permission.requested`

Plugin notification used to bridge ACP's agent-initiated permission request
without enabling plugin-initiated requests on the fleetd outer transport:

```json
{
  "jsonrpc": "2.0",
  "method": "harness.acp.permission.requested",
  "params": {
    "fence": {
      "binding_id": "uuid",
      "binding_generation": 1,
      "owner_epoch": 3,
      "invocation_id": "uuid",
      "fence_token": "opaque-random-value"
    },
    "permission_id": "opaque-driver-id",
    "event_seq": 8,
    "tool_call": {},
    "options": [],
    "expires_at_ms": 1787533260000
  }
}
```

## `harness.acp.permission.resolve`

Host request:

```json
{
  "fence": {
    "binding_id": "uuid",
    "binding_generation": 1,
    "owner_epoch": 3,
    "invocation_id": "uuid",
    "fence_token": "opaque-random-value"
  },
  "permission_id": "opaque-driver-id",
  "outcome": {
    "kind": "selected",
    "option_id": "allow_once"
  }
}
```

The resolution is idempotent when its content is identical. Conflicting reuse
is a protocol error. Cancellation resolves every outstanding permission as
cancelled before the prompt can become quiescent. Expiry defaults to denial and
is recorded; it never silently selects a permissive option.

Other ACP agent-to-client requests, including optional filesystem or terminal
services, are not tunneled through this interface. They require another
explicitly negotiated operational interface or remain unadvertised to the
inner agent.

## `harness.acp.turn.cancel`

Request:

```json
{
  "fence": {
    "binding_id": "uuid",
    "binding_generation": 1,
    "owner_epoch": 3,
    "invocation_id": "uuid",
    "fence_token": "opaque-random-value"
  },
  "reason": "wall_deadline"
}
```

Result:

```json
{
  "accepted": true
}
```

Cancellation is idempotent for the same fence. The driver sends ACP
`session/cancel`, continues forwarding terminal updates, cancels outstanding
permission requests, and waits for the original prompt to return. A known,
quiescent response cannot erase the host cancellation: `stop_reason` remains
the host reason and the native response's claim is preserved separately as
`runtime_stop_reason`. The driver emits `harness.acp.turn.terminal` only after
that drain, or after the drain deadline classifies the outcome as unknown and
the ACP process group is terminated.

## `harness.acp.turn.terminal`

Plugin notification:

```json
{
  "jsonrpc": "2.0",
  "method": "harness.acp.turn.terminal",
  "params": {
    "fence": {
      "binding_id": "uuid",
      "binding_generation": 1,
      "owner_epoch": 3,
      "invocation_id": "uuid",
      "fence_token": "opaque-random-value"
    },
    "last_event_seq": 12,
    "stop_reason": "end_turn",
    "execution_certainty": "outcome_known",
    "session_quiescent": true,
    "session_persistence": "confirmed",
    "assistant_messages": [
      {
        "message_id": "optional-acp-message-id",
        "content": [
          {
            "type": "text",
            "text": "Bounded final answer"
          }
        ],
        "complete": true,
        "first_event_seq": 1,
        "last_event_seq": 12
      }
    ],
    "usage": {
      "input_tokens": {
        "value": 7962,
        "scope": "session_cumulative",
        "source": "acp.prompt_response.usage.inputTokens",
        "reliable": true
      }
    },
    "raw_prompt_response": {}
  }
}
```

`runtime_stop_reason` is omitted for a turn the host did not cancel. When a
host deadline or explicit cancellation drains to a known native response, the
terminal instead carries both layers, for example `stop_reason:
"wall_deadline"` and `runtime_stop_reason: "end_turn"`. Admission policy must
use the effective `stop_reason`; the runtime field is evidence, not authority.

Valid execution certainty values are `not_started`, `outcome_known`, and
`outcome_unknown`. `session_quiescent` may be true only after the original
prompt has terminated and all admitted terminal updates are drained.
`session_persistence` is `confirmed`, `runtime_claimed`, or `unknown`; a
quiescent turn must not be promoted to confirmed durability without qualified
runtime evidence.

`assistant_messages` is a deterministic assembly of the ACP message updates,
not a second model response. When capture limits prevent complete assembly,
`complete` is false and the record includes truncation evidence or an artifact
reference. Incomplete output is not result-admissible by default.

ACP `messageId` is preserved. Adjacent chunks with the same ID belong to one
message, and a changed ID begins another. An ID that reappears after a different
message is a protocol violation. When the runtime omits the optional ID, the
host may assemble one un-ID'd contiguous message but must not invent a boundary
that the protocol did not provide. See
[ADR 0015](../adr/0015-protocol-bounded-structured-results.md).

The controller validates result content and policy separately. A terminal
notification never acknowledges an inbox delivery and never grants itself a
retry.

## `harness.acp.session.transcript.start`

Replays one native session's stored conversation. Added in `0.2.0`.

This is retrieval, not adoption or work. It carries the owning binding rather
than a fence, because it starts nothing.

Request:

```json
{
  "binding_id": "uuid",
  "binding_generation": 1,
  "owner_epoch": 3,
  "session_ref": "opaque-native-session-reference"
}
```

Result:

```json
{
  "accepted": true
}
```

The result means the replay is under way, not finished. Entries then arrive as
`harness.acp.session.transcript.entry` notifications and exactly one
`harness.acp.session.transcript.complete` closes the replay. Answering only
after the whole replay would deadlock: a plugin drains its notification channel
between requests, so a long conversation would fill the channel while the
request it belongs to was still being served. A turn already has this shape and
a replay follows it.

The driver refuses a replay when the inner runtime cannot replay at all, when
the supplied binding does not own the session lane, when a turn is active on
that session, and when a replay is already in flight for it. A runtime that
cannot replay reports that rather than returning an empty transcript, which
would read like an agent that did nothing.

## `harness.acp.session.transcript.entry`

One stored conversation entry, as the runtime replayed it. Added in `0.2.0`.

```json
{
  "method": "harness.acp.session.transcript.entry",
  "params": {
    "session_ref": "opaque-native-session-reference",
    "entry_seq": 1,
    "observed_at_ms": 1700000000000,
    "classification": "reasoning_content",
    "raw_update": {"sessionUpdate": "agent_thought_chunk"}
  }
}
```

`entry_seq` orders the replay and is unrelated to a turn's `event_seq`. A replay
carries each entry's final state rather than the streamed updates that produced
it, so an entry is not an event and must never be folded into an invocation's
durable evidence. A transcript notification arriving while a turn is draining is
a protocol violation and fails that turn.

## `harness.acp.session.transcript.complete`

The end of one replay, complete or not. Added in `0.2.0`.

```json
{
  "method": "harness.acp.session.transcript.complete",
  "params": {
    "session_ref": "opaque-native-session-reference",
    "entry_count": 3,
    "observed_payload_bytes": 240,
    "truncated": false,
    "failure": null
  }
}
```

Fleetd cannot bound what a runtime chooses to replay, so the driver bounds it:
10,000 entries and 8 MiB. `truncated` distinguishes a capped replay from a short
conversation, so completeness is never inferred from a stream that stopped.
`failure` carries a bounded diagnostic when the runtime refused mid-replay.

## `harness.acp.session.close`

Request:

```json
{
  "binding_id": "uuid",
  "binding_generation": 1,
  "owner_epoch": 3,
  "session_ref": "opaque-native-session-reference",
  "reason": "rotation"
}
```

Result:

```json
{
  "ownership_retired": true,
  "native_resources_released": true
}
```

Close is allowed only when the binding is not active. If the inner ACP runtime
does not support session close, the driver may still report
`ownership_retired: true` with `native_resources_released: false`. That
distinction is retained as evidence.

## Supervisor-synthesized terminal evidence

The controller creates terminal evidence when the plugin cannot:

- failure before a prompt write: `not_started`;
- process exit or protocol loss after acceptance: `outcome_unknown`;
- forced termination after cancellation drain overrun: `outcome_unknown`.

Synthetic evidence names the plugin instance, exit status, last admitted event
sequence, and exact boundary that was crossed. Absence of a terminal plugin
notification is never converted into success or proof of non-execution.
