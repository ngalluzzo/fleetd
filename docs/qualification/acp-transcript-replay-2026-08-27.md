# ACP transcript replay qualification — 2026-08-27

## Scope

This checkpoint measures what an ACP `session/load` replay actually contains,
because [ADR 0020](../adr/0020-bounded-operational-observations.md) delegates the
raw trajectory to "the native harness transcript" and
[ADR 0029](../adr/0029-harness-transcript-retrieval.md) depends on that
transcript being complete enough to be worth retrieving.

It qualifies the ACP replay contract against one real harness. It does not
qualify a Fleetd interface — no Fleetd code participated. The probe spoke ACP
directly to `opencode acp` over stdio so the measurement is of the harness, not
of Fleetd's translation of it.

Two phases per run:

1. a fresh `opencode acp` process, `session/new`, one prompt requiring both
   reasoning and a file-reading tool, recording every `session/update`;
2. a **separate** `opencode acp` process, started after the first was killed,
   `session/load` on the same session id, recording every `session/update`.

Runs used OpenCode 1.4.0 with the model selected per session through
`session/set_config_option`, so the operator's own configuration was never
modified.

## Reasoning survives byte for byte

Model `zai-coding-plan/glm-5.3`, session
`ses_fbbaeb4d9ffebnsqbi1ZnjBeYT`.

| Update | Live | Replayed |
| --- | --- | --- |
| `user_message_chunk` | 1 | 1 |
| `agent_thought_chunk` | 127 | 2 |
| `tool_call` | 1 | 1 |
| `tool_call_update` | 2 | 1 |
| `agent_message_chunk` | 33 | 1 |
| `usage_update` | 1 | 1 |
| `available_commands_update` | 0 | 1 |

The collapse is chunk coalescing, not loss. Concatenating the text of every
update of one kind gives identical results in both phases:

- reasoning: 471 characters live, 471 replayed, strings equal;
- assistant message: 134 characters live, 134 replayed, strings equal.

The replayed reasoning opens and closes exactly as the live stream did:

```
The user wants me to think step by step, read notes.txt in the current working
directory, reason about whether the port it names is a well-known OpenTelemetry
port, and reply with one sentence. Let me
...
 yes, 4318 is a well-known OpenTelemetry port -- it's the standard OTLP over
HTTP port. I should reply with one sentence.
```

Nothing present live was absent from the replay. The one addition,
`available_commands_update`, is session setup rather than conversation.

## Tool calls replay with their arguments and output

An earlier run on the operator's default model produced one tool call. Its
replayed entry carried `toolCallId`, `kind`, `status`, `title`, `rawInput` with
the exact resolved file path, `rawOutput`, and the tool's output `content`.

The live stream contained three tool notifications — `pending`, `in_progress`,
`completed` — and the replay contained one, at `completed`. A replay is each
entry's final state.

## What is therefore lost

Only two things, both about *when* rather than *what*:

- streaming chunk boundaries, so the progression of a message or a reasoning
  block within one entry is gone;
- intermediate `tool_call_update` states, so a tool call that was `pending` and
  then `in_progress` replays only as `completed`.

Content is complete. An operator reading a replay learns what the agent
reasoned, which tools it called with which arguments, what those tools
returned, and what it concluded.

## Limits

One harness. OpenCode persists reasoning as a conversation part for a model
that emits it; a harness or model that never emits `agent_thought_chunk` has
none to replay, and `opencode export` on such a session shows only text, tool,
and step parts. Reasoning retrieval is therefore a property of the
harness-and-model pair, not of ACP.

Not exercised: a session containing many turns (all runs held one), transcript
retrieval through a Fleetd interface, `session/list` or `session/delete`
lifetimes, or how long OpenCode retains a session before pruning.

`opencode/big-pickle/high` stalled past 240 seconds producing zero updates on
two attempts. Unrelated to the replay question, but it is why the reasoning
measurement used the Z.ai route.
