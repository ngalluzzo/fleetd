# OpenCode transcript retrieval qualification — 2026-08-27

## Scope

This is the first run of `fleetd transcript` and
`fleetd.harness-acp@0.2.0` against a real vendor harness rather than a mock ACP
runtime, and it answers the segmentation question
[ADR 0029](../adr/0029-harness-transcript-retrieval.md) left open: whether a
session holding several invocations comes back attributable.

The run used OpenCode 1.4.0 through `fleetd.harness.opencode`, model route
`zai-coding-plan/glm-5.3-flash`, the semantic-neutral envelope adapter, and two
`worker run --once` turns addressed to one channel so both landed on one native
session lane. An isolated database, loopback daemon, and two agent identities
were created solely for this qualification.

## Two invocations, one lane, both attributed

Session `ses_fbaee2935ffe4eJChKuGhhrWKt` held both turns. One
`fleetd transcript` read returned eight entries, untruncated:

```
classifications: metadata 1, unknown 2, reasoning_content 2,
                 agent_message_content 2, usage 1
```

The two `unknown` entries are the replayed user messages — the prompts Fleetd
sent. Each carried its envelope intact, 779 characters across multiple lines, as
**one** entry rather than several:

```
You received the following durable fleetd message. Act on its request and make
your final response suitable to return to the sending agent. ...

{
  "invocation": {
    "id": "d99be49a-64e9-4cbb-bcc6-7830442fbb30",
    "delivery_attempt": 1
  },
  ...
```

Extracting each envelope from its first `{` yielded
`d99be49a-64e9-4cbb-bcc6-7830442fbb30` and
`eaaaef0f-ac2c-45ac-90e8-c8df769aeed8`. Both matched invocations
`fleetd invocation list` reported for the agent, and the segment count equalled
the invocation count exactly.

So per-invocation attribution is exact. It is not a projection, and it needs no
cooperation from the harness beyond replaying prompt text verbatim.

## The product now does the splitting

A second run of the same shape, after `segment_transcript` and the `prompt`
event class landed, confirmed the command does this itself. Session
`ses_fbad07470ffenvqt4NSeFTa4Om`, two dispatched invocations:

```
turns: 3  attributed: 2  of 2 dispatched invocations
  seq  1  None                                  unattributed  [metadata]
  seq  2  3defc1c8-547d-4ce6-bd73-d9e257cbfc81  known
              [user_message_content, reasoning_content, agent_message_content]
  seq  5  8144601d-680b-4b0f-b4b6-b6d08d286f7e  known
              [user_message_content, reasoning_content, agent_message_content, usage]
```

The set of attributed invocation ids equalled the set Fleetd dispatched, each
appeared in exactly one turn, and the session setup that precedes the first
prompt stayed in its own unattributed group rather than being folded into the
first real turn. The prompts now classify as `user_message_content` rather than
falling through to `unknown`.

## Two mistakes this run corrected

ADR 0029 originally proposed attributing entries to invocations **by time
window**, matching them against each observation's `first_event_at_ms` and
`last_event_at_ms`. That is impossible. No replayed update carries an original
timestamp — the keys a real replay contains are `content`, `messageId`,
`sessionUpdate`, `toolCallId`, `kind`, `status`, `rawInput`, `rawOutput`,
`title`, and `locations` — and the `observed_at_ms` Fleetd stamps on an entry is
when the replay was read. This run saw three distinct timestamp values across
eight entries, all within the read.

The replacement proposal was also wrong in a smaller way: the envelope does not
arrive as bare JSON. The adapter prepends an instruction preamble, so parsing
the whole user message fails and a reader must take the text from its first
brace.

## An unnamed classification

`classify_update` has no case for ACP's `user_message_chunk`, so it falls
through to `unknown`. Two consequences, both pre-existing and neither caused by
transcript retrieval:

- the entries that carry the segmentation key are labelled `unknown` rather than
  named, so a reader has to inspect `raw_update.sessionUpdate`;
- a live turn against OpenCode also emits `user_message_chunk`, so every managed
  OpenCode turn has been incrementing `InvocationEventCounts.unknown` by one for
  a recognised ACP kind. ADR 0020 keeps that counter because "an unrecognized
  update is the one an operator most needs to see"; a constant offset on every
  turn is exactly what makes such a signal useless.

Naming it costs an `EventClass` variant, a counter field, and a forward
migration. It is recorded here rather than fixed in the same change.

## Limits

Two invocations, one harness, one model route, one read. Not exercised: a
session holding a night of invocations, where compaction, pruning, or a
rewritten prompt could break the key that makes attribution exact; Codex, which
is unqualified even at `@0.1.0`; a session whose turns were not all dispatched
by Fleetd; and a transcript large enough to reach the 10,000-entry or 8 MiB
bound.

The OpenCode Zen provider was unusable throughout this session, stalling past
300 seconds with no output on three attempts, which is why the run used the
Z.ai route.
