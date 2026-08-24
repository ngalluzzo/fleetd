# Continuous two-seat OpenCode loop qualification — 2026-08-24

## Scope

This checkpoint reruns the A -> B -> A message-capability exchange with A and B
started concurrently as unbounded continuous workers before the seed exists.
It qualifies inbound acceptance v1 against real OpenCode turns and proves that
cross-addressed completion envelopes do not become accidental work.

Both seats used OpenCode 1.4.0, model route `opencode/gpt-5.6-sol`, the
`fleet.messaging.send` grant, and worker desired-state schema 2. A accepted
only `loop.start` and `loop.reply`; B accepted only `loop.delegate`.

## Exchange

Upstream seed `8e4976a5-cfb7-4d53-87e1-a0d8eef1337c` produced these
capability-authored hops:

| Sequence | Message | Sender -> recipient | Kind | Correlation | Causation |
| --- | --- | --- | --- | --- | --- |
| 2 | `a0e3506e-3c63-4b10-905c-40db9251a677` | A -> B | `loop.delegate` | seed | seed |
| 4 | `47c872f0-c19b-4230-b0d5-d93a80e98c74` | B -> A | `loop.reply` | seed | sequence 2 |
| 5 | `f77bc6e6-bff6-4f56-b7e7-ad757b329dd6` | A -> upstream | `loop.final` | seed | sequence 4 |

The final payload was:

```json
{"proof":"continuous-a-b-a","status":"complete","answer":42}
```

A completed two turns in one plugin generation and reused one ready session
binding. B completed one turn in one generation. Together they recorded three
reservations, three known completions, zero restarts, zero blocks, and zero
pre-arm retries.

## Non-consumption proof

Atomic completion also produced:

- sequence 6, `loop.a.result`, addressed from A to B;
- sequence 7, `loop.b.result`, addressed from B to A.

Both workers remained live for more than ten seconds after the final message,
polling every 200 milliseconds. A reached 267 idle polls and B reached 286.
Catalog inspection then showed both result deliveries still `pending`, with
attempt 0 and no lease. The invocation count remained exactly three: two for A
and one for B.

This proves a non-matching earlier or later envelope is not leased, released,
acknowledged, or converted into an invocation merely because a continuous seat
is polling. Automated tests separately prove that an earlier non-match does not
block a later match and that changing the acceptance set rotates session
compatibility.

This checkpoint qualifies one exact-kind selector and one harness
implementation. It does not stabilize the message capability under the
two-implementation rule or establish payload-, sender-, or lineage-based
selection.
