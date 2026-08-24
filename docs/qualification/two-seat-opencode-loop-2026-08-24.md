# Two-seat OpenCode message loop qualification — 2026-08-24

## Scope

This checkpoint composes the continuous worker, durable session binding, and
invocation-scoped `fleet.messaging.send` capability across two real OpenCode
seats. It tests a bounded three-hop exchange:

```text
upstream -> A -> B -> A -> upstream
```

Both seats used `fleetd.harness.opencode`, OpenCode 1.4.0, model route
`opencode/gpt-5.6-sol`, and the semantic-neutral envelope adapter. Each worker
process received only the named grant. Fleetd resolved it to an ephemeral
controller-owned MCP endpoint; neither OpenCode process received a Fleetd
bearer credential.

The run used three `worker run --once` processes: A, then B, then a fresh A.
This made message eligibility explicit while also forcing A to recover its
durable native session after process exit.

## Durable exchange

Upstream seed `68143320-f20c-45e9-8ea9-b3ae923601f5` asked A to delegate one
exact instruction to B. The following capability-authored messages committed:

| Sequence | Message | Sender -> recipient | Kind | Correlation | Causation |
| --- | --- | --- | --- | --- | --- |
| 2 | `fbf2cd7f-a3d0-452a-9145-b88d89ce31ad` | A -> B | `loop.delegate` | seed | seed |
| 4 | `5198c892-553d-48a3-8a07-dbf56a76b4fa` | B -> A | `loop.reply` | seed | sequence 2 |
| 6 | `65f31597-1223-406d-aa5d-423f7492ab68` | A -> upstream | `loop.final` | seed | sequence 4 |

The final payload was:

```json
{"proof":"a-b-a","status":"complete","answer":42}
```

Each tool call supplied only operation ID, recipient, kind, and payload.
Independent catalog inspection verified that Fleetd derived sender, channel,
correlation, causation, and idempotency from the active invocation. All three
invocations armed once and completed with `outcome_known`; there were no
retries, worker restarts, or durable blocks.

## Restart and resumption evidence

A's first invocation used binding generation 1 and owner epoch 1. After that
worker exited, the return hop was processed by a newly launched A worker. It
reacquired the same binding generation and opaque native session reference at
owner epoch 2. Both A turns and B's turn became quiescent with
`runtime_claimed` session persistence.

This proves that the loop did not depend on one in-memory process or three
unrelated native sessions. It does not prove recovery from an interrupted
armed turn; that remains covered by the controller's conservative block
semantics and automated crash-window tests.

## Boundary exposed by the run

The envelope worker currently treats every addressed message as eligible
input. Atomic completion also publishes a result message to the input sender.
Two unbounded seats can therefore consume each other's generic completion
results and create an accidental response loop even though explicit peer
messages are correct. The bounded run avoided that by reserving one intended
message per process, leaving the final automatic result unread.

This is not a reason to add workflow meaning to the kernel. The next slice
needs an explicit, versioned inbound acceptance policy at the worker/adapter
boundary: a seat must declare which message contracts it consumes and what
terminal result contract it emits. Continuous multi-seat dogfood should remain
bounded until that policy exists.

This checkpoint qualifies one harness implementation only. It does not make
`fleet.messaging.send` stable under the repository's two-implementation rule,
and it does not claim semantic validation of arbitrary payloads, group sends,
inbox-read authority, or remote transport.
