# Interruptible Qwen turn qualification — 2026-08-28

## Claim

A newer accepted message addressed to the same agent and channel can interrupt
an armed Qwen turn without locking the channel. Fleetd durably settles the old
invocation, retires the cancellation-tainted native session, and handles the
follow-up in a fresh native session generation built from durable channel
history.

## Runtime under test

- Fleetd development worker and OpenCode harness plugin `0.1.0`
- OpenCode `1.4.0` over ACP v1
- MLX-VLM `0.6.15` on `127.0.0.1:18082`
- Qwen model `/Users/ngalluzzo/Models/qwen3.8-27b-8bit`
- Qwen MTP draft model `/Users/ngalluzzo/Models/qwen3.8-27b-mtp-8bit`
- model-server thinking enabled; OpenCode reasoning effort `xhigh`
- worker interruption reconciliation interval: 250 milliseconds

## Live result

The first request was message
`7e1deade-a1d5-4365-b12c-dbf4b97aa54c`, channel sequence 52. Once its
invocation was `dispatch_armed`, message
`2f72a6e5-9f36-4b97-9e1a-002c7a91a9ea`, sequence 53, directed the agent to
stop and answer in exactly one sentence.

Fleetd committed result `558f7d86-23de-426c-b067-7e259ad3c45a`, sequence 54,
156 milliseconds after the follow-up. It was causally linked to sequence 52
and reported:

```json
{
  "status": "interrupted",
  "stop_reason": "host_newer_message",
  "interrupted_by_message_id": "2f72a6e5-9f36-4b97-9e1a-002c7a91a9ea"
}
```

The worker retired native session binding generation 2, opened generation 3,
and reserved the follow-up normally. Qwen completed the new request, and
Fleetd committed result `3ba82e9a-0740-4c27-bc52-1e9d2eba044b`, sequence 55,
with `status: "completed"` and `stop_reason: "end_turn"`. Its one assistant
sentence confirmed that sequence 53 replaced sequence 52. The replacement
binding then returned to `ready`, and no invocation remained active.

The supervisor was also stopped with `SIGTERM` after rebuilding this slice. It
shut down its dependent worker, plugin, and shared backend cleanly, exited with
status zero, and was restarted by launchd. The active-turn shutdown path is
covered separately by the automated cancellation test below.

## Failed first attempt and resulting policy

The first live attempt settled the old invocation correctly but reused the
same OpenCode native session. Its follow-up produced no assistant output and
failed at the 180-second idle deadline. MLX-VLM recorded
`stream_closed_before_completion` for the cancelled request.

That result narrowed the policy: quiescent cancellation proves the old
invocation can be settled, but it does not prove the harness's private session
state is reusable. Fleetd now rotates the native session after a
`newer_message` interruption. Worker shutdown still drains without opening a
replacement because no follow-up will be routed by that worker generation.

## Automated evidence

- The worker integration test starts a cancellation-aware mock harness,
  appends a newer accepted message only after dispatch is armed, and proves the
  old result is interrupted, the follow-up completes, binding generation 1 is
  retired, binding generation 2 is ready, and the plugin process is not
  restarted.
- The backlog test proves messages already present at turn start do not cause
  interruption.
- The active-shutdown test proves worker cancellation drains and settles an
  armed turn cleanly.
- Controller tests preserve wall-deadline and unknown-outcome safety while
  distinguishing host interruptions from ordinary completion.

The repository's full `bin/ci` gate passed for this qualification revision,
including all Rust and JavaScript tests, the crash/restart demonstration,
generated-artifact determinism, desktop checks, rustdoc warnings, the
production dependency audit, and the qualification bundle build.
