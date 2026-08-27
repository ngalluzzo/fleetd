# ACP concurrent session read qualification — 2026-08-27

## Scope

[ADR 0029](../adr/0029-harness-transcript-retrieval.md) deferred the operator
transcript surface with one unknown behind it: the harness process is owned by a
running worker, so an operator path either goes through that worker or starts a
second plugin process against a session the first one already holds. This
measures whether a runtime tolerates the second option.

It qualifies OpenCode 1.4.0 only, and it qualifies concurrent *reading*. No
Fleetd code participated: the probe spoke ACP directly to two `opencode acp`
processes over stdio, so the measurement is of the harness rather than of
Fleetd's translation of it.

Model `zai-coding-plan/glm-5.3-flash`, selected per session through
`session/set_config_option` so the operator's own configuration was never
modified. Session `ses_fbb723bf5ffeG4Pwdmj560wEoq`.

## A holder keeps the session; a reader loads it anyway

The holder opened a session and completed one turn, then stayed alive with the
session open for the rest of the run.

| Phase | Action | Result |
| --- | --- | --- |
| A | reader `session/load` on the held session | replayed the completed turn |
| B | reader `session/load` while the holder was mid-turn | replayed, holder unaffected |
| C | holder runs a third turn after being read twice | `end_turn`, 35 thought chunks |

Phase A replayed one user message, one reasoning block, and one assistant
message: turn one, complete.

Phase B was verified to be genuinely concurrent — the holder's `session/prompt`
was confirmed still unanswered before the reader's load was sent — and the
holder's turn afterwards returned `end_turn` normally.

Phase C confirms the holder was not left degraded: a third turn ran with its
own reasoning and answer.

## The mid-turn read is stale rather than torn

Phase B's replay carried two `user_message_chunk` entries and only one
assistant message. The holder's second prompt had already been stored, but the
assistant output it was still generating had not, so the reader saw the
conversation up to the last completed entry and nothing of the turn in flight.

That is the useful property. A concurrent read cannot observe a half-written
entry, so it needs no coordination with the worker to be consistent — it is
simply behind. An operator surface can therefore read at any time and describe
what it returns as "complete through the last settled entry" without inspecting
whether a turn is running.

## What this unblocks, and what it does not

An operator transcript path can be a short-lived second plugin process launched
from the same worker configuration. It needs no worker control channel, no
daemon involvement, and no coordination with the running seat.

Not qualified:

- **Writing.** The reader only ever loaded. Whether a second process may prompt
  a session another holds is unmeasured and should be treated as forbidden: a
  retrieval path has no reason to.
- **A long-lived reader.** The reader loaded, observed, and exited within one
  run. Holding a foreign session open indefinitely is untested.
- **More than one reader**, and more than one harness. Codex is unqualified
  even at `fleetd.harness-acp@0.1.0`.
- **Fleetd's own path.** This proves the runtime tolerates it; it does not
  exercise `harness.acp.session.transcript.start` through a second plugin
  process.

Unrelated but worth recording: `opencode/big-pickle` timed out at 300 seconds
producing no output, both here and on two earlier attempts, which is why the
measurement used the Z.ai route. The OpenCode Zen provider was unusable
throughout this session.
