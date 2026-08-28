# ADR 0033: Reading another seat's reasoning is a grant, not a permission

- Status: withdrawn
- Date: 2026-08-28
- Withdrawn: 2026-08-28, before implementation

## Why this is kept

The decision is withdrawn because its premise was false. The record stays
because the way it came to be written is worth not repeating.

## What it claimed

That a reviewer seat cannot read the reasoning behind the work it was asked to
review, because every ACP permission request is answered
`PermissionOutcome::Cancelled` and `permission_policy` accepts only
`controller`. It proposed `fleet.transcript.read` as a second turn grant over
[ADR 0016](0016-invocation-scoped-message-grant.md)'s machinery, armed for one
invocation, scoped to the invocations behind the channel's messages.

## Why it was wrong

The reviewer could read the transcript the whole time. It needed to be standing
in a directory where the fleet was reachable.

The evidence was already in the durable record when the ADR was written. An
author invocation had run 967 tool events with **zero** permission events —
every one of them inside its own working directory. The reviewer, placed in an
isolated worktree that did not contain `.fleetd` or the `fleetd` binary, asked
three times across two attempts and was refused three times. Moving its working
directory to the repository root and changing nothing else, it read the author's
session — binding, turns, reasoning — on its nineteenth tool call, then verified
the ADR under review against the source rather than trusting it.

The permission refusal is real and correctly describes the code. It was not what
stopped the reviewer. Placement was.

## The mistake worth naming

The requirement this ADR existed to satisfy was never asked for. "The reviewer
reads the author's reasoning before the diff" was introduced as an argument for
*why dogfooding was worth doing*, then written into `docs/seats/reviewer.md` as
an instruction, then treated as a constraint the daemon had to satisfy. Three
steps, no request. The story was persuasive, which is exactly why it stopped
being examined.

A configuration mistake was then diagnosed as a missing capability, and a
product decision was drafted to fix a deployment error. The disproof was one
row of a table that had already been read aloud.

## What survives

The permission analysis stands and moved to
[ADR 0034](0034-os-level-harness-sandboxing.md), which never depended on this
one: fleetd bounds a harness by asking it politely, and a harness that does not
ask is unbounded. That is true whether or not any reviewer exists.

The capability-gating defect found while checking this ADR's reasoning was real
and is fixed: `additional_directories` was sent without reading
`sessionCapabilities.additionalDirectories`, so a declared workspace root was
silently dropped by any harness that did not support it.

If a seat ever genuinely cannot reach the fleet — a remote worker, an isolated
runner — the argument here becomes live again. Fleetd is loopback-only with no
remote workers, so that seat does not exist yet. Reopen this number then, with a
seat that actually needs it rather than one that was placed badly.
