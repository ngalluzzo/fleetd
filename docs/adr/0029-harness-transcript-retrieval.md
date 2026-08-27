# ADR 0029: Trajectory has two retrieval paths, and Fleetd stores neither

- Status: proposed
- Date: 2026-08-27

## Context

[ADR 0020](0020-bounded-operational-observations.md) folds every harness update
into counters and a chain digest and keeps no content, on the stated grounds
that "the native harness transcript owns the raw trajectory." That sentence has
been doing more work than it earned.

Fleetd has no path to that transcript. The
`fleetd.harness-acp@0.1.0` interface has nine methods — describe, session open
and close, turn start, event, cancel and terminal, and the two permission
methods — and none of them reads a conversation. The plugins do not know where
their runtime stores sessions. An operator who wants to know how a task was
approached opens the harness by hand and correlates by eye.

Worse, Fleetd already receives a transcript and discards it. ACP requires an
agent advertising `loadSession` to "replay the entire conversation to the Client
in the form of `session/update` notifications" and to withhold the
`session/load` response until every entry has been streamed. Fleetd calls
`session/load` on every restart adoption purely to re-attach, so a full replay
arrives on every worker restart and is dropped at the one line in
`acp-host/src/runtime.rs` that ignores an update with no active turn.

That drop is correct. Attributing replayed updates to the *new* invocation
would corrupt the very evidence ADR 0020 exists to protect: `event_count`,
`last_event_seq`, and the chain digest would all describe a turn that did not
produce them. The defect is not the drop; it is that Fleetd is using the method
whose contract it does not want. ACP separates the two deliberately —
`session/resume` "MUST NOT replay the conversation history."

A probe against real OpenCode 1.4.0 measured what a replay actually carries. A
fresh process loaded a session created by an already-killed one and received
every entry the live turn produced: a tool call with its exact `rawInput`,
`rawOutput`, and output `content`, and — on a reasoning model — the agent's
thinking. Concatenated, the replayed reasoning was byte-identical to the live
stream, 471 characters against 471, and the assistant message likewise. Nothing
present live was absent.

What a replay loses is *when*, not *what*. One hundred and twenty-seven live
`agent_thought_chunk` notifications arrived as two coalesced blocks, thirty-three
message chunks as one message, and three tool notifications as one at
`completed`. A replay is each entry's final state with its content whole, not
the progression toward it. See the
[transcript replay qualification](../qualification/acp-transcript-replay-2026-08-27.md).

So there are two different artifacts, and neither contains the other:

- the **native transcript**, durable for as long as the harness keeps it,
  complete in content, and final-state in timing;
- the **live update stream**, which exists for the duration of one
  `drain_turn` loop, carries chunk-level progression and timing, and today
  reaches only the lossy sink of
  [ADR 0028](0028-opentelemetry-is-a-projection.md).

Reasoning retrieval is a property of the harness-and-model pair rather than of
ACP. A model that never emits a thought chunk leaves none to replay, so this
decision makes reasoning *retrievable where it exists* and cannot make it
exist.

## Decision

Fleetd gains a way to *retrieve* trajectory and remains a system that does not
*store* it.

**Split the two ACP methods by the purpose each was written for.** Restart
adoption uses `session/resume`, which does not replay. `session/load` is called
only when a caller has asked for a transcript. Adoption stops paying for a
replay it discards, and a replay stops arriving at a moment when nothing may
attribute it.

**Add one transcript method to the harness interface.** It streams the
load-time replay to its caller instead of dropping it, and it is gated on the
runtime advertising `loadSession`: a harness without that capability reports
that it cannot, rather than returning an empty transcript that reads like an
agent which did nothing. The interface becomes
`fleetd.harness-acp@0.2.0`; per this repository's rule, the method is unstable
until two independent integrations implement it, so OpenCode and Codex both
qualify before the contract is treated as settled.

**Fleetd transports and does not retain.** No new kernel table, no new column,
no migration. A transcript passes through to whoever asked and is not written
to SQLite, so ADR 0020's decision stands unchanged and its retention and
redaction questions stay closed. What changes is that the delegation now has an
address.

**Retrieval is operator authority, not agent authority.** A transcript is the
most sensitive artifact Fleetd can touch. No agent credential may read one, no
transcript may be appended to a channel as a message, and no transcript may
influence a settlement, a fence, or a park. A control-plane decision that reads
a transcript is a bug.

**Per-invocation segmentation is approximate, and says so.** One durable
session binding serves a whole channel, so a session accumulates many
invocations while `session/load` replays all of them. Fleetd already stores
`first_event_at_ms`, `last_event_at_ms`, and `event_count` per invocation
observation, which is enough to attribute a replayed span of entries to an
invocation by time window. That attribution is a projection, not a proof, and
must be labelled as such wherever it is presented. Making it exact would
require the harness to carry a turn boundary through the replay, which ACP does
not specify today.

**The two paths stay complementary, and the split is timing against content.**
The transcript answers what was reasoned, called, and concluded, completely, and
survives everything. The live sink answers how a turn unfolded moment to moment
and is allowed to lose that. Because content is complete on either path, the
sink is no longer the only route to reasoning, and its remaining unique value is
chunk-level timing and watching a turn while it runs. The bounded observation
row remains the only authority on ordering, counts, and certainty; it is what
detects a transcript or a trace that is missing something.

## Consequences

An operator can ask what an agent actually did, with exact tool arguments and
outputs, for any session the harness still holds — including work that ran
before any of this existed, since the evidence is the harness's, not Fleetd's.

Restart adoption gets cheaper and quieter. Today every adoption streams and
discards an entire conversation; afterwards it streams nothing.

The durable/lossy split becomes legible rather than accidental. Before this,
"we delegate the transcript" and "the egress sink is optional and lossy"
together meant that in a default deployment the interior of a turn was
unrecoverable by any means. Afterwards there is one path that always works and
one that is better when it is running.

Trajectory retention becomes the harness's retention. Fleetd's evidence rows
outlive any transcript, so a `fleetd trace` that resolves against an
invocation whose transcript has been pruned is expected, not broken. ACP's
`session/list` and `session/delete` — neither of which Fleetd uses — are how
that lifetime becomes inspectable, and that is its own decision.

An interface version bump obliges both harness plugins. Codex has no
real-runtime qualification even at `0.1.0`, so this raises the cost of the
already-open milestone rather than adding an independent one.

## Deliberately not here

Multi-turn segmentation, measured. Every probe session held exactly one turn, so
the time-window attribution above is reasoned rather than tested. A session that
accumulates a night of invocations is the case that will show whether an
approximate boundary is good enough to present to an operator.

Transcript lifetime. How long a harness keeps a session before pruning is
unmeasured, and `session/list` and `session/delete` — the ACP methods that would
make it inspectable — remain unused.

Durable retention of selected artifacts. ADR 0020 reserved room for it, and a
transcript path makes it more tempting rather than less. It stays a separate
decision because it reopens retention and redaction, which is exactly the cost
0020 chose to defer.

Presenting a transcript. This ADR adds an address, not an operator surface. Who
renders one, at what granularity, and with what redaction is downstream of the
segmentation limit above.
