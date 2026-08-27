# ADR 0029: Trajectory has two retrieval paths, and Fleetd stores neither

- Status: accepted for experimental dogfood
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
`fleetd.harness-acp@0.2.0`, declared beside `0.1.0` because identity is matched
by equality rather than by SemVer range; per this repository's rule, the method
is unstable until two independent integrations implement it, so OpenCode and
Codex both qualify before the contract is treated as settled.

**The method starts a replay rather than returning one.** It answers as soon as
the replay is under way, entries arrive as notifications, and exactly one
completion closes it. This is a constraint, not a style: a plugin drains its
notification channel only between requests, so a method that awaited the whole
replay would deadlock once a conversation outgrew the channel — an intermittent
hang on long sessions and nowhere else. A turn already has this shape.

**A replay is bounded and says when it was bounded.** Fleetd cannot limit what a
runtime chooses to replay, so the driver stops at 10,000 entries or 8 MiB and
reports `truncated`. Completeness is never inferred from a stream that stopped.
Two refusals beyond the three above fall out of the same reasoning: a second
concurrent replay on one session, and a transcript notification arriving while a
turn drains, which fails that turn rather than being tolerated.

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

**Per-invocation segmentation is exact, because Fleetd put the key in the
conversation.** One durable session binding serves a whole channel, so a session
accumulates many invocations while `session/load` replays all of them. The
envelope adapter's prompt names its own invocation, and a replay carries prompt
text verbatim, so every turn Fleetd dispatched opens with a user message
containing that invocation's id. A reader splits the replay on user messages,
reads the id out of each, and attributes every following entry to it.

This was measured rather than assumed, and the first draft of this decision got
it wrong twice. Attribution by time window is impossible: no replayed update
carries an original timestamp, and the `observed_at_ms` on an entry is when the
replay was read, so every entry in one replay shares roughly one instant. The
envelope also does not arrive as bare JSON — the adapter prepends an instruction
preamble — so a reader takes the text from its first `{` rather than parsing the
whole thing. Against a real OpenCode seat holding two invocations, both segments
resolved to invocations Fleetd had dispatched, and the multi-line envelope
replayed as one entry rather than several. See the
[real-harness qualification](../qualification/opencode-transcript-retrieval-2026-08-27.md).

Two consequences follow. A turn that Fleetd did not dispatch has a user message
that contains no envelope, which makes foreign activity identifiable rather than
silently mis-attributed. And this is a property of the adapter, not of ACP: an
adapter that wants attributable transcripts must name its invocation in the
prompt, which is now a stated requirement of the interface rather than a
fortunate accident.

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

## What was built

Three commits, each green through `bin/ci`:

- adoption sends `session/resume` where the runtime advertises it and falls back
  to `session/load` otherwise, so a replacement worker no longer streams and
  discards an entire conversation on every restart;
- `harness.acp.session.transcript.start` with its two notifications, the
  `0.2.0` declaration, and the four refusals;
- coverage through the real `fleetd-acp-reference` binary against a mock ACP
  runtime, which is the only place `acp-host` is exercised for real, plus the
  restart demonstration asserting that adoption used `session/resume`.

Nothing in the product calls the method yet. That is deliberate — see below.

## Deliberately not here

Segmentation at scale. Two invocations on one lane resolved exactly, so the
mechanism is tested rather than reasoned, but a session accumulating a night of
invocations is where the key could fail: compaction, pruning, or a rewritten
prompt would take the invocation id with them, and none of those is measured.

Grouping in the product. Fleetd knows the key and does not use it: `fleetd
transcript` returns a flat lane and leaves splitting to the reader. Presenting a
transcript per invocation is now a small change rather than an open question,
which makes it a decision about the command's output rather than about whether
attribution works.

Naming the prompt. ACP's `user_message_chunk` has no case in
`classify_update`, so the entries carrying the segmentation key are labelled
`unknown`, and every live OpenCode turn has been adding one to
`InvocationEventCounts.unknown` for a recognised kind. That erodes the signal
ADR 0020 kept the counter for. Fixing it needs an `EventClass` variant, a
counter field, and a forward migration.

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

The concurrency question behind it is now answered. A second `opencode acp`
process loaded a session a first one was still holding, including while that
holder was mid-turn, without disturbing it; the replay carried the conversation
through the last settled entry and nothing of the turn in flight, so a
concurrent read is stale rather than torn and needs no coordination with the
worker. An operator path can therefore be a short-lived second plugin process
rather than a worker control channel. See the
[concurrent session read qualification](../qualification/acp-concurrent-session-read-2026-08-27.md).
What remains unmeasured is writing from a second process, which a retrieval path
has no reason to do, and every harness other than OpenCode.
