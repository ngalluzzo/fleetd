# ADR 0031: Work created without a human is an inbound trigger

- Status: accepted
- Date: 2026-08-27

## Context

Work enters Fleetd exactly one way: an authenticated principal appends an
immutable message. That principal is one of two things. An operator or agent
holding a bearer credential, or an agent inside an armed invocation holding the
narrow `fleet.messaging.send` grant, which
[ADR 0016](0016-invocation-scoped-message-grant.md) activates after the fence
commits and revokes before settlement.

Neither covers a thing that creates work on its own. A recurring job, a webhook
receiver, a file watcher, an issue poller: each of them appends work with no
human present and no invocation to scope to. Today they all live outside
entirely, as a program calling `fleetd message send` with a bearer token.

That should remain the answer for the simple case. A crontab line is a scheduler,
it survives reboots, and it costs Fleetd nothing. What it costs the *operator* is
the part worth weighing: a second thing to run, and a trigger whose identity,
history, and failure are invisible to the fleet. A cron entry that stopped firing
three days ago leaves no trace in the durable record — the fleet simply looks
idle, which is exactly what a healthy quiet night also looks like.

There is also an authority asymmetry. An external trigger holds a full bearer
credential, so it may append anything to any channel as any kind. The narrowest
thing in the system — an agent mid-turn — is far more constrained than a shell
script that runs unattended every fifteen minutes.

[ADR 0004](0004-out-of-process-plugins.md) already contemplates plugins for
"harnesses and external systems", and the lifecycle is generic. Two things are
missing rather than one. Nothing daemon-side hosts a plugin: only a worker seat
and `fleetd transcript` launch one. And harder, there is no authority category
for a long-lived process that creates work with neither a human nor an
invocation behind it.

Scheduling is the instance that prompted this. The shape is not specific to it.

## Decision

Name the category rather than the instance. An **inbound trigger** is a
supervised child process that may append messages of declared kinds to a
declared channel, and may do nothing else.

**Its authority is standing but narrow, in the shape ADR 0016 already
established.** Fleetd derives sender, channel, correlation, causation, and the
durable idempotency key from the trigger's registration. A trigger chooses only
recipient, message kind from its declared set, opaque payload, and an occurrence
identifier used for exact retries. It cannot name a sender, reach another
channel, or forge a kind it did not declare, for the same reason a harness
cannot: identity is constructed, never supplied.

**Declared kinds participate in trigger identity**, the way inbound acceptance
participates in a worker's session compatibility. Changing what a trigger may
create changes what it is.

**Idempotency belongs to Fleetd, not the trigger.** The characteristic failure
of an unattended scheduler is a double fire — an overlapping run, a machine
waking, a retry after a lost response. A trigger supplies an occurrence
identifier and Fleetd derives the durable key from the pair of trigger identity
and occurrence, so a repeat is absorbed exactly and two triggers cannot collide.
This is the one thing a trigger inside Fleetd does better than a crontab line,
which has to construct a distinct key itself and silently creates no work when
it gets that wrong.

**A trigger has no back channel.** It creates work. It does not read the fleet,
settle a delivery, observe an outcome, or learn whether what it created
succeeded. A trigger that can see results is a workflow engine, and workflow
belongs outside the daemon.

**Overlap is not Fleetd's to decide, and that exclusion is deliberate.** Every
comparable system grew one: Temporal schedules have an overlap policy, Kubernetes
CronJob has `concurrencyPolicy`, Nomad has `prohibit_overlap`. Fleetd does not,
because "skip if last night is still unsettled" requires a trigger to read fleet
state, which the previous rule forbids. This is a known omission rather than an
oversight, and the honest expectation is that it will be the first thing asked
for. Answering it means revisiting the no-back-channel rule, not adding a flag.

**Fleetd never interprets what makes a trigger fire.** A cron expression, a
webhook payload, a filesystem event, a queue message: all opaque. The trigger
decides *when*; Fleetd decides only *whether it may*. Fleetd carries no
scheduling vocabulary, no calendar, and no timezone.

**The daemon supervises it as it supervises a plugin generation**: durable
identity before it may create anything, health while it runs, restart with
bounded backoff, and a durable record of its last accepted occurrence. That
record is the reason to bring a trigger inside at all — a trigger that has
fired nothing since Tuesday becomes a fact an operator can read, rather than an
absence they have to notice.

## Consequences

The authority model gains a third category. It was operator-held or
invocation-scoped; it becomes operator-held, invocation-scoped, or
trigger-standing. That is the real cost of this decision and it should be
weighed as such: a standing grant is a durable capability, and every future
question about what a trigger may do will push against its edges.

Scheduling stops being a special case, and so do the three integrations that
would otherwise have arrived asking for their own hole in the wall.

The daemon acquires a second class of supervised process. Plugin generations
and triggers share supervision but not purpose, and the operator read models
that expose one will want to expose the other.

A recurring trigger is the first thing that makes a session lane grow without
bound. One durable session binding serves a channel, so work arriving nightly
forever accumulates in one native session that is never rotated. Claude Code's
desktop scheduling avoids this by starting a fresh session per firing; Fleetd
has no equivalent, and its transcript retrieval has only ever been measured
against sessions holding one or two invocations. Triggers make the untested case
routine rather than hypothetical.

An external trigger over a bearer token remains supported and remains the right
choice for anything simple. This is not a migration.

## Deliberately not here

The scheduler. Nothing in Fleetd will parse a cron expression; a scheduling
trigger is a contribution, and the first one will show whether the interface is
right.

Reacting to fleet state. "Fire only when the seat is idle" and "skip if last
night is still unsettled" require a trigger to read the fleet, which this
decision forbids. If that turns out to be necessary, it wants the live
operator-event subscription M4 already owes, and a separate decision about
whether a trigger may consume it.

What bounds a lane that a trigger feeds forever. Rotating the binding on a
cadence, giving each firing its own session, and leaving it to accumulate are all
defensible, and the choice interacts with session compatibility, transcript
retrieval, and whatever a harness does when a conversation grows past its own
limits. It should be decided with a real trigger running, not before.

Agent-initiated scheduling, which is the field's convention and not an
oversight here. Block's goose exposes a `manage_schedule` platform tool so an
agent maintains its own schedules, and Claude Code offers `cron_create`,
`cron_list`, and `cron_delete` the same way. Both are single-user agents acting
as their operator's proxy, where self-scheduling is the agent doing what that
person would have done. Fleetd is not that: an agent that can grant itself
recurring future work holds a standing capability nobody registered, which is
the third authority category this decision exists to guard.

The cost is real and worth stating plainly. "Check back on the deploy in an
hour" -- an agent arranging its own follow-up -- is natural in those systems and
impossible here. That is a large share of what people want from agent
scheduling, and refusing it is a position rather than a simplification. If it is
revisited, the shape to reach for is an invocation-scoped grant that creates one
future occurrence, not a standing one: ADR 0016 already shows how narrow
authority survives being handed to a harness.

Multi-channel triggers, trigger-to-trigger composition, and any notion of a
trigger that can be paused by another agent rather than by an operator.

Whether the first trigger interface should be versioned as
`fleetd.trigger@0.1.0` under ADR 0004's negotiation, or whether triggers want a
lifecycle of their own. The answer depends on whether a trigger's supervision
really is the same shape as a plugin generation's, which building one will
settle faster than arguing about it will.
