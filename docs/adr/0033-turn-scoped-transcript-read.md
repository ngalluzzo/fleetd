# ADR 0033: Reading another seat's reasoning is a grant, not a permission

- Status: accepted
- Date: 2026-08-28
- Supersedes nothing; paired with [ADR 0034](0034-os-level-harness-sandboxing.md)

## Context

[ADR 0029](0029-harness-transcript-retrieval.md) made a harness's reasoning
retrievable: the transcript of a native session, read through a short-lived
second plugin process, so a running worker keeps its own session untouched.
Fleetd stores none of it. The reason to build that was never operator
curiosity. It was so a reviewer could read *how* a change was decided before
reading the change, which is the one thing this system offers that a diff-based
review does not.

The first author/reviewer loop on this repository proved that is currently
impossible, and the durable record says so precisely. The author's invocation
`cf79c705` produced 967 tool events and **zero** permission events: everything
it did was inside its own working directory. The reviewer's two attempts,
`da871d0c` and `5b9ec17b`, produced one and two permission events, every one of
them denied, and returned no review. Its transcript reads back as
`"The user rejected permission to use this specific tool call."` There is no
user. There is a controller that always says no.

That denial is not a bug and this decision does not remove it.
`controller.rs` answers every `PermissionRequested` with
`PermissionOutcome::Cancelled`, and `permission_policy` accepts exactly one
value — `controller` — checked independently in the worker, the driver, and the
plugin host. A managed turn has no human to ask, and a daemon that guesses at
consent on a human's behalf is worse than one that refuses. Refusing is the
same principle [ADR 0016](0016-invocation-scoped-message-grant.md) applied to
messaging: authority is constructed, never supplied.

What is missing is the other half. ADR 0016 refused the harness a bearer
credential *and* gave it a narrow grant to do the real thing. Permissions have
only the refusal. A seat that needs to do something outside its worktree has no
route at all, however narrow and however legitimate.

The obvious answer is a permission policy: let worker desired state declare
which tool calls are auto-approved. ACP sanctions it — *"Clients MAY
automatically allow or reject permission requests according to the user
settings"* — and offers `allow_once`, `allow_always`, `reject_once`, and
`reject_always` for exactly that. The spec says nothing at all about unattended
clients, so this is undefined territory rather than forbidden territory. It is
the extension point ACP left open, and this ADR does not take it. The reason is
not that allowlists are unsound in principle. It is what they need underneath
them.

**An allowlist over agent-authored strings leaks, and the field knows it.**
Claude Code's rules are string-prefix matches rather than parsed semantics:
`Bash(git push --force:*)` is bypassed by `git push -f`, and `Bash(git:*)` does
not match `/usr/bin/git status`. CVE-2026-24053 is a path-restriction bypass via
ZSH clobber syntax that wrote outside the working directory with no prompt at
all. The matched surface is chosen by the agent whose authority is in question,
and shell is not a language that pattern-matching bounds.

**What makes an allowlist usable anyway is an OS sandbox beneath it, and fleetd
has none.** Claude Code now runs two independent layers: permission rules
evaluated before a tool runs, and `sandbox-runtime` enforcing filesystem and
network limits through `sandbox-exec` and `bubblewrap`, deny by default. Codex
CLI is the same shape — Seatbelt, Landlock and seccomp, restricted tokens —
expressed as declared modes rather than per-call consent. The rules carry
intent; the sandbox is the boundary. Unattended cloud agents drop the prompts
entirely and lean on ephemeral isolation.

Fleetd has the refusal and nothing else. So an allow policy adopted today would
be *strictly weaker* than the denial it replaced: the same leaky matching, with
nothing to catch what leaks. **The sandbox is a prerequisite for the policy, not
an alternative to it**, and that ordering is the reason this ADR is a grant
rather than a policy. [ADR 0034](0034-os-level-harness-sandboxing.md) takes the
sandbox; a permission policy becomes arguable after it lands, and not before.

**Handing the reviewer a transcript in its request payload** is the other
tempting answer, and it is simply not available. An operator retrieves the
transcript out of band and puts it in the message; this appears to need no
daemon change, and it violates ADR 0029 outright. Messages are immutable and
permanent, so a transcript in a payload is retained reasoning, which is exactly
what fleetd refuses to hold. It also moves the choice of what to look at from
the reviewer to the dispatcher, which defeats the point.

## Decision

**The reviewer's need is not "run commands outside my worktree." It is "read
the stored reasoning behind one invocation." Provision that capability; do not
permit the command.**

Add `fleet.transcript.read` as a second turn grant over the machinery ADR 0016
already built and this repository already runs. Worker desired state declares
the grant name; the binary starts the endpoint, because whether a turn is
offered one is a deployment decision; the controller arms it for exactly one
invocation and revokes it before settlement; the harness reaches it as an MCP
tool it was given rather than a directory it can reach. Nothing new is invented
— `ManagedTurnGrant`, `TurnGrant`, `ResolvedMcpGrant`, and the loopback MCP
surface are the existing parts, and a second grant is the first evidence that
that seam generalises past the one grant it was built for.

**The controller's unconditional denial stays exactly as it is.** A granted
capability and a permitted command are different things, and this decision
deliberately widens only the first. A seat holding this grant still cannot run
`git`, read a file, or reach a network outside its own worktree. That the
reviewer's need turned out to be expressible as a capability is what makes this
tractable; a need that genuinely required arbitrary local execution would want a
different decision, and should be forced to argue for one.

**What may be read is derived from the invocation, never supplied by the
caller.** The grant is armed for one invocation; that invocation's message names
a channel; the readable set is the invocations that produced messages in that
channel. A reviewer reads the reasoning behind the work in the conversation it
was asked about, and nothing else. It cannot name a session, an agent, or an
arbitrary invocation, for the same reason a harness cannot name a sender. This
is also why the channel-per-issue habit matters operationally rather than
aesthetically: the channel is the boundary of what a reviewer may see.

**A read is addressed by invocation, and answers with that invocation's
segment.** Not a session reference, and not a whole session's replay. ADR 0029
established that per-invocation attribution is exact because the envelope names
its invocation in the prompt; the grant returns that segmentation rather than
handing back a conversation the caller must slice. A session holding a night of
unrelated invocations therefore cannot leak the other ones through a single
read.

**The read is bounded the way every other observation here is bounded.** A cap
on entries and encoded bytes per read, and a cap on reads per invocation, in the
shape `MAX_MESSAGES_PER_INVOCATION` already takes. Reasoning is the largest
thing in the system and an unbounded read is an unbounded prompt.

**Fleetd still retains nothing.** The grant is a pass-through: the plugin
replays, the tool answers, and no transcript content is written to the durable
record. What is recorded is that a read happened — reader invocation, subject
invocation, entry and byte counts — folded into the reader's own observations
like any other event. ADR 0029's rule is unchanged and this is the second place
it has to hold.

**Which plugin serves the read is operator-declared, not daemon-inferred.**
This is the load-bearing awkwardness of the decision and it should not be
hidden. Retrieval needs the subject session's harness launch profile, and
[ADR 0004](0004-out-of-process-plugins.md) deliberately keeps launch profiles
out of the daemon: fleetd stores a `profile_digest` and an executable digest,
which identify a profile but cannot start one. So a reviewer's desired state
names the peer worker configurations it may read through, explicitly, the same
way it already names its own plugin. An undeclared peer is not readable, and the
daemon never learns how to launch something an operator did not hand it.

## Consequences

The authority model gains a *read*, and that is a genuine first. Every grant
before this one creates or settles; a trigger explicitly has no back channel,
and an agent mid-turn observes nothing it was not sent. Whether "a seat may read
another seat's reasoning" is the same kind of thing as "a trigger may not read
fleet state" deserves to be uncomfortable. The distinction this decision rests
on: a reviewer reads *reasoning already produced about the work it was assigned*,
not *current fleet state it could steer itself by*. If that line moves, this is
the ADR that moved it.

A transcript read is not cheap. It spawns a plugin process, resumes a session to
attach, and loads it to replay — mid-turn, on demand, possibly more than once.
The concurrent-read qualification already showed a holder is undisturbed and a
concurrent read is stale rather than torn, so correctness is settled; cost is
not. A reviewer that reads six transcripts has started six harness processes,
and the per-invocation read cap is the only thing standing between that and a
seat that spends its turn on process churn.

`permission_event_count` becomes a first-class diagnostic rather than a
curiosity. A turn with a non-zero count asked for something and was refused,
and that is now the signal that a seat wants a capability it was not granted —
readable from `fleetd status` without opening a transcript. Both failed reviews
above are visible that way in hindsight, which is the check that would have
caught this design error before it was written into seat instructions.

The reviewer instructions in `docs/seats/reviewer.md` describe reading a
transcript through a shell command. They were written before this was known to
be impossible and are wrong today; they become right when this ships, against
the tool rather than the command.

## Deliberately not here

A permission policy, for now and on stated grounds rather than on principle.
ACP sanctions one and the field runs one; what the field also runs is an OS
sandbox underneath it, which ADR 0034 owes and fleetd does not have. Revisit
this once that lands. `allow_always` should stay refused even then: it is a
standing grant nobody registered, which is the third authority category
[ADR 0031](0031-inbound-triggers.md) exists to guard, and `allow_once` under a
sandbox is the same capability without the durability.

A seat serving reads of its own session. It is the more elegant answer to the
launch-profile problem, since the owner already holds the profile: the reviewer
asks the fleet, and the author's worker answers. It is also a back channel
between seats, which is a much larger decision than this one, and it would
arrive entangled with M4's live operator-event subscription. Operator-declared
peer configurations are the smaller thing that works now.

Reading a transcript outside the channel. Cross-channel review, fleet-wide
reasoning search, and "show me every invocation that touched this file" are all
plausible and all widen the readable set past the request that armed the grant.
The channel boundary is the only thing making this narrow.

Retaining any of it. Caching a retrieved transcript, storing a summary of one,
or attaching one to a message are the same violation of ADR 0029 wearing three
hats. If reasoning should be durable, that is a reversal of 0029 and needs to be
argued as one.

Whether a reviewer should read reasoning *while* it is being produced. The
live case was raised and refused once already, for the right reason: monitoring
an agent mid-thought is a workflow, and workflow belongs outside the daemon.
This grant reads what is finished.

Bounding what a seat may infer from what it reads. A reviewer that reads
transcripts accumulates a picture of how another agent works, which is
information the durable record does not otherwise expose in one place. Nothing
here limits that, and it is not obvious that anything should — but it is a new
concentration and worth naming before it surprises someone.
