# ADR 0032: The first scheduler is a stateless interval trigger

- Status: accepted
- Date: 2026-08-27

## Context

ADR 0031 named the inbound trigger and refused to name its first instance:
"a scheduling trigger is a contribution, and the first one will show whether
the interface is right." This is that first one, as design only. It adds a
decision about the shape of the contributed process and nothing to the daemon.

The interface it must live on is already fixed, so it is worth restating
exactly. A trigger is a durable registration — channel, sender, accepted
kinds — plus a credential that reaches exactly one operation: reporting an
occurrence. When it fires, the trigger chooses a recipient, a kind from its
declared set, an opaque payload, and an occurrence identifier. Fleetd derives
everything else, including the durable idempotency key, from the pair of
trigger identity and occurrence identifier, so an exact repeat is absorbed and
answers `created: false`. A trigger cannot read anything. Not the fleet, not
an inbox, not an outcome: the credential's single operation is a write, and
the ignorance is the design, not an implementation gap.

The use case that prompted this is "every 15 minutes." The failure mode that
must not happen is a scheduler whose double-fire protection silently does
nothing, because every firing was keyed by when it happened to happen.

Every decision below is the same move. The scheduler keeps no state: its
config file is its only input, fleetd's durable key is its only memory, and
each hard case is answered by that shape rather than negotiated with it.

## Decision

**The scheduler is a stateless process with one file and one credential.**
Its whole loop is: read the schedule file, compute the newest slot whose
scheduled instant has passed, fire that slot with an occurrence identifier
derived from the slot, sleep until the next slot under the config it just
read. No journal, no timers worth the name, no memory across restarts. The
five decisions that follow are what that loop commits to.

**1. The occurrence identifier names the scheduled instant, never the
firing.** A slot is `anchor + n × interval`: integer arithmetic on the UTC
timeline, no calendar anywhere. The occurrence identifier is
`<schedule>/<slot>` — a short digest of the schedule configuration, then the
slot's scheduled instant rendered in UTC at a fixed resolution, comfortably
inside the interface's 128-byte bound. Two derivations have to stay straight:
fleetd derives the durable key from the trigger and the occurrence
identifier; the scheduler derives the occurrence identifier from the
schedule. This design is a claim about the second one only — it is a pure
function of the config file and the slot, computable before the firing
without a single clock reading taken at fire time.

The wrong answer is defensible, which is why it gets named. Keying by the
wall clock at fire time, or by a counter incremented per tick, produces a
fresh identifier for every firing; it works every day and in every test. It
also means the double fire arrives with a different clock reading, mints a
different occurrence, and is answered `created: true` twice — the protection
0031 built exists and does nothing, and nothing can fail until the night a
response is lost or a second copy is running. The bug lives only in the
correlation between two firings, so no single-firing test can see it.

A reader can tell the rule was followed three ways. Recompute: from the
config file and a calendar alone, derive the identifier of any slot — if a
fire-time reading, a process counter, or a random component is required, the
rule is broken. Read the lateness: the identifier embeds the scheduled
instant and the registration records when the firing was accepted, so a late
firing shows its lateness as the distance between the two, with no
scheduler-side log at all — an identifier derived at fire time is on time by
construction and can never show this. Witness the absorption: re-fire a slot
deliberately — a restart, a duplicate process — and read `created: false`.
The same slot answering `created: true` twice is the failure observed rather
than prevented.

One schedule fires through exactly one registration. The durable key starts
from trigger identity, so the same slot fired through two registrations is
two creations, not a duplicate: an operator running two copies has not built
redundancy, they have built double work, and the config naming its trigger
is what keeps that pairing explicit.

**2. Fixed intervals, not cron.** "Every 15 minutes" needs two numbers and
addition. It needs no calendar, no timezone, and no daylight-saving rule,
which is the same nothing fleetd itself carries — the first contribution
should not import a calendar before the interface has been shown right even
once, and slot derivation staying additive is what makes decision 1's pure
function possible at all. Under cron, "the next occurrence" is a question
with a tzdata-dependent answer, and the occurrence identifier inherits that
fragility.

What cron would bring, said fairly: calendar alignment. "First of the month"
and "weekdays at nine" are not expressible with an interval, and that is the
real cost of this choice. What it would also bring: a parser turning a config
file into an input path, timezone selection semantics, and DST edges that
make the hard cases harder — 02:30 does not exist in spring and occurs twice
in fall, and "catch up the missed 02:00" becomes ill-posed across a
transition in a way integer arithmetic never is. An explicit anchor already
gives "every 15 minutes" a clock-aligned phase when the operator wants one;
alignment is a choice of anchor, not a calendar. Operators who need calendars
keep what 0031 already blesses — a crontab line and a credential — and a
calendar scheduler remains available as a second contribution that inherits
decisions 1, 3, 4, and 5 here and replaces only the arithmetic. Fleetd still
parses nothing either way.

**3. Downtime catches up to one firing, not a ledger.** The machine was off
six hours, the interval is fifteen minutes: fire the newest missed slot once.
The twenty-three older slots never fire.

The reason is what a firing is: an agent turn, spending real time and tokens
on an instruction. A firing's value decays, and the 03:15 instruction
executed at 09:00 is stale in a way the scheduler cannot evaluate — it cannot
see whether the 03:15 *work* mattered, only that it was scheduled, which is
decision 4's ignorance arriving one decision early. The scheduler also cannot
know whether the last pre-downtime work is still unsettled, so a burst of
twenty-four firings is a flood it cannot manage and the seat must drain
serially. And the accounting model is already right: fleetd owes the *trigger*
exactly-once creation, and nobody owes the calendar exactly-once execution. A
schedule of agent work is a heartbeat, not a ledger.

What this means for the job that was supposed to run at 02:00: if the machine
returns inside that slot's window, 02:00 fires, late, and the lateness is
readable in the record by decision 1's second check. If later slots have also
passed, 02:00 is skipped — never fired, and visible as a gap in the firing
record rather than hidden by a backfill. The newest firing's payload, opaque
to fleetd and the trigger's own to shape, carries the scheduled instant and
how many slots were coalesced, so the worker receiving stale instructions can
see how stale they are without fleetd learning what a slot is.

Catch-up therefore needs no journal, which is the point. On wake, compute the
newest due slot and fire it with its pre-derived identifier; if it already
fired before the crash, the durable key absorbs the repeat. Catch-up is
decision 1 applied to restarts — the scheduler needs no memory because
fleetd's key is the memory.

The cost, plainly: a job whose missed occurrences must all run — backups,
reconciliation sweeps — is a ledger schedule and is not served here.
Coalescing assumes decay. That job stays on the crontab, or arrives as the
separate contribution named under Deliberately not here.

**4. Overlap: the scheduler cannot know, and does not guess.** ADR 0031
refused fleetd an overlap policy because "skip if last night is still
unsettled" requires reading fleet state and a trigger has no back channel.
The scheduler inherits the limitation exactly, and the credential enforces it
mechanically: its one operation is a write, so a scheduler could not read the
fleet to decide overlap even by accident. The ignorance is held in place by
the authority model, not by good intentions.

The wrong answers are quiet and pass CI. A local rule — "don't fire again
until the last fire's response arrived," "skip if I fired within one
interval" — is an overlap policy made of heuristics, and it skips firings
with no durable trace. That is precisely the cron blindness 0031 exists to
end: a scheduler that quietly stops creating work is indistinguishable from a
broken one. What is allowed is transport hygiene, and it is required: one
fire in flight at a time, retrying the same occurrence identifier after a
lost response, and the `created` flag in that response — the one thing the
scheduler learns, which is a fact about its own request, not a reading of
the fleet. It bounds sends; it bounds nothing else, and a slow turn still
overlaps the next slot.

Where overlap lands is where 0031 put it: work is created on schedule and
queues behind the seat, which drains serially. A queue that grows is readable
in fleet health, and the honest responses are the operator's — widen the
interval, split the recipient, add a seat — not a skip the record cannot
show. If schedules that must not overlap become necessary, the route is the
one 0031 already named: revisit the no-back-channel rule, presumably over
M4's live operator-event subscription, as a decision about what triggers may
read. Never a heuristic bolted into one scheduler.

**5. The schedule is one operator-owned local file, re-read every cycle.**
It sits beside the fleet's other operational files and carries the schedule's
name, the interval, an explicit UTC anchor, the trigger credential's file
path, the recipient, the declared kind, and the payload. The registration is
not duplicated here; it stays where fleetd holds it. Because the loop re-reads
the file at the top of every cycle, a change takes effect at the next slot
computation — no signals, no restart, and no reload protocol to get wrong,
which a process with no runtime state could not justify anyway.

Two rules make an edit safe, and the second is load-bearing. Changes apply
forward only: slots already fired are durable facts under the config that
fired them, and nothing re-fires or un-fires history. And the occurrence
identifier embeds a digest of the schedule, per decision 1. Hold the anchor,
change the interval from 15 to 30 minutes, and a new-interval slot lands on
the same instant as an old-interval slot: without the digest the identifiers
collide and the old firing silently absorbs the new one — `created: false`,
no work, no error — which is the silent-nothing failure mode again. The
mirror matters too, because fleetd's idempotency is content-bound: the same
key with a changed recipient or payload is a conflict. The digest moves an
edited schedule into a fresh key space, so its next firing is guaranteed new
work rather than a collision with its own past.

"Pause" is deliberately not a config edit. A schedule that should stop is
retired through its registration, with the reason the retirement requires,
because an active registration gone deliberately silent is indistinguishable
from a broken one, and the firing record exists to keep that difference
readable.

## Consequences

The contribution's entire durable state is one config file plus fleetd's own
rows. A crash anywhere reduces to running the loop again; two copies running
reduce to one slot fired twice, one creation, one absorption. Every failure
the scheduler has is the same failure, and the same property answers it —
which is the first real evidence that 0031's interface is right: one write,
no reads, and a derived key are enough to schedule, exactly because the
design refuses everything a scheduler is tempted to want.

Supervision, still open in M3, gets cheaper without being designed here:
restart with bounded backoff recovers a process that has nothing to recover.
Whether a trigger's lifecycle really is a plugin generation's shape remains
the open question it was.

The unbounded session lane 0031 warned about arrives on schedule: a
fifteen-minute trigger feeds one binding forever, and transcript retrieval
has still only been measured against sessions holding a couple of
invocations. Unanswered here, because 0031 already owns it and said it should
be decided with a real trigger running — which this design now permits.

The costs of the whole decision, together: no calendar alignment; skipped
slots are gaps rather than rows, countable only through the payload the
newest firing carried; overlap queues work instead of skipping it; and
punctuality is best-effort — a machine that sleeps misses boundaries, and a
clock that steps backward can fire a coalesced-away slot late, though never
twice, because the key holds.

## Deliberately not here

The implementation, and daemon-side supervision with it. This ADR designs the
contributed process; M3's question of whether a trigger's lifecycle is a
plugin generation's shape is a separate decision it must not preempt.

Calendar schedules. A second contribution may add one; it inherits decisions
1, 3, 4, and 5 and replaces only the slot arithmetic. Fleetd parses no
expression and ships no calendar, in this design or any other.

Ledger catch-up — run-every-missed-slot, or a bounded burst of the k newest.
Defensible for backup-shaped work, but it wants its own burst policy, and its
cost lands on the seat that must drain it. It is not a mode of this
scheduler; a mode would be a second policy to hold against the same
interface, and nothing here is stable enough yet to carry two.

Any overlap policy. The refusal is inherited from 0031, and so is the route
to revisiting it: a decision about what triggers may read, over the live
operator-event subscription M4 still owes — never a local guess inside one
scheduler.

Bounding the session lane a recurring trigger feeds forever. 0031's open
question, unchanged, and made routine the day this design runs.

Soft pause. Retirement with a recorded reason is the mechanism that keeps an
intentional silence distinguishable from a failure; a pause would spend that
difference to save an operator one command.
