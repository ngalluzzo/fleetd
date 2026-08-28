# ADR 0035: The trajectory record is an export obligation, not an archive

- Status: accepted
- Date: 2026-08-28

## Context

One real invocation, measured: 15,209 reasoning events, 967 tool events,
10,323,355 observed payload bytes, folded into fixed counters and a chain
digest. The content existed for the duration of one `drain_turn` loop and then
existed nowhere fleetd could reach. That is
[ADR 0020](0020-bounded-operational-observations.md) doing exactly what it
decided, and it leaves the mission half served: fleetd can prove what happened
and cannot answer what was thought, last month, by the seat that did it.

Two paths answer content today, and neither is fleetd's promise.

`fleetd transcript` ([ADR 0029](0029-harness-transcript-retrieval.md))
retrieves from the harness for as long as the harness keeps the session. The
binding records `session_persistence: "runtime_claimed"` — fleetd writing down
that the runtime *said* it persists, never verifying — and sessions are pruned
and compacted under policies fleetd does not see. Whether last month's
reasoning is retrievable is a fact about a store fleetd does not own,
discovered at read time.

The OTLP trajectory sink ([ADR 0028](0028-opentelemetry-is-a-projection.md)) is
lossy by contract, absent by default, and truncates at its `full` level. It was
built lossy because the question it answered was *live observability* — how an
operator watches a running turn — and that question permits sampling and
dropping. Whether a lossless record of the reasoning exists afterwards is a
different question, and nothing answers it. The listings fleetd *does* publish
losslessly — keyset cursors, `EvidenceOrder::Oldest`, walkable by an external
collector from its last position — carry the counters.

So fleetd has a lossless channel for the shape of thinking and a lossy channel
for its content. For a mission that includes understanding how the work evolves
over time, that is backwards. Three shapes were on the table: detect and warn
when reasoning becomes unretrievable; owe a lossless export of it; retain it in
the authoritative store under operator opt-in. The maintainers leaned toward
the second, on the grounds that fleetd already owns an at-least-once delivery
machine and 0028's lossiness was a framing choice rather than a necessity.

## Decision

**Fleetd owes the trajectory as an export and stores no archive. When an
operator has put a seat under a trajectory export obligation, every harness
event the worker observes is durably owed to a collector outside fleetd:
verbatim, whole, at-least-once, on the cursor discipline the evidence listings
already publish. A collector outside fleetd keeps the record. The bounded
observation row stays the only authority on what happened; the archive is what
can be audited against it.**

The obligation's terms, before the hard questions:

- **Opt-in per seat, absent by default.** The default deployment is unchanged:
  no obligation, no buffer, and 0020 and 0029 stand exactly as written. Like
  egress, the obligation lives in worker desired state and changes nothing the
  harness sees, so enabling it must not rotate a session binding.
- **Incurred at the fold.** The outbox write commits in the same immediate
  transaction as the counters and the chain digest — the same enlistment
  pattern settlement uses — so nothing is counted that is not owed, and a crash
  between the two states cannot exist.
- **Verbatim and whole.** The owed stream is the live update stream the drain
  loop already observes, chunk-level, keying each row by
  `(invocation_id, event_seq)` so redelivery is idempotent by identity. It is
  strictly richer than a transcript: 0029 measured a replay as the final state
  of each entry, so the live stream carries everything a transcript holds plus
  the progression toward it — and unlike a transcript it does not depend on
  session lifetime at all.
- **Drained on the listing discipline, acknowledged durably.** A collector
  walks `Oldest` from its last position over an operator-only export surface,
  processes, then acknowledges; the acknowledgement advances a durable
  watermark and reclaims space. Leases exist in the inbox because many agents
  compete for work; one collector drains one record, so the discipline is
  cursor plus watermark, and escalation is reserved for the bound (question 1).
- **Not a kernel concept, not a message.** The outbox joins the family 0020
  named — controller-owned operational records outside the messaging kernel,
  beside `invocation_observations` — and nothing in it ever becomes a channel
  message, per 0029's rule and M4's rule against synthetic activity. Reading it
  is operator authority; no agent credential may reach it, and a
  control-plane decision that reads trajectory content is still a bug. Plugins
  are not involved: the worker already receives every event, and the
  `fleetd.harness-acp` interfaces do not change.

**Why not retention, shape 3, said as a cost rather than a slur.** Retaining in
the authoritative store is the one-system answer, and its costs are the ones
0020 already priced: 10 MB per invocation into the file the fence and the
delivery machine commit against on every settlement; an append-heavy blob
inside a store whose job is small, fast, recoverable control state; offline
backup and restore swelling from megabytes to gigabytes; and a redaction and
retention policy fleetd would have to own before shipping rather than defer,
because verbatim content cannot be redacted in flight (question 4) and the
kernel has no deletion story — membership is permanent and messages are
immutable by design. The mission needs a lossless record to *exist*; it does
not need fleetd to *be* the archive. Retention systems — query, compaction,
columnar backends, deletion — are a competence fleetd has twice declined and
would be rebuilding beside the one it has.

**Why not verify-and-warn, shape 1 — but its instrument survives.** Shape 1's
instrument, the durable loss fact, is exactly right; its object is wrong.
Watching the harness's session store means auditing a promise fleetd never
made (`runtime_claimed` already records that nobody verified it), through
mechanisms fleetd does not have (`session/list`, polling, harness-by-harness
differences), and it reports the loss after the content is already
unrecoverable. Detection pointed at the one boundary fleetd actually controls —
its own obligation — is prevention, and that is where this decision puts it:
the loss facts of shape 1 become the expiry records of question 1. What shape 1
would have made durable *about the harness* stays 0029's open lifetime question.

**The maintainers' lean is accepted with one edge corrected.** As posed, shape
2's delivery machine answers everything except the collector being absent: grow
without bound, or drop. Unbounded growth is not a third answer to that case; it
is the fleet-stopping answer wearing a costume — the disk fills, the fold
fails, the fleet stops, and the outage is discovered as ENOSPC with the control
store taken down alongside the backlog. Dropping is the thing this ADR exists
to stop. The correction is that the obligation, not the buffer, is what fleetd
owes — which settles the five questions below.

### 1. Nobody collecting: grow to a declared bound, then expire loudly

The outbox grows while nobody drains, its growth is visible in the operator
surfaces beside the delivery census, and the operator declares a byte bound.
The bound is enforced before the disk bound, so the control plane never
starves: passing it expires the oldest undrained content as a durable, named
transition — a loss record carrying the invocation, the sequence range, and
the byte count — escalated in the operator surfaces the way a blocked delivery
is, because it is the same category of fact: an obligation fleetd cannot
discharge that a person must see. The fleet keeps running turns. An operator
may declare an unbounded buffer, and takes responsibility for the ENOSPC
version; the escalation fires as the budget approaches either way.

What this costs, plainly: loss is possible. No finite machine owes absolute
losslessness, and this decision does not pretend otherwise. What fleetd owes is
narrower and checkable: **no silent loss, no unaccounted loss, and no loss the
operator did not bound.** Every event is counted at the fold; every counted
event is owed, held, acked, or expired; and the expiry is a first-class durable
fact rather than a counter on a sink. That is the difference from 0028, where
dropping is silent by design — here, loss without a durable record is a bug,
not a behavior. A second cost is write amplification on the hot path: under
obligation, each event commits a fold and an insert in one transaction, and
that price is part of why the obligation is opt-in.

### 2. An outbox amends 0029; it does not squeak past it

0029's sentence is "Fleetd transports and does not retain," and its stated
reason was that retention would reopen the questions 0020 chose to defer. A
durable buffer is retention. Retention-until-acknowledged is retention with a
deadline, which is still retention. Asserting the deadline makes it consistent
would be exactly the move this ADR was asked not to make.

What 0029 actually decided, read whole: retrieval passes through, nothing
durable is added, and the retention and redaction questions stay closed
*because no requirement then on the table justified reopening them*. An
operator who has declared an archival obligation is that requirement. This ADR
therefore amends 0029 rather than claiming compatibility: retrieval keeps its
rule unchanged — `fleetd transcript` still passes through and stores nothing —
while export under obligation gains a bounded exception, and the exception is
held to the narrowest form that answers the requirement: verbatim,
opt-in per seat, retention only until acknowledged or expired, owned by
execution rather than the kernel, readable only by the operator. The questions
0029 kept closed are reopened here on purpose, in this bounded shape, and
named as reopened.

### 3. A stolen `fleetd.db` reveals raw thought, transiently, while it is owed

Today the authoritative store holds correspondence and counters and no
reasoning content. Under an obligation with a backlog, it transiently holds
verbatim reasoning, tool inputs and outputs, and plans — unredacted by
construction (question 4), for exactly as long as they are undrained. The
file's sensitivity becomes time-varying: in a drained steady state it is what
it is today; during a collector outage it is everything undelivered. That
changes what the file is while an obligation runs, and this decision says so
rather than leaving it to be discovered.

The containment is structural, not cryptographic: default-off keeps the default
file exactly what 0020 and 0029 left it; the obligation's scope is precisely
the seats that declared it; loopback listeners and owner-only files bound
*reach*, not content at rest, and 0034 bounds the harness rather than a thief
who holds the disk — none of that changes. Whether the buffer can live in a
place with different at-rest protection is a storage-format decision this ADR
deliberately refuses to make, and the obligation is stated so that no
implementation can use format to soften the fact: whoever holds fleetd's
trajectory buffer holds the reasoning, for as long as the buffer does.

### 4. Verbatim, because a cap is loss and a truncation is forgery

`raw_update` is harness-authored JSON of unbounded size. Any byte cap makes the
stream lossy again, and truncation is worse than loss: a tool output cut at N
bytes is a record that lies about what the tool said, and a forged trajectory
is harder to detect than a missing one. So the obligation is whole-event: an
event is exported entire or expired entire, and admission pressure applies
oldest-first under the bound of question 1. An event that cannot fit under the
declared budget expires unexported and is recorded; it is never trimmed into
something that fits.

What bounds anything at all, then, is three things that are not the content:
the operator's budget and its expiry; the whole-event admission rule, which
keeps every decision binary and auditable; and the account — the counters and
chain digest, which bound the truth independently of the content. That third
bound is what makes the archive trustworthy without making fleetd the archive:
a collector that holds a subset can always be checked against the row that
proves how many events occurred and in what order. The existing `full` level's
`max_attribute_bytes` truncation is unaffected — it belongs to the lossy sink,
which never promised losslessness, and the obligation must not share that path.

### 5. Two sinks, as a decision: live projection, archival record

The OTLP sink stays exactly as 0028 built it: lossy, off by default, never
delaying a settlement or failing a turn, feeding the tracing ecosystem —
`gen_ai` conventions, live dashboards, collector-side sampling. The export
obligation is the record. The two are deliberately different guarantees rather
than one sink with configuration, because a lossless sink wearing OTLP clothes
would invite the reading that they are one system with a flag between them.
Neither reads the other, no control-plane decision reads either, and the
durable row audits both: `event_count` and the chain digest state what existed,
so a trace or an archive that is missing something is visibly incomplete.

One consequence is worth stating as part of the decision: once the record
exists, the sink's `full` content level loses most of its point — metadata
serves the live view when the record lands elsewhere — but the level stays for
operators who want content in a tracing UI, and its security posture (named,
never defaulted) is unchanged.

## Consequences

"What was it thinking, last month" becomes answerable by whoever drained, and
the answer is checkable: the archive is auditable against the bounded row, so
gaps are detected rather than trusted away. Fleetd remains not-the-archive:
0020's row is still the only authority, and retention, redaction, and query
become the collector operator's concerns — moved off fleetd's critical path,
not abolished but placed.

The authoritative store gains a transient growth mode under obligation. An
operator who declares one is taking on capacity planning: the declared bound
becomes a fact about the fleet, the undrained backlog is visible beside the
delivery census, and expiry records accumulate as the durable history of loss.
Escalation joins the operator surfaces: an unpaid obligation should be as hard
to miss as a blocked delivery.

The collector becomes sensitive infrastructure holding the most sensitive
artifact fleetd touches, reached only with operator authority. Its
acknowledgement is content-addressed by position and monotonic — safe to retry
after a lost response, unable to move a watermark backward — so the
at-least-once rule that governs every other delivery here governs this one.

The obligation is not retroactive. Invocations that ran before a seat declared
one keep today's answer — the transcript, for as long as the harness holds the
session — and the `runtime_claimed` honesty problem is routed around rather
than solved: fleetd stops needing the harness's session store to persist,
because the live stream is owed at the moment it is observed. Fleetd still does
not verify harness retention, and 0029's open questions about session lifetime
and segmentation at scale are unchanged.

Implementation, migration, contract, and the export surface's exact routes are
not here. This ADR settles what fleetd owes; building it is separate work that
will have to answer to every constraint above, including the three-way audit
— counted, owed, held — staying reconstructible at every instant.

## Deliberately not here

A storage format, file layout, collector implementation, or contract change.
The non-goal is the point: coupling the obligation to a format is the mistake,
and where content lands is a separate decision this one deliberately does not
make.

Retention and redaction policy for the archive. Collector-side, the operator's,
and no simpler for being pushed outward; this ADR only refuses to import the
problem into the control store.

At-rest encryption of the buffer. A format question, deferred with the rest,
and deferred in a way that cannot weaken question 3's fact.

Making the OTLP sink lossless, or giving it acknowledgements. Two guarantees
over one protocol is how a projection quietly becomes a system of record; the
sinks stay separate on purpose.

Verifying harness session persistence, adopting `session/list`, or supervising
session lifetime. Shape 1's object, rejected above; the obligation makes it
unnecessary for the live stream, and 0029's lifetime questions remain 0029's.

Per-tool child spans derived from the durable record, and any change to
`fleetd transcript`. The first stays 0028's rejected leftover — the counters
cannot produce that tree; the second is untouched by request.
