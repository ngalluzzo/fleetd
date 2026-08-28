# ADR 0036: The trajectory archive is JSONL behind a first-party collector

- Status: proposed
- Date: 2026-08-28

## Context

[ADR 0035](0035-trajectory-export-obligation.md) owes the trajectory as a
lossless export and deliberately stored no archive: no format, no file layout,
no collector, retention pushed to "collector-side, the operator's." That leaves
a hole exactly the size of a collector. The obligation is only worth having if
something drains it, and today nothing does — the export surface itself is
unbuilt, and the chain digest that
[ADR 0020](0020-bounded-operational-observations.md) built so a third party
could check a held subset against the row that proves what occurred has never
been used to check anything.

The measured shape, from 0035: one invocation of 15,209 events and 10,323,355
observed payload bytes — roughly 680 bytes per event — folded into counters and
a digest and then existing nowhere. The question the archive answers is 0035's
own ("what was it thinking, last month"): longitudinal in the aggregate, but
per invocation in the small, and checkable against `invocation_observations`
because that row carries `event_count`, per-class counts,
`observed_payload_bytes`, `last_event_seq`, and `event_chain_digest`.

The maintainers lean toward DuckLake behind a first-party reference collector,
for three named reasons: the catalog-in-SQL design puts the watermark and the
data in one transactional store; data inlining suits a stream of small appends;
and native snapshots are the mission's "how the work evolves over time" as a
feature. Against that: DuckLake reached v1.0 in April 2026 and is four months
old against an artifact meant to outlive every format decision in this
repository; DuckDB is a large native dependency that must then live somewhere;
and no collector has ever run, so no measurement has ever demanded columnar
scans over reasoning logs. This ADR takes the invitation to argue the lean
seriously, and declines it — on a named trigger rather than forever.

Constraints fixed by 0035 and the repository's rules, not open here: delivery is
at-least-once keyed `(invocation_id, event_seq)`; reading the surface is
operator authority; fleetd gains no storage engine, so whatever is chosen lives
beside the daemon; the bounded row stays the only authority on what happened.

## Decision

**A first-party reference collector ships in this repository beside the daemon,
as its own small crate and binary with no new dependencies. It drains the
operator-only export surface and writes one JSONL file per invocation into an
operator-chosen directory — fixed envelope columns plus the raw update verbatim
— compressing each file to gzip when the invocation closes provably complete.
Its watermark and per-invocation index live in its own SQLite manifest. An
invocation is proven complete by recomputing fleetd's chain digest from the
archived events and matching the observation row: an audit that needs no
engine, no fleetd code, and no library beyond SHA-256 and a JSON parser.
DuckLake is the named successor, adopted when a measured trigger is crossed,
not before.**

### 1. Whose collector: first-party, because the format below makes it cheap

This repository holds a precedent for each shape on offer. First-party in-tree
code when the thing is fleetd's own promise and small — the CLI, the soak
runner, the OTLP sink crate. A documented contract with an external reference
implementation when the thing integrates a foreign system with its own vendor
and release cadence — the harness plugins, the author-review draft, the pinned
HTTP-adapter integration. Nothing but a paragraph when an ecosystem already
exists — [0028](0028-opentelemetry-is-a-projection.md) shipped no collector
because pointing any OpenTelemetry collector at the public cursors is a solved
problem in someone else's ecosystem.

No ecosystem exists for a surface that does not exist yet, and nothing foreign
is being integrated: a JSONL collector is a few hundred lines over an HTTP
client, a file appender, and a digest loop. The genuinely subtle part is the
audit construction — digest inputs, canonical serialization, torn-tail repair,
gap classification — and audit code is exactly what this repository writes,
reviews, and qualifies. Under a zero-dependency format, first-party costs one
small crate and no supply chain, and it guarantees the obligation is drainable
out of the box rather than after an operator's weekend project.

The collector is not a plugin, by 0004's own boundary:
[0004](0004-out-of-process-plugins.md) plugins are daemon-launched integrations
starved of credentials by design, while the collector is an operator-side
client that must hold operator authority and must never see an agent
credential. It is launched and supervised by the operator — cron, launchd, a
terminal — and fleetd does not watch it, because 0035's question 1 already
covers the missing-collector case: an undrained obligation grows visibly to a
declared bound, then expires as an escalated, durable loss record.

First-party does not mean proprietary. The collector is only an HTTP client of
the public operator surface; this ADR's file layout and audit are the
interoperability contract, and an operator may replace the implementation with
one that honors both. One placement rule is decided now so a later flip is one
move instead of a fight: if the storage decision is ever revisited toward an
engine-backed format (section 2's trigger), the collector's home moves with it
to a separately versioned external integration, so this repository never gains
the engine as a dependency of anything.

### 2. Where content lands: compressed JSONL, one file per invocation

Layout: `<archive>/invocations/<id[0..2]>/<invocation_id>.jsonl`, written by
append while the invocation drains, finalized read-only and compressed to
`.jsonl.gz` once the invocation is terminal, acknowledged, and
digest-verified. The two-hex shard keeps one directory from holding every
invocation a fleet ever runs, which is its own filesystem cliff. Compression
is on by default — gzip over JSON runs roughly three to five times smaller,
turning the measured 10.3 MB invocation into a few megabytes and a heavy
year of dogfood into about a hundred gigabytes — and off with a flag for the
operator who wants raw bytes greppable at rest; `zgrep` and `zcat` keep every
workflow either way. The manifest (section 4) is a SQLite database beside the
files.

This is the boring option, chosen rather than defaulted to, so its costs are
stated as costs:

- **Torn tails.** A crash mid-append can leave a partial final line. Detected
  by parse failure or sequence discontinuity; repaired by truncating to the
  last complete event and replaying from fleetd. Bounded, and code rather
  than correctness.
- **A hand-rolled manifest.** Real work that DuckLake would have supplied. Its
  schema is one invocation row, not one row per event (section 4), so it stays
  small — but it is ours to write and ours when it breaks.
- **Size.** Uncompressed JSON is three to five times a columnar layout;
  compressed it is roughly on par with what Parquet-plus-overhead delivers for
  JSON-shaped content. Retention beyond one laptop disk at the uncompressed
  ratio is the first real wall, and compression moves it past any scale a
  local-first fleet on one machine produces this year.
- **Longitudinal analysis is batch-time.** Per-invocation questions are
  file-local at any scale. Needle search across the whole archive is `zgrep`
  and stays fast. Structural whole-archive scans — every event whose payload
  matches a predicate — are a script over every file, minutes at the
  hundred-gigabyte scale, not interactive. That is accepted, not hidden: a
  query service over the archive is a non-goal below, and the mission's
  "last month" question is per-invocation reads plus an index, which this
  serves.
- **Snapshots are not native.** Nothing is ever rewritten, so "the archive as
  of T" is the set of files present at T plus the manifest's append-only
  history — a weaker, simpler time travel that fits an append-once record.

What specifically breaks with JSONL, and at what scale — the question posed —
is therefore precise: per-invocation and needle-search questions do not break
at any scale this fleet can produce, because one file per invocation keeps
them proportional to one invocation. Two things do break. Capacity breaks when
sustained ingest times the retention target exceeds the archive volume in
compressed form — an operator policy that no longer fits the disk, a fact the
collector's own census reports, not a record count. Structural scans break
interactive use somewhere in the hundreds of gigabytes, when a batch pass
stops being something an operator runs while thinking. Until either is
measured, an engine buys nothing the audit needs.

Why the maintainers' three reasons do not carry it, answered as arguments:

- **One transactional store** (watermark and data committing together) is real
  and section 6 grants it in full — and then shows it buys relief from bounded
  re-reads, not a correctness property, because at-least-once delivery makes
  idempotent-by-key insertion mandatory for any correct collector on any
  backend. The joint commit removes work, not a bug class.
- **Inlining** exists to save small appends from becoming tiny Parquet files.
  JSONL *is* the staging area — one file, appended in place. The argument is
  decisive against plain Parquet and weightless against JSONL.
- **Snapshots as the mission sentence.** The mission sentence is "what was it
  thinking, last month" — an indexed per-invocation read, not a columnar scan.
  Columnar scans over reasoning logs are the query-service future this ADR's
  non-goals exclude.

And the two named worries stand: a four-month-old format is a poor place for
a decade-scale archive to live, and DuckDB is a large native dependency that
would sit under the most sensitive artifact fleetd touches. The deciding
property is that DuckLake's own decay path — should the extension ever stall —
is Parquet files plus a SQL catalog. This decision ships that degenerate,
engine-free form now, which is exactly why adopting the engine later, on the
trigger above, is a mechanical backfill from our own manifest rather than a
rescue. The reversible choice goes first.

The other two candidates, briefly, as instructed. Plain Parquet with a
hand-rolled manifest takes on a Parquet writer — a heavyweight dependency —
and chunk-and-compaction management, to obtain columnar files that no shipped
engine queries, while raw JSON inside Parquet is no more queryable than a
JSONL line and less greppable: all of the format work, none of the tooling.
A separate SQLite event store recreates beside the daemon exactly what
[0020](0020-bounded-operational-observations.md) and 0035 priced out of the
control store — an unbounded append-heavy blob warehouse in one single-writer
file, needing VACUUM to reclaim space, with no per-invocation deletion unit
and no streaming tools.

### 3. How the operator knows it is complete: recompute the chain

The audit is the point of the record, so it is specified first among the
consequences and designed to depend on nothing but itself. Per invocation:

1. **Read the authority.** Fetch the `invocation_observations` row from the
   operator surface: `event_count`, the per-class counts,
   `observed_payload_bytes`, `last_event_seq`, `last_event_digest`,
   `event_chain_digest`.
2. **Prove no gaps.** Read the invocation's file and require `event_seq`
   dense from 1 through `last_event_seq`. Density *is* the no-gaps proof: the
   fold admits only contiguous sequences, so against a terminal row a hole in
   the archive is collector loss by construction, not a fleetd behavior.
3. **Recompute the chain.** Hash each event exactly as the fold does — the
   event's sequence and timestamp as big-endian integers, its classification,
   a separator, and the compact serialization of the raw JSON value — then
   link each event's digest into the next with the previous digest and a
   fixed genesis tag, and compare the final chain against
   `event_chain_digest`, the per-class counts, and the byte total.
4. **Classify every gap.** Cross-reference fleetd's expiry records — 0035
   question 1's durable loss facts — which the collector archives alongside
   events, so a hole is *expired by the operator's bound* or *lost by the
   collector*, named rather than guessed.

This places two binding requirements on the export surface, which is separate
work and not changed here: every owed row must carry `event_seq`,
`observed_at_ms`, `classification`, and the raw JSON value verbatim —
precisely the digest's inputs — and the digest construction itself must be
published as a normative, versioned algorithm rather than left as a private
detail of the fold. A construction no third party can reproduce from
specification makes the audit a claim, not a record.

Engine-free is a trust property, not an aesthetic one. Auditing gzipped JSONL
is `zcat`, SHA-256, and a JSON parser — tens of lines anyone can read — so the
only program standing between the disk and the proof is the collector, which
the same audit checks. A DuckLake archive is auditable too, but its auditor
must trust an engine, an extension, and a Parquet reader to hold still for a
decade. When the record's entire value is "the archive is what can be audited
against the row," the shorter trust chain wins the tie.

The honest boundary, stated rather than implied: the chain proves the archive
agrees with fleetd's fold. It does not prove the fold observed everything the
harness emitted — that authority is 0020's design, unchanged here — and it
does not prove the harness told the truth. The audit's object is collector
faithfulness, which is exactly the hole 0035 left open.

### 4. The schema: fixed digest-bearing columns, raw verbatim, index in the manifest

One JSON object per line:

- `invocation_id` and `event_seq` — the identity 0035 already fixed, and the
  dedup and contiguity key.
- `observed_at_ms` and `classification` — digest inputs that are not
  re-derivable from content: the classification is fleetd's fixed vocabulary
  applied when the event was observed, and a harness-authored payload cannot
  reproduce it after the fact.
- `raw` — the harness JSON value verbatim, nested under the envelope key,
  unknown fields preserved, so an event is one parse and the envelope can
  never collide with a harness field name.

Every column is load-bearing — the identity, the time, the class, the
content — and nothing else is stored per event, because nothing else is needed
to prove, replay, or dedup. Value-verbatim is the correct fidelity bar, and it
is the bar the proof itself uses: the fold digests the canonical serialization
of the parsed value, so preserving the value preserves the proof; byte-stream
layout was never the authority. A single blob per event — raw update alone —
would drop the envelope and with it the audit. Wide typed columns derived
from `raw_update`'s innards would either lose bytes (violating verbatim) or
grow a schema per harness, which is a semantic contract this repository
deliberately never holds.

The manifest answers the questions files cannot index. One row per
invocation: its fleetd identities (`agent_id`, `source_message_id`,
`generation_id`, `binding_id`), timestamps, `stop_reason`,
`execution_certainty`, event count and chain digest, file name and
compression, and acknowledgement state — plus a snapshot of fleetd's terminal
observation row at close, so an audit is reproducible against the authority
as-of-close with fleetd offline (a terminal row is never written again, so
the snapshot cannot stale), plus the append-only deletion log of section 5.
The common operator questions — what ran Tuesday, everything by this agent,
every invocation that parked — are manifest queries; content questions are
file-local. Per-event rows are deliberately absent: per-event state lives in
the file, where the audit reads it, and the manifest stays proportional to
invocations, not to the 15,209.

### 5. Retention: the operator's policy, executed whole-file

Two independent clocks exist and neither is new. Fleetd's, unchanged: the
outbox grows to the operator's declared byte bound and expires oldest-first
as durable, escalated loss records (0035 question 1). The archive's, decided
here: the collector enforces `--retain-days` and `--max-bytes` at start and
daily thereafter, over finalized files only.

The deletion unit is the whole invocation file, never an event range, because
the audit's unit is the invocation — partial deletion would manufacture
archives that are neither complete nor honestly absent. Every deletion
appends to the manifest's deletion log: invocation, byte count, reason,
timestamp. That log is the collector-side twin of fleetd's expiry records, so
"what was here, and when did it stop being here" survives the deletion, and
`verify` reports archive coverage, fleetd expiries, and collector deletions
in one census. Nothing expires silently, and acknowledgement is never driven
by retention: drain and retention are decoupled on purpose, so deleting old
work can never rewind what fleetd believes delivered.

What the operator runs is two verbs of one binary: `collect` — drain, index,
audit at close, compress, expire — and `verify` — the independent audit pass
of section 3, exiting nonzero on any gap, mismatch, or unclassified hole.
Redaction stays out because verbatim cannot be redacted in flight (0035
question 4): the only redaction an honest archive supports is deleting a
whole invocation, which is what the policy offers.

### 6. At-least-once: dedup by identity, watermark in the manifest, ack after both

The collector's read position lives in the manifest SQLite, as the durable
local cursor over the export surface's keyset listing, committed in one
transaction with the invocation's index rows. The JSONL appends are
deliberately outside that transaction. Order is fixed: flush the file, commit
the watermark and index together, then acknowledge fleetd — whose own
watermark is content-addressed by position and monotonic per 0035, safe to
retry after a lost response, unable to move backward. The invariant that
matters is **an acknowledged event is always durably in the archive**; every
crash window reduces to bounded replay, never to loss and never to a
duplicate row.

Duplicates arrive and are absorbed rather than prevented, because
at-least-once means they must be: crash after commit but before
acknowledgement, and fleetd redelivers. Dedup is by `(invocation_id,
event_seq)`, which is the file's own contiguity — on resume, appending
continues after the file's last complete sequence and events at or below it
are dropped, idempotent by identity, the exact key 0035 already chose.
A torn tail is repaired into the same rule: truncate to the last complete
event, let replay refill it.

The transaction argument is granted and then declined, as an argument.
DuckLake's catalog-in-SQL would commit data and watermark together — one
transaction, one fewer crash window, and section 2 scores it honestly. But
at-least-once delivery makes idempotent-by-key insertion mandatory for any
correct collector on any storage: redelivery across a lost acknowledgement is
unavoidable, so the joint commit removes bounded re-read work, not a
correctness obligation. Fleetd draws the identical line one layer down —
deliveries are idempotent by message identity, not transactional with
consumers' side effects — and this ADR does not buy an engine for the
weaker half of the same property.

## Consequences

"What keeps the record" has an owner and a shape; "how an operator trusts it"
has a procedure that runs to SHA-256 and back. The chain digest is used for
the purpose 0020 built it, for the first time, by a program that is not
fleetd — and the audit it enables is reproducible by any third party with the
archive directory and the observation rows, no engine and no fleetd code.

The maintainers' lean is declined on stated grounds, not ignored: DuckLake is
the named successor, adopted by a future ADR when the capacity or scan-latency
trigger of section 2 is measured rather than imagined, and the migration is a
backfill from the manifest because today's layout is deliberately that
engine's own decay path. A future that wants the engine also moves the
collector out-of-repo, per section 1's placement rule — fleetd's repository
gains no storage dependency in either world.

The export surface, when built, inherits two binding requirements from
section 3 — digest inputs on every owed row, and the digest construction
published as a normative algorithm — plus machine-readable expiry records.
An export surface without them makes this audit vacuous, and that is the
constraint to carry into its design review; the surface's routes and outbox
remain 0035's deferred implementation work, untouched here.

The collector, when built, is one crate beside the surfaces and one binary
with two verbs, zero new dependencies, qualified the way the soak runner is
qualified — including restart mid-drain, mid-append, and mid-ack, since every
claim in section 6 is a crash-window claim. Nothing in the daemon changes: no
migration, no contract change, no dependency, and no code in this ADR.

Two costs are accepted rather than solved. Longitudinal questions beyond
needle search are batch jobs until a future ADR decides otherwise, with real
data to decide with. And the manifest is ours: small by design, but written
and maintained by hand where an engine would have supplied it — the price of
the boring option, paid knowingly.

## Deliberately not here

Implementation of anything: the export surface and outbox, the collector
crate, migrations, and contract changes. Design only, as requested.

A query service, UI, or API over the archive. The format must not smuggle one
in; interactive whole-archive analytics are the trigger for revisiting the
format, not a feature of this one.

Adopting DuckLake, DuckDB, or any storage engine in this repository — now, or
as a consequence of this text. If it ever happens, it is a new ADR carrying
measured numbers from a running collector.

Redaction, legal hold, and encryption at rest for the archive directory:
operator-side concerns named but not designed. The whole-file deletion unit
is chosen so a future policy remains expressible.

Revisiting 0035's obligation. The audit depends on the obligation's terms and
strengthens them; nothing here amends them.

Concurrent or multi-host collectors over one archive. One collector drains
one record, per 0035, and one writer owns an invocation file at a time;
scaling the drain outward is a new decision with new failure modes.
