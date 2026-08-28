# Operations

## Configuration and authority

`fleetd init` writes configuration schema 1. The default file is
`.fleetd/config.json`; `--fleet-config` and `FLEETD_CONFIG` select another file.
Relative database and operator-token paths resolve from the configuration
file's directory.

Configuration precedence is:

1. an explicit command option;
2. its `FLEETD_*` environment variable;
3. the selected configuration file;
4. the local defaults used by `fleetd init`.

The configuration file contains locations, never credentials. Operator and
agent token files must remain owner-only. Worker desired state must not contain
a Fleetd bearer or provider secret.

## Health and traces

`fleetd status` reads `/v1/fleet-health`: the latest durable plugin generation
and session generation for each agent, the invocations still owed an outcome,
and a bounded 500-record delivery census. Scope it to one seat with `--agent`
and bound the census with `--delivery-limit`.

The daemon composes that report in one read, so the answer cannot differ
between surfaces. A successful response is itself the liveness signal;
`/health` remains available for a bare process check.

Fleetd does not invent a separate worker-health boolean. A continuous worker
owns exactly one negotiated plugin generation at a time, and the generation's
heartbeat is advanced by that worker. Its `active`, `stale`, or `stopped`
health therefore reports the durable worker/plugin execution generation;
session and invocation state show whether that generation is making progress.

Plugin health has three exact values:

- `active`: the latest durable heartbeat is within the generation's bound;
- `stale`: the generation still claims active ownership but missed that bound;
- `stopped`: Fleetd recorded terminal shutdown evidence.

A hard-killed worker initially leaves a stale generation. Its replacement
creates a new generation and, when compatible, adopts the native-session lane
under a higher owner epoch. `fleetd trace --invocation ID` joins the exact
invocation to the generation and binding that executed it.

## Reading what an agent actually did

`fleetd trace` reports counters, digests, and outcomes. It does not report
content, because Fleetd stores none: reasoning, tool arguments, and intermediate
plans belong to the native harness. `fleetd transcript` asks the harness for
them.

```sh
fleetd transcript --config .fleetd/worker.json --session NATIVE_SESSION_REF
```

The session reference is what `fleetd status --agent AGENT_ID` reports for the
seat. The command reads through a short-lived second plugin process, so a
running worker keeps its own session: it resumes the session to attach and then
loads it to read, which is the split ACP defines, and it never closes the
session because the seat still owns that lane.

What comes back is grouped into `turns`, one per invocation, each carrying the
prompt that opened it and the reasoning, tool calls with their exact arguments
and output, and assistant messages that followed. A turn's `invocation_id` names
the invocation it belongs to; `attributed_turns` counts how many were
identified. A replay carries each entry's final state rather than the stream that
produced it, so chunk boundaries and intermediate tool states are gone while
content is whole.

A turn with `invocation_id: null` is one Fleetd did not dispatch — the session
setup that precedes the first prompt, or work something else started against the
same session. Those keep their own group rather than being folded into the turn
before them, because attributing a stranger's conversation to a Fleetd
invocation would be worse than admitting the gap.

Three limits are worth knowing before relying on it:

- **It is the harness's memory, not Fleetd's.** A session the harness has pruned
  cannot be replayed, and Fleetd's own evidence rows outlive it. A trace that
  resolves against an invocation whose transcript is gone is expected.
- **A read during an active turn is stale, not torn.** The turn in flight has
  stored nothing yet, so the reply is complete through the last settled entry.
- **One session serves a whole channel.** A replay covers every invocation on
  that lane. Attribution is exact rather than approximate: the envelope adapter
  names its invocation in the prompt, and a replay carries prompt text verbatim,
  so each turn opens with a user message whose text contains that invocation's
  id after an instruction preamble. Split on user messages, take the text from
  its first `{`, and read `invocation.id`. A user message with no envelope is a
  turn something other than Fleetd started.
- **Entry timestamps are read times, not event times.** A replay carries no
  original timestamps, so `observed_at_ms` says when the entry was replayed.
  Ordering comes from `entry_seq`.

A runtime that cannot replay says so rather than returning an empty transcript,
and an unknown session names the agent whose bindings to check. See
[ADR 0029](adr/0029-harness-transcript-retrieval.md).

## Inbound triggers

A trigger is a thing that creates work with no human present: a recurring job, a
webhook receiver, a file watcher. Registering one declares the channel it may
reach, the agent its messages are attributed to, and the exact kinds it may
create, and returns its credential exactly once.

```sh
fleetd trigger add --name nightly-sweep \
  --channel CHANNEL_ID --sender AGENT_ID \
  --kind task.request --credential-file .fleetd/nightly.token
```

The declared kinds are the whole of its authority over content, and they are
fixed at registration: changing what a trigger may create means registering a
different trigger. The credential file is owner-only, and the token is never
recoverable afterwards -- `fleetd trigger rotate-credential` issues a
replacement and revokes the old one immediately.

Firing uses the trigger's own credential, not the operator's:

```sh
fleetd --token-file .fleetd/nightly.token trigger fire \
  --trigger TRIGGER_ID --occurrence 2026-08-27T02:00 \
  --recipient AGENT_ID --kind task.request --payload '{"sweep":"nightly"}'
```

`--occurrence` names the firing, and fleetd derives the durable idempotency key
from the trigger and that name together. A scheduler that fires twice -- an
overlapping run, a machine waking, a retry after a lost response -- creates work
once, and the response's `created` says which call made it. This is the reason
to register a trigger rather than run a crontab line: a crontab line has to
construct a distinct key itself and silently creates nothing when it gets that
wrong.

Reading the registry is how an idle fleet is told apart from a broken one:

```sh
fleetd trigger list
fleetd trigger show --trigger TRIGGER_ID
```

Each row carries `last_fired_at_ms`, `last_occurrence_id`, and
`accepted_occurrences` -- occurrences that produced work, not calls received. A
trigger re-firing Tuesday's occurrence all week has created nothing since
Tuesday, and the record says so rather than reporting a healthy pulse.

Retiring ends the standing grant and revokes every credential that could fire
it, in one transaction. The registration stays, because a grant that was
withdrawn is a fact worth reading later:

```sh
fleetd trigger retire --trigger TRIGGER_ID \
  --reason 'the deploy it watched was decommissioned'
```

The reason is required and is what the next operator reads. Retiring twice is
not an error; a retired trigger cannot be handed a replacement credential, so
stopping one is final.

Nothing in fleetd decides *when* a trigger fires. A cron expression, a webhook
body, and a filesystem event are all opaque, and no scheduler ships here.

## Delivery inspection and controls

Read-only delivery views never expose lease tokens:

```sh
fleetd deliveries --state pending
fleetd deliveries --state leased --agent AGENT_ID
fleetd deliveries --state blocked
```

`lease_expired` distinguishes a persisted leased row whose owner deadline has
passed. The next safe claim or managed reservation performs recovery. A
`dispatch_armed` invocation whose lease expired is blocked rather than silently
repeated.

There are three deliberately different controls:

- `retry` is an agent-owned settlement for a live lease whose external effect
  is provably unstarted. It requires the active lease token and rejects an
  armed invocation.
- `requeue` is an operator decision for a blocked, ambiguous attempt after the
  operator has established that another attempt is acceptable.
- `abandon` is an operator decision that makes blocked work terminal.

```sh
fleetd --token-file .fleetd/agent.token inbox retry \
  --agent AGENT_ID --message MESSAGE_ID --lease LEASE_TOKEN \
  --retry-after-ms 5000 --error 'model server was unavailable before dispatch'

fleetd inbox resolve --block BLOCK_ID --resolution requeue --retry-after-ms 1000 \
  --note 'external system confirms no effect occurred'

fleetd inbox resolve --block BLOCK_ID --resolution abandon \
  --note 'external effect occurred; do not execute again'
```

Settlement is idempotent for an identical retry. A changed or stale lease and
a conflicting second operator decision fail closed.

## Archiving evidence

The two evidence listings are cursor-addressed, so an external collector can
copy every row into its own store without polling-and-diffing and without
losing rows that scroll past a bounded page:

```sh
curl -sH "Authorization: Bearer $TOKEN" \
  "$FLEETD/v1/invocation-observations?order=oldest&settled=true&limit=500"
```

Each listing is ordered by the clock that every durable change to a row
advances -- `last_heartbeat_at_ms` for a plugin generation, `updated_at_ms`
for an invocation observation. A collector resumes by passing the last row it
received back as `after_ms` and `after_id`. Both halves are required: a
millisecond alone cannot address a boundary between two rows that share it,
and a half cursor is rejected rather than read as "start from the beginning".

`settled=true` reports only rows that can never change again -- a stopped
generation, a terminal invocation. Their clocks are frozen, so a settled row
archived once is archived correctly and forever. Rows still in flight keep
moving and reappear ahead of the cursor each time their evidence changes,
which is why a collector that wants immutable records asks for settled ones
and a collector that wants current state does not.

`order=newest` is the default and is what an operator reads. Cursor, order,
and `settled` are the whole mechanism: Fleetd does not push evidence anywhere,
does not know what a collector does with it, and holds no retention policy of
its own. Deleting archived evidence from the control database is not yet
supported; rows accumulate until one exists.

## Backup

The default Fleetd directory contains both durable state and operator
authority. Agent token files and worker configuration may also live there.
Treat every backup as a secret.

The simplest consistent backup is an offline directory snapshot:

1. Stop every worker and the daemon with `Ctrl-C` and wait for them to exit.
2. Confirm no Fleetd process is using the database.
3. Archive the complete initialized directory, including `fleetd.db`, any
   `fleetd.db-wal` or `fleetd.db-shm` files, `config.json`, token files, and
   worker desired-state files.
4. Store the archive with owner-only permissions and encryption appropriate to
   the host.

For the default layout:

```sh
tar -czf fleetd-backup-$(date +%Y%m%d-%H%M%S).tar.gz .fleetd
chmod 600 fleetd-backup-*.tar.gz
```

Do not copy only `fleetd.db` while writers are active. SQLite is authoritative
and may have committed pages in its WAL. Online backup can be added later with
SQLite's backup API; the current supported procedure is an offline snapshot.

## Restore

1. Stop all Fleetd processes targeting the restore location.
2. Move the current directory aside; do not merge a backup into live files.
3. Extract the archive as one directory.
4. Restore owner-only permissions on directories and token files.
5. Run the daemon against the restored configuration, then inspect status,
   delivery state, and the latest invocation trace before starting workers.

```sh
mv .fleetd .fleetd.before-restore
tar -xzf fleetd-backup-YYYYMMDD-HHMMSS.tar.gz
chmod 700 .fleetd
chmod 600 .fleetd/*.token
fleetd serve
fleetd status
fleetd deliveries --state blocked
fleetd invocation list
```

The database migrations are forward-only. Restore with the same Fleetd version
that created the backup when possible, then upgrade normally. Never point an
older binary at a database already migrated by a newer release.

## Tagged releases

Pull requests and pushes to `main` run the full `bin/ci` contract. A tag named
exactly `v` plus the root Cargo package version triggers native release builds
for Linux x86-64 and Apple Silicon. The workflow packages `fleetd`, the
OpenCode and Codex plugins, the development ACP reference plugin, operator
guides, and the restart demonstration; it publishes SHA-256 checksums with the
GitHub release.

Release procedure:

1. Update every published Cargo package version intentionally and commit the
   lockfile.
2. Ensure `bin/ci` passes on the exact commit.
3. Create an annotated `vMAJOR.MINOR.PATCH` tag on that commit.
4. Push the tag and verify both native build jobs and the published checksums.
5. Download one archive on each supported platform and run
   `examples/restart-demo/run.sh` before announcing it.
