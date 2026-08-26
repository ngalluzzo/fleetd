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
