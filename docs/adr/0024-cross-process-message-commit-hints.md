# ADR 0024: Cross-process message commit hints

- Status: accepted
- Date: 2026-08-25

## Context

Fleetd's daemon and continuous workers deliberately open the same authoritative
SQLite database from separate processes. HTTP message appends happen inside the
daemon, so the daemon can wake its process-local WebSocket broadcast bus after
commit. A managed worker, however, atomically appends its causal result while
settling an invocation through its own SQLite connection and process.

Cursor reconnect always recovered that durable result, but an already-open
daemon WebSocket had no event that caused it to reconcile SQLite. The result
was durable but not live. A process-local broadcast bus cannot by itself satisfy
the live-conversation contract for out-of-process worker commits.

SQLite offers no portable cross-process commit callback. Moving worker
settlement behind an undocumented daemon endpoint would introduce a second
authorization path and couple crash-safe settlement to daemon availability.
Client or server polling would obscure the missing notification edge rather
than represent it.

## Decision

On Unix, `fleetd serve` binds one private local Unix datagram address derived
from the canonical database identity. The containing directory is owned by the
database owner with mode `0700`; the socket uses mode `0600`. A second live
daemon for the same database is rejected, while a stale socket left by process
death is replaced.

`fleetd worker run` opens the same database with a best-effort message-commit
notifier. After and only after a newly created message commits, it sends one
content-free byte to that database's address. This covers atomic managed-result
completion and invocation-scoped peer-message publication. Idempotent replays
that create no message send no hint.

The daemon translates the datagram into an in-process stream wake. Every
authorized stream then replays its own exact channel and principal-relative
cursor from SQLite. The datagram carries no channel ID, message ID, payload,
credential, or settlement authority.

The hint is explicitly lossy:

- send failure does not roll back or fail a committed operation;
- duplicate or spurious hints cause only another bounded durable replay;
- a daemon crash can lose a hint, after which cursor reconnect recovers; and
- SQLite sequence and message identity remain the only ordering and content
  authorities.

The existing direct in-process wake remains for daemon-owned HTTP commits.
Both wake sources converge on the same durable replay implementation.

## Consequences

An open native or browser stream now observes worker-authored results without
polling and without giving the worker a Fleetd bearer credential. Daemon
restart remains independent of worker execution; the stable database identity
rebinds the same local hint address.

The first implementation is Unix-local, matching the currently qualified
worker process-containment deployment. Non-Unix deployments retain durable
cursor replay but do not gain this cross-process live-wakeup guarantee yet.
Remote workers still require encrypted transport and enrollment rather than an
extension of this local mechanism.

The regression suite opens two independent `Store` instances and proves that a
commit through the externally notifying instance wakes a daemon-owned
WebSocket, which then reads the exact opaque envelope from SQLite. The Phase C
product qualification additionally runs the writer and daemon as separate OS
processes.
