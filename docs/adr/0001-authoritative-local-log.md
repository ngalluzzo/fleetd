# ADR 0001: SQLite is the authoritative local log

- Status: accepted
- Date: 2026-08-24

## Context

Agents require low-latency notifications, but in-memory delivery disappears on
restart and cannot prove what an agent had an opportunity to process.

## Decision

SQLite is authoritative for identities, membership, messages, and delivery
state. Writes commit there before any notification is emitted. WebSockets and
future platform notifications are wake-up hints; a consumer always recovers its
state from the durable API.

Migrations are ordered SQL files applied and checksummed by `sqlx`. The daemon
uses foreign keys, write-ahead logging, a bounded busy timeout, and a connection
pool. A future clustered store may implement the same storage interface, but no
distributed-system semantics leak into the first node.

## Consequences

One node remains easy to operate and inspect. A client can reconnect after any
cursor and recover channel history. Multiple daemon processes must not be
treated as one coherent notification bus; clustering requires a separate
decision and implementation.
