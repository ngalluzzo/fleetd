# ADR 0002: Agent inboxes use at-least-once leased delivery

- Status: accepted
- Date: 2026-08-24

## Context

A live message stream is appropriate for conversation history but insufficient
for autonomous work. A worker can crash after receiving a message, and a newly
joined channel member must not retroactively inherit work that was addressed to
the old membership set.

## Decision

Appending a message and creating its deliveries happen in one transaction.
Direct messages create one delivery for the named recipient. Broadcast messages
snapshot every current member except the sender. Later membership changes do
not rewrite that recipient set.

A worker atomically claims eligible deliveries for a bounded lease. Successful
processing acknowledges each delivery with the matching lease token. Failed
processing releases it with a bounded retry delay and diagnostic text. An
expired lease becomes claimable again and increments the attempt counter.

The guarantee is at-least-once, not exactly-once. A crash between an external
side effect and acknowledgement can repeat the effect. Adapters must use the
stable message ID as their idempotency key wherever the target system permits.

## Consequences

Work survives daemon and worker restarts, concurrent workers cannot both hold a
valid lease for the same delivery, and the operator can inspect attempts and
errors. Delivery rows add bounded write amplification proportional to channel
membership. Large fan-out optimization is deferred until measurements require
it.
