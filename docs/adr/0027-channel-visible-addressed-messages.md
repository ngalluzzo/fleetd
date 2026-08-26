# ADR 0027: Addressed messages remain visible to the channel

- Status: accepted
- Date: 2026-08-26

## Context

Fleetd originally treated `recipient_id` as both an inbox address and a read
access rule. In a shared channel, a third member could see broadcasts but not a
message addressed between two teammates. That behaves unlike a team room,
makes agent collaboration impossible to follow, and duplicates the privacy
already provided by first-class two-member direct conversations.

## Decision

Every operator or authenticated channel member reads the same complete ordered
message log through HTTP history, native WebSockets, and browser WebSockets.
The `recipient_id` field remains part of the immutable envelope but controls
addressing and durable inbox delivery only:

- an addressed message creates a delivery only for its `inbox` recipient;
- a broadcast creates deliveries for every other `inbox` member; and
- `stream_only` members see both forms without accumulating delivery rows.

Private communication uses a `direct` conversation. Its exact two-member
membership is the visibility boundary. Shared-channel messages are never
secret from another member, including one added later under Fleetd's permanent
membership rule.

Membership authorization still applies before any history page, stream grant,
or native WebSocket is opened. Credentials do not grant access to channels of
which their agent is not a member. Operator authority remains read-only for
messages and cannot impersonate an agent.

## Consequences

Fleetd now matches Slack- and Discord-style channel expectations: `to` means
who should act, not who may observe the conversation. Human `stream_only`
members can follow agent-to-agent work in real time without receiving leased
work. The history query and both stream transports share one channel-wide
replay rule, while inbox delivery retains its existing targeted semantics.

This is a pre-1.0 behavioral break. Existing addressed messages in a shared
channel become visible to every current and future member. No schema migration
is needed because the envelope and delivery rows do not change.
