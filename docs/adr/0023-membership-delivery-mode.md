# ADR 0023: Channel membership declares delivery mode

- Status: accepted
- Date: 2026-08-25

## Context

Fleetd currently creates a durable inbox delivery for every direct recipient
and every broadcast channel member other than the sender. That is correct for
autonomous worker seats, but an addressable human or passive client consumes
the durable channel log through history and live replay rather than through a
leased work inbox.

Treating a human as an ordinary current member would leave every reply pending
in an inbox with no worker. Having a browser claim and acknowledge those rows
would instead conflate successful work processing with rendering a timeline or
reading a message. Inferring behavior from opaque agent metadata would couple
the kernel to participant types.

## Decision

Each channel membership will carry one immutable operational delivery mode:

- `inbox`: addressed and broadcast messages snapshot a leased durable delivery
  for this member under the existing rules.
- `stream_only`: messages remain visible through authenticated history and live
  replay, but appending them creates no inbox delivery for this member.

The mode belongs to membership rather than agent identity. One addressable
participant may operate an inbox seat in one channel and a passive stream seat
in another. Fleetd still does not know whether that participant is a human,
model, service, or UI.

Existing memberships migrate to `inbox`, preserving every current behavior.
The existing `member_ids` channel-creation input also creates `inbox`
memberships. An additive `members` input permits exact modes in the same atomic
channel-creation transaction and rejects duplicate agents across both inputs.
Adding a member accepts an optional mode defaulting to `inbox`. Repeating the
exact membership is idempotent; attempting to re-add an existing member under
another mode returns a conflict. Membership and mode remain permanent for that
channel generation.

Message append continues to require current sender and direct-recipient
membership. Within the append transaction:

- a direct message creates a delivery only when its recipient's membership is
  `inbox`;
- a broadcast snapshots only other `inbox` members; and
- every member, regardless of mode, retains the same principal-relative
  history and live-stream visibility.

The public API will expose a channel-membership read model containing channel
ID, agent ID, agent name, join timestamp, and delivery mode. It will not expose
opaque agent metadata to an ordinary member. Operators and members of the exact
channel may list it, allowing clients to address peers without inference.

Stream-only is not a read receipt and has no settlement state. Offline clients
recover exclusively from the immutable message cursor. Inbox delivery remains
the stronger autonomous-work guarantee and keeps its existing lease,
acknowledgement, retry, and block semantics.

The stable contract is
[`channel-membership-delivery-v1.md`](../contracts/channel-membership-delivery-v1.md).

## Rejected alternatives

- **Leave the human inbox pending:** makes backlog and worker-health views report
  work that no worker is intended to process.
- **Acknowledge on browser render:** changes a work-settlement fact into a UI or
  read-receipt fact and is unsafe across multiple clients.
- **Infer human versus worker from metadata:** violates opaque metadata and
  makes delivery semantics depend on an unversioned convention.
- **Create an operator-authored message type:** adds impersonation or a second
  sender model instead of using one addressable-participant abstraction.
- **Disable delivery per message:** lets senders choose recipient reliability
  and makes broadcast snapshots inconsistent for one member.

## Consequences

The kernel gains one operational membership property and one forward migration.
Delivery creation becomes a join against the recipient snapshot's declared
mode. Existing channels and workers behave exactly as before.

Human-controlled identities can participate, send attributed messages, receive
causal replies, remain offline, and recover by cursor without accumulating
false work. A participant that later needs a different delivery mode uses a new
channel generation or a new identity until an explicit membership-transition
protocol is designed.
