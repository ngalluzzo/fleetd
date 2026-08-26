# Author-review workflow draft 0.0.1

## Status and scope

This is a deliberately unstable contract exercised by exactly one external
runner and one author-review plugin. It must not be promoted into Fleetd's base
plugin lifecycle or treated as a general workflow interface until another real
workflow demonstrates the shared semantics.

Fleetd's daemon remains unaware of this contract. It stores every workflow
event as an ordinary opaque message and creates ordinary addressed deliveries.

## Authority split

The runner owns the Fleetd agent credential, inbox lease, channel access,
idempotency, settlement, retry, block, and plugin process. The child plugin
receives none of those authorities. It receives only:

- one immutable public input envelope;
- the correlation-scoped immutable channel history;
- the public channel-membership projection;
- the runner agent and workflow correlation IDs; and
- credential-free opaque plugin configuration.

The plugin proposes `operation_id`, `recipient_id`, `kind`, and `payload`.
Before append, the runner proves the recipient is a channel member, the kind
was declared by `workflow.describe`, operation IDs are unique and bounded, and
payloads remain bounded. The runner derives:

- sender from its authenticated credential;
- channel and correlation from the leased input;
- causation from the leased message ID; and
- idempotency as `workflow/{input_message_id}/{operation_id}`.

## Wire

The runner launches an absolute executable without a shell, clears its
environment, and supplies bounded newline-delimited JSON-RPC 2.0 on stdin and
stdout. Each frame is at most 1 MiB. Requests and responses carry one unsigned
integer ID. Unknown fields are rejected by typed decoders.

### `workflow.describe`

The request params are exactly `{}`. The result contains:

```json
{
  "interface_id": "fleetd.workflow-draft",
  "interface_version": "0.0.1",
  "plugin_id": "fleetd.workflow.author-review",
  "plugin_version": "0.0.1",
  "roles": ["coordinator", "author", "reviewer"],
  "event_schemas": [
    { "kind": "work.requested", "schema": {} }
  ]
}
```

The actual response contains exactly seven unique event records with the
plugin-owned semantic payload schema for each initial vocabulary kind. The
paired runner rejects another interface ID, version, plugin ID, duplicate kind,
or incomplete vocabulary before leasing work.

### `workflow.evaluate`

The request contains:

```json
{
  "configuration": {},
  "runner_agent_id": "agent-id",
  "workflow_id": "FLEETD-001",
  "input": { "seq": 1, "id": "message-id" },
  "history": [],
  "members": []
}
```

`input`, `history`, and `members` use the complete credential-free structures
defined in `protocol.rs`; the abbreviated example is not a permissive wire
shape. History must contain the input, belong to one channel, be unique and
strictly sequence ordered, and contain no more than 10,000 workflow-correlated
messages. Membership is capped at 256.

The result is:

```json
{
  "projection": {
    "workflow_id": "FLEETD-001",
    "root_request_id": "FLEETD-001",
    "phase": "awaiting_review",
    "child_count": 2,
    "completed_children": 1,
    "blocked_children": 0
  },
  "proposals": [
    {
      "operation_id": "request-review:FLEETD-001-B:0:message-id",
      "recipient_id": "reviewer-agent-id",
      "kind": "review.requested",
      "payload": {}
    }
  ]
}
```

At most 32 proposals are accepted. Projection is deterministic diagnostic
output; Fleetd does not persist or interpret it as a second workflow store.
The immutable channel messages remain the projection authority.

## Author-review policy

The plugin accepts runner deliveries only for initial `work.requested`,
`artifact.proposed`, and `review.completed`. It emits the seven-kind vocabulary.
Configuration supplies one coordinator ID, non-empty author and reviewer
candidate lists, `max_children` from 1 through 16, and
`max_revision_rounds` from 0 through 8. Every role candidate must be a current
`inbox` member; the selected reviewer must differ from the child author.

The coordinator proposes a decomposition artifact. Each unique child becomes
a first-class `work.requested` assigned round-robin to an author. A change
artifact is reviewed against its exact message and artifact revisions. Approval
completes the child; revision returns it to its original author. The parent
completes only when every child is complete and blocks when a child exceeds its
revision bound.

## Recovery and failure

The plugin is deterministic and owns no storage. Each evaluation reconstructs
state from immutable correlated history and returns only missing effects. A
runner crash after append but before acknowledgement is safe: replacement
either sees the already committed event or reuses the same Fleetd idempotency
key. A changed effect for that identity returns conflict and blocks the input.

Protocol rejection, invalid attribution, invalid semantic payload, undeclared
kind, non-member recipient, or idempotency conflict blocks the exact delivery.
Plugin unavailability and transient Fleetd failures release it for bounded
retry. No failure grants the plugin a credential or permits a direct external
effect.
