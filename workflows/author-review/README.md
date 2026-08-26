# Author-review workflow dogfood

This package is Fleetd's first external workflow experiment. It pairs one
credential-owning runner with one real author-review plugin and deliberately
keeps their interface at `fleetd.workflow-draft@0.0.1`. It is not a generic
workflow SDK and is not part of Fleetd's daemon or base plugin manifest.

```text
Fleetd inbox
    ↓ leased by runner agent credential
fleetd-author-review-runner
    ↓ typed JSON lines; no credential, lease, sender, or channel authority
fleetd-author-review-plugin
    ↓ bounded ProposedMessage values
runner appends idempotent ordinary Fleetd messages
```

The plugin owns the initial vocabulary:

- `work.requested`
- `artifact.proposed`
- `review.requested`
- `review.completed`
- `revision.requested`
- `work.completed`
- `work.blocked`

The first flow requires a coordinator to propose a bounded decomposition. The
plugin materializes every child as an ordinary `work.requested` addressed to a
selected author in the same channel. Proposed changes go to a distinct
reviewer. Approval completes a child; revision returns it to its original
author; exhausting the configured revision rounds blocks the child and parent.
The parent completes only when every child is approved.

All messages remain visible to every channel member. Addressing selects the
agent expected to act and controls inbox delivery. Fleetd itself continues to
interpret none of the workflow kinds or payloads.

## Draft protocol

The runner launches the absolute plugin executable directly with an empty
environment and communicates over bounded JSON-lines stdin/stdout. It first
calls `workflow.describe`, verifies the exact paired interface and plugin
identity, then calls `workflow.evaluate` for one leased input. See the
[draft contract](../../docs/contracts/author-review-workflow-draft.md).

The evaluate request contains only public immutable messages, public channel
membership, opaque plugin configuration, the runner agent ID, and workflow
correlation ID. It never contains a Fleetd bearer or delivery lease. A proposal
may choose only operation ID, recipient, kind, and payload. The runner derives
sender, channel, correlation, causation, and idempotency from the leased input.

If the runner crashes after appending an effect but before acknowledging its
input, durable replay observes logical effects across the complete correlated
history. Already committed siblings are suppressed while any missing sibling
is still proposed. A changed replay conflicts and is blocked rather than
silently producing a different transition. Operation IDs are short per-input
labels, so bounded semantic request IDs do not overflow Fleetd idempotency
keys.

## Build and run

```sh
cargo build -p fleetd-author-review
cp workflows/author-review/runner.example.json .fleetd/author-review.json
# Fill exact agent IDs, absolute paths, and the runner credential-file path.
cargo run -p fleetd-author-review --bin fleetd-author-review-runner -- \
  --config .fleetd/author-review.json
```

The runner identity must be an `inbox` member of the selected team channel.
The human participant may be `stream_only`; coordinator, author, and reviewer
candidates must be `inbox` members. Worker seats should use these exact inbound
and result kinds:

| Seat | Accepted kinds | Result kind |
|---|---|---|
| coordinator | `work.requested` | `artifact.proposed` |
| author | `work.requested`, `revision.requested` | `artifact.proposed` |
| reviewer | `review.requested` | `review.completed` |

The current envelope worker returns a bounded assistant transcript. For
`artifact.proposed` and `review.completed`, the plugin accepts either a direct
semantic payload or one exact complete final-assistant JSON object carried by
that transcript. The seven declared Draft 2020-12 schemas describe the same
semantic objects after that extraction, including every variant, closed nested
objects, required fields, and global bounds. This is an intentional
first-integration coupling, not a normalization layer.

A completed review is accepted only when its message causation identifies the
exact `review.requested` assignment. That assignment binds the exact proposed
artifact message and projection; an absent or mismatched causal assignment, a
different second artifact at the same revision, or a conflicting second review
result is rejected.

Submit the root request to the runner with `correlation_id` equal to
`request_id`:

```json
{
  "schema_version": 1,
  "request_id": "FLEETD-001",
  "title": "Dogfood author-review",
  "objective": "Run a real Fleetd change through the visible workflow",
  "repository": {
    "path": "/absolute/path/to/fleetd",
    "base_revision": "exact-git-revision"
  },
  "scope": ["one bounded product change"],
  "acceptance_criteria": ["tests pass", "review approves exact revision"]
}
```

The experiment currently permits one fan-out level, at most 16 children, and a
configured `max_revision_rounds` from 0 through 8 inclusive. Revision `0` is
the initial artifact and does not consume a round. A value `N` permits
additional revisions `1` through `N`; a `revise` decision on revision `N`
blocks the child and parent. Therefore `0` blocks the first `revise` decision,
while `8` permits eight additional author attempts. The experiment does not
create Git worktrees, merge code, or interpret repository state. Existing
agents and Git tools retain those responsibilities while the workflow records
their exact artifact references.
