# fleetd

`fleetd` is a local-first control plane for software agents that need to talk to
each other and keep working across process restarts.

The foundation is deliberately small: durable agent identities, bounded
channels, immutable messages, cursor-based replay, live WebSocket streams, and
leased agent inboxes that resume after worker crashes. It does not host Git,
understand a particular model harness, or require a federated identity protocol.

## Run it

```sh
cargo run -- serve --db .fleetd/fleetd.db
```

In another terminal, register two agents. Save the `id` fields printed by these
commands:

```sh
cargo run -- agent add --name piler --metadata '{"harness":"dsh"}'
cargo run -- agent add --name weaver --metadata '{"harness":"codex"}'
```

Create a channel containing both IDs:

```sh
cargo run -- channel create --name gooir-001 \
  --member Piler_ID --member Weaver_ID
```

Watch it from one process:

```sh
cargo run -- message watch --channel CHANNEL_ID
```

Then send a durable message from another:

```sh
cargo run -- message send --channel CHANNEL_ID --from Piler_ID \
  --to Weaver_ID --text 'review commit 5fe343f'
```

Weaver's adapter can atomically lease the message:

```sh
cargo run -- inbox claim --agent Weaver_ID --limit 1 --lease-ms 300000
```

After completing the work, it acknowledges the returned message and lease IDs:

```sh
cargo run -- inbox ack --agent Weaver_ID \
  --message MESSAGE_ID --lease LEASE_TOKEN
```

If processing fails, `inbox retry` releases the delivery with a delay and
diagnostic message. If the worker disappears, the lease expires and another
worker can claim the delivery with an incremented attempt count.

Every message also accepts a machine-readable JSON payload, a semantic `kind`,
and optional correlation and causation IDs.

## Current trust model

Version 0.1 binds only to localhost by default and assumes every local client is
trusted. The API does not authenticate `sender_id`, so it must not be exposed to
a network yet. The daemon rejects non-loopback listen addresses while this is
true. Agent-bound bearer authentication is the next security boundary and must
land before remote workers are supported.

See [the vision](VISION.md), [architecture](docs/ARCHITECTURE.md),
[protocol](docs/PROTOCOL.md), and [milestones](docs/MILESTONES.md) for the
intended boundaries and next slices.
