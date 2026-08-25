# OpenCode durable-message grant qualification — 2026-08-24

## Scope

This checkpoint exercises the first invocation-scoped runtime grant made available to a real
continuous-worker turn. It proves protocol plumbing and durable authority, not
the semantics of the peer request or a general-purpose agent API.

The worker used:

- Fleetd grant `fleet.messaging.send`;
- MCP tool `publish_durable_message` served by the official Rust MCP SDK;
- `fleetd.harness.opencode` with OpenCode 1.4.0;
- model route `opencode/gpt-5.6-sol`;
- the semantic-neutral envelope adapter.

The isolated database, loopback daemon, worker seat, three agent identities,
and channel were created solely for this qualification. No Fleetd bearer token
was included in worker desired state, plugin configuration, ACP session setup,
or the OpenCode environment. ACP received only the random, narrow broker token
needed to reach the loopback MCP endpoint.

## Real turn

Source message `70aa01a0-236a-40f2-a554-3b9ae4235524` addressed worker agent
`a2264162-69ca-4e2c-ab87-25ab7c52c36a`. It asked the model to call the MCP tool
with operation `dogfood-send-v1`, peer
`b49fc1c4-712f-4295-9628-cc4a1f241695`, kind
`dogfood.peer.request`, and payload:

```json
{"proof":"real-opencode-mcp","value":42}
```

Invocation `b6fdf661-284e-4d3f-8862-8b6c643ff383` armed once, completed once,
and produced no retry, restart, or block. OpenCode discovered the tool through
ACP, called it, and reported the returned committed message ID in its final
assistant message.

Fleetd committed peer message `c59fe136-bc24-43f2-a423-dcf3aae0d66f` at global
sequence 2. Independent catalog inspection verified:

- sender was the active worker agent, not a tool argument;
- recipient was the exact channel peer;
- channel was inherited from the source invocation;
- kind and payload matched the bounded tool input;
- correlation was established as source message
  `70aa01a0-236a-40f2-a554-3b9ae4235524`;
- causation was the same exact source message.

The controller then revoked the broker grant before atomically completing the
input with result message `c7471a17-6f3e-4ec0-86b1-b952891cc05e`. The worker
report was one plugin generation, one reservation, one completion, zero
operational restarts, zero blocks, and zero pre-arm retries.

## Deterministic and security tests

The automated MCP transport test independently verifies:

- requests without the ephemeral token receive HTTP 401;
- calls outside an active invocation fail;
- exact `(invocation, operation_id)` replay returns the same message with
  `created: false`;
- conflicting reuse fails;
- the eight-message bound rejects a ninth new operation while still admitting
  an exact replay;
- sender, channel, correlation, causation, membership, and idempotency remain
  controller/store authority;
- calls after revocation fail;
- debug output redacts the broker token.

The ACP host also rejects non-loopback, missing, duplicate, and unrequested MCP
resolutions. This checkpoint does not claim OS sandboxing against a malicious
same-user process, peer-response waiting, inbox-read authority, hop-policy
enforcement, or semantic acceptance of any message payload.
