# `@fleetd/client`

This directory contains the TypeScript wire client. HTTP operations and wire
types under `src/generated` are disposable output from
`../../openapi/fleetd-v1.json`; do not edit them by hand. The handwritten
browser channel-stream adapter is presentation-agnostic and consumes the
generated immutable `Message` type without adding a DOM or UI dependency.

```sh
npm ci
npm run generate
npm run typecheck
npm test
```

Configure the generated Fetch client once when the UI starts:

```ts
import { client } from "@fleetd/client/client";

client.setConfig({
  baseUrl: "http://127.0.0.1:4317",
  auth: () => operatorToken,
});
```

The OpenAPI operation for `/v1/channels/{channel_id}/stream` describes the
WebSocket upgrade and its `Message` frame through `x-fleetd-websocket`.
Because Fetch cannot perform that upgrade, the generator deliberately excludes
extension-marked operations instead of emitting a misleading HTTP function.
Streaming code must use the generated `Message` type and the protocol
documented in `../../docs/API_CONTRACT.md`.

Browser presentations use the exported wire adapter rather than Fetch history
polling:

```ts
import { openBrowserChannelStream } from "@fleetd/client";

const stream = openBrowserChannelStream({
  origin: window.location.origin,
  channelId,
  credential: viewingCredential,
  after: retainedCursor,
  async accept(message) {
    await retainForPresentation(message);
  },
});

await stream.closed;
```

The credential and each one-time grant remain in memory. Each attempt performs
only the authenticated stream-grant POST and the exact browser WebSocket
upgrade. `cursor` advances only after `accept` resolves. Reconnects are bounded,
duplicate sequences are not re-presented, and a missing or mismatched grant
linkage fails closed without an HTTP history fallback. `maxPendingMessages`
bounds frames waiting behind the one message currently being accepted.

## Headless conversation session

Presentation targets compose the wire client through `ConversationTransport`
instead of duplicating channel selection and message projection in each UI.
The shipped browser transport uses the operator credential only for channel
discovery and the human participant credential for membership, stream, and
send operations. `ConversationSession` then owns generation-fenced selection,
per-channel cursors, bounded immutable-envelope retention, send/echo
convergence, and honest local connection state without importing a DOM.

```ts
import {
  ConversationSession,
  createBrowserConversationTransport,
} from "@fleetd/client";

const transport = createBrowserConversationTransport({
  origin: window.location.origin,
  participantId: humanAgentId,
  operatorCredential,
  participantCredential,
});
const conversation = new ConversationSession(transport);

conversation.subscribe(renderSnapshot);
await conversation.start();
await conversation.selectChannel(channelId);
await conversation.send({
  idempotency_key: crypto.randomUUID(),
  recipient_id: workerAgentId,
  kind: configuredRequestKind,
  payload: { text: composerText },
});
```

The session does not interpret agent execution, harness events, or application
payload conformance. A future TUI supplies a native bearer-WebSocket transport
to the same interface.

## Operator collaboration client

Channel lifecycle, direct conversations, and the agent directory use an
operator-owned client separate from the participant-owned conversation
session. Its request and response types come directly from the generated
OpenAPI contract. Shared-channel mutations and idempotent one-to-one direct
conversation opening are deliberately distinct methods:

```ts
import { createFleetdOperatorClient } from "@fleetd/client/operator";

const operator = createFleetdOperatorClient({
  origin: window.location.origin,
  operatorCredential,
});

const conversations = await operator.listConversations();
const direct = await operator.openDirectConversation({
  members: [
    { agent_id: humanAgentId, delivery_mode: "stream_only" },
    { agent_id: workerAgentId, delivery_mode: "inbox" },
  ],
});

await conversation.refreshChannels();
await conversation.selectChannel(direct.id);
```

`listAgents`, `listPluginGenerations`, and `listSessionBindings` expose the
authoritative public read models without manufacturing presence from them.
HTTP failures throw `FleetdOperatorClientError`, whose `status` and generated
`ErrorResponse` body allow presentations to distinguish authorization,
not-found, and conflict failures without hand-written Fetch calls.
