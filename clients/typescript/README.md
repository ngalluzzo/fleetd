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
import { client } from '@fleetd/client/client';

client.setConfig({
  baseUrl: 'http://127.0.0.1:4317',
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
import { openBrowserChannelStream } from '@fleetd/client';

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
