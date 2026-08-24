# `@fleetd/client`

This directory contains the disposable TypeScript client generated from
`../../openapi/fleetd-v1.json`. Do not edit `src/generated` by hand.

```sh
npm ci
npm run generate
npm run typecheck
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
