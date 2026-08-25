# Browser channel-stream WebView qualification — 2026-08-25

## Scope

This run qualifies the presentation-agnostic browser channel-stream client at
Fleetd revision `2575a12`. The checked-in
`tools/qualify-browser-channel-stream.ts` probe bundles the exported client,
loads it into Bun's native WebKit-backed `WebView`, and exercises its real
grant-to-ready path with a page-side `WebSocket` constructor.

The client mints a single-use grant with an in-memory operator credential,
opens the fixed browser stream path with the fixed subprotocol, redeems the
grant, validates the exact `ready` linkage, and closes. The probe records only
operation method/path metadata; it never records headers, request bodies, the
credential, or the grant.

## Environment

- macOS 26.5.2 on arm64;
- Bun 1.4.0;
- `Bun.WebView` with the explicit `webkit` backend and ephemeral storage; and
- Fleetd bound to `127.0.0.1:17429` over plaintext loopback HTTP.

## Command

With the exact Fleetd revision running on the bound address and the values kept
only in the command environment:

```sh
FLEETD_BROWSER_QUALIFICATION_ORIGIN=http://127.0.0.1:17429 \
FLEETD_BROWSER_QUALIFICATION_CREDENTIAL='<redacted>' \
FLEETD_BROWSER_QUALIFICATION_CHANNEL_ID='<redacted>' \
  bun tools/qualify-browser-channel-stream.ts
```

Observed result, with the non-secret channel identifier redacted from the
checked-in artifact:

```json
{"bun_version":"1.4.0","backend":"webkit","outcome":"ready","protocol":"fleetd.channel-stream.browser.v1","url":"ws://127.0.0.1:17429/v1/browser/channel-stream","firstApplicationFrameType":"ready","acceptedMessages":0,"operations":[{"kind":"fetch","method":"POST","path":"/v1/channels/<channel-id>/stream-grants"},{"kind":"websocket","path":"/v1/browser/channel-stream","protocol":"fleetd.channel-stream.browser.v1"}]}
```

## Finding

The actual exported adapter completed the authenticated grant → exact browser
WebSocket → redemption → `ready` sequence in WebKit. Its observed network
surface contained exactly one grant `POST` and one WebSocket construction. It
made no channel-history request and therefore did not substitute HTTP polling
for failed or missing grant linkage.

The negotiated URL contained neither credential nor grant. The first server
application frame was `ready`, the exact constant subprotocol was selected, and
no conversation message was presented to the consumer during this empty-log
run. The bootstrap's raw WebView evaluation errors are deliberately suppressed
because the evaluated source carries the in-memory credential.

## Limits

- Bun marks `WebView` experimental, so the exact Bun version is evidence.
- This run exercises macOS WebKit, not Chromium or another browser engine.
- This run proves the real adapter's grant-to-ready linkage and absence of a
  polling fallback. It does not prove replay/live message acceptance in WebKit
  because the qualified channel contained no messages.
- Deterministic adapter tests separately prove serialized consumer acceptance,
  cursor advancement after acceptance, every prior reconnect cursor, stable
  duplicate tolerance, conflict rejection, bounded ready/reconnect behavior,
  and queue-overflow replay.
- Rust integration tests separately prove server-side replay/live parity,
  principal visibility, revocation, restart, and failure boundaries.
- This remains an explicit qualification command rather than a mandatory
  `bin/ci` dependency because Bun and macOS WebKit are not universal Rust build
  inputs.
