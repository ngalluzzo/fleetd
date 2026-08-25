# Browser channel-stream WebView qualification — 2026-08-25

## Scope

This run qualifies the browser-only WebSocket handshake implemented at Fleetd
revision `21c65bb591028e7d415004cb628c676ea051317b`. It uses the checked-in
`tools/qualify-browser-channel-stream.ts` probe and a real page-side
`WebSocket` constructor in Bun's native WebKit-backed `WebView`.

The probe navigates to Fleetd's loopback operator page, opens the fixed browser
stream path with the fixed subprotocol, observes the negotiated browser state,
and closes without redeeming a grant. It transmits no credential or grant.

## Environment

- macOS 26.5.2 on arm64;
- Bun 1.4.0;
- `Bun.WebView` with the explicit `webkit` backend and ephemeral storage; and
- Fleetd bound to `127.0.0.1:17419` over plaintext loopback HTTP.

## Command

With the exact Fleetd revision running on the bound address:

```sh
FLEETD_BROWSER_QUALIFICATION_ORIGIN=http://127.0.0.1:17419 \
  bun tools/qualify-browser-channel-stream.ts
```

Observed result:

```json
{"bun_version":"1.4.0","backend":"webkit","outcome":"open","protocol":"fleetd.channel-stream.browser.v1","url":"ws://127.0.0.1:17419/v1/browser/channel-stream","applicationFrameBeforeRedemption":false}
```

## Finding

The browser constructor successfully negotiated exactly
`fleetd.channel-stream.browser.v1` at the fixed secret-free path. No
application frame arrived during the probe's post-open observation window
before grant redemption.

This closes only the real-browser constructor and protocol-negotiation portion
of the draft qualification matrix. Rust integration tests separately cover the
complete redemption protocol, replay, visibility, revocation, and failure
boundaries.

## Limits

- Bun marks `WebView` experimental, so the exact Bun version is evidence.
- This run exercises macOS WebKit, not Chromium or another browser engine.
- The short pre-redemption observation corroborates the server integration
  test; the socket-bound late-redemption test proves the complete first-frame
  deadline behavior.
- This is an explicit qualification command rather than a mandatory `bin/ci`
  dependency because Bun and macOS WebKit are not universal Rust build inputs.
