# Browser channel-stream WebView qualification — 2026-08-25

## Scope

This run qualifies the presentation-agnostic browser channel-stream client at
Fleetd revision `d213a72`. The checked-in
`tools/qualify-browser-channel-stream.ts` runner creates a dedicated agent and
channel through public Fleetd operations, commits one replay fixture, bundles
the exported client, and loads it into Bun's native WebKit-backed `WebView`.

The canonical Fleetd page accepts the replay fixture and one message committed
after `ready`. Separate WebViews then prove that a page loaded through the
`localhost` hostname alias and a page served from another loopback port receive
no application data. Fixture creation and live-message stimulus run outside the
page, so the adapter's recorded operation list remains an exact account of its
own network surface.

## Environment

- macOS 26.5.2 on arm64;
- Bun 1.4.0;
- `Bun.WebView` with the explicit `webkit` backend and a fresh ephemeral data
  store for each origin;
- Fleetd revision `d213a72`; and
- Fleetd bound to `127.0.0.1:17459` over plaintext loopback HTTP.

## Command

With that exact Fleetd revision running on the bound address, the operator
credential is supplied only through the process environment:

```sh
FLEETD_BROWSER_QUALIFICATION_ORIGIN=http://127.0.0.1:17459 \
FLEETD_BROWSER_QUALIFICATION_CREDENTIAL='<redacted>' \
  bun tools/qualify-browser-channel-stream.ts
```

Observed result:

```json
{"bun_version":"1.4.0","backend":"webkit","fixture":{"fresh_channel":true,"replay_messages_accepted":1,"live_messages_accepted":1},"csp":{"exact_policy":true,"same_origin_connect_succeeded":true,"set_cookie_headers":0},"same_origin":{"outcome":"complete","protocol":"fleetd.channel-stream.browser.v1","first_frames":["ready","message","message"],"operations":[{"kind":"fetch","method":"POST","path":"/v1/channels/<channel-id>/stream-grants"},{"kind":"websocket","path":"/v1/browser/channel-stream","protocol":"fleetd.channel-stream.browser.v1"}],"audit":{"no_secret_detected":true,"history_entry_enumeration_authoritative":false,"cookie_setter_instrumented":true,"indexed_db":{"available":true,"authoritative":true,"databases":0},"cache_api":{"available":true,"authoritative":true,"caches":0},"service_workers":{"available":true,"authoritative":true,"registrations":0},"console_calls":0,"page_errors":0,"unhandled_rejections":0}},"hostname_alias":{"page_origin":"http://localhost:17459","socket_opened":false,"application_frames":0},"foreign_origin":{"page_origin":"http://127.0.0.1:50643","adapter_application_frames":0,"direct_socket_opened":false,"direct_application_frames":0}}
```

## Findings

### Actual adapter replay and live continuation

The canonical page was served with Fleetd's exact policy:

```text
default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'
```

Under that CSP, the actual exported adapter performed exactly one authenticated
grant `POST` and one construction of the fixed browser WebSocket. It emitted no
history request or polling fallback. WebKit selected the exact
`fleetd.channel-stream.browser.v1` subprotocol, then delivered `ready`, the
already-committed replay message, and the post-ready live message in that
order. The consumer accepted each message once.

### Origin rejection

The hostname-alias view successfully loaded Fleetd's operator page from
`http://localhost:17459`, minted a grant, and constructed the browser socket.
The socket never opened and delivered zero application frames because the
server requires its canonical `127.0.0.1` origin and authority.

The genuinely foreign view loaded from a second `127.0.0.1` port. Its actual
adapter attempted grant linkage but did not fall back to history or construct a
socket after the cross-origin Fetch failed. In parallel, that page used a real
browser `WebSocket` with a valid host-minted grant; the foreign Origin was
rejected before the socket opened or any application frame arrived.

### Secret surfaces

Each view held its exact credential and grant only in JavaScript memory. The
runner emits only booleans, counts, fixed paths, and protocol names. It never
prints request headers, bodies, evaluation source, credentials, or grants, and
raw WebView evaluation causes are deliberately suppressed because bootstrap
source contains the in-memory values.

Before starting the client, the page instrumented six console methods, both
History mutation methods, all three Storage mutation methods, the cookie
setter, IndexedDB open/delete, Cache API open/match/delete, service-worker
registration, `error`, and `unhandledrejection`. After acceptance it compared
the exact credential and observed grant against:

- current location, referrer, History state, and every observed History
  mutation argument;
- current cookies and every observed cookie write;
- all final local/session Storage keys and values plus mutation arguments;
- enumerated IndexedDB databases;
- every Cache API request URL/header and cached response header/body;
- every service-worker registration scope and script URL;
- requested and selected WebSocket subprotocols and the socket URL; and
- captured console arguments, page errors, and rejection reasons.

No exact secret was found. The fresh ephemeral stores contained zero IndexedDB
databases, caches, or service-worker registrations. The runner also observed no
`Set-Cookie` header on its exact Fleetd HTTP responses, no console calls, no
page errors, and no unhandled rejections.

## Limits

- Bun marks `WebView` experimental, so the exact Bun version is evidence.
- This run exercises macOS WebKit, not Chromium or another browser engine.
- WebKit exposes the current location and History state but no API for
  enumerating complete back/forward entry URLs. The runner therefore proves an
  unchanged location, no History mutation calls, and no secret in current
  state, but reports complete history-entry enumeration as non-authoritative.
- Browser JavaScript cannot read `HttpOnly` cookies or `Set-Cookie` response
  headers. Cookie setter/current-cookie inspection is paired with host-side
  verification that the exact Fleetd fixture, page, and grant responses set no
  cookie.
- Because the ephemeral IndexedDB catalog was empty, authoritative database
  enumeration was sufficient; the runner does not claim a general-purpose scan
  of arbitrary pre-existing IndexedDB values.
- Deterministic adapter tests separately cover every prior reconnect cursor,
  stable duplicate tolerance and conflicts, serialized consumer acceptance,
  cursor advancement after acceptance, ready/reconnect bounds, queue-overflow
  replay, and terminal grant-linkage failure without polling.
- Rust integration tests remain the authority for cross-principal visibility,
  revocation, daemon restart, global capacity, and server send deadlines.
- This is an explicit qualification rather than a mandatory `bin/ci` step
  because Bun and macOS WebKit are not universal Rust build inputs.
