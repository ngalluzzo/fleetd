# ADR 0022: Browser channel streams redeem single-use grants

- Status: accepted
- Date: 2026-08-25

## Context

Fleetd's channel WebSocket authenticates the upgrade with the same bearer
header as every other protected API operation. Native clients can set that
header, but the browser `WebSocket` constructor accepts only a URL and optional
subprotocol names. Putting the Fleetd bearer in the URL would expose a
long-lived principal credential to browser history, diagnostics, logs, and
intermediaries. Replacing the stream with polling would weaken the product
contract rather than solve authentication.

Cookies would introduce ambient authority, cross-site request-forgery state,
and ambiguity when one surface deliberately composes an operator viewer with a
human participant sender. Encoding a credential in
`Sec-WebSocket-Protocol` would misuse protocol negotiation and cause the value
to be reflected in the successful upgrade. Sending the long-lived Fleetd
bearer as the first application frame would unnecessarily expose full
principal authority beyond the authenticated HTTP API.

## Decision

Fleetd will add an ephemeral browser stream grant. An authenticated principal
mints the grant for one exact channel, exclusive replay cursor, and browser
stream protocol. The response returns 256 bits of random bearer entropy once,
with `Cache-Control: no-store`. Fleetd stores only its digest and exact scope in
process memory.

The grant:

- expires 15 seconds after issuance using a monotonic enforcement deadline;
- is bound to the issuing credential ID and principal visibility;
- is bound to one channel, cursor, protocol, and daemon process;
- can be redeemed exactly once through an atomic remove-before-use operation;
- is never accepted in a URI, cookie, HTTP authorization header, or WebSocket
  subprotocol; and
- is neither persisted nor recoverable after daemon restart.

The browser connects to a dedicated unauthenticated-upgrade path using the
constant subprotocol `fleetd.channel-stream.browser.v1`. That path accepts only
the canonical origin derived from the daemon's exact loopback listen address,
scheme, and port. The canonical host is a loopback IP literal rather than a
caller-supplied hostname alias. The path rejects missing, `null`, unexpected,
or DNS-rebound origins before upgrading.

After upgrade, Fleetd releases no application data. Within five seconds the
client must send one bounded text frame containing only the grant redemption
request. Fleetd atomically consumes the grant, revalidates that the issuing
credential is still active, and rechecks channel access. Invalid, expired,
already redeemed, revoked, or mismatched grants all fail without disclosing
which check rejected them.

After successful redemption, Fleetd subscribes to live notifications before
reading durable history, sends one `ready` frame, replays every visible message
with `seq > after`, and continues live. Broadcast lag re-enters durable replay.
Every message is wrapped in the browser stream's tagged server-frame contract;
the underlying immutable envelope is unchanged.

An active browser stream revalidates its issuing credential before releasing a
new application message and at least every 30 seconds while idle. Revocation
closes the connection with no further message delivery. Membership is already
permanent for a channel's lifetime, but access is rechecked during redemption.

Unauthenticated connections, unused grants per credential, total unused grants,
frame sizes, authentication time, send time, and socket buffering are bounded.
A slow authenticated consumer is disconnected rather than allowed to create
unbounded memory. It reconnects with a newly minted grant and its last accepted
cursor.

The existing bearer-authenticated WebSocket remains the native/TUI transport.
Both entry paths call the same principal-relative replay/live implementation.
Neither path grants message-send authority over the socket.

The exact stable wire contract is
[`browser-channel-stream-v1.md`](../contracts/browser-channel-stream-v1.md).

## Security boundary

Origin validation is required but is not authentication. The grant proves that
an already authenticated principal authorized this one connection. The grant
is deliberately attenuated relative to that credential and is useful only for
reading one channel from one cursor during a short redemption window.

The loopback-only deployment constraint remains. The grant does not make
plaintext loopback HTTP appropriate for a remote machine. Remote browser or
worker access still requires TLS, endpoint authentication, and enrollment.

Malicious code or processes running as the same operating-system user remain
outside Fleetd's isolation boundary. The design does reduce exposure through
URLs, ambient cookies, cross-origin browser access, replay, persistence, and
ordinary request logging.

## Rejected alternatives

- **Poll channel history:** does not implement a live subscription and hides a
  missing transport capability in presentation code.
- **Bearer or grant in the URL:** secrets leak through commonly retained URI
  surfaces.
- **Credential in `Sec-WebSocket-Protocol`:** negotiation values are not
  credential fields and the selected value is echoed by the server.
- **Long-lived bearer in the first frame:** grants the socket the complete
  reusable principal credential rather than an attenuated one-time authority.
- **Authentication cookie:** creates ambient authority and CSRF/session-lifetime
  machinery while making dual-principal surfaces harder to reason about.
- **Browser-only fetch stream:** duplicates the already qualified WebSocket
  replay/live transport instead of adapting its authentication edge.

## Consequences

The browser path adds an in-memory grant broker and one small pre-authentication
state machine, but no migration or durable authority. Browser and native
clients receive the same messages and visibility after authentication.

The browser transport is available through the stable client after the grant
broker, origin policy, wire contract, revocation behavior, and real-browser
matrix passed. There is no polling fallback.

## Qualification

The complete automated matrix and integrated actual-client WebKit run passed
at Fleetd revision `e0a2798`. The WebKit run covers grant linkage, replay, live
continuation, exact CSP, canonical-origin success, hostname-alias and foreign-
origin rejection, secret surfaces, and absence of HTTP history polling. Its
exact runtime versions, observed result, and browser-API limits are recorded in
[`browser-channel-stream-webview-2026-08-25.md`](../qualification/browser-channel-stream-webview-2026-08-25.md).

## Standards basis

The browser WebSocket API exposes only the connection URL and optional
subprotocol names, while the underlying protocol permits non-browser clients
to use ordinary HTTP authentication headers. WebSocket servers intended for
specific browser origins are expected to validate `Origin`. Bearer credentials
in URI query parameters are discouraged because URIs are commonly retained in
logs and history.

- [WHATWG WebSockets Standard](https://websockets.spec.whatwg.org/)
- [RFC 6455: The WebSocket Protocol](https://www.rfc-editor.org/rfc/rfc6455.html)
- [RFC 6750: Bearer Token Usage](https://www.rfc-editor.org/rfc/rfc6750.html)
