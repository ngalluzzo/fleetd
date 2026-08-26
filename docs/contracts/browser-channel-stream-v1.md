# Browser channel stream v1

This contract specifies the browser-compatible authentication edge for
Fleetd's existing durable channel stream. [ADR 0022](../adr/0022-browser-channel-stream-grants.md)
accepts the design. The complete automated matrix and the actual-client WebKit
qualification passed at Fleetd revision `e0a2798`; the reproducible browser
evidence is recorded in
[`browser-channel-stream-webview-2026-08-25.md`](../qualification/browser-channel-stream-webview-2026-08-25.md).

## Constants and bounds

| Property | Value |
| --- | --- |
| Protocol | `fleetd.channel-stream.browser.v1` |
| Grant entropy | 256 random bits, base64url without padding |
| Grant prefix | `fl_sg_` |
| Grant redemption lifetime | 15 seconds |
| First-frame deadline | 5 seconds after upgrade |
| Maximum redemption frame | 1,024 UTF-8 bytes |
| Maximum unused grants per credential | 8 |
| Maximum unused grants per daemon | 1,024 |
| Maximum pre-authentication sockets per daemon | 64 |
| Maximum active browser streams per credential | 16 |
| Maximum active browser streams per daemon | 1,024 |
| Idle credential revalidation | at most 30 seconds |
| Application-frame send deadline | 10 seconds |

Implementations enforce timeouts with monotonic time. Returned wall-clock
timestamps are informational.

## 1. Grant issuance

```http
POST /v1/channels/{channel_id}/stream-grants
Authorization: Bearer <operator-or-member-token>
Content-Type: application/json
```

```json
{
  "after": 42,
  "protocol": "fleetd.channel-stream.browser.v1"
}
```

`after` is an exclusive non-negative global message cursor. Unknown fields,
another protocol value, an unknown channel, or insufficient channel access are
rejected. The ordinary bearer middleware authenticates this request. Operators
may mint a stream for any channel; agents must be channel members. Operator and
member streams replay the same complete channel log.

Success returns `201 Created` and `Cache-Control: no-store`:

```json
{
  "grant": "fl_sg_<base64url-entropy>",
  "expires_at_ms": 1787666400000,
  "websocket_path": "/v1/browser/channel-stream",
  "protocol": "fleetd.channel-stream.browser.v1"
}
```

The raw grant is returned once and must never be logged. Fleetd retains only a
SHA-256 digest, monotonic expiry, issuing credential ID, principal kind and
optional agent ID, channel ID, cursor, and exact protocol. Grant state is
process-local and non-durable.

Issuance fails with `429 Too Many Requests` when either unused-grant bound is
reached. Expired grants are pruned before capacity is evaluated. Errors never
include an existing or newly generated grant.

## 2. Browser upgrade

The client constructs:

```js
new WebSocket(
  "ws://127.0.0.1:<port>/v1/browser/channel-stream",
  "fleetd.channel-stream.browser.v1"
)
```

The grant, channel, and cursor do not appear in the URI or subprotocol. The
endpoint is outside Fleetd's ordinary bearer middleware because browser code
cannot set that header.

Before returning `101 Switching Protocols`, Fleetd requires:

- one canonical browser origin derived from the daemon's configured loopback IP
  listen address, scheme, and port, exactly matching the `Origin` header;
- a `Host` authority exactly matching that configured origin;
- no missing or `null` origin;
- the exact requested subprotocol; and
- normal WebSocket upgrade validity.

An origin or authority mismatch returns HTTP `403` without upgrading. Fleetd
does not reflect a caller-derived origin and does not accept `localhost`, a DNS
alias, or a wildcard in place of the canonical loopback IP literal. The daemon
advertises the canonical browser URL it derived at startup; browser clients
must load the embedded surface from that exact origin. The successful response
selects only the constant protocol name.
The upgrade returns `503 Service Unavailable` before switching protocols when
the pre-authentication or total active-stream bound is exhausted.

## 3. Grant redemption

Fleetd sends no application frame immediately after upgrade. The first complete
client message must arrive within five seconds, be a text message no larger
than 1,024 UTF-8 bytes, and decode exactly as:

```json
{
  "type": "redeem",
  "grant": "fl_sg_<base64url-entropy>"
}
```

Unknown fields, binary data, multiple messages, invalid UTF-8, malformed JSON,
or an invalid token shape fail the handshake. Fragment reassembly must not
permit the complete message to exceed the same bound.

Redemption performs one atomic remove by grant digest. The removed record is
not restored when a later check fails. Fleetd then verifies monotonic expiry,
exact protocol, active issuing credential ID and principal invariant, channel
existence, and principal channel access. No client-supplied scope is accepted
at redemption. Redemption also reserves one credential-scoped active-stream
slot; exhaustion consumes the grant and closes without establishing authority.

Before successful redemption Fleetd emits no application data. It closes with
one of these fixed private-use codes and reasons:

| Code | Reason | Meaning |
| ---: | --- | --- |
| 4400 | `invalid_handshake` | malformed or unsupported first message |
| 4401 | `grant_rejected` | missing, expired, reused, revoked, or invalid grant |
| 4408 | `grant_timeout` | first message deadline elapsed |

The rejection class intentionally does not reveal which grant check failed.

## 4. Ready, replay, and live frames

After redemption, Fleetd creates the in-memory channel subscription before
reading history, then sends:

```json
{
  "type": "ready",
  "protocol": "fleetd.channel-stream.browser.v1",
  "channel_id": "channel-uuid",
  "after": 42
}
```

Every replayed or live message uses:

```json
{
  "type": "message",
  "message": {
    "seq": 43,
    "id": "message-uuid",
    "channel_id": "channel-uuid",
    "sender_id": "agent-uuid",
    "recipient_id": null,
    "kind": "unknown-contract/v7",
    "payload": {"extension": "preserved"},
    "correlation_id": null,
    "causation_id": null,
    "created_at_ms": 1787666400001
  }
}
```

Messages are ordered by ascending `seq` and preserve every field of the exact
immutable Fleetd envelope. The envelope itself is closed: Fleetd rejects
unknown top-level message fields instead of silently discarding or assigning
semantics to them. Ecosystem contracts extend messages through the versioned
`kind` and opaque `payload`; Fleetd stores and forwards both unchanged. Replay
emits every channel message with `seq > after` in bounded pages. The
live receiver then continues from the last emitted cursor. Broadcast lag
returns to durable replay rather than skipping records.

Client application messages after redemption are unsupported. Close frames
are honored; other data closes with `4400 invalid_handshake`. Protocol ping and
pong frames may be used for connection liveness and are not application data.

Before emitting a new message and at least every 30 seconds while idle, Fleetd
checks that the issuing credential ID remains active with the same principal.
Revocation closes the stream with `4401 grant_rejected` before another message
is released. Active-stream slots are released on every close path. Internal
failures after readiness close with WebSocket code 1011.

Each application send has a ten-second deadline. A slow or disconnected client
is closed; Fleetd does not buffer an unbounded private queue and does not mutate
the durable cursor on behalf of the client.

## 5. Reconnection

The client retains the highest message sequence it has accepted. After any
disconnect it mints a new grant with that value as `after`, opens a new socket,
and redeems the new grant. A grant is never retried.

If the client crashes before retaining its cursor, it may reconnect with an
earlier cursor. It must deduplicate by `seq` and stable message ID. If the
daemon restarts, every outstanding grant disappears and the client mints a new
one using its long-lived Fleetd credential.

## 6. Client secret handling

Browser clients keep the Fleetd credential and raw grant only in JavaScript
memory. They must not use query strings, fragments, cookies, Web Storage,
IndexedDB, service-worker caches, analytics, exception payloads, or console
logging for either value. The grant variable is discarded immediately after
the redemption frame is sent.

Fleetd's embedded surface continues to prohibit third-party scripts, inline
scripts, framing, referrers, and storage-backed credential helpers. The mint
response and every static asset carrying authentication code use
`Cache-Control: no-store`.

## 7. Qualification matrix

The protocol is not complete until automated tests prove:

1. operator and member issuance replay the same complete channel log;
2. a real browser `WebSocket` constructor negotiates the constant protocol;
3. no application frame is emitted before successful redemption;
4. missing, malformed, oversized, late, expired, reused, and concurrent
   redemption attempts fail closed, with exactly one concurrent winner;
5. revocation between issuance and redemption fails;
6. revocation after readiness closes before later messages and within the idle
   revalidation bound;
7. wrong, missing, `null`, and DNS-rebound origins fail before upgrade;
8. the raw bearer and grant appear in no URL, subprotocol, response header,
   log, SQLite value, or generated contract fixture;
9. a message committed between issuance, redemption, subscription, and replay
   is delivered exactly through replay or live continuation;
10. broadcast lag and send backpressure close or replay without a silent gap;
11. reconnect from every prior cursor is ordered and duplicate-tolerant;
12. addressed messages remain visible to every channel member while a
    non-member cannot mint or redeem that channel's stream;
13. unknown kinds and opaque payloads survive unchanged, while unknown
    top-level envelope fields fail closed;
14. daemon restart invalidates unused grants without affecting durable replay;
15. native bearer and browser-grant streams produce equivalent visible message
    sequences for the same principal and cursor; and
16. unused-grant, pre-authentication socket, per-credential stream, and global
    stream bounds reject excess work and release capacity after expiry or
    close; and
17. no browser implementation falls back to polling when grant linkage is
    unavailable.
