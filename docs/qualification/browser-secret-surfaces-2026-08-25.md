# Browser stream secret-surface qualification — 2026-08-25

## Scope

This qualification exercises the implemented browser stream edge through its
public loopback HTTP and WebSocket transports. It covers the exact raw operator
credential, agent credential, and single-use stream grant produced by the test
daemon. It does not infer safety from source text. A separate black-box slice
launches the production binary and captures its complete process output.

The automated slice proves that:

- the long-lived agent bearer is carried by the authenticated grant-issuance
  request's `Authorization` header, not its URL;
- the grant-issuance URL and response headers contain neither raw secret;
- the browser upgrade URI has no query, carries no authorization header, and
  negotiates only the constant protocol name;
- successful upgrade response headers contain neither raw secret;
- the one-time grant crosses the WebSocket boundary only in the bounded
  redemption frame, while the long-lived bearer does not enter that frame;
- public Debug representations of the observed registration, grant response,
  redemption request, authentication service, and ready frame contain neither
  raw secret;
- live SQLite values contain neither the raw text nor raw bytes of the exact
  operator credential, agent credential, or grant; and
- generated and committed OpenAPI documents contain no token-shaped raw
  credential or grant fixture and advertise no secret-bearing browser-upgrade
  field; and
- the production tracing subscriber, configured with the global `trace` filter,
  emits none of the exact generated operator credential, agent credential, or
  redeemed grant to either captured process output stream.

The embedded operator assets are also checked for `Cache-Control: no-store`
and `Referrer-Policy: no-referrer` in addition to their existing content
security policy.

## Evidence

The executable evidence lives in:

- `tests/browser_stream.rs`;
- `tests/browser_runtime_logs.rs`;
- `tests/openapi.rs`; and
- `tests/operator_web.rs`.

The SQLite check queries every value of every user table through a separate
read-only connection. Assertions deliberately report only the affected surface
class; failure output does not echo the secret or offending stored value.

The runtime-log test launches the real `fleetd serve` binary on an
operating-system-selected loopback port, waits for its production readiness
event, and performs public agent registration, channel creation, grant
issuance, browser redemption, and live delivery. It closes the stream, requests
the daemon's normal signal-driven shutdown, and captures every byte written to
both stdout and stderr. Exact-secret checks run only after the process exits;
their failure messages name the surface class without including the secret or
offending log contents. The subscriber permanently disables
`tungstenite::protocol` events because that dependency's protocol-level trace
renders complete application frames, including the redemption credential; the
black-box slice runs every other target at `trace` and verifies the enforced
exclusion through the resulting process bytes.

This closes browser-stream qualification matrix item 8 for the currently
configured production subscriber and process sinks. A future additional log
sink or HTTP instrumentation layer must be added to this black-box capture
before making the same claim for that configuration.
