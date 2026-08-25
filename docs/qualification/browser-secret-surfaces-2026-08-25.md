# Browser stream secret-surface qualification — 2026-08-25

## Scope

This qualification exercises the implemented browser stream edge through its
public loopback HTTP and WebSocket transports. It covers the exact raw operator
credential, agent credential, and single-use stream grant produced by the test
daemon. It does not infer safety from source text.

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
  field.

The embedded operator assets are also checked for `Cache-Control: no-store`
and `Referrer-Policy: no-referrer` in addition to their existing content
security policy.

## Evidence

The executable evidence lives in:

- `tests/browser_stream.rs`;
- `tests/openapi.rs`; and
- `tests/operator_web.rs`.

The SQLite check queries every value of every user table through a separate
read-only connection. Assertions deliberately report only the affected surface
class; failure output does not echo the secret or offending stored value.

## Residual gap

This slice does **not** qualify runtime logs. The in-process server used here
does not install and capture the production launch environment's complete
subscriber and HTTP instrumentation stack. A source-text scan would not prove
the absence of runtime disclosure, so browser-stream qualification matrix item
8 remains open until a black-box daemon run captures every configured log sink
and checks the exact issued secrets against those bytes.
