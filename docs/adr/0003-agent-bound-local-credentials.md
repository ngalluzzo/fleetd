# ADR 0003: Agent-bound local bearer credentials

- Status: proposed
- Date: 2026-08-24

## Context

The localhost-only development API trusts caller-supplied agent IDs. That is
adequate for the storage and delivery spike but cannot safely run remote workers
or distinguish a compromised adapter from the operator.

## Proposed decision

The first-run daemon creates an operator credential in a file readable only by
its operating-system user. Registering an agent returns one high-entropy bearer
credential exactly once. The database stores only a cryptographic digest and
credential metadata.

Operator credentials manage agents, membership, and policy. Agent credentials
may claim and settle only their own inbox and may send only as their bound agent
identity. The authenticated identity replaces, rather than merely checks, any
caller-supplied sender identity. Credentials can be rotated and revoked without
changing the stable agent ID.

Loopback HTTP remains the default. Listening on a non-loopback interface will be
rejected unless authenticated transport is explicitly configured. TLS and
machine-to-machine enrollment are separate deployment decisions.

## Acceptance criteria

- Tokens never appear in logs, list responses, or persisted plaintext.
- Cross-agent claim, settlement, and sender spoofing fail in integration tests.
- Revocation takes effect on the next request.
- A lost create response does not silently create an unrecoverable identity;
  the operator can rotate its credential.
- Existing unauthenticated databases require an explicit development-mode flag
  until credentials are provisioned.
