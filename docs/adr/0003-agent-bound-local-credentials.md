# ADR 0003: Agent-bound local bearer credentials

- Status: accepted
- Date: 2026-08-24

## Context

The localhost-only development API trusts caller-supplied agent IDs. That is
adequate for the storage and delivery spike but cannot safely run remote workers
or distinguish a compromised adapter from the operator.

## Decision

The first-run daemon creates an operator credential in a file readable only by
its operating-system user. That file is authoritative for the local node; its
digest is reconciled transactionally at startup. Reconciliation never
resurrects: an unchanged file is a no-op, a new digest revokes every prior
operator credential and adopts the file, and a file whose digest matches a
revoked credential blocks startup until it is deleted. Replacing or deleting
the file is the operator rotation procedure; deletion provisions a fresh
credential on the next start. Multiple daemons may share one database only
through the same token file; distinct files take authority last-writer-wins on
restart. Registering an agent returns one 256-bit random bearer credential
exactly once. The database stores only a SHA-256 digest and credential
metadata.

Operator credentials manage agents, membership, and policy. Agent credentials
may claim and settle only their own inbox and may send only as their bound agent
identity. The authenticated identity replaces, rather than merely checks, any
caller-supplied sender identity. Credentials can be rotated and revoked without
changing the stable agent ID.

Loopback HTTP is enforced. TLS and machine-to-machine enrollment are separate
deployment decisions that must precede remote workers.

Legacy databases receive the credential schema through a forward migration and
securely provision the operator token on their next daemon start. There is no
unauthenticated compatibility mode.

## Verified properties

- Tokens never appear in logs, list responses, or persisted plaintext.
- Cross-agent claim, settlement, and sender spoofing fail in integration tests.
- Direct messages surface in history and streams only to their sender,
  recipient, and operator scope.
- Revocation takes effect on the next request.
- Operator revocation is permanent: a restored file holding a revoked digest
  fails startup instead of reactivating the credential.
- A lost create response does not silently create an unrecoverable identity;
  the operator can rotate its credential.
- Existing unauthenticated databases migrate without an unauthenticated window.
