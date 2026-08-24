# Capability work request v1

Status: experimental cross-project dogfood contract

This contract binds one missing semantic capability to exact input facts. It is
carried as an immutable fleetd message without adding capability meaning to the
message kernel.

## Request envelope

- message kind: `work.capability.request/v1`
- recipient: the selected agent identity
- sender: supplied by the authenticated fleetd principal, never by the payload
- correlation ID: exactly the request `request_id`
- idempotency key: by default `capability-request/{request_id}`

The payload is:

```json
{
  "request_id": "sha256:...",
  "capability": {
    "package": "dev.fleetd.capability",
    "name": "generate_runnable_web_surface",
    "version": "0.1.0"
  },
  "requires": [
    {
      "fact": {
        "package": "org.gooi.target.web",
        "name": "fleetd_blocked_delivery",
        "version": "0.1.0"
      },
      "acceptance": "complete_only"
    }
  ],
  "inputs": [
    {
      "id": "sha256:...",
      "fact_type": {
        "package": "org.gooi.target.web",
        "name": "fleetd_blocked_delivery",
        "version": "0.1.0"
      },
      "coverage": "complete",
      "payload": {},
      "derivation": {}
    }
  ],
  "produces": [
    {
      "package": "org.gooi.artifact.web",
      "name": "runnable_fleetd_surface",
      "version": "0.1.0"
    }
  ],
  "conformance_suite": "dev.fleetd.conformance.runnable_web_surface@0.1.0"
}
```

Identity package, name, and version parts are exact and never range-matched.
The required and bound-input fact sets must be equal. Duplicate inputs or
outputs are rejected. A `complete_only` requirement rejects a partial bound
fact before any harness effect is armed.

`request_id` is lower-case `sha256:` over the RFC 8785 JSON Canonicalization
Scheme bytes of the payload body excluding `request_id`. This binds capability,
requirements, exact fact instances, expected outputs, and conformance suite
independently of ordinary JSON key order or whitespace.

The input fact ID and derivation are claims made by the producing semantic
system. fleetd preserves and binds them but does not reinterpret or admit them
as trusted. Provider conformance remains a later acceptance step.

## Submission and execution

Submit a request emitted by GOOIR:

```sh
cargo run -- --token-file .fleetd/requester.token work submit \
  --channel CHANNEL_ID --to PROVIDER_AGENT_ID \
  --request /path/to/runnable-web-request.json
```

The capability worker configuration names the exact capabilities that seat may
attempt. The adapter rejects any other capability before arming dispatch. A
valid request receives a `per-work-contract` session lane keyed by
`request_id`; the existing invocation and session tables durably bind the
immutable request message to binding generation and owner epoch.

The first response kind is `work.capability.attempt/v1`. It is harness terminal
evidence correlated to the request, not an accepted semantic output. The agent
is instructed to return candidate produced facts and to mark conformance
`unverified`. fleetd does not yet extract those candidates or run the named
suite.

## Deliberate omissions

- No capability broker chooses a recipient; submission selects one exact
  agent.
- Agent eligibility is configured at the worker adapter and is not proof of
  conformance.
- No arbitrary validation commands or generic plugin execution method appear
  in the contract.
- Progress, dependency, review, approval, and accepted-result contracts remain
  separate M2 work.
- A provider-specific protocol such as ACP or a vendor harness is not part of
  the request meaning.
