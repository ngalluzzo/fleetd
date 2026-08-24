# ACP driver qualification checkpoint — 2026-08-24

This record captures observed behavior of the experimental `harness.acp` v1
slice. It is not a declaration that the complete acceptance matrix passes.

## Driver under test

- fleetd ACP driver: workspace version `0.1.0`
- inner client: official Rust `agent-client-protocol` SDK `2.0.0`, pinned exactly
- ACP protocol selected: stable v1
- outer frame limit: 1 MiB
- captured turn-output limit: 512 KiB
- environment: cleared at the fleetd boundary, then reconstructed from the
  driver's explicit non-secret allowlist

## Results

| Runtime | Exact observed identity | Initialize | Session | Turn | Status |
| --- | --- | --- | --- | --- | --- |
| Test fixture | `mock-acp` `1.0.0` | Pass | Pass | Pass | Full driver integration fixture |
| Codex adapter | `@agentclientprotocol/codex-acp` `1.6.2` | Pass | Pass | Pass | One real no-tool prompt completed |
| DSH adapter | `dsh-acp` `0.4.22` | Pass | Blocked | Not run | Local runtime requires authentication |

The Codex adapter script digest was
`sha256:8c7fc8af156596668a95ce23d52309f70ad576e75bac6dc209d30378bdbb8ebe`.
The prompt `Reply exactly: fleetd-codex-ok. Do not use tools.` produced eleven
ordered updates, assistant text `fleetd-codex-ok`, stop reason `end_turn`,
`outcome_known`, a quiescent session, runtime-claimed persistence, and preserved
raw usage evidence.

The DSH adapter script digest was
`sha256:5ba26ceb1816bf4ecae17084a162db06b457e2c77efbd6a0f4f564f2694d4a42`.
Initialization returned the expected identity and capabilities. Session
creation failed closed with the adapter's authentication-required error. No raw
credential was read, copied, logged, or added to the profile to bypass that
boundary. DSH session, prompt, cancellation, and resume cases therefore remain
unqualified.

## Automated evidence

- The typed host test covers exact capability negotiation, create/turn/close,
  unknown-extension preservation, stale-fence rejection, local effect-boundary
  validation, and notification-overflow failure.
- The driver integration test exercises the official SDK against a real ACP
  child process and preserves raw initialize, session, update, and prompt
  extension fields.
- The managed-controller test exercises reserve, durable arm, typed turn,
  correlated atomic result append, and input acknowledgement. Its failure case
  proves a post-arm stale fence parks the delivery.
- The lifecycle test proves dropping a driver owner kills a spawned descendant
  in the same process group.

## Reproduction

Build the driver, then invoke the qualification example with exact local
adapter paths and versions:

```sh
cargo build -p fleetd-acp-driver
cargo run -p fleetd-acp-driver --example qualify -- \
  /absolute/path/to/target/debug/fleetd-acp-driver \
  /absolute/path/to/node \
  /absolute/path/to/acp-adapter.js \
  EXPECTED_AGENT_NAME EXPECTED_AGENT_VERSION \
  /absolute/working/directory \
  '["optional", "adapter", "arguments"]' \
  'Reply exactly: qualification-ok.'
```

Omit the final prompt to stop after session creation. The example passes only
allowlisted environment settings; it does not accept raw credentials on its
command line.

## Remaining common matrix

Neither adapter has yet passed restart/resume, tool-active cancellation,
pre-write and post-acceptance crash injection, generation replacement, or the
complete credential-leak suite through this driver. DSH must additionally pass
session creation and a real turn through an operator-approved credential or
gateway profile. Until then, capability version `1` is negotiable for
experimentation but not stable.
