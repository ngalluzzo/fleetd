# ADR 0011: Harness launch policy belongs to vendor-owned plugins

- Status: accepted
- Date: 2026-08-24

## Context

A shared ACP transport does not make harness launch policy universal. OpenCode,
Codex, DSH, and future runtimes have different executable discovery, arguments,
environment variables, model routes, session roots, and compatibility rules.
Putting those branches in the worker would turn Fleetd into a vendor switch.

## Decision

Each harness integration is a separately identified plugin executable. The
shared `fleetd-acp-host` library owns ACP translation and process containment.
Each vendor plugin owns:

- its Fleetd plugin ID and version;
- its strict configuration schema and allowed environment names;
- native executable/version validation and launch arguments;
- model and backend routing;
- effective profile and executable digests;
- native session-compatibility policy.

Every ACP-backed vendor plugin negotiates the same exact operational interface,
`fleetd.harness-acp@0.1.0`. The worker depends only on that interface and
contains no OpenCode, Codex, DSH, or model-vendor branches.

The OpenCode plugin builds its native configuration internally and can expose a
credential-free loopback OpenAI-compatible model route. That route and plugin
policy version participate in the profile digest. Other vendor plugins retain
their own schemas instead of accepting a generic environment map.

## Consequences

Adding or updating a harness means adding or updating a plugin package, not
editing the worker. Configuration-edge repetition is intentional: shared
transport code cannot erase real vendor policy differences. OpenCode is the
first production-shaped proof; Codex requires independent real-runtime
qualification before the authoring surface is stable.
