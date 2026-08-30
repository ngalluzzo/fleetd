# DeepSeek Harness plugin contract checkpoint — 2026-08-29

## Scope

This checkpoint records the upstream contract used to add the official
`fleetd.harness.deepseek` vendor plugin, the executable-shaped Fleetd tests,
and the first real direct-provider and sandboxed-tool qualification through
Fleetd.

## Upstream observed

- repository: `deepseek-ai/deepseek-harness`
- source tag: `dsh-v0.1.2-alpha.1`
- source commit: `cd5ef8148158c3a752a658978873241fdf8e2bbc`
- CLI package version: `0.1.2-alpha.1`
- ACP package version: `0.1.2-alpha.1`
- ACP `agentInfo`: `deepseek-harness-acp` version `0.0.1`
- ACP SDK dependency: `@agentclientprotocol/sdk` version `1.4.0`

The tagged source documents `dsh --profile acp` as its automation-only stdio
server. It creates persistent sessions, resumes them without replay, emits
ordered standard ACP updates, accepts cancellation and permission responses,
and accepts Streamable HTTP MCP servers. The shipped profile selects the
official DeepSeek provider and `deepseek-v4-flash` by default.

The shipped base also mounts DSH's settings and credential services. Provider
routes are configured under `llm-pi-ai`, credentials resolve per request from
the private DSH home, and the tagged `dsh-llm-pi-ai` package requires
`@earendil-works/pi-ai ^0.84.2`. Provider/model selection and credential
ownership are therefore native DSH mechanisms rather than an inference service
Fleetd must reproduce.

The same contract explicitly omits `session/load`, transcript replay, and
additional workspace directories. Fleetd therefore advertises
`fleetd.harness-acp@0.1.0` for this runtime and does not advertise `@0.2.0`,
whose only addition is transcript retrieval through `session/load`.

At the time the adapter was started, npm's `latest` tag for `@deepseek-ai/dsh` was
`0.1.1-rc.2`. That published CLI did not package the newer ACP application
profile, while the tagged `0.1.2-alpha.1` source required Node `^22.19.0` or
`>=24.0.0`. The live qualification below therefore used a source-built,
content-addressed runtime at the pinned tag rather than the older published
CLI.

## Fleetd launch policy

- plugin identity: `fleetd.harness.deepseek` version `0.1.0`
- binary: `fleetd-harness-deepseek`
- fixed runtime invocation: `dsh --profile acp`
- fixed permission fallback: `DSH_PERMISSION_MODE=workspace-write`
- explicit environment: `HOME`, `DSH_HOME`, `PATH`, optional `TERM` and
  `TMPDIR`; no ambient variables are inherited
- exactly one model route:
  - a native DSH `provider`/`model` pair, preserving DSH-owned
    `settings.yaml` and `.credentials.yaml`; or
  - a supervisor-injected, credential-free loopback `inference` route with
    exact local reasoning, context, output, and idle-timeout policy
- credentials: DSH's owner-only `$DSH_HOME/.credentials.yaml` in provider
  mode; no API-key field exists in Fleetd's plugin configuration and no raw
  provider key enters the child environment
- compatibility identity: plugin/policy versions, executable path and digest,
  expected ACP version, fixed profile and permission mode, explicit non-secret
  paths, and the selected provider/model or exact injected backend identity

## Executable-shaped result

`cargo test -p fleetd-harness-deepseek` passes against a protocol-pure mock
runtime shaped like the observed DSH ACP server. Eight adapter unit tests and
two executable-shaped process tests prove:

- exact plugin and inner-runtime identity;
- fixed `--profile acp` launch arguments;
- explicit `DSH_HOME` and `workspace-write` policy with no provider key in the
  child environment;
- provider mode preserves DSH-owned settings and credential bytes while
  pinning the selected provider/model in the generated ACP profile;
- local-inference mode disables settings and credentials, rejects remote
  routes, and retains the exact credential-free loopback composition;
- provider mode and local-inference mode are mutually exclusive, and local
  reasoning/token controls are rejected in provider mode instead of being
  guessed for an external dialect;
- truthful negotiation of only `fleetd.harness-acp@0.1.0` when
  `loadSession` is false;
- session creation, thought and assistant updates, terminal evidence, and
  bounded shutdown through the shared ACP host.

## Live direct-provider qualification

The qualification used DSH's own Models settings surface to configure the
installed `zai` catalog route and store its API key in the owner-only private
DSH credential store. Fleetd received only `provider: zai`, `model: glm-5.3`,
and the path to that DSH home. A direct headless control returned the exact
sentinel `GLM53_OK` before Fleetd was introduced.

The first Fleetd message was deliberately not counted: it reused an agent ID
already owned by the running supervisor, so the existing OpenCode generation
won the reservation race. The durable trace named
`fleetd.harness.opencode`, proving that a result string alone is insufficient
qualification evidence and that two worker implementations must not compete
for one agent identity during a harness comparison.

A fresh qualification agent and channel then produced two outcome-known
invocations through a generation whose trace names
`fleetd.harness.deepseek`, runtime `deepseek-harness-acp` `0.0.1`, and profile
digest
`sha256:bbc3979e9d4550a901005d272d194a38b979c8c851b17e92c76de853148aaee1`:

- invocation `1906913a-280e-4fab-a674-12d5dd0e4d2b` returned the exact
  `FLEETD_DSH_GLM53_OK` sentinel with `end_turn`, a quiescent
  runtime-claimed session, and no tool or permission events;
- invocation `3460294e-32d4-46c5-99e1-40955bffc456` created an exact
  `DSH_TOOL_WRITE_OK\n` file inside an isolated workspace under Fleetd's
  write-scoped macOS Seatbelt, returned `DSH_TOOL_WRITE_OK`, and recorded two
  tool events with zero permission events.

This closes the real DSH process, provider-owned model turn, and basic
sandboxed tool-write gates.

The follow-on
[hermetic runtime qualification](dsh-hermetic-runtime-strict-2026-08-29.md)
also closes dependency-closure boot under strict deny-by-default Seatbelt. A
content-addressed runtime completed an exact GLM-5.3 sentinel turn while an
invalid package at a normally discoverable ancestor `node_modules` path was
outside the seat's admitted reads. Remaining gates are native-session adoption
after process replacement and the invocation-scoped HTTP MCP grant. The local
Qwen route has a separate preserved qualification history and remains
available for shared machine inference. Transcript retrieval is not a gate for
this upstream generation because the runtime truthfully does not offer it.
