# OpenCode harness plugin qualification — 2026-08-24

## Scope

This checkpoint qualifies the first vendor-owned harness plugin introduced by
ADR 0011. It covers exact plugin/runtime identity, plugin-owned model routing,
one native session, one real model turn, ordered evidence capture, and graceful
shutdown. It does not yet cover the continuous worker, native resume after
plugin restart, tool permission mediation, or crash ambiguity.

## Observed profile

- fleetd plugin: `fleetd.harness.opencode` version `0.1.0`
- shared ACP host: version `0.1.0`
- ACP SDK: `2.0.0`
- ACP protocol: v1
- OpenCode: `OpenCode 1.4.0`
- OpenCode executable digest:
  `sha256:3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`
- selected route: `zai-coding-plan/glm-5.3`
- effective profile digest:
  `sha256:a9bd188ad0291828d919f9f06b0cd9c0dbe8bda0fdcf40c44265a9d17e652137`

The plugin configuration contained an exact executable/version, a typed model
route, and non-secret process paths. The plugin constructed
`OPENCODE_CONFIG_CONTENT` internally. No arbitrary environment map or provider
credential field crossed the fleetd plugin configuration boundary.

## Observed runtime features

OpenCode reported native session loading, session list/resume/fork extensions,
image and embedded-context prompt support, and HTTP/SSE MCP transports. The
fleetd profile continues to grant no MCP servers and makes no stronger budget
claim from those advertisements.

## Real turn

The plugin created native session `ses_fcaed76fdffea08oPLM6c7yVp2` in the
isolated fleetd UI-probe worktree and sent a no-tool qualification prompt. The
turn produced 45 contiguous events: one preserved unknown update, reasoning
updates, assistant text, and usage. It terminated with:

- stop reason `end_turn`;
- execution certainty `outcome_known`;
- quiescent session with runtime-claimed persistence;
- complete assistant text `fleetd OpenCode plugin qualification passed`;
- prompt-response usage of 9,926 total, 9,751 input, 9 output, 38 thought, and
  128 cached-read tokens.

OpenCode's own authoritative message record identifies the user turn as agent
`build`, provider `zai-coding-plan`, model `glm-5.3`; the assistant record ended
with `finish=stop` and no error. This independently verifies that the typed
plugin model route took effect.

## Result and remaining gates

The vendor-owned plugin boundary passes one real create-and-turn checkpoint.
The preserved unknown first update must not be reclassified without inspecting
its exact shape. The next qualification is the durable continuous-worker loop,
followed by plugin restart/native resume and a second independently versioned
harness plugin. Until then, `harness.acp` v1 remains experimental.
