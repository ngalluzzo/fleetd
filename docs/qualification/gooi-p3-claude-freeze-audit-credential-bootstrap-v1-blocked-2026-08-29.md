# GOOI P3 Claude credential bootstrap v1 — ACP blocked before dispatch

Date: 2026-08-29

Verdict: `BLOCKED_ACP_SESSION_OPEN`. The least-privilege credential bootstrap made Claude Code report an authenticated first-party subscription, but the same strict Fleetd route still failed while opening the ACP session. No model prompt was armed, no audit result exists, and this is not semantic evidence for or against P3 freeze.

## Least-privilege bootstrap

A fresh qualification-private HOME was created. It received exactly:

1. a root metadata object containing only `hasCompletedOnboarding` and `oauthAccount`;
2. one exact owner-only Claude credential file copied into the qualification-private Claude credential path.

The credential is deliberately described only by permitted metadata:

- source path class: `ambient_claude_credentials`;
- destination path class: `qualification_private_home_credentials`;
- bytes: 501;
- mode: `0600`;
- sole top-level key name: `claudeAiOauth`.

Credential contents were not printed, parsed, hashed, placed in reports, or persisted after the worker stopped. Projects, caches, settings, history, and other ambient state were not copied. The value-free bootstrap schema is retained at `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-credential-bootstrap-v1-2026-08-29/evidence/bootstrap-safe-schema.json`.

The only direct Claude command was the authorized `auth status` probe under that private HOME. It passed:

```text
exit_code=0 loggedIn=true authMethod=claude.ai subscriptionClassification=firstParty
```

No raw auth response was retained.

## Fresh Fleetd audit route

The passed gate authorized a fresh Fleetd route:

- auditor agent: `4c118dca-6f90-43d0-9bb5-fa28bba64d12` (`gooi-p3-claude-freeze-auditor-credential-v1-20260829`);
- channel: `62b3410a-2ee9-458a-acee-24ea5d4661bb` (`gooi-p3-corpus-audit-claude-credential-v1-20260829`);
- request: seq 124, message `34ae0fdf-6a86-4bcb-abeb-eb56407b902a`;
- idempotency key: `gooi/P3/corpus-audit/claude/f83d29d6/snapshot-3782b7a0/credential-bootstrap-v1`.

The 7,440 payload bytes are byte-identical to the frozen audit request, SHA-256 `195ae94da6e070397adc0ab00c123bb24e3d0c293370270e6fbb895135890ed0`. They name GOOI commit `f83d29d662f0d7725445ddf13bd66752f8087926` and snapshot manifest `3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c`.

The worker retained the same strict macOS Seatbelt posture, declared-and-system reads, read-only snapshot grant, no ambient GOOI grant, and outbound network. Its config is SHA-256 `0f9c762bc268c67fe62d9900524a70ba2e929df837f1b6d2790a4c75ea61d096`. Runtime identity remained:

- `fleetd.harness.claude` 0.1.0, SHA-256 `223bfc34ad2a3a0559b849a8fc1950476c9ecde38a76b20309e8deaedea69692`;
- `@zed-industries/claude-code-acp` 0.16.2, entrypoint SHA-256 `0d0a87a08b316df91d9134f77cd376cce8fd7373c303abc0460aed8046dcf8e7`;
- Claude Code 2.1.251, SHA-256 `625869b01e0050f260b2980fac248fd9cef9e462612bded4ec9d3d49ff8969a5`;
- profile digest `sha256:5161071770e0de01e1f179daf0245a3d54e4e0d26a7ac292cf9f2d04cff4d1e5`;
- compatibility digest `sha256:4b7a7b1dd4359a7ee55f9c4f74b972a2c5acf8b8e68a974a1dbbc5278add3085`.

## Exact failure and event-chain proof

ACP initialize succeeded and reported Claude Code through the pinned adapter. Both bounded session-open attempts then failed:

```text
plugin call harness.acp.session.open failed with JSON-RPC error -32000:
inner ACP runtime error: Internal error: {
  "details": "Query closed before response received"
}
```

The narrowest observed failure point is inside the adapter's `createSession`: it constructs the Claude Agent SDK query at `acp-agent.js:867` and then awaits `q.initializationResult()` at `acp-agent.js:878`. The query closed during that await, before Fleetd could complete `session.open` and before dispatch could arm. The exact subprocess cause is not observable because the underlying SDK/Claude stderr is not preserved through this pre-arm error.

The durable proof is:

- invocation `b17e816c-cc3d-41aa-a62e-b3fc029a3b8b`, generation `de235d56-1869-45b9-8bca-6315c8959714`;
- invocation `96839938-db71-4fc5-8caa-95ab6d51efd8`, generation `4b795f62-c824-44d6-9580-c7cd8eba8591`;
- 2 invocations, both terminal `retry`, both `execution_certainty=not_started`;
- 0 invocations with `dispatch_armed_at_ms`;
- 0 `invocation_observations` rows, therefore no prompt, assistant, reasoning, tool, permission, usage, or other adapter events;
- `event_chain_digest = NULL` because no event chain began;
- 0 `work.result/v1` messages and no result text.

Seq 124 remains pending at attempt 2 after the qualification worker was stopped. Seq 123 was not reactivated.

## Cleanup and containment

After the qualification worker and its descendants were fully stopped, only the duplicate qualification credential was deleted. Its destination was verified absent. The ambient credential remains present at 501 bytes and mode `0600`; it was neither deleted nor modified.

GOOI remains clean at `f83d29d662f0d7725445ddf13bd66752f8087926`. The immutable snapshot manifest still hashes to `3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c`. There was no direct Claude audit, reference access by the worker, GOOI or snapshot mutation, adapter upgrade, unrelated seat restart, or commit.

This run identifies two separate Fleetd gaps. The successful manual secret copy is an untyped, qualification-local substitute for a missing Claude credential-onboarding secret-reference contract with explicit lifecycle and compatibility identity. After authentication qualified, ACP session opening still failed and needs a separately qualified diagnostic/runtime path; widening credential state is not justified.

Machine-readable evidence: `docs/qualification/gooi-p3-claude-freeze-audit-credential-bootstrap-v1-blocked-2026-08-29.json`.

Worker logs:

- stdout SHA-256 `7517c5339e04096f40ece95c9f7fa0bab7abe37f120682d42583a1268dfbc034`;
- stderr empty-file SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
