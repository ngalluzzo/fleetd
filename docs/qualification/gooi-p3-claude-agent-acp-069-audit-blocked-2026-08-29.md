# GOOI P3 audit with Claude Agent ACP 0.69.0 — typed temp capability blocked

Date: 2026-08-29

Verdict: `BLOCKED_TYPED_TEMP_CAPABILITY`. The exact new adapter initialized successfully and exposed an actionable Claude Code sandbox denial, but the request never armed. No Claude audit verdict exists, and this run is not semantic evidence for or against freezing P3.

## Corrected runtime identity

Preflight found that `target/debug/fleetd-harness-claude` still predated the source update and embedded the retired `@zed-industries/claude-code-acp` identity. Only the already-edited Claude plugin was rebuilt:

```text
cargo build -p fleetd-harness-claude
cargo test -p fleetd-harness-claude
```

All 6 focused tests passed. The rebuilt binary is SHA-256 `12b8394abc55589c47a2a8b617c0c9558b88c7e461a1d3ada308c274a9ea4968` and embeds only `@agentclientprotocol/claude-agent-acp` as its expected adapter identity.

The exact runtime was:

- Fleetd binary SHA-256 `978251f2372bf44d06d7d334a194a2682fd60a0c59137700b92db496db808fea`;
- `fleetd.harness.claude` 0.1.0, SHA-256 `12b8394abc55589c47a2a8b617c0c9558b88c7e461a1d3ada308c274a9ea4968`;
- `@agentclientprotocol/claude-agent-acp` 0.69.0, entrypoint SHA-256 `260aac90bf75f197b93640087c1de66441761d43c2784efa035fdcee60b5dacd`;
- adapter package SHA-256 `b45f4fe9301303d39cad609858c6417958dfb739290cf08e231ba23f48531da1`;
- Claude Agent SDK 0.3.232;
- Claude Code 2.1.251, SHA-256 `625869b01e0050f260b2980fac248fd9cef9e462612bded4ec9d3d49ff8969a5`.

The model-free Fleetd boot succeeded and reported exactly `@agentclientprotocol/claude-agent-acp`, title `Claude Agent`, version `0.69.0`, protocol 1. The plugin profile digest is `sha256:3b78493bcbc763eac09bca7f658eba90648553ac44a3754439b214ffed01e6d7`; session compatibility is `sha256:02302c2e6d794bae27e6529bc7b3d44b76bcb5de1345c81e13cc1c0c9fda1c36`.

## Credential-only bootstrap

The fresh disposable HOME received only the exact Claude credential file. No ambient root `.claude.json`, projects, caches, settings, or history were copied. Credential evidence is deliberately restricted to:

- source path class `ambient_claude_credentials`;
- destination path class `qualification_private_home_credentials`;
- 501 bytes;
- mode `0600`;
- sole top-level key name `claudeAiOauth`.

The credential was never printed, parsed, hashed, logged, or included in evidence. The safe metadata record is `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-claude-agent-acp-0.69.0-2026-08-29/evidence/credential-bootstrap-safe-metadata.json`.

## Fresh Fleetd request

The isolated route was:

- auditor `28bb43e0-abc7-44c3-abcc-f9679b1b2e01` (`gooi-p3-claude-auditor-acp069-20260829`);
- channel `acc690d2-28dc-46fa-909f-11b7051c7622` (`gooi-p3-corpus-audit-claude-acp069-20260829`);
- request seq 125, message `29c5e2c3-e7dd-405e-8975-c2d1ecde5722`;
- idempotency key `gooi/P3/corpus-audit/claude/f83d29d6/snapshot-3782b7a0/claude-agent-acp-0.69.0/v1`.

The 7,440 payload bytes are byte-identical to the frozen request, SHA-256 `195ae94da6e070397adc0ab00c123bb24e3d0c293370270e6fbb895135890ed0`. They target GOOI commit `f83d29d662f0d7725445ddf13bd66752f8087926` and immutable snapshot manifest `3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c`.

The worker profile, SHA-256 `435b1d261cbb1685d9b3de4457eae39fd78c18ea2fa74bf38e6de45c59866a9b`, retained strict deny-default macOS Seatbelt, declared-and-system reads, the read-only snapshot, no ambient GOOI root, outbound network, and a qualification-private `TMPDIR`.

## Exact pre-arm failure

The new adapter preserved the underlying Claude Code stderr:

```text
Claude Code process exited with code 1. stderr:
EPERM: operation not permitted, open '/tmp/claude-501'
```

Claude Code attempted the UID-scoped ambient path `/tmp/claude-501` even though the worker supplied a qualification-private `TMPDIR`. That path is intentionally outside strict Seatbelt grants. Fleetd stopped rather than widen the sandbox.

The durable evidence is:

- invocation `27bf69fb-9474-42f7-9d4c-8b1798d55888` on generation `b943ffcb-7d0c-4771-b9c1-70138b808af0`;
- terminal `retry`, `execution_certainty=not_started`, and `dispatch_armed_at_ms=NULL`;
- 0 `invocation_observations` rows, hence no prompt, assistant, reasoning, tool, permission, usage, or other adapter events;
- `event_chain_digest=NULL` because no event chain began;
- 0 result messages and no result text.

A restart generation, `5354951d-b6d3-42eb-8458-2c5e40148695`, had initialized before the coordinator stop but reserved no invocation. Seq 125 remains pending at attempt 1. Seq 123 and 124 were not touched.

The narrow follow-up is not blanket `/tmp` access. Either the adapter/Claude Code must honor the supplied private `TMPDIR`, or Fleetd needs a typed UID-private Claude temp capability that fixes the exact normalized path, ownership, mode, creation/cleanup lifecycle, and profile/compatibility identity.

## Cleanup and containment

After the worker and descendants fully stopped, only the qualification credential duplicate was deleted and verified absent. The ambient credential remains present at 501 bytes and mode `0600`.

GOOI remains clean at `f83d29d662f0d7725445ddf13bd66752f8087926`; the snapshot manifest digest is unchanged. There was no direct Claude CLI invocation, model dispatch, GOOI or snapshot mutation, reference access, unrelated seat restart, or commit.

Machine-readable evidence: `docs/qualification/gooi-p3-claude-agent-acp-069-audit-blocked-2026-08-29.json`.

Worker logs:

- stdout SHA-256 `6c8e6f5e86880d1e881bc8e60007db801b93f77068c1023958191b1899afb6b7`;
- stderr empty-file SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
