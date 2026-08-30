# GOOI P3 Claude freeze audit through Fleetd — blocked before dispatch

Date: 2026-08-29

Verdict: `BLOCKED_TRANSPORT`. There is no Claude audit verdict. Fleetd durably accepted the exact audit request, booted the pinned Claude ACP route inside the strict macOS boundary, and then failed closed before the model prompt was armed. This record must not be cited as semantic evidence for or against freezing P3.

## What was frozen

The candidate is GOOI commit `f83d29d662f0d7725445ddf13bd66752f8087926`. Its P3 candidate tree is Git object `c87a36de07945d7028cc3e3d6a7ec799e447871c`; `candidate/manifest.json` is SHA-256 `15c41d974c9a4e73df10312330b76eb1b56436fb6adac665c2906fd4a0e10da5`; and `run-corpus.mjs` is SHA-256 `ac0cb8c2c3af7b7c8808ec49c6adad7adc49b4efe45bca083b781d1c27585a51`.

The content-addressed audit snapshot is:

`/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/snapshots/sha256-3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c`

Its canonical manifest has the same SHA-256, covers 145 regular files and 427,854 bytes, and is stored at:

`/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/evidence/snapshot-manifest.sha256-3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c.json`

All snapshot files are mode `0444`; all directories are `0555`; there are no symlinks. A post-run verification recomputed every file digest and the exact path set. The snapshot contains only the authorized vision, roadmap, program state, Fleet protocol, decision 0003, protocol v0.3 normative documents and schemas, P3 work packet, GLM audit, Codex precheck, and the complete P3 candidate tree. Path checks reject both Rust and TypeScript reference implementations, kernel-duel material, and every other federated path.

The coordinator-only preflight, run from the snapshot without `--write`, passed:

```text
PASS files=126 cases=31 permutations=3100
```

The 31 expected plan statuses are 16 `ready`, 8 `blocked`, and 7 `invalid`.

## Exact request and Fleetd identities

The permanent human-readable payload fixture is `docs/qualification/fixtures/gooi-p3-claude-freeze-audit-v1.payload.json`, SHA-256 `d0d159782dc82996148c9e90fc97fc16b11cfc29daf14a2df75ff7541f531387`. Its canonical durable form is 7,440 bytes before Fleetd CLI newline framing and SHA-256 `195ae94da6e070397adc0ab00c123bb24e3d0c293370270e6fbb895135890ed0`. The retained newline-terminated evidence is 7,441 bytes, SHA-256 `98133641bd5418d3eb1b383c320e90a6ef437083c07b70592381dcfaf67eb1d7`. A byte comparison against SQLite message seq 123 passed.

The request asks an independent auditor to verify all 31 cases, the GLM and Codex closure claims, enum/carrier equality including unused registered packages, malformed-package precedence, off-enum per-claim behavior, exact canonical outputs and hashes, authorities, diagnostics, phase purity, and 100 permutations. Its only allowed verdicts are `APPROVE-FREEZE`, `REPAIR`, or `BLOCKED`; approval requires exact promotion with only coordinator README/digest bookkeeping left.

Fleetd routed it as:

- coordinator agent: `7e5185dc-d3ec-4b5d-a960-4edea250494c`;
- Claude auditor agent: `ef5a9b01-8e7a-49dc-bea5-e5f452f0a14d` (`gooi-p3-claude-freeze-auditor-20260829`);
- channel: `f27a66c6-6ae1-40c2-9d18-4df6daf7305c` (`gooi-p3-corpus-audit-claude-20260829`);
- message: seq 123, id `65e7e00b-808d-41e2-9e44-90b925d796cb`, kind `work.request/v1`;
- idempotency key: `gooi/P3/corpus-audit/claude/f83d29d6/snapshot-3782b7a0/v1`.

This was not a direct Claude CLI invocation.

## Boundary and runtime

The worker profile is `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/claude-worker.json`, SHA-256 `c5ca6a7d09346f43674da0c204ac8f794526698041124a3e78b4220e720e5f7b`. It uses the strict deny-default macOS Seatbelt posture with declared-and-system reads, outbound network, a disposable writable workspace, and no GOOI repository read root. The only semantic source granted to the worker is the read-only snapshot. `allow_once` remains contingent on the OS sandbox. The turn limits are 20-minute wall, five-minute idle, 128 tools, and 16,000 maximum thinking tokens.

Runtime identity:

- Fleetd commit `8af2743b78ac8220d090221440ad1f1a9d7d8935`; binary SHA-256 `c781514f1571143c757321eadffb2b572e4f5cb2131e10fde2ffd3761495391f`;
- `fleetd.harness.claude` 0.1.0; binary SHA-256 `223bfc34ad2a3a0559b849a8fc1950476c9ecde38a76b20309e8deaedea69692`;
- `@zed-industries/claude-code-acp` 0.16.2; entrypoint SHA-256 `0d0a87a08b316df91d9134f77cd376cce8fd7373c303abc0460aed8046dcf8e7`;
- Claude Agent SDK 0.2.44;
- Claude Code 2.1.251; binary SHA-256 `625869b01e0050f260b2980fac248fd9cef9e462612bded4ec9d3d49ff8969a5`;
- Node v25.9.0; binary SHA-256 `32e234a5b6bec67d72a016f2baadf7fadf3afd328470b395b73af473fdee0d85`;
- plugin profile digest `sha256:cf1c25a164655b1a25932a5b57a7caefe380018c707124a99a2f9925d2eda604`;
- session compatibility digest `sha256:6e289ea62b6fb7311224f158d8f18f37b3c114e96a6339cab39d3fbfc21e56b2`.

Every generation completed ACP initialize and reported `@zed-industries/claude-code-acp` 0.16.2, title `Claude Code`, protocol 1, and auth method `claude-login`.

## Failure evidence

Every `harness.acp.session.open` failed while awaiting the Claude Agent SDK initialization result:

```text
plugin call harness.acp.session.open failed with JSON-RPC error -32000:
inner ACP runtime error: Internal error: {
  "details": "Query closed before response received"
}
```

Fleetd made 12 bounded pre-arm retries. The invocation IDs, generation IDs, and exact common result are in the adjacent JSON record. Every invocation is terminal `retry`, has `dispatch_armed_at_ms = NULL`, and has execution certainty `not_started`. Every generation shut down gracefully with exit code 0. After the twelfth identical failure, the coordinator stopped only this qualification worker. The request remains pending at attempt 12; it was not acknowledged or lost.

There are zero prompt, assistant, reasoning, tool, permission, usage, or other adapter events; no event chain exists; no result message or result text arrived. No model prompt was dispatched.

The disposable Claude HOME acquired only two private runtime files: `.claude.json` and its initial backup. Their redacted, value-free shape is retained at `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/evidence/claude-private-state-safe-shape.json`. The private state lacks the ambient `hasCompletedOnboarding` and `oauthAccount` keys. That difference is diagnostic correlation, not proof of why the SDK query closed.

The exact blocker is therefore a missing qualified Claude subscription/session-open contract inside an isolated HOME, compounded by insufficient pre-arm SDK stderr preservation. A safe follow-up must provide a typed, content-addressed minimum auth/onboarding bootstrap or another explicitly qualified subscription mechanism, and must preserve redacted underlying diagnostics. It must not grant the worker ambient-home reads, copy the operator's unrelated Claude state, weaken Seatbelt, or run the audit directly.

## Containment

GOOI remained at `f83d29d662f0d7725445ddf13bd66752f8087926` with a clean worktree before and after. Its status evidence has identical SHA-256 `a7c11e7221ce6478b41590dc906bcf38b8bbbd6590e139054e7f9a36e978c79e`. The immutable snapshot still matches all 145 manifest digests. The Claude harness made no writes outside disposable Fleetd qualification state. The coordinator added only this bounded report, its machine-readable companion, and the frozen payload fixture under `docs/qualification`. There was no reference implementation inspection, unrelated seat restart, or commit.

Machine-readable evidence: `docs/qualification/gooi-p3-claude-freeze-audit-fleetd-blocked-2026-08-29.json`.

Private retained logs:

- `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/logs/worker.stdout.log`, SHA-256 `82aaf4a92a2996dd2f12512fa197b0d159316ed051ec9bd4b42b87cc885007e2`;
- `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/logs/worker.stderr.log`, empty-file SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
