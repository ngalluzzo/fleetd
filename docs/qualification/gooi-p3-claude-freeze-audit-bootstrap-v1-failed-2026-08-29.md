# GOOI P3 Claude bootstrap probe v1 — failed gate

Date: 2026-08-29

Verdict: `BLOCKED_BOOTSTRAP`. The one authorized private-HOME bootstrap was performed, but pinned Claude Code returned `loggedIn:false`. The conditional Fleetd audit was therefore not created or dispatched. This is not a semantic audit of the GOOI P3 candidate.

## Bounded bootstrap

A new qualification-local disposable HOME was created at:

`/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-bootstrap-v1-2026-08-29/workspace/claude-home`

From the ordinary `/Users/ngalluzzo/.claude.json`, the coordinator copied exactly two top-level fields:

- `hasCompletedOnboarding`;
- `oauthAccount`.

Before the copy, recursive field names were checked for token-, secret-, password-, credential-, API-key-, and authorization-shaped names; none matched. The copy did not include projects, caches, settings, history, or any other ambient field. It did not copy the ambient file wholesale. The private file was mode `0600`.

Only a redacted schema and field-name record is retained at `/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-bootstrap-v1-2026-08-29/evidence/bootstrap-safe-schema.json`. It contains no field values or PII. The initial private bootstrap was 873 bytes, SHA-256 `cf000d81c6f6e727be40b19f47fff4741a9ce9f6cdfe3af478a8c9704a032b82`.

## Acceptance probe

The only direct Claude command was the explicitly authorized, non-audit probe:

```text
env -i HOME=<private-home> USER=ngalluzzo LOGNAME=ngalluzzo \
  PATH=<pinned-path> TERM=xterm-256color TMPDIR=<private-tmp> \
  /Users/ngalluzzo/.local/share/claude/versions/2.1.251 auth status
```

Claude Code 2.1.251 is SHA-256 `625869b01e0050f260b2980fac248fd9cef9e462612bded4ec9d3d49ff8969a5`.

The probe returned exit code 1 and valid JSON. Its redacted response schema contained `analyticsDisabled`, `apiProvider`, `authMethod`, `loggedIn`, and `projectsDirectory`. The only retained result value is the acceptance field:

```text
loggedIn=false
```

No raw response was retained. Claude Code added its own first-start and migration metadata to the private file during the probe. The post-probe file remained mode `0600`, grew to 1,213 bytes, and has SHA-256 `2c1c6fb64aa11045264d2aef3003ec42cb7e33dca56d13f90d16aa69744ef4bd`; those generated values are not preserved in the evidence record.

## Stop and containment

The failed gate required an immediate stop. The copied field set was not expanded. No fresh Fleetd agent, channel, message, invocation, or worker was created. Prior request seq 123 remains pending at attempt 12 and was not reactivated. There was no direct Claude audit, adapter upgrade, ambient-state copy, GOOI mutation, reference-path access, unrelated seat restart, or commit.

GOOI remains clean at `f83d29d662f0d7725445ddf13bd66752f8087926`. The intended immutable snapshot remains `sha256:3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c` and was never opened by Claude in this probe.

This manual experiment exposes a missing typed Fleetd feature: onboarding/account metadata alone does not present a usable Claude subscription to an isolated HOME. Fleetd needs a content-addressed, privacy-bounded Claude onboarding and subscription bootstrap contract that neither grants ambient-home reads nor puts provider secrets into desired state.

Machine-readable evidence: `docs/qualification/gooi-p3-claude-freeze-audit-bootstrap-v1-failed-2026-08-29.json`.
