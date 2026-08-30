# GOOI P3 Claude Agent ACP 0.69.0 audit — completed

Date: 2026-08-29

Qualification verdict: `AUDITOR_APPROVED_FREEZE`.

The fresh Fleetd-only audit completed and its durable result begins exactly `VERDICT: APPROVE-FREEZE`. Claude independently accepted the exact P3 candidate without semantic or fixture repair. Promotion and coordinator bookkeeping remain outside this run.

## Frozen corpus and request

The run used GOOI commit `f83d29d662f0d7725445ddf13bd66752f8087926` and the unchanged content-addressed snapshot:

```text
/Users/ngalluzzo/repos/fleetd/.fleetd/qualification/p3-claude-freeze-audit-2026-08-29/snapshots/sha256-3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c
```

Its manifest still hashes to `3782b7a015def1e73f0defc4936777022021cee2cd1a2655d07d2a0cf2a1ea2c`. A post-run verification checked all 145 files, all 427,854 bytes, and every file digest against that manifest; all matched, all files remained read-only, and the snapshot contains no symlinks, Git metadata, Rust, or TypeScript.

Request seq 126 is byte-identical to the frozen canonical request after excluding its evidence-file newline: 7,440 bytes, SHA-256 `195ae94da6e070397adc0ab00c123bb24e3d0c293370270e6fbb895135890ed0`. It was not reconstructed or paraphrased.

## Fleetd route and runtime

- auditor `7f3cefaa-405c-4281-bd4f-ccd0f0afae48` (`gooi-p3-claude-auditor-acp069-private-tmp-20260829`);
- channel `f1db32eb-33c2-4467-acca-910706973c05` (`gooi-p3-corpus-audit-claude-acp069-private-tmp-20260829`);
- request seq 126, message `229601d3-7c89-4670-ae43-ce30a0ecfc22`;
- idempotency key `gooi/P3/corpus-audit/claude/f83d29d6/snapshot-3782b7a0/claude-agent-acp-0.69.0/private-tmp-v1`;
- result seq 127, message `c3d9626a-b9cb-4d5f-bfd1-ddc582033673`, causation `229601d3-7c89-4670-ae43-ce30a0ecfc22`;
- invocation `1aeb97c1-f4a3-45b9-865d-e463659fa3ce` on generation `5948133d-3f32-4d18-9d07-ffccabeee7ec`;
- worker profile SHA-256 `4e14b9cca2437a54cae12fa02592dbc6d648299d6a8c1061344e1e40be404346`;
- profile digest `sha256:866107b61dc7785d8a68d2e2bd07c43187ec82dcd7680a28e0198b3a55549d52`;
- compatibility digest `sha256:c83682f8a23d0c5a1b7efe2e6f678562bf3eb99c403272927d2ee3ecfe4b3625`.

The runtime initialized as `@agentclientprotocol/claude-agent-acp` 0.69.0, title `Claude Agent`, ACP protocol 1. The adapter entrypoint is SHA-256 `260aac90bf75f197b93640087c1de66441761d43c2784efa035fdcee60b5dacd`; its package manifest is `b45f4fe9301303d39cad609858c6417958dfb739290cf08e231ba23f48531da1`. It drove Claude Code 2.1.251, SHA-256 `625869b01e0050f260b2980fac248fd9cef9e462612bded4ec9d3d49ff8969a5`.

Before dispatch, the running Fleetd executable measured `978251f2372bf44d06d7d334a194a2682fd60a0c59137700b92db496db808fea` and the rebuilt Claude plugin measured `d83da78b09ec1fc399ab78efb7858b7ca99b0454a130564f3a7dd9e9eb56dfa`; the focused plugin suite passed 6/6. Unrelated concurrent builds later replaced those shared `target/debug` paths with different bytes. That does not alter the already-running processes, but it means those two launch artifacts are identified by the pre-dispatch measurements rather than retained immutable copies. The adapter and Claude Code paths remained stable. A content-addressed Fleetd/plugin executable store would remove this evidence weakness.

## Boundary and credential handling

The strict deny-default macOS Seatbelt profile remained in force for the plugin process group and descendants. It allowed declared/system reads, the immutable snapshot, outbound network, and writes only within the qualification working directory. It did not grant ambient GOOI access. The rebuilt plugin mapped the configured private `tmpdir` to both `TMPDIR` and `CLAUDE_CODE_TMPDIR`; this kept Claude's UID-scoped sockets/cache state below the already writable qualification temp root and avoided the prior `/tmp/claude-501` denial without widening Seatbelt.

The fresh private HOME received exactly the ambient Claude credential file and no root metadata, projects, caches, settings, or history. Evidence retains only its path classes, 501-byte size, `0600` mode, and sole top-level key name `claudeAiOauth`; the credential was never printed, parsed, hashed, or copied into evidence. After the worker and descendants stopped gracefully, only the duplicate credential was deleted and verified absent. The ambient credential remains present at 501 bytes and mode `0600`.

This manual qualification-local secret copy remains a missing typed Fleetd credential-onboarding feature.

## Durable result and findings

The invocation armed, completed with `execution_certainty=outcome_known`, and produced one `work.result/v1` message. Its exact 12,874-byte payload hashes to `df517932cb99b68d1c5b284f60cb0be96dc21d541ea29b34c9888f4dfc46bcca`. The exact 5,931-byte final assistant text hashes to `7b4e87e2fa6294188106652e7b270cfce9b596044eef2033ecbf32203cf7e89a` and begins `VERDICT: APPROVE-FREEZE`.

The auditor reported:

- all supplied governing and snapshot identities matched; candidate manifest `15c41d974c9a4e73df10312330b76eb1b56436fb6adac665c2906fd4a0e10da5`, runner `ac0cb8c2c3af7b7c8808ec49c6adad7adc49b4efe45bca083b781d1c27585a51`, and tree OID `c87a36de07945d7028cc3e3d6a7ec799e447871c` were independently reproduced;
- a from-scratch composition kernel derived from the normative documents matched all 31 expected outputs byte-for-byte: 16 ready, 8 blocked, 7 invalid, with 24 exit-0 and 7 exit-2 classifications;
- graph/plan hashes, diagnostics, self-hashes, exact authorities, effective values, four algebras, bounded-meet static validation, unused packages, malformed-package precedence, off-enum behavior, and phase purity all matched;
- 3,100 independent fixed-seed recomputations produced zero mismatches;
- the candidate verifier independently corroborated this with `PASS files=126 cases=31 permutations=3100`, exit 0;
- P3-AUD-001 through P3-AUD-007 and P3-PRE-001 through P3-PRE-002 were all closed;
- no reference implementation was read or run, and no promotion bookkeeping was performed.

The adapter produced 674 normalized events: 94 assistant, 352 tool-adapter updates, 155 usage, 72 permission, and 1 metadata event. These are adapter events, not 352 semantic tool calls. Fleetd currently retains the aggregate counts and chained digest, not each event body, so an exact semantic tool-call count is unavailable. The event-chain digest is `sha256:53d40619a8389994e52e8495208d677e0f41f88d88ec1cc55b609fba903e91fd`. The run consumed 221,965 of a 1,000,000 reported session window and reported cost USD 3.6204932. Dispatch-to-terminal wall time was 991,416 ms.

The final response disclosed one early `dangerouslyDisableSandbox` Bash option while testing Git. This affected Claude Code's inner tool setting only: Fleetd's outer process-group Seatbelt could not be disabled by the model and still denied Git's `/dev/null` access. No later conclusion depended on that attempt. Fleetd retained 72 aggregate permission events but not their individual bodies; the final response is therefore the durable disclosure of that option.

## Containment

Post-run verification found GOOI clean at `f83d29d662f0d7725445ddf13bd66752f8087926`, the snapshot byte-identical to its manifest, no `.git` below the snapshot or workspace, and no credential duplicate. All 144 generated workspace files remained inside the qualification root: 133 scratch/corpus-analysis files, 10 private Claude state files, and one private-temp result. No direct Claude CLI audit, GOOI mutation, snapshot mutation, unrelated seat restart, or commit occurred.

Machine-readable evidence: `docs/qualification/gooi-p3-claude-agent-acp-069-private-tmp-audit-approved-2026-08-29.json`.

Worker logs:

- stdout SHA-256 `1dad351761f7d8f06a6aa452c09f1a3a12822f32af3de2fa21b6d3508d886d1b`;
- stderr empty-file SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
