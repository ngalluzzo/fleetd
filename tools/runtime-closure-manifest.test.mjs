import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { buildClosureManifest } from "./runtime-closure-manifest.mjs";

test("manifest identity covers bytes, modes, and in-closure symlinks", async () => {
  const parent = await mkdtemp(join(tmpdir(), "fleetd-runtime-manifest-"));
  try {
    const root = join(parent, "runtime");
    await mkdir(join(root, "bin"), { recursive: true });
    await writeFile(join(root, "bin", "entry"), "first\n", { mode: 0o755 });
    await symlink("entry", join(root, "bin", "alias"));

    const first = await buildClosureManifest(root);
    assert.match(first.closure_digest, /^sha256:[0-9a-f]{64}$/);
    assert.deepEqual(first.entries.map((entry) => entry.path), [
      "bin",
      "bin/alias",
      "bin/entry",
    ]);
    assert.equal(first.entries[1].target, "entry");
    assert.equal(first.entries[2].mode, "0755");

    await writeFile(join(root, "bin", "entry"), "second\n", { mode: 0o755 });
    const second = await buildClosureManifest(root);
    assert.notEqual(second.closure_digest, first.closure_digest);
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});

test("manifest rejects a symlink that escapes the closure", async () => {
  const parent = await mkdtemp(join(tmpdir(), "fleetd-runtime-manifest-"));
  try {
    const root = join(parent, "runtime");
    await mkdir(root);
    await symlink("../outside", join(root, "escape"));
    await assert.rejects(
      buildClosureManifest(root),
      /symlink escape escapes runtime closure/,
    );
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});
