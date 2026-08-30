import { createHash } from "node:crypto";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readdir,
  readFile,
  readlink,
  realpath,
  rename,
  unlink,
} from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

const FORMAT_VERSION = 1;

export async function buildClosureManifest(rootPath) {
  const root = await realpath(rootPath);
  const entries = [];
  await walk(root, root, entries);
  entries.sort((left, right) => Buffer.compare(
    Buffer.from(left.path),
    Buffer.from(right.path),
  ));

  const identity = { format_version: FORMAT_VERSION, entries };
  const digest = createHash("sha256")
    .update(JSON.stringify(identity))
    .digest("hex");
  return {
    format_version: FORMAT_VERSION,
    algorithm: "sha256",
    closure_digest: `sha256:${digest}`,
    entries,
  };
}

export async function writeClosureManifest(rootPath, outputPath) {
  const root = await realpath(rootPath);
  const output = resolve(outputPath);
  assertOutsideRoot(root, output, "manifest output");

  const manifest = await buildClosureManifest(root);
  await mkdir(dirname(output), { recursive: true });
  const temporary = `${output}.${process.pid}.tmp`;
  const handle = await open(temporary, "wx", 0o644);
  try {
    await handle.writeFile(`${JSON.stringify(manifest, null, 2)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await chmod(temporary, 0o644);
    await rename(temporary, output);
  } catch (error) {
    await unlink(temporary).catch(() => {});
    throw error;
  }
  return manifest;
}

async function walk(root, directory, entries) {
  const children = await readdir(directory, { withFileTypes: true });
  children.sort((left, right) => Buffer.compare(
    Buffer.from(left.name),
    Buffer.from(right.name),
  ));

  for (const child of children) {
    const absolute = resolve(directory, child.name);
    const path = relative(root, absolute).split(sep).join("/");
    const metadata = await lstat(absolute);
    const mode = `0${(metadata.mode & 0o7777).toString(8)}`;

    if (metadata.isDirectory()) {
      entries.push({ path, kind: "directory", mode });
      await walk(root, absolute, entries);
      continue;
    }
    if (metadata.isFile()) {
      const bytes = await readFile(absolute);
      entries.push({
        path,
        kind: "file",
        mode,
        size: metadata.size,
        sha256: createHash("sha256").update(bytes).digest("hex"),
      });
      continue;
    }
    if (metadata.isSymbolicLink()) {
      const target = await readlink(absolute);
      const resolvedTarget = isAbsolute(target)
        ? resolve(target)
        : resolve(dirname(absolute), target);
      assertInsideRoot(root, resolvedTarget, `symlink ${path}`);
      entries.push({ path, kind: "symlink", mode, target });
      continue;
    }
    throw new Error(`runtime closure contains unsupported file type: ${path}`);
  }
}

function assertInsideRoot(root, candidate, label) {
  if (candidate !== root && !candidate.startsWith(`${root}${sep}`)) {
    throw new Error(`${label} escapes runtime closure: ${candidate}`);
  }
}

function assertOutsideRoot(root, candidate, label) {
  if (candidate === root || candidate.startsWith(`${root}${sep}`)) {
    throw new Error(`${label} must be outside runtime closure: ${candidate}`);
  }
}

if (process.argv[1]
  && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [root, output] = process.argv.slice(2);
  if (!root || !output) {
    throw new Error("usage: node tools/runtime-closure-manifest.mjs <runtime-root> <manifest-output>");
  }
  const manifest = await writeClosureManifest(root, output);
  console.log(manifest.closure_digest);
}
