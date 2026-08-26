// Every generated artifact in this repository is committed, and something else
// is the source of truth for it. This regenerates each one and asks git whether
// the result differs. Drift is otherwise invisible: the daemon embeds the built
// bundle, so stale UI ships without a single test failing.

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const run = (command, args) =>
  execFileSync(command, args, { stdio: "inherit", encoding: "utf8" });

const changed = (path) => {
  const status = execFileSync("git", ["status", "--porcelain", "--", path], {
    encoding: "utf8",
  });
  return status.trim();
};

const failures = [];

function verify(label, rebuild, path, remedy) {
  process.stdout.write(`\nverifying ${label}\n`);
  rebuild();
  const drift = changed(path);
  if (drift) {
    failures.push(
      `${label} is stale. ${path} changed when regenerated:\n${drift}\n  fix: ${remedy}`,
    );
  }
}

// The committed client must match the committed contract.
verify(
  "generated TypeScript client",
  () => run("npm", ["run", "generate"]),
  "clients/typescript/src/generated",
  "npm run generate, then commit the result",
);

// The daemon embeds these files with include_str!, so a stale bundle ships.
verify(
  "served conversation bundle",
  () => run("npm", ["run", "build"]),
  "web/conversation",
  "npm run build, then commit the result",
);

// The client is versioned against the contract it was generated from.
const contractVersion = JSON.parse(
  readFileSync(new URL("../openapi/fleetd-v1.json", import.meta.url), "utf8"),
).info.version;
const clientVersion = JSON.parse(
  readFileSync(
    new URL("../clients/typescript/package.json", import.meta.url),
    "utf8",
  ),
).version;
process.stdout.write(`\nverifying client version\n`);
if (contractVersion !== clientVersion) {
  failures.push(
    `client version ${clientVersion} does not match contract version ${contractVersion}.\n` +
      "  fix: set clients/typescript/package.json version to the contract's info.version",
  );
}

if (failures.length > 0) {
  process.stderr.write(`\n${failures.join("\n\n")}\n`);
  process.exit(1);
}
process.stdout.write("\nall generated artifacts match their sources\n");
