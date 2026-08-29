import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";

import {
  NativeChannelStreamError,
  openNativeChannelStream,
} from "../clients/typescript/src/native-channel-stream.ts";
import type { Message } from "../clients/typescript/src/generated/types.gen.ts";

const repositoryRoot = resolve(dirname(import.meta.dirname));
const fleetdExecutable = resolve(
  process.env.FLEETD_BIN ?? join(repositoryRoot, "target/debug/fleetd"),
);
const runDirectory = await mkdtemp(
  join(tmpdir(), "fleetd-native-channel-stream-"),
);
const configPath = join(runDirectory, "config.json");
const operatorTokenPath = join(runDirectory, "operator.token");
const port = await reservePort();
const origin = `http://127.0.0.1:${port}`;
const runId = crypto.randomUUID();
let daemon: ManagedDaemon | undefined;
let listenerCredential = "";
let authorCredential = "";
let outsiderCredential = "";

try {
  const initialized = spawnSync(
    fleetdExecutable,
    ["--fleet-config", configPath, "init", "--listen", `127.0.0.1:${port}`],
    { cwd: repositoryRoot, encoding: "utf8" },
  );
  if (initialized.status !== 0) {
    throw new Error(
      `fleetd init failed: ${boundedDiagnostic(initialized.stderr)}`,
    );
  }

  daemon = startDaemon();
  await waitForHealth(daemon);
  const operatorCredential = (await readFile(operatorTokenPath, "utf8")).trim();
  assert.ok(operatorCredential, "operator credential was empty");

  const listener = await registerAgent(operatorCredential, "listener");
  const author = await registerAgent(operatorCredential, "author");
  const outsider = await registerAgent(operatorCredential, "outsider");
  listenerCredential = listener.credential.token;
  authorCredential = author.credential.token;
  outsiderCredential = outsider.credential.token;

  const channel = await postJson(
    "/v1/channels",
    operatorCredential,
    {
      name: `native-stream-${runId}`,
      metadata: { qualification: "native-channel-stream" },
      member_ids: [listener.agent.id, author.agent.id],
      members: [],
    },
    201,
  );
  assert.equal(typeof channel?.id, "string", "channel ID");
  const channelId = channel.id as string;

  const expected: Message[] = [];
  expected.push(await appendMessage(channelId, "replay"));

  const accepted: Message[] = [];
  const statuses: string[] = [];
  const stream = openNativeChannelStream({
    origin,
    channelId,
    credential: listenerCredential,
    after: 0,
    reconnectDelaysMs: Array.from({ length: 200 }, () => 50),
    accept(message) {
      accepted.push(structuredClone(message));
    },
    statusChanged(status) {
      statuses.push(status);
    },
  });
  const unexpectedClose = stream.closed.then(
    () => new Error("native stream closed before qualification completed"),
    (error: unknown) =>
      error instanceof Error
        ? error
        : new Error("native stream failed with a non-error value"),
  );

  await waitUntil(
    () => accepted.length === expected.length,
    unexpectedClose,
    "durable replay",
  );
  expected.push(await appendMessage(channelId, "live"));
  await waitUntil(
    () => accepted.length === expected.length,
    unexpectedClose,
    "live delivery",
  );

  daemon = await stopDaemon(daemon, true);
  daemon = startDaemon();
  await waitForHealth(daemon);
  expected.push(await appendMessage(channelId, "daemon-restart"));
  await waitUntil(
    () => accepted.length === expected.length,
    unexpectedClose,
    "restart replay",
  );

  assert.deepEqual(accepted, expected, "replay/live envelopes diverged");
  assert.deepEqual(
    accepted.map((message) => message.seq),
    [...new Set(accepted.map((message) => message.seq))],
    "native stream presented a duplicate sequence",
  );
  assert.ok(statuses.includes("reconnecting"), "restart was not observed");
  assert.equal(stream.cursor, expected.at(-1)?.seq, "accepted cursor");
  assert.deepEqual(
    accepted.map((message) => message.payload),
    expected.map((message) => message.payload),
    "opaque payload fields changed",
  );

  stream.close();
  const explicitClose = await Promise.race([
    stream.closed.then(() => "closed" as const),
    delay(5_000).then(() => "timeout" as const),
  ]);
  assert.equal(explicitClose, "closed", "explicit stream close was bounded");

  const forbidden = openNativeChannelStream({
    origin,
    channelId,
    credential: outsiderCredential,
    reconnectDelaysMs: [],
    accept() {},
  });
  const rejection = await forbidden.closed.then(
    () => undefined,
    (error: unknown) => error,
  );
  assert.ok(rejection instanceof NativeChannelStreamError);
  assert.equal(rejection.code, "upgrade_rejected");
  assert.equal(rejection.status, 403);
  assert.equal(String(rejection).includes(outsiderCredential), false);

  console.log("native channel stream qualification passed");
  console.log(`  replay/live/restart messages: ${expected.length}`);
  console.log(`  final accepted cursor: ${expected.at(-1)?.seq}`);
  console.log(`  observed states: ${statuses.join(" -> ")}`);
  console.log("  non-member upgrade: HTTP 403 without credential disclosure");
} finally {
  listenerCredential = "";
  authorCredential = "";
  outsiderCredential = "";
  if (daemon) await stopDaemon(daemon, true);
  await rm(runDirectory, { recursive: true, force: true });
}

interface ManagedDaemon {
  process: ChildProcessWithoutNullStreams;
  stdout: string;
  stderr: string;
}

function startDaemon(): ManagedDaemon {
  const process = spawn(
    fleetdExecutable,
    ["--fleet-config", configPath, "serve"],
    {
      cwd: repositoryRoot,
      env: { ...processEnvWithoutSecrets() },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  const managed: ManagedDaemon = { process, stdout: "", stderr: "" };
  process.stdout.setEncoding("utf8");
  process.stderr.setEncoding("utf8");
  process.stdout.on("data", (chunk: string) => {
    managed.stdout = boundedAppend(managed.stdout, chunk);
  });
  process.stderr.on("data", (chunk: string) => {
    managed.stderr = boundedAppend(managed.stderr, chunk);
  });
  return managed;
}

async function stopDaemon(
  managed: ManagedDaemon,
  hard = false,
): Promise<undefined> {
  if (managed.process.exitCode !== null || managed.process.signalCode !== null) {
    return undefined;
  }
  if (hard) {
    managed.process.kill("SIGKILL");
    await new Promise<void>((resolve) =>
      managed.process.once("exit", () => resolve()),
    );
    return undefined;
  }
  managed.process.kill("SIGINT");
  const stopped = await Promise.race([
    new Promise<boolean>((resolve) => managed.process.once("exit", () => resolve(true))),
    delay(5_000).then(() => false),
  ]);
  if (!stopped) {
    managed.process.kill("SIGKILL");
    await new Promise<void>((resolve) => managed.process.once("exit", () => resolve()));
  }
  return undefined;
}

async function waitForHealth(managed: ManagedDaemon): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (managed.process.exitCode !== null || managed.process.signalCode !== null) {
      throw new Error(
        `fleetd daemon exited during startup: ${boundedDiagnostic(managed.stderr)}`,
      );
    }
    try {
      const response = await fetch(new URL("/health", origin));
      if (response.status === 200) return;
    } catch {
      // The listener is not ready yet.
    }
    await delay(25);
  }
  throw new Error(
    `fleetd daemon did not become healthy: ${boundedDiagnostic(managed.stderr)}`,
  );
}

async function registerAgent(operatorCredential: string, role: string): Promise<any> {
  const registered = await postJson(
    "/v1/agents",
    operatorCredential,
    {
      name: `native-stream-${role}-${runId}`,
      metadata: { qualification: "native-channel-stream", role },
    },
    201,
  );
  assert.equal(typeof registered?.agent?.id, "string", `${role} agent ID`);
  assert.equal(
    typeof registered?.credential?.token,
    "string",
    `${role} credential`,
  );
  return registered;
}

async function appendMessage(channelId: string, phase: string): Promise<Message> {
  const appended = await postJson(
    `/v1/channels/${encodeURIComponent(channelId)}/messages`,
    authorCredential,
    {
      idempotency_key: `native-stream/${runId}/${phase}`,
      recipient_id: null,
      kind: `qualification.native-channel-stream.${phase}/v9`,
      payload: {
        phase,
        extension: { retained: true, run_id: runId },
      },
      correlation_id: runId,
      causation_id: null,
    },
    201,
  );
  assert.equal(typeof appended?.id, "string", `${phase} message ID`);
  assert.ok(Number.isSafeInteger(appended?.seq), `${phase} message sequence`);
  return appended as Message;
}

async function postJson(
  path: string,
  credential: string,
  body: unknown,
  expectedStatus: number,
): Promise<any> {
  let response: Response;
  try {
    response = await fetch(new URL(path, origin), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${credential}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      cache: "no-store",
      credentials: "omit",
      referrerPolicy: "no-referrer",
    });
  } catch {
    throw new Error(`qualification request to ${path} failed`);
  }
  if (response.status !== expectedStatus) {
    throw new Error(
      `qualification request to ${path} returned HTTP ${response.status}`,
    );
  }
  return response.json();
}

async function waitUntil(
  predicate: () => boolean,
  unexpectedClose: Promise<Error>,
  label: string,
): Promise<void> {
  const result = await Promise.race([
    (async () => {
      for (let attempt = 0; attempt < 1_000; attempt += 1) {
        if (predicate()) return "ready" as const;
        await delay(10);
      }
      return "timeout" as const;
    })(),
    unexpectedClose,
  ]);
  if (result instanceof Error) throw result;
  if (result !== "ready") throw new Error(`${label} timed out`);
}

async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  assert.ok(address && typeof address === "object", "reserved loopback port");
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return address.port;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function boundedAppend(current: string, chunk: string): string {
  return `${current}${chunk}`.slice(-16_384);
}

function boundedDiagnostic(value: string | null | undefined): string {
  const normalized = (value ?? "").trim();
  return normalized ? normalized.slice(-2_048) : "no diagnostic output";
}

function processEnvWithoutSecrets(): NodeJS.ProcessEnv {
  const environment = { ...process.env };
  for (const key of Object.keys(environment)) {
    if (/credential|secret|token/i.test(key)) delete environment[key];
  }
  return environment;
}
