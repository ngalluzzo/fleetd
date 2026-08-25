import { Database } from "bun:sqlite";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

const protocol = "fleetd.channel-stream.browser.v1";
const resultAttribute = "data-fleetd-live-conversation-qualification";
const profilePath = Bun.argv[2];

if (!profilePath) {
  throw new Error(
    "usage: bun run tools/qualify-live-conversation.ts <qualification-profile.json>",
  );
}

interface PluginProfile {
  id: string;
  executable: string;
  args: string[];
  config: Record<string, unknown>;
  initialize_timeout_ms: number;
  request_timeout_ms: number;
  shutdown_timeout_ms: number;
}

interface WorkerProfile {
  working_directory: string;
  additional_directories: string[];
  mcp_grants: string[];
  plugin: PluginProfile;
  lease_duration_ms: number;
  poll_interval_ms: number;
  restart_backoff_ms: number;
  pre_arm_retry_delay_ms: number;
  turn: {
    idle_timeout_ms: number;
    wall_timeout_ms: number;
    cancel_drain_timeout_ms: number;
    max_captured_output_bytes: number;
    tool_budget: number;
  };
}

interface QualificationProfile {
  schema_version: 1;
  fleetd_executable: string;
  fleetd_revision: string;
  request_kind: string;
  result_kind: string;
  turn_timeout_ms: number;
  worker: WorkerProfile;
}

interface ManagedProcess {
  label: string;
  process: Bun.Subprocess<"ignore", "pipe", "pipe">;
  stdout: Promise<string>;
  stderr: Promise<string>;
}

interface StreamState {
  outcome: "pending" | "closed" | "error";
  ready: boolean;
  selectedProtocol: string | null;
  cursor: number;
  frameTypes: string[];
  messages: any[];
  errorCode?: string;
}

const profileSource = await readFile(resolve(profilePath), "utf8");
const profile = validateProfile(JSON.parse(profileSource));
const runDirectory = await mkdtemp(join(tmpdir(), "fleetd-live-conversation-"));
const databasePath = join(runDirectory, "fleetd.db");
const operatorTokenPath = join(runDirectory, "operator.token");
const workerConfigPath = join(runDirectory, "worker.json");
const runId = crypto.randomUUID();
const port = reservePort();
const origin = `http://127.0.0.1:${port}`;
const repositoryRoot = resolve(dirname(import.meta.dir));
assertFleetdRevision(profile.fleetd_revision);
const bundle = await buildWebViewBundle();
let daemon: ManagedProcess | undefined;
let worker: ManagedProcess | undefined;
let operatorCredential = "";
let humanCredential = "";
const processEvidence: Record<string, unknown>[] = [];

try {
  daemon = startDaemon();
  await waitForHealth(daemon);
  operatorCredential = (await readFile(operatorTokenPath, "utf8")).trim();
  if (!operatorCredential)
    throw new Error("operator credential file was empty");

  const human = await postJson(
    "/v1/agents",
    operatorCredential,
    {
      name: `phase-c-human-${runId}`,
      metadata: {
        qualification: "live-human-agent-conversation",
        role: "human-controlled",
      },
    },
    201,
  );
  const workerAgent = await postJson(
    "/v1/agents",
    operatorCredential,
    {
      name: `phase-c-worker-${runId}`,
      metadata: {
        qualification: "live-human-agent-conversation",
        role: "continuous-worker",
      },
    },
    201,
  );
  assertRegisteredAgent(human, "human");
  assertRegisteredAgent(workerAgent, "worker");
  humanCredential = human.credential.token;

  const channel = await postJson(
    "/v1/channels",
    operatorCredential,
    {
      name: `phase-c-conversation-${runId}`,
      metadata: { qualification: "live-human-agent-conversation" },
      member_ids: [],
      members: [
        { agent_id: human.agent.id, delivery_mode: "stream_only" },
        { agent_id: workerAgent.agent.id, delivery_mode: "inbox" },
      ],
    },
    201,
  );
  if (typeof channel?.id !== "string") {
    throw new Error("channel creation returned an unexpected body");
  }
  const memberships = await getJson(
    `/v1/channels/${encodeURIComponent(channel.id)}/members`,
    humanCredential,
  );
  assertMemberships(memberships, human.agent.id, workerAgent.agent.id);

  await writeWorkerConfig(workerAgent.agent.id);
  worker = startWorker();
  await waitForActiveGenerations(workerAgent.agent.id, 1, worker);

  let cursor = 0;
  const turnRecords: BrowserTurn[] = [];
  const first = await runBrowserTurn(
    channel.id,
    human.agent.id,
    workerAgent.agent.id,
    cursor,
    "initial",
  );
  turnRecords.push(first);
  cursor = first.cursor;

  const browserReconnect = await runBrowserTurn(
    channel.id,
    human.agent.id,
    workerAgent.agent.id,
    cursor,
    "browser-reconnect",
  );
  turnRecords.push(browserReconnect);
  cursor = browserReconnect.cursor;

  daemon = await stopManaged(daemon);
  daemon = startDaemon();
  await waitForHealth(daemon);
  const daemonRestart = await runBrowserTurn(
    channel.id,
    human.agent.id,
    workerAgent.agent.id,
    cursor,
    "daemon-restart",
  );
  turnRecords.push(daemonRestart);
  cursor = daemonRestart.cursor;

  const beforeWorkerRestart = await operatorJson(
    `/v1/session-bindings?agent=${encodeURIComponent(workerAgent.agent.id)}`,
  );
  assertReadyBinding(beforeWorkerRestart, channel.id, 1);
  const firstBinding = beforeWorkerRestart[0];

  worker = await stopManaged(worker);
  await waitForStoppedGenerations(workerAgent.agent.id, 1);
  worker = startWorker();
  await waitForActiveGenerations(workerAgent.agent.id, 2, worker);

  const workerRestart = await runBrowserTurn(
    channel.id,
    human.agent.id,
    workerAgent.agent.id,
    cursor,
    "worker-harness-restart",
  );
  turnRecords.push(workerRestart);
  cursor = workerRestart.cursor;

  const adoptedBindings = await operatorJson(
    `/v1/session-bindings?agent=${encodeURIComponent(workerAgent.agent.id)}`,
  );
  assertReadyBinding(adoptedBindings, channel.id, 2);
  const adopted = adoptedBindings[0];
  if (
    adopted.binding.binding_id !== firstBinding.binding.binding_id ||
    adopted.binding.binding_generation !==
      firstBinding.binding.binding_generation ||
    adopted.session_ref !== firstBinding.session_ref ||
    adopted.binding.owner_epoch !== firstBinding.binding.owner_epoch + 1
  ) {
    throw new Error(
      "worker replacement did not adopt the compatible native session",
    );
  }

  const history = await getJson(
    `/v1/channels/${encodeURIComponent(channel.id)}/messages?after=0&limit=100`,
    humanCredential,
  );
  assertHistory(history, turnRecords);
  const observations = await operatorJson(
    `/v1/invocation-observations?agent=${encodeURIComponent(workerAgent.agent.id)}`,
  );
  assertObservations(observations, turnRecords);
  assertNoHumanInbox(databasePath, human.agent.id);

  worker = await stopManaged(worker);
  const generations = await waitForStoppedGenerations(workerAgent.agent.id, 2);
  assertCompatibleGenerations(generations);
  const finalBindings = await operatorJson(
    `/v1/session-bindings?agent=${encodeURIComponent(workerAgent.agent.id)}`,
  );
  assertReadyBinding(finalBindings, channel.id, 2);

  const evidence = {
    schema_version: 1,
    qualification: "live-human-agent-conversation",
    run_id: runId,
    passed: true,
    runtime: {
      bun_version: Bun.version,
      browser_backend: "webkit",
      fleetd_revision: profile.fleetd_revision,
      qualification_profile_sha256: await sha256Text(profileSource),
      fleetd_executable_sha256: await sha256File(profile.fleetd_executable),
      plugin_executable_sha256: await sha256File(
        profile.worker.plugin.executable,
      ),
      plugin_id: profile.worker.plugin.id,
      plugin_version: generations[0].plugin_version,
      runtime_name: generations[0].runtime_name,
      runtime_version: generations[0].runtime_version,
      runtime_executable_digest: generations[0].runtime_executable_digest,
      profile_digest: generations[0].profile_digest,
      compatibility_digest: generations[0].compatibility_digest,
    },
    participants: {
      human_id: human.agent.id,
      human_delivery_mode: "stream_only",
      worker_id: workerAgent.agent.id,
      worker_delivery_mode: "inbox",
      channel_id: channel.id,
    },
    conversation: {
      turn_count: turnRecords.length,
      final_cursor: cursor,
      turns: turnRecords.map((turn) => turn.evidence),
      exact_history_match: true,
      human_delivery_rows: 0,
    },
    restarts: {
      browser_connections: 4,
      daemon_processes: 2,
      worker_processes: 2,
      plugin_generations: generations.map(summarizeGeneration),
      binding_id: adopted.binding.binding_id,
      binding_generation: adopted.binding.binding_generation,
      owner_epoch_before: firstBinding.binding.owner_epoch,
      owner_epoch_after: adopted.binding.owner_epoch,
      native_session_ref_preserved: true,
      session_persistence: adopted.session_persistence,
    },
    observations: observations.map(summarizeObservation),
    process_cleanup: {
      runner_started_daemon_stopped: false,
      runner_started_worker_stopped: true,
      runner_started_model_server: false,
    },
  };

  daemon = await stopManaged(daemon);
  evidence.process_cleanup.runner_started_daemon_stopped = true;
  Object.assign(evidence.process_cleanup, { processes: processEvidence });
  console.log(JSON.stringify(evidence));
} finally {
  humanCredential = "";
  operatorCredential = "";
  if (worker) {
    try {
      worker = await stopManaged(worker);
    } catch {}
  }
  if (daemon) {
    try {
      daemon = await stopManaged(daemon);
    } catch {}
  }
  await rm(runDirectory, { recursive: true, force: true });
}

function validateProfile(value: any): QualificationProfile {
  if (!value || typeof value !== "object" || value.schema_version !== 1) {
    throw new Error("qualification profile must use schema_version 1");
  }
  exactKeys(
    value,
    [
      "schema_version",
      "fleetd_executable",
      "fleetd_revision",
      "request_kind",
      "result_kind",
      "turn_timeout_ms",
      "worker",
    ],
    "qualification profile",
  );
  absoluteFile(value.fleetd_executable, "fleetd executable");
  if (
    typeof value.fleetd_revision !== "string" ||
    !/^[0-9a-f]{40}$/.test(value.fleetd_revision)
  ) {
    throw new Error(
      "fleetd revision must be an exact lowercase 40-character Git revision",
    );
  }
  boundedString(value.request_kind, "request kind", 256);
  boundedString(value.result_kind, "result kind", 256);
  boundedInteger(value.turn_timeout_ms, "turn timeout", 1_000, 3_600_000);
  const worker = value.worker;
  if (!worker || typeof worker !== "object")
    throw new Error("worker profile is required");
  exactKeys(
    worker,
    [
      "working_directory",
      "additional_directories",
      "mcp_grants",
      "plugin",
      "lease_duration_ms",
      "poll_interval_ms",
      "restart_backoff_ms",
      "pre_arm_retry_delay_ms",
      "turn",
    ],
    "worker profile",
  );
  absolutePath(worker.working_directory, "worker working directory");
  stringArray(worker.additional_directories, "additional directories");
  worker.additional_directories.forEach((path: string) =>
    absolutePath(path, "additional directory"),
  );
  stringArray(worker.mcp_grants, "MCP grants");
  validatePlugin(worker.plugin);
  for (const key of [
    "lease_duration_ms",
    "poll_interval_ms",
    "restart_backoff_ms",
    "pre_arm_retry_delay_ms",
  ]) {
    boundedInteger(
      worker[key],
      key,
      key === "pre_arm_retry_delay_ms" ? 0 : 1,
      3_600_000,
    );
  }
  const turn = worker.turn;
  if (!turn || typeof turn !== "object")
    throw new Error("worker turn profile is required");
  exactKeys(
    turn,
    [
      "idle_timeout_ms",
      "wall_timeout_ms",
      "cancel_drain_timeout_ms",
      "max_captured_output_bytes",
      "tool_budget",
    ],
    "worker turn profile",
  );
  for (const [key, maximum] of [
    ["idle_timeout_ms", 3_600_000],
    ["wall_timeout_ms", 3_600_000],
    ["cancel_drain_timeout_ms", 600_000],
    ["max_captured_output_bytes", 524_288],
    ["tool_budget", 10_000],
  ] as const)
    boundedInteger(turn[key], key, 1, maximum);
  return value as QualificationProfile;
}

function validatePlugin(plugin: any): void {
  if (!plugin || typeof plugin !== "object")
    throw new Error("worker plugin is required");
  exactKeys(
    plugin,
    [
      "id",
      "executable",
      "args",
      "config",
      "initialize_timeout_ms",
      "request_timeout_ms",
      "shutdown_timeout_ms",
    ],
    "worker plugin",
  );
  boundedString(plugin.id, "plugin ID", 256);
  absoluteFile(plugin.executable, "plugin executable");
  stringArray(plugin.args, "plugin arguments");
  if (
    !plugin.config ||
    typeof plugin.config !== "object" ||
    Array.isArray(plugin.config)
  ) {
    throw new Error("plugin config must be an object");
  }
  for (const key of [
    "initialize_timeout_ms",
    "request_timeout_ms",
    "shutdown_timeout_ms",
  ])
    boundedInteger(plugin[key], key, 1, 600_000);
}

function exactKeys(
  value: Record<string, unknown>,
  keys: string[],
  label: string,
): void {
  const expected = [...keys].sort();
  const actual = Object.keys(value).sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label} fields do not match schema 1`);
  }
}

function boundedString(
  value: unknown,
  label: string,
  maximum: number,
): asserts value is string {
  if (typeof value !== "string" || !value.trim() || value.length > maximum) {
    throw new Error(`${label} must contain between 1 and ${maximum} bytes`);
  }
}

function boundedInteger(
  value: unknown,
  label: string,
  minimum: number,
  maximum: number,
): asserts value is number {
  if (
    !Number.isSafeInteger(value) ||
    Number(value) < minimum ||
    Number(value) > maximum
  ) {
    throw new Error(
      `${label} must be an integer between ${minimum} and ${maximum}`,
    );
  }
}

function stringArray(value: unknown, label: string): asserts value is string[] {
  if (
    !Array.isArray(value) ||
    value.some((entry) => typeof entry !== "string")
  ) {
    throw new Error(`${label} must be an array of strings`);
  }
}

function absolutePath(value: unknown, label: string): asserts value is string {
  boundedString(value, label, 4_096);
  if (!isAbsolute(value)) throw new Error(`${label} must be absolute`);
}

function absoluteFile(value: unknown, label: string): asserts value is string {
  absolutePath(value, label);
  if (!Bun.file(value).size)
    throw new Error(`${label} must be an existing non-empty file`);
}

function reservePort(): number {
  const probe = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () => new Response(null, { status: 503 }),
  });
  const selected = probe.port;
  probe.stop(true);
  return selected;
}

function assertFleetdRevision(expected: string): void {
  const result = Bun.spawnSync(
    ["git", "-C", repositoryRoot, "rev-parse", "HEAD"],
    {
      env: { PATH: Bun.env.PATH ?? "/usr/bin:/bin" },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const observed = result.stdout.toString().trim();
  if (result.exitCode !== 0 || observed !== expected) {
    throw new Error(
      "qualification profile revision does not match the Fleetd checkout",
    );
  }
}

async function buildWebViewBundle(): Promise<string> {
  const entrypoint = join(
    import.meta.dir,
    "live-conversation-webview-entry.ts",
  );
  const build = await Bun.build({
    entrypoints: [entrypoint],
    format: "iife",
    target: "browser",
    write: false,
  });
  if (!build.success || build.outputs.length !== 1) {
    throw new Error("live conversation browser bundle failed");
  }
  return (await build.outputs[0].text()).trim().replace(/;$/, "");
}

function startDaemon(): ManagedProcess {
  return spawnManaged("daemon", [
    profile.fleetd_executable,
    "serve",
    "--listen",
    `127.0.0.1:${port}`,
    "--db",
    databasePath,
    "--operator-token-file",
    operatorTokenPath,
  ]);
}

function startWorker(): ManagedProcess {
  return spawnManaged("worker", [
    profile.fleetd_executable,
    "worker",
    "run",
    "--db",
    databasePath,
    "--config",
    workerConfigPath,
  ]);
}

function spawnManaged(label: string, command: string[]): ManagedProcess {
  const process = Bun.spawn(command, {
    cwd: repositoryRoot,
    env: { RUST_LOG: "fleetd=info" },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  return {
    label,
    process,
    stdout: readBounded(process.stdout, `${label} stdout`),
    stderr: readBounded(process.stderr, `${label} stderr`),
  };
}

async function readBounded(
  stream: ReadableStream<Uint8Array>,
  label: string,
): Promise<string> {
  const limit = 2 * 1024 * 1024;
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
    if (text.length > limit) {
      throw new Error(`${label} exceeded the bounded capture limit`);
    }
  }
  return text + decoder.decode();
}

async function stopManaged(managed: ManagedProcess): Promise<undefined> {
  if (managed.process.exitCode === null) managed.process.kill("SIGINT");
  const code = await withTimeout(
    managed.process.exited,
    20_000,
    `${managed.label} graceful shutdown`,
    async () => managed.process.kill("SIGKILL"),
  );
  const [stdout, stderr] = await Promise.all([managed.stdout, managed.stderr]);
  if (code !== 0 && code !== 130 && code !== 143) {
    throw new Error(
      `${managed.label} exited with code ${code}: ${boundedTail(stderr || stdout)}`,
    );
  }
  processEvidence.push({
    label: managed.label,
    exit_code: code,
    stdout_bytes: new TextEncoder().encode(stdout).length,
    stderr_bytes: new TextEncoder().encode(stderr).length,
  });
  return undefined;
}

async function waitForHealth(managed: ManagedProcess): Promise<void> {
  const deadline = performance.now() + 20_000;
  while (performance.now() < deadline) {
    if (managed.process.exitCode !== null) {
      throw new Error(`${managed.label} exited before becoming healthy`);
    }
    try {
      const response = await fetch(`${origin}/health`, { cache: "no-store" });
      if (response.status === 200 && (await response.json())?.status === "ok")
        return;
    } catch {}
    await Bun.sleep(25);
  }
  throw new Error(`${managed.label} did not become healthy`);
}

async function writeWorkerConfig(agentId: string): Promise<void> {
  const worker = profile.worker;
  const value = {
    schema_version: 2,
    agent_id: agentId,
    working_directory: worker.working_directory,
    additional_directories: worker.additional_directories,
    mcp_grants: worker.mcp_grants,
    adapter: {
      kind: "envelope",
      inbound: { schema_version: 1, message_kinds: [profile.request_kind] },
    },
    plugin: worker.plugin,
    result_kind: profile.result_kind,
    lease_duration_ms: worker.lease_duration_ms,
    poll_interval_ms: worker.poll_interval_ms,
    restart_backoff_ms: worker.restart_backoff_ms,
    pre_arm_retry_delay_ms: worker.pre_arm_retry_delay_ms,
    turn: worker.turn,
  };
  await writeFile(workerConfigPath, `${JSON.stringify(value, null, 2)}\n`, {
    mode: 0o600,
  });
}

async function postJson(
  path: string,
  credential: string,
  body: unknown,
  expectedStatus: number,
): Promise<any> {
  const response = await fetch(`${origin}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${credential}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (response.status !== expectedStatus) {
    throw new Error(`POST ${path} returned HTTP ${response.status}`);
  }
  return await response.json();
}

async function getJson(path: string, credential: string): Promise<any> {
  const response = await fetch(`${origin}${path}`, {
    headers: { Authorization: `Bearer ${credential}` },
    cache: "no-store",
  });
  if (response.status !== 200) {
    throw new Error(`GET ${path} returned HTTP ${response.status}`);
  }
  return await response.json();
}

async function operatorJson(path: string): Promise<any> {
  return await getJson(path, operatorCredential);
}

function assertRegisteredAgent(value: any, label: string): void {
  if (
    typeof value?.agent?.id !== "string" ||
    typeof value?.credential?.token !== "string" ||
    !value.credential.token
  ) {
    throw new Error(`${label} registration returned an unexpected body`);
  }
}

function assertMemberships(
  value: any,
  humanId: string,
  workerId: string,
): void {
  if (!Array.isArray(value) || value.length !== 2) {
    throw new Error(
      "membership read model did not return exactly two participants",
    );
  }
  const modes = new Map(
    value.map((membership: any) => [
      membership.agent_id,
      membership.delivery_mode,
    ]),
  );
  if (modes.get(humanId) !== "stream_only" || modes.get(workerId) !== "inbox") {
    throw new Error(
      "membership delivery modes did not match the Phase C composition",
    );
  }
}

async function waitForActiveGenerations(
  agentId: string,
  count: number,
  managed: ManagedProcess,
): Promise<any[]> {
  return await poll(
    async () => {
      if (managed.process.exitCode !== null) {
        throw new Error(
          "worker exited before its plugin generation became active",
        );
      }
      return await operatorJson(
        `/v1/plugin-generations?agent=${encodeURIComponent(agentId)}`,
      );
    },
    (values) =>
      Array.isArray(values) &&
      values.length === count &&
      values.filter(
        (value: any) => value.state === "active" && value.health === "active",
      ).length === 1,
    30_000,
    "active plugin generation",
  );
}

async function waitForStoppedGenerations(
  agentId: string,
  count: number,
): Promise<any[]> {
  return await poll(
    () =>
      operatorJson(
        `/v1/plugin-generations?agent=${encodeURIComponent(agentId)}`,
      ),
    (values) =>
      Array.isArray(values) &&
      values.length === count &&
      values.every(
        (value: any) =>
          value.state === "stopped" &&
          value.health === "stopped" &&
          value.shutdown_outcome === "graceful",
      ),
    10_000,
    "stopped plugin generations",
  );
}

async function runBrowserTurn(
  channelId: string,
  humanId: string,
  workerId: string,
  after: number,
  phase: string,
): Promise<BrowserTurn> {
  await using view = new Bun.WebView({
    backend: "webkit",
    dataStore: "ephemeral",
  });
  try {
    await view.navigate(`${origin}/operator/`);
    await view.evaluate("document.readyState");
    await view.evaluate(bundle);
    await view.evaluate(
      `Reflect.get(globalThis, "__fleetdLiveConversationQualification").start(${JSON.stringify(
        {
          origin,
          channelId,
          credential: humanCredential,
          after,
        },
      )})`,
    );
  } catch {
    throw new Error(`browser ${phase} stream bootstrap failed`);
  }

  await waitForView(
    view,
    (state) => state.ready === true || state.outcome === "error",
    15_000,
    `${phase} browser readiness`,
  );
  const correlationId = `${runId}/${phase}`;
  const requestPayload = {
    text: `Reply briefly to the human. Include the exact marker ${phase}.`,
    phase,
    extension: {
      contract_owner: "external-phase-c-qualification",
      unknown_fields_must_survive: [1, { nested: true }, "three"],
    },
  };
  const request = await postJson(
    `/v1/channels/${encodeURIComponent(channelId)}/messages`,
    humanCredential,
    {
      idempotency_key: `live-conversation/${runId}/${phase}`,
      recipient_id: workerId,
      kind: profile.request_kind,
      payload: requestPayload,
      correlation_id: correlationId,
      causation_id: null,
    },
    201,
  );
  if (
    request.sender_id !== humanId ||
    request.recipient_id !== workerId ||
    request.kind !== profile.request_kind ||
    JSON.stringify(request.payload) !== JSON.stringify(requestPayload)
  ) {
    throw new Error(
      `${phase} human request attribution or opaque payload changed`,
    );
  }

  const state = await waitForView(
    view,
    (candidate) =>
      candidate.messages.some(
        (message: any) =>
          message.kind === profile.result_kind &&
          message.causation_id === request.id,
      ) || candidate.outcome === "error",
    profile.turn_timeout_ms,
    `${phase} causal worker result`,
  );
  if (state.outcome === "error") {
    throw new Error(
      `${phase} browser stream failed with ${state.errorCode ?? "unknown"}`,
    );
  }
  const streamedRequest = state.messages.find(
    (message: any) => message.id === request.id,
  );
  const result = state.messages.find(
    (message: any) =>
      message.kind === profile.result_kind &&
      message.causation_id === request.id,
  );
  if (JSON.stringify(streamedRequest) !== JSON.stringify(request)) {
    throw new Error(`${phase} browser stream rewrote the human request`);
  }
  if (
    !result ||
    result.sender_id !== workerId ||
    result.recipient_id !== humanId ||
    result.correlation_id !== correlationId ||
    result.payload?.status !== "completed" ||
    !Array.isArray(result.payload?.assistant_messages) ||
    result.payload.assistant_messages.length === 0
  ) {
    throw new Error(
      `${phase} did not produce one successful causal worker result`,
    );
  }
  if (
    state.selectedProtocol !== protocol ||
    state.frameTypes[0] !== "ready" ||
    !state.frameTypes.includes("message") ||
    state.cursor !== result.seq
  ) {
    throw new Error(`${phase} did not use the exact browser stream contract`);
  }
  try {
    await view.evaluate(
      `Reflect.get(globalThis, "__fleetdLiveConversationQualification").close()`,
    );
  } catch {
    throw new Error(`browser ${phase} stream cleanup failed`);
  }
  return {
    cursor: result.seq,
    request,
    result,
    evidence: {
      phase,
      browser_after: after,
      request_id: request.id,
      request_seq: request.seq,
      result_id: result.id,
      result_seq: result.seq,
      correlation_id: correlationId,
      causation_preserved: true,
      request_payload_preserved: true,
      result_payload_preserved: true,
      selected_protocol: state.selectedProtocol,
      accepted_message_ids: state.messages.map((message: any) => message.id),
    },
  };
}

interface BrowserTurn {
  cursor: number;
  request: any;
  result: any;
  evidence: Record<string, unknown>;
}

async function readView(view: Bun.WebView): Promise<StreamState> {
  return await view.evaluate(
    `JSON.parse(document.documentElement.getAttribute("${resultAttribute}"))`,
  );
}

async function waitForView(
  view: Bun.WebView,
  predicate: (state: StreamState) => boolean,
  timeoutMs: number,
  label: string,
): Promise<StreamState> {
  const deadline = performance.now() + timeoutMs;
  let state = await readView(view);
  while (!predicate(state) && performance.now() < deadline) {
    await Bun.sleep(25);
    state = await readView(view);
  }
  if (!predicate(state)) {
    throw new Error(
      `${label} did not complete before its deadline: ${boundedTail(JSON.stringify(state))}`,
    );
  }
  return state;
}

function assertReadyBinding(
  value: any,
  channelId: string,
  ownerEpoch: number,
): void {
  if (
    !Array.isArray(value) ||
    value.length !== 1 ||
    value[0].lane_policy !== "per-channel" ||
    value[0].lane_key !== channelId ||
    value[0].state !== "ready" ||
    value[0].binding.binding_generation !== 1 ||
    value[0].binding.owner_epoch !== ownerEpoch ||
    typeof value[0].session_ref !== "string" ||
    !value[0].session_ref
  ) {
    throw new Error(
      `session binding was not ready at owner epoch ${ownerEpoch}`,
    );
  }
}

function assertHistory(history: any, turns: BrowserTurn[]): void {
  if (!history || !Array.isArray(history.messages)) {
    throw new Error("conversation history returned an unexpected body");
  }
  const expectedMessages = turns.flatMap((turn) => [turn.request, turn.result]);
  const expectedIds = expectedMessages.map((message) => message.id);
  const actualIds = history.messages.map((message: any) => message.id);
  if (JSON.stringify(actualIds) !== JSON.stringify(expectedIds)) {
    throw new Error(
      "durable human history did not exactly match browser-accepted turns",
    );
  }
  for (let index = 0; index < history.messages.length; index += 1) {
    if (
      JSON.stringify(history.messages[index]) !==
      JSON.stringify(expectedMessages[index])
    ) {
      throw new Error(
        "durable history rewrote a browser-accepted opaque envelope",
      );
    }
  }
}

function assertObservations(observations: any, turns: BrowserTurn[]): void {
  if (!Array.isArray(observations) || observations.length !== turns.length) {
    throw new Error(
      "invocation observation count did not match conversation turns",
    );
  }
  const expected = new Map(
    turns.map((turn) => [turn.request.id, turn.result.id]),
  );
  for (const observation of observations) {
    if (
      expected.get(observation.source_message_id) !==
        observation.result_message_id ||
      observation.execution_certainty !== "outcome_known" ||
      observation.session_quiescent !== true ||
      typeof observation.event_chain_digest !== "string"
    ) {
      throw new Error(
        "invocation observation did not preserve exact causal terminal evidence",
      );
    }
  }
}

function assertNoHumanInbox(path: string, humanId: string): void {
  const database = new Database(path, { readonly: true, strict: true });
  try {
    const row = database
      .query(
        "SELECT COUNT(*) AS count FROM agent_deliveries WHERE agent_id = ?",
      )
      .get(humanId) as { count: number };
    if (row.count !== 0) {
      throw new Error("stream_only human accumulated leased inbox rows");
    }
  } finally {
    database.close();
  }
}

function assertCompatibleGenerations(generations: any[]): void {
  if (
    new Set(generations.map((generation) => generation.profile_digest)).size !==
      1 ||
    new Set(generations.map((generation) => generation.compatibility_digest))
      .size !== 1 ||
    new Set(generations.map((generation) => generation.plugin_id)).size !== 1 ||
    new Set(
      generations.map((generation) => generation.runtime_executable_digest),
    ).size !== 1
  ) {
    throw new Error(
      "worker replacement generations did not preserve one compatible profile",
    );
  }
}

function summarizeGeneration(generation: any): Record<string, unknown> {
  return {
    id: generation.id,
    state: generation.state,
    health: generation.health,
    stop_disposition: generation.stop_disposition,
    shutdown_outcome: generation.shutdown_outcome,
    process_id: generation.process_id,
  };
}

function summarizeObservation(observation: any): Record<string, unknown> {
  return {
    invocation_id: observation.invocation_id,
    source_message_id: observation.source_message_id,
    result_message_id: observation.result_message_id,
    generation_id: observation.generation_id,
    binding_generation: observation.binding_generation,
    owner_epoch: observation.owner_epoch,
    event_count: observation.event_count,
    observed_payload_bytes: observation.observed_payload_bytes,
    event_chain_digest: observation.event_chain_digest,
    counts: observation.counts,
    stop_reason: observation.stop_reason,
    execution_certainty: observation.execution_certainty,
    session_quiescent: observation.session_quiescent,
    session_persistence: observation.session_persistence,
    usage: observation.usage,
  };
}

async function sha256File(path: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    await Bun.file(path).arrayBuffer(),
  );
  return `sha256:${Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

async function sha256Text(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(value),
  );
  return `sha256:${Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

async function poll<T>(
  read: () => Promise<T>,
  predicate: (value: T) => boolean,
  timeoutMs: number,
  label: string,
): Promise<T> {
  const deadline = performance.now() + timeoutMs;
  let value = await read();
  while (!predicate(value) && performance.now() < deadline) {
    await Bun.sleep(50);
    value = await read();
  }
  if (!predicate(value))
    throw new Error(`${label} did not reach its required state`);
  return value;
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
  onTimeout: () => Promise<void> | void,
): Promise<T> {
  let timeout: Timer | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(async () => {
          await onTimeout();
          reject(new Error(`${label} exceeded ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

function boundedTail(value: string): string {
  return value.slice(-2_048).replace(/[\r\n]+/g, " ");
}
