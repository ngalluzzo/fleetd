// Qualifies the actual presentation-agnostic browser stream client against a
// running loopback Fleetd daemon using Bun's native macOS WebKit view.
const protocol = "fleetd.channel-stream.browser.v1";
const browserStreamPath = "/v1/browser/channel-stream";
const expectedCsp =
  "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const origin =
  process.env.FLEETD_BROWSER_QUALIFICATION_ORIGIN ??
  "http://127.0.0.1:7419";
const operatorCredential =
  process.env.FLEETD_BROWSER_QUALIFICATION_CREDENTIAL;
const pageUrl = new URL("/operator/", origin);
const socketUrl = new URL(browserStreamPath, origin);
socketUrl.protocol = pageUrl.protocol === "https:" ? "wss:" : "ws:";

if (!operatorCredential) {
  throw new Error(
    "browser stream qualification requires an in-memory operator credential",
  );
}
if (
  !pageUrl.hostname ||
  !["127.0.0.1", "::1", "[::1]"].includes(pageUrl.hostname)
) {
  throw new Error("browser stream qualification requires an exact loopback origin");
}
if (!pageUrl.port) {
  throw new Error("browser stream qualification requires an explicit bound port");
}

interface HttpEvidence {
  setCookieHeaders: number;
}

const httpEvidence: HttpEvidence = { setCookieHeaders: 0 };

async function requestJson(
  label: string,
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
    throw new Error(`${label} transport failed`);
  }
  if (response.headers.has("set-cookie")) httpEvidence.setCookieHeaders += 1;
  if (response.status !== expectedStatus) {
    throw new Error(`${label} returned HTTP ${response.status}`);
  }
  try {
    return await response.json();
  } catch {
    throw new Error(`${label} response was not valid JSON`);
  }
}

const runId = crypto.randomUUID();
const registered = await requestJson(
  "qualification agent registration",
  "/v1/agents",
  operatorCredential,
  {
    name: `browser-qualification-${runId}`,
    metadata: { qualification: "browser-channel-stream" },
  },
  201,
);
if (
  typeof registered?.agent?.id !== "string" ||
  typeof registered?.credential?.token !== "string"
) {
  throw new Error("qualification agent registration had an unexpected shape");
}
const authorId = registered.agent.id as string;
let authorCredential = registered.credential.token as string;

const channel = await requestJson(
  "qualification channel creation",
  "/v1/channels",
  operatorCredential,
  {
    name: `browser-qualification-${runId}`,
    metadata: { qualification: "browser-channel-stream" },
    member_ids: [authorId],
    members: [],
  },
  201,
);
if (typeof channel?.id !== "string") {
  throw new Error("qualification channel creation had an unexpected shape");
}
const channelId = channel.id as string;

async function appendFixtureMessage(kind: string, phase: string): Promise<any> {
  const appended = await requestJson(
    `qualification ${phase} append`,
    `/v1/channels/${encodeURIComponent(channelId)}/messages`,
    authorCredential,
    {
      idempotency_key: `browser-qualification/${runId}/${phase}`,
      recipient_id: null,
      kind,
      payload: { phase, run_id: runId },
      correlation_id: runId,
      causation_id: null,
    },
    201,
  );
  if (typeof appended?.id !== "string" || !Number.isSafeInteger(appended?.seq)) {
    throw new Error(`qualification ${phase} append had an unexpected shape`);
  }
  return appended;
}

const replayMessage = await appendFixtureMessage(
  "qualification.browser.replay/v1",
  "replay",
);

let operatorPageResponse: Response;
try {
  operatorPageResponse = await fetch(pageUrl, {
    cache: "no-store",
    credentials: "omit",
    referrerPolicy: "no-referrer",
  });
} catch {
  throw new Error("operator page evidence request failed");
}
if (operatorPageResponse.headers.has("set-cookie")) {
  httpEvidence.setCookieHeaders += 1;
}
if (
  operatorPageResponse.status !== 200 ||
  operatorPageResponse.headers.get("content-security-policy") !== expectedCsp ||
  operatorPageResponse.headers.get("cache-control") !== "no-store" ||
  operatorPageResponse.headers.get("referrer-policy") !== "no-referrer"
) {
  throw new Error("operator page did not expose the exact hardened CSP surface");
}

const entrypoint = new URL(
  "./browser-channel-stream-webview-entry.ts",
  import.meta.url,
).pathname;
const build = await Bun.build({
  entrypoints: [entrypoint],
  format: "iife",
  target: "browser",
  write: false,
});
if (!build.success || build.outputs.length !== 1) {
  throw new Error("browser stream client qualification bundle failed");
}
const clientBundle = (await build.outputs[0].text()).trim().replace(/;$/, "");

async function prepareView(view: Bun.WebView, url: URL): Promise<void> {
  try {
    await view.navigate(url.href);
    await view.evaluate("document.readyState");
    await view.evaluate(clientBundle);
  } catch {
    // Evaluation failures may retain credential-bearing source in their cause.
    throw new Error("browser qualification page or bundle failed to initialize");
  }
}

async function startView(
  view: Bun.WebView,
  method: "startAlias" | "startForeign" | "startSameOrigin",
  config: Record<string, unknown>,
): Promise<void> {
  try {
    await view.evaluate(
      `Reflect.get(globalThis, "__fleetdBrowserChannelStreamClient").${method}(${JSON.stringify(config)})`,
    );
  } catch {
    // Never attach the raw WebView cause: evaluated config contains secrets.
    throw new Error(`browser qualification ${method} bootstrap failed`);
  }
}

const readResult = (view: Bun.WebView) =>
  view.evaluate(
    "JSON.parse(document.documentElement.getAttribute('data-fleetd-browser-stream-qualification'))",
  );

async function waitFor(
  view: Bun.WebView,
  predicate: (state: any) => boolean,
  label: string,
): Promise<any> {
  const deadline = performance.now() + 10_000;
  let state = await readResult(view);
  while (!predicate(state) && performance.now() < deadline) {
    await Bun.sleep(10);
    state = await readResult(view);
  }
  if (!predicate(state)) {
    throw new Error(`${label} did not reach its bounded terminal state`);
  }
  return state;
}

async function closeViewStream(view: Bun.WebView): Promise<void> {
  try {
    await view.evaluate(
      "(() => { Reflect.get(document.documentElement, '__fleetdQualificationStream')?.close(); Reflect.get(document.documentElement, '__fleetdQualificationDirectSocket')?.close(); return true; })()",
    );
  } catch {
    throw new Error("browser qualification stream cleanup failed");
  }
}

function expectedAdapterOperations() {
  return [
    {
      kind: "fetch",
      method: "POST",
      path: `/v1/channels/${encodeURIComponent(channelId)}/stream-grants`,
    },
    {
      kind: "websocket",
      path: browserStreamPath,
      protocol,
    },
  ];
}

function assertSecretAudit(label: string, audit: any): void {
  if (
    audit?.credentialObserved !== true ||
    audit?.grantObserved !== true ||
    audit?.noSecretDetected !== true ||
    audit?.instrumentation?.consoleMethods !== 6 ||
    audit?.instrumentation?.historyMethods !== 2 ||
    audit?.instrumentation?.storageMethods !== 3 ||
    audit?.instrumentation?.pageFailures !== true ||
    audit?.location?.secretDetected !== false ||
    audit?.history?.mutationCalls !== 0 ||
    audit?.history?.currentStateSecretDetected !== false ||
    audit?.cookies?.writes !== 0 ||
    audit?.cookies?.setterInstrumented !== true ||
    audit?.cookies?.secretDetected !== false ||
    audit?.localStorage?.entries !== 0 ||
    audit?.localStorage?.secretDetected !== false ||
    audit?.sessionStorage?.entries !== 0 ||
    audit?.sessionStorage?.secretDetected !== false ||
    audit?.indexedDb?.mutationCalls !== 0 ||
    (audit?.indexedDb?.available === true &&
      audit?.indexedDb?.instrumented !== true) ||
    audit?.indexedDb?.secretDetected !== false ||
    audit?.cacheApi?.mutationCalls !== 0 ||
    (audit?.cacheApi?.available === true &&
      audit?.cacheApi?.instrumented !== true) ||
    audit?.cacheApi?.secretDetected !== false ||
    audit?.serviceWorkers?.registrationCalls !== 0 ||
    (audit?.serviceWorkers?.available === true &&
      audit?.serviceWorkers?.instrumented !== true) ||
    audit?.serviceWorkers?.secretDetected !== false ||
    audit?.console?.secretDetected !== false ||
    audit?.pageFailures?.secretDetected !== false
  ) {
    throw new Error(`${label} secret-surface audit failed`);
  }
}

await using sameOriginView = new Bun.WebView({
  backend: "webkit",
  dataStore: "ephemeral",
});
await prepareView(sameOriginView, pageUrl);
await startView(sameOriginView, "startSameOrigin", {
  origin,
  channelId,
  credential: operatorCredential,
  replayMessageId: replayMessage.id,
});
await waitFor(
  sameOriginView,
  (state) => state?.stage === "replay_accepted" || state?.outcome === "error",
  "same-origin replay",
);
const liveMessage = await appendFixtureMessage(
  "qualification.browser.live/v1",
  "live",
);
const sameOriginResult = await waitFor(
  sameOriginView,
  (state) => state?.outcome === "complete" || state?.outcome === "error",
  "same-origin live continuation",
);
await closeViewStream(sameOriginView);

if (
  sameOriginResult?.outcome !== "complete" ||
  sameOriginResult?.selectedProtocol !== protocol ||
  sameOriginResult?.requestedProtocol !== protocol ||
  sameOriginResult?.socketUrl !== socketUrl.href ||
  JSON.stringify(sameOriginResult?.operations) !==
    JSON.stringify(expectedAdapterOperations()) ||
  JSON.stringify(sameOriginResult?.frameTypes) !==
    JSON.stringify(["ready", "message", "message"]) ||
  JSON.stringify(sameOriginResult?.acceptedIds) !==
    JSON.stringify([replayMessage.id, liveMessage.id]) ||
  sameOriginResult?.audit?.location?.unchanged !== true
) {
  throw new Error("same-origin replay/live adapter qualification failed");
}
assertSecretAudit("same-origin", sameOriginResult.audit);

const aliasPageUrl = new URL(pageUrl);
aliasPageUrl.hostname = "localhost";
await using aliasView = new Bun.WebView({
  backend: "webkit",
  dataStore: "ephemeral",
});
await prepareView(aliasView, aliasPageUrl);
await startView(aliasView, "startAlias", {
  channelId,
  credential: operatorCredential,
});
const aliasResult = await waitFor(
  aliasView,
  (state) => state?.outcome === "closed",
  "hostname-alias rejection",
);
await closeViewStream(aliasView);
if (
  aliasResult?.pageOrigin !== aliasPageUrl.origin ||
  aliasResult?.socketOpened !== false ||
  aliasResult?.applicationFrames !== 0 ||
  JSON.stringify(aliasResult?.operations) !==
    JSON.stringify(expectedAdapterOperations())
) {
  throw new Error("hostname-alias page obtained browser stream authority");
}
assertSecretAudit("hostname-alias", aliasResult.audit);

const foreignGrantResponse = await requestJson(
  "foreign-origin grant issuance",
  `/v1/channels/${encodeURIComponent(channelId)}/stream-grants`,
  operatorCredential,
  { after: 0, protocol },
  201,
);
if (
  typeof foreignGrantResponse?.grant !== "string" ||
  foreignGrantResponse?.protocol !== protocol ||
  foreignGrantResponse?.websocket_path !== browserStreamPath
) {
  throw new Error("foreign-origin grant response had an unexpected shape");
}
let foreignGrant = foreignGrantResponse.grant as string;

const foreignCsp = `default-src 'none'; connect-src ${origin} ${socketUrl.origin}`;
const foreignServer = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  fetch() {
    return new Response("<!doctype html><title>foreign qualification</title>", {
      headers: {
        "cache-control": "no-store",
        "content-security-policy": foreignCsp,
        "content-type": "text/html; charset=utf-8",
        "referrer-policy": "no-referrer",
      },
    });
  },
});
const foreignPageUrl = new URL(`http://127.0.0.1:${foreignServer.port}/`);
let foreignResult: any;
try {
  await using foreignView = new Bun.WebView({
    backend: "webkit",
    dataStore: "ephemeral",
  });
  await prepareView(foreignView, foreignPageUrl);
  await startView(foreignView, "startForeign", {
    fleetOrigin: origin,
    channelId,
    credential: operatorCredential,
    grant: foreignGrant,
  });
  foreignResult = await waitFor(
    foreignView,
    (state) => state?.outcome === "complete",
    "foreign-origin rejection",
  );
  await closeViewStream(foreignView);
} finally {
  foreignServer.stop(true);
}
foreignGrant = "";

if (
  foreignResult?.pageOrigin !== foreignPageUrl.origin ||
  foreignResult?.adapterApplicationFrames !== 0 ||
  foreignResult?.directSocketOpened !== false ||
  foreignResult?.directApplicationFrames !== 0 ||
  foreignResult?.directRequestedProtocol !== protocol ||
  JSON.stringify(foreignResult?.adapterOperations) !==
    JSON.stringify([
      {
        kind: "fetch",
        method: "POST",
        path: `/v1/channels/${encodeURIComponent(channelId)}/stream-grants`,
      },
    ])
) {
  throw new Error("foreign-origin page obtained browser stream authority");
}
assertSecretAudit("foreign-origin", foreignResult.audit);

authorCredential = "";

const summarizeAudit = (audit: any) => ({
  no_secret_detected: audit.noSecretDetected,
  history_entry_enumeration_authoritative:
    audit.history.entryEnumerationAuthoritative,
  cookie_setter_instrumented: audit.cookies.setterInstrumented,
  indexed_db: {
    available: audit.indexedDb.available,
    authoritative: audit.indexedDb.authoritative,
    databases: audit.indexedDb.databaseCount,
  },
  cache_api: {
    available: audit.cacheApi.available,
    authoritative: audit.cacheApi.authoritative,
    caches: audit.cacheApi.cacheCount,
  },
  service_workers: {
    available: audit.serviceWorkers.available,
    authoritative: audit.serviceWorkers.authoritative,
    registrations: audit.serviceWorkers.registrationCount,
  },
  console_calls: audit.console.calls,
  page_errors: audit.pageFailures.errors,
  unhandled_rejections: audit.pageFailures.unhandledRejections,
});

console.log(
  JSON.stringify({
    bun_version: Bun.version,
    backend: "webkit",
    fixture: {
      fresh_channel: true,
      replay_messages_accepted: 1,
      live_messages_accepted: 1,
    },
    csp: {
      exact_policy: true,
      same_origin_connect_succeeded: true,
      set_cookie_headers: httpEvidence.setCookieHeaders,
    },
    same_origin: {
      outcome: sameOriginResult.outcome,
      protocol: sameOriginResult.selectedProtocol,
      first_frames: sameOriginResult.frameTypes,
      operations: sameOriginResult.operations.map((operation: any) => ({
        ...operation,
        path: operation.path.replace(channelId, "<channel-id>"),
      })),
      audit: summarizeAudit(sameOriginResult.audit),
    },
    hostname_alias: {
      page_origin: aliasResult.pageOrigin,
      socket_opened: aliasResult.socketOpened,
      application_frames: aliasResult.applicationFrames,
    },
    foreign_origin: {
      page_origin: foreignResult.pageOrigin,
      adapter_application_frames: foreignResult.adapterApplicationFrames,
      direct_socket_opened: foreignResult.directSocketOpened,
      direct_application_frames: foreignResult.directApplicationFrames,
    },
  }),
);
