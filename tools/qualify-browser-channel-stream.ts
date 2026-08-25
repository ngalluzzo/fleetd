// Qualifies the actual presentation-agnostic browser stream client against a
// running loopback Fleetd daemon using Bun's native macOS WebKit view.
const protocol = "fleetd.channel-stream.browser.v1";
const browserStreamPath = "/v1/browser/channel-stream";
const origin =
  process.env.FLEETD_BROWSER_QUALIFICATION_ORIGIN ??
  "http://127.0.0.1:7419";
const credential = process.env.FLEETD_BROWSER_QUALIFICATION_CREDENTIAL;
const channelId = process.env.FLEETD_BROWSER_QUALIFICATION_CHANNEL_ID;
const pageUrl = new URL("/operator/", origin);
const socketUrl = new URL(browserStreamPath, origin);
socketUrl.protocol = pageUrl.protocol === "https:" ? "wss:" : "ws:";

if (!credential || !channelId) {
  throw new Error(
    "browser stream qualification requires an in-memory credential and channel ID",
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

await using view = new Bun.WebView({
  backend: "webkit",
  dataStore: "ephemeral",
});
await view.navigate(pageUrl.href);
await view.evaluate("document.readyState");
try {
  await view.evaluate(clientBundle);
} catch {
  throw new Error("browser stream client bundle did not evaluate in WebKit");
}
try {
  await view.evaluate(`
  (() => {
    const root = document.documentElement;
    const operations = [];
    const state = {
      outcome: "pending",
      protocol: null,
      url: null,
      firstApplicationFrameType: null,
      acceptedMessages: 0,
      operations,
    };
    const publish = () => root.setAttribute(
      "data-fleetd-browser-stream-qualification",
      JSON.stringify(state),
    );
    publish();

    const client = Reflect.get(globalThis, "__fleetdBrowserChannelStreamClient");
    if (
      !client ||
      client.protocol !== ${JSON.stringify(protocol)} ||
      client.path !== ${JSON.stringify(browserStreamPath)}
    ) {
      state.outcome = "client_linkage_error";
      publish();
      return;
    }

    try {
      const stream = client.open({
        origin: location.origin,
        channelId: ${JSON.stringify(channelId)},
        credential: ${JSON.stringify(credential)},
        after: 0,
        reconnectDelaysMs: [],
        accept() {
          state.acceptedMessages += 1;
          publish();
        },
        fetch(input, init) {
          const url = new URL(String(input), location.origin);
          operations.push({
            kind: "fetch",
            method: init.method,
            path: url.pathname,
          });
          publish();
          return globalThis.fetch(input, init);
        },
        createWebSocket(url, requestedProtocol) {
          operations.push({
            kind: "websocket",
            path: new URL(url).pathname,
            protocol: requestedProtocol,
          });
          const socket = new WebSocket(url, requestedProtocol);
          socket.addEventListener("open", () => {
            state.protocol = socket.protocol;
            state.url = socket.url;
            publish();
          });
          socket.addEventListener("message", (event) => {
            if (state.firstApplicationFrameType === null) {
              try {
                state.firstApplicationFrameType = JSON.parse(event.data).type;
              } catch {
                state.firstApplicationFrameType = "invalid";
              }
              state.outcome =
                state.firstApplicationFrameType === "ready" ? "ready" : "error";
              publish();
            }
          });
          return socket;
        },
      });
      stream.closed.catch((error) => {
        if (state.outcome === "pending") {
          state.outcome = "error";
          state.errorCode = error?.code ?? "unknown";
          publish();
        }
      });
      Reflect.set(root, "__fleetdQualificationStream", stream);
    } catch {
      state.outcome = "error";
      publish();
    }
  })()
`);
} catch {
  // WebView evaluation errors may retain source text. The bootstrap contains
  // the in-memory credential, so never attach or print the raw cause.
  throw new Error("browser stream client bootstrap did not evaluate in WebKit");
}

const readResult = () =>
  view.evaluate(
    "JSON.parse(document.documentElement.getAttribute('data-fleetd-browser-stream-qualification'))",
  );
const deadline = performance.now() + 5_000;
let result = await readResult();
while (result?.outcome === "pending" && performance.now() < deadline) {
  await Bun.sleep(10);
  result = await readResult();
}

await Bun.sleep(100);
result = await readResult();
await view.evaluate(
  "(() => { Reflect.get(document.documentElement, '__fleetdQualificationStream')?.close(); return true; })()",
);

const expectedOperations = [
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
if (
  result?.outcome !== "ready" ||
  result?.protocol !== protocol ||
  result?.url !== socketUrl.href ||
  result?.firstApplicationFrameType !== "ready" ||
  result?.acceptedMessages !== 0 ||
  JSON.stringify(result?.operations) !== JSON.stringify(expectedOperations)
) {
  throw new Error(
    `browser channel-stream client qualification failed: ${JSON.stringify(result)}`,
  );
}

console.log(
  JSON.stringify({
    bun_version: Bun.version,
    backend: "webkit",
    ...result,
  }),
);
