// Qualifies the browser-only WebSocket edge against a running loopback Fleetd
// daemon using Bun's native macOS WebKit view. The daemon must serve its
// operator page at FLEETD_BROWSER_QUALIFICATION_ORIGIN.
const protocol = "fleetd.channel-stream.browser.v1";
const browserStreamPath = "/v1/browser/channel-stream";
const origin =
  process.env.FLEETD_BROWSER_QUALIFICATION_ORIGIN ??
  "http://127.0.0.1:7419";
const pageUrl = new URL("/operator/", origin);
const socketUrl = new URL(browserStreamPath, origin);
socketUrl.protocol = pageUrl.protocol === "https:" ? "wss:" : "ws:";

if (
  !pageUrl.hostname ||
  !["127.0.0.1", "::1", "[::1]"].includes(pageUrl.hostname)
) {
  throw new Error("browser stream qualification requires an exact loopback origin");
}
if (!pageUrl.port) {
  throw new Error("browser stream qualification requires an explicit bound port");
}

await using view = new Bun.WebView({
  backend: "webkit",
  dataStore: "ephemeral",
});
await view.navigate(pageUrl.href);
await view.evaluate("document.readyState");
await view.evaluate(`
  (() => {
    const root = document.documentElement;
    const socket = new WebSocket(${JSON.stringify(socketUrl.href)}, ${JSON.stringify(protocol)});
    root.setAttribute("data-fleetd-browser-stream-qualification", JSON.stringify({
      outcome: "pending",
      applicationFrameBeforeRedemption: false,
    }));
    socket.addEventListener("message", () => {
      const state = JSON.parse(
        root.getAttribute("data-fleetd-browser-stream-qualification"),
      );
      state.applicationFrameBeforeRedemption = true;
      root.setAttribute(
        "data-fleetd-browser-stream-qualification",
        JSON.stringify(state),
      );
    });
    socket.addEventListener("open", () => {
      const state = JSON.parse(
        root.getAttribute("data-fleetd-browser-stream-qualification"),
      );
      root.setAttribute(
        "data-fleetd-browser-stream-qualification",
        JSON.stringify({
          outcome: "open",
          protocol: socket.protocol,
          url: socket.url,
          applicationFrameBeforeRedemption:
            state.applicationFrameBeforeRedemption,
        }),
      );
    }, { once: true });
    socket.addEventListener("error", () => {
      root.setAttribute(
        "data-fleetd-browser-stream-qualification",
        JSON.stringify({ outcome: "error" }),
      );
    }, { once: true });
    Reflect.set(root, "__fleetdQualificationSocket", socket);
    return true;
  })()
`);

const readResult = () =>
  view.evaluate(
    "JSON.parse(document.documentElement.getAttribute('data-fleetd-browser-stream-qualification'))",
  );
const deadline = performance.now() + 2_000;
let result = await readResult();
while (result?.outcome === "pending" && performance.now() < deadline) {
  await Bun.sleep(10);
  result = await readResult();
}

await Bun.sleep(100);
result = await readResult();
await view.evaluate(
  "(() => { Reflect.get(document.documentElement, '__fleetdQualificationSocket')?.close(); return true; })()",
);

if (
  result?.outcome !== "open" ||
  result?.protocol !== protocol ||
  result?.url !== socketUrl.href ||
  result?.applicationFrameBeforeRedemption !== false
) {
  throw new Error(
    `browser channel-stream qualification failed: ${JSON.stringify(result)}`,
  );
}

console.log(
  JSON.stringify({
    bun_version: Bun.version,
    backend: "webkit",
    ...result,
  }),
);
