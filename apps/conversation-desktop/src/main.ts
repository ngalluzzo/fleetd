import { BrowserWindow } from "electrobun/main";
import { homedir } from "node:os";
import { join } from "node:path";
import { buildConversationBootstrap } from "./bootstrap.ts";
import { loadDesktopProfile, parseProfilePath } from "./desktop-profile.ts";

const defaultProfilePath = join(
  homedir(),
  ".fleetd",
  "conversation-desktop.json",
);

try {
  const configuredProfilePath =
    process.env["FLEETD_CONVERSATION_PROFILE"] ?? defaultProfilePath;
  const profilePath = parseProfilePath(
    process.argv.slice(2),
    configuredProfilePath,
  );
  const profile = await loadDesktopProfile(profilePath);
  const supervisor =
    profile.schemaVersion === 2 &&
    profile.fleetdExecutable &&
    profile.fleetConfigFile &&
    profile.workerProfilesFile
      ? Bun.spawn({
          cmd: [
            profile.fleetdExecutable,
            "--fleet-config",
            profile.fleetConfigFile,
            "worker",
            "supervise",
            "--profiles",
            profile.workerProfilesFile,
          ],
          stdin: "ignore",
          stdout: "inherit",
          stderr: "inherit",
          env: {},
        })
      : undefined;
  if (supervisor) {
    process.once("exit", () => supervisor.kill());
  }
  const conversationUrl = `${profile.origin}/conversation/`;
  let bootstrap = buildConversationBootstrap({
    participantId: profile.participantId,
    operatorCredential: profile.operatorCredential,
    participantCredential: profile.participantCredential,
    requestKind: profile.requestKind,
    resultKind: profile.resultKind,
    runtimeProfiles: profile.runtimeProfiles,
    ...(profile.channelId === undefined ? {} : { channelId: profile.channelId }),
  });
  profile.operatorCredential = "";
  profile.participantCredential = "";

  const window = new BrowserWindow({
    title: "Fleetd Conversation",
    url: conversationUrl,
    renderer: "native",
    frame: { width: 1_180, height: 780 },
    hidden: false,
    sandbox: true,
    spellCheck: true,
    navigationRules: JSON.stringify([conversationUrl, `${conversationUrl}*`]),
  });
  window.show();
  window.activate();
  let handedOff = false;
  window.webview.on("dom-ready", () => {
    if (!handedOff) {
      handedOff = true;
      window.webview.executeJavascript(bootstrap);
      bootstrap = "";
    }
  });
} catch (error) {
  const message = error instanceof Error ? error.message : "unknown startup error";
  console.error(`Fleetd Conversation could not start: ${message}`);
  process.exitCode = 1;
}
