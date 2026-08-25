export interface ConversationBootstrapProfile {
  participantId: string;
  operatorCredential: string;
  participantCredential: string;
  requestKind: string;
  resultKind: string;
  channelId?: string;
}

/** Builds one fire-and-forget handoff to the same-origin presentation. */
export function buildConversationBootstrap(
  profile: ConversationBootstrapProfile,
): string {
  const encoded = Buffer.from(JSON.stringify(profile), "utf8").toString("base64");
  return `(() => {
    let encoded = ${JSON.stringify(encoded)};
    let profile = JSON.parse(new TextDecoder().decode(Uint8Array.from(atob(encoded), (value) => value.charCodeAt(0))));
    encoded = "";
    const connect = () => {
      const app = globalThis.__fleetdConversation;
      if (!app || document.documentElement.dataset.fleetdConversationReady !== "true") return false;
      Promise.resolve(app.connect(profile)).catch(() => {
        document.documentElement.dataset.fleetdConversationHost = "failed";
      });
      profile.operatorCredential = "";
      profile.participantCredential = "";
      profile = null;
      document.documentElement.dataset.fleetdConversationHost = "connected";
      return true;
    };
    if (connect()) return;
    const observer = new MutationObserver(() => {
      if (!connect()) return;
      observer.disconnect();
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-fleetd-conversation-ready"] });
    setTimeout(() => {
      observer.disconnect();
      if (profile) {
        profile.operatorCredential = "";
        profile.participantCredential = "";
        profile = null;
        document.documentElement.dataset.fleetdConversationHost = "failed";
      }
    }, 10000);
  })();`;
}
