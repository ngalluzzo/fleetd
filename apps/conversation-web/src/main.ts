import {
  ConversationSession,
  type ConversationSnapshot,
} from "../../../clients/typescript/src/conversation-session.ts";
import { createBrowserConversationTransport } from "../../../clients/typescript/src/conversation-transport.ts";
import type { ConversationPresentationContract } from "./presentation-contract.ts";
import {
  applyComposerAvailability,
  composerAvailability,
  isComposerSendShortcut,
  resizeComposer,
} from "./ui/composer.ts";
import {
  MessageListView,
  renderChannelHeader,
  renderChannelList,
  renderConnectionStatus,
  renderEmptyConversation,
  renderMemberTargets,
} from "./ui/components.ts";

interface ConnectionProfile {
  participantId: string;
  operatorCredential: string;
  participantCredential: string;
  requestKind: string;
  resultKind: string;
  channelId?: string;
}

interface PublicConversationApp {
  connect(profile: ConnectionProfile): Promise<void>;
  disconnect(): void;
  inspect(): Record<string, unknown>;
}

declare global {
  interface Window {
    __fleetdConversation?: PublicConversationApp;
  }
}

const elements = {
  connectPanel: required<HTMLElement>("connect-panel"),
  connectForm: required<HTMLFormElement>("connect-form"),
  operatorCredential: required<HTMLInputElement>("operator-credential"),
  participantCredential: required<HTMLInputElement>("participant-credential"),
  participantId: required<HTMLInputElement>("participant-id"),
  requestKind: required<HTMLInputElement>("request-kind"),
  resultKind: required<HTMLInputElement>("result-kind"),
  app: required<HTMLElement>("conversation-app"),
  status: required<HTMLElement>("connection-status"),
  channels: required<HTMLElement>("channel-list"),
  channelTitle: required<HTMLElement>("channel-title"),
  channelMeta: required<HTMLElement>("channel-meta"),
  messages: required<HTMLElement>("message-list"),
  empty: required<HTMLElement>("empty-conversation"),
  emptyTitle: required<HTMLElement>("empty-conversation-title"),
  emptyCopy: required<HTMLElement>("empty-conversation-copy"),
  target: required<HTMLSelectElement>("message-target"),
  composer: required<HTMLFormElement>("composer"),
  composerText: required<HTMLTextAreaElement>("composer-text"),
  send: required<HTMLButtonElement>("send-message"),
  disconnect: required<HTMLButtonElement>("disconnect"),
};
const connectSubmit = requiredDescendant<HTMLButtonElement>(
  elements.connectForm,
  'button[type="submit"]',
);
const connectSubmitLabel = requiredDescendant<HTMLElement>(
  connectSubmit,
  ".button-label",
);
const connectSubmitIcon = requiredDescendant<HTMLElement>(
  connectSubmit,
  ".button-icon",
);

let session: ConversationSession | undefined;
let unsubscribe: (() => void) | undefined;
let contract: ConversationPresentationContract | undefined;
let latestSnapshot: ConversationSnapshot | undefined;
let renderFrame: number | undefined;
let sendInFlight = false;
let connectInFlight = false;
let appGeneration = 0;
const messageList = new MessageListView(elements.messages);

const publicApp: PublicConversationApp = {
  connect,
  disconnect,
  inspect() {
    const snapshot = latestSnapshot;
    return {
      connected: session !== undefined,
      phase: snapshot?.phase ?? "disconnected",
      participant_id: snapshot?.participantId ?? null,
      selected_channel_id: snapshot?.selectedChannelId ?? null,
      cursor: snapshot?.cursor ?? 0,
      channel_count: snapshot?.channels.length ?? 0,
      member_count: snapshot?.members.length ?? 0,
      message_ids: snapshot?.messages.map((message) => message.id) ?? [],
      message_sequences: snapshot?.messages.map((message) => message.seq) ?? [],
      pending_sends: snapshot?.pendingSends ?? 0,
      error_code: snapshot?.error?.code ?? null,
    };
  },
};

Object.defineProperty(window, "__fleetdConversation", {
  configurable: false,
  enumerable: false,
  writable: false,
  value: publicApp,
});
document.documentElement.dataset.fleetdConversationReady = "true";

elements.connectForm.addEventListener("submit", (event) => {
  event.preventDefault();
  if (connectInFlight) return;
  const profile: ConnectionProfile = {
    participantId: elements.participantId.value,
    operatorCredential: elements.operatorCredential.value,
    participantCredential: elements.participantCredential.value,
    requestKind: elements.requestKind.value,
    resultKind: elements.resultKind.value,
  };
  elements.operatorCredential.value = "";
  elements.participantCredential.value = "";
  connectInFlight = true;
  setConnectBusy(true);
  void connect(profile)
    .catch(() => {
      showConnectError("Check your workspace and participant keys, then try again.");
    })
    .finally(() => {
      connectInFlight = false;
      setConnectBusy(false);
    });
});

elements.disconnect.addEventListener("click", disconnect);
elements.channels.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest<HTMLButtonElement>("button[data-channel-id]");
  if (!button?.dataset.channelId || !session) return;
  void session.selectChannel(button.dataset.channelId).catch(() => {
    if (session) scheduleRender(session.snapshot);
  });
});
elements.composer.addEventListener("submit", (event) => {
  event.preventDefault();
  void sendComposerMessage();
});
elements.composerText.addEventListener("input", () => {
  resizeComposer(elements.composerText);
  renderComposerAvailability();
});
elements.composerText.addEventListener("keydown", (event) => {
  if (!isComposerSendShortcut(event)) return;
  event.preventDefault();
  elements.composer.requestSubmit();
});
elements.target.addEventListener("change", () => {
  renderComposerAvailability();
  renderComposerContext();
  const selected = elements.target.selectedOptions[0];
  if (selected?.title) elements.target.title = selected.title;
});
resizeComposer(elements.composerText);

async function connect(profileInput: ConnectionProfile): Promise<void> {
  disconnect();
  const generation = appGeneration;
  let profile: ConnectionProfile;
  try {
    profile = { ...validateProfile(profileInput) };
  } finally {
    clearProfileCredentials(profileInput);
  }
  required<HTMLElement>("connect-error").hidden = true;
  contract = {
    requestKind: profile.requestKind,
    resultKind: profile.resultKind,
  };
  const transport = (() => {
    try {
      return createBrowserConversationTransport({
        origin: window.location.origin,
        participantId: profile.participantId,
        operatorCredential: profile.operatorCredential,
        participantCredential: profile.participantCredential,
      });
    } finally {
      clearProfileCredentials(profile);
    }
  })();
  const activeSession = new ConversationSession(transport);
  session = activeSession;
  unsubscribe = activeSession.subscribe(scheduleRender);
  elements.connectPanel.hidden = true;
  elements.app.hidden = false;
  try {
    await activeSession.start();
    if (generation !== appGeneration || session !== activeSession) {
      throw new Error("Fleetd conversation connection was superseded");
    }
    const channelId = profile.channelId;
    if (channelId) await activeSession.selectChannel(channelId);
  } catch {
    if (generation === appGeneration && session === activeSession) disconnect();
    throw new Error("Fleetd conversation connection failed");
  }
}

function disconnect(): void {
  appGeneration += 1;
  sendInFlight = false;
  unsubscribe?.();
  unsubscribe = undefined;
  session?.close();
  session = undefined;
  contract = undefined;
  latestSnapshot = undefined;
  if (renderFrame !== undefined) cancelAnimationFrame(renderFrame);
  renderFrame = undefined;
  elements.app.hidden = true;
  elements.connectPanel.hidden = false;
  messageList.clear();
  elements.channels.replaceChildren();
  elements.target.replaceChildren();
  elements.composerText.value = "";
  resizeComposer(elements.composerText);
  renderComposerAvailability();
}

function scheduleRender(snapshot: ConversationSnapshot): void {
  latestSnapshot = snapshot;
  if (renderFrame !== undefined) return;
  renderFrame = requestAnimationFrame(() => {
    renderFrame = undefined;
    if (latestSnapshot) render(latestSnapshot);
  });
}

function render(snapshot: ConversationSnapshot): void {
  renderConnectionStatus(elements.status, snapshot, sendInFlight);
  renderChannelList(
    elements.channels,
    snapshot.channels,
    snapshot.selectedChannelId,
    snapshot,
  );
  renderChannelHeader(snapshot, {
    title: elements.channelTitle,
    meta: elements.channelMeta,
  });
  renderMemberTargets(elements.target, snapshot.members, snapshot.participantId);
  renderComposerContext();
  messageList.render(snapshot, requiredContract());
  renderEmptyConversation(snapshot, {
    root: elements.empty,
    title: elements.emptyTitle,
    copy: elements.emptyCopy,
  });
  renderComposerAvailability();
}

async function sendComposerMessage(): Promise<void> {
  const activeSession = session;
  if (!activeSession || !contract || sendInFlight) return;
  const draft = elements.composerText.value;
  const text = draft.trim();
  const recipientId = elements.target.value;
  if (!text || !recipientId) return;
  const turnId = crypto.randomUUID();
  const generation = appGeneration;
  sendInFlight = true;
  renderConnectionStatus(elements.status, activeSession.snapshot, true);
  renderComposerAvailability();
  try {
    await activeSession.send({
      idempotency_key: `fleetd-conversation/${turnId}`,
      recipient_id: recipientId,
      kind: contract.requestKind,
      payload: { text },
      correlation_id: turnId,
      causation_id: null,
    });
    if (
      generation === appGeneration &&
      session === activeSession &&
      elements.composerText.value === draft
    ) {
      elements.composerText.value = "";
      resizeComposer(elements.composerText);
    }
  } catch {
    if (generation === appGeneration && session === activeSession) {
      elements.composerText.focus();
    }
  } finally {
    if (generation === appGeneration && session === activeSession) {
      sendInFlight = false;
      scheduleRender(activeSession.snapshot);
      renderComposerAvailability();
      elements.composerText.focus();
    }
  }
}

function renderComposerAvailability(): void {
  const snapshot = latestSnapshot;
  const availability = composerAvailability({
    phase: snapshot?.phase ?? "closed",
    selectedChannelId: snapshot?.selectedChannelId ?? null,
    targetId: elements.target.value,
    draft: elements.composerText.value,
    pendingSends: snapshot?.pendingSends ?? 0,
    sending: sendInFlight,
  });
  applyComposerAvailability(availability, {
    form: elements.composer,
    textarea: elements.composerText,
    target: elements.target,
    send: elements.send,
  });
}

function renderComposerContext(): void {
  const recipient = elements.target.selectedOptions[0]?.textContent?.trim();
  elements.composerText.placeholder = recipient
    ? `Message ${recipient}…`
    : "Write a message…";
}

function validateProfile(value: ConnectionProfile): ConnectionProfile {
  if (!value || typeof value !== "object") throw new Error("profile required");
  boundedProfileField(value.participantId, "participantId", 256);
  boundedProfileField(value.operatorCredential, "operatorCredential", 4_096);
  boundedProfileField(
    value.participantCredential,
    "participantCredential",
    4_096,
  );
  boundedProfileField(value.requestKind, "requestKind", 256);
  boundedProfileField(value.resultKind, "resultKind", 256);
  if (
    value.channelId !== undefined &&
    (typeof value.channelId !== "string" ||
      value.channelId.trim().length === 0 ||
      value.channelId.length > 256)
  ) {
    throw new Error("invalid conversation profile field: channelId");
  }
  return value;
}

function boundedProfileField(
  value: string,
  name: string,
  maximumLength: number,
): void {
  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    value.length > maximumLength
  ) {
    throw new Error(`invalid conversation profile field: ${name}`);
  }
}

function clearProfileCredentials(profile: ConnectionProfile): void {
  try {
    profile.operatorCredential = "";
    profile.participantCredential = "";
  } catch {
    // A frozen native-host input cannot be scrubbed; it remains caller-owned.
  }
}

function showConnectError(message: string): void {
  const output = required<HTMLElement>("connect-error");
  output.textContent = message;
  output.hidden = false;
}

function setConnectBusy(busy: boolean): void {
  elements.connectForm.setAttribute("aria-busy", String(busy));
  connectSubmit.disabled = busy;
  connectSubmitLabel.textContent = busy
    ? "Connecting…"
    : "Continue to conversations";
  connectSubmitIcon.textContent = busy ? "…" : "→";
}

function requiredContract(): ConversationPresentationContract {
  if (!contract) throw new Error("conversation presentation is disconnected");
  return contract;
}

function required<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`missing conversation element: ${id}`);
  return element as T;
}

function requiredDescendant<T extends HTMLElement>(
  parent: ParentNode,
  selector: string,
): T {
  const element = parent.querySelector(selector);
  if (!element) throw new Error(`missing conversation element: ${selector}`);
  return element as T;
}
