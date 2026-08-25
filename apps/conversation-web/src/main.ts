import {
  ConversationSession,
  type ConversationSnapshot,
} from "../../../clients/typescript/src/conversation-session.ts";
import { createBrowserConversationTransport } from "../../../clients/typescript/src/conversation-transport.ts";
import type {
  Channel,
  ChannelMember,
  Message,
} from "../../../clients/typescript/src/generated/types.gen.ts";
import {
  renderMessageBody,
  type ConversationPresentationContract,
} from "./presentation-contract.ts";

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

let session: ConversationSession | undefined;
let unsubscribe: (() => void) | undefined;
let contract: ConversationPresentationContract | undefined;
let latestSnapshot: ConversationSnapshot | undefined;
let renderFrame: number | undefined;

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
  const profile: ConnectionProfile = {
    participantId: elements.participantId.value,
    operatorCredential: elements.operatorCredential.value,
    participantCredential: elements.participantCredential.value,
    requestKind: elements.requestKind.value,
    resultKind: elements.resultKind.value,
  };
  elements.operatorCredential.value = "";
  elements.participantCredential.value = "";
  void connect(profile).catch(() => {
    showConnectError("Could not connect with the supplied Fleetd authorities.");
  });
});

elements.disconnect.addEventListener("click", disconnect);
elements.channels.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest<HTMLButtonElement>("button[data-channel-id]");
  if (!button?.dataset.channelId || !session) return;
  void session.selectChannel(button.dataset.channelId).catch(() => {});
});
elements.composer.addEventListener("submit", (event) => {
  event.preventDefault();
  void sendComposerMessage();
});
elements.composerText.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" || event.shiftKey || event.isComposing) return;
  event.preventDefault();
  elements.composer.requestSubmit();
});

async function connect(profileInput: ConnectionProfile): Promise<void> {
  disconnect();
  const profile = validateProfile(profileInput);
  required<HTMLElement>("connect-error").hidden = true;
  contract = {
    requestKind: profile.requestKind,
    resultKind: profile.resultKind,
  };
  const transport = createBrowserConversationTransport({
    origin: window.location.origin,
    participantId: profile.participantId,
    operatorCredential: profile.operatorCredential,
    participantCredential: profile.participantCredential,
  });
  try {
    profileInput.operatorCredential = "";
    profileInput.participantCredential = "";
  } catch {
    // A frozen native-host input may not be mutable; transport owns its copy.
  }
  session = new ConversationSession(transport);
  unsubscribe = session.subscribe(scheduleRender);
  elements.connectPanel.hidden = true;
  elements.app.hidden = false;
  try {
    await session.start();
    const channelId = profile.channelId;
    if (channelId) await session.selectChannel(channelId);
  } catch {
    if (session.snapshot.phase !== "failed") disconnect();
    throw new Error("Fleetd conversation connection failed");
  }
}

function disconnect(): void {
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
  elements.messages.replaceChildren();
  elements.channels.replaceChildren();
  elements.target.replaceChildren();
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
  renderStatus(snapshot);
  renderChannels(snapshot.channels, snapshot.selectedChannelId);
  renderMembers(snapshot);
  renderMessages(snapshot);
  const selectedTarget = elements.target.value;
  const canSend =
    snapshot.selectedChannelId !== null &&
    selectedTarget !== "" &&
    snapshot.phase !== "failed" &&
    snapshot.phase !== "closed";
  elements.composerText.disabled = !canSend;
  elements.send.disabled = !canSend || snapshot.pendingSends > 0;
}

function renderStatus(snapshot: ConversationSnapshot): void {
  const labels: Record<string, string> = {
    idle: "not connected",
    loading_channels: "loading channels",
    ready: "choose a channel",
    connecting: "connecting stream",
    live: "live",
    reconnecting: "reconnecting",
    failed: "connection failed",
    closed: "closed",
  };
  elements.status.textContent = labels[snapshot.phase] ?? snapshot.phase;
  elements.status.dataset.phase = snapshot.phase;
  elements.status.title = snapshot.error?.message ?? "Local transport state";
}

function renderChannels(
  channels: readonly Channel[],
  selectedChannelId: string | null,
): void {
  const nodes = channels.map((channel) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "channel-button";
    button.dataset.channelId = channel.id;
    button.setAttribute(
      "aria-pressed",
      String(channel.id === selectedChannelId),
    );
    const marker = document.createElement("span");
    marker.className = "channel-marker";
    marker.textContent = "#";
    const label = document.createElement("span");
    label.textContent = channel.name;
    button.append(marker, label);
    return button;
  });
  elements.channels.replaceChildren(...nodes);
}

function renderMembers(snapshot: ConversationSnapshot): void {
  const channel = snapshot.channels.find(
    (candidate) => candidate.id === snapshot.selectedChannelId,
  );
  elements.channelTitle.textContent = channel?.name ?? "Select a channel";
  elements.channelMeta.textContent = channel
    ? `${snapshot.members.length} participants · cursor ${snapshot.cursor}`
    : "Durable human-to-agent conversations";

  const prior = elements.target.value;
  const candidates = snapshot.members.filter(
    (member) => member.agent_id !== snapshot.participantId,
  );
  const options = candidates.map((member) => {
    const option = document.createElement("option");
    option.value = member.agent_id;
    option.textContent = `${member.agent_name} · ${member.delivery_mode}`;
    return option;
  });
  elements.target.replaceChildren(...options);
  if (candidates.some((member) => member.agent_id === prior)) {
    elements.target.value = prior;
  } else {
    const inbox = candidates.find((member) => member.delivery_mode === "inbox");
    elements.target.value = inbox?.agent_id ?? candidates[0]?.agent_id ?? "";
  }
}

function renderMessages(snapshot: ConversationSnapshot): void {
  const wasNearBottom =
    elements.messages.scrollHeight -
      elements.messages.scrollTop -
      elements.messages.clientHeight <
    96;
  const names = new Map(
    snapshot.members.map((member) => [member.agent_id, member.agent_name]),
  );
  const nodes = snapshot.messages.map((message) =>
    messageNode(message, snapshot.participantId, names),
  );
  elements.messages.replaceChildren(...nodes);
  elements.empty.hidden = snapshot.messages.length !== 0;
  const selected = snapshot.selectedChannelId !== null;
  elements.emptyTitle.textContent = selected
    ? "Start the conversation"
    : "Choose a channel";
  elements.emptyCopy.textContent = selected
    ? "Send the first durable message to an agent."
    : "History and new replies will arrive through one live cursor.";
  if (wasNearBottom)
    elements.messages.scrollTop = elements.messages.scrollHeight;
}

function messageNode(
  message: Message,
  participantId: string,
  names: ReadonlyMap<string, string>,
): HTMLElement {
  const article = document.createElement("article");
  article.className =
    message.sender_id === participantId ? "message message-self" : "message";
  article.dataset.messageId = message.id;

  const header = document.createElement("header");
  const sender = document.createElement("strong");
  sender.textContent =
    message.sender_id === participantId
      ? "you"
      : (names.get(message.sender_id) ?? shortId(message.sender_id));
  const kind = document.createElement("code");
  kind.textContent = message.kind;
  const time = document.createElement("time");
  time.dateTime = new Date(message.created_at_ms).toISOString();
  time.textContent = new Date(message.created_at_ms).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
  header.append(sender, kind, time);

  const rendered = renderMessageBody(message, requiredContract());
  const body = document.createElement(rendered.format === "json" ? "pre" : "p");
  body.className = rendered.format === "json" ? "message-json" : "message-text";
  body.textContent = rendered.text;

  const footer = document.createElement("footer");
  if (rendered.status) {
    const status = document.createElement("span");
    status.className = "result-status";
    status.textContent = rendered.status;
    footer.append(status);
  }
  const details = document.createElement("details");
  const summary = document.createElement("summary");
  summary.textContent = `envelope · seq ${message.seq}`;
  const envelope = document.createElement("pre");
  envelope.textContent = JSON.stringify(message, null, 2);
  details.append(summary, envelope);
  footer.append(details);
  article.append(header, body, footer);
  return article;
}

async function sendComposerMessage(): Promise<void> {
  const activeSession = session;
  if (!activeSession || !contract) return;
  const text = elements.composerText.value.trim();
  const recipientId = elements.target.value;
  if (!text || !recipientId) return;
  const turnId = crypto.randomUUID();
  try {
    await activeSession.send({
      idempotency_key: `fleetd-conversation/${turnId}`,
      recipient_id: recipientId,
      kind: contract.requestKind,
      payload: { text },
      correlation_id: turnId,
      causation_id: null,
    });
    elements.composerText.value = "";
    elements.composerText.focus();
  } catch {
    elements.status.title = "The message was not accepted by Fleetd";
  }
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

function showConnectError(message: string): void {
  const output = required<HTMLElement>("connect-error");
  output.textContent = message;
  output.hidden = false;
}

function requiredContract(): ConversationPresentationContract {
  if (!contract) throw new Error("conversation presentation is disconnected");
  return contract;
}

function shortId(value: string): string {
  return value.length > 12 ? `${value.slice(0, 8)}…` : value;
}

function required<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`missing conversation element: ${id}`);
  return element as T;
}
