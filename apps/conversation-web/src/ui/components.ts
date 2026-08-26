import type { ConversationSnapshot } from "../../../../clients/typescript/src/conversation-session.ts";
import type {
  Channel,
  ChannelMember,
  Message,
} from "../../../../clients/typescript/src/generated/types.gen.ts";
import {
  renderMessageBody,
  type ConversationPresentationContract,
} from "../presentation-contract.ts";
import {
  connectionStatusView,
  displayName,
  emptyConversationView,
  memberOptionView,
  recipientLabel,
  senderLabel,
} from "./view-models.ts";

export function renderConnectionStatus(
  element: HTMLElement,
  snapshot: ConversationSnapshot,
  sending: boolean,
): void {
  const status = connectionStatusView({
    phase: snapshot.phase,
    pendingSends: snapshot.pendingSends,
    errorMessage: snapshot.error?.message,
    sending,
  });
  element.textContent = status.label;
  element.dataset.phase = snapshot.phase;
  element.dataset.activity = status.busy ? "busy" : "settled";
  element.title = status.description;
  element.setAttribute("aria-live", snapshot.phase === "failed" ? "assertive" : "polite");
  element.setAttribute("aria-atomic", "true");
  element.setAttribute("aria-busy", String(status.busy));
}

export function renderChannelList(
  container: HTMLElement,
  channels: readonly Channel[],
  selectedChannelId: string | null,
  snapshot: Pick<ConversationSnapshot, "phase" | "error">,
): void {
  container.setAttribute(
    "aria-busy",
    String(snapshot.phase === "loading_channels"),
  );
  if (channels.length === 0) {
    const state = document.createElement("p");
    state.className = `channel-state channel-state-${snapshot.phase}`;
    state.setAttribute("role", "status");
    state.textContent =
      snapshot.phase === "loading_channels"
        ? "Loading conversations…"
        : snapshot.phase === "failed"
          ? (snapshot.error?.message ?? "Conversations unavailable")
          : "No conversations available";
    container.replaceChildren(state);
    return;
  }
  const existing = new Map<string, HTMLButtonElement>();
  for (const button of container.querySelectorAll<HTMLButtonElement>(
    "button[data-channel-id]",
  )) {
    if (button.dataset.channelId) {
      existing.set(button.dataset.channelId, button);
    }
  }
  const rows = channels.map((channel) => {
    const selected = channel.id === selectedChannelId;
    const row = existing.get(channel.id) ?? channelRow(channel, selected);
    updateChannelRow(row, channel, selected);
    return row;
  });
  reconcileChildren(container, rows);
}

export function channelRow(
  channel: Channel,
  selected: boolean,
): HTMLButtonElement {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "channel-button nav-item";
  button.dataset.channelId = channel.id;

  const marker = document.createElement("span");
  marker.className = "channel-marker nav-item__icon";
  marker.append(icon("channel", "#"));

  const content = document.createElement("span");
  content.className = "nav-item__content";
  const label = document.createElement("span");
  label.className = "channel-label nav-item__label";
  label.textContent = channel.name;
  content.append(label);

  const chevron = icon("chevron", "›", "nav-item__chevron");
  button.append(marker, content, chevron);
  updateChannelRow(button, channel, selected);
  return button;
}

function updateChannelRow(
  button: HTMLButtonElement,
  channel: Channel,
  selected: boolean,
): void {
  button.title = selected ? `${channel.name}, current channel` : channel.name;
  const label = button.querySelector<HTMLElement>(".channel-label");
  if (label) label.textContent = displayName(channel.name);
  button.setAttribute("aria-pressed", String(selected));
  if (selected) {
    button.setAttribute("aria-current", "page");
  } else {
    button.removeAttribute("aria-current");
  }
}

export function renderChannelHeader(
  snapshot: ConversationSnapshot,
  elements: {
    readonly title: HTMLElement;
    readonly meta: HTMLElement;
  },
): void {
  const channel = snapshot.channels.find(
    (candidate) => candidate.id === snapshot.selectedChannelId,
  );
  elements.title.textContent = channel
    ? displayName(channel.name)
    : "Select a channel";
  elements.title.title = channel?.name ?? "";
  elements.meta.textContent = channel
    ? `${snapshot.members.length} participants · ${snapshot.messages.length} messages`
    : snapshot.phase === "loading_channels"
      ? "Finding conversations…"
      : "Choose a conversation to begin.";
}

export function renderMemberTargets(
  select: HTMLSelectElement,
  members: readonly ChannelMember[],
  participantId: string,
): string {
  const prior = select.value;
  const candidates = members
    .filter((member) => member.agent_id !== participantId)
    .map(memberOptionView);
  if (candidates.length === 0) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No other participants";
    option.disabled = true;
    select.replaceChildren(option);
    select.value = "";
    select.title = "This channel has no other message recipient.";
    return "";
  }
  const existing = new Map(
    Array.from(select.options).map((option) => [option.value, option]),
  );
  const options = candidates.map((candidate) => {
    const option = existing.get(candidate.id) ?? document.createElement("option");
    option.value = candidate.id;
    option.textContent = candidate.label;
    option.title = candidate.description;
    return option;
  });
  reconcileChildren(select, options);
  const selected = candidates.some((candidate) => candidate.id === prior)
    ? prior
    : (candidates.find((candidate) => candidate.preferred)?.id ??
      candidates[0]?.id ??
      "");
  select.value = selected;
  select.title =
    candidates.find((candidate) => candidate.id === selected)?.description ??
    "Message recipient";
  return selected;
}

export function renderEmptyConversation(
  snapshot: ConversationSnapshot,
  elements: {
    readonly root: HTMLElement;
    readonly title: HTMLElement;
    readonly copy: HTMLElement;
  },
): void {
  const state = emptyConversationView({
    selected: snapshot.selectedChannelId !== null,
    phase: snapshot.phase,
    messageCount: snapshot.messages.length,
    errorMessage: snapshot.error?.message,
  });
  elements.root.hidden = state.hidden;
  elements.root.dataset.state = state.state;
  elements.root.setAttribute("aria-busy", String(state.state === "loading"));
  elements.title.textContent = state.title;
  elements.copy.textContent = state.copy;
}

export class MessageListView {
  readonly #container: HTMLElement;
  #channelId: string | null = null;

  constructor(container: HTMLElement) {
    this.#container = container;
  }

  clear(): void {
    this.#container.replaceChildren();
    this.#channelId = null;
  }

  render(
    snapshot: ConversationSnapshot,
    contract: ConversationPresentationContract,
  ): void {
    const changedChannel = this.#channelId !== snapshot.selectedChannelId;
    const nearBottom = isNearBottom(this.#container);
    const anchor =
      !changedChannel && !nearBottom ? visibleAnchor(this.#container) : undefined;
    const names = new Map(
      snapshot.members.map((member) => [member.agent_id, member.agent_name]),
    );
    const existing = new Map<string, HTMLElement>();
    for (const child of Array.from(this.#container.children)) {
      if (!(child instanceof HTMLElement)) continue;
      const messageId = child.dataset.messageId;
      if (messageId) existing.set(messageId, child);
    }
    const nodes = snapshot.messages.map((message) => {
      const current = existing.get(message.id);
      if (current?.dataset.messageSeq === String(message.seq)) {
        updateMessageLabels(current, message, snapshot.participantId, names);
        return current;
      }
      return messageCard(message, snapshot.participantId, names, contract);
    });
    reconcileChildren(this.#container, nodes);
    this.#container.setAttribute(
      "aria-busy",
      String(
        snapshot.phase === "connecting" || snapshot.phase === "reconnecting",
      ),
    );
    this.#channelId = snapshot.selectedChannelId;
    if (changedChannel || nearBottom) {
      this.#container.scrollTop = this.#container.scrollHeight;
    } else if (anchor) {
      restoreAnchor(this.#container, anchor);
    }
  }
}

export function messageCard(
  message: Message,
  participantId: string,
  names: ReadonlyMap<string, string>,
  contract: ConversationPresentationContract,
): HTMLElement {
  const article = document.createElement("article");
  article.className =
    message.sender_id === participantId
      ? "message message-card message-self"
      : "message message-card";
  article.dataset.messageId = message.id;
  article.dataset.messageSeq = String(message.seq);
  article.dataset.direction =
    message.sender_id === participantId ? "outgoing" : "incoming";

  const avatar = document.createElement("span");
  avatar.className = "message-card__avatar";
  avatar.setAttribute("aria-hidden", "true");

  const content = document.createElement("div");
  content.className = "message-card__content";

  const header = document.createElement("header");
  header.className = "message-card__header";
  const identity = document.createElement("span");
  identity.className = "message-card__identity";
  const sender = document.createElement("strong");
  sender.className = "message-sender";
  const kind = document.createElement("code");
  kind.className = "message-kind";
  kind.textContent = messageKindLabel(message.kind, contract);
  kind.title = message.kind;
  identity.append(sender, kind);
  const time = document.createElement("time");
  const created = new Date(message.created_at_ms);
  time.dateTime = created.toISOString();
  time.textContent = created.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
  time.title = created.toLocaleString();
  header.append(identity, time);

  const rendered = renderMessageBody(message, contract);
  const body = document.createElement(rendered.format === "json" ? "pre" : "p");
  body.className =
    rendered.format === "json"
      ? "message-json message-card__body"
      : "message-text message-card__body";
  body.textContent = rendered.text;

  const footer = document.createElement("div");
  footer.className = "message-card__footer";
  const delivery = document.createElement("span");
  delivery.className = "message-card__delivery";
  if (rendered.status) {
    const status = document.createElement("span");
    status.className = "result-status";
    status.dataset.tone = statusTone(rendered.status);
    const statusLabel = document.createElement("span");
    statusLabel.className = "result-status__label";
    statusLabel.textContent = rendered.status;
    status.append(icon("status", "✓"), statusLabel);
    delivery.append(status);
  }
  const recipient = document.createElement("span");
  recipient.className = "message-recipient";
  delivery.append(recipient);
  footer.append(delivery);
  const details = document.createElement("details");
  details.className = "message-details";
  const summary = document.createElement("summary");
  summary.className = "message-details__trigger";
  summary.append(
    icon("envelope", "···"),
    text(`Details · message ${message.seq}`),
  );
  const envelope = document.createElement("pre");
  envelope.className = "message-details__envelope";
  envelope.textContent = JSON.stringify(message, null, 2);
  details.append(summary, envelope);
  footer.append(details);
  content.append(header, body, footer);
  article.append(avatar, content);
  updateMessageLabels(article, message, participantId, names);
  return article;
}

/** Product-facing label for an exact message contract retained in the tooltip. */
export function messageKindLabel(
  kind: string,
  contract: ConversationPresentationContract,
): "Message" | "Reply" | "Event" {
  if (kind === contract.requestKind) return "Message";
  if (kind === contract.resultKind) return "Reply";
  return "Event";
}

/** Restricts untrusted result status strings to a stable visual vocabulary. */
export function statusTone(
  status: string,
): "success" | "warning" | "danger" | "neutral" {
  const normalized = status.trim().toLowerCase();
  if (
    ["complete", "completed", "done", "success", "succeeded"].includes(
      normalized,
    )
  ) {
    return "success";
  }
  if (
    ["queued", "pending", "running", "working", "in_progress"].includes(
      normalized,
    )
  ) {
    return "warning";
  }
  if (
    ["error", "failed", "failure", "cancelled", "canceled"].includes(
      normalized,
    )
  ) {
    return "danger";
  }
  return "neutral";
}

function updateMessageLabels(
  article: HTMLElement,
  message: Message,
  participantId: string,
  names: ReadonlyMap<string, string>,
): void {
  const sender = displayName(senderLabel(message, participantId, names));
  const recipient = displayName(recipientLabel(message, participantId, names));
  const senderElement = article.querySelector<HTMLElement>(".message-sender");
  const recipientElement =
    article.querySelector<HTMLElement>(".message-recipient");
  const avatarElement = article.querySelector<HTMLElement>(
    ".message-card__avatar",
  );
  if (senderElement) senderElement.textContent = sender;
  if (recipientElement) recipientElement.textContent = `to ${recipient}`;
  if (senderElement) {
    senderElement.title = exactParticipantLabel(message.sender_id, names);
  }
  if (recipientElement && message.recipient_id) {
    recipientElement.title = exactParticipantLabel(
      message.recipient_id,
      names,
    );
  }
  if (avatarElement) avatarElement.textContent = avatarLabel(sender);
  const outgoing = message.sender_id === participantId;
  article.classList.toggle("message-self", outgoing);
  article.dataset.direction = outgoing ? "outgoing" : "incoming";
  article.setAttribute("aria-label", `Message from ${sender} to ${recipient}`);
}

function avatarLabel(sender: string): string {
  const meaningful = sender.trim().replace(/^@/, "");
  return meaningful.slice(0, 1).toLocaleUpperCase() || "·";
}

function exactParticipantLabel(
  id: string,
  names: ReadonlyMap<string, string>,
): string {
  const name = names.get(id);
  return name ? `${name} · ${id}` : id;
}

function icon(name: string, glyph: string, extraClass = ""): HTMLSpanElement {
  const element = document.createElement("span");
  element.className = ["ui-icon", `ui-icon-${name}`, extraClass]
    .filter(Boolean)
    .join(" ");
  element.setAttribute("aria-hidden", "true");
  element.textContent = glyph;
  return element;
}

function text(value: string): Text {
  return document.createTextNode(value);
}

function isNearBottom(container: HTMLElement): boolean {
  return (
    container.scrollHeight - container.scrollTop - container.clientHeight < 96
  );
}

interface ScrollAnchor {
  readonly messageId: string;
  readonly top: number;
}

function visibleAnchor(container: HTMLElement): ScrollAnchor | undefined {
  const containerTop = container.getBoundingClientRect().top;
  for (const child of Array.from(container.children)) {
    if (!(child instanceof HTMLElement) || !child.dataset.messageId) continue;
    const top = child.getBoundingClientRect().top;
    if (child.getBoundingClientRect().bottom >= containerTop) {
      return { messageId: child.dataset.messageId, top };
    }
  }
  return undefined;
}

function restoreAnchor(container: HTMLElement, anchor: ScrollAnchor): void {
  const element = Array.from(container.children).find(
    (child) =>
      child instanceof HTMLElement &&
      child.dataset.messageId === anchor.messageId,
  );
  if (!(element instanceof HTMLElement)) return;
  container.scrollTop += element.getBoundingClientRect().top - anchor.top;
}

function reconcileChildren(
  container: HTMLElement,
  desired: readonly Element[],
): void {
  for (const [index, element] of desired.entries()) {
    const current = container.children.item(index);
    if (current !== element) container.insertBefore(element, current);
  }
  while (container.children.length > desired.length) {
    container.lastElementChild?.remove();
  }
}
