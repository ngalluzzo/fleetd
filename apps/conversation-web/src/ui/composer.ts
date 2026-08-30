import type { ConversationSessionPhase } from "@fleetd/client/conversation";

export interface ComposerAvailabilityInput {
  readonly phase: ConversationSessionPhase;
  readonly selectedChannelId: string | null;
  readonly draft: string;
  readonly pendingSends: number;
  readonly sending: boolean;
}

export interface ComposerAvailability {
  readonly textareaDisabled: boolean;
  readonly sendDisabled: boolean;
  readonly sending: boolean;
}

export function composerAvailability(
  input: ComposerAvailabilityInput,
): ComposerAvailability {
  const channelReady =
    input.phase === "live" && input.selectedChannelId !== null;
  const sending = input.sending || input.pendingSends > 0;
  return {
    textareaDisabled: !channelReady,
    sendDisabled: !channelReady || input.draft.trim() === "" || sending,
    sending,
  };
}

export function isComposerSendShortcut(event: {
  readonly key: string;
  readonly shiftKey: boolean;
  readonly ctrlKey: boolean;
  readonly altKey: boolean;
  readonly metaKey: boolean;
  readonly isComposing: boolean;
}): boolean {
  return (
    event.key === "Enter" &&
    !event.shiftKey &&
    !event.ctrlKey &&
    !event.altKey &&
    !event.metaKey &&
    !event.isComposing
  );
}

export function resizeComposer(textarea: HTMLTextAreaElement): void {
  textarea.style.height = "auto";
  const maximum = Number.parseFloat(getComputedStyle(textarea).maxHeight);
  const nextHeight = Number.isFinite(maximum)
    ? Math.min(textarea.scrollHeight, maximum)
    : textarea.scrollHeight;
  textarea.style.height = `${nextHeight}px`;
  textarea.style.overflowY = textarea.scrollHeight > nextHeight ? "auto" : "hidden";
}

export function applyComposerAvailability(
  availability: ComposerAvailability,
  elements: {
    readonly form: HTMLFormElement;
    readonly textarea: HTMLTextAreaElement;
    readonly send: HTMLButtonElement;
  },
): void {
  elements.textarea.disabled = availability.textareaDisabled;
  elements.send.disabled = availability.sendDisabled;
  elements.form.setAttribute("aria-busy", String(availability.sending));
  elements.send.setAttribute("aria-busy", String(availability.sending));
  elements.send.setAttribute(
    "aria-label",
    availability.sending ? "Sending message" : "Send message",
  );
  const label =
    elements.send.querySelector<HTMLElement>(".send-button-label") ??
    sendLabel();
  const icon =
    elements.send.querySelector<HTMLElement>(".ui-icon-send") ?? arrowIcon();
  label.textContent = availability.sending ? "Sending…" : "Send";
  if (label.parentElement !== elements.send || icon.parentElement !== elements.send) {
    elements.send.replaceChildren(label, icon);
  }
}

function sendLabel(): HTMLSpanElement {
  const label = document.createElement("span");
  label.className = "send-button-label";
  return label;
}

function arrowIcon(): HTMLSpanElement {
  const icon = document.createElement("span");
  icon.className = "ui-icon ui-icon-send";
  icon.setAttribute("aria-hidden", "true");
  icon.textContent = "↗";
  return icon;
}
