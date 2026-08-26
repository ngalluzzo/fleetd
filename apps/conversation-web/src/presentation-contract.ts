import type { Message } from "@fleetd/client/types";

export interface ConversationPresentationContract {
  requestKind: string;
  resultKind: string;
}

export interface RenderedMessageBody {
  readonly format: "text" | "json";
  readonly text: string;
  readonly status?: string;
}

interface MessageRenderer {
  matches(
    message: Message,
    contract: ConversationPresentationContract,
  ): boolean;
  render(message: Message): RenderedMessageBody | undefined;
}

const renderers: readonly MessageRenderer[] = [
  {
    matches: (message, contract) => message.kind === contract.requestKind,
    render(message) {
      const payload = record(message.payload);
      return typeof payload?.text === "string"
        ? { format: "text", text: payload.text }
        : undefined;
    },
  },
  {
    matches: (message, contract) => message.kind === contract.resultKind,
    render(message) {
      const payload = record(message.payload);
      const text = assistantText(payload?.assistant_messages);
      if (!text) return undefined;
      return {
        format: "text",
        text,
        status:
          typeof payload?.status === "string" ? payload.status : undefined,
      };
    },
  },
];

/** Renders configured contracts while preserving an exact JSON fallback. */
export function renderMessageBody(
  message: Message,
  contract: ConversationPresentationContract,
): RenderedMessageBody {
  for (const renderer of renderers) {
    if (!renderer.matches(message, contract)) continue;
    const rendered = renderer.render(message);
    if (rendered) return rendered;
  }
  return {
    format: "json",
    text: JSON.stringify(message.payload, null, 2),
  };
}

function assistantText(value: unknown): string {
  if (!Array.isArray(value)) return "";
  const fragments: string[] = [];
  for (const assistantMessage of value) {
    const content = record(assistantMessage)?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (typeof block === "string") {
        fragments.push(block);
        continue;
      }
      const text = record(block)?.text;
      if (typeof text === "string") fragments.push(text);
    }
  }
  return fragments.join("").trim();
}

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
