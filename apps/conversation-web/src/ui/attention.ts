import type { ConversationAttention } from "@fleetd/client/types";

export interface ConversationAttentionBadge {
  readonly text: string;
  readonly description: string;
  readonly tone: "unread" | "addressed";
}

/** Presents only exact unread and explicit-recipient facts. */
export function conversationAttentionBadge(
  attention?: ConversationAttention,
): ConversationAttentionBadge | undefined {
  const unread = attention?.unread_count ?? 0;
  const addressed = attention?.addressed_unread_count ?? 0;
  if (unread <= 0) return undefined;
  return addressed > 0
    ? {
        text: `@${boundedCount(addressed)}`,
        description: `${unread} unread, ${addressed} addressed to you`,
        tone: "addressed",
      }
    : {
        text: boundedCount(unread),
        description: `${unread} unread`,
        tone: "unread",
      };
}

function boundedCount(count: number): string {
  return count > 99 ? "99+" : String(count);
}
