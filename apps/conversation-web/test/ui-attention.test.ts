import { describe, expect, test } from "bun:test";

import { conversationAttentionBadge } from "../src/ui/attention.ts";

function attention(unread: number, addressed: number) {
  return {
    channel_id: "channel-1",
    read_through_seq: 4,
    latest_message_seq: 9,
    unread_count: unread,
    addressed_unread_count: addressed,
    first_unread_seq: unread > 0 ? 5 : null,
    first_addressed_unread_seq: addressed > 0 ? 6 : null,
  };
}

describe("conversation attention presentation", () => {
  test("omits settled conversations", () => {
    expect(conversationAttentionBadge(attention(0, 0))).toBeUndefined();
  });

  test("distinguishes ordinary unread from explicit addressing", () => {
    expect(conversationAttentionBadge(attention(7, 0))).toEqual({
      text: "7",
      description: "7 unread",
      tone: "unread",
    });
    expect(conversationAttentionBadge(attention(7, 2))).toEqual({
      text: "@2",
      description: "7 unread, 2 addressed to you",
      tone: "addressed",
    });
  });

  test("bounds visual counts without changing the exact description", () => {
    expect(conversationAttentionBadge(attention(140, 0))).toEqual({
      text: "99+",
      description: "140 unread",
      tone: "unread",
    });
  });
});
