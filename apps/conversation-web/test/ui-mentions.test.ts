import { describe, expect, test } from "bun:test";

import type { ChannelMember } from "@fleetd/client/types";
import {
  applyMention,
  directRecipient,
  mentionCandidates,
  mentionQueryAt,
  mentionSelectionPresent,
} from "../src/ui/mentions.ts";

const members: readonly ChannelMember[] = [
  member("human", "Nic", "stream_only"),
  member("south", "South", "inbox"),
  member(
    "north",
    "north-seat-0198e62f-6e0d-7cc8-a92a-82e6247bc517",
    "inbox",
  ),
  member("observer", "Observer", "stream_only"),
];

describe("channel mention interaction", () => {
  test("finds only a mention immediately before the caret", () => {
    expect(mentionQueryAt("Ask @nor", 8)).toEqual({
      start: 4,
      end: 8,
      text: "nor",
    });
    expect(mentionQueryAt("email@example.com", 17)).toBeUndefined();
    expect(mentionQueryAt("Ask @north later", 16)).toBeUndefined();
    expect(mentionQueryAt("@", 1)).toEqual({ start: 0, end: 1, text: "" });
  });

  test("searches current peers and preserves exact stable IDs", () => {
    const candidates = mentionCandidates(members, "human", "north");
    expect(candidates).toHaveLength(1);
    expect(candidates[0]).toMatchObject({
      recipientId: "north",
      label: "North seat",
      receivesInboxWork: true,
    });
  });

  test("inserts visible text while retaining the selected member identity", () => {
    const query = mentionQueryAt("Ask @sou", 8);
    const candidate = mentionCandidates(members, "human", "south")[0];
    if (!query || !candidate) throw new Error("mention fixture missing");
    const applied = applyMention("Ask @sou", query, candidate);
    expect(applied).toEqual({
      draft: "Ask @South ",
      caret: 11,
      selection: {
        recipientId: "south",
        token: "@South",
        label: "South",
      },
    });
    expect(mentionSelectionPresent(applied.draft, applied.selection)).toBe(true);
    expect(mentionSelectionPresent("Ask South", applied.selection)).toBe(false);
  });

  test("direct conversations resolve their only peer automatically", () => {
    expect(directRecipient(members.slice(0, 2), "human")?.recipientId).toBe(
      "south",
    );
    expect(directRecipient(members, "human")).toBeUndefined();
  });
});

function member(
  agentId: string,
  agentName: string,
  deliveryMode: ChannelMember["delivery_mode"],
): ChannelMember {
  return {
    channel_id: "channel-1",
    agent_id: agentId,
    agent_name: agentName,
    joined_at_ms: 1,
    delivery_mode: deliveryMode,
  };
}
