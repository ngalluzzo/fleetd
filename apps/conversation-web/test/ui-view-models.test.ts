import { describe, expect, test } from "bun:test";

import type {
  ChannelMember,
  Message,
} from "../../../clients/typescript/src/generated/types.gen.ts";
import {
  connectionStatusView,
  emptyConversationView,
  memberOptionView,
  recipientLabel,
  senderLabel,
  shortId,
} from "../src/ui/view-models.ts";

describe("conversation UI view models", () => {
  test("announces transport and send activity without disguising failures", () => {
    expect(
      connectionStatusView({
        phase: "reconnecting",
        pendingSends: 0,
      }),
    ).toEqual({
      label: "reconnecting",
      description: "Restoring the selected conversation.",
      busy: true,
    });
    expect(
      connectionStatusView({
        phase: "live",
        pendingSends: 1,
      }),
    ).toEqual({
      label: "sending message",
      description: "Your message is being sent.",
      busy: true,
    });
    expect(
      connectionStatusView({
        phase: "failed",
        pendingSends: 0,
        errorMessage: "The Fleetd message send failed",
      }),
    ).toEqual({
      label: "needs attention",
      description: "The Fleetd message send failed",
      busy: false,
    });
  });

  test("keeps the qualified selected-channel empty title exact", () => {
    expect(
      emptyConversationView({
        selected: true,
        phase: "live",
        messageCount: 0,
      }),
    ).toEqual({
      hidden: false,
      title: "Start the conversation",
      copy: "Send the first message to an agent.",
      state: "empty",
    });
    expect(
      emptyConversationView({
        selected: true,
        phase: "live",
        messageCount: 1,
      }).hidden,
    ).toBe(true);
    expect(
      emptyConversationView({
        selected: true,
        phase: "reconnecting",
        messageCount: 0,
      }).state,
    ).toBe("loading");
  });

  test("formats targets and message attribution without inventing roles", () => {
    expect(memberOptionView(member("agent-worker-long", "Piler", "inbox"))).toEqual({
      id: "agent-worker-long",
      label: "Piler",
      description: "Piler (agent-wo…)",
      preferred: true,
    });
    expect(
      memberOptionView(member("human", "Nic", "stream_only")).label,
    ).toBe("Nic");

    const names = new Map([
      ["agent-worker-long", "Piler"],
      ["human", "Nic"],
    ]);
    const direct = message({
      sender_id: "agent-worker-long",
      recipient_id: "human",
    });
    expect(senderLabel(direct, "human", names)).toBe("Piler");
    expect(recipientLabel(direct, "human", names)).toBe("you");
    expect(recipientLabel(message({ recipient_id: null }), "human", names)).toBe(
      "channel",
    );
    expect(shortId("0123456789abcdef")).toBe("01234567…");
  });
});

function member(
  agentId: string,
  agentName: string,
  deliveryMode: ChannelMember["delivery_mode"],
): ChannelMember {
  return {
    agent_id: agentId,
    agent_name: agentName,
    channel_id: "channel-1",
    delivery_mode: deliveryMode,
    joined_at_ms: 1,
  };
}

function message(overrides: Partial<Message> = {}): Message {
  return {
    seq: 1,
    id: "message-1",
    channel_id: "channel-1",
    sender_id: "human",
    recipient_id: "agent-worker-long",
    kind: "future.opaque/v1",
    payload: { preserved: true },
    correlation_id: null,
    causation_id: null,
    created_at_ms: 1,
    ...overrides,
  };
}
