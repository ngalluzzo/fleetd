import { describe, expect, test } from "bun:test";
import { buildConversationBootstrap } from "../src/bootstrap.ts";

describe("conversation bootstrap", () => {
  test("hands the profile to the presentation once without logging it", async () => {
    const calls: unknown[] = [];
    const dataset: Record<string, string> = {
      fleetdConversationReady: "true",
    };
    const source = buildConversationBootstrap({
      participantId: "human-id",
      operatorCredential: "operator-secret",
      participantCredential: "participant-secret",
      requestKind: "conversation.prompt/v1",
      resultKind: "conversation.result/v1",
      channelId: "channel-id",
      runtimeProfiles: [
        {
          id: "opencode-default",
          label: "OpenCode",
          description: "Approved runtime",
        },
      ],
    });
    expect(source).not.toContain("operator-secret");
    expect(source).not.toContain("participant-secret");

    const execute = new Function(
      "globalThis",
      "document",
      "MutationObserver",
      "setTimeout",
      "atob",
      "TextDecoder",
      source,
    );
    execute(
      {
        __fleetdConversation: {
          connect(profile: unknown) {
            calls.push(structuredClone(profile));
          },
        },
      },
      { documentElement: { dataset } },
      class {},
      () => 0,
      atob,
      TextDecoder,
    );
    await Promise.resolve();
    expect(calls).toEqual([
      {
        participantId: "human-id",
        operatorCredential: "operator-secret",
        participantCredential: "participant-secret",
        requestKind: "conversation.prompt/v1",
        resultKind: "conversation.result/v1",
        channelId: "channel-id",
        runtimeProfiles: [
          {
            id: "opencode-default",
            label: "OpenCode",
            description: "Approved runtime",
          },
        ],
      },
    ]);
    expect(dataset["fleetdConversationHost"]).toBe("connected");
  });
});
