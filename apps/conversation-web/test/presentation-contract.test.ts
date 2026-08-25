import { describe, expect, test } from "bun:test";

import type { Message } from "../../../clients/typescript/src/generated/types.gen.ts";
import { renderMessageBody } from "../src/presentation-contract.ts";

const contract = {
  requestKind: "conversation.prompt/test-v1",
  resultKind: "conversation.result/test-v1",
};

describe("conversation presentation contract", () => {
  test("renders only the configured request kind as human text", () => {
    expect(
      renderMessageBody(
        message(contract.requestKind, { text: "hello fleet" }),
        contract,
      ),
    ).toEqual({ format: "text", text: "hello fleet" });
  });

  test("assembles bounded assistant content for the configured result", () => {
    expect(
      renderMessageBody(
        message(contract.resultKind, {
          status: "completed",
          assistant_messages: [
            { content: [{ type: "text", text: "hello " }] },
            { content: ["from ", { type: "text", text: "the fleet" }] },
          ],
          unknown_completion_field: { preserved: true },
        }),
        contract,
      ),
    ).toEqual({
      format: "text",
      text: "hello from the fleet",
      status: "completed",
    });
  });

  test("retains exact JSON fallback for unknown kinds and malformed matches", () => {
    const payload = {
      future: [null, true, 42, { untouched: "yes" }],
    };
    expect(
      renderMessageBody(message("future.unknown/v99", payload), contract),
    ).toEqual({
      format: "json",
      text: JSON.stringify(payload, null, 2),
    });
    expect(
      renderMessageBody(message(contract.requestKind, payload), contract),
    ).toEqual({
      format: "json",
      text: JSON.stringify(payload, null, 2),
    });
  });
});

function message(kind: string, payload: unknown): Message {
  return {
    seq: 1,
    id: "message-1",
    channel_id: "channel-1",
    sender_id: "agent-1",
    recipient_id: "agent-2",
    kind,
    payload,
    correlation_id: null,
    causation_id: null,
    created_at_ms: 1_787_000_000_000,
  };
}
