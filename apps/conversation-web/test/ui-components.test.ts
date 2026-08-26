import { describe, expect, test } from "bun:test";

import {
  messageKindLabel,
  statusTone,
} from "../src/ui/components.ts";

const contract = {
  requestKind: "conversation.prompt/v1",
  resultKind: "conversation.result/v1",
} as const;

describe("conversation UI component semantics", () => {
  test("presents configured message contracts without leaking version strings", () => {
    expect(messageKindLabel(contract.requestKind, contract)).toBe("Message");
    expect(messageKindLabel(contract.resultKind, contract)).toBe("Reply");
    expect(messageKindLabel("vendor.extension/v9", contract)).toBe("Event");
  });

  test("maps untrusted statuses to the finite visual tone vocabulary", () => {
    expect(statusTone("completed")).toBe("success");
    expect(statusTone("in_progress")).toBe("warning");
    expect(statusTone("FAILED")).toBe("danger");
    expect(statusTone("custom-provider-state")).toBe("neutral");
  });
});
