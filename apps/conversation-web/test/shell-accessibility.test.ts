import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "bun:test";

const html = readFileSync(
  fileURLToPath(new URL("../index.html", import.meta.url)),
  "utf8",
);

describe("conversation shell accessibility and information architecture", () => {
  test("exposes onboarding, navigation, conversation, and composer landmarks", () => {
    expect(openingTag("connect-panel")).toContain(
      'aria-labelledby="connect-title"',
    );
    expect(html).toMatch(/<main\b[^>]*id="conversation-app"/i);
    expect(html).toMatch(
      /<aside\b[^>]*aria-label="Workspace navigation"/i,
    );
    expect(html).toMatch(
      /<nav\b[^>]*aria-labelledby="channels-heading"[^>]*aria-describedby="channels-description"/i,
    );
    expect(openingTag("conversation-content")).toContain(
      'aria-labelledby="channel-title"',
    );
    expect(openingTag("composer")).toContain('aria-label="Write a message"');
  });

  test("gives every user-entered field a visible label and useful description", () => {
    for (const id of [
      "participant-id",
      "operator-credential",
      "participant-credential",
      "request-kind",
      "result-kind",
      "composer-text",
      "agent-seat-profile",
      "agent-seat-instructions",
      "agent-seat-desired-state",
    ]) {
      expect(html).toContain(`for="${id}"`);
    }
    for (const id of [
      "participant-id",
      "operator-credential",
      "participant-credential",
      "composer-text",
    ]) {
      expect(openingTag(id)).toMatch(/aria-describedby="[^"]+"/);
    }
    expect(openingTag("composer-text")).toContain(
      'aria-controls="mention-suggestions"',
    );
    expect(openingTag("composer-text")).toContain('aria-expanded="false"');
    expect(openingTag("mention-suggestions")).toContain('role="listbox"');
  });

  test("announces connection, empty, and message updates without stealing focus", () => {
    const connection = openingTag("connection-status");
    expect(connection).toContain('role="status"');
    expect(connection).toContain('aria-live="polite"');

    const empty = openingTag("empty-conversation");
    expect(empty).toContain('role="status"');
    expect(empty).toContain('aria-live="polite"');

    const messages = openingTag("message-list");
    expect(messages).toContain('role="log"');
    expect(messages).toContain('aria-live="polite"');
    expect(messages).toContain('aria-relevant="additions"');
    expect(messages).toContain('tabindex="0"');
  });

  test("labels collaboration dialogs and destructive confirmation explicitly", () => {
    expect(openingTag("agent-directory-dialog")).toContain(
      'aria-labelledby="agent-directory-title"',
    );
    expect(openingTag("channel-dialog")).toContain(
      'aria-labelledby="channel-dialog-title"',
    );
    expect(openingTag("conversation-details-dialog")).toContain(
      'aria-labelledby="conversation-details-title"',
    );
    expect(openingTag("archive-channel-dialog")).toContain(
      'aria-labelledby="archive-channel-title"',
    );
    expect(openingTag("agent-seat-dialog")).toContain(
      'aria-labelledby="agent-seat-title"',
    );
    expect(html).toContain('for="channel-name"');
    expect(html).toContain('for="rename-channel-name"');
    expect(html).toContain('for="add-member-agent"');
    expect(openingTag("channel-form-error")).toContain('role="alert"');
    expect(openingTag("conversation-details-error")).toContain('role="alert"');
    expect(openingTag("agent-seat-error")).toContain('role="alert"');
  });

  test("uses product language instead of internal implementation language", () => {
    const visibleCopy = html.toLowerCase();
    for (const term of [
      "compiler",
      "dialect",
      "lowering",
      "intermediate representation",
      "websocket",
      "sqlite",
      "cursor",
    ]) {
      expect(visibleCopy, term).not.toContain(term);
    }
  });
});

function openingTag(id: string): string {
  const match = html.match(new RegExp(`<[^>]+\\bid="${id}"[^>]*>`, "i"));
  if (!match) throw new Error(`missing shell element: ${id}`);
  return match[0];
}
