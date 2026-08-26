import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "bun:test";

const htmlPath = fileURLToPath(
  new URL("../index.html", import.meta.url),
);
const sourcePath = fileURLToPath(new URL("../src", import.meta.url));
const html = readFileSync(htmlPath, "utf8");
const applicationSource = readTypeScriptSource(sourcePath);

const requiredIds = [
  "connect-panel",
  "connect-form",
  "connect-error",
  "participant-id",
  "operator-credential",
  "participant-credential",
  "request-kind",
  "result-kind",
  "conversation-app",
  "connection-status",
  "channel-list",
  "direct-list",
  "open-agent-directory",
  "new-channel",
  "new-direct-message",
  "disconnect",
  "channel-title",
  "channel-meta",
  "channel-avatar",
  "open-conversation-details",
  "message-target",
  "empty-conversation",
  "empty-conversation-title",
  "empty-conversation-copy",
  "message-list",
  "composer",
  "composer-text",
  "send-message",
  "agent-directory-dialog",
  "agent-list",
  "channel-dialog",
  "channel-form",
  "conversation-details-dialog",
  "archive-channel-dialog",
] as const;

const structuralClasses = [
  "onboarding-shell",
  "site-brand",
  "local-badge",
  "benefit-list",
  "connect-card-header",
  "form-section",
  "field",
  "advanced-settings",
  "security-note",
  "channel-navigation",
  "navigation-heading",
  "channel-list-empty",
  "conversation-toolbar",
  "recipient-field",
  "conversation-content",
  "empty-illustration",
  "composer-shell",
  "composer-input",
  "keyboard-key",
  "conversation-group",
  "agent-list",
  "workspace-dialog",
] as const;

describe("conversation product shell contract", () => {
  test("preserves every application and qualification hook exactly once", () => {
    for (const id of requiredIds) {
      expect(idOccurrences(id), id).toBe(1);
    }
    expect(applicationSource).toContain('"Start the conversation"');
  });

  test("publishes stable structural hooks for presentation styling", () => {
    const classes = new Set(
      [...html.matchAll(/\bclass="([^"]+)"/g)].flatMap((match) =>
        match[1].split(/\s+/),
      ),
    );
    for (const className of structuralClasses) {
      expect(classes.has(className), className).toBe(true);
    }
  });

  test("keeps the public bootstrap and behavior out of inline markup", () => {
    expect(html).toContain(
      '<link rel="stylesheet" href="/conversation/conversation.css" />',
    );
    expect(html).toContain(
      '<script src="/conversation/conversation.js" defer></script>',
    );
    expect(html).not.toMatch(/<script(?![^>]*\bsrc=)[^>]*>/i);
    expect(html).not.toMatch(/\son[a-z]+\s*=/i);
  });

  test("retains memory-only credential form behavior", () => {
    const form = openingTag("connect-form");
    expect(form).toContain('autocomplete="off"');
    expect(form).not.toMatch(/\saction=/i);

    for (const id of ["operator-credential", "participant-credential"]) {
      const input = openingTag(id);
      expect(input).toContain('type="password"');
      expect(input).toContain("required");
      expect(input).toContain('autocomplete="off"');
    }

    for (const id of ["participant-id", "request-kind", "result-kind"]) {
      expect(openingTag(id)).toContain("required");
    }
    expect(openingTag("connect-error")).toContain('role="alert"');
    expect(openingTag("connect-error")).toContain("hidden");
    expect(openingTag("conversation-app")).toContain("hidden");
  });

  test("retains the configured message-type defaults", () => {
    expect(openingTag("request-kind")).toContain(
      'value="conversation.prompt/phase-c-v1"',
    );
    expect(openingTag("result-kind")).toContain(
      'value="conversation.result/phase-c-v1"',
    );
  });

  test("presents channels, direct messages, and the agent directory as distinct surfaces", () => {
    expect(html).toContain('id="shared-channels-heading">Channels</h3>');
    expect(html).toContain('id="direct-messages-heading">Direct messages</h3>');
    expect(html).toContain('id="agent-directory-title">Agent directory</h2>');
    expect(applicationSource).toContain("openDirectConversation");
    expect(applicationSource).toContain("createSharedChannel");
    expect(applicationSource).toContain("archiveSharedChannel");
    expect(applicationSource).not.toMatch(/\b(start|stop|restart)(Agent|Worker)/);
  });
});

function idOccurrences(id: string): number {
  return html.match(new RegExp(`\\bid="${escapeRegExp(id)}"`, "g"))?.length ?? 0;
}

function openingTag(id: string): string {
  const match = html.match(
    new RegExp(`<[^>]+\\bid="${escapeRegExp(id)}"[^>]*>`, "i"),
  );
  if (!match) throw new Error(`missing shell element: ${id}`);
  return match[0];
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function readTypeScriptSource(directory: string): string {
  return readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) return readTypeScriptSource(path);
      return entry.isFile() && entry.name.endsWith(".ts")
        ? readFileSync(path, "utf8")
        : "";
    })
    .join("\n");
}
