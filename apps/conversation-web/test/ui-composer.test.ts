import { describe, expect, test } from "bun:test";

import {
  composerAvailability,
  isComposerSendShortcut,
} from "../src/ui/composer.ts";

describe("conversation composer interaction model", () => {
  test("enables only a non-empty live targeted send", () => {
    expect(
      composerAvailability({
        phase: "live",
        selectedChannelId: "channel-1",
        targetId: "agent-1",
        draft: " hello ",
        pendingSends: 0,
        sending: false,
      }),
    ).toEqual({
      textareaDisabled: false,
      targetDisabled: false,
      sendDisabled: false,
      sending: false,
    });
    expect(
      composerAvailability({
        phase: "live",
        selectedChannelId: "channel-1",
        targetId: "agent-1",
        draft: "  \n ",
        pendingSends: 0,
        sending: false,
      }).sendDisabled,
    ).toBe(true);
  });

  test("fails closed while opening, failed, or already sending", () => {
    for (const phase of ["connecting", "reconnecting", "failed"] as const) {
      const state = composerAvailability({
        phase,
        selectedChannelId: "channel-1",
        targetId: "agent-1",
        draft: "hello",
        pendingSends: 0,
        sending: false,
      });
      expect(state.textareaDisabled).toBe(true);
      expect(state.sendDisabled).toBe(true);
    }
    expect(
      composerAvailability({
        phase: "live",
        selectedChannelId: "channel-1",
        targetId: "agent-1",
        draft: "hello",
        pendingSends: 1,
        sending: false,
      }),
    ).toEqual({
      textareaDisabled: false,
      targetDisabled: false,
      sendDisabled: true,
      sending: true,
    });
  });

  test("sends on plain Enter but preserves deliberate newline and IME input", () => {
    expect(isComposerSendShortcut(key())).toBe(true);
    expect(isComposerSendShortcut(key({ shiftKey: true }))).toBe(false);
    expect(isComposerSendShortcut(key({ ctrlKey: true }))).toBe(false);
    expect(isComposerSendShortcut(key({ altKey: true }))).toBe(false);
    expect(isComposerSendShortcut(key({ metaKey: true }))).toBe(false);
    expect(isComposerSendShortcut(key({ isComposing: true }))).toBe(false);
    expect(isComposerSendShortcut(key({ key: "Escape" }))).toBe(false);
  });
});

function key(
  overrides: Partial<Parameters<typeof isComposerSendShortcut>[0]> = {},
): Parameters<typeof isComposerSendShortcut>[0] {
  return {
    key: "Enter",
    shiftKey: false,
    ctrlKey: false,
    altKey: false,
    metaKey: false,
    isComposing: false,
    ...overrides,
  };
}
