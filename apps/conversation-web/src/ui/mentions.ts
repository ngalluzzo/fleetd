import type { ChannelMember } from "@fleetd/client/types";

import { memberOptionView } from "./view-models.ts";

const MAX_MENTION_CANDIDATES = 8;

export interface MentionQuery {
  readonly start: number;
  readonly end: number;
  readonly text: string;
}

export interface MentionCandidate {
  readonly recipientId: string;
  readonly label: string;
  readonly exactName: string;
  readonly description: string;
  readonly receivesInboxWork: boolean;
}

export interface MentionSelection {
  readonly recipientId: string;
  readonly token: string;
  readonly label: string;
}

export interface AppliedMention {
  readonly draft: string;
  readonly caret: number;
  readonly selection: MentionSelection;
}

/** Finds the incomplete mention immediately before the textarea caret. */
export function mentionQueryAt(
  draft: string,
  caret: number,
): MentionQuery | undefined {
  if (!Number.isSafeInteger(caret) || caret < 0 || caret > draft.length) {
    return undefined;
  }
  const prefix = draft.slice(0, caret);
  const match = prefix.match(/(?:^|[\s([{])@([^\s@]*)$/u);
  if (!match) return undefined;
  const marker = prefix.lastIndexOf("@");
  return {
    start: marker,
    end: caret,
    text: match[1] ?? "",
  };
}

/** Projects exact channel members into a small, deterministic suggestion set. */
export function mentionCandidates(
  members: readonly ChannelMember[],
  participantId: string,
  query: string,
): readonly MentionCandidate[] {
  const normalizedQuery = normalize(query);
  return members
    .filter((member) => member.agent_id !== participantId)
    .map((member) => {
      const option = memberOptionView(member);
      return {
        recipientId: member.agent_id,
        label: option.label,
        exactName: member.agent_name,
        description: option.description,
        receivesInboxWork: member.delivery_mode === "inbox",
      };
    })
    .filter((candidate) => {
      if (normalizedQuery === "") return true;
      return [candidate.label, candidate.exactName].some((value) =>
        normalize(value).includes(normalizedQuery),
      );
    })
    .sort((left, right) => {
      const leftStarts = normalize(left.label).startsWith(normalizedQuery);
      const rightStarts = normalize(right.label).startsWith(normalizedQuery);
      if (leftStarts !== rightStarts) return leftStarts ? -1 : 1;
      if (left.receivesInboxWork !== right.receivesInboxWork) {
        return left.receivesInboxWork ? -1 : 1;
      }
      return left.label.localeCompare(right.label);
    })
    .slice(0, MAX_MENTION_CANDIDATES);
}

/** Inserts visible mention text while retaining the exact selected member ID. */
export function applyMention(
  draft: string,
  query: MentionQuery,
  candidate: MentionCandidate,
): AppliedMention {
  const token = `@${candidate.label}`;
  const suffix = draft.slice(query.end);
  const spacer = suffix === "" || /^\s/u.test(suffix) ? " " : "";
  const inserted = `${token}${spacer}`;
  return {
    draft: `${draft.slice(0, query.start)}${inserted}${suffix}`,
    caret: query.start + inserted.length,
    selection: {
      recipientId: candidate.recipientId,
      token,
      label: candidate.label,
    },
  };
}

/** A selected stable ID remains active only while its visible token remains. */
export function mentionSelectionPresent(
  draft: string,
  selection: MentionSelection,
): boolean {
  return draft.includes(selection.token);
}

/** Direct conversations target their only peer without requiring a mention. */
export function directRecipient(
  members: readonly ChannelMember[],
  participantId: string,
): MentionCandidate | undefined {
  const peers = mentionCandidates(members, participantId, "");
  return peers.length === 1 ? peers[0] : undefined;
}

function normalize(value: string): string {
  return value.trim().toLocaleLowerCase();
}
