import type { ConversationSessionPhase } from "../../../../clients/typescript/src/conversation-session.ts";
import type {
  ChannelMember,
  Message,
} from "../../../../clients/typescript/src/generated/types.gen.ts";

export interface ConnectionStatusInput {
  readonly phase: ConversationSessionPhase;
  readonly pendingSends: number;
  readonly errorMessage?: string;
  readonly sending?: boolean;
}

export interface ConnectionStatusView {
  readonly label: string;
  readonly description: string;
  readonly busy: boolean;
}

const PHASE_STATUS: Record<
  ConversationSessionPhase,
  Omit<ConnectionStatusView, "busy">
> = {
  idle: {
    label: "offline",
    description: "No conversation is connected.",
  },
  loading_channels: {
    label: "loading",
    description: "Finding conversations on this machine.",
  },
  ready: {
    label: "ready",
    description: "Choose a conversation to begin.",
  },
  connecting: {
    label: "connecting",
    description: "Opening the selected conversation.",
  },
  live: {
    label: "live",
    description: "Connected and up to date.",
  },
  reconnecting: {
    label: "reconnecting",
    description: "Restoring the selected conversation.",
  },
  failed: {
    label: "needs attention",
    description: "This conversation needs attention before it can continue.",
  },
  closed: {
    label: "closed",
    description: "The local conversation session is closed.",
  },
};

export function connectionStatusView(
  input: ConnectionStatusInput,
): ConnectionStatusView {
  if (input.sending || input.pendingSends > 0) {
    return {
      label: "sending message",
      description: "Your message is being sent.",
      busy: true,
    };
  }
  const status = PHASE_STATUS[input.phase];
  return {
    ...status,
    description: input.errorMessage ?? status.description,
    busy: ["loading_channels", "connecting", "reconnecting"].includes(
      input.phase,
    ),
  };
}

export interface EmptyConversationInput {
  readonly selected: boolean;
  readonly phase: ConversationSessionPhase;
  readonly messageCount: number;
  readonly errorMessage?: string;
}

export interface EmptyConversationView {
  readonly hidden: boolean;
  readonly title: string;
  readonly copy: string;
  readonly state:
    | "hidden"
    | "unselected"
    | "loading"
    | "empty"
    | "error";
}

export function emptyConversationView(
  input: EmptyConversationInput,
): EmptyConversationView {
  if (input.messageCount > 0) {
    return {
      hidden: true,
      title: "",
      copy: "",
      state: "hidden",
    };
  }
  if (!input.selected) {
    if (input.phase === "loading_channels") {
      return {
        hidden: false,
        title: "Loading conversations",
        copy: "Finding the conversations available on this machine.",
        state: "loading",
      };
    }
    if (input.phase === "failed") {
      return {
        hidden: false,
        title: "Conversations unavailable",
        copy: input.errorMessage ?? "Reconnect to try channel discovery again.",
        state: "error",
      };
    }
    return {
      hidden: false,
      title: "Choose a channel",
      copy: "Choose a channel to see its saved history and new replies.",
      state: "unselected",
    };
  }
  if (input.phase === "failed" || input.phase === "closed") {
    return {
      hidden: false,
      title: "Conversation unavailable",
      copy: input.errorMessage ?? "Choose the channel again to reconnect.",
      state: "error",
    };
  }
  if (input.phase !== "live") {
    return {
      hidden: false,
      title:
        input.phase === "reconnecting"
          ? "Reconnecting conversation"
          : "Opening conversation",
      copy: "Restoring saved history and checking for new replies.",
      state: "loading",
    };
  }
  return {
    hidden: false,
    title: "Start the conversation",
    copy: "Send the first message to an agent.",
    state: "empty",
  };
}

export interface MemberOptionView {
  readonly id: string;
  readonly label: string;
  readonly description: string;
  readonly preferred: boolean;
}

export function memberOptionView(member: ChannelMember): MemberOptionView {
  return {
    id: member.agent_id,
    label: member.agent_name,
    description: `${member.agent_name} (${shortId(member.agent_id)})`,
    preferred: member.delivery_mode === "inbox",
  };
}

export function senderLabel(
  message: Message,
  participantId: string,
  names: ReadonlyMap<string, string>,
): string {
  return message.sender_id === participantId
    ? "you"
    : (names.get(message.sender_id) ?? shortId(message.sender_id));
}

export function recipientLabel(
  message: Message,
  participantId: string,
  names: ReadonlyMap<string, string>,
): string {
  if (message.recipient_id == null) return "channel";
  if (message.recipient_id === participantId) return "you";
  return names.get(message.recipient_id) ?? shortId(message.recipient_id);
}

export function shortId(value: string): string {
  return value.length > 12 ? `${value.slice(0, 8)}…` : value;
}
