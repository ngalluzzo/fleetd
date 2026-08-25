import type {
  Channel,
  ChannelMember,
  Message,
  SendMessage,
} from "./generated/types.gen.ts";
import type {
  ConversationConnectionState,
  ConversationTransport,
  ConversationTransportStream,
} from "./conversation-transport.ts";

const DEFAULT_MAX_RETAINED_MESSAGES = 512;

export type ConversationSessionPhase =
  | "idle"
  | "loading_channels"
  | "ready"
  | "connecting"
  | "live"
  | "reconnecting"
  | "failed"
  | "closed";

export type ConversationSessionErrorCode =
  | "channel_discovery_failed"
  | "invalid_state"
  | "membership_failed"
  | "message_conflict"
  | "participant_not_member"
  | "send_failed"
  | "stream_failed";

export interface ConversationSessionFailure {
  readonly code: ConversationSessionErrorCode;
  readonly message: string;
}

export interface ConversationSnapshot {
  readonly revision: number;
  readonly phase: ConversationSessionPhase;
  readonly participantId: string;
  readonly channels: readonly Channel[];
  readonly selectedChannelId: string | null;
  readonly members: readonly ChannelMember[];
  readonly messages: readonly Message[];
  /** Highest cursor accepted from the authoritative stream, not a send reply. */
  readonly cursor: number;
  readonly pendingSends: number;
  readonly error: ConversationSessionFailure | null;
}

export interface ConversationSessionOptions {
  maxRetainedMessages?: number;
}

export type ConversationSnapshotListener = (
  snapshot: ConversationSnapshot,
) => void;

interface ChannelLane {
  readonly channelId: string;
  members: readonly ChannelMember[];
  membersReady: boolean;
  messages: Message[];
  readonly bySequence: Map<number, Message>;
  readonly byId: Map<string, Message>;
  cursor: number;
  connection: ConversationConnectionState;
  stream?: ConversationTransportStream;
}

/**
 * Target-neutral durable-conversation projection.
 *
 * This class owns neither credentials nor a DOM. All authority and wire
 * behavior enters through `ConversationTransport`.
 */
export class ConversationSession {
  readonly #transport: ConversationTransport;
  readonly #maxRetainedMessages: number;
  readonly #listeners = new Set<ConversationSnapshotListener>();
  readonly #lanes = new Map<string, ChannelLane>();
  #revision = 0;
  #phase: ConversationSessionPhase = "idle";
  #channels: readonly Channel[] = [];
  #selectedChannelId: string | null = null;
  #selectionGeneration = 0;
  #cancelSelection?: () => void;
  #pendingSends = 0;
  #error: ConversationSessionFailure | null = null;
  #closed = false;

  constructor(
    transport: ConversationTransport,
    options: ConversationSessionOptions = {},
  ) {
    this.#transport = transport;
    const maxRetainedMessages =
      options.maxRetainedMessages ?? DEFAULT_MAX_RETAINED_MESSAGES;
    if (
      !Number.isSafeInteger(maxRetainedMessages) ||
      maxRetainedMessages < 16 ||
      maxRetainedMessages > 4_096
    ) {
      throw new Error("maxRetainedMessages must be between 16 and 4096");
    }
    this.#maxRetainedMessages = maxRetainedMessages;
  }

  get snapshot(): ConversationSnapshot {
    const lane = this.#selectedLane();
    return {
      revision: this.#revision,
      phase: this.#phase,
      participantId: this.#transport.participantId,
      channels: this.#channels,
      selectedChannelId: this.#selectedChannelId,
      members: lane?.members ?? [],
      messages: lane?.messages ?? [],
      cursor: lane?.cursor ?? 0,
      pendingSends: this.#pendingSends,
      error: this.#error,
    };
  }

  subscribe(listener: ConversationSnapshotListener): () => void {
    this.#listeners.add(listener);
    listener(this.snapshot);
    return () => this.#listeners.delete(listener);
  }

  async start(): Promise<void> {
    this.#assertOpen();
    this.#phase = "loading_channels";
    this.#error = null;
    this.#publish();
    try {
      this.#channels = [...(await this.#transport.listChannels())];
    } catch {
      this.#fail("channel_discovery_failed", "Fleetd channel discovery failed");
      throw new Error("Fleetd channel discovery failed");
    }
    if (this.#closed) return;
    this.#phase = "ready";
    this.#publish();
  }

  async refreshChannels(): Promise<void> {
    this.#assertOpen();
    try {
      this.#channels = [...(await this.#transport.listChannels())];
      this.#error = null;
      this.#publish();
    } catch {
      this.#fail("channel_discovery_failed", "Fleetd channel discovery failed");
      throw new Error("Fleetd channel discovery failed");
    }
  }

  async selectChannel(channelId: string): Promise<void> {
    this.#assertOpen();
    if (!this.#channels.some((channel) => channel.id === channelId)) {
      throw this.#invalidState("the selected channel was not discovered");
    }

    this.#cancelSelection?.();
    this.#cancelSelection = undefined;
    const generation = ++this.#selectionGeneration;
    this.#selectedLane()?.stream?.close();
    this.#selectedChannelId = channelId;
    this.#error = null;
    const lane = this.#lane(channelId);
    lane.membersReady = false;
    lane.connection = "connecting";
    this.#phase = "connecting";
    this.#publish();

    let liveResolve!: () => void;
    let liveReject!: () => void;
    const live = new Promise<void>((resolve, reject) => {
      liveResolve = resolve;
      liveReject = reject;
    });
    this.#cancelSelection = liveReject;
    const members = this.#transport.listMembers(channelId);

    try {
      const stream = this.#transport.openStream({
        channelId,
        after: lane.cursor,
        accept: (message) => {
          if (!this.#isCurrent(channelId, generation)) return;
          this.#acceptMessage(lane, message, true);
        },
        statusChanged: (status) => {
          if (!this.#isCurrent(channelId, generation)) return;
          lane.connection = status;
          if (status === "live") liveResolve();
          if (status === "failed" || status === "closed") {
            if (this.#error === null) {
              this.#fail(
                "stream_failed",
                status === "failed"
                  ? "The selected Fleetd stream failed"
                  : "The selected Fleetd stream closed",
              );
            }
            liveReject();
          }
          this.#publishLaneState(lane);
        },
      });
      lane.stream = stream;
      void stream.closed.then(
        () => {
          if (!this.#isCurrent(channelId, generation) || this.#closed) return;
          if (this.#error === null) {
            this.#fail("stream_failed", "The selected Fleetd stream closed");
          }
        },
        () => {
          if (!this.#isCurrent(channelId, generation) || this.#closed) return;
          if (this.#error === null) {
            this.#fail("stream_failed", "The selected Fleetd stream failed");
          }
        },
      );

      const [observedMembers] = await Promise.all([members, live]);
      if (!this.#isCurrent(channelId, generation)) return;
      if (
        !observedMembers.some(
          (member) => member.agent_id === this.#transport.participantId,
        )
      ) {
        stream.close();
        this.#fail(
          "participant_not_member",
          "The human participant is not a member of the selected channel",
        );
        throw new Error(
          "The human participant is not a member of the selected channel",
        );
      }
      lane.members = [...observedMembers];
      lane.membersReady = true;
      this.#cancelSelection = undefined;
      this.#publishLaneState(lane);
    } catch {
      if (!this.#isCurrent(channelId, generation)) return;
      this.#cancelSelection = undefined;
      if (this.#error === null) {
        this.#fail(
          "membership_failed",
          "The selected Fleetd channel could not be opened",
        );
      }
      lane.stream?.close();
      throw new Error("The selected Fleetd channel could not be opened");
    }
  }

  async send(message: SendMessage): Promise<Message> {
    this.#assertOpen();
    const lane = this.#selectedLane();
    if (!lane || this.#phase === "failed") {
      throw this.#invalidState("a healthy selected channel is required");
    }
    this.#pendingSends += 1;
    this.#publish();
    try {
      const sent = await this.#transport.send(lane.channelId, message);
      if (
        sent.channel_id !== lane.channelId ||
        sent.sender_id !== this.#transport.participantId
      ) {
        this.#fail(
          "message_conflict",
          "Fleetd returned a message outside the selected participant lane",
        );
        throw new Error(
          "Fleetd returned a message outside the selected participant lane",
        );
      }
      this.#acceptMessage(lane, sent, false);
      return sent;
    } catch {
      if (this.#error === null) {
        this.#fail("send_failed", "The Fleetd message send failed");
      }
      throw new Error("The Fleetd message send failed");
    } finally {
      this.#pendingSends -= 1;
      this.#publish();
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#cancelSelection?.();
    this.#cancelSelection = undefined;
    this.#selectionGeneration += 1;
    this.#selectedLane()?.stream?.close();
    this.#transport.close();
    this.#phase = "closed";
    this.#error = null;
    this.#publish();
    this.#listeners.clear();
  }

  #acceptMessage(lane: ChannelLane, message: Message, fromStream: boolean) {
    if (message.channel_id !== lane.channelId) {
      this.#fail(
        "message_conflict",
        "The conversation transport crossed channel boundaries",
      );
      throw new Error("conversation message crossed channel boundaries");
    }
    const bySequence = lane.bySequence.get(message.seq);
    const byId = lane.byId.get(message.id);
    if (
      (bySequence && bySequence.id !== message.id) ||
      (byId && byId.seq !== message.seq) ||
      (bySequence && !jsonEqual(bySequence, message)) ||
      (byId && !jsonEqual(byId, message))
    ) {
      this.#fail(
        "message_conflict",
        "Fleetd reused a stable message identity inconsistently",
      );
      throw new Error("Fleetd reused a stable message identity inconsistently");
    }

    if (!bySequence && !byId && message.seq > lane.cursor) {
      lane.bySequence.set(message.seq, message);
      lane.byId.set(message.id, message);
      lane.messages = [...lane.messages, message].sort(
        (left, right) => left.seq - right.seq,
      );
      this.#enforceMessageBound(lane);
    }
    if (fromStream && message.seq > lane.cursor) lane.cursor = message.seq;
    if (this.#selectedChannelId === lane.channelId) this.#publish();
  }

  #enforceMessageBound(lane: ChannelLane) {
    while (lane.messages.length > this.#maxRetainedMessages) {
      const expired = lane.messages.shift();
      if (!expired) return;
      lane.bySequence.delete(expired.seq);
      lane.byId.delete(expired.id);
    }
  }

  #lane(channelId: string): ChannelLane {
    let lane = this.#lanes.get(channelId);
    if (lane) return lane;
    lane = {
      channelId,
      members: [],
      membersReady: false,
      messages: [],
      bySequence: new Map(),
      byId: new Map(),
      cursor: 0,
      connection: "connecting",
    };
    this.#lanes.set(channelId, lane);
    return lane;
  }

  #selectedLane(): ChannelLane | undefined {
    return this.#selectedChannelId === null
      ? undefined
      : this.#lanes.get(this.#selectedChannelId);
  }

  #isCurrent(channelId: string, generation: number): boolean {
    return (
      !this.#closed &&
      this.#selectedChannelId === channelId &&
      this.#selectionGeneration === generation
    );
  }

  #publishLaneState(lane: ChannelLane) {
    if (this.#selectedChannelId !== lane.channelId) return;
    if (lane.connection === "live") {
      this.#phase = lane.membersReady ? "live" : "connecting";
    } else if (lane.connection === "reconnecting") {
      this.#phase = "reconnecting";
    } else if (lane.connection === "failed") {
      this.#phase = "failed";
    } else if (lane.connection === "closed") {
      this.#phase = "closed";
    } else {
      this.#phase = "connecting";
    }
    this.#publish();
  }

  #fail(code: ConversationSessionErrorCode, message: string) {
    this.#phase = "failed";
    this.#error = { code, message };
    this.#publish();
  }

  #invalidState(message: string): Error {
    this.#error = { code: "invalid_state", message };
    this.#publish();
    return new Error(message);
  }

  #assertOpen() {
    if (this.#closed)
      throw this.#invalidState("conversation session is closed");
  }

  #publish() {
    this.#revision += 1;
    const snapshot = this.snapshot;
    for (const listener of [...this.#listeners]) {
      try {
        listener(snapshot);
      } catch {
        this.#listeners.delete(listener);
      }
    }
  }
}

function jsonEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => jsonEqual(value, right[index]))
    );
  }
  if (
    typeof left !== "object" ||
    left === null ||
    typeof right !== "object" ||
    right === null
  ) {
    return false;
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = Object.keys(leftRecord).sort();
  const rightKeys = Object.keys(rightRecord).sort();
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key, index) =>
        key === rightKeys[index] &&
        jsonEqual(leftRecord[key], rightRecord[key]),
    )
  );
}
