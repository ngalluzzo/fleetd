import type {
  Channel,
  ChannelMember,
  Message,
  SendMessage,
} from "./generated/types.gen.ts";
import type {
  ConversationTransport,
  ConversationTransportStream,
  OpenConversationStream,
} from "./conversation-transport.ts";
import {
  openNativeChannelStream,
  type NativeChannelStreamSocketFactory,
  type NativeChannelStreamTimeoutScheduler,
} from "./native-channel-stream.ts";
import {
  boundedCredential,
  boundedIdentifier,
  boundedRequestTimeout,
  exactHttpOrigin,
} from "./client-options.ts";

/**
 * Structural form of the participant attention projection.
 *
 * Keeping this private lets the native transport remain compatible with the
 * original ConversationTransport contract while the generated attention type
 * is added independently. Once present, the two types are structurally exact.
 */
interface ParticipantAttention {
  addressed_unread_count: number;
  channel_id: string;
  first_addressed_unread_seq?: number | null;
  first_unread_seq?: number | null;
  latest_message_seq?: number | null;
  read_through_seq: number;
  unread_count: number;
}

export interface NativeConversationTransportOptions {
  origin: string;
  participantId: string;
  /** Operator authority is used only for fleet-wide channel discovery. */
  operatorCredential: string;
  /** Participant authority owns membership, attention, streams, and sends. */
  participantCredential: string;
  reconnectDelaysMs?: readonly number[];
  readyTimeoutMs?: number;
  maxPendingMessages?: number;
  /** Bounds each channel, membership, attention, and send HTTP operation. */
  requestTimeoutMs?: number;
  fetch?: typeof globalThis.fetch;
  createWebSocket?: NativeChannelStreamSocketFactory;
  delay?: (milliseconds: number) => Promise<void>;
  scheduleTimeout?: NativeChannelStreamTimeoutScheduler;
}

type NativeStreamConfiguration = Pick<
  NativeConversationTransportOptions,
  | "reconnectDelaysMs"
  | "readyTimeoutMs"
  | "maxPendingMessages"
  | "createWebSocket"
  | "delay"
  | "scheduleTimeout"
>;

/** Creates a native/service transport for the headless ConversationSession. */
export function createNativeConversationTransport(
  options: NativeConversationTransportOptions,
): ConversationTransport {
  return new NativeConversationTransport(options);
}

class NativeConversationTransport implements ConversationTransport {
  readonly participantId: string;

  readonly #origin: string;
  readonly #streamConfiguration: NativeStreamConfiguration;
  readonly #requestTimeoutMs: number;
  readonly #fetch: typeof globalThis.fetch;
  readonly #streams = new Set<ConversationTransportStream>();
  readonly #activeRequests = new Set<AbortController>();
  #operatorCredential: string;
  #participantCredential: string;
  #closed = false;

  constructor(options: NativeConversationTransportOptions) {
    this.#origin = exactHttpOrigin(options.origin);
    this.participantId = boundedIdentifier(
      options.participantId,
      "participantId",
    );
    this.#operatorCredential = boundedCredential(
      options.operatorCredential,
      "operatorCredential",
    );
    this.#participantCredential = boundedCredential(
      options.participantCredential,
      "participantCredential",
    );
    this.#requestTimeoutMs = boundedRequestTimeout(options.requestTimeoutMs);
    this.#fetch =
      options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.#streamConfiguration = {
      reconnectDelaysMs: options.reconnectDelaysMs,
      readyTimeoutMs: options.readyTimeoutMs,
      maxPendingMessages: options.maxPendingMessages,
      createWebSocket: options.createWebSocket,
      delay: options.delay,
      scheduleTimeout: options.scheduleTimeout,
    };
  }

  listChannels(): Promise<readonly Channel[]> {
    this.#assertOpen();
    return this.#request<Channel[]>(
      "/v1/channels",
      this.#operatorCredential,
      { method: "GET" },
      [200],
    );
  }

  listMembers(channelId: string): Promise<readonly ChannelMember[]> {
    this.#assertOpen();
    const exactChannelId = boundedIdentifier(channelId, "channelId");
    return this.#request<ChannelMember[]>(
      `/v1/channels/${encodeURIComponent(exactChannelId)}/members`,
      this.#participantCredential,
      { method: "GET" },
      [200],
    );
  }

  listAttention(): Promise<readonly ParticipantAttention[]> {
    this.#assertOpen();
    return this.#request<ParticipantAttention[]>(
      "/v1/conversations/attention",
      this.#participantCredential,
      { method: "GET" },
      [200],
    );
  }

  openStream(streamOptions: OpenConversationStream): ConversationTransportStream {
    this.#assertOpen();
    const stream = openNativeChannelStream({
      origin: this.#origin,
      channelId: boundedIdentifier(streamOptions.channelId, "channelId"),
      credential: this.#participantCredential,
      after: streamOptions.after,
      accept: streamOptions.accept,
      statusChanged: streamOptions.statusChanged,
      reconnectDelaysMs: this.#streamConfiguration.reconnectDelaysMs,
      readyTimeoutMs: this.#streamConfiguration.readyTimeoutMs,
      maxPendingMessages: this.#streamConfiguration.maxPendingMessages,
      createWebSocket: this.#streamConfiguration.createWebSocket,
      delay: this.#streamConfiguration.delay,
      scheduleTimeout: this.#streamConfiguration.scheduleTimeout,
    });
    this.#streams.add(stream);
    void stream.closed.then(
      () => this.#streams.delete(stream),
      () => this.#streams.delete(stream),
    );
    return stream;
  }

  send(channelId: string, message: SendMessage): Promise<Message> {
    this.#assertOpen();
    const exactChannelId = boundedIdentifier(channelId, "channelId");
    return this.#request<Message>(
      `/v1/channels/${encodeURIComponent(exactChannelId)}/messages`,
      this.#participantCredential,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(message),
      },
      [200, 201],
    );
  }

  advanceRead(
    channelId: string,
    throughSeq: number,
  ): Promise<ParticipantAttention> {
    this.#assertOpen();
    const exactChannelId = boundedIdentifier(channelId, "channelId");
    if (!Number.isSafeInteger(throughSeq) || throughSeq < 0) {
      throw new Error("throughSeq must be a non-negative safe integer");
    }
    return this.#request<ParticipantAttention>(
      `/v1/channels/${encodeURIComponent(exactChannelId)}/read-cursor`,
      this.#participantCredential,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ through_seq: throughSeq }),
      },
      [200],
    );
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const request of this.#activeRequests) request.abort();
    this.#activeRequests.clear();
    for (const stream of this.#streams) stream.close();
    this.#streams.clear();
    this.#operatorCredential = "";
    this.#participantCredential = "";
  }

  async #request<T>(
    path: string,
    credential: string,
    init: RequestInit,
    acceptedStatuses: readonly number[],
  ): Promise<T> {
    const controller = new AbortController();
    this.#activeRequests.add(controller);
    const timeout = setTimeout(() => controller.abort(), this.#requestTimeoutMs);
    try {
      const response = await this.#fetch(new URL(path, this.#origin), {
        ...init,
        headers: {
          Authorization: `Bearer ${credential}`,
          ...init.headers,
        },
        cache: "no-store",
        credentials: "omit",
        referrerPolicy: "no-referrer",
        signal: controller.signal,
      });
      if (!acceptedStatuses.includes(response.status)) {
        throw new Error(`Fleetd request failed with HTTP ${response.status}`);
      }
      return (await response.json()) as T;
    } finally {
      clearTimeout(timeout);
      this.#activeRequests.delete(controller);
    }
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("conversation transport is closed");
  }
}
