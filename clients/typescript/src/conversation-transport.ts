import type {
  ConversationAttention,
  Channel,
  ChannelMember,
  Message,
  SendMessage,
} from "./generated/types.gen.ts";
import {
  openBrowserChannelStream,
  type BrowserChannelStreamSocketFactory,
  type BrowserChannelStreamStatus,
  type BrowserChannelStreamTimeoutScheduler,
} from "./browser-channel-stream.ts";
import {
  boundedCredential,
  boundedIdentifier,
  boundedRequestTimeout,
  exactHttpOrigin,
} from "./client-options.ts";

export type ConversationConnectionState = BrowserChannelStreamStatus;

export interface ConversationTransportStream {
  readonly cursor: number;
  readonly status: ConversationConnectionState;
  readonly closed: Promise<void>;
  close(): void;
}

export interface OpenConversationStream {
  channelId: string;
  after: number;
  accept(message: Message): void | Promise<void>;
  statusChanged?(status: ConversationConnectionState): void;
}

/**
 * Authority-aware wire edge consumed by the headless conversation session.
 * A native or TUI target can implement this without importing browser code.
 */
export interface ConversationTransport {
  readonly participantId: string;
  listChannels(): Promise<readonly Channel[]>;
  listAttention(): Promise<readonly ConversationAttention[]>;
  listMembers(channelId: string): Promise<readonly ChannelMember[]>;
  openStream(options: OpenConversationStream): ConversationTransportStream;
  send(channelId: string, message: SendMessage): Promise<Message>;
  advanceRead(
    channelId: string,
    throughSeq: number,
  ): Promise<ConversationAttention>;
  close(): void;
}

export interface BrowserConversationTransportOptions {
  origin: string;
  participantId: string;
  /** Operator authority is used only for fleet-wide channel discovery. */
  operatorCredential: string;
  /** Participant authority owns membership reads, streams, and sends. */
  participantCredential: string;
  reconnectDelaysMs?: readonly number[];
  readyTimeoutMs?: number;
  maxPendingMessages?: number;
  /** Bounds each channel, membership, and send HTTP operation. */
  requestTimeoutMs?: number;
  fetch?: typeof globalThis.fetch;
  createWebSocket?: BrowserChannelStreamSocketFactory;
  delay?: (milliseconds: number) => Promise<void>;
  scheduleTimeout?: BrowserChannelStreamTimeoutScheduler;
}

/**
 * Creates the exact two-principal browser transport.
 *
 * Credentials are retained only in this closure and are overwritten on
 * `close`. Existing streams are closed before the values are cleared.
 */
export function createBrowserConversationTransport(
  options: BrowserConversationTransportOptions,
): ConversationTransport {
  const origin = exactHttpOrigin(options.origin);
  const participantId = boundedIdentifier(
    options.participantId,
    "participantId",
  );
  let operatorCredential = boundedCredential(
    options.operatorCredential,
    "operatorCredential",
  );
  let participantCredential = boundedCredential(
    options.participantCredential,
    "participantCredential",
  );
  let closed = false;
  const streams = new Set<ConversationTransportStream>();
  const activeRequests = new Set<AbortController>();
  const requestTimeoutMs = boundedRequestTimeout(options.requestTimeoutMs);
  const fetchImplementation: typeof globalThis.fetch =
    options.fetch ?? ((input, init) => globalThis.fetch(input, init));
  const assertOpen = () => {
    if (closed) {
      throw new Error("conversation transport is closed");
    }
  };

  return {
    participantId,
    async listChannels() {
      assertOpen();
      return requestJson<Channel[]>(
        fetchImplementation,
        origin,
        "/v1/channels",
        operatorCredential,
        { method: "GET" },
        [200],
        requestTimeoutMs,
        activeRequests,
      );
    },
    async listMembers(channelId) {
      assertOpen();
      const exactChannelId = boundedIdentifier(channelId, "channelId");
      return requestJson<ChannelMember[]>(
        fetchImplementation,
        origin,
        `/v1/channels/${encodeURIComponent(exactChannelId)}/members`,
        participantCredential,
        { method: "GET" },
        [200],
        requestTimeoutMs,
        activeRequests,
      );
    },
    async listAttention() {
      assertOpen();
      return requestJson<ConversationAttention[]>(
        fetchImplementation,
        origin,
        "/v1/conversations/attention",
        participantCredential,
        { method: "GET" },
        [200],
        requestTimeoutMs,
        activeRequests,
      );
    },
    openStream(streamOptions) {
      assertOpen();
      const stream = openBrowserChannelStream({
        origin,
        channelId: boundedIdentifier(streamOptions.channelId, "channelId"),
        credential: participantCredential,
        after: streamOptions.after,
        accept: streamOptions.accept,
        statusChanged: streamOptions.statusChanged,
        reconnectDelaysMs: options.reconnectDelaysMs,
        readyTimeoutMs: options.readyTimeoutMs,
        maxPendingMessages: options.maxPendingMessages,
        fetch: (input, init) => fetchImplementation(input, init),
        createWebSocket: options.createWebSocket,
        delay: options.delay,
        scheduleTimeout: options.scheduleTimeout,
      });
      streams.add(stream);
      void stream.closed.then(
        () => streams.delete(stream),
        () => streams.delete(stream),
      );
      return stream;
    },
    async send(channelId, message) {
      assertOpen();
      const exactChannelId = boundedIdentifier(channelId, "channelId");
      return requestJson<Message>(
        fetchImplementation,
        origin,
        `/v1/channels/${encodeURIComponent(exactChannelId)}/messages`,
        participantCredential,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(message),
        },
        [200, 201],
        requestTimeoutMs,
        activeRequests,
      );
    },
    async advanceRead(channelId, throughSeq) {
      assertOpen();
      const exactChannelId = boundedIdentifier(channelId, "channelId");
      if (!Number.isSafeInteger(throughSeq) || throughSeq < 0) {
        throw new Error("throughSeq must be a non-negative safe integer");
      }
      return requestJson<ConversationAttention>(
        fetchImplementation,
        origin,
        `/v1/channels/${encodeURIComponent(exactChannelId)}/read-cursor`,
        participantCredential,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ through_seq: throughSeq }),
        },
        [200],
        requestTimeoutMs,
        activeRequests,
      );
    },
    close() {
      if (closed) return;
      closed = true;
      for (const request of activeRequests) request.abort();
      activeRequests.clear();
      for (const stream of streams) stream.close();
      streams.clear();
      operatorCredential = "";
      participantCredential = "";
    },
  };
}

async function requestJson<T>(
  fetchImplementation: typeof globalThis.fetch,
  origin: string,
  path: string,
  credential: string,
  init: RequestInit,
  acceptedStatuses: readonly number[],
  timeoutMs: number,
  activeRequests: Set<AbortController>,
): Promise<T> {
  const controller = new AbortController();
  activeRequests.add(controller);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetchImplementation(new URL(path, origin), {
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
    activeRequests.delete(controller);
  }
}
