import type { Message } from "./generated/types.gen.ts";

export const DEFAULT_CHANNEL_STREAM_RECONNECT_DELAYS_MS = [
  250,
  1_000,
  2_000,
] as const;
export const DEFAULT_CHANNEL_STREAM_MAX_PENDING_MESSAGES = 64;
export const DEFAULT_CHANNEL_STREAM_READY_TIMEOUT_MS = 10_000;

const MAX_RETAINED_IDENTITIES = 4_096;

/** Locally observed transport state. It never represents remote agent activity. */
export type ChannelStreamStatus =
  | "connecting"
  | "live"
  | "reconnecting"
  | "failed"
  | "closed";

export interface AcceptedIdentityIndex {
  bySequence: Map<number, string>;
  byId: Map<string, number>;
  order: number[];
}

export function createAcceptedIdentityIndex(): AcceptedIdentityIndex {
  return {
    bySequence: new Map(),
    byId: new Map(),
    order: [],
  };
}

export type MessageAcceptanceFailure =
  | { readonly type: "consumer_rejected"; readonly cause: unknown }
  | { readonly type: "identity_conflict" };

export interface MessageAcceptanceQueueOptions {
  accept(message: Message): void | Promise<void>;
  maxPendingMessages: number;
  acceptedIdentities: AcceptedIdentityIndex;
  getCursor(): number;
  setCursor(cursor: number): void;
  failed(failure: MessageAcceptanceFailure): void;
  idle(): void;
}

/**
 * Serializes durable consumer acceptance independently of a WebSocket dialect.
 *
 * A transport may disappear while acceptance is in flight. Frames already in
 * this bounded queue finish first; only a resolved consumer advances the
 * replay cursor. A later transport attempt therefore starts at the exact last
 * accepted message rather than the last frame received from the network.
 */
export class MessageAcceptanceQueue {
  readonly #options: MessageAcceptanceQueueOptions;
  readonly #pending: Message[] = [];
  #accepting = false;
  #terminal = false;

  constructor(options: MessageAcceptanceQueueOptions) {
    this.#options = options;
  }

  get busy(): boolean {
    return this.#accepting;
  }

  get pending(): number {
    return this.#pending.length;
  }

  /** Returns false when bounded backpressure requires transport replay. */
  offer(message: Message): boolean {
    if (this.#terminal) return true;
    if (this.#pending.length >= this.#options.maxPendingMessages) return false;
    this.#pending.push(message);
    void this.#drain();
    return true;
  }

  clear(): void {
    this.#pending.length = 0;
  }

  stop(): void {
    this.#terminal = true;
    this.clear();
  }

  async #drain(): Promise<void> {
    if (this.#accepting || this.#terminal) return;
    this.#accepting = true;
    try {
      while (!this.#terminal && this.#pending.length > 0) {
        const message = this.#pending.shift();
        if (!message) break;
        const duplicate = classifyDuplicate(
          message,
          this.#options.getCursor(),
          this.#options.acceptedIdentities,
        );
        if (duplicate === "conflict") {
          this.#terminal = true;
          this.clear();
          this.#options.failed({ type: "identity_conflict" });
          break;
        }
        if (duplicate === "duplicate") continue;

        try {
          await this.#options.accept(message);
        } catch (cause) {
          this.#terminal = true;
          this.clear();
          this.#options.failed({ type: "consumer_rejected", cause });
          break;
        }
        this.#options.setCursor(message.seq);
        rememberIdentity(message, this.#options.acceptedIdentities);
      }
    } finally {
      this.#accepting = false;
      this.#options.idle();
    }
  }
}

export interface ChannelMessageDecodeFailure {
  readonly error: string;
}

/** Validates the fixed envelope while retaining its opaque kind and payload. */
export function decodeChannelMessage(
  value: unknown,
  channelId: string,
): Message | ChannelMessageDecodeFailure {
  if (
    !isExactRecord(value, [
      "causation_id",
      "channel_id",
      "correlation_id",
      "created_at_ms",
      "id",
      "kind",
      "payload",
      "recipient_id",
      "sender_id",
      "seq",
    ]) ||
    !Number.isSafeInteger(value.seq) ||
    (value.seq as number) <= 0 ||
    typeof value.id !== "string" ||
    value.id.length === 0 ||
    value.channel_id !== channelId ||
    typeof value.sender_id !== "string" ||
    value.sender_id.length === 0 ||
    !isNullableString(value.recipient_id) ||
    typeof value.kind !== "string" ||
    value.kind.length === 0 ||
    !isNullableString(value.correlation_id) ||
    !isNullableString(value.causation_id) ||
    !Number.isSafeInteger(value.created_at_ms)
  ) {
    return { error: "invalid immutable message envelope" };
  }
  return value as unknown as Message;
}

export function isDecodeFailure(
  value: Message | ChannelMessageDecodeFailure,
): value is ChannelMessageDecodeFailure {
  return "error" in value;
}

export function isExactRecord(
  value: unknown,
  keys: readonly string[],
): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return (
    actual.length === expected.length &&
    actual.every((key, index) => key === expected[index])
  );
}

export function isChannelStreamCursor(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function classifyDuplicate(
  message: Message,
  cursor: number,
  identities: AcceptedIdentityIndex,
): "new" | "duplicate" | "conflict" {
  const sequenceForId = identities.byId.get(message.id);
  if (sequenceForId !== undefined && sequenceForId !== message.seq) {
    return "conflict";
  }
  const idForSequence = identities.bySequence.get(message.seq);
  if (idForSequence !== undefined && idForSequence !== message.id) {
    return "conflict";
  }
  if (message.seq <= cursor) return "duplicate";
  return "new";
}

function rememberIdentity(
  message: Message,
  identities: AcceptedIdentityIndex,
): void {
  identities.bySequence.set(message.seq, message.id);
  identities.byId.set(message.id, message.seq);
  identities.order.push(message.seq);
  if (identities.order.length <= MAX_RETAINED_IDENTITIES) return;
  const expiredSequence = identities.order.shift();
  if (expiredSequence === undefined) return;
  const expiredId = identities.bySequence.get(expiredSequence);
  identities.bySequence.delete(expiredSequence);
  if (expiredId !== undefined) identities.byId.delete(expiredId);
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}
