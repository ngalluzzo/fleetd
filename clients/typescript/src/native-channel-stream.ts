import WebSocket from "ws";

import type { Message } from "./generated/types.gen.ts";
import {
  DEFAULT_CHANNEL_STREAM_MAX_PENDING_MESSAGES,
  DEFAULT_CHANNEL_STREAM_READY_TIMEOUT_MS,
  DEFAULT_CHANNEL_STREAM_RECONNECT_DELAYS_MS,
  MessageAcceptanceQueue,
  createAcceptedIdentityIndex,
  decodeChannelMessage,
  isChannelStreamCursor,
  isDecodeFailure,
  type ChannelStreamStatus,
} from "./channel-stream-core.ts";

export type NativeChannelStreamErrorCode =
  | "consumer_rejected"
  | "invalid_options"
  | "reconnect_exhausted"
  | "server_protocol_error"
  | "socket_open_failed"
  | "upgrade_rejected";

export type NativeChannelStreamStatus = ChannelStreamStatus;

export class NativeChannelStreamError extends Error {
  declare readonly code: NativeChannelStreamErrorCode;
  declare readonly status?: number;

  constructor(
    code: NativeChannelStreamErrorCode,
    message: string,
    options?: ErrorOptions & { readonly status?: number },
  ) {
    super(message, options);
    this.name = "NativeChannelStreamError";
    this.code = code;
    if (options?.status !== undefined) this.status = options.status;
  }
}

export interface NativeChannelStreamSocket {
  onclose: ((event: { readonly code?: number }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  onmessage:
    | ((event: { readonly data: unknown; readonly isBinary: boolean }) => void)
    | null;
  onopen: ((event: unknown) => void) | null;
  onunexpectedresponse:
    | ((event: { readonly status: number }) => void)
    | null;
  close(code?: number, reason?: string): void;
}

export interface NativeChannelStreamSocketRequest {
  /** Header names and values passed only to the WebSocket upgrade request. */
  readonly headers: Readonly<Record<string, string>>;
}

export type NativeChannelStreamSocketFactory = (
  url: string,
  request: NativeChannelStreamSocketRequest,
) => NativeChannelStreamSocket;

export type NativeChannelStreamTimeoutScheduler = (
  callback: () => void,
  milliseconds: number,
) => () => void;

export interface NativeChannelStreamOptions {
  /** Exact Fleetd HTTP(S) origin. */
  origin: string;
  channelId: string;
  /** Held only in this adapter's memory and sent only as an upgrade header. */
  credential: string;
  /** Highest message sequence already accepted by the consumer. */
  after?: number;
  /** Resolving accepts one message; rejection terminates without advancing. */
  accept(message: Message): void | Promise<void>;
  /** Optional observation of exact local connection-state transitions. */
  statusChanged?(status: NativeChannelStreamStatus): void;
  /** One delay per permitted reconnect after the initial connection. */
  reconnectDelaysMs?: readonly number[];
  /** Bounds frames waiting behind one asynchronous consumer acceptance. */
  maxPendingMessages?: number;
  /** Bounds each WebSocket upgrade attempt. */
  readyTimeoutMs?: number;
  /** Transport seams for deterministic tests and alternate native runtimes. */
  createWebSocket?: NativeChannelStreamSocketFactory;
  delay?: (milliseconds: number) => Promise<void>;
  scheduleTimeout?: NativeChannelStreamTimeoutScheduler;
}

export interface NativeChannelStream {
  /** Highest sequence whose consumer acceptance has resolved successfully. */
  readonly cursor: number;
  /** Exact locally observed connection state. */
  readonly status: NativeChannelStreamStatus;
  /** Resolves after an explicit close; rejects on terminal adapter failure. */
  readonly closed: Promise<void>;
  close(): void;
}

interface NormalizedOptions {
  origin: string;
  channelId: string;
  initialCursor: number;
  accept(message: Message): void | Promise<void>;
  statusChanged?(status: NativeChannelStreamStatus): void;
  reconnectDelaysMs: readonly number[];
  maxPendingMessages: number;
  readyTimeoutMs: number;
  createWebSocket: NativeChannelStreamSocketFactory;
  delay(milliseconds: number): Promise<void>;
  scheduleTimeout: NativeChannelStreamTimeoutScheduler;
}

/**
 * Opens Fleetd's bearer-authenticated native channel stream.
 *
 * Every server text frame is one immutable Message. The adapter performs no
 * history polling: disconnects, lag, and backpressure all recover by opening
 * the same stream from the highest cursor the consumer durably accepted.
 */
export function openNativeChannelStream(
  options: NativeChannelStreamOptions,
): NativeChannelStream {
  const normalized = normalizeOptions(options);
  let credential = options.credential;
  let cursor = normalized.initialCursor;
  let status: NativeChannelStreamStatus = "connecting";
  let stopRequested = false;
  let activeSocket: NativeChannelStreamSocket | undefined;
  let activeAttempt: AbortController | undefined;
  const acceptedIdentities = createAcceptedIdentityIndex();
  const setStatus = (next: NativeChannelStreamStatus) => {
    if (status === next) return;
    status = next;
    try {
      normalized.statusChanged?.(next);
    } catch {
      // Status observation never participates in message acceptance.
    }
  };
  try {
    normalized.statusChanged?.(status);
  } catch {
    // Status observation never participates in message acceptance.
  }

  const closed = (async () => {
    let reconnectIndex = 0;
    try {
      while (!stopRequested) {
        if (reconnectIndex > 0) {
          const reconnectDelay =
            normalized.reconnectDelaysMs[reconnectIndex - 1];
          if (reconnectDelay === undefined) {
            throw new NativeChannelStreamError(
              "reconnect_exhausted",
              "native channel stream exhausted its bounded reconnect budget",
            );
          }
          setStatus("reconnecting");
          await normalized.delay(reconnectDelay);
          if (stopRequested) return;
        }

        const attemptCursor = cursor;
        const attemptController = new AbortController();
        activeAttempt = attemptController;
        let attemptTimedOut = false;
        let releaseTimeout = normalized.scheduleTimeout(() => {
          attemptTimedOut = true;
          attemptController.abort();
        }, normalized.readyTimeoutMs);
        let socket: NativeChannelStreamSocket | undefined;
        try {
          const url = nativeSocketUrl(
            normalized.origin,
            normalized.channelId,
            attemptCursor,
          );
          try {
            socket = normalized.createWebSocket(url, {
              headers: { Authorization: `Bearer ${credential}` },
            });
          } catch (cause) {
            throw new NativeChannelStreamError(
              "socket_open_failed",
              "native channel stream could not create its WebSocket",
              { cause },
            );
          }
          activeSocket = socket;
          await consumeNativeSocketAttempt({
            socket,
            channelId: normalized.channelId,
            accept: normalized.accept,
            maxPendingMessages: normalized.maxPendingMessages,
            acceptedIdentities,
            signal: attemptController.signal,
            readyTimeout: () => {
              if (attemptTimedOut) return;
              releaseTimeout();
              releaseTimeout = () => {};
            },
            ready: () => setStatus("live"),
            getCursor: () => cursor,
            setCursor: (accepted) => {
              cursor = accepted;
            },
            isStopped: () => stopRequested,
          });
        } finally {
          releaseTimeout();
          if (activeAttempt === attemptController) activeAttempt = undefined;
          if (socket && activeSocket === socket) activeSocket = undefined;
        }

        if (!stopRequested) {
          setStatus("reconnecting");
          reconnectIndex += 1;
        }
      }
    } catch (error) {
      setStatus("failed");
      throw error;
    } finally {
      credential = "";
      activeAttempt = undefined;
      activeSocket = undefined;
      if (stopRequested) setStatus("closed");
    }
  })();

  return {
    get cursor() {
      return cursor;
    },
    get status() {
      return status;
    },
    closed,
    close() {
      if (stopRequested) return;
      stopRequested = true;
      credential = "";
      activeAttempt?.abort();
      closeSocket(activeSocket, "fleetd_client_stop");
    },
  };
}

function normalizeOptions(options: NativeChannelStreamOptions): NormalizedOptions {
  if (!options || typeof options !== "object") {
    throw invalidOptions("options are required");
  }
  if (typeof options.credential !== "string" || options.credential.length === 0) {
    throw invalidOptions("a non-empty in-memory credential is required");
  }
  if (typeof options.channelId !== "string" || options.channelId.length === 0) {
    throw invalidOptions("a non-empty channel ID is required");
  }
  if (typeof options.accept !== "function") {
    throw invalidOptions("an accept callback is required");
  }

  let parsedOrigin: URL;
  try {
    parsedOrigin = new URL(options.origin);
  } catch (cause) {
    throw invalidOptions("origin must be an absolute HTTP(S) origin", cause);
  }
  if (
    !["http:", "https:"].includes(parsedOrigin.protocol) ||
    parsedOrigin.username ||
    parsedOrigin.password ||
    (parsedOrigin.pathname !== "/" && parsedOrigin.pathname !== "") ||
    parsedOrigin.search ||
    parsedOrigin.hash
  ) {
    throw invalidOptions("origin must contain only an HTTP(S) authority");
  }

  const initialCursor = options.after ?? 0;
  if (!isChannelStreamCursor(initialCursor)) {
    throw invalidOptions("after must be a non-negative safe integer");
  }

  const reconnectDelaysMs =
    options.reconnectDelaysMs ?? DEFAULT_CHANNEL_STREAM_RECONNECT_DELAYS_MS;
  if (
    !Array.isArray(reconnectDelaysMs) ||
    reconnectDelaysMs.some(
      (delay) => !Number.isSafeInteger(delay) || delay < 0 || delay > 60_000,
    )
  ) {
    throw invalidOptions(
      "reconnect delays must be integers between zero and 60000 milliseconds",
    );
  }

  const maxPendingMessages =
    options.maxPendingMessages ?? DEFAULT_CHANNEL_STREAM_MAX_PENDING_MESSAGES;
  if (
    !Number.isSafeInteger(maxPendingMessages) ||
    maxPendingMessages < 1 ||
    maxPendingMessages > 4_096
  ) {
    throw invalidOptions("maxPendingMessages must be between 1 and 4096");
  }

  const readyTimeoutMs =
    options.readyTimeoutMs ?? DEFAULT_CHANNEL_STREAM_READY_TIMEOUT_MS;
  if (
    !Number.isSafeInteger(readyTimeoutMs) ||
    readyTimeoutMs < 100 ||
    readyTimeoutMs > 60_000
  ) {
    throw invalidOptions("readyTimeoutMs must be between 100 and 60000");
  }

  const delay =
    options.delay ??
    ((milliseconds: number) =>
      new Promise<void>((resolve) => setTimeout(resolve, milliseconds)));
  const scheduleTimeout =
    options.scheduleTimeout ??
    ((callback: () => void, milliseconds: number) => {
      const handle = setTimeout(callback, milliseconds);
      return () => clearTimeout(handle);
    });

  return {
    origin: parsedOrigin.origin,
    channelId: options.channelId,
    initialCursor,
    accept: options.accept,
    statusChanged: options.statusChanged,
    reconnectDelaysMs: [...reconnectDelaysMs],
    maxPendingMessages,
    readyTimeoutMs,
    createWebSocket: options.createWebSocket ?? createNativeWebSocket,
    delay,
    scheduleTimeout,
  };
}

interface SocketAttemptOptions {
  socket: NativeChannelStreamSocket;
  channelId: string;
  accept(message: Message): void | Promise<void>;
  maxPendingMessages: number;
  acceptedIdentities: ReturnType<typeof createAcceptedIdentityIndex>;
  signal: AbortSignal;
  readyTimeout(): void;
  ready(): void;
  getCursor(): number;
  setCursor(cursor: number): void;
  isStopped(): boolean;
}

function consumeNativeSocketAttempt(options: SocketAttemptOptions): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let opened = false;
    let disconnected = false;
    let settled = false;
    let terminalError: NativeChannelStreamError | undefined;
    let lane: MessageAcceptanceQueue;

    const detach = () => {
      options.socket.onopen = null;
      options.socket.onmessage = null;
      options.socket.onerror = null;
      options.socket.onclose = null;
      options.socket.onunexpectedresponse = null;
      options.signal.removeEventListener("abort", abortAttempt);
    };

    const settleIfPossible = () => {
      if (settled || lane.busy) return;
      if (terminalError) {
        settled = true;
        detach();
        reject(terminalError);
      } else if (disconnected && lane.pending === 0) {
        settled = true;
        detach();
        resolve();
      }
    };

    const fail = (error: NativeChannelStreamError) => {
      if (settled || terminalError) return;
      terminalError = error;
      lane.stop();
      closeSocket(options.socket, "fleetd_client_error");
      settleIfPossible();
    };

    const disconnect = (reason: string) => {
      if (settled || terminalError || disconnected) return;
      disconnected = true;
      closeSocket(options.socket, reason);
      settleIfPossible();
    };

    lane = new MessageAcceptanceQueue({
      accept: options.accept,
      maxPendingMessages: options.maxPendingMessages,
      acceptedIdentities: options.acceptedIdentities,
      getCursor: options.getCursor,
      setCursor: options.setCursor,
      failed(failure) {
        if (failure.type === "identity_conflict") {
          fail(
            new NativeChannelStreamError(
              "server_protocol_error",
              "native channel stream reused a stable message identity inconsistently",
            ),
          );
        } else {
          fail(
            new NativeChannelStreamError(
              "consumer_rejected",
              "native channel stream consumer rejected a message",
              { cause: failure.cause },
            ),
          );
        }
      },
      idle: settleIfPossible,
    });

    options.socket.onopen = () => {
      if (settled || terminalError || disconnected) return;
      if (opened) {
        fail(
          new NativeChannelStreamError(
            "server_protocol_error",
            "native channel stream opened more than once",
          ),
        );
        return;
      }
      opened = true;
      options.readyTimeout();
      options.ready();
    };

    options.socket.onmessage = (event) => {
      if (settled || terminalError || disconnected) return;
      if (!opened || event.isBinary) {
        fail(
          new NativeChannelStreamError(
            "server_protocol_error",
            "native channel stream received a frame outside the text protocol",
          ),
        );
        return;
      }
      const text = nativeTextFrame(event.data);
      if (text === undefined) {
        fail(
          new NativeChannelStreamError(
            "server_protocol_error",
            "native channel stream received an unreadable text frame",
          ),
        );
        return;
      }
      let frame: unknown;
      try {
        frame = JSON.parse(text);
      } catch {
        fail(
          new NativeChannelStreamError(
            "server_protocol_error",
            "native channel stream received invalid JSON",
          ),
        );
        return;
      }
      const message = decodeChannelMessage(frame, options.channelId);
      if (isDecodeFailure(message)) {
        fail(
          new NativeChannelStreamError(
            "server_protocol_error",
            "native channel stream received an invalid immutable message envelope",
          ),
        );
        return;
      }
      if (!lane.offer(message)) {
        lane.clear();
        disconnect("fleetd_client_backpressure");
      }
    };

    options.socket.onunexpectedresponse = (event) => {
      fail(
        new NativeChannelStreamError(
          "upgrade_rejected",
          `native channel stream upgrade returned HTTP ${event.status}`,
          { status: event.status },
        ),
      );
    };
    options.socket.onerror = () => {
      disconnect("fleetd_client_transport");
    };
    options.socket.onclose = (event) => {
      disconnected = true;
      if (options.isStopped()) lane.clear();
      if (opened && event.code === 1008) {
        fail(
          new NativeChannelStreamError(
            "upgrade_rejected",
            "native channel stream authorization was revoked",
          ),
        );
        return;
      }
      settleIfPossible();
    };
    const abortAttempt = () => {
      disconnect("fleetd_client_ready_timeout");
    };
    options.signal.addEventListener("abort", abortAttempt, { once: true });
    if (options.signal.aborted) abortAttempt();
  });
}

function createNativeWebSocket(
  url: string,
  request: NativeChannelStreamSocketRequest,
): NativeChannelStreamSocket {
  return new NodeWebSocketAdapter(
    new WebSocket(url, { headers: { ...request.headers } }),
  );
}

class NodeWebSocketAdapter implements NativeChannelStreamSocket {
  onclose: ((event: { readonly code?: number }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  onmessage:
    | ((event: { readonly data: unknown; readonly isBinary: boolean }) => void)
    | null = null;
  onopen: ((event: unknown) => void) | null = null;
  onunexpectedresponse:
    | ((event: { readonly status: number }) => void)
    | null = null;

  readonly #socket: WebSocket;

  constructor(socket: WebSocket) {
    this.#socket = socket;
    socket.on("open", () => this.onopen?.({}));
    socket.on("message", (data, isBinary) =>
      this.onmessage?.({ data, isBinary }),
    );
    socket.on("error", (error) => this.onerror?.(error));
    socket.on("close", (code) => this.onclose?.({ code }));
    socket.on("unexpected-response", (_request, response) => {
      response.resume();
      this.onunexpectedresponse?.({ status: response.statusCode ?? 0 });
    });
  }

  close(code?: number, reason?: string): void {
    if (this.#socket.readyState === WebSocket.CONNECTING) {
      this.#socket.terminate();
      return;
    }
    if (
      this.#socket.readyState === WebSocket.OPEN ||
      this.#socket.readyState === WebSocket.CLOSING
    ) {
      this.#socket.close(code, reason);
    }
  }
}

function nativeTextFrame(data: unknown): string | undefined {
  if (typeof data === "string") return data;
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  if (ArrayBuffer.isView(data)) {
    return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString(
      "utf8",
    );
  }
  if (Array.isArray(data) && data.every((part) => Buffer.isBuffer(part))) {
    return Buffer.concat(data as Buffer[]).toString("utf8");
  }
  return undefined;
}

function nativeSocketUrl(origin: string, channelId: string, after: number): string {
  const url = new URL(
    `/v1/channels/${encodeURIComponent(channelId)}/stream`,
    origin,
  );
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("after", String(after));
  return url.href;
}

function invalidOptions(
  message: string,
  cause?: unknown,
): NativeChannelStreamError {
  return new NativeChannelStreamError("invalid_options", message, { cause });
}

function closeSocket(
  socket: NativeChannelStreamSocket | undefined,
  reason: string,
): void {
  if (!socket) return;
  try {
    socket.close(1000, reason);
  } catch {
    // Closing is best-effort; durable replay begins from the accepted cursor.
  }
}
