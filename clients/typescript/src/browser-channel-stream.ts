import type { Message } from "./generated/types.gen.ts";

export const BROWSER_CHANNEL_STREAM_PROTOCOL =
  "fleetd.channel-stream.browser.v1" as const;
export const BROWSER_CHANNEL_STREAM_PATH =
  "/v1/browser/channel-stream" as const;

const DEFAULT_RECONNECT_DELAYS_MS = [250, 1_000, 2_000] as const;
const DEFAULT_MAX_PENDING_MESSAGES = 64;
const DEFAULT_READY_TIMEOUT_MS = 10_000;
const MAX_RETAINED_IDENTITIES = 4_096;

export type BrowserChannelStreamErrorCode =
  | "consumer_rejected"
  | "grant_issue_failed"
  | "grant_linkage_mismatch"
  | "invalid_options"
  | "reconnect_exhausted"
  | "server_protocol_error"
  | "socket_protocol_mismatch";

/** Locally observed wire state. It never represents remote agent activity. */
export type BrowserChannelStreamStatus =
  | "connecting"
  | "live"
  | "reconnecting"
  | "failed"
  | "closed";

export class BrowserChannelStreamError extends Error {
  declare readonly code: BrowserChannelStreamErrorCode;

  constructor(
    code: BrowserChannelStreamErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "BrowserChannelStreamError";
    this.code = code;
  }
}

/** The browser WebSocket subset used by the adapter and exposed for tests. */
export interface BrowserChannelStreamSocket {
  readonly protocol: string;
  onclose: ((event: unknown) => void) | null;
  onerror: ((event: unknown) => void) | null;
  onmessage: ((event: { readonly data: unknown }) => void) | null;
  onopen: ((event: unknown) => void) | null;
  close(code?: number, reason?: string): void;
  send(data: string): void;
}

export type BrowserChannelStreamSocketFactory = (
  url: string,
  protocol: typeof BROWSER_CHANNEL_STREAM_PROTOCOL,
) => BrowserChannelStreamSocket;

export type BrowserChannelStreamFetch = (
  input: string,
  init: RequestInit,
) => Promise<Response>;

export type BrowserChannelStreamTimeoutScheduler = (
  callback: () => void,
  milliseconds: number,
) => () => void;

export interface BrowserChannelStreamOptions {
  /** Exact Fleetd HTTP(S) origin serving the browser surface. */
  origin: string;
  channelId: string;
  /** Held only in this adapter's memory and used only to mint stream grants. */
  credential: string;
  /** Highest message sequence already accepted by the consumer. */
  after?: number;
  /** Resolving accepts one message; rejection terminates without advancing. */
  accept(message: Message): void | Promise<void>;
  /** Optional observation of exact local connection-state transitions. */
  statusChanged?(status: BrowserChannelStreamStatus): void;
  /** One delay per permitted reconnect after the initial connection. */
  reconnectDelaysMs?: readonly number[];
  /** Bounds frames waiting behind one asynchronous consumer acceptance. */
  maxPendingMessages?: number;
  /** Bounds grant issuance, WebSocket opening, redemption, and ready linkage. */
  readyTimeoutMs?: number;
  /** Transport seams for deterministic tests and non-window browser shells. */
  fetch?: BrowserChannelStreamFetch;
  createWebSocket?: BrowserChannelStreamSocketFactory;
  delay?: (milliseconds: number) => Promise<void>;
  scheduleTimeout?: BrowserChannelStreamTimeoutScheduler;
}

export interface BrowserChannelStream {
  /** Highest sequence whose consumer acceptance has resolved successfully. */
  readonly cursor: number;
  /** Exact locally observed connection state. */
  readonly status: BrowserChannelStreamStatus;
  /** Resolves after an explicit close; rejects on terminal adapter failure. */
  readonly closed: Promise<void>;
  close(): void;
}

interface NormalizedOptions {
  origin: string;
  channelId: string;
  initialCursor: number;
  accept(message: Message): void | Promise<void>;
  statusChanged?(status: BrowserChannelStreamStatus): void;
  reconnectDelaysMs: readonly number[];
  maxPendingMessages: number;
  readyTimeoutMs: number;
  fetch: BrowserChannelStreamFetch;
  createWebSocket: BrowserChannelStreamSocketFactory;
  delay(milliseconds: number): Promise<void>;
  scheduleTimeout: BrowserChannelStreamTimeoutScheduler;
}

interface AcceptedIdentityIndex {
  bySequence: Map<number, string>;
  byId: Map<string, number>;
  order: number[];
}

class RetryableTransportError extends Error {}

/**
 * Starts the browser-only Fleetd channel wire adapter.
 *
 * The adapter has exactly two network edges: authenticated grant issuance and
 * the fixed browser WebSocket. It never reads HTTP history and has no polling
 * fallback. Presentation code owns rendering and decides when a message has
 * been accepted by resolving `accept`.
 */
export function openBrowserChannelStream(
  options: BrowserChannelStreamOptions,
): BrowserChannelStream {
  const normalized = normalizeOptions(options);
  let credential = options.credential;
  let cursor = normalized.initialCursor;
  let status: BrowserChannelStreamStatus = "connecting";
  let stopRequested = false;
  let activeSocket: BrowserChannelStreamSocket | undefined;
  let activeGrantRequest: AbortController | undefined;
  const acceptedIdentities: AcceptedIdentityIndex = {
    bySequence: new Map(),
    byId: new Map(),
    order: [],
  };
  const setStatus = (next: BrowserChannelStreamStatus) => {
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
          const delay = normalized.reconnectDelaysMs[reconnectIndex - 1];
          if (delay === undefined) {
            throw new BrowserChannelStreamError(
              "reconnect_exhausted",
              "browser channel stream exhausted its bounded reconnect budget",
            );
          }
          setStatus("reconnecting");
          await normalized.delay(delay);
          if (stopRequested) return;
        }

        const attemptCursor = cursor;
        const attemptController = new AbortController();
        activeGrantRequest = attemptController;
        const attemptSignal = attemptController.signal;
        let attemptTimedOut = false;
        let releaseTimeout = normalized.scheduleTimeout(() => {
          attemptTimedOut = true;
          attemptController.abort();
        }, normalized.readyTimeoutMs);
        let grant: string | undefined;
        let socket: BrowserChannelStreamSocket | undefined;
        try {
          let result:
            | { readonly type: "grant"; readonly issuedGrant: string }
            | { readonly type: "aborted" };
          try {
            result = await Promise.race([
              issueGrant(normalized, credential, attemptCursor, attemptSignal).then(
                (issuedGrant) => ({ type: "grant" as const, issuedGrant }),
              ),
              aborted(attemptSignal).then(() => ({ type: "aborted" as const })),
            ]);
          } catch (error) {
            if (stopRequested) return;
            if (error instanceof RetryableTransportError) {
              setStatus("reconnecting");
              reconnectIndex += 1;
              continue;
            }
            throw error;
          }
          if (result.type === "aborted") {
            if (stopRequested) return;
            setStatus("reconnecting");
            reconnectIndex += 1;
            continue;
          }
          grant = result.issuedGrant;
          if (stopRequested) return;

          const socketUrl = browserSocketUrl(normalized.origin);
          try {
            socket = normalized.createWebSocket(
              socketUrl,
              BROWSER_CHANNEL_STREAM_PROTOCOL,
            );
          } catch (cause) {
            throw new BrowserChannelStreamError(
              "grant_linkage_mismatch",
              "browser channel stream could not create its fixed WebSocket",
              { cause },
            );
          }

          activeSocket = socket;
          await consumeSocketAttempt({
            socket,
            grant,
            channelId: normalized.channelId,
            expectedAfter: attemptCursor,
            accept: normalized.accept,
            maxPendingMessages: normalized.maxPendingMessages,
            acceptedIdentities,
            signal: attemptSignal,
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
          grant = undefined;
          if (activeGrantRequest === attemptController) {
            activeGrantRequest = undefined;
          }
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
      activeGrantRequest = undefined;
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
      activeGrantRequest?.abort();
      closeSocket(activeSocket, "fleetd_client_stop");
    },
  };
}

function normalizeOptions(options: BrowserChannelStreamOptions): NormalizedOptions {
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
  if (!isCursor(initialCursor)) {
    throw invalidOptions("after must be a non-negative safe integer");
  }

  const reconnectDelaysMs =
    options.reconnectDelaysMs ?? DEFAULT_RECONNECT_DELAYS_MS;
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
    options.maxPendingMessages ?? DEFAULT_MAX_PENDING_MESSAGES;
  if (
    !Number.isSafeInteger(maxPendingMessages) ||
    maxPendingMessages < 1 ||
    maxPendingMessages > 4_096
  ) {
    throw invalidOptions("maxPendingMessages must be between 1 and 4096");
  }

  const readyTimeoutMs = options.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;
  if (
    !Number.isSafeInteger(readyTimeoutMs) ||
    readyTimeoutMs < 100 ||
    readyTimeoutMs > 15_000
  ) {
    throw invalidOptions("readyTimeoutMs must be between 100 and 15000");
  }

  const fetchImplementation =
    options.fetch ??
    ((input: string, init: RequestInit) => globalThis.fetch(input, init));
  const socketFactory =
    options.createWebSocket ??
    ((url, protocol) =>
      new WebSocket(url, protocol) as unknown as BrowserChannelStreamSocket);
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
    fetch: fetchImplementation,
    createWebSocket: socketFactory,
    delay,
    scheduleTimeout,
  };
}

function invalidOptions(message: string, cause?: unknown): BrowserChannelStreamError {
  return new BrowserChannelStreamError("invalid_options", message, { cause });
}

async function issueGrant(
  options: NormalizedOptions,
  credential: string,
  after: number,
  signal: AbortSignal,
): Promise<string> {
  const url = new URL(
    `/v1/channels/${encodeURIComponent(options.channelId)}/stream-grants`,
    options.origin,
  ).href;
  let response: Response;
  try {
    response = await options.fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${credential}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        after,
        protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
      }),
      cache: "no-store",
      credentials: "omit",
      referrerPolicy: "no-referrer",
      signal,
    });
  } catch (cause) {
    throw new RetryableTransportError("stream grant transport failed", {
      cause,
    });
  }

  if (response.status !== 201) {
    throw new BrowserChannelStreamError(
      "grant_issue_failed",
      `browser channel stream grant issuance returned HTTP ${response.status}`,
    );
  }
  if (!response.headers.get("cache-control")?.toLowerCase().includes("no-store")) {
    throw new BrowserChannelStreamError(
      "grant_linkage_mismatch",
      "browser channel stream grant response was not marked no-store",
    );
  }

  let value: unknown;
  try {
    value = await response.json();
  } catch (cause) {
    throw new BrowserChannelStreamError(
      "grant_linkage_mismatch",
      "browser channel stream grant response was not valid JSON",
      { cause },
    );
  }
  if (!isExactRecord(value, ["expires_at_ms", "grant", "protocol", "websocket_path"])) {
    throw new BrowserChannelStreamError(
      "grant_linkage_mismatch",
      "browser channel stream grant response had an unexpected shape",
    );
  }
  if (
    typeof value.grant !== "string" ||
    !/^fl_sg_[A-Za-z0-9_-]{43}$/.test(value.grant) ||
    !Number.isSafeInteger(value.expires_at_ms) ||
    value.protocol !== BROWSER_CHANNEL_STREAM_PROTOCOL ||
    value.websocket_path !== BROWSER_CHANNEL_STREAM_PATH
  ) {
    throw new BrowserChannelStreamError(
      "grant_linkage_mismatch",
      "browser channel stream grant response did not link the exact protocol edge",
    );
  }

  return value.grant;
}

interface SocketAttemptOptions {
  socket: BrowserChannelStreamSocket;
  grant: string;
  channelId: string;
  expectedAfter: number;
  accept(message: Message): void | Promise<void>;
  maxPendingMessages: number;
  acceptedIdentities: AcceptedIdentityIndex;
  signal: AbortSignal;
  readyTimeout(): void;
  ready(): void;
  getCursor(): number;
  setCursor(cursor: number): void;
  isStopped(): boolean;
}

function consumeSocketAttempt(options: SocketAttemptOptions): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const pending: Message[] = [];
    let grant: string | undefined = options.grant;
    let opened = false;
    let ready = false;
    let accepting = false;
    let disconnected = false;
    let settled = false;
    let terminalError: BrowserChannelStreamError | undefined;

    const detach = () => {
      options.socket.onopen = null;
      options.socket.onmessage = null;
      options.socket.onerror = null;
      options.socket.onclose = null;
      options.signal.removeEventListener("abort", abortAttempt);
      grant = undefined;
    };

    const settleIfPossible = () => {
      if (settled || accepting) return;
      if (terminalError) {
        settled = true;
        detach();
        reject(terminalError);
      } else if (disconnected && pending.length === 0) {
        settled = true;
        detach();
        resolve();
      }
    };

    const fail = (error: BrowserChannelStreamError) => {
      if (settled || terminalError) return;
      terminalError = error;
      pending.length = 0;
      closeSocket(options.socket, "fleetd_client_error");
      settleIfPossible();
    };

    const disconnect = (reason: string) => {
      if (settled || terminalError || disconnected) return;
      disconnected = true;
      closeSocket(options.socket, reason);
      settleIfPossible();
    };

    const drain = async () => {
      if (accepting || settled || terminalError) return;
      accepting = true;
      try {
        while (!settled && !terminalError && pending.length > 0) {
          const message = pending.shift();
          if (!message) break;
          const duplicate = classifyDuplicate(
            message,
            options.getCursor(),
            options.acceptedIdentities,
          );
          if (duplicate === "conflict") {
            fail(
              new BrowserChannelStreamError(
                "server_protocol_error",
                "browser channel stream reused a stable message identity inconsistently",
              ),
            );
            break;
          }
          if (duplicate === "duplicate") continue;

          try {
            await options.accept(message);
          } catch (cause) {
            fail(
              new BrowserChannelStreamError(
                "consumer_rejected",
                "browser channel stream consumer rejected a message",
                { cause },
              ),
            );
            break;
          }
          options.setCursor(message.seq);
          rememberIdentity(message, options.acceptedIdentities);
        }
      } finally {
        accepting = false;
        settleIfPossible();
      }
    };

    options.socket.onopen = () => {
      if (settled || terminalError || disconnected) return;
      if (opened || options.socket.protocol !== BROWSER_CHANNEL_STREAM_PROTOCOL) {
        grant = undefined;
        fail(
          new BrowserChannelStreamError(
            "socket_protocol_mismatch",
            "browser channel stream did not negotiate the exact protocol",
          ),
        );
        return;
      }
      opened = true;
      let redemptionFrame: string | undefined;
      try {
        redemptionFrame = JSON.stringify({ type: "redeem", grant });
        options.socket.send(redemptionFrame);
      } catch {
        disconnect("fleetd_client_transport");
      } finally {
        redemptionFrame = undefined;
        grant = undefined;
      }
    };

    options.socket.onmessage = (event) => {
      if (settled || terminalError || disconnected) return;
      if (typeof event.data !== "string") {
        fail(protocolFailure("browser channel stream received a non-text frame"));
        return;
      }

      let frame: unknown;
      try {
        frame = JSON.parse(event.data);
      } catch {
        fail(protocolFailure("browser channel stream received invalid JSON"));
        return;
      }

      if (!ready) {
        if (!isReadyFrame(frame, options.channelId, options.expectedAfter)) {
          fail(
            protocolFailure(
              "browser channel stream did not establish the exact grant linkage",
            ),
          );
          return;
        }
        ready = true;
        options.readyTimeout();
        options.ready();
        return;
      }

      const message = decodeMessageFrame(frame, options.channelId);
      if (message instanceof BrowserChannelStreamError) {
        fail(message);
        return;
      }
      if (pending.length >= options.maxPendingMessages) {
        pending.length = 0;
        disconnect("fleetd_client_backpressure");
        return;
      }
      pending.push(message);
      void drain();
    };

    options.socket.onerror = () => {
      disconnect("fleetd_client_transport");
    };
    options.socket.onclose = () => {
      disconnected = true;
      if (options.isStopped()) pending.length = 0;
      settleIfPossible();
    };
    const abortAttempt = () => {
      disconnect("fleetd_client_ready_timeout");
    };
    options.signal.addEventListener("abort", abortAttempt, { once: true });
    if (options.signal.aborted) abortAttempt();
  });
}

function isReadyFrame(
  value: unknown,
  channelId: string,
  expectedAfter: number,
): boolean {
  return (
    isExactRecord(value, ["after", "channel_id", "protocol", "type"]) &&
    value.type === "ready" &&
    value.protocol === BROWSER_CHANNEL_STREAM_PROTOCOL &&
    value.channel_id === channelId &&
    value.after === expectedAfter
  );
}

function decodeMessageFrame(
  value: unknown,
  channelId: string,
): Message | BrowserChannelStreamError {
  if (!isExactRecord(value, ["message", "type"]) || value.type !== "message") {
    return protocolFailure("browser channel stream received an unsupported frame");
  }
  const message = value.message;
  if (
    !isExactRecord(message, [
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
    !Number.isSafeInteger(message.seq) ||
    (message.seq as number) <= 0 ||
    typeof message.id !== "string" ||
    message.id.length === 0 ||
    message.channel_id !== channelId ||
    typeof message.sender_id !== "string" ||
    message.sender_id.length === 0 ||
    !isNullableString(message.recipient_id) ||
    typeof message.kind !== "string" ||
    message.kind.length === 0 ||
    !isNullableString(message.correlation_id) ||
    !isNullableString(message.causation_id) ||
    !Number.isSafeInteger(message.created_at_ms)
  ) {
    return protocolFailure(
      "browser channel stream received an invalid immutable message envelope",
    );
  }
  return message as unknown as Message;
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

function rememberIdentity(message: Message, identities: AcceptedIdentityIndex): void {
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

function browserSocketUrl(origin: string): string {
  const url = new URL(BROWSER_CHANNEL_STREAM_PATH, origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.href;
}

function closeSocket(
  socket: BrowserChannelStreamSocket | undefined,
  reason: string,
): void {
  if (!socket) return;
  try {
    socket.close(1000, reason);
  } catch {
    // Closing is best-effort; durable replay begins from the accepted cursor.
  }
}

function isExactRecord(
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

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isCursor(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function protocolFailure(message: string): BrowserChannelStreamError {
  return new BrowserChannelStreamError("server_protocol_error", message);
}

function aborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
}
