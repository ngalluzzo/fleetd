(() => {
  // ../../clients/typescript/src/conversation-session.ts
  var DEFAULT_MAX_RETAINED_MESSAGES = 512;

  class ConversationSession {
    #transport;
    #maxRetainedMessages;
    #listeners = new Set;
    #lanes = new Map;
    #revision = 0;
    #phase = "idle";
    #channels = [];
    #selectedChannelId = null;
    #selectionGeneration = 0;
    #cancelSelection;
    #pendingSends = 0;
    #error = null;
    #closed = false;
    constructor(transport, options = {}) {
      this.#transport = transport;
      const maxRetainedMessages = options.maxRetainedMessages ?? DEFAULT_MAX_RETAINED_MESSAGES;
      if (!Number.isSafeInteger(maxRetainedMessages) || maxRetainedMessages < 16 || maxRetainedMessages > 4096) {
        throw new Error("maxRetainedMessages must be between 16 and 4096");
      }
      this.#maxRetainedMessages = maxRetainedMessages;
    }
    get snapshot() {
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
        error: this.#error
      };
    }
    subscribe(listener) {
      this.#listeners.add(listener);
      listener(this.snapshot);
      return () => this.#listeners.delete(listener);
    }
    async start() {
      this.#assertOpen();
      this.#phase = "loading_channels";
      this.#error = null;
      this.#publish();
      try {
        this.#channels = [...await this.#transport.listChannels()];
      } catch {
        this.#fail("channel_discovery_failed", "Fleetd channel discovery failed");
        throw new Error("Fleetd channel discovery failed");
      }
      if (this.#closed)
        return;
      this.#phase = "ready";
      this.#publish();
    }
    async refreshChannels() {
      this.#assertOpen();
      try {
        this.#channels = [...await this.#transport.listChannels()];
        this.#error = null;
        this.#publish();
      } catch {
        this.#fail("channel_discovery_failed", "Fleetd channel discovery failed");
        throw new Error("Fleetd channel discovery failed");
      }
    }
    async selectChannel(channelId) {
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
      let liveResolve;
      let liveReject;
      const live = new Promise((resolve, reject) => {
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
            if (!this.#isCurrent(channelId, generation))
              return;
            this.#acceptMessage(lane, message, true);
          },
          statusChanged: (status) => {
            if (!this.#isCurrent(channelId, generation))
              return;
            lane.connection = status;
            if (status === "live")
              liveResolve();
            if (status === "failed" || status === "closed") {
              if (this.#error === null) {
                this.#fail("stream_failed", status === "failed" ? "The selected Fleetd stream failed" : "The selected Fleetd stream closed");
              }
              liveReject();
            }
            this.#publishLaneState(lane);
          }
        });
        lane.stream = stream;
        stream.closed.then(() => {
          if (!this.#isCurrent(channelId, generation) || this.#closed)
            return;
          if (this.#error === null) {
            this.#fail("stream_failed", "The selected Fleetd stream closed");
          }
        }, () => {
          if (!this.#isCurrent(channelId, generation) || this.#closed)
            return;
          if (this.#error === null) {
            this.#fail("stream_failed", "The selected Fleetd stream failed");
          }
        });
        const [observedMembers] = await Promise.all([members, live]);
        if (!this.#isCurrent(channelId, generation))
          return;
        if (!observedMembers.some((member) => member.agent_id === this.#transport.participantId)) {
          stream.close();
          this.#fail("participant_not_member", "The human participant is not a member of the selected channel");
          throw new Error("The human participant is not a member of the selected channel");
        }
        lane.members = [...observedMembers];
        lane.membersReady = true;
        this.#cancelSelection = undefined;
        this.#publishLaneState(lane);
      } catch {
        if (!this.#isCurrent(channelId, generation))
          return;
        this.#cancelSelection = undefined;
        if (this.#error === null) {
          this.#fail("membership_failed", "The selected Fleetd channel could not be opened");
        }
        lane.stream?.close();
        throw new Error("The selected Fleetd channel could not be opened");
      }
    }
    async send(message) {
      this.#assertOpen();
      const lane = this.#selectedLane();
      if (!lane || this.#phase === "failed") {
        throw this.#invalidState("a healthy selected channel is required");
      }
      this.#pendingSends += 1;
      this.#publish();
      try {
        const sent = await this.#transport.send(lane.channelId, message);
        if (sent.channel_id !== lane.channelId || sent.sender_id !== this.#transport.participantId) {
          this.#fail("message_conflict", "Fleetd returned a message outside the selected participant lane");
          throw new Error("Fleetd returned a message outside the selected participant lane");
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
    close() {
      if (this.#closed)
        return;
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
    #acceptMessage(lane, message, fromStream) {
      if (message.channel_id !== lane.channelId) {
        this.#fail("message_conflict", "The conversation transport crossed channel boundaries");
        throw new Error("conversation message crossed channel boundaries");
      }
      const bySequence = lane.bySequence.get(message.seq);
      const byId = lane.byId.get(message.id);
      if (bySequence && bySequence.id !== message.id || byId && byId.seq !== message.seq || bySequence && !jsonEqual(bySequence, message) || byId && !jsonEqual(byId, message)) {
        this.#fail("message_conflict", "Fleetd reused a stable message identity inconsistently");
        throw new Error("Fleetd reused a stable message identity inconsistently");
      }
      if (!bySequence && !byId && message.seq > lane.cursor) {
        lane.bySequence.set(message.seq, message);
        lane.byId.set(message.id, message);
        lane.messages = [...lane.messages, message].sort((left, right) => left.seq - right.seq);
        this.#enforceMessageBound(lane);
      }
      if (fromStream && message.seq > lane.cursor)
        lane.cursor = message.seq;
      if (this.#selectedChannelId === lane.channelId)
        this.#publish();
    }
    #enforceMessageBound(lane) {
      while (lane.messages.length > this.#maxRetainedMessages) {
        const expired = lane.messages.shift();
        if (!expired)
          return;
        lane.bySequence.delete(expired.seq);
        lane.byId.delete(expired.id);
      }
    }
    #lane(channelId) {
      let lane = this.#lanes.get(channelId);
      if (lane)
        return lane;
      lane = {
        channelId,
        members: [],
        membersReady: false,
        messages: [],
        bySequence: new Map,
        byId: new Map,
        cursor: 0,
        connection: "connecting"
      };
      this.#lanes.set(channelId, lane);
      return lane;
    }
    #selectedLane() {
      return this.#selectedChannelId === null ? undefined : this.#lanes.get(this.#selectedChannelId);
    }
    #isCurrent(channelId, generation) {
      return !this.#closed && this.#selectedChannelId === channelId && this.#selectionGeneration === generation;
    }
    #publishLaneState(lane) {
      if (this.#selectedChannelId !== lane.channelId)
        return;
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
    #fail(code, message) {
      this.#phase = "failed";
      this.#error = { code, message };
      this.#publish();
    }
    #invalidState(message) {
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
  function jsonEqual(left, right) {
    if (Object.is(left, right))
      return true;
    if (Array.isArray(left) || Array.isArray(right)) {
      return Array.isArray(left) && Array.isArray(right) && left.length === right.length && left.every((value, index) => jsonEqual(value, right[index]));
    }
    if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
      return false;
    }
    const leftRecord = left;
    const rightRecord = right;
    const leftKeys = Object.keys(leftRecord).sort();
    const rightKeys = Object.keys(rightRecord).sort();
    return leftKeys.length === rightKeys.length && leftKeys.every((key, index) => key === rightKeys[index] && jsonEqual(leftRecord[key], rightRecord[key]));
  }

  // ../../clients/typescript/src/browser-channel-stream.ts
  var BROWSER_CHANNEL_STREAM_PROTOCOL = "fleetd.channel-stream.browser.v1";
  var BROWSER_CHANNEL_STREAM_PATH = "/v1/browser/channel-stream";
  var DEFAULT_RECONNECT_DELAYS_MS = [250, 1000, 2000];
  var DEFAULT_MAX_PENDING_MESSAGES = 64;
  var DEFAULT_READY_TIMEOUT_MS = 1e4;
  var MAX_RETAINED_IDENTITIES = 4096;

  class BrowserChannelStreamError extends Error {
    constructor(code, message, options) {
      super(message, options);
      this.name = "BrowserChannelStreamError";
      this.code = code;
    }
  }

  class RetryableTransportError extends Error {
  }
  function openBrowserChannelStream(options) {
    const normalized = normalizeOptions(options);
    let credential = options.credential;
    let cursor = normalized.initialCursor;
    let status = "connecting";
    let stopRequested = false;
    let activeSocket;
    let activeGrantRequest;
    const acceptedIdentities = {
      bySequence: new Map,
      byId: new Map,
      order: []
    };
    const setStatus = (next) => {
      if (status === next)
        return;
      status = next;
      try {
        normalized.statusChanged?.(next);
      } catch {}
    };
    try {
      normalized.statusChanged?.(status);
    } catch {}
    const closed = (async () => {
      let reconnectIndex = 0;
      try {
        while (!stopRequested) {
          if (reconnectIndex > 0) {
            const delay = normalized.reconnectDelaysMs[reconnectIndex - 1];
            if (delay === undefined) {
              throw new BrowserChannelStreamError("reconnect_exhausted", "browser channel stream exhausted its bounded reconnect budget");
            }
            setStatus("reconnecting");
            await normalized.delay(delay);
            if (stopRequested)
              return;
          }
          const attemptCursor = cursor;
          const attemptController = new AbortController;
          activeGrantRequest = attemptController;
          const attemptSignal = attemptController.signal;
          let attemptTimedOut = false;
          let releaseTimeout = normalized.scheduleTimeout(() => {
            attemptTimedOut = true;
            attemptController.abort();
          }, normalized.readyTimeoutMs);
          let grant;
          let socket;
          try {
            let result;
            try {
              result = await Promise.race([
                issueGrant(normalized, credential, attemptCursor, attemptSignal).then((issuedGrant) => ({ type: "grant", issuedGrant })),
                aborted(attemptSignal).then(() => ({ type: "aborted" }))
              ]);
            } catch (error) {
              if (stopRequested)
                return;
              if (error instanceof RetryableTransportError) {
                setStatus("reconnecting");
                reconnectIndex += 1;
                continue;
              }
              throw error;
            }
            if (result.type === "aborted") {
              if (stopRequested)
                return;
              setStatus("reconnecting");
              reconnectIndex += 1;
              continue;
            }
            grant = result.issuedGrant;
            if (stopRequested)
              return;
            const socketUrl = browserSocketUrl(normalized.origin);
            try {
              socket = normalized.createWebSocket(socketUrl, BROWSER_CHANNEL_STREAM_PROTOCOL);
            } catch (cause) {
              throw new BrowserChannelStreamError("grant_linkage_mismatch", "browser channel stream could not create its fixed WebSocket", { cause });
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
                if (attemptTimedOut)
                  return;
                releaseTimeout();
                releaseTimeout = () => {};
              },
              ready: () => setStatus("live"),
              getCursor: () => cursor,
              setCursor: (accepted) => {
                cursor = accepted;
              },
              isStopped: () => stopRequested
            });
          } finally {
            releaseTimeout();
            grant = undefined;
            if (activeGrantRequest === attemptController) {
              activeGrantRequest = undefined;
            }
            if (socket && activeSocket === socket)
              activeSocket = undefined;
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
        if (stopRequested)
          setStatus("closed");
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
        if (stopRequested)
          return;
        stopRequested = true;
        credential = "";
        activeGrantRequest?.abort();
        closeSocket(activeSocket, "fleetd_client_stop");
      }
    };
  }
  function normalizeOptions(options) {
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
    let parsedOrigin;
    try {
      parsedOrigin = new URL(options.origin);
    } catch (cause) {
      throw invalidOptions("origin must be an absolute HTTP(S) origin", cause);
    }
    if (!["http:", "https:"].includes(parsedOrigin.protocol) || parsedOrigin.username || parsedOrigin.password || parsedOrigin.pathname !== "/" && parsedOrigin.pathname !== "" || parsedOrigin.search || parsedOrigin.hash) {
      throw invalidOptions("origin must contain only an HTTP(S) authority");
    }
    const initialCursor = options.after ?? 0;
    if (!isCursor(initialCursor)) {
      throw invalidOptions("after must be a non-negative safe integer");
    }
    const reconnectDelaysMs = options.reconnectDelaysMs ?? DEFAULT_RECONNECT_DELAYS_MS;
    if (!Array.isArray(reconnectDelaysMs) || reconnectDelaysMs.some((delay2) => !Number.isSafeInteger(delay2) || delay2 < 0 || delay2 > 60000)) {
      throw invalidOptions("reconnect delays must be integers between zero and 60000 milliseconds");
    }
    const maxPendingMessages = options.maxPendingMessages ?? DEFAULT_MAX_PENDING_MESSAGES;
    if (!Number.isSafeInteger(maxPendingMessages) || maxPendingMessages < 1 || maxPendingMessages > 4096) {
      throw invalidOptions("maxPendingMessages must be between 1 and 4096");
    }
    const readyTimeoutMs = options.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;
    if (!Number.isSafeInteger(readyTimeoutMs) || readyTimeoutMs < 100 || readyTimeoutMs > 15000) {
      throw invalidOptions("readyTimeoutMs must be between 100 and 15000");
    }
    const fetchImplementation = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    const socketFactory = options.createWebSocket ?? ((url, protocol) => new WebSocket(url, protocol));
    const delay = options.delay ?? ((milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)));
    const scheduleTimeout = options.scheduleTimeout ?? ((callback, milliseconds) => {
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
      scheduleTimeout
    };
  }
  function invalidOptions(message, cause) {
    return new BrowserChannelStreamError("invalid_options", message, { cause });
  }
  async function issueGrant(options, credential, after, signal) {
    const url = new URL(`/v1/channels/${encodeURIComponent(options.channelId)}/stream-grants`, options.origin).href;
    let response;
    try {
      response = await options.fetch(url, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${credential}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          after,
          protocol: BROWSER_CHANNEL_STREAM_PROTOCOL
        }),
        cache: "no-store",
        credentials: "omit",
        referrerPolicy: "no-referrer",
        signal
      });
    } catch (cause) {
      throw new RetryableTransportError("stream grant transport failed", {
        cause
      });
    }
    if (response.status !== 201) {
      throw new BrowserChannelStreamError("grant_issue_failed", `browser channel stream grant issuance returned HTTP ${response.status}`);
    }
    if (!response.headers.get("cache-control")?.toLowerCase().includes("no-store")) {
      throw new BrowserChannelStreamError("grant_linkage_mismatch", "browser channel stream grant response was not marked no-store");
    }
    let value;
    try {
      value = await response.json();
    } catch (cause) {
      throw new BrowserChannelStreamError("grant_linkage_mismatch", "browser channel stream grant response was not valid JSON", { cause });
    }
    if (!isExactRecord(value, ["expires_at_ms", "grant", "protocol", "websocket_path"])) {
      throw new BrowserChannelStreamError("grant_linkage_mismatch", "browser channel stream grant response had an unexpected shape");
    }
    if (typeof value.grant !== "string" || !/^fl_sg_[A-Za-z0-9_-]{43}$/.test(value.grant) || !Number.isSafeInteger(value.expires_at_ms) || value.protocol !== BROWSER_CHANNEL_STREAM_PROTOCOL || value.websocket_path !== BROWSER_CHANNEL_STREAM_PATH) {
      throw new BrowserChannelStreamError("grant_linkage_mismatch", "browser channel stream grant response did not link the exact protocol edge");
    }
    return value.grant;
  }
  function consumeSocketAttempt(options) {
    return new Promise((resolve, reject) => {
      const pending = [];
      let grant = options.grant;
      let opened = false;
      let ready = false;
      let accepting = false;
      let disconnected = false;
      let settled = false;
      let terminalError;
      const detach = () => {
        options.socket.onopen = null;
        options.socket.onmessage = null;
        options.socket.onerror = null;
        options.socket.onclose = null;
        options.signal.removeEventListener("abort", abortAttempt);
        grant = undefined;
      };
      const settleIfPossible = () => {
        if (settled || accepting)
          return;
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
      const fail = (error) => {
        if (settled || terminalError)
          return;
        terminalError = error;
        pending.length = 0;
        closeSocket(options.socket, "fleetd_client_error");
        settleIfPossible();
      };
      const disconnect = (reason) => {
        if (settled || terminalError || disconnected)
          return;
        disconnected = true;
        closeSocket(options.socket, reason);
        settleIfPossible();
      };
      const drain = async () => {
        if (accepting || settled || terminalError)
          return;
        accepting = true;
        try {
          while (!settled && !terminalError && pending.length > 0) {
            const message = pending.shift();
            if (!message)
              break;
            const duplicate = classifyDuplicate(message, options.getCursor(), options.acceptedIdentities);
            if (duplicate === "conflict") {
              fail(new BrowserChannelStreamError("server_protocol_error", "browser channel stream reused a stable message identity inconsistently"));
              break;
            }
            if (duplicate === "duplicate")
              continue;
            try {
              await options.accept(message);
            } catch (cause) {
              fail(new BrowserChannelStreamError("consumer_rejected", "browser channel stream consumer rejected a message", { cause }));
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
        if (settled || terminalError || disconnected)
          return;
        if (opened || options.socket.protocol !== BROWSER_CHANNEL_STREAM_PROTOCOL) {
          grant = undefined;
          fail(new BrowserChannelStreamError("socket_protocol_mismatch", "browser channel stream did not negotiate the exact protocol"));
          return;
        }
        opened = true;
        let redemptionFrame;
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
        if (settled || terminalError || disconnected)
          return;
        if (typeof event.data !== "string") {
          fail(protocolFailure("browser channel stream received a non-text frame"));
          return;
        }
        let frame;
        try {
          frame = JSON.parse(event.data);
        } catch {
          fail(protocolFailure("browser channel stream received invalid JSON"));
          return;
        }
        if (!ready) {
          if (!isReadyFrame(frame, options.channelId, options.expectedAfter)) {
            fail(protocolFailure("browser channel stream did not establish the exact grant linkage"));
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
        drain();
      };
      options.socket.onerror = () => {
        disconnect("fleetd_client_transport");
      };
      options.socket.onclose = () => {
        disconnected = true;
        if (options.isStopped())
          pending.length = 0;
        settleIfPossible();
      };
      const abortAttempt = () => {
        disconnect("fleetd_client_ready_timeout");
      };
      options.signal.addEventListener("abort", abortAttempt, { once: true });
      if (options.signal.aborted)
        abortAttempt();
    });
  }
  function isReadyFrame(value, channelId, expectedAfter) {
    return isExactRecord(value, ["after", "channel_id", "protocol", "type"]) && value.type === "ready" && value.protocol === BROWSER_CHANNEL_STREAM_PROTOCOL && value.channel_id === channelId && value.after === expectedAfter;
  }
  function decodeMessageFrame(value, channelId) {
    if (!isExactRecord(value, ["message", "type"]) || value.type !== "message") {
      return protocolFailure("browser channel stream received an unsupported frame");
    }
    const message = value.message;
    if (!isExactRecord(message, [
      "causation_id",
      "channel_id",
      "correlation_id",
      "created_at_ms",
      "id",
      "kind",
      "payload",
      "recipient_id",
      "sender_id",
      "seq"
    ]) || !Number.isSafeInteger(message.seq) || message.seq <= 0 || typeof message.id !== "string" || message.id.length === 0 || message.channel_id !== channelId || typeof message.sender_id !== "string" || message.sender_id.length === 0 || !isNullableString(message.recipient_id) || typeof message.kind !== "string" || message.kind.length === 0 || !isNullableString(message.correlation_id) || !isNullableString(message.causation_id) || !Number.isSafeInteger(message.created_at_ms)) {
      return protocolFailure("browser channel stream received an invalid immutable message envelope");
    }
    return message;
  }
  function classifyDuplicate(message, cursor, identities) {
    const sequenceForId = identities.byId.get(message.id);
    if (sequenceForId !== undefined && sequenceForId !== message.seq) {
      return "conflict";
    }
    const idForSequence = identities.bySequence.get(message.seq);
    if (idForSequence !== undefined && idForSequence !== message.id) {
      return "conflict";
    }
    if (message.seq <= cursor)
      return "duplicate";
    return "new";
  }
  function rememberIdentity(message, identities) {
    identities.bySequence.set(message.seq, message.id);
    identities.byId.set(message.id, message.seq);
    identities.order.push(message.seq);
    if (identities.order.length <= MAX_RETAINED_IDENTITIES)
      return;
    const expiredSequence = identities.order.shift();
    if (expiredSequence === undefined)
      return;
    const expiredId = identities.bySequence.get(expiredSequence);
    identities.bySequence.delete(expiredSequence);
    if (expiredId !== undefined)
      identities.byId.delete(expiredId);
  }
  function browserSocketUrl(origin) {
    const url = new URL(BROWSER_CHANNEL_STREAM_PATH, origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    return url.href;
  }
  function closeSocket(socket, reason) {
    if (!socket)
      return;
    try {
      socket.close(1000, reason);
    } catch {}
  }
  function isExactRecord(value, keys) {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return false;
    }
    const actual = Object.keys(value).sort();
    const expected = [...keys].sort();
    return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
  }
  function isNullableString(value) {
    return value === null || typeof value === "string";
  }
  function isCursor(value) {
    return Number.isSafeInteger(value) && value >= 0;
  }
  function protocolFailure(message) {
    return new BrowserChannelStreamError("server_protocol_error", message);
  }
  function aborted(signal) {
    if (signal.aborted)
      return Promise.resolve();
    return new Promise((resolve) => {
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }

  // ../../clients/typescript/src/conversation-transport.ts
  var DEFAULT_REQUEST_TIMEOUT_MS = 1e4;
  function createBrowserConversationTransport(options) {
    const origin = exactHttpOrigin(options.origin);
    const participantId = boundedIdentifier(options.participantId, "participantId");
    let operatorCredential = boundedCredential(options.operatorCredential, "operatorCredential");
    let participantCredential = boundedCredential(options.participantCredential, "participantCredential");
    let closed = false;
    const streams = new Set;
    const activeRequests = new Set;
    const requestTimeoutMs = boundedRequestTimeout(options.requestTimeoutMs);
    const fetchImplementation = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    const assertOpen = () => {
      if (closed) {
        throw new Error("conversation transport is closed");
      }
    };
    return {
      participantId,
      async listChannels() {
        assertOpen();
        return requestJson(fetchImplementation, origin, "/v1/channels", operatorCredential, { method: "GET" }, [200], requestTimeoutMs, activeRequests);
      },
      async listMembers(channelId) {
        assertOpen();
        const exactChannelId = boundedIdentifier(channelId, "channelId");
        return requestJson(fetchImplementation, origin, `/v1/channels/${encodeURIComponent(exactChannelId)}/members`, participantCredential, { method: "GET" }, [200], requestTimeoutMs, activeRequests);
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
          scheduleTimeout: options.scheduleTimeout
        });
        streams.add(stream);
        stream.closed.then(() => streams.delete(stream), () => streams.delete(stream));
        return stream;
      },
      async send(channelId, message) {
        assertOpen();
        const exactChannelId = boundedIdentifier(channelId, "channelId");
        return requestJson(fetchImplementation, origin, `/v1/channels/${encodeURIComponent(exactChannelId)}/messages`, participantCredential, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(message)
        }, [200, 201], requestTimeoutMs, activeRequests);
      },
      close() {
        if (closed)
          return;
        closed = true;
        for (const request of activeRequests)
          request.abort();
        activeRequests.clear();
        for (const stream of streams)
          stream.close();
        streams.clear();
        operatorCredential = "";
        participantCredential = "";
      }
    };
  }
  async function requestJson(fetchImplementation, origin, path, credential, init, acceptedStatuses, timeoutMs, activeRequests) {
    const controller = new AbortController;
    activeRequests.add(controller);
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImplementation(new URL(path, origin), {
        ...init,
        headers: {
          Authorization: `Bearer ${credential}`,
          ...init.headers
        },
        cache: "no-store",
        credentials: "omit",
        referrerPolicy: "no-referrer",
        signal: controller.signal
      });
      if (!acceptedStatuses.includes(response.status)) {
        throw new Error(`Fleetd request failed with HTTP ${response.status}`);
      }
      return await response.json();
    } finally {
      clearTimeout(timeout);
      activeRequests.delete(controller);
    }
  }
  function boundedRequestTimeout(value) {
    const timeout = value ?? DEFAULT_REQUEST_TIMEOUT_MS;
    if (!Number.isSafeInteger(timeout) || timeout < 100 || timeout > 60000) {
      throw new Error("requestTimeoutMs must be between 100 and 60000");
    }
    return timeout;
  }
  function exactHttpOrigin(value) {
    let parsed;
    try {
      parsed = new URL(value);
    } catch (cause) {
      throw new Error("origin must be an absolute HTTP(S) origin", { cause });
    }
    if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password || parsed.pathname !== "/" && parsed.pathname !== "" || parsed.search || parsed.hash) {
      throw new Error("origin must contain only an HTTP(S) authority");
    }
    return parsed.origin;
  }
  function boundedIdentifier(value, name) {
    if (typeof value !== "string" || value.trim().length === 0 || value.length > 256) {
      throw new Error(`${name} must contain between 1 and 256 characters`);
    }
    return value;
  }
  function boundedCredential(value, name) {
    if (typeof value !== "string" || value.length === 0 || value.length > 4096) {
      throw new Error(`${name} must contain between 1 and 4096 characters`);
    }
    return value;
  }

  // src/ui/composer.ts
  function composerAvailability(input) {
    const channelReady = input.phase === "live" && input.selectedChannelId !== null;
    const targetReady = channelReady && input.targetId !== "";
    const sending = input.sending || input.pendingSends > 0;
    return {
      textareaDisabled: !targetReady,
      targetDisabled: !channelReady,
      sendDisabled: !targetReady || input.draft.trim() === "" || sending,
      sending
    };
  }
  function isComposerSendShortcut(event) {
    return event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.altKey && !event.metaKey && !event.isComposing;
  }
  function resizeComposer(textarea) {
    textarea.style.height = "auto";
    const maximum = Number.parseFloat(getComputedStyle(textarea).maxHeight);
    const nextHeight = Number.isFinite(maximum) ? Math.min(textarea.scrollHeight, maximum) : textarea.scrollHeight;
    textarea.style.height = `${nextHeight}px`;
    textarea.style.overflowY = textarea.scrollHeight > nextHeight ? "auto" : "hidden";
  }
  function applyComposerAvailability(availability, elements) {
    elements.textarea.disabled = availability.textareaDisabled;
    elements.target.disabled = availability.targetDisabled;
    elements.send.disabled = availability.sendDisabled;
    elements.form.setAttribute("aria-busy", String(availability.sending));
    elements.send.setAttribute("aria-busy", String(availability.sending));
    elements.send.setAttribute("aria-label", availability.sending ? "Sending message" : "Send message");
    const label = elements.send.querySelector(".send-button-label") ?? sendLabel();
    const icon = elements.send.querySelector(".ui-icon-send") ?? arrowIcon();
    label.textContent = availability.sending ? "Sending…" : "Send";
    if (label.parentElement !== elements.send || icon.parentElement !== elements.send) {
      elements.send.replaceChildren(label, icon);
    }
  }
  function sendLabel() {
    const label = document.createElement("span");
    label.className = "send-button-label";
    return label;
  }
  function arrowIcon() {
    const icon = document.createElement("span");
    icon.className = "ui-icon ui-icon-send";
    icon.setAttribute("aria-hidden", "true");
    icon.textContent = "↗";
    return icon;
  }

  // src/presentation-contract.ts
  var renderers = [
    {
      matches: (message, contract) => message.kind === contract.requestKind,
      render(message) {
        const payload = record(message.payload);
        return typeof payload?.text === "string" ? { format: "text", text: payload.text } : undefined;
      }
    },
    {
      matches: (message, contract) => message.kind === contract.resultKind,
      render(message) {
        const payload = record(message.payload);
        const text = assistantText(payload?.assistant_messages);
        if (!text)
          return;
        return {
          format: "text",
          text,
          status: typeof payload?.status === "string" ? payload.status : undefined
        };
      }
    }
  ];
  function renderMessageBody(message, contract) {
    for (const renderer of renderers) {
      if (!renderer.matches(message, contract))
        continue;
      const rendered = renderer.render(message);
      if (rendered)
        return rendered;
    }
    return {
      format: "json",
      text: JSON.stringify(message.payload, null, 2)
    };
  }
  function assistantText(value) {
    if (!Array.isArray(value))
      return "";
    const fragments = [];
    for (const assistantMessage of value) {
      const content = record(assistantMessage)?.content;
      if (!Array.isArray(content))
        continue;
      for (const block of content) {
        if (typeof block === "string") {
          fragments.push(block);
          continue;
        }
        const text = record(block)?.text;
        if (typeof text === "string")
          fragments.push(text);
      }
    }
    return fragments.join("").trim();
  }
  function record(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value) ? value : undefined;
  }

  // src/ui/view-models.ts
  var PHASE_STATUS = {
    idle: {
      label: "offline",
      description: "No conversation is connected."
    },
    loading_channels: {
      label: "loading",
      description: "Finding conversations on this machine."
    },
    ready: {
      label: "ready",
      description: "Choose a conversation to begin."
    },
    connecting: {
      label: "connecting",
      description: "Opening the selected conversation."
    },
    live: {
      label: "live",
      description: "Connected and up to date."
    },
    reconnecting: {
      label: "reconnecting",
      description: "Restoring the selected conversation."
    },
    failed: {
      label: "needs attention",
      description: "This conversation needs attention before it can continue."
    },
    closed: {
      label: "closed",
      description: "The local conversation session is closed."
    }
  };
  function connectionStatusView(input) {
    if (input.sending || input.pendingSends > 0) {
      return {
        label: "sending message",
        description: "Your message is being sent.",
        busy: true
      };
    }
    const status = PHASE_STATUS[input.phase];
    return {
      ...status,
      description: input.errorMessage ?? status.description,
      busy: ["loading_channels", "connecting", "reconnecting"].includes(input.phase)
    };
  }
  function emptyConversationView(input) {
    if (input.messageCount > 0) {
      return {
        hidden: true,
        title: "",
        copy: "",
        state: "hidden"
      };
    }
    if (!input.selected) {
      if (input.phase === "loading_channels") {
        return {
          hidden: false,
          title: "Loading conversations",
          copy: "Finding the conversations available on this machine.",
          state: "loading"
        };
      }
      if (input.phase === "failed") {
        return {
          hidden: false,
          title: "Conversations unavailable",
          copy: input.errorMessage ?? "Reconnect to try channel discovery again.",
          state: "error"
        };
      }
      return {
        hidden: false,
        title: "Choose a channel",
        copy: "Choose a channel to see its saved history and new replies.",
        state: "unselected"
      };
    }
    if (input.phase === "failed" || input.phase === "closed") {
      return {
        hidden: false,
        title: "Conversation unavailable",
        copy: input.errorMessage ?? "Choose the channel again to reconnect.",
        state: "error"
      };
    }
    if (input.phase !== "live") {
      return {
        hidden: false,
        title: input.phase === "reconnecting" ? "Reconnecting conversation" : "Opening conversation",
        copy: "Restoring saved history and checking for new replies.",
        state: "loading"
      };
    }
    return {
      hidden: false,
      title: "Start the conversation",
      copy: "Send the first message to an agent.",
      state: "empty"
    };
  }
  function memberOptionView(member) {
    return {
      id: member.agent_id,
      label: member.agent_name,
      description: `${member.agent_name} (${shortId(member.agent_id)})`,
      preferred: member.delivery_mode === "inbox"
    };
  }
  function senderLabel(message, participantId, names) {
    return message.sender_id === participantId ? "you" : names.get(message.sender_id) ?? shortId(message.sender_id);
  }
  function recipientLabel(message, participantId, names) {
    if (message.recipient_id == null)
      return "channel";
    if (message.recipient_id === participantId)
      return "you";
    return names.get(message.recipient_id) ?? shortId(message.recipient_id);
  }
  function shortId(value) {
    return value.length > 12 ? `${value.slice(0, 8)}…` : value;
  }

  // src/ui/components.ts
  function renderConnectionStatus(element, snapshot, sending) {
    const status = connectionStatusView({
      phase: snapshot.phase,
      pendingSends: snapshot.pendingSends,
      errorMessage: snapshot.error?.message,
      sending
    });
    element.textContent = status.label;
    element.dataset.phase = snapshot.phase;
    element.dataset.activity = status.busy ? "busy" : "settled";
    element.title = status.description;
    element.setAttribute("aria-live", snapshot.phase === "failed" ? "assertive" : "polite");
    element.setAttribute("aria-atomic", "true");
    element.setAttribute("aria-busy", String(status.busy));
  }
  function renderChannelList(container, channels, selectedChannelId, snapshot) {
    container.setAttribute("aria-busy", String(snapshot.phase === "loading_channels"));
    if (channels.length === 0) {
      const state = document.createElement("p");
      state.className = `channel-state channel-state-${snapshot.phase}`;
      state.setAttribute("role", "status");
      state.textContent = snapshot.phase === "loading_channels" ? "Loading conversations…" : snapshot.phase === "failed" ? snapshot.error?.message ?? "Conversations unavailable" : "No conversations available";
      container.replaceChildren(state);
      return;
    }
    const existing = new Map;
    for (const button of container.querySelectorAll("button[data-channel-id]")) {
      if (button.dataset.channelId) {
        existing.set(button.dataset.channelId, button);
      }
    }
    const rows = channels.map((channel) => {
      const selected = channel.id === selectedChannelId;
      const row = existing.get(channel.id) ?? channelRow(channel, selected);
      updateChannelRow(row, channel, selected);
      return row;
    });
    reconcileChildren(container, rows);
  }
  function channelRow(channel, selected) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "channel-button";
    button.dataset.channelId = channel.id;
    const label = document.createElement("span");
    label.className = "channel-label";
    label.textContent = channel.name;
    button.append(icon("channel", "#", "channel-marker"), label);
    updateChannelRow(button, channel, selected);
    return button;
  }
  function updateChannelRow(button, channel, selected) {
    button.title = selected ? `${channel.name}, current channel` : channel.name;
    const label = button.querySelector(".channel-label");
    if (label)
      label.textContent = channel.name;
    button.setAttribute("aria-pressed", String(selected));
    if (selected) {
      button.setAttribute("aria-current", "page");
    } else {
      button.removeAttribute("aria-current");
    }
  }
  function renderChannelHeader(snapshot, elements) {
    const channel = snapshot.channels.find((candidate) => candidate.id === snapshot.selectedChannelId);
    elements.title.textContent = channel?.name ?? "Select a channel";
    elements.meta.textContent = channel ? `${snapshot.members.length} participants · ${snapshot.messages.length} messages` : snapshot.phase === "loading_channels" ? "Finding conversations…" : "Choose a conversation to begin.";
  }
  function renderMemberTargets(select, members, participantId) {
    const prior = select.value;
    const candidates = members.filter((member) => member.agent_id !== participantId).map(memberOptionView);
    if (candidates.length === 0) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No other participants";
      option.disabled = true;
      select.replaceChildren(option);
      select.value = "";
      select.title = "This channel has no other message recipient.";
      return "";
    }
    const existing = new Map(Array.from(select.options).map((option) => [option.value, option]));
    const options = candidates.map((candidate) => {
      const option = existing.get(candidate.id) ?? document.createElement("option");
      option.value = candidate.id;
      option.textContent = candidate.label;
      option.title = candidate.description;
      return option;
    });
    reconcileChildren(select, options);
    const selected = candidates.some((candidate) => candidate.id === prior) ? prior : candidates.find((candidate) => candidate.preferred)?.id ?? candidates[0]?.id ?? "";
    select.value = selected;
    select.title = candidates.find((candidate) => candidate.id === selected)?.description ?? "Message recipient";
    return selected;
  }
  function renderEmptyConversation(snapshot, elements) {
    const state = emptyConversationView({
      selected: snapshot.selectedChannelId !== null,
      phase: snapshot.phase,
      messageCount: snapshot.messages.length,
      errorMessage: snapshot.error?.message
    });
    elements.root.hidden = state.hidden;
    elements.root.dataset.state = state.state;
    elements.root.setAttribute("aria-busy", String(state.state === "loading"));
    elements.title.textContent = state.title;
    elements.copy.textContent = state.copy;
  }

  class MessageListView {
    #container;
    #channelId = null;
    constructor(container) {
      this.#container = container;
    }
    clear() {
      this.#container.replaceChildren();
      this.#channelId = null;
    }
    render(snapshot, contract) {
      const changedChannel = this.#channelId !== snapshot.selectedChannelId;
      const nearBottom = isNearBottom(this.#container);
      const anchor = !changedChannel && !nearBottom ? visibleAnchor(this.#container) : undefined;
      const names = new Map(snapshot.members.map((member) => [member.agent_id, member.agent_name]));
      const existing = new Map;
      for (const child of Array.from(this.#container.children)) {
        if (!(child instanceof HTMLElement))
          continue;
        const messageId = child.dataset.messageId;
        if (messageId)
          existing.set(messageId, child);
      }
      const nodes = snapshot.messages.map((message) => {
        const current = existing.get(message.id);
        if (current?.dataset.messageSeq === String(message.seq)) {
          updateMessageLabels(current, message, snapshot.participantId, names);
          return current;
        }
        return messageCard(message, snapshot.participantId, names, contract);
      });
      reconcileChildren(this.#container, nodes);
      this.#container.setAttribute("aria-busy", String(snapshot.phase === "connecting" || snapshot.phase === "reconnecting"));
      this.#channelId = snapshot.selectedChannelId;
      if (changedChannel || nearBottom) {
        this.#container.scrollTop = this.#container.scrollHeight;
      } else if (anchor) {
        restoreAnchor(this.#container, anchor);
      }
    }
  }
  function messageCard(message, participantId, names, contract) {
    const article = document.createElement("article");
    article.className = message.sender_id === participantId ? "message message-self" : "message";
    article.dataset.messageId = message.id;
    article.dataset.messageSeq = String(message.seq);
    const header = document.createElement("header");
    const sender = document.createElement("strong");
    sender.className = "message-sender";
    const kind = document.createElement("code");
    kind.textContent = message.kind;
    kind.title = message.kind;
    const time = document.createElement("time");
    const created = new Date(message.created_at_ms);
    time.dateTime = created.toISOString();
    time.textContent = created.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit"
    });
    time.title = created.toLocaleString();
    header.append(sender, kind, time);
    const rendered = renderMessageBody(message, contract);
    const body = document.createElement(rendered.format === "json" ? "pre" : "p");
    body.className = rendered.format === "json" ? "message-json" : "message-text";
    body.textContent = rendered.text;
    const footer = document.createElement("footer");
    if (rendered.status) {
      const status = document.createElement("span");
      status.className = "result-status";
      status.textContent = rendered.status;
      footer.append(status);
    }
    const recipient = document.createElement("span");
    recipient.className = "message-recipient";
    footer.append(recipient);
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.append(icon("envelope", "◇"), text(`Details · message ${message.seq}`));
    const envelope = document.createElement("pre");
    envelope.textContent = JSON.stringify(message, null, 2);
    details.append(summary, envelope);
    footer.append(details);
    article.append(header, body, footer);
    updateMessageLabels(article, message, participantId, names);
    return article;
  }
  function updateMessageLabels(article, message, participantId, names) {
    const sender = senderLabel(message, participantId, names);
    const recipient = recipientLabel(message, participantId, names);
    const senderElement = article.querySelector(".message-sender");
    const recipientElement = article.querySelector(".message-recipient");
    if (senderElement)
      senderElement.textContent = sender;
    if (recipientElement)
      recipientElement.textContent = `to ${recipient}`;
    article.setAttribute("aria-label", `Message from ${sender} to ${recipient}`);
  }
  function icon(name, glyph, extraClass = "") {
    const element = document.createElement("span");
    element.className = ["ui-icon", `ui-icon-${name}`, extraClass].filter(Boolean).join(" ");
    element.setAttribute("aria-hidden", "true");
    element.textContent = glyph;
    return element;
  }
  function text(value) {
    return document.createTextNode(value);
  }
  function isNearBottom(container) {
    return container.scrollHeight - container.scrollTop - container.clientHeight < 96;
  }
  function visibleAnchor(container) {
    const containerTop = container.getBoundingClientRect().top;
    for (const child of Array.from(container.children)) {
      if (!(child instanceof HTMLElement) || !child.dataset.messageId)
        continue;
      const top = child.getBoundingClientRect().top;
      if (child.getBoundingClientRect().bottom >= containerTop) {
        return { messageId: child.dataset.messageId, top };
      }
    }
    return;
  }
  function restoreAnchor(container, anchor) {
    const element = Array.from(container.children).find((child) => child instanceof HTMLElement && child.dataset.messageId === anchor.messageId);
    if (!(element instanceof HTMLElement))
      return;
    container.scrollTop += element.getBoundingClientRect().top - anchor.top;
  }
  function reconcileChildren(container, desired) {
    for (const [index, element] of desired.entries()) {
      const current = container.children.item(index);
      if (current !== element)
        container.insertBefore(element, current);
    }
    while (container.children.length > desired.length) {
      container.lastElementChild?.remove();
    }
  }

  // src/main.ts
  var elements = {
    connectPanel: required("connect-panel"),
    connectForm: required("connect-form"),
    operatorCredential: required("operator-credential"),
    participantCredential: required("participant-credential"),
    participantId: required("participant-id"),
    requestKind: required("request-kind"),
    resultKind: required("result-kind"),
    app: required("conversation-app"),
    status: required("connection-status"),
    channels: required("channel-list"),
    channelTitle: required("channel-title"),
    channelMeta: required("channel-meta"),
    messages: required("message-list"),
    empty: required("empty-conversation"),
    emptyTitle: required("empty-conversation-title"),
    emptyCopy: required("empty-conversation-copy"),
    target: required("message-target"),
    composer: required("composer"),
    composerText: required("composer-text"),
    send: required("send-message"),
    disconnect: required("disconnect")
  };
  var connectSubmit = requiredDescendant(elements.connectForm, 'button[type="submit"]');
  var connectSubmitLabel = requiredDescendant(connectSubmit, ".button-label");
  var connectSubmitIcon = requiredDescendant(connectSubmit, ".button-icon");
  var session;
  var unsubscribe;
  var contract;
  var latestSnapshot;
  var renderFrame;
  var sendInFlight = false;
  var connectInFlight = false;
  var appGeneration = 0;
  var messageList = new MessageListView(elements.messages);
  var publicApp = {
    connect,
    disconnect,
    inspect() {
      const snapshot = latestSnapshot;
      return {
        connected: session !== undefined,
        phase: snapshot?.phase ?? "disconnected",
        participant_id: snapshot?.participantId ?? null,
        selected_channel_id: snapshot?.selectedChannelId ?? null,
        cursor: snapshot?.cursor ?? 0,
        channel_count: snapshot?.channels.length ?? 0,
        member_count: snapshot?.members.length ?? 0,
        message_ids: snapshot?.messages.map((message) => message.id) ?? [],
        message_sequences: snapshot?.messages.map((message) => message.seq) ?? [],
        pending_sends: snapshot?.pendingSends ?? 0,
        error_code: snapshot?.error?.code ?? null
      };
    }
  };
  Object.defineProperty(window, "__fleetdConversation", {
    configurable: false,
    enumerable: false,
    writable: false,
    value: publicApp
  });
  document.documentElement.dataset.fleetdConversationReady = "true";
  elements.connectForm.addEventListener("submit", (event) => {
    event.preventDefault();
    if (connectInFlight)
      return;
    const profile = {
      participantId: elements.participantId.value,
      operatorCredential: elements.operatorCredential.value,
      participantCredential: elements.participantCredential.value,
      requestKind: elements.requestKind.value,
      resultKind: elements.resultKind.value
    };
    elements.operatorCredential.value = "";
    elements.participantCredential.value = "";
    connectInFlight = true;
    setConnectBusy(true);
    connect(profile).catch(() => {
      showConnectError("Could not connect with the supplied Fleetd authorities.");
    }).finally(() => {
      connectInFlight = false;
      setConnectBusy(false);
    });
  });
  elements.disconnect.addEventListener("click", disconnect);
  elements.channels.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element))
      return;
    const button = target.closest("button[data-channel-id]");
    if (!button?.dataset.channelId || !session)
      return;
    session.selectChannel(button.dataset.channelId).catch(() => {
      if (session)
        scheduleRender(session.snapshot);
    });
  });
  elements.composer.addEventListener("submit", (event) => {
    event.preventDefault();
    sendComposerMessage();
  });
  elements.composerText.addEventListener("input", () => {
    resizeComposer(elements.composerText);
    renderComposerAvailability();
  });
  elements.composerText.addEventListener("keydown", (event) => {
    if (!isComposerSendShortcut(event))
      return;
    event.preventDefault();
    elements.composer.requestSubmit();
  });
  elements.target.addEventListener("change", () => {
    renderComposerAvailability();
    const selected = elements.target.selectedOptions[0];
    if (selected?.title)
      elements.target.title = selected.title;
  });
  resizeComposer(elements.composerText);
  async function connect(profileInput) {
    disconnect();
    const generation = appGeneration;
    let profile;
    try {
      profile = { ...validateProfile(profileInput) };
    } finally {
      clearProfileCredentials(profileInput);
    }
    required("connect-error").hidden = true;
    contract = {
      requestKind: profile.requestKind,
      resultKind: profile.resultKind
    };
    const transport = (() => {
      try {
        return createBrowserConversationTransport({
          origin: window.location.origin,
          participantId: profile.participantId,
          operatorCredential: profile.operatorCredential,
          participantCredential: profile.participantCredential
        });
      } finally {
        clearProfileCredentials(profile);
      }
    })();
    const activeSession = new ConversationSession(transport);
    session = activeSession;
    unsubscribe = activeSession.subscribe(scheduleRender);
    elements.connectPanel.hidden = true;
    elements.app.hidden = false;
    try {
      await activeSession.start();
      if (generation !== appGeneration || session !== activeSession) {
        throw new Error("Fleetd conversation connection was superseded");
      }
      const channelId = profile.channelId;
      if (channelId)
        await activeSession.selectChannel(channelId);
    } catch {
      if (generation === appGeneration && session === activeSession)
        disconnect();
      throw new Error("Fleetd conversation connection failed");
    }
  }
  function disconnect() {
    appGeneration += 1;
    sendInFlight = false;
    unsubscribe?.();
    unsubscribe = undefined;
    session?.close();
    session = undefined;
    contract = undefined;
    latestSnapshot = undefined;
    if (renderFrame !== undefined)
      cancelAnimationFrame(renderFrame);
    renderFrame = undefined;
    elements.app.hidden = true;
    elements.connectPanel.hidden = false;
    messageList.clear();
    elements.channels.replaceChildren();
    elements.target.replaceChildren();
    elements.composerText.value = "";
    resizeComposer(elements.composerText);
    renderComposerAvailability();
  }
  function scheduleRender(snapshot) {
    latestSnapshot = snapshot;
    if (renderFrame !== undefined)
      return;
    renderFrame = requestAnimationFrame(() => {
      renderFrame = undefined;
      if (latestSnapshot)
        render(latestSnapshot);
    });
  }
  function render(snapshot) {
    renderConnectionStatus(elements.status, snapshot, sendInFlight);
    renderChannelList(elements.channels, snapshot.channels, snapshot.selectedChannelId, snapshot);
    renderChannelHeader(snapshot, {
      title: elements.channelTitle,
      meta: elements.channelMeta
    });
    renderMemberTargets(elements.target, snapshot.members, snapshot.participantId);
    messageList.render(snapshot, requiredContract());
    renderEmptyConversation(snapshot, {
      root: elements.empty,
      title: elements.emptyTitle,
      copy: elements.emptyCopy
    });
    renderComposerAvailability();
  }
  async function sendComposerMessage() {
    const activeSession = session;
    if (!activeSession || !contract || sendInFlight)
      return;
    const draft = elements.composerText.value;
    const text2 = draft.trim();
    const recipientId = elements.target.value;
    if (!text2 || !recipientId)
      return;
    const turnId = crypto.randomUUID();
    const generation = appGeneration;
    sendInFlight = true;
    renderConnectionStatus(elements.status, activeSession.snapshot, true);
    renderComposerAvailability();
    try {
      await activeSession.send({
        idempotency_key: `fleetd-conversation/${turnId}`,
        recipient_id: recipientId,
        kind: contract.requestKind,
        payload: { text: text2 },
        correlation_id: turnId,
        causation_id: null
      });
      if (generation === appGeneration && session === activeSession && elements.composerText.value === draft) {
        elements.composerText.value = "";
        resizeComposer(elements.composerText);
      }
    } catch {
      if (generation === appGeneration && session === activeSession) {
        elements.composerText.focus();
      }
    } finally {
      if (generation === appGeneration && session === activeSession) {
        sendInFlight = false;
        scheduleRender(activeSession.snapshot);
        renderComposerAvailability();
        elements.composerText.focus();
      }
    }
  }
  function renderComposerAvailability() {
    const snapshot = latestSnapshot;
    const availability = composerAvailability({
      phase: snapshot?.phase ?? "closed",
      selectedChannelId: snapshot?.selectedChannelId ?? null,
      targetId: elements.target.value,
      draft: elements.composerText.value,
      pendingSends: snapshot?.pendingSends ?? 0,
      sending: sendInFlight
    });
    applyComposerAvailability(availability, {
      form: elements.composer,
      textarea: elements.composerText,
      target: elements.target,
      send: elements.send
    });
  }
  function validateProfile(value) {
    if (!value || typeof value !== "object")
      throw new Error("profile required");
    boundedProfileField(value.participantId, "participantId", 256);
    boundedProfileField(value.operatorCredential, "operatorCredential", 4096);
    boundedProfileField(value.participantCredential, "participantCredential", 4096);
    boundedProfileField(value.requestKind, "requestKind", 256);
    boundedProfileField(value.resultKind, "resultKind", 256);
    if (value.channelId !== undefined && (typeof value.channelId !== "string" || value.channelId.trim().length === 0 || value.channelId.length > 256)) {
      throw new Error("invalid conversation profile field: channelId");
    }
    return value;
  }
  function boundedProfileField(value, name, maximumLength) {
    if (typeof value !== "string" || value.trim().length === 0 || value.length > maximumLength) {
      throw new Error(`invalid conversation profile field: ${name}`);
    }
  }
  function clearProfileCredentials(profile) {
    try {
      profile.operatorCredential = "";
      profile.participantCredential = "";
    } catch {}
  }
  function showConnectError(message) {
    const output = required("connect-error");
    output.textContent = message;
    output.hidden = false;
  }
  function setConnectBusy(busy) {
    elements.connectForm.setAttribute("aria-busy", String(busy));
    connectSubmit.disabled = busy;
    connectSubmitLabel.textContent = busy ? "Connecting…" : "Open conversations";
    connectSubmitIcon.textContent = busy ? "…" : "→";
  }
  function requiredContract() {
    if (!contract)
      throw new Error("conversation presentation is disconnected");
    return contract;
  }
  function required(id) {
    const element = document.getElementById(id);
    if (!element)
      throw new Error(`missing conversation element: ${id}`);
    return element;
  }
  function requiredDescendant(parent, selector) {
    const element = parent.querySelector(selector);
    if (!element)
      throw new Error(`missing conversation element: ${selector}`);
    return element;
  }
})();
