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
    #attention = [];
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
        attention: this.#attention,
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
        const [channels, attention] = await Promise.all([
          this.#transport.listChannels(),
          this.#transport.listAttention()
        ]);
        this.#channels = [...channels];
        this.#attention = checkedAttention(attention);
      } catch {
        this.#fail("channel_discovery_failed", "Fleetd conversation discovery failed");
        throw new Error("Fleetd conversation discovery failed");
      }
      if (this.#closed)
        return;
      this.#phase = "ready";
      this.#publish();
    }
    async refreshChannels() {
      this.#assertOpen();
      try {
        const [channels, attention] = await Promise.all([
          this.#transport.listChannels(),
          this.#transport.listAttention()
        ]);
        this.#channels = [...channels];
        this.#attention = checkedAttention(attention);
        this.#error = null;
        this.#publish();
      } catch {
        this.#fail("channel_discovery_failed", "Fleetd channel discovery failed");
        throw new Error("Fleetd channel discovery failed");
      }
    }
    async refreshAttention() {
      this.#assertOpen();
      try {
        this.#attention = checkedAttention(await this.#transport.listAttention());
        this.#publish();
      } catch {
        throw new Error("Fleetd conversation attention refresh failed");
      }
    }
    async markRead(channelId, throughSeq) {
      this.#assertOpen();
      if (!Number.isSafeInteger(throughSeq) || throughSeq < 0) {
        throw this.#invalidState("read cursor must be a non-negative safe integer");
      }
      const updated = checkedAttention([
        await this.#transport.advanceRead(channelId, throughSeq)
      ])[0];
      if (updated.channel_id !== channelId || updated.read_through_seq < throughSeq) {
        throw this.#invalidState("Fleetd returned attention outside the acknowledged participant lane");
      }
      this.#attention = [
        ...this.#attention.filter((attention) => attention.channel_id !== updated.channel_id),
        updated
      ];
      this.#publish();
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
    clearSelection() {
      this.#assertOpen();
      this.#cancelSelection?.();
      this.#cancelSelection = undefined;
      this.#selectionGeneration += 1;
      const selected = this.#selectedLane();
      this.#selectedChannelId = null;
      selected?.stream?.close();
      this.#phase = "ready";
      this.#error = null;
      this.#publish();
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
  function checkedAttention(values) {
    const seen = new Set;
    for (const value of values) {
      const latest = value.latest_message_seq;
      const firstUnread = value.first_unread_seq;
      const firstAddressed = value.first_addressed_unread_seq;
      if (typeof value.channel_id !== "string" || value.channel_id.length === 0 || seen.has(value.channel_id) || !Number.isSafeInteger(value.read_through_seq) || value.read_through_seq < 0 || latest !== null && (typeof latest !== "number" || !Number.isSafeInteger(latest) || latest < value.read_through_seq) || !Number.isSafeInteger(value.unread_count) || value.unread_count < 0 || !Number.isSafeInteger(value.addressed_unread_count) || value.addressed_unread_count < 0 || value.addressed_unread_count > value.unread_count || value.unread_count === 0 !== (firstUnread === null) || value.addressed_unread_count === 0 !== (firstAddressed === null) || firstUnread !== null && (typeof firstUnread !== "number" || !Number.isSafeInteger(firstUnread) || firstUnread <= value.read_through_seq || latest === null || typeof latest !== "number" || firstUnread > latest) || firstAddressed !== null && (typeof firstAddressed !== "number" || !Number.isSafeInteger(firstAddressed) || firstAddressed <= value.read_through_seq || latest === null || typeof latest !== "number" || firstAddressed > latest)) {
        throw new Error("Fleetd returned invalid conversation attention");
      }
      seen.add(value.channel_id);
    }
    return [...values];
  }

  // ../../clients/typescript/src/channel-stream-core.ts
  var DEFAULT_CHANNEL_STREAM_RECONNECT_DELAYS_MS = [
    250,
    1000,
    2000
  ];
  var DEFAULT_CHANNEL_STREAM_MAX_PENDING_MESSAGES = 64;
  var DEFAULT_CHANNEL_STREAM_READY_TIMEOUT_MS = 1e4;
  var MAX_RETAINED_IDENTITIES = 4096;
  function createAcceptedIdentityIndex() {
    return {
      bySequence: new Map,
      byId: new Map,
      order: []
    };
  }

  class MessageAcceptanceQueue {
    #options;
    #pending = [];
    #accepting = false;
    #terminal = false;
    constructor(options) {
      this.#options = options;
    }
    get busy() {
      return this.#accepting;
    }
    get pending() {
      return this.#pending.length;
    }
    offer(message) {
      if (this.#terminal)
        return true;
      if (this.#pending.length >= this.#options.maxPendingMessages)
        return false;
      this.#pending.push(message);
      this.#drain();
      return true;
    }
    clear() {
      this.#pending.length = 0;
    }
    stop() {
      this.#terminal = true;
      this.clear();
    }
    async#drain() {
      if (this.#accepting || this.#terminal)
        return;
      this.#accepting = true;
      try {
        while (!this.#terminal && this.#pending.length > 0) {
          const message = this.#pending.shift();
          if (!message)
            break;
          const duplicate = classifyDuplicate(message, this.#options.getCursor(), this.#options.acceptedIdentities);
          if (duplicate === "conflict") {
            this.#terminal = true;
            this.clear();
            this.#options.failed({ type: "identity_conflict" });
            break;
          }
          if (duplicate === "duplicate")
            continue;
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
  function decodeChannelMessage(value, channelId) {
    if (!isExactRecord(value, [
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
    ]) || !Number.isSafeInteger(value.seq) || value.seq <= 0 || typeof value.id !== "string" || value.id.length === 0 || value.channel_id !== channelId || typeof value.sender_id !== "string" || value.sender_id.length === 0 || !isNullableString(value.recipient_id) || typeof value.kind !== "string" || value.kind.length === 0 || !isNullableString(value.correlation_id) || !isNullableString(value.causation_id) || !Number.isSafeInteger(value.created_at_ms)) {
      return { error: "invalid immutable message envelope" };
    }
    return value;
  }
  function isDecodeFailure(value) {
    return "error" in value;
  }
  function isExactRecord(value, keys) {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return false;
    }
    const actual = Object.keys(value).sort();
    const expected = [...keys].sort();
    return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
  }
  function isChannelStreamCursor(value) {
    return Number.isSafeInteger(value) && value >= 0;
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
  function isNullableString(value) {
    return value === null || typeof value === "string";
  }

  // ../../clients/typescript/src/browser-channel-stream.ts
  var BROWSER_CHANNEL_STREAM_PROTOCOL = "fleetd.channel-stream.browser.v1";
  var BROWSER_CHANNEL_STREAM_PATH = "/v1/browser/channel-stream";

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
    const acceptedIdentities = createAcceptedIdentityIndex();
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
    const reconnectDelaysMs = options.reconnectDelaysMs ?? DEFAULT_CHANNEL_STREAM_RECONNECT_DELAYS_MS;
    if (!Array.isArray(reconnectDelaysMs) || reconnectDelaysMs.some((delay2) => !Number.isSafeInteger(delay2) || delay2 < 0 || delay2 > 60000)) {
      throw invalidOptions("reconnect delays must be integers between zero and 60000 milliseconds");
    }
    const maxPendingMessages = options.maxPendingMessages ?? DEFAULT_CHANNEL_STREAM_MAX_PENDING_MESSAGES;
    if (!Number.isSafeInteger(maxPendingMessages) || maxPendingMessages < 1 || maxPendingMessages > 4096) {
      throw invalidOptions("maxPendingMessages must be between 1 and 4096");
    }
    const readyTimeoutMs = options.readyTimeoutMs ?? DEFAULT_CHANNEL_STREAM_READY_TIMEOUT_MS;
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
      let grant = options.grant;
      let opened = false;
      let ready = false;
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
      let lane;
      const settleIfPossible = () => {
        if (settled || lane.busy)
          return;
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
      const fail = (error) => {
        if (settled || terminalError)
          return;
        terminalError = error;
        lane.stop();
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
      lane = new MessageAcceptanceQueue({
        accept: options.accept,
        maxPendingMessages: options.maxPendingMessages,
        acceptedIdentities: options.acceptedIdentities,
        getCursor: options.getCursor,
        setCursor: options.setCursor,
        failed(failure) {
          if (failure.type === "identity_conflict") {
            fail(new BrowserChannelStreamError("server_protocol_error", "browser channel stream reused a stable message identity inconsistently"));
          } else {
            fail(new BrowserChannelStreamError("consumer_rejected", "browser channel stream consumer rejected a message", { cause: failure.cause }));
          }
        },
        idle: settleIfPossible
      });
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
        if (!lane.offer(message)) {
          lane.clear();
          disconnect("fleetd_client_backpressure");
          return;
        }
      };
      options.socket.onerror = () => {
        disconnect("fleetd_client_transport");
      };
      options.socket.onclose = () => {
        disconnected = true;
        if (options.isStopped())
          lane.clear();
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
    const message = decodeChannelMessage(value.message, channelId);
    if (isDecodeFailure(message)) {
      return protocolFailure("browser channel stream received an invalid immutable message envelope");
    }
    return message;
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
  function isCursor(value) {
    return isChannelStreamCursor(value);
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

  // ../../clients/typescript/src/client-options.ts
  var DEFAULT_REQUEST_TIMEOUT_MS = 1e4;
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

  // ../../clients/typescript/src/conversation-transport.ts
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
      async listAttention() {
        assertOpen();
        return requestJson(fetchImplementation, origin, "/v1/conversations/attention", participantCredential, { method: "GET" }, [200], requestTimeoutMs, activeRequests);
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
      async advanceRead(channelId, throughSeq) {
        assertOpen();
        const exactChannelId = boundedIdentifier(channelId, "channelId");
        if (!Number.isSafeInteger(throughSeq) || throughSeq < 0) {
          throw new Error("throughSeq must be a non-negative safe integer");
        }
        return requestJson(fetchImplementation, origin, `/v1/channels/${encodeURIComponent(exactChannelId)}/read-cursor`, participantCredential, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ through_seq: throughSeq })
        }, [200], requestTimeoutMs, activeRequests);
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

  // ../../clients/typescript/src/generated/core/bodySerializer.gen.ts
  var jsonBodySerializer = {
    bodySerializer: (body) => JSON.stringify(body, (_key, value) => typeof value === "bigint" ? value.toString() : value)
  };
  // ../../clients/typescript/src/generated/core/params.gen.ts
  var extraPrefixesMap = {
    $body_: "body",
    $headers_: "headers",
    $path_: "path",
    $query_: "query"
  };
  var extraPrefixes = Object.entries(extraPrefixesMap);
  // ../../clients/typescript/src/generated/core/serverSentEvents.gen.ts
  function createSseClient({
    onRequest,
    onSseError,
    onSseEvent,
    responseTransformer,
    responseValidator,
    sseDefaultRetryDelay,
    sseMaxRetryAttempts,
    sseMaxRetryDelay,
    sseSleepFn,
    url,
    ...options
  }) {
    let lastEventId;
    const sleep = sseSleepFn ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    const createStream = async function* () {
      let retryDelay = sseDefaultRetryDelay ?? 3000;
      let attempt = 0;
      const signal = options.signal ?? new AbortController().signal;
      while (true) {
        if (signal.aborted)
          break;
        attempt++;
        const headers = options.headers instanceof Headers ? options.headers : new Headers(options.headers);
        if (lastEventId !== undefined) {
          headers.set("Last-Event-ID", lastEventId);
        }
        try {
          const requestInit = {
            redirect: "follow",
            ...options,
            body: options.serializedBody,
            headers,
            signal
          };
          let request = new Request(url, requestInit);
          if (onRequest) {
            request = await onRequest(url, requestInit);
          }
          const _fetch = options.fetch ?? globalThis.fetch;
          const response = await _fetch(request);
          if (!response.ok)
            throw new Error(`SSE failed: ${response.status} ${response.statusText}`);
          if (!response.body)
            throw new Error("No body in SSE response");
          const reader = response.body.pipeThrough(new TextDecoderStream).getReader();
          let buffer = "";
          const abortHandler = () => {
            try {
              reader.cancel();
            } catch {}
          };
          signal.addEventListener("abort", abortHandler);
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done)
                break;
              buffer += value;
              buffer = buffer.replace(/\r\n?/g, `
`);
              const chunks = buffer.split(`

`);
              buffer = chunks.pop() ?? "";
              for (const chunk of chunks) {
                const lines = chunk.split(`
`);
                const dataLines = [];
                let eventName;
                for (const line of lines) {
                  if (line.startsWith("data:")) {
                    dataLines.push(line.replace(/^data:\s*/, ""));
                  } else if (line.startsWith("event:")) {
                    eventName = line.replace(/^event:\s*/, "");
                  } else if (line.startsWith("id:")) {
                    lastEventId = line.replace(/^id:\s*/, "");
                  } else if (line.startsWith("retry:")) {
                    const parsed = Number.parseInt(line.replace(/^retry:\s*/, ""), 10);
                    if (!Number.isNaN(parsed)) {
                      retryDelay = parsed;
                    }
                  }
                }
                let data;
                let parsedJson = false;
                if (dataLines.length) {
                  const rawData = dataLines.join(`
`);
                  try {
                    data = JSON.parse(rawData);
                    parsedJson = true;
                  } catch {
                    data = rawData;
                  }
                }
                if (parsedJson) {
                  if (responseValidator) {
                    await responseValidator(data);
                  }
                  if (responseTransformer) {
                    data = await responseTransformer(data);
                  }
                }
                onSseEvent?.({
                  data,
                  event: eventName,
                  id: lastEventId,
                  retry: retryDelay
                });
                if (dataLines.length) {
                  yield data;
                }
              }
            }
          } finally {
            signal.removeEventListener("abort", abortHandler);
            reader.releaseLock();
          }
          break;
        } catch (error) {
          onSseError?.(error);
          if (sseMaxRetryAttempts !== undefined && attempt >= sseMaxRetryAttempts) {
            break;
          }
          const backoff = Math.min(retryDelay * 2 ** (attempt - 1), sseMaxRetryDelay ?? 30000);
          await sleep(backoff);
        }
      }
    };
    const stream = createStream();
    return { stream };
  }

  // ../../clients/typescript/src/generated/core/pathSerializer.gen.ts
  var separatorArrayExplode = (style) => {
    switch (style) {
      case "label":
        return ".";
      case "matrix":
        return ";";
      case "simple":
        return ",";
      default:
        return "&";
    }
  };
  var separatorArrayNoExplode = (style) => {
    switch (style) {
      case "form":
        return ",";
      case "pipeDelimited":
        return "|";
      case "spaceDelimited":
        return "%20";
      default:
        return ",";
    }
  };
  var separatorObjectExplode = (style) => {
    switch (style) {
      case "label":
        return ".";
      case "matrix":
        return ";";
      case "simple":
        return ",";
      default:
        return "&";
    }
  };
  var serializeArrayParam = ({
    allowReserved,
    explode,
    name,
    style,
    value
  }) => {
    if (!explode) {
      const joinedValues2 = (allowReserved ? value : value.map((v) => encodeURIComponent(v))).join(separatorArrayNoExplode(style));
      switch (style) {
        case "label":
          return `.${joinedValues2}`;
        case "matrix":
          return `;${name}=${joinedValues2}`;
        case "simple":
          return joinedValues2;
        default:
          return `${name}=${joinedValues2}`;
      }
    }
    const separator = separatorArrayExplode(style);
    const joinedValues = value.map((v) => {
      if (style === "label" || style === "simple") {
        return allowReserved ? v : encodeURIComponent(v);
      }
      return serializePrimitiveParam({
        allowReserved,
        name,
        value: v
      });
    }).join(separator);
    return style === "label" || style === "matrix" ? separator + joinedValues : joinedValues;
  };
  var serializePrimitiveParam = ({
    allowReserved,
    name,
    value
  }) => {
    if (value === undefined || value === null) {
      return "";
    }
    if (typeof value === "object") {
      throw new Error("Deeply-nested arrays/objects aren’t supported. Provide your own `querySerializer()` to handle these.");
    }
    return `${name}=${allowReserved ? value : encodeURIComponent(value)}`;
  };
  var serializeObjectParam = ({
    allowReserved,
    explode,
    name,
    style,
    value,
    valueOnly
  }) => {
    if (value instanceof Date) {
      return valueOnly ? value.toISOString() : `${name}=${value.toISOString()}`;
    }
    if (style !== "deepObject" && !explode) {
      let values = [];
      Object.entries(value).forEach(([key, v]) => {
        values = [...values, key, allowReserved ? v : encodeURIComponent(v)];
      });
      const joinedValues2 = values.join(",");
      switch (style) {
        case "form":
          return `${name}=${joinedValues2}`;
        case "label":
          return `.${joinedValues2}`;
        case "matrix":
          return `;${name}=${joinedValues2}`;
        default:
          return joinedValues2;
      }
    }
    const separator = separatorObjectExplode(style);
    const joinedValues = Object.entries(value).map(([key, v]) => serializePrimitiveParam({
      allowReserved,
      name: style === "deepObject" ? `${name}[${key}]` : key,
      value: v
    })).join(separator);
    return style === "label" || style === "matrix" ? separator + joinedValues : joinedValues;
  };

  // ../../clients/typescript/src/generated/core/utils.gen.ts
  var PATH_PARAM_RE = /\{[^{}]+\}/g;
  var defaultPathSerializer = ({ path, url: _url }) => {
    let url = _url;
    const matches = _url.match(PATH_PARAM_RE);
    if (matches) {
      for (const match of matches) {
        let explode = false;
        let name = match.substring(1, match.length - 1);
        let style = "simple";
        if (name.endsWith("*")) {
          explode = true;
          name = name.substring(0, name.length - 1);
        }
        if (name.startsWith(".")) {
          name = name.substring(1);
          style = "label";
        } else if (name.startsWith(";")) {
          name = name.substring(1);
          style = "matrix";
        }
        const value = path[name];
        if (value === undefined || value === null) {
          continue;
        }
        if (Array.isArray(value)) {
          url = url.replace(match, serializeArrayParam({ explode, name, style, value }));
          continue;
        }
        if (typeof value === "object") {
          url = url.replace(match, serializeObjectParam({
            explode,
            name,
            style,
            value,
            valueOnly: true
          }));
          continue;
        }
        if (style === "matrix") {
          url = url.replace(match, `;${serializePrimitiveParam({
            name,
            value
          })}`);
          continue;
        }
        const replaceValue = encodeURIComponent(style === "label" ? `.${value}` : value);
        url = url.replace(match, replaceValue);
      }
    }
    return url;
  };
  var getUrl = ({
    baseUrl,
    path,
    query,
    querySerializer,
    url: _url
  }) => {
    const pathUrl = _url.startsWith("/") ? _url : `/${_url}`;
    let url = (baseUrl ?? "") + pathUrl;
    if (path) {
      url = defaultPathSerializer({ path, url });
    }
    let search = query ? querySerializer(query) : "";
    if (search.startsWith("?")) {
      search = search.substring(1);
    }
    if (search) {
      url += `?${search}`;
    }
    return url;
  };
  function getValidRequestBody(options) {
    const hasBody = options.body !== undefined;
    const isSerializedBody = hasBody && options.bodySerializer;
    if (isSerializedBody) {
      if ("serializedBody" in options) {
        const hasSerializedBody = options.serializedBody !== undefined && options.serializedBody !== "";
        return hasSerializedBody ? options.serializedBody : null;
      }
      return options.body !== "" ? options.body : null;
    }
    if (hasBody) {
      return options.body;
    }
    return;
  }

  // ../../clients/typescript/src/generated/core/auth.gen.ts
  var getAuthToken = async (auth, callback) => {
    const token = typeof callback === "function" ? await callback(auth) : callback;
    if (!token) {
      return;
    }
    if (auth.scheme === "bearer") {
      return `Bearer ${token}`;
    }
    if (auth.scheme === "basic") {
      return `Basic ${btoa(token)}`;
    }
    return token;
  };

  // ../../clients/typescript/src/generated/client/utils.gen.ts
  var createQuerySerializer = ({
    parameters = {},
    ...args
  } = {}) => {
    const querySerializer = (queryParams) => {
      const search = [];
      if (queryParams && typeof queryParams === "object") {
        for (const name in queryParams) {
          const value = queryParams[name];
          if (value === undefined || value === null) {
            continue;
          }
          const options = parameters[name] || args;
          if (Array.isArray(value)) {
            const serializedArray = serializeArrayParam({
              allowReserved: options.allowReserved,
              explode: true,
              name,
              style: "form",
              value,
              ...options.array
            });
            if (serializedArray)
              search.push(serializedArray);
          } else if (typeof value === "object") {
            const serializedObject = serializeObjectParam({
              allowReserved: options.allowReserved,
              explode: true,
              name,
              style: "deepObject",
              value,
              ...options.object
            });
            if (serializedObject)
              search.push(serializedObject);
          } else {
            const serializedPrimitive = serializePrimitiveParam({
              allowReserved: options.allowReserved,
              name,
              value
            });
            if (serializedPrimitive)
              search.push(serializedPrimitive);
          }
        }
      }
      return search.join("&");
    };
    return querySerializer;
  };
  var getParseAs = (contentType) => {
    if (!contentType) {
      return "stream";
    }
    const cleanContent = contentType.split(";")[0]?.trim();
    if (!cleanContent) {
      return;
    }
    if (cleanContent.startsWith("application/json") || cleanContent.endsWith("+json")) {
      return "json";
    }
    if (cleanContent === "multipart/form-data") {
      return "formData";
    }
    if (["application/", "audio/", "image/", "video/"].some((type) => cleanContent.startsWith(type))) {
      return "blob";
    }
    if (cleanContent.startsWith("text/")) {
      return "text";
    }
    return;
  };
  var checkForExistence = (options, name) => {
    if (!name) {
      return false;
    }
    if (options.headers.has(name) || options.query?.[name] || options.headers.get("Cookie")?.includes(`${name}=`)) {
      return true;
    }
    return false;
  };
  async function setAuthParams(options) {
    for (const auth of options.security ?? []) {
      if (checkForExistence(options, auth.name)) {
        continue;
      }
      const token = await getAuthToken(auth, options.auth);
      if (!token) {
        continue;
      }
      const name = auth.name ?? "Authorization";
      switch (auth.in) {
        case "query":
          if (!options.query) {
            options.query = {};
          }
          options.query[name] = token;
          break;
        case "cookie":
          options.headers.append("Cookie", `${name}=${token}`);
          break;
        case "header":
        default:
          options.headers.set(name, token);
          break;
      }
    }
  }
  var buildUrl = (options) => getUrl({
    baseUrl: options.baseUrl,
    path: options.path,
    query: options.query,
    querySerializer: typeof options.querySerializer === "function" ? options.querySerializer : createQuerySerializer(options.querySerializer),
    url: options.url
  });
  var mergeConfigs = (a, b) => {
    const config = { ...a, ...b };
    if (config.baseUrl?.endsWith("/")) {
      config.baseUrl = config.baseUrl.substring(0, config.baseUrl.length - 1);
    }
    config.headers = mergeHeaders(a.headers, b.headers);
    return config;
  };
  var headersEntries = (headers) => {
    const entries = [];
    headers.forEach((value, key) => {
      entries.push([key, value]);
    });
    return entries;
  };
  var mergeHeaders = (...headers) => {
    const mergedHeaders = new Headers;
    for (const header of headers) {
      if (!header) {
        continue;
      }
      const iterator = header instanceof Headers ? headersEntries(header) : Object.entries(header);
      for (const [key, value] of iterator) {
        if (value === null) {
          mergedHeaders.delete(key);
        } else if (Array.isArray(value)) {
          for (const v of value) {
            mergedHeaders.append(key, v);
          }
        } else if (value !== undefined) {
          mergedHeaders.set(key, typeof value === "object" ? JSON.stringify(value) : value);
        }
      }
    }
    return mergedHeaders;
  };

  class Interceptors {
    fns = [];
    clear() {
      this.fns = [];
    }
    eject(id) {
      const index = this.getInterceptorIndex(id);
      if (this.fns[index]) {
        this.fns[index] = null;
      }
    }
    exists(id) {
      const index = this.getInterceptorIndex(id);
      return Boolean(this.fns[index]);
    }
    getInterceptorIndex(id) {
      if (typeof id === "number") {
        return this.fns[id] ? id : -1;
      }
      return this.fns.indexOf(id);
    }
    update(id, fn) {
      const index = this.getInterceptorIndex(id);
      if (this.fns[index]) {
        this.fns[index] = fn;
        return id;
      }
      return false;
    }
    use(fn) {
      this.fns.push(fn);
      return this.fns.length - 1;
    }
  }
  var createInterceptors = () => ({
    error: new Interceptors,
    request: new Interceptors,
    response: new Interceptors
  });
  var defaultQuerySerializer = createQuerySerializer({
    allowReserved: false,
    array: {
      explode: true,
      style: "form"
    },
    object: {
      explode: true,
      style: "deepObject"
    }
  });
  var defaultHeaders = {
    "Content-Type": "application/json"
  };
  var createConfig = (override = {}) => ({
    ...jsonBodySerializer,
    headers: defaultHeaders,
    parseAs: "auto",
    querySerializer: defaultQuerySerializer,
    ...override
  });

  // ../../clients/typescript/src/generated/client/client.gen.ts
  var createClient = (config = {}) => {
    let _config = mergeConfigs(createConfig(), config);
    const getConfig = () => ({ ..._config });
    const setConfig = (config2) => {
      _config = mergeConfigs(_config, config2);
      return getConfig();
    };
    const interceptors = createInterceptors();
    const beforeRequest = async (options) => {
      const opts = {
        ..._config,
        ...options,
        fetch: options.fetch ?? _config.fetch ?? globalThis.fetch,
        headers: mergeHeaders(_config.headers, options.headers),
        serializedBody: undefined
      };
      if (opts.security) {
        await setAuthParams(opts);
      }
      if (opts.requestValidator) {
        await opts.requestValidator(opts);
      }
      if (opts.body !== undefined && opts.bodySerializer) {
        opts.serializedBody = opts.bodySerializer(opts.body);
      }
      if (opts.body === undefined || opts.serializedBody === "") {
        opts.headers.delete("Content-Type");
      }
      const resolvedOpts = opts;
      const url = buildUrl(resolvedOpts);
      return { opts: resolvedOpts, url };
    };
    const request = async (options) => {
      const throwOnError = options.throwOnError ?? _config.throwOnError;
      const responseStyle = options.responseStyle ?? _config.responseStyle;
      let request2;
      let response;
      try {
        const { opts, url } = await beforeRequest(options);
        const requestInit = {
          redirect: "follow",
          ...opts,
          body: getValidRequestBody(opts)
        };
        request2 = new Request(url, requestInit);
        for (const fn of interceptors.request.fns) {
          if (fn) {
            request2 = await fn(request2, opts);
          }
        }
        const _fetch = opts.fetch;
        response = await _fetch(request2);
        for (const fn of interceptors.response.fns) {
          if (fn) {
            response = await fn(response, request2, opts);
          }
        }
        const result = {
          request: request2,
          response
        };
        if (response.ok) {
          const parseAs = (opts.parseAs === "auto" ? getParseAs(response.headers.get("Content-Type")) : opts.parseAs) ?? "json";
          if (response.status === 204 || response.headers.get("Content-Length") === "0") {
            let emptyData;
            switch (parseAs) {
              case "arrayBuffer":
              case "blob":
              case "text":
                emptyData = await response[parseAs]();
                break;
              case "formData":
                emptyData = new FormData;
                break;
              case "stream":
                emptyData = response.body;
                break;
              case "json":
              default:
                emptyData = {};
                break;
            }
            return opts.responseStyle === "data" ? emptyData : {
              data: emptyData,
              ...result
            };
          }
          let data;
          switch (parseAs) {
            case "arrayBuffer":
            case "blob":
            case "formData":
            case "text":
              data = await response[parseAs]();
              break;
            case "json": {
              const text = await response.text();
              data = text ? JSON.parse(text) : {};
              break;
            }
            case "stream":
              return opts.responseStyle === "data" ? response.body : {
                data: response.body,
                ...result
              };
          }
          if (parseAs === "json") {
            if (opts.responseValidator) {
              await opts.responseValidator(data);
            }
            if (opts.responseTransformer) {
              data = await opts.responseTransformer(data);
            }
          }
          return opts.responseStyle === "data" ? data : {
            data,
            ...result
          };
        }
        const textError = await response.text();
        let jsonError;
        try {
          jsonError = JSON.parse(textError);
        } catch {}
        throw jsonError ?? textError;
      } catch (error) {
        let finalError = error;
        for (const fn of interceptors.error.fns) {
          if (fn) {
            finalError = await fn(finalError, response, request2, options);
          }
        }
        finalError = finalError || {};
        if (throwOnError) {
          throw finalError;
        }
        return responseStyle === "data" ? undefined : {
          error: finalError,
          request: request2,
          response
        };
      }
    };
    const makeMethodFn = (method) => (options) => request({ ...options, method });
    const makeSseFn = (method) => async (options) => {
      const { opts, url } = await beforeRequest(options);
      return createSseClient({
        ...opts,
        body: opts.body,
        method,
        onRequest: async (url2, init) => {
          let request2 = new Request(url2, init);
          for (const fn of interceptors.request.fns) {
            if (fn) {
              request2 = await fn(request2, opts);
            }
          }
          return request2;
        },
        serializedBody: getValidRequestBody(opts),
        url
      });
    };
    const _buildUrl = (options) => buildUrl({ ..._config, ...options });
    return {
      buildUrl: _buildUrl,
      connect: makeMethodFn("CONNECT"),
      delete: makeMethodFn("DELETE"),
      get: makeMethodFn("GET"),
      getConfig,
      head: makeMethodFn("HEAD"),
      interceptors,
      options: makeMethodFn("OPTIONS"),
      patch: makeMethodFn("PATCH"),
      post: makeMethodFn("POST"),
      put: makeMethodFn("PUT"),
      request,
      setConfig,
      sse: {
        connect: makeSseFn("CONNECT"),
        delete: makeSseFn("DELETE"),
        get: makeSseFn("GET"),
        head: makeSseFn("HEAD"),
        options: makeSseFn("OPTIONS"),
        patch: makeSseFn("PATCH"),
        post: makeSseFn("POST"),
        put: makeSseFn("PUT"),
        trace: makeSseFn("TRACE")
      },
      trace: makeMethodFn("TRACE")
    };
  };
  // ../../clients/typescript/src/generated/client.gen.ts
  var client = createClient(createConfig());

  // ../../clients/typescript/src/generated/sdk.gen.ts
  var listAgentSeatConfigurations = (options) => (options?.client ?? client).get({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/agent-seat-configurations",
    ...options
  });
  var listAgentSeats = (options) => (options?.client ?? client).get({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/agent-seats",
    ...options
  });
  var listAgents = (options) => (options?.client ?? client).get({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/agents",
    ...options
  });
  var configureAgentSeat = (options) => (options.client ?? client).put({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/agents/{agent_id}/seat-configuration",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers
    }
  });
  var restartAgentSeat = (options) => (options.client ?? client).post({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/agents/{agent_id}/seat-restart",
    ...options
  });
  var createChannel = (options) => (options.client ?? client).post({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/channels",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers
    }
  });
  var renameChannel = (options) => (options.client ?? client).patch({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/channels/{channel_id}",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers
    }
  });
  var archiveChannel = (options) => (options.client ?? client).post({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/channels/{channel_id}/archive",
    ...options
  });
  var addChannelMember = (options) => (options.client ?? client).post({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/channels/{channel_id}/members",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers
    }
  });
  var listConversations = (options) => (options?.client ?? client).get({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/conversations",
    ...options
  });
  var openDirectConversation = (options) => (options.client ?? client).post({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/direct-conversations",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers
    }
  });
  var listPluginGenerations = (options) => (options?.client ?? client).get({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/plugin-generations",
    ...options
  });
  var listSessionBindings = (options) => (options?.client ?? client).get({
    security: [{ scheme: "bearer", type: "http" }],
    url: "/v1/session-bindings",
    ...options
  });

  // ../../clients/typescript/src/operator-client.ts
  class FleetdOperatorClientError extends Error {
    status;
    body;
    constructor(operation, status, body, cause) {
      super(status === null ? `Fleetd ${operation} request failed before receiving a response` : `Fleetd ${operation} request failed with HTTP ${status}`, cause === undefined ? undefined : { cause });
      this.name = "FleetdOperatorClientError";
      this.status = status;
      this.body = body;
    }
  }
  function createFleetdOperatorClient(options) {
    const origin = exactHttpOrigin(options.origin);
    let operatorCredential = boundedCredential(options.operatorCredential, "operatorCredential");
    const requestTimeoutMs = boundedRequestTimeout(options.requestTimeoutMs);
    const fetchImplementation = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    const wireClient = createClient({
      auth: () => operatorCredential,
      baseUrl: origin,
      fetch: fetchImplementation
    });
    const activeRequests = new Set;
    let closed = false;
    const execute = async (operation, invoke) => {
      if (closed)
        throw new Error("Fleetd operator client is closed");
      const controller = new AbortController;
      activeRequests.add(controller);
      const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
      try {
        const result = await invoke(controller.signal);
        if (result.error === undefined && result.response?.ok) {
          return result.data;
        }
        throw new FleetdOperatorClientError(operation, result.response?.status ?? null, result.error ?? null);
      } catch (cause) {
        if (cause instanceof FleetdOperatorClientError)
          throw cause;
        throw new FleetdOperatorClientError(operation, null, null, cause);
      } finally {
        clearTimeout(timeout);
        activeRequests.delete(controller);
      }
    };
    return {
      listAgents: () => execute("list agents", (signal) => listAgents({ client: wireClient, signal })),
      listAgentSeats: () => execute("list agent seats", (signal) => listAgentSeats({ client: wireClient, signal })),
      listAgentSeatConfigurations: () => execute("list agent seat configurations", (signal) => listAgentSeatConfigurations({ client: wireClient, signal })),
      listConversations: (query) => execute("list conversations", (signal) => listConversations({ client: wireClient, query, signal })),
      createSharedChannel: (body) => execute("create shared channel", (signal) => createChannel({ body, client: wireClient, signal })),
      renameSharedChannel: (channelId, body) => execute("rename shared channel", (signal) => renameChannel({
        body,
        client: wireClient,
        path: { channel_id: channelId },
        signal
      })),
      archiveSharedChannel: (channelId) => execute("archive shared channel", (signal) => archiveChannel({
        client: wireClient,
        path: { channel_id: channelId },
        signal
      })),
      addSharedChannelMember: (channelId, body) => execute("add shared channel member", (signal) => addChannelMember({
        body,
        client: wireClient,
        path: { channel_id: channelId },
        signal
      })),
      openDirectConversation: (body) => execute("open direct conversation", (signal) => openDirectConversation({ body, client: wireClient, signal })),
      listPluginGenerations: (query) => execute("list plugin generations", (signal) => listPluginGenerations({ client: wireClient, query, signal })),
      listSessionBindings: (query) => execute("list session bindings", (signal) => listSessionBindings({ client: wireClient, query, signal })),
      configureAgentSeat: (agentId, body) => execute("configure agent seat", (signal) => configureAgentSeat({
        body,
        client: wireClient,
        path: { agent_id: agentId },
        signal
      })),
      restartAgentSeat: (agentId) => execute("restart agent seat", (signal) => restartAgentSeat({
        client: wireClient,
        path: { agent_id: agentId },
        signal
      })),
      close() {
        if (closed)
          return;
        closed = true;
        for (const request of activeRequests)
          request.abort();
        activeRequests.clear();
        operatorCredential = "";
      }
    };
  }

  // src/ui/composer.ts
  function composerAvailability(input) {
    const channelReady = input.phase === "live" && input.selectedChannelId !== null;
    const sending = input.sending || input.pendingSends > 0;
    return {
      textareaDisabled: !channelReady,
      sendDisabled: !channelReady || input.draft.trim() === "" || sending,
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
      label: displayName(member.agent_name),
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
  function displayName(value) {
    const trimmed = value.trim();
    const stem = trimmed.replace(/[-_](?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i, "");
    if (stem === trimmed)
      return value;
    const words = stem.split(/[-_]+/).filter(Boolean);
    if (words.length === 0)
      return value;
    return words.map((word, index) => {
      if (/^[a-z]$/i.test(word))
        return word.toUpperCase();
      if (index === 0)
        return `${word.charAt(0).toUpperCase()}${word.slice(1)}`;
      return word.toLowerCase();
    }).join(" ");
  }

  // src/ui/attention.ts
  function conversationAttentionBadge(attention) {
    const unread = attention?.unread_count ?? 0;
    const addressed = attention?.addressed_unread_count ?? 0;
    if (unread <= 0)
      return;
    return addressed > 0 ? {
      text: `@${boundedCount(addressed)}`,
      description: `${unread} unread, ${addressed} addressed to you`,
      tone: "addressed"
    } : {
      text: boundedCount(unread),
      description: `${unread} unread`,
      tone: "unread"
    };
  }
  function boundedCount(count) {
    return count > 99 ? "99+" : String(count);
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
  function renderChannelList(container, channels, selectedChannelId, snapshot, attention = new Map) {
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
      updateChannelRow(row, channel, selected, undefined, attention.get(channel.id));
      return row;
    });
    reconcileChildren(container, rows);
  }
  function renderConversationNavigation(elements, conversations, selectedChannelId, snapshot, agentHealth = new Map, attention = new Map) {
    const active = conversations.filter((conversation) => conversation.archived_at_ms == null && conversation.members.some((member) => member.agent_id === snapshot.participantId));
    const shared = active.filter((conversation) => conversation.kind === "shared");
    const direct = active.filter((conversation) => conversation.kind === "direct");
    renderChannelList(elements.channels, shared, selectedChannelId, snapshot, attention);
    renderDirectList(elements.directs, direct, selectedChannelId, snapshot, agentHealth, attention);
  }
  function renderDirectList(container, conversations, selectedChannelId, snapshot, agentHealth, attention) {
    container.setAttribute("aria-busy", String(snapshot.phase === "loading_channels"));
    if (conversations.length === 0) {
      const state = document.createElement("p");
      state.className = `channel-state channel-state-${snapshot.phase}`;
      state.setAttribute("role", "status");
      state.textContent = snapshot.phase === "loading_channels" ? "Loading direct messages…" : snapshot.phase === "failed" ? snapshot.error?.message ?? "Direct messages unavailable" : "No direct messages yet";
      container.replaceChildren(state);
      return;
    }
    const existing = new Map;
    for (const button of container.querySelectorAll("button[data-channel-id]")) {
      if (button.dataset.channelId)
        existing.set(button.dataset.channelId, button);
    }
    const rows = conversations.map((conversation) => {
      const selected = conversation.id === selectedChannelId;
      const peer = conversation.members.find((member) => member.agent_id !== snapshot.participantId);
      const label = peer ? displayName(peer.agent_name) : "Direct conversation";
      const row = existing.get(conversation.id) ?? channelRow(conversation, selected);
      updateChannelRow(row, conversation, selected, {
        label,
        marker: avatarLabel(label)
      }, attention.get(conversation.id));
      if (peer) {
        row.dataset.directAgentId = peer.agent_id;
        row.dataset.agentHealth = agentHealth.get(peer.agent_id) ?? "unmanaged";
        const attentionDescription = row.dataset.attentionDescription;
        row.title = `${selected ? `${peer.agent_name}, current direct message` : `Direct message with ${peer.agent_name}`}${attentionDescription ? ` · ${attentionDescription}` : ""}`;
      }
      return row;
    });
    reconcileChildren(container, rows);
  }
  function channelRow(channel, selected) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "channel-button nav-item";
    button.dataset.channelId = channel.id;
    const marker = document.createElement("span");
    marker.className = "channel-marker nav-item__icon";
    marker.append(icon("channel", "#"));
    const content = document.createElement("span");
    content.className = "nav-item__content";
    const label = document.createElement("span");
    label.className = "channel-label nav-item__label";
    label.textContent = channel.name;
    content.append(label);
    const attention = document.createElement("span");
    attention.className = "nav-item__attention";
    attention.hidden = true;
    attention.setAttribute("aria-hidden", "true");
    const chevron = icon("chevron", "›", "nav-item__chevron");
    button.append(marker, content, attention, chevron);
    updateChannelRow(button, channel, selected);
    return button;
  }
  function updateChannelRow(button, channel, selected, presentation, attention) {
    const attentionDescription = updateAttentionBadge(button, attention);
    button.title = `${selected ? `${channel.name}, current channel` : channel.name}${attentionDescription ? ` · ${attentionDescription}` : ""}`;
    const label = button.querySelector(".channel-label");
    if (label)
      label.textContent = presentation?.label ?? displayName(channel.name);
    const marker = button.querySelector(".channel-marker .ui-icon");
    if (marker && presentation)
      marker.textContent = presentation.marker;
    button.setAttribute("aria-pressed", String(selected));
    if (selected) {
      button.setAttribute("aria-current", "page");
    } else {
      button.removeAttribute("aria-current");
    }
    const labelText = presentation?.label ?? displayName(channel.name);
    button.setAttribute("aria-label", `${labelText}${selected ? ", current conversation" : ""}${attentionDescription ? `, ${attentionDescription}` : ""}`);
  }
  function updateAttentionBadge(button, attention) {
    const badge = button.querySelector(".nav-item__attention");
    if (!badge)
      return;
    const unread = attention?.unread_count ?? 0;
    const addressed = attention?.addressed_unread_count ?? 0;
    const view = conversationAttentionBadge(attention);
    button.dataset.unreadCount = String(unread);
    button.dataset.addressedUnreadCount = String(addressed);
    if (!view) {
      badge.hidden = true;
      badge.textContent = "";
      delete badge.dataset.tone;
      delete button.dataset.attentionDescription;
      return;
    }
    badge.hidden = false;
    badge.dataset.tone = view.tone;
    badge.textContent = view.text;
    badge.title = view.description;
    button.dataset.attentionDescription = view.description;
    return view.description;
  }
  function renderChannelHeader(snapshot, elements, conversation) {
    const channel = snapshot.channels.find((candidate) => candidate.id === snapshot.selectedChannelId);
    const peer = conversation?.kind === "direct" ? conversation.members.find((member) => member.agent_id !== snapshot.participantId) : undefined;
    const title = peer ? displayName(peer.agent_name) : channel ? displayName(channel.name) : "Select a conversation";
    elements.title.textContent = title;
    elements.title.title = peer?.agent_name ?? channel?.name ?? "";
    if (elements.avatar) {
      elements.avatar.textContent = peer ? avatarLabel(title) : "#";
      elements.avatar.dataset.kind = conversation?.kind ?? "shared";
    }
    elements.meta.textContent = channel ? `${conversation?.kind === "direct" ? "Direct message" : `${snapshot.members.length} participants`} · ${snapshot.messages.length} messages` : snapshot.phase === "loading_channels" ? "Finding conversations…" : "Choose a conversation to begin.";
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
    #firstUnreadSeq = null;
    #unreadMarker;
    constructor(container) {
      this.#container = container;
    }
    clear() {
      this.#container.replaceChildren();
      this.#channelId = null;
      this.#firstUnreadSeq = null;
      this.#unreadMarker = undefined;
    }
    visibleThroughSeq() {
      const viewport = this.#container.getBoundingClientRect();
      let through = null;
      for (const article of this.#container.querySelectorAll("[data-message-seq]")) {
        const sequence = Number(article.dataset.messageSeq);
        const bounds = article.getBoundingClientRect();
        if (Number.isSafeInteger(sequence) && sequence >= 0 && bounds.top < viewport.bottom && bounds.bottom > viewport.top) {
          through = through === null ? sequence : Math.max(through, sequence);
        }
      }
      return through;
    }
    render(snapshot, contract, firstUnreadSeq = null) {
      const changedChannel = this.#channelId !== snapshot.selectedChannelId;
      const changedUnreadBoundary = this.#firstUnreadSeq !== firstUnreadSeq;
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
      const nodes = [];
      let unreadMarkerInserted = false;
      for (const message of snapshot.messages) {
        if (!unreadMarkerInserted && firstUnreadSeq !== null && message.seq >= firstUnreadSeq) {
          if (changedUnreadBoundary || !this.#unreadMarker) {
            this.#unreadMarker = unreadMarker();
          }
          nodes.push(this.#unreadMarker);
          unreadMarkerInserted = true;
        }
        const current = existing.get(message.id);
        if (current?.dataset.messageSeq === String(message.seq)) {
          updateMessageLabels(current, message, snapshot.participantId, names);
          nodes.push(current);
        } else {
          nodes.push(messageCard(message, snapshot.participantId, names, contract));
        }
      }
      reconcileChildren(this.#container, nodes);
      this.#container.setAttribute("aria-busy", String(snapshot.phase === "connecting" || snapshot.phase === "reconnecting"));
      this.#channelId = snapshot.selectedChannelId;
      this.#firstUnreadSeq = firstUnreadSeq;
      if ((changedChannel || changedUnreadBoundary) && unreadMarkerInserted) {
        const marker = this.#unreadMarker;
        if (marker) {
          this.#container.scrollTop = Math.max(0, marker.offsetTop - this.#container.offsetTop - 16);
        }
      } else if (changedChannel || nearBottom) {
        this.#container.scrollTop = this.#container.scrollHeight;
      } else if (anchor) {
        restoreAnchor(this.#container, anchor);
      }
    }
  }
  function unreadMarker() {
    const marker = document.createElement("div");
    marker.className = "unread-marker";
    marker.dataset.unreadMarker = "true";
    marker.setAttribute("role", "separator");
    marker.setAttribute("aria-label", "New messages");
    const label = document.createElement("span");
    label.textContent = "New messages";
    marker.append(label);
    return marker;
  }
  function messageCard(message, participantId, names, contract) {
    const article = document.createElement("article");
    article.className = message.sender_id === participantId ? "message message-card message-self" : "message message-card";
    article.dataset.messageId = message.id;
    article.dataset.messageSeq = String(message.seq);
    article.dataset.direction = message.sender_id === participantId ? "outgoing" : "incoming";
    const avatar = document.createElement("span");
    avatar.className = "message-card__avatar";
    avatar.setAttribute("aria-hidden", "true");
    const content = document.createElement("div");
    content.className = "message-card__content";
    const header = document.createElement("header");
    header.className = "message-card__header";
    const identity = document.createElement("span");
    identity.className = "message-card__identity";
    const sender = document.createElement("strong");
    sender.className = "message-sender";
    const kind = document.createElement("code");
    kind.className = "message-kind";
    kind.textContent = messageKindLabel(message.kind, contract);
    kind.title = message.kind;
    identity.append(sender, kind);
    const time = document.createElement("time");
    const created = new Date(message.created_at_ms);
    time.dateTime = created.toISOString();
    time.textContent = created.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit"
    });
    time.title = created.toLocaleString();
    header.append(identity, time);
    const rendered = renderMessageBody(message, contract);
    const body = document.createElement(rendered.format === "json" ? "pre" : "p");
    body.className = rendered.format === "json" ? "message-json message-card__body" : "message-text message-card__body";
    body.textContent = rendered.text;
    const footer = document.createElement("div");
    footer.className = "message-card__footer";
    const delivery = document.createElement("span");
    delivery.className = "message-card__delivery";
    if (rendered.status) {
      const status = document.createElement("span");
      status.className = "result-status";
      status.dataset.tone = statusTone(rendered.status);
      const statusLabel = document.createElement("span");
      statusLabel.className = "result-status__label";
      statusLabel.textContent = rendered.status;
      status.append(icon("status", "✓"), statusLabel);
      delivery.append(status);
    }
    const recipient = document.createElement("span");
    recipient.className = "message-recipient";
    delivery.append(recipient);
    footer.append(delivery);
    const details = document.createElement("details");
    details.className = "message-details";
    const summary = document.createElement("summary");
    summary.className = "message-details__trigger";
    summary.append(icon("envelope", "···"), text(`Details · message ${message.seq}`));
    const envelope = document.createElement("pre");
    envelope.className = "message-details__envelope";
    envelope.textContent = JSON.stringify(message, null, 2);
    details.append(summary, envelope);
    footer.append(details);
    content.append(header, body, footer);
    article.append(avatar, content);
    updateMessageLabels(article, message, participantId, names);
    return article;
  }
  function messageKindLabel(kind, contract) {
    if (kind === contract.requestKind)
      return "Message";
    if (kind === contract.resultKind)
      return "Reply";
    return "Event";
  }
  function statusTone(status) {
    const normalized = status.trim().toLowerCase();
    if (["complete", "completed", "done", "success", "succeeded"].includes(normalized)) {
      return "success";
    }
    if (["queued", "pending", "running", "working", "in_progress"].includes(normalized)) {
      return "warning";
    }
    if (["error", "failed", "failure", "cancelled", "canceled"].includes(normalized)) {
      return "danger";
    }
    return "neutral";
  }
  function updateMessageLabels(article, message, participantId, names) {
    const sender = displayName(senderLabel(message, participantId, names));
    const recipient = displayName(recipientLabel(message, participantId, names));
    const senderElement = article.querySelector(".message-sender");
    const recipientElement = article.querySelector(".message-recipient");
    const avatarElement = article.querySelector(".message-card__avatar");
    if (senderElement)
      senderElement.textContent = sender;
    if (recipientElement)
      recipientElement.textContent = `to ${recipient}`;
    if (senderElement) {
      senderElement.title = exactParticipantLabel(message.sender_id, names);
    }
    if (recipientElement && message.recipient_id) {
      recipientElement.title = exactParticipantLabel(message.recipient_id, names);
    }
    if (avatarElement)
      avatarElement.textContent = avatarLabel(sender);
    const outgoing = message.sender_id === participantId;
    article.classList.toggle("message-self", outgoing);
    article.dataset.direction = outgoing ? "outgoing" : "incoming";
    article.setAttribute("aria-label", `Message from ${sender} to ${recipient}`);
  }
  function avatarLabel(sender) {
    const meaningful = sender.trim().replace(/^@/, "");
    return meaningful.slice(0, 1).toLocaleUpperCase() || "·";
  }
  function exactParticipantLabel(id, names) {
    const name = names.get(id);
    return name ? `${name} · ${id}` : id;
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

  // src/ui/mentions.ts
  var MAX_MENTION_CANDIDATES = 8;
  function mentionQueryAt(draft, caret) {
    if (!Number.isSafeInteger(caret) || caret < 0 || caret > draft.length) {
      return;
    }
    const prefix = draft.slice(0, caret);
    const match = prefix.match(/(?:^|[\s([{])@([^\s@]*)$/u);
    if (!match)
      return;
    const marker = prefix.lastIndexOf("@");
    return {
      start: marker,
      end: caret,
      text: match[1] ?? ""
    };
  }
  function mentionCandidates(members, participantId, query) {
    const normalizedQuery = normalize(query);
    return members.filter((member) => member.agent_id !== participantId).map((member) => {
      const option = memberOptionView(member);
      return {
        recipientId: member.agent_id,
        label: option.label,
        exactName: member.agent_name,
        description: option.description,
        receivesInboxWork: member.delivery_mode === "inbox"
      };
    }).filter((candidate) => {
      if (normalizedQuery === "")
        return true;
      return [candidate.label, candidate.exactName].some((value) => normalize(value).includes(normalizedQuery));
    }).sort((left, right) => {
      const leftStarts = normalize(left.label).startsWith(normalizedQuery);
      const rightStarts = normalize(right.label).startsWith(normalizedQuery);
      if (leftStarts !== rightStarts)
        return leftStarts ? -1 : 1;
      if (left.receivesInboxWork !== right.receivesInboxWork) {
        return left.receivesInboxWork ? -1 : 1;
      }
      return left.label.localeCompare(right.label);
    }).slice(0, MAX_MENTION_CANDIDATES);
  }
  function applyMention(draft, query, candidate) {
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
        label: candidate.label
      }
    };
  }
  function mentionSelectionPresent(draft, selection) {
    return draft.includes(selection.token);
  }
  function directRecipient(members, participantId) {
    const peers = mentionCandidates(members, participantId, "");
    return peers.length === 1 ? peers[0] : undefined;
  }
  function normalize(value) {
    return value.trim().toLocaleLowerCase();
  }

  // src/ui/workspace-components.ts
  function renderAgentDirectory(container, items) {
    if (items.length === 0) {
      const empty = document.createElement("div");
      empty.className = "dialog-state";
      empty.textContent = "No other agents are registered in this workspace.";
      container.replaceChildren(empty);
      return;
    }
    const rows = items.map((item) => {
      const article = document.createElement("article");
      article.className = "agent-card";
      article.dataset.agentId = item.id;
      article.dataset.health = item.health;
      const avatar = document.createElement("span");
      avatar.className = "agent-avatar";
      avatar.setAttribute("aria-hidden", "true");
      avatar.textContent = item.initials;
      const content = document.createElement("div");
      const name = document.createElement("strong");
      name.textContent = item.name;
      name.title = item.exactName;
      const status = document.createElement("small");
      status.className = "agent-card__status";
      status.textContent = `${item.status} · ${item.description}`;
      content.append(name, status);
      const actions = document.createElement("div");
      actions.className = "agent-card__actions";
      const manage = document.createElement("button");
      manage.type = "button";
      manage.className = "button";
      manage.dataset.manageAgentId = item.id;
      manage.textContent = item.configured ? "Manage" : "Set up";
      manage.setAttribute("aria-label", `${item.configured ? "Manage" : "Set up"} ${item.name}`);
      const message = document.createElement("button");
      message.type = "button";
      message.className = "button";
      message.dataset.directAgentId = item.id;
      message.textContent = "Message";
      message.setAttribute("aria-label", `Message ${item.name}`);
      actions.append(manage, message);
      article.append(avatar, content, actions);
      return article;
    });
    container.replaceChildren(...rows);
  }
  function renderChannelMemberOptions(container, agents, participantId) {
    const options = agents.filter((agent) => agent.id !== participantId).sort((left, right) => left.name.localeCompare(right.name)).map((agent) => {
      const label = document.createElement("label");
      label.className = "member-option";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.name = "member";
      input.value = agent.id;
      const content = document.createElement("span");
      const name = document.createElement("strong");
      name.textContent = displayName(agent.name);
      name.title = `${agent.name} · ${agent.id}`;
      const detail = document.createElement("small");
      detail.textContent = `Agent · ${shortId(agent.id)}`;
      content.append(name, detail);
      label.append(input, content);
      return label;
    });
    if (options.length === 0) {
      const empty = document.createElement("div");
      empty.className = "dialog-state";
      empty.textContent = "Register another agent before creating a shared channel.";
      container.replaceChildren(empty);
      return;
    }
    container.replaceChildren(...options);
  }
  function selectedMemberIds(container) {
    return Array.from(container.querySelectorAll('input[name="member"]:checked'), (input) => input.value);
  }
  function renderConversationMembers(container, members, participantId) {
    const rows = members.map((member) => {
      const row = document.createElement("div");
      row.className = "member-row";
      row.dataset.agentId = member.agent_id;
      const avatar = document.createElement("span");
      avatar.className = "member-avatar";
      avatar.setAttribute("aria-hidden", "true");
      avatar.textContent = avatarLabel2(member.agent_name);
      const content = document.createElement("div");
      const name = document.createElement("strong");
      name.textContent = member.agent_id === participantId ? "You" : displayName(member.agent_name);
      name.title = `${member.agent_name} · ${member.agent_id}`;
      const detail = document.createElement("small");
      detail.textContent = shortId(member.agent_id);
      content.append(name, detail);
      const role = document.createElement("span");
      role.className = "member-role";
      role.textContent = member.delivery_mode === "stream_only" ? "Participant" : "Agent";
      row.append(avatar, content, role);
      return row;
    });
    container.replaceChildren(...rows);
  }
  function renderAddMemberOptions(select, agents, members, participantId) {
    const memberIds = new Set(members.map((member) => member.agent_id));
    const available = agents.filter((agent) => agent.id !== participantId && !memberIds.has(agent.id)).sort((left, right) => left.name.localeCompare(right.name));
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = available.length === 0 ? "All agents are members" : "Choose an agent";
    placeholder.disabled = true;
    placeholder.selected = true;
    const options = available.map((agent) => {
      const option = document.createElement("option");
      option.value = agent.id;
      option.textContent = displayName(agent.name);
      option.title = `${agent.name} · ${agent.id}`;
      return option;
    });
    select.replaceChildren(placeholder, ...options);
    select.disabled = available.length === 0;
  }
  function avatarLabel2(name) {
    return displayName(name).trim().slice(0, 1).toLocaleUpperCase() || "·";
  }

  // src/ui/workspace-view-models.ts
  function agentDirectoryItems(agents, generations, bindings, participantId, configurations = []) {
    const configurationByAgent = new Map(configurations.map((configuration) => [configuration.agent_id, configuration]));
    const latestGenerations = latestByAgent(generations, (generation) => generation.agent_id, (generation) => generation.started_at_ms);
    const latestBindings = latestByAgent(bindings, (binding) => binding.agent_id, (binding) => binding.updated_at_ms);
    return agents.filter((agent) => agent.id !== participantId).map((agent) => agentDirectoryItem(agent, latestGenerations.get(agent.id), latestBindings.get(agent.id), configurationByAgent.get(agent.id))).sort((left, right) => {
      const healthDifference = healthOrder(left.health) - healthOrder(right.health);
      return healthDifference || left.name.localeCompare(right.name);
    });
  }
  function agentDirectoryItem(agent, generation, binding, configuration) {
    const name = displayName(agent.name);
    const identity = `${agent.name} · ${agent.id}`;
    if (binding?.state === "active" && binding.active_invocation_id != null && generation?.health === "active") {
      return {
        id: agent.id,
        name,
        exactName: identity,
        initials: initials(name),
        health: "working",
        status: "Working",
        description: `${generation.runtime_name} has an active invocation.`,
        configured: configuration !== undefined,
        desiredState: configuration?.desired_state
      };
    }
    if (binding?.state === "uncertain") {
      return {
        id: agent.id,
        name,
        exactName: identity,
        initials: initials(name),
        health: "stale",
        status: "Needs attention",
        description: "Its latest session has an uncertain outcome.",
        configured: configuration !== undefined,
        desiredState: configuration?.desired_state
      };
    }
    if (generation?.health === "active") {
      const hasActiveSession = binding?.state === "active";
      return {
        id: agent.id,
        name,
        exactName: identity,
        initials: initials(name),
        health: "active",
        status: hasActiveSession ? "Session active" : "Worker observed",
        description: hasActiveSession ? `${generation.runtime_name} owns an active session with no active invocation recorded.` : `Fleetd observed an active ${generation.runtime_name} plugin generation.`,
        configured: configuration !== undefined,
        desiredState: configuration?.desired_state
      };
    }
    if (generation?.health === "stale") {
      return {
        id: agent.id,
        name,
        exactName: identity,
        initials: initials(name),
        health: "stale",
        status: "Connection stale",
        description: "Fleetd has not observed a recent worker heartbeat.",
        configured: configuration !== undefined,
        desiredState: configuration?.desired_state
      };
    }
    if (generation?.health === "stopped") {
      return {
        id: agent.id,
        name,
        exactName: identity,
        initials: initials(name),
        health: "stopped",
        status: "Offline",
        description: "Its latest observed worker generation has stopped.",
        configured: configuration !== undefined,
        desiredState: configuration?.desired_state
      };
    }
    return {
      id: agent.id,
      name,
      exactName: identity,
      initials: initials(name),
      health: "unmanaged",
      status: configuration?.desired_state === "running" ? "Starting" : configuration?.desired_state === "stopped" ? "Stopped by you" : "Not configured",
      description: configuration?.desired_state === "running" ? "This machine is reconciling its approved runtime." : configuration?.desired_state === "stopped" ? "Its configured runtime is not currently requested." : `Registered participant ${shortId(agent.id)} has no runtime profile.`,
      configured: configuration !== undefined,
      desiredState: configuration?.desired_state
    };
  }
  function latestByAgent(values, agentId, timestamp) {
    const latest = new Map;
    for (const value of values) {
      const key = agentId(value);
      const current = latest.get(key);
      if (!current || timestamp(value) > timestamp(current))
        latest.set(key, value);
    }
    return latest;
  }
  function healthOrder(health) {
    return {
      working: 0,
      active: 1,
      stale: 2,
      stopped: 3,
      unmanaged: 4
    }[health];
  }
  function initials(name) {
    return name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toLocaleUpperCase() ?? "").join("") || "·";
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
    directs: required("direct-list"),
    openAgentDirectory: required("open-agent-directory"),
    newChannel: required("new-channel"),
    newDirectMessage: required("new-direct-message"),
    channelTitle: required("channel-title"),
    channelMeta: required("channel-meta"),
    channelAvatar: required("channel-avatar"),
    messages: required("message-list"),
    empty: required("empty-conversation"),
    emptyTitle: required("empty-conversation-title"),
    emptyCopy: required("empty-conversation-copy"),
    composer: required("composer"),
    composerText: required("composer-text"),
    composerAudience: required("composer-audience"),
    composerAudienceLabel: requiredDescendant(required("composer-audience"), ".composer-audience-label"),
    clearMention: required("clear-mention"),
    mentionSuggestions: required("mention-suggestions"),
    send: required("send-message"),
    disconnect: required("disconnect"),
    openConversationDetails: required("open-conversation-details"),
    agentDirectoryDialog: required("agent-directory-dialog"),
    agentDirectoryState: required("agent-directory-state"),
    agentList: required("agent-list"),
    agentSeatDialog: required("agent-seat-dialog"),
    agentSeatForm: required("agent-seat-form"),
    agentSeatTitle: required("agent-seat-title"),
    agentSeatCopy: required("agent-seat-copy"),
    agentSeatProfileSummary: required("agent-seat-profile-summary"),
    agentSeatProfile: required("agent-seat-profile"),
    agentSeatInstructions: required("agent-seat-instructions"),
    agentSeatDesiredState: required("agent-seat-desired-state"),
    agentSeatError: required("agent-seat-error"),
    restartAgentSeat: required("restart-agent-seat"),
    saveAgentSeat: required("save-agent-seat"),
    channelDialog: required("channel-dialog"),
    channelForm: required("channel-form"),
    channelName: required("channel-name"),
    channelMemberOptions: required("channel-member-options"),
    channelFormError: required("channel-form-error"),
    createChannel: required("create-channel"),
    conversationDetailsDialog: required("conversation-details-dialog"),
    conversationDetailsKicker: required("conversation-details-kicker"),
    conversationDetailsTitle: required("conversation-details-title"),
    conversationDetailsCopy: required("conversation-details-copy"),
    renameChannelForm: required("rename-channel-form"),
    renameChannelName: required("rename-channel-name"),
    renameChannel: required("rename-channel"),
    conversationMemberCount: required("conversation-member-count"),
    conversationMemberList: required("conversation-member-list"),
    addMemberForm: required("add-member-form"),
    addMemberAgent: required("add-member-agent"),
    addMember: required("add-member"),
    conversationDetailsError: required("conversation-details-error"),
    channelDangerZone: required("channel-danger-zone"),
    requestArchiveChannel: required("request-archive-channel"),
    archiveChannelDialog: required("archive-channel-dialog"),
    archiveChannelForm: required("archive-channel-form"),
    archiveChannelCopy: required("archive-channel-copy"),
    archiveChannel: required("archive-channel")
  };
  var connectSubmit = requiredDescendant(elements.connectForm, 'button[type="submit"]');
  var connectSubmitLabel = requiredDescendant(connectSubmit, ".button-label");
  var connectSubmitIcon = requiredDescendant(connectSubmit, ".button-icon");
  var session;
  var operatorClient;
  var unsubscribe;
  var contract;
  var latestSnapshot;
  var renderFrame;
  var sendInFlight = false;
  var connectInFlight = false;
  var appGeneration = 0;
  var agents = [];
  var conversations = [];
  var pluginGenerations = [];
  var sessionBindings = [];
  var seatConfigurations = [];
  var runtimeProfiles = [];
  var configuredAgentId;
  var workspaceError;
  var workspaceBusy = false;
  var mentionSelection;
  var mentionQuery;
  var mentionOptions = [];
  var activeMentionIndex = 0;
  var mentionDismissed = false;
  var renderedChannelId = null;
  var visitFirstUnreadSeq = null;
  var pendingReadAdvances = new Map;
  var readAdvanceRunning = false;
  var attentionRefreshRunning = false;
  var attentionRefreshTimer;
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
        direct_conversation_count: conversations.filter((conversation) => conversation.kind === "direct").length,
        agent_count: agents.length,
        member_count: snapshot?.members.length ?? 0,
        message_ids: snapshot?.messages.map((message) => message.id) ?? [],
        message_sequences: snapshot?.messages.map((message) => message.seq) ?? [],
        attention: snapshot?.attention ?? [],
        first_unread_seq: visitFirstUnreadSeq,
        pending_sends: snapshot?.pendingSends ?? 0,
        composer_recipient_id: currentRecipientId(),
        mention_suggestions_open: !elements.mentionSuggestions.hidden,
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
      showConnectError("Check your workspace and participant keys, then try again.");
    }).finally(() => {
      connectInFlight = false;
      setConnectBusy(false);
    });
  });
  elements.disconnect.addEventListener("click", disconnect);
  elements.channels.addEventListener("click", selectConversationFromEvent);
  elements.directs.addEventListener("click", selectConversationFromEvent);
  elements.openAgentDirectory.addEventListener("click", openAgentDirectory);
  elements.newDirectMessage.addEventListener("click", openAgentDirectory);
  elements.newChannel.addEventListener("click", openCreateChannel);
  elements.openConversationDetails.addEventListener("click", openConversationDetails);
  elements.agentList.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element))
      return;
    const manage = target.closest("button[data-manage-agent-id]");
    if (manage?.dataset.manageAgentId) {
      openAgentSeatConfiguration(manage.dataset.manageAgentId);
      return;
    }
    const button = target.closest("button[data-direct-agent-id]");
    if (!button?.dataset.directAgentId)
      return;
    openDirectMessage(button.dataset.directAgentId, button);
  });
  elements.agentSeatForm.addEventListener("submit", (event) => {
    event.preventDefault();
    saveAgentSeatConfiguration();
  });
  elements.restartAgentSeat.addEventListener("click", () => {
    restartConfiguredAgent();
  });
  elements.agentSeatProfile.addEventListener("change", renderSelectedProfileSummary);
  elements.channelForm.addEventListener("submit", (event) => {
    event.preventDefault();
    createSharedChannel();
  });
  elements.renameChannelForm.addEventListener("submit", (event) => {
    event.preventDefault();
    renameSelectedChannel();
  });
  elements.addMemberForm.addEventListener("submit", (event) => {
    event.preventDefault();
    addSelectedChannelMember();
  });
  elements.requestArchiveChannel.addEventListener("click", confirmChannelArchive);
  elements.archiveChannelForm.addEventListener("submit", (event) => {
    event.preventDefault();
    archiveSelectedChannel();
  });
  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element))
      return;
    const button = target.closest("button[data-close-dialog]");
    const dialogId = button?.dataset.closeDialog;
    if (!dialogId)
      return;
    const dialog = document.getElementById(dialogId);
    if (dialog instanceof HTMLDialogElement)
      dialog.close();
  });
  elements.composer.addEventListener("submit", (event) => {
    event.preventDefault();
    sendComposerMessage();
  });
  elements.composerText.addEventListener("input", () => {
    mentionDismissed = false;
    if (mentionSelection && !mentionSelectionPresent(elements.composerText.value, mentionSelection)) {
      mentionSelection = undefined;
    }
    refreshMentionSuggestions();
    resizeComposer(elements.composerText);
    renderComposerAvailability();
    renderComposerContext();
  });
  elements.composerText.addEventListener("keydown", (event) => {
    if (handleMentionKeydown(event))
      return;
    if (!isComposerSendShortcut(event))
      return;
    event.preventDefault();
    elements.composer.requestSubmit();
  });
  elements.composerText.addEventListener("click", refreshMentionSuggestions);
  elements.composerText.addEventListener("keyup", (event) => {
    if (["ArrowUp", "ArrowDown", "Enter", "Tab", "Escape"].includes(event.key)) {
      return;
    }
    refreshMentionSuggestions();
  });
  elements.mentionSuggestions.addEventListener("mousedown", (event) => {
    event.preventDefault();
  });
  elements.mentionSuggestions.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element))
      return;
    const option = target.closest("button[data-recipient-id]");
    const recipientId = option?.dataset.recipientId;
    if (!recipientId)
      return;
    const candidate = mentionOptions.find((item) => item.recipientId === recipientId);
    if (candidate)
      selectMention(candidate);
  });
  elements.clearMention.addEventListener("click", clearSelectedMention);
  elements.messages.addEventListener("scroll", queueCurrentVisibleRead);
  window.addEventListener("focus", refreshAttentionOnReturn);
  window.addEventListener("resize", queueCurrentVisibleRead);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible")
      refreshAttentionOnReturn();
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
    runtimeProfiles = profile.runtimeProfiles ?? [];
    let activeOperatorClient;
    const transport = (() => {
      try {
        activeOperatorClient = createFleetdOperatorClient({
          origin: window.location.origin,
          operatorCredential: profile.operatorCredential
        });
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
    if (!activeOperatorClient)
      throw new Error("Fleetd workspace connection failed");
    operatorClient = activeOperatorClient;
    const activeSession = new ConversationSession(transport);
    session = activeSession;
    unsubscribe = activeSession.subscribe(scheduleRender);
    elements.connectPanel.hidden = true;
    elements.app.hidden = false;
    try {
      await Promise.all([
        activeSession.start(),
        refreshWorkspace(activeOperatorClient)
      ]);
      if (generation !== appGeneration || session !== activeSession) {
        throw new Error("Fleetd conversation connection was superseded");
      }
      const channelId = profile.channelId;
      if (channelId)
        await activeSession.selectChannel(channelId);
      scheduleAttentionRefresh();
    } catch {
      if (generation === appGeneration && session === activeSession)
        disconnect();
      throw new Error("Fleetd conversation connection failed");
    }
  }
  function disconnect() {
    appGeneration += 1;
    sendInFlight = false;
    if (attentionRefreshTimer !== undefined) {
      window.clearTimeout(attentionRefreshTimer);
      attentionRefreshTimer = undefined;
    }
    unsubscribe?.();
    unsubscribe = undefined;
    session?.close();
    session = undefined;
    operatorClient?.close();
    operatorClient = undefined;
    contract = undefined;
    latestSnapshot = undefined;
    agents = [];
    conversations = [];
    pluginGenerations = [];
    sessionBindings = [];
    seatConfigurations = [];
    runtimeProfiles = [];
    configuredAgentId = undefined;
    workspaceError = undefined;
    workspaceBusy = false;
    resetMentionState();
    renderedChannelId = null;
    visitFirstUnreadSeq = null;
    pendingReadAdvances.clear();
    if (renderFrame !== undefined)
      cancelAnimationFrame(renderFrame);
    renderFrame = undefined;
    elements.app.hidden = true;
    elements.connectPanel.hidden = false;
    for (const dialog of [
      elements.agentDirectoryDialog,
      elements.agentSeatDialog,
      elements.channelDialog,
      elements.conversationDetailsDialog,
      elements.archiveChannelDialog
    ]) {
      if (dialog.open)
        dialog.close();
    }
    messageList.clear();
    elements.channels.replaceChildren();
    elements.directs.replaceChildren();
    elements.composerText.value = "";
    elements.openConversationDetails.disabled = true;
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
    const attention = new Map(snapshot.attention.map((item) => [item.channel_id, item]));
    const directory = agentDirectoryItems(agents, pluginGenerations, sessionBindings, snapshot.participantId);
    renderConversationNavigation({ channels: elements.channels, directs: elements.directs }, conversations, snapshot.selectedChannelId, snapshot, new Map(directory.map((item) => [item.id, item.health])), attention);
    const selectedConversation = conversations.find((conversation) => conversation.id === snapshot.selectedChannelId);
    if (renderedChannelId !== snapshot.selectedChannelId) {
      resetMentionState();
      renderedChannelId = snapshot.selectedChannelId;
      visitFirstUnreadSeq = snapshot.selectedChannelId === null ? null : attention.get(snapshot.selectedChannelId)?.first_unread_seq ?? null;
    }
    renderChannelHeader(snapshot, {
      title: elements.channelTitle,
      meta: elements.channelMeta,
      avatar: elements.channelAvatar
    }, selectedConversation);
    elements.openConversationDetails.disabled = selectedConversation == null;
    refreshMentionSuggestions();
    renderComposerContext();
    messageList.render(snapshot, requiredContract(), visitFirstUnreadSeq);
    renderEmptyConversation(snapshot, {
      root: elements.empty,
      title: elements.emptyTitle,
      copy: elements.emptyCopy
    });
    renderComposerAvailability();
    queueVisibleReadAdvance(snapshot, attention);
  }
  function selectConversationFromEvent(event) {
    const target = event.target;
    if (!(target instanceof Element))
      return;
    const button = target.closest("button[data-channel-id]");
    if (!button?.dataset.channelId || !session)
      return;
    const activeSession = session;
    const channelId = button.dataset.channelId;
    (async () => {
      try {
        await activeSession.refreshAttention();
      } catch {}
      await activeSession.selectChannel(channelId);
    })().catch(() => {
      if (session === activeSession)
        scheduleRender(activeSession.snapshot);
    });
  }
  function queueVisibleReadAdvance(snapshot, attention) {
    const channelId = snapshot.selectedChannelId;
    if (channelId === null || snapshot.phase !== "live" || snapshot.cursor <= 0 || !documentIsAttended()) {
      return;
    }
    const readThrough = attention.get(channelId)?.read_through_seq ?? 0;
    const throughSeq = messageList.visibleThroughSeq();
    if (throughSeq === null || throughSeq <= readThrough)
      return;
    pendingReadAdvances.set(channelId, Math.max(pendingReadAdvances.get(channelId) ?? 0, throughSeq));
    if (!readAdvanceRunning)
      drainReadAdvances();
  }
  function queueCurrentVisibleRead() {
    const snapshot = latestSnapshot;
    if (!snapshot)
      return;
    queueVisibleReadAdvance(snapshot, new Map(snapshot.attention.map((item) => [item.channel_id, item])));
  }
  async function drainReadAdvances() {
    if (readAdvanceRunning)
      return;
    readAdvanceRunning = true;
    try {
      while (pendingReadAdvances.size > 0) {
        const next = pendingReadAdvances.entries().next().value;
        if (!next)
          break;
        const [channelId, throughSeq] = next;
        pendingReadAdvances.delete(channelId);
        const activeSession = session;
        if (!activeSession)
          continue;
        try {
          await activeSession.markRead(channelId, throughSeq);
        } catch {}
      }
    } finally {
      readAdvanceRunning = false;
      if (pendingReadAdvances.size > 0)
        drainReadAdvances();
    }
  }
  function refreshAttentionOnReturn() {
    const activeSession = session;
    if (!activeSession || attentionRefreshRunning || !documentIsAttended())
      return;
    attentionRefreshRunning = true;
    activeSession.refreshAttention().catch(() => {}).finally(() => {
      attentionRefreshRunning = false;
      if (session === activeSession) {
        scheduleRender(activeSession.snapshot);
      }
    });
  }
  function scheduleAttentionRefresh() {
    if (!session || attentionRefreshTimer !== undefined)
      return;
    const generation = appGeneration;
    attentionRefreshTimer = window.setTimeout(() => {
      attentionRefreshTimer = undefined;
      if (session && generation === appGeneration) {
        refreshAttentionOnReturn();
        scheduleAttentionRefresh();
      }
    }, 5000);
  }
  function documentIsAttended() {
    return document.visibilityState === "visible" && document.hasFocus();
  }
  async function refreshWorkspace(client2 = requiredOperatorClient()) {
    workspaceBusy = true;
    elements.agentList.setAttribute("aria-busy", "true");
    renderAgentDirectoryState();
    try {
      const [
        nextAgents,
        nextConversations,
        nextGenerations,
        nextBindings,
        nextSeatConfigurations
      ] = await Promise.all([
        client2.listAgents(),
        client2.listConversations({ include_archived: false }),
        client2.listPluginGenerations(),
        client2.listSessionBindings(),
        client2.listAgentSeatConfigurations()
      ]);
      if (client2 !== operatorClient)
        return;
      agents = nextAgents;
      conversations = nextConversations;
      pluginGenerations = nextGenerations;
      sessionBindings = nextBindings;
      seatConfigurations = nextSeatConfigurations;
      workspaceError = undefined;
      renderAgentDirectoryState();
      if (session)
        scheduleRender(session.snapshot);
    } catch (error) {
      if (client2 === operatorClient) {
        workspaceError = operatorErrorMessage(error, "Workspace details are unavailable.");
        renderAgentDirectoryState();
      }
      throw error;
    } finally {
      if (client2 === operatorClient) {
        workspaceBusy = false;
        elements.agentList.setAttribute("aria-busy", "false");
        renderAgentDirectoryState();
      }
    }
  }
  function openAgentDirectory() {
    renderAgentDirectoryState();
    showDialog(elements.agentDirectoryDialog);
  }
  function renderAgentDirectoryState() {
    const snapshot = latestSnapshot;
    const items = agentDirectoryItems(agents, pluginGenerations, sessionBindings, snapshot?.participantId ?? "", seatConfigurations);
    renderAgentDirectory(elements.agentList, items);
    if (workspaceError) {
      elements.agentDirectoryState.textContent = workspaceError;
      elements.agentDirectoryState.dataset.tone = "danger";
      elements.agentDirectoryState.hidden = false;
    } else if (workspaceBusy) {
      elements.agentDirectoryState.textContent = "Refreshing agent status…";
      delete elements.agentDirectoryState.dataset.tone;
      elements.agentDirectoryState.hidden = false;
    } else {
      elements.agentDirectoryState.hidden = true;
      elements.agentDirectoryState.textContent = "";
    }
  }
  function openAgentSeatConfiguration(agentId) {
    const agent = agents.find((candidate) => candidate.id === agentId);
    if (!agent)
      return;
    configuredAgentId = agentId;
    const current = seatConfigurations.find((configuration) => configuration.agent_id === agentId);
    elements.agentSeatTitle.textContent = `${current ? "Manage" : "Set up"} ${displayName(agent.name)}`;
    elements.agentSeatCopy.textContent = current ? `This runtime follows ${displayName(agent.name)} wherever the agent participates.` : `Make ${displayName(agent.name)} an active collaborator on this machine.`;
    const options = runtimeProfiles.map((profile) => {
      const option = document.createElement("option");
      option.value = profile.id;
      option.textContent = profile.label;
      return option;
    });
    if (current && !runtimeProfiles.some((profile) => profile.id === current.profile_id)) {
      const unavailable = document.createElement("option");
      unavailable.value = current.profile_id;
      unavailable.textContent = `${current.profile_id} · unavailable on this host`;
      options.unshift(unavailable);
    }
    if (options.length === 0) {
      const unavailable = document.createElement("option");
      unavailable.value = "";
      unavailable.textContent = "No approved profiles available";
      options.push(unavailable);
    }
    elements.agentSeatProfile.replaceChildren(...options);
    elements.agentSeatProfile.value = current?.profile_id ?? runtimeProfiles[0]?.id ?? "";
    elements.agentSeatInstructions.value = current?.instructions ?? "";
    elements.agentSeatDesiredState.value = current?.desired_state ?? "running";
    elements.restartAgentSeat.disabled = current?.desired_state !== "running";
    elements.saveAgentSeat.disabled = runtimeProfiles.length === 0;
    setDialogError(elements.agentSeatError);
    renderSelectedProfileSummary();
    if (elements.agentDirectoryDialog.open)
      elements.agentDirectoryDialog.close();
    if (elements.conversationDetailsDialog.open)
      elements.conversationDetailsDialog.close();
    showDialog(elements.agentSeatDialog);
  }
  function renderSelectedProfileSummary() {
    const selected = runtimeProfiles.find((profile) => profile.id === elements.agentSeatProfile.value);
    elements.agentSeatProfileSummary.textContent = selected ? selected.description : runtimeProfiles.length === 0 ? "This host has not published an approved runtime catalog. Configure the desktop host to activate agents." : "The previously selected profile is not approved on this host.";
    elements.agentSeatProfileSummary.dataset.tone = selected ? "neutral" : "warning";
  }
  async function saveAgentSeatConfiguration() {
    const client2 = operatorClient;
    const agentId = configuredAgentId;
    if (!client2 || !agentId || !elements.agentSeatProfile.value)
      return;
    setFormBusy(elements.agentSeatForm, elements.saveAgentSeat, true, "Applying…");
    elements.restartAgentSeat.disabled = true;
    setDialogError(elements.agentSeatError);
    try {
      await client2.configureAgentSeat(agentId, {
        profile_id: elements.agentSeatProfile.value,
        instructions: elements.agentSeatInstructions.value,
        desired_state: elements.agentSeatDesiredState.value === "stopped" ? "stopped" : "running"
      });
      await refreshWorkspace(client2);
      elements.agentSeatDialog.close();
    } catch (error) {
      setDialogError(elements.agentSeatError, operatorErrorMessage(error, "The agent configuration could not be applied."));
    } finally {
      setFormBusy(elements.agentSeatForm, elements.saveAgentSeat, false, "Save and apply");
      const current = seatConfigurations.find((configuration) => configuration.agent_id === agentId);
      elements.restartAgentSeat.disabled = current?.desired_state !== "running";
    }
  }
  async function restartConfiguredAgent() {
    const client2 = operatorClient;
    const agentId = configuredAgentId;
    if (!client2 || !agentId)
      return;
    elements.restartAgentSeat.disabled = true;
    elements.restartAgentSeat.textContent = "Restarting…";
    setDialogError(elements.agentSeatError);
    try {
      await client2.restartAgentSeat(agentId);
      await refreshWorkspace(client2);
      elements.agentSeatDialog.close();
    } catch (error) {
      setDialogError(elements.agentSeatError, operatorErrorMessage(error, "The agent could not be restarted."));
    } finally {
      elements.restartAgentSeat.textContent = "Restart now";
      const current = seatConfigurations.find((configuration) => configuration.agent_id === agentId);
      elements.restartAgentSeat.disabled = current?.desired_state !== "running";
    }
  }
  function openCreateChannel() {
    const snapshot = latestSnapshot;
    if (!snapshot)
      return;
    elements.channelForm.reset();
    setDialogError(elements.channelFormError);
    renderChannelMemberOptions(elements.channelMemberOptions, agents, snapshot.participantId);
    showDialog(elements.channelDialog);
    elements.channelName.focus();
  }
  async function createSharedChannel() {
    const client2 = operatorClient;
    const activeSession = session;
    const snapshot = latestSnapshot;
    if (!client2 || !activeSession || !snapshot)
      return;
    const memberIds = selectedMemberIds(elements.channelMemberOptions);
    if (memberIds.length === 0) {
      setDialogError(elements.channelFormError, "Choose at least one agent for this channel.");
      return;
    }
    setFormBusy(elements.channelForm, elements.createChannel, true, "Creating…");
    setDialogError(elements.channelFormError);
    try {
      const channel = await client2.createSharedChannel({
        name: elements.channelName.value.trim(),
        members: [
          { agent_id: snapshot.participantId, delivery_mode: "stream_only" },
          ...memberIds.map((agentId) => ({
            agent_id: agentId,
            delivery_mode: "inbox"
          }))
        ]
      });
      elements.channelDialog.close();
      await refreshAndSelectConversation(channel.id);
    } catch (error) {
      setDialogError(elements.channelFormError, operatorErrorMessage(error, "The channel could not be created."));
    } finally {
      setFormBusy(elements.channelForm, elements.createChannel, false, "Create channel");
    }
  }
  async function openDirectMessage(agentId, button) {
    const client2 = operatorClient;
    const snapshot = latestSnapshot;
    if (!client2 || !session || !snapshot)
      return;
    button.disabled = true;
    button.textContent = "Opening…";
    setDialogState("Opening direct message…");
    try {
      const conversation = await client2.openDirectConversation({
        members: [
          { agent_id: snapshot.participantId, delivery_mode: "stream_only" },
          { agent_id: agentId, delivery_mode: "inbox" }
        ]
      });
      elements.agentDirectoryDialog.close();
      await refreshAndSelectConversation(conversation.id);
    } catch (error) {
      setDialogState(operatorErrorMessage(error, "The direct message could not be opened."), "danger");
    } finally {
      button.disabled = false;
      button.textContent = "Message";
    }
  }
  function openConversationDetails() {
    const conversation = selectedConversation();
    const snapshot = latestSnapshot;
    if (!conversation || !snapshot)
      return;
    const direct = conversation.kind === "direct";
    const peer = direct ? conversation.members.find((member) => member.agent_id !== snapshot.participantId) : undefined;
    elements.conversationDetailsKicker.textContent = direct ? "Direct message" : "Channel";
    elements.conversationDetailsTitle.textContent = peer ? displayName(peer.agent_name) : displayName(conversation.name);
    elements.conversationDetailsCopy.textContent = direct ? "A private conversation between two workspace participants." : "Manage the channel name and its members.";
    elements.renameChannelName.value = conversation.name;
    elements.renameChannelForm.hidden = direct;
    elements.addMemberForm.hidden = direct;
    elements.channelDangerZone.hidden = direct;
    elements.conversationMemberCount.textContent = String(conversation.members.length);
    renderConversationMembers(elements.conversationMemberList, conversation.members, snapshot.participantId);
    renderAddMemberOptions(elements.addMemberAgent, agents, conversation.members, snapshot.participantId);
    elements.addMember.disabled = elements.addMemberAgent.disabled;
    setDialogError(elements.conversationDetailsError);
    showDialog(elements.conversationDetailsDialog);
  }
  async function renameSelectedChannel() {
    const client2 = operatorClient;
    const conversation = selectedConversation();
    if (!client2 || conversation?.kind !== "shared")
      return;
    setFormBusy(elements.renameChannelForm, elements.renameChannel, true, "Saving…");
    setDialogError(elements.conversationDetailsError);
    try {
      await client2.renameSharedChannel(conversation.id, {
        name: elements.renameChannelName.value.trim()
      });
      await refreshConversationState();
      openConversationDetails();
    } catch (error) {
      setDialogError(elements.conversationDetailsError, operatorErrorMessage(error, "The channel name could not be saved."));
    } finally {
      setFormBusy(elements.renameChannelForm, elements.renameChannel, false, "Save name");
    }
  }
  async function addSelectedChannelMember() {
    const client2 = operatorClient;
    const conversation = selectedConversation();
    const agentId = elements.addMemberAgent.value;
    if (!client2 || conversation?.kind !== "shared" || !agentId)
      return;
    setFormBusy(elements.addMemberForm, elements.addMember, true, "Adding…");
    setDialogError(elements.conversationDetailsError);
    try {
      await client2.addSharedChannelMember(conversation.id, {
        agent_id: agentId,
        delivery_mode: "inbox"
      });
      await refreshConversationState();
      const configured = seatConfigurations.some((configuration) => configuration.agent_id === agentId);
      if (!configured && runtimeProfiles.length > 0) {
        openAgentSeatConfiguration(agentId);
      } else {
        openConversationDetails();
      }
    } catch (error) {
      setDialogError(elements.conversationDetailsError, operatorErrorMessage(error, "The agent could not be added."));
    } finally {
      setFormBusy(elements.addMemberForm, elements.addMember, false, "Add member");
    }
  }
  function confirmChannelArchive() {
    const conversation = selectedConversation();
    if (conversation?.kind !== "shared")
      return;
    elements.archiveChannelCopy.textContent = `#${displayName(conversation.name)} will leave the sidebar. Its history will not be deleted.`;
    elements.conversationDetailsDialog.close();
    showDialog(elements.archiveChannelDialog);
  }
  async function archiveSelectedChannel() {
    const client2 = operatorClient;
    const activeSession = session;
    const conversation = selectedConversation();
    if (!client2 || !activeSession || conversation?.kind !== "shared")
      return;
    setFormBusy(elements.archiveChannelForm, elements.archiveChannel, true, "Archiving…");
    try {
      await client2.archiveSharedChannel(conversation.id);
      activeSession.clearSelection();
      await refreshConversationState();
      elements.archiveChannelDialog.close();
    } catch (error) {
      elements.archiveChannelCopy.textContent = operatorErrorMessage(error, "The channel could not be archived.");
    } finally {
      setFormBusy(elements.archiveChannelForm, elements.archiveChannel, false, "Archive channel");
    }
  }
  async function refreshAndSelectConversation(channelId) {
    await refreshConversationState();
    await session?.selectChannel(channelId);
  }
  async function refreshConversationState() {
    const activeSession = session;
    const client2 = operatorClient;
    if (!activeSession || !client2)
      return;
    await Promise.all([activeSession.refreshChannels(), refreshWorkspace(client2)]);
  }
  function selectedConversation() {
    const channelId = latestSnapshot?.selectedChannelId;
    return conversations.find((conversation) => conversation.id === channelId);
  }
  function requiredOperatorClient() {
    if (!operatorClient)
      throw new Error("Fleetd workspace is disconnected");
    return operatorClient;
  }
  function showDialog(dialog) {
    if (dialog.open)
      return;
    dialog.showModal();
  }
  function setDialogError(element, message) {
    element.textContent = message ?? "";
    element.hidden = message == null;
  }
  function setDialogState(message, tone) {
    elements.agentDirectoryState.textContent = message;
    elements.agentDirectoryState.hidden = false;
    if (tone)
      elements.agentDirectoryState.dataset.tone = tone;
    else
      delete elements.agentDirectoryState.dataset.tone;
  }
  function setFormBusy(form, button, busy, label) {
    form.setAttribute("aria-busy", String(busy));
    button.disabled = busy;
    button.textContent = label;
  }
  function operatorErrorMessage(error, fallback) {
    if (!(error instanceof FleetdOperatorClientError))
      return fallback;
    if (error.status === 401)
      return "The workspace key is no longer valid. Reconnect to continue.";
    if (error.status === 403)
      return "This action requires workspace owner access.";
    if (error.status === 404)
      return "This conversation no longer exists. Refresh the workspace and try again.";
    if (error.status === 409)
      return "The workspace changed before this action completed. Refresh and try again.";
    return fallback;
  }
  async function sendComposerMessage() {
    const activeSession = session;
    if (!activeSession || !contract || sendInFlight)
      return;
    const draft = elements.composerText.value;
    const text2 = draft.trim();
    const recipientId = currentRecipientId();
    if (!text2)
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
        resetMentionState();
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
      draft: elements.composerText.value,
      pendingSends: snapshot?.pendingSends ?? 0,
      sending: sendInFlight
    });
    applyComposerAvailability(availability, {
      form: elements.composer,
      textarea: elements.composerText,
      send: elements.send
    });
  }
  function renderComposerContext() {
    const snapshot = latestSnapshot;
    const conversation = selectedConversation();
    const direct = conversation?.kind === "direct" ? directRecipient(snapshot?.members ?? [], snapshot?.participantId ?? "") : undefined;
    const selected = mentionSelection && mentionSelectionPresent(elements.composerText.value, mentionSelection) ? mentionSelection : undefined;
    const recipientId = direct?.recipientId ?? selected?.recipientId ?? "";
    const recipientLabel2 = direct?.label ?? selected?.label;
    elements.composerAudience.dataset.recipientId = recipientId;
    elements.clearMention.hidden = direct !== undefined || selected === undefined;
    elements.composerAudienceLabel.textContent = !conversation ? "Select a conversation" : recipientLabel2 ? `Notifying ${recipientLabel2}` : "Notifying everyone";
    elements.composerText.placeholder = !conversation ? "Write a message…" : conversation.kind === "direct" && recipientLabel2 ? `Message ${recipientLabel2}…` : "Message the channel…";
  }
  function currentRecipientId() {
    const snapshot = latestSnapshot;
    const conversation = selectedConversation();
    if (!snapshot || !conversation)
      return null;
    if (conversation.kind === "direct") {
      return directRecipient(snapshot.members, snapshot.participantId)?.recipientId ?? null;
    }
    if (mentionSelection && mentionSelectionPresent(elements.composerText.value, mentionSelection)) {
      return mentionSelection.recipientId;
    }
    return null;
  }
  function refreshMentionSuggestions() {
    const snapshot = latestSnapshot;
    const conversation = selectedConversation();
    if (mentionDismissed || snapshot?.phase !== "live" || conversation?.kind !== "shared" || mentionSelection !== undefined) {
      hideMentionSuggestions();
      return;
    }
    const caret = elements.composerText.selectionStart;
    mentionQuery = mentionQueryAt(elements.composerText.value, caret);
    mentionOptions = mentionQuery ? mentionCandidates(snapshot.members, snapshot.participantId, mentionQuery.text) : [];
    if (!mentionQuery || mentionOptions.length === 0) {
      hideMentionSuggestions();
      return;
    }
    activeMentionIndex = Math.min(activeMentionIndex, mentionOptions.length - 1);
    const options = mentionOptions.map((candidate, index) => {
      const option = document.createElement("button");
      option.type = "button";
      option.className = "mention-option";
      option.id = `mention-option-${index}`;
      option.dataset.recipientId = candidate.recipientId;
      option.setAttribute("role", "option");
      option.setAttribute("aria-selected", String(index === activeMentionIndex));
      option.title = candidate.description;
      const name = document.createElement("span");
      name.className = "mention-option-name";
      name.textContent = `@${candidate.label}`;
      const note = document.createElement("span");
      note.className = "mention-option-note";
      note.textContent = candidate.receivesInboxWork ? "will be notified" : "watching channel";
      option.append(name, note);
      return option;
    });
    elements.mentionSuggestions.replaceChildren(...options);
    elements.mentionSuggestions.hidden = false;
    elements.composerText.setAttribute("aria-expanded", "true");
    elements.composerText.setAttribute("aria-activedescendant", `mention-option-${activeMentionIndex}`);
  }
  function handleMentionKeydown(event) {
    if (elements.mentionSuggestions.hidden || mentionOptions.length === 0) {
      return false;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const direction = event.key === "ArrowDown" ? 1 : -1;
      activeMentionIndex = (activeMentionIndex + direction + mentionOptions.length) % mentionOptions.length;
      refreshMentionSuggestions();
      return true;
    }
    if (event.key === "Enter" || event.key === "Tab") {
      event.preventDefault();
      const candidate = mentionOptions[activeMentionIndex];
      if (candidate)
        selectMention(candidate);
      return true;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      mentionDismissed = true;
      hideMentionSuggestions();
      return true;
    }
    return false;
  }
  function selectMention(candidate) {
    if (!mentionQuery)
      return;
    const applied = applyMention(elements.composerText.value, mentionQuery, candidate);
    mentionSelection = applied.selection;
    elements.composerText.value = applied.draft;
    elements.composerText.setSelectionRange(applied.caret, applied.caret);
    mentionDismissed = false;
    hideMentionSuggestions();
    resizeComposer(elements.composerText);
    renderComposerAvailability();
    renderComposerContext();
    elements.composerText.focus();
  }
  function clearSelectedMention() {
    if (!mentionSelection)
      return;
    const tokenIndex = elements.composerText.value.indexOf(mentionSelection.token);
    if (tokenIndex >= 0) {
      const before = elements.composerText.value.slice(0, tokenIndex);
      const after = elements.composerText.value.slice(tokenIndex + mentionSelection.token.length);
      elements.composerText.value = before.endsWith(" ") && after.startsWith(" ") ? `${before}${after.slice(1)}` : `${before}${after}`;
      const caret = Math.min(tokenIndex, elements.composerText.value.length);
      elements.composerText.setSelectionRange(caret, caret);
    }
    mentionSelection = undefined;
    mentionDismissed = false;
    hideMentionSuggestions();
    resizeComposer(elements.composerText);
    renderComposerAvailability();
    renderComposerContext();
    elements.composerText.focus();
  }
  function hideMentionSuggestions() {
    mentionQuery = undefined;
    mentionOptions = [];
    activeMentionIndex = 0;
    elements.mentionSuggestions.hidden = true;
    elements.mentionSuggestions.replaceChildren();
    elements.composerText.setAttribute("aria-expanded", "false");
    elements.composerText.removeAttribute("aria-activedescendant");
  }
  function resetMentionState() {
    mentionSelection = undefined;
    mentionDismissed = false;
    hideMentionSuggestions();
    renderComposerContext();
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
    if (value.runtimeProfiles !== undefined) {
      if (!Array.isArray(value.runtimeProfiles) || value.runtimeProfiles.length > 128) {
        throw new Error("invalid conversation profile field: runtimeProfiles");
      }
      const ids = new Set;
      for (const profile of value.runtimeProfiles) {
        boundedProfileField(profile.id, "runtimeProfiles.id", 128);
        boundedProfileField(profile.label, "runtimeProfiles.label", 256);
        boundedProfileField(profile.description, "runtimeProfiles.description", 2048);
        if (!/^[A-Za-z0-9._-]+$/u.test(profile.id) || ids.has(profile.id)) {
          throw new Error("invalid conversation profile field: runtimeProfiles.id");
        }
        ids.add(profile.id);
      }
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
    connectSubmitLabel.textContent = busy ? "Connecting…" : "Continue to conversations";
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
