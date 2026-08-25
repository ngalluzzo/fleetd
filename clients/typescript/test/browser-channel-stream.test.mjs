import assert from "node:assert/strict";
import test from "node:test";

import {
  BROWSER_CHANNEL_STREAM_PATH,
  BROWSER_CHANNEL_STREAM_PROTOCOL,
  BrowserChannelStreamError,
  openBrowserChannelStream,
} from "../src/browser-channel-stream.ts";

const ORIGIN = "http://127.0.0.1:7419";
const CHANNEL_ID = "channel-1";
const CREDENTIAL = "credential-only-in-memory";

class FakeSocket {
  protocol = "";
  onclose = null;
  onerror = null;
  onmessage = null;
  onopen = null;
  sent = [];
  closes = [];

  constructor(negotiatedProtocol = BROWSER_CHANNEL_STREAM_PROTOCOL) {
    this.negotiatedProtocol = negotiatedProtocol;
    this.emitCloseWhenClientCloses = true;
  }

  open() {
    this.protocol = this.negotiatedProtocol;
    this.onopen?.({});
  }

  message(frame) {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }

  rawMessage(data) {
    this.onmessage?.({ data });
  }

  serverClose() {
    this.onclose?.({ code: 1006 });
  }

  send(data) {
    this.sent.push(data);
  }

  close(code, reason) {
    this.closes.push({ code, reason });
    if (this.emitCloseWhenClientCloses) {
      queueMicrotask(() => this.onclose?.({ code, reason }));
    }
  }
}

class ManualTimeouts {
  entries = [];

  schedule = (callback, milliseconds) => {
    const entry = { callback, milliseconds, cancelled: false };
    this.entries.push(entry);
    return () => {
      entry.cancelled = true;
    };
  };

  fire(index) {
    const entry = this.entries[index];
    assert.ok(entry, `missing timeout ${index}`);
    assert.equal(entry.cancelled, false, `timeout ${index} was cancelled`);
    entry.callback();
  }
}

function createTransport({ mutateGrantResponse } = {}) {
  const requests = [];
  const sockets = [];
  const socketRequests = [];
  let grantIndex = 0;

  const fetch = async (url, init) => {
    requests.push({ url, init });
    const suffix = (++grantIndex).toString(36).padEnd(43, "A");
    const body = {
      expires_at_ms: 1_787_666_400_001,
      grant: `fl_sg_${suffix}`,
      protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
      websocket_path: BROWSER_CHANNEL_STREAM_PATH,
    };
    mutateGrantResponse?.(body);
    return new Response(JSON.stringify(body), {
      status: 201,
      headers: {
        "cache-control": "no-store",
        "content-type": "application/json",
      },
    });
  };

  const createWebSocket = (url, protocol) => {
    socketRequests.push({ url, protocol });
    const socket = new FakeSocket();
    sockets.push(socket);
    return socket;
  };

  return { requests, sockets, socketRequests, fetch, createWebSocket };
}

function message(seq, overrides = {}) {
  return {
    causation_id: null,
    channel_id: CHANNEL_ID,
    correlation_id: null,
    created_at_ms: 1_787_666_400_000 + seq,
    id: `message-${seq}`,
    kind: "opaque.contract/v7",
    payload: { extension: { seq } },
    recipient_id: null,
    sender_id: "sender-1",
    seq,
    ...overrides,
  };
}

function ready(after) {
  return {
    after,
    channel_id: CHANNEL_ID,
    protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
    type: "ready",
  };
}

function messageFrame(value) {
  return { message: value, type: "message" };
}

function requestBody(request) {
  return JSON.parse(request.init.body);
}

async function eventually(predicate, label = "condition") {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.fail(`timed out waiting for ${label}`);
}

function assertStreamError(code) {
  return (error) => {
    assert.ok(error instanceof BrowserChannelStreamError);
    assert.equal(error.code, code);
    return true;
  };
}

test("uses only exact grant POSTs and the fixed secret-free WebSocket", async () => {
  const transport = createTransport();
  const accepted = [];
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept(value) {
      accepted.push(value);
    },
    reconnectDelaysMs: [],
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "first socket");
  assert.equal(transport.requests.length, 1);
  const request = transport.requests[0];
  assert.equal(
    request.url,
    `${ORIGIN}/v1/channels/${CHANNEL_ID}/stream-grants`,
  );
  assert.equal(request.init.method, "POST");
  assert.equal(request.init.headers.Authorization, `Bearer ${CREDENTIAL}`);
  assert.equal(request.init.cache, "no-store");
  assert.equal(request.init.credentials, "omit");
  assert.deepEqual(requestBody(request), {
    after: 0,
    protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
  });

  assert.deepEqual(transport.socketRequests, [
    {
      url: `${ORIGIN.replace("http:", "ws:")}${BROWSER_CHANNEL_STREAM_PATH}`,
      protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
    },
  ]);
  assert.equal(transport.socketRequests[0].url.includes(CREDENTIAL), false);
  assert.equal(
    transport.socketRequests[0].url.includes("fl_sg_"),
    false,
  );

  const socket = transport.sockets[0];
  socket.open();
  assert.equal(socket.sent.length, 1);
  const redemption = JSON.parse(socket.sent[0]);
  assert.equal(redemption.type, "redeem");
  assert.match(redemption.grant, /^fl_sg_[A-Za-z0-9_-]{43}$/);
  assert.equal(socket.sent[0].includes(CREDENTIAL), false);
  socket.message(ready(0));

  stream.close();
  await stream.closed;
  assert.deepEqual(accepted, []);
  assert.ok(
    transport.requests.every(
      ({ url, init }) =>
        init.method === "POST" && url.endsWith("/stream-grants"),
    ),
  );
});

test("reconnects from every accepted cursor and suppresses stable duplicates", async () => {
  const transport = createTransport();
  const accepted = [];
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept(value) {
      accepted.push(value);
    },
    reconnectDelaysMs: [0, 0, 0],
    delay: async () => {},
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  for (let seq = 1; seq <= 4; seq += 1) {
    await eventually(
      () => transport.sockets.length === seq,
      `socket for cursor ${seq - 1}`,
    );
    const socket = transport.sockets[seq - 1];
    socket.open();
    socket.message(ready(seq - 1));
    for (let prior = 1; prior < seq; prior += 1) {
      socket.message(messageFrame(message(prior)));
    }
    socket.message(messageFrame(message(seq)));
    socket.message(messageFrame(message(seq)));
    await eventually(() => stream.cursor === seq, `accepted cursor ${seq}`);
    if (seq < 4) socket.serverClose();
  }

  assert.deepEqual(
    accepted.map(({ seq }) => seq),
    [1, 2, 3, 4],
  );
  assert.deepEqual(
    transport.requests.map(requestBody).map(({ after }) => after),
    [0, 1, 2, 3],
  );
  assert.ok(
    transport.requests.every(
      ({ url, init }) =>
        init.method === "POST" && url.endsWith("/stream-grants"),
    ),
  );

  stream.close();
  await stream.closed;
});

test("serializes acceptance and advances only after consumer acceptance", async () => {
  const transport = createTransport();
  let releaseFirst;
  const firstAccepted = new Promise((resolve) => {
    releaseFirst = resolve;
  });
  const started = [];
  let activeConsumers = 0;
  let maximumConsumers = 0;
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    async accept(value) {
      started.push(value.seq);
      activeConsumers += 1;
      maximumConsumers = Math.max(maximumConsumers, activeConsumers);
      if (value.seq === 1) await firstAccepted;
      activeConsumers -= 1;
    },
    reconnectDelaysMs: [],
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "socket");
  const socket = transport.sockets[0];
  socket.open();
  socket.message(ready(0));
  socket.message(messageFrame(message(1)));
  socket.message(messageFrame(message(2)));

  await eventually(() => started.length === 1, "first consumer");
  assert.equal(stream.cursor, 0);
  assert.deepEqual(started, [1]);
  releaseFirst();
  await eventually(() => stream.cursor === 2, "second acceptance");
  assert.deepEqual(started, [1, 2]);
  assert.equal(maximumConsumers, 1);

  stream.close();
  await stream.closed;
});

test("consumer rejection is terminal and retains the prior cursor", async () => {
  const transport = createTransport();
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept() {
      throw new Error("presentation did not retain the message");
    },
    reconnectDelaysMs: [0, 0],
    delay: async () => {},
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "socket");
  transport.sockets[0].open();
  transport.sockets[0].message(ready(0));
  transport.sockets[0].message(messageFrame(message(1)));

  await assert.rejects(stream.closed, assertStreamError("consumer_rejected"));
  assert.equal(stream.cursor, 0);
  assert.equal(transport.requests.length, 1);
});

test("bounds hung open and hung ready attempts from the unchanged cursor", async () => {
  const transport = createTransport();
  const timeouts = new ManualTimeouts();
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept() {},
    reconnectDelaysMs: [0, 0],
    readyTimeoutMs: 100,
    delay: async () => {},
    scheduleTimeout: timeouts.schedule,
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "hung socket");
  transport.sockets[0].emitCloseWhenClientCloses = false;
  assert.equal(timeouts.entries[0].milliseconds, 100);
  timeouts.fire(0);

  await eventually(() => transport.sockets.length === 2, "retry socket");
  transport.sockets[1].emitCloseWhenClientCloses = false;
  transport.sockets[1].open();
  assert.equal(transport.sockets[1].sent.length, 1);
  timeouts.fire(1);

  await eventually(() => transport.sockets.length === 3, "ready retry socket");
  assert.deepEqual(
    transport.requests.map(requestBody).map(({ after }) => after),
    [0, 0, 0],
  );
  transport.sockets[2].open();
  transport.sockets[2].message(ready(0));
  assert.equal(timeouts.entries[2].cancelled, true);

  stream.close();
  await stream.closed;
});

test("bounds queued frames behind the current acceptance and replays the gap", async () => {
  const transport = createTransport();
  let releaseFirst;
  const firstAccepted = new Promise((resolve) => {
    releaseFirst = resolve;
  });
  const accepted = [];
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    async accept(value) {
      if (value.seq === 1 && accepted.length === 0) await firstAccepted;
      accepted.push(value.seq);
    },
    maxPendingMessages: 1,
    reconnectDelaysMs: [0],
    delay: async () => {},
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "first socket");
  const first = transport.sockets[0];
  first.open();
  first.message(ready(0));
  first.message(messageFrame(message(1)));
  first.message(messageFrame(message(2)));
  first.message(messageFrame(message(3)));
  assert.equal(
    first.closes.some(({ reason }) => reason === "fleetd_client_backpressure"),
    true,
  );
  assert.equal(stream.cursor, 0);

  releaseFirst();
  await eventually(() => transport.sockets.length === 2, "replay socket");
  assert.equal(requestBody(transport.requests[1]).after, 1);
  const second = transport.sockets[1];
  second.open();
  second.message(ready(1));
  second.message(messageFrame(message(2)));
  second.message(messageFrame(message(3)));
  await eventually(() => stream.cursor === 3, "replayed gap");
  assert.deepEqual(accepted, [1, 2, 3]);

  stream.close();
  await stream.closed;
});

test("preserves opaque kind and payload while rejecting envelope drift", async () => {
  const transport = createTransport();
  const accepted = [];
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept(value) {
      accepted.push(value);
    },
    reconnectDelaysMs: [],
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "socket");
  const socket = transport.sockets[0];
  socket.open();
  socket.message(ready(0));
  const opaque = message(1, {
    kind: "future.unknown/v99",
    payload: {
      nested: [null, true, 42, { untouched: "yes" }],
    },
  });
  socket.message(messageFrame(opaque));
  await eventually(() => stream.cursor === 1, "opaque message");
  assert.deepEqual(accepted, [opaque]);

  socket.message(
    messageFrame({ ...message(2), unexpected_envelope_extension: true }),
  );
  await assert.rejects(
    stream.closed,
    assertStreamError("server_protocol_error"),
  );
  assert.equal(stream.cursor, 1);
});

test("rejects conflicting stable identities instead of redelivering", async () => {
  const transport = createTransport();
  const accepted = [];
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept(value) {
      accepted.push(value.id);
    },
    reconnectDelaysMs: [],
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await eventually(() => transport.sockets.length === 1, "socket");
  const socket = transport.sockets[0];
  socket.open();
  socket.message(ready(0));
  socket.message(messageFrame(message(1)));
  await eventually(() => stream.cursor === 1, "first message");
  socket.message(messageFrame(message(1, { id: "different-id" })));

  await assert.rejects(
    stream.closed,
    assertStreamError("server_protocol_error"),
  );
  assert.deepEqual(accepted, ["message-1"]);
});

test("grant linkage failure is terminal and never falls back to history polling", async () => {
  const transport = createTransport({
    mutateGrantResponse(response) {
      response.websocket_path = "/v1/channels/channel-1/messages";
    },
  });
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept() {},
    reconnectDelaysMs: [0, 0],
    delay: async () => {},
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  await assert.rejects(
    stream.closed,
    assertStreamError("grant_linkage_mismatch"),
  );
  assert.equal(transport.requests.length, 1);
  assert.equal(transport.requests[0].init.method, "POST");
  assert.equal(transport.requests[0].url.endsWith("/stream-grants"), true);
  assert.equal(transport.sockets.length, 0);
});

test("reconnect exhaustion is finite and performs no polling fallback", async () => {
  const transport = createTransport();
  const delays = [];
  const stream = openBrowserChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept() {},
    reconnectDelaysMs: [0, 0],
    delay: async (milliseconds) => {
      delays.push(milliseconds);
    },
    fetch: transport.fetch,
    createWebSocket: transport.createWebSocket,
  });

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    await eventually(() => transport.sockets.length === attempt, `socket ${attempt}`);
    transport.sockets[attempt - 1].serverClose();
  }

  await assert.rejects(
    stream.closed,
    assertStreamError("reconnect_exhausted"),
  );
  assert.deepEqual(delays, [0, 0]);
  assert.equal(transport.requests.length, 3);
  assert.ok(
    transport.requests.every(
      ({ url, init }) =>
        init.method === "POST" && url.endsWith("/stream-grants"),
    ),
  );
});
