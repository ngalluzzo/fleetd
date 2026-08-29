import assert from "node:assert/strict";
import test from "node:test";

import {
  NativeChannelStreamError,
  openNativeChannelStream,
} from "../src/native-channel-stream.ts";

const ORIGIN = "http://127.0.0.1:7419";
const CHANNEL_ID = "channel-1";
const CREDENTIAL = "credential-only-in-memory";

class FakeSocket {
  onclose = null;
  onerror = null;
  onmessage = null;
  onopen = null;
  onunexpectedresponse = null;
  closes = [];
  emitCloseWhenClientCloses = true;

  open() {
    this.onopen?.({});
  }

  message(value, { asBuffer = false, isBinary = false } = {}) {
    const text = typeof value === "string" ? value : JSON.stringify(value);
    this.onmessage?.({
      data: asBuffer ? Buffer.from(text) : text,
      isBinary,
    });
  }

  serverClose(code = 1006) {
    this.onclose?.({ code });
  }

  unexpected(status) {
    this.onunexpectedresponse?.({ status });
  }

  close(code, reason) {
    this.closes.push({ code, reason });
    if (this.emitCloseWhenClientCloses) {
      queueMicrotask(() => this.onclose?.({ code, reason }));
    }
  }
}

function createTransport() {
  const requests = [];
  const sockets = [];
  return {
    requests,
    sockets,
    createWebSocket(url, request) {
      requests.push({ url, request });
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  };
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

async function eventually(predicate, label = "condition") {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.fail(`timed out waiting for ${label}`);
}

function assertStreamError(code, status) {
  return (error) => {
    assert.ok(error instanceof NativeChannelStreamError);
    assert.equal(error.code, code);
    if (status !== undefined) assert.equal(error.status, status);
    return true;
  };
}

test("uses the native stream route with header-only bearer authority", async () => {
  const transport = createTransport();
  const accepted = [];
  const statuses = [];
  const stream = openNativeChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    after: 7,
    accept(value) {
      accepted.push(value);
    },
    statusChanged(status) {
      statuses.push(status);
    },
    reconnectDelaysMs: [],
    createWebSocket: transport.createWebSocket,
  });

  assert.equal(transport.requests.length, 1);
  assert.equal(
    transport.requests[0].url,
    `${ORIGIN.replace("http", "ws")}/v1/channels/${CHANNEL_ID}/stream?after=7`,
  );
  assert.equal(transport.requests[0].url.includes(CREDENTIAL), false);
  assert.deepEqual(transport.requests[0].request.headers, {
    Authorization: `Bearer ${CREDENTIAL}`,
  });

  transport.sockets[0].open();
  transport.sockets[0].message(message(8), { asBuffer: true });
  await eventually(() => stream.cursor === 8, "accepted native frame");
  assert.deepEqual(accepted, [message(8)]);
  assert.deepEqual(statuses, ["connecting", "live"]);

  stream.close();
  await stream.closed;
  assert.equal(stream.status, "closed");
  assert.equal(JSON.stringify(transport.sockets[0].closes).includes(CREDENTIAL), false);
});

test("reconnects from accepted cursors and suppresses stable duplicates", async () => {
  const transport = createTransport();
  const accepted = [];
  const stream = openNativeChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept(value) {
      accepted.push(value.seq);
    },
    reconnectDelaysMs: [0, 0],
    delay: async () => {},
    createWebSocket: transport.createWebSocket,
  });

  transport.sockets[0].open();
  transport.sockets[0].message(message(1));
  await eventually(() => stream.cursor === 1, "first cursor");
  transport.sockets[0].serverClose();
  await eventually(() => transport.sockets.length === 2, "reconnect socket");
  assert.equal(new URL(transport.requests[1].url).searchParams.get("after"), "1");

  transport.sockets[1].open();
  transport.sockets[1].message(message(1));
  transport.sockets[1].message(message(2));
  await eventually(() => stream.cursor === 2, "replayed cursor");
  assert.deepEqual(accepted, [1, 2]);

  stream.close();
  await stream.closed;
});

test("advances only after serialized consumer acceptance", async () => {
  const transport = createTransport();
  const releases = [];
  const accepted = [];
  const stream = openNativeChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept(value) {
      accepted.push(value.seq);
      return new Promise((resolve) => releases.push(resolve));
    },
    reconnectDelaysMs: [],
    createWebSocket: transport.createWebSocket,
  });

  transport.sockets[0].open();
  transport.sockets[0].message(message(1));
  transport.sockets[0].message(message(2));
  await eventually(() => releases.length === 1, "first acceptance");
  assert.equal(stream.cursor, 0);
  assert.deepEqual(accepted, [1]);

  releases.shift()();
  await eventually(() => releases.length === 1, "second acceptance");
  assert.equal(stream.cursor, 1);
  assert.deepEqual(accepted, [1, 2]);
  releases.shift()();
  await eventually(() => stream.cursor === 2, "second cursor");

  stream.close();
  await stream.closed;
});

test("fails closed on consumer rejection without advancing", async () => {
  const transport = createTransport();
  const refusal = new Error("persistence failed");
  const stream = openNativeChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept() {
      throw refusal;
    },
    reconnectDelaysMs: [0],
    createWebSocket: transport.createWebSocket,
  });

  transport.sockets[0].open();
  transport.sockets[0].message(message(1));
  await assert.rejects(
    stream.closed,
    (error) =>
      assertStreamError("consumer_rejected")(error) && error.cause === refusal,
  );
  assert.equal(stream.cursor, 0);
  assert.equal(transport.sockets.length, 1);
});

test("surfaces rejected upgrades without exposing the credential", async () => {
  const transport = createTransport();
  const stream = openNativeChannelStream({
    origin: ORIGIN,
    channelId: CHANNEL_ID,
    credential: CREDENTIAL,
    accept() {},
    reconnectDelaysMs: [0],
    createWebSocket: transport.createWebSocket,
  });

  transport.sockets[0].unexpected(403);
  await assert.rejects(stream.closed, assertStreamError("upgrade_rejected", 403));
  assert.equal(stream.status, "failed");
  await assert.rejects(stream.closed, (error) => {
    assert.equal(String(error).includes(CREDENTIAL), false);
    return true;
  });
});

test("rejects binary and drifting envelopes as protocol failures", async () => {
  for (const emit of [
    (socket) => socket.message(message(1), { isBinary: true }),
    (socket) => socket.message(message(1, { channel_id: "another-channel" })),
  ]) {
    const transport = createTransport();
    const stream = openNativeChannelStream({
      origin: ORIGIN,
      channelId: CHANNEL_ID,
      credential: CREDENTIAL,
      accept() {},
      reconnectDelaysMs: [],
      createWebSocket: transport.createWebSocket,
    });
    transport.sockets[0].open();
    emit(transport.sockets[0]);
    await assert.rejects(
      stream.closed,
      assertStreamError("server_protocol_error"),
    );
  }
});
