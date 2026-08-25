import assert from "node:assert/strict";
import test from "node:test";

import { BROWSER_CHANNEL_STREAM_PROTOCOL } from "../src/browser-channel-stream.ts";
import { ConversationSession } from "../src/conversation-session.ts";
import { createBrowserConversationTransport } from "../src/conversation-transport.ts";

const PARTICIPANT_ID = "human-1";

function channel(id) {
  return {
    id,
    name: `channel ${id}`,
    metadata: { opaque: id },
    created_at_ms: 1_787_000_000_000,
  };
}

function member(channelId, agentId = PARTICIPANT_ID) {
  return {
    agent_id: agentId,
    agent_name: agentId,
    channel_id: channelId,
    delivery_mode: agentId === PARTICIPANT_ID ? "stream_only" : "inbox",
    joined_at_ms: 1_787_000_000_001,
  };
}

function message(seq, channelId = "alpha", overrides = {}) {
  return {
    seq,
    id: `message-${seq}`,
    channel_id: channelId,
    sender_id: seq % 2 === 0 ? PARTICIPANT_ID : "worker-1",
    recipient_id: seq % 2 === 0 ? "worker-1" : PARTICIPANT_ID,
    kind: "opaque.future/v9",
    payload: { nested: [null, true, { seq }] },
    correlation_id: "conversation-1",
    causation_id: seq === 1 ? null : `message-${seq - 1}`,
    created_at_ms: 1_787_000_000_000 + seq,
    ...overrides,
  };
}

class FakeStream {
  cursor;
  status = "connecting";

  constructor(options) {
    this.options = options;
    this.cursor = options.after;
    this.closed = new Promise((resolve, reject) => {
      this.resolveClosed = resolve;
      this.rejectClosed = reject;
    });
    options.statusChanged?.("connecting");
  }

  setStatus(status) {
    this.status = status;
    this.options.statusChanged?.(status);
  }

  async emit(value) {
    await this.options.accept(value);
    this.cursor = value.seq;
  }

  fail() {
    this.setStatus("failed");
    this.rejectClosed(new Error("fixed fake failure"));
  }

  close() {
    if (this.status === "closed") return;
    this.setStatus("closed");
    this.resolveClosed();
  }
}

class FakeTransport {
  participantId = PARTICIPANT_ID;
  channels = [channel("alpha"), channel("beta")];
  members = new Map([
    ["alpha", [member("alpha"), member("alpha", "worker-1")]],
    ["beta", [member("beta"), member("beta", "worker-2")]],
  ]);
  streams = [];
  sends = [];
  closed = false;

  async listChannels() {
    return this.channels;
  }

  async listMembers(channelId) {
    return this.members.get(channelId) ?? [];
  }

  openStream(options) {
    const stream = new FakeStream(options);
    this.streams.push(stream);
    return stream;
  }

  async send(channelId, body) {
    this.sends.push({ channelId, body });
    return message(2, channelId, {
      kind: body.kind,
      payload: body.payload,
      recipient_id: body.recipient_id,
      correlation_id: body.correlation_id,
      causation_id: body.causation_id,
    });
  }

  close() {
    this.closed = true;
    for (const stream of this.streams) stream.close();
  }
}

test("projects replay and converges an attributed send with its stream echo", async () => {
  const transport = new FakeTransport();
  const session = new ConversationSession(transport);
  const phases = [];
  session.subscribe((snapshot) => phases.push(snapshot.phase));

  await session.start();
  const selected = session.selectChannel("alpha");
  const stream = transport.streams[0];
  stream.setStatus("live");
  await selected;
  assert.equal(session.snapshot.phase, "live");
  assert.deepEqual(session.snapshot.members, transport.members.get("alpha"));

  const replay = message(1);
  await stream.emit(replay);
  assert.equal(session.snapshot.cursor, 1);
  assert.deepEqual(session.snapshot.messages, [replay]);

  const sent = await session.send({
    idempotency_key: "desktop/turn-1",
    recipient_id: "worker-1",
    kind: "conversation.prompt/test-v1",
    payload: { text: "hello", extension: { untouched: true } },
    correlation_id: "conversation-1",
    causation_id: null,
  });
  assert.equal(
    session.snapshot.cursor,
    1,
    "send replies are not replay cursors",
  );
  assert.deepEqual(session.snapshot.messages, [replay, sent]);
  assert.equal(transport.sends[0].body.payload.extension.untouched, true);

  await stream.emit({
    ...sent,
    payload: { extension: { untouched: true }, text: "hello" },
  });
  assert.equal(session.snapshot.cursor, 2);
  assert.equal(session.snapshot.messages.length, 2);
  assert.ok(phases.includes("connecting"));
  assert.ok(phases.includes("live"));

  session.close();
  assert.equal(session.snapshot.phase, "closed");
  assert.equal(transport.closed, true);
});

test("fences abandoned channel selection and resumes each retained cursor", async () => {
  const transport = new FakeTransport();
  const memberResolvers = new Map();
  transport.listMembers = (channelId) =>
    new Promise((resolve) => memberResolvers.set(channelId, resolve));
  const session = new ConversationSession(transport);
  await session.start();

  const alphaSelection = session.selectChannel("alpha");
  const alphaStream = transport.streams[0];
  const betaSelection = session.selectChannel("beta");
  const betaStream = transport.streams[1];
  memberResolvers.get("alpha")(transport.members.get("alpha"));
  memberResolvers.get("beta")(transport.members.get("beta"));
  betaStream.setStatus("live");
  await Promise.all([alphaSelection, betaSelection]);

  await alphaStream.emit(message(1, "alpha"));
  await betaStream.emit(message(3, "beta"));
  assert.equal(session.snapshot.selectedChannelId, "beta");
  assert.deepEqual(
    session.snapshot.messages.map(({ seq }) => seq),
    [3],
  );

  const reselect = session.selectChannel("alpha");
  const resumedAlpha = transport.streams[2];
  assert.equal(
    resumedAlpha.options.after,
    0,
    "abandoned messages were ignored",
  );
  memberResolvers.get("alpha")(transport.members.get("alpha"));
  resumedAlpha.setStatus("live");
  await reselect;
  await resumedAlpha.emit(message(5, "alpha"));

  const backToBeta = session.selectChannel("beta");
  const resumedBeta = transport.streams[3];
  assert.equal(resumedBeta.options.after, 3);
  memberResolvers.get("beta")(transport.members.get("beta"));
  resumedBeta.setStatus("live");
  await backToBeta;
  assert.deepEqual(
    session.snapshot.messages.map(({ seq }) => seq),
    [3],
  );
  session.close();
});

test("fails closed on stable identity conflicts and wrong participant lanes", async () => {
  const transport = new FakeTransport();
  const session = new ConversationSession(transport);
  await session.start();
  const selected = session.selectChannel("alpha");
  const stream = transport.streams[0];
  stream.setStatus("live");
  await selected;
  await stream.emit(message(1));

  await assert.rejects(
    stream.emit(message(1, "alpha", { id: "different-id" })),
    /stable message identity/,
  );
  assert.equal(session.snapshot.phase, "failed");
  assert.equal(session.snapshot.error.code, "message_conflict");

  const otherTransport = new FakeTransport();
  otherTransport.send = async () =>
    message(2, "beta", { sender_id: "someone-else" });
  const other = new ConversationSession(otherTransport);
  await other.start();
  const otherSelection = other.selectChannel("alpha");
  otherTransport.streams[0].setStatus("live");
  await otherSelection;
  await assert.rejects(
    other.send({ kind: "x", payload: null }),
    /Fleetd message send failed/,
  );
  assert.equal(other.snapshot.error.code, "message_conflict");
  other.close();
  session.close();
});

test("distinguishes stream failure from invalid participant membership", async () => {
  const failedTransport = new FakeTransport();
  const failed = new ConversationSession(failedTransport);
  await failed.start();
  const failedSelection = failed.selectChannel("alpha");
  failedTransport.streams[0].fail();
  await assert.rejects(failedSelection, /could not be opened/);
  assert.equal(failed.snapshot.error.code, "stream_failed");
  failed.close();

  const nonMemberTransport = new FakeTransport();
  nonMemberTransport.members.set("alpha", [member("alpha", "worker-1")]);
  const nonMember = new ConversationSession(nonMemberTransport);
  await nonMember.start();
  const nonMemberSelection = nonMember.selectChannel("alpha");
  nonMemberTransport.streams[0].setStatus("live");
  await assert.rejects(nonMemberSelection, /could not be opened/);
  assert.equal(nonMember.snapshot.error.code, "participant_not_member");
  nonMember.close();
});

test("bounds retained presentation messages without changing the replay cursor", async () => {
  const transport = new FakeTransport();
  const session = new ConversationSession(transport, {
    maxRetainedMessages: 16,
  });
  await session.start();
  const selected = session.selectChannel("alpha");
  const stream = transport.streams[0];
  stream.setStatus("live");
  await selected;
  for (let seq = 1; seq <= 20; seq += 1) {
    await stream.emit(message(seq));
  }
  assert.equal(session.snapshot.cursor, 20);
  assert.deepEqual(
    session.snapshot.messages.map(({ seq }) => seq),
    Array.from({ length: 16 }, (_, index) => index + 5),
  );
  session.close();
});

class FakeSocket {
  protocol = "";
  onclose = null;
  onerror = null;
  onmessage = null;
  onopen = null;

  open() {
    this.protocol = BROWSER_CHANNEL_STREAM_PROTOCOL;
    this.onopen?.({});
  }

  message(value) {
    this.onmessage?.({ data: JSON.stringify(value) });
  }

  send() {}

  close(code, reason) {
    queueMicrotask(() => this.onclose?.({ code, reason }));
  }
}

test("browser transport keeps discovery authority separate from participant operations", async () => {
  const requests = [];
  const sockets = [];
  const fetch = async (input, init) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const body =
      request.method === "POST" ? await request.clone().json() : null;
    requests.push({
      path: new URL(request.url).pathname,
      method: request.method,
      authorization: request.headers.get("authorization"),
      body,
    });
    const path = new URL(request.url).pathname;
    if (path === "/v1/channels") return json([channel("alpha")]);
    if (path.endsWith("/members")) {
      return json([member("alpha"), member("alpha", "worker-1")]);
    }
    if (path.endsWith("/messages")) return json(message(1), 201);
    if (path.endsWith("/stream-grants")) {
      return json(
        {
          expires_at_ms: 1_787_000_015_000,
          grant: `fl_sg_${"A".repeat(43)}`,
          protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
          websocket_path: "/v1/browser/channel-stream",
        },
        201,
        { "cache-control": "no-store" },
      );
    }
    return json({ error: "not found" }, 404);
  };
  const transport = createBrowserConversationTransport({
    origin: "http://127.0.0.1:4317",
    participantId: PARTICIPANT_ID,
    operatorCredential: "operator-secret",
    participantCredential: "participant-secret",
    reconnectDelaysMs: [],
    fetch,
    createWebSocket() {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });

  await transport.listChannels();
  await transport.listMembers("alpha");
  await transport.send("alpha", { kind: "opaque", payload: null });
  const stream = transport.openStream({
    channelId: "alpha",
    after: 0,
    accept() {},
  });
  await eventually(() => sockets.length === 1, "browser stream socket");
  sockets[0].open();
  sockets[0].message({
    type: "ready",
    protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
    channel_id: "alpha",
    after: 0,
  });
  await eventually(() => stream.status === "live", "live browser transport");

  assert.deepEqual(
    requests.map(({ path, authorization }) => [path, authorization]),
    [
      ["/v1/channels", "Bearer operator-secret"],
      ["/v1/channels/alpha/members", "Bearer participant-secret"],
      ["/v1/channels/alpha/messages", "Bearer participant-secret"],
      ["/v1/channels/alpha/stream-grants", "Bearer participant-secret"],
    ],
  );
  assert.ok(requests.every(({ path }) => !path.includes("operator-secret")));
  transport.close();
  await stream.closed;
  await assert.rejects(transport.listChannels(), /transport is closed/);
});

test("closing browser transport aborts bounded in-flight authority requests", async () => {
  let observedSignal;
  const fetch = (_input, init) =>
    new Promise((_resolve, reject) => {
      observedSignal = init.signal;
      observedSignal.addEventListener(
        "abort",
        () => reject(new DOMException("aborted", "AbortError")),
        { once: true },
      );
    });
  const transport = createBrowserConversationTransport({
    origin: "http://127.0.0.1:4317",
    participantId: PARTICIPANT_ID,
    operatorCredential: "operator-secret",
    participantCredential: "participant-secret",
    requestTimeoutMs: 60_000,
    fetch,
  });

  const pending = transport.listChannels();
  await eventually(() => observedSignal !== undefined, "request signal");
  assert.equal(observedSignal.aborted, false);
  transport.close();
  await assert.rejects(pending, { name: "AbortError" });
  assert.equal(observedSignal.aborted, true);
});

function json(value, status = 200, headers = {}) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

async function eventually(predicate, label) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.fail(`timed out waiting for ${label}`);
}
