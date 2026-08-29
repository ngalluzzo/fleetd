import assert from "node:assert/strict";
import test from "node:test";

import { ConversationSession } from "../src/conversation-session.ts";
import { createNativeConversationTransport } from "../src/native-conversation-transport.ts";

const ORIGIN = "http://127.0.0.1:7419";
const CHANNEL_ID = "channel-1";
const PARTICIPANT_ID = "participant-1";
const OPERATOR_CREDENTIAL = "operator-only-in-memory";
const PARTICIPANT_CREDENTIAL = "participant-only-in-memory";

class FakeSocket {
  onclose = null;
  onerror = null;
  onmessage = null;
  onopen = null;
  onunexpectedresponse = null;

  open() {
    this.onopen?.({});
  }

  message(value) {
    this.onmessage?.({ data: JSON.stringify(value), isBinary: false });
  }

  close(code, reason) {
    queueMicrotask(() => this.onclose?.({ code, reason }));
  }
}

function immutableMessage(seq, overrides = {}) {
  return {
    causation_id: null,
    channel_id: CHANNEL_ID,
    correlation_id: null,
    created_at_ms: 1_787_666_400_000 + seq,
    id: `message-${seq}`,
    kind: "opaque.contract/v7",
    payload: { extension: { seq } },
    recipient_id: null,
    sender_id: "author-1",
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

test("composes native authority and streaming through ConversationSession", async () => {
  const requests = [];
  const socketRequests = [];
  const sockets = [];
  const attention = {
    addressed_unread_count: 0,
    channel_id: CHANNEL_ID,
    first_addressed_unread_seq: null,
    first_unread_seq: 1,
    latest_message_seq: 1,
    read_through_seq: 0,
    unread_count: 1,
  };
  const fetch = async (input, init) => {
    const url = new URL(input);
    requests.push({ url, init });
    if (url.pathname === "/v1/channels" && init.method === "GET") {
      return Response.json([
        {
          archived_at_ms: null,
          created_at_ms: 1,
          id: CHANNEL_ID,
          kind: "shared",
          metadata: {},
          name: "native",
        },
      ]);
    }
    if (url.pathname.endsWith("/members") && init.method === "GET") {
      return Response.json([
        {
          agent_id: PARTICIPANT_ID,
          agent_name: "participant",
          channel_id: CHANNEL_ID,
          delivery_mode: "stream_only",
          joined_at_ms: 1,
        },
      ]);
    }
    if (url.pathname === "/v1/conversations/attention") {
      return Response.json([attention]);
    }
    if (url.pathname.endsWith("/read-cursor") && init.method === "PUT") {
      const { through_seq } = JSON.parse(init.body);
      return Response.json({
        ...attention,
        first_unread_seq: null,
        read_through_seq: through_seq,
        unread_count: 0,
      });
    }
    throw new Error(`unexpected request ${init.method} ${url.pathname}`);
  };
  const transport = createNativeConversationTransport({
    origin: ORIGIN,
    participantId: PARTICIPANT_ID,
    operatorCredential: OPERATOR_CREDENTIAL,
    participantCredential: PARTICIPANT_CREDENTIAL,
    fetch,
    reconnectDelaysMs: [],
    createWebSocket(url, request) {
      socketRequests.push({ url, request });
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const conversation = new ConversationSession(transport);

  await conversation.start();
  const selection = conversation.selectChannel(CHANNEL_ID);
  sockets[0].open();
  await selection;
  sockets[0].message(immutableMessage(1));
  await eventually(() => conversation.snapshot.cursor === 1, "session cursor");
  assert.equal(conversation.snapshot.phase, "live");
  assert.deepEqual(conversation.snapshot.messages, [immutableMessage(1)]);

  await conversation.markRead(CHANNEL_ID, 1);
  assert.equal(conversation.snapshot.attention[0].read_through_seq, 1);
  assert.equal(conversation.snapshot.attention[0].unread_count, 0);

  const channelRequest = requests.find(
    ({ url, init }) => url.pathname === "/v1/channels" && init.method === "GET",
  );
  assert.equal(
    channelRequest.init.headers.Authorization,
    `Bearer ${OPERATOR_CREDENTIAL}`,
  );
  for (const request of requests.filter(
    ({ url }) => url.pathname !== "/v1/channels",
  )) {
    assert.equal(
      request.init.headers.Authorization,
      `Bearer ${PARTICIPANT_CREDENTIAL}`,
    );
  }
  assert.equal(socketRequests.length, 1);
  assert.equal(socketRequests[0].url.includes(PARTICIPANT_CREDENTIAL), false);
  assert.deepEqual(socketRequests[0].request.headers, {
    Authorization: `Bearer ${PARTICIPANT_CREDENTIAL}`,
  });

  conversation.close();
  assert.equal(conversation.snapshot.phase, "closed");
});

test("closing the native transport aborts requests and rejects later use", async () => {
  let requestSignal;
  const transport = createNativeConversationTransport({
    origin: ORIGIN,
    participantId: PARTICIPANT_ID,
    operatorCredential: OPERATOR_CREDENTIAL,
    participantCredential: PARTICIPANT_CREDENTIAL,
    fetch(_input, init) {
      requestSignal = init.signal;
      return new Promise((_resolve, reject) => {
        init.signal.addEventListener(
          "abort",
          () => reject(new DOMException("aborted", "AbortError")),
          { once: true },
        );
      });
    },
  });

  const pending = transport.listChannels();
  await eventually(() => requestSignal !== undefined, "active request");
  transport.close();
  assert.equal(requestSignal.aborted, true);
  assert.throws(() => transport.openStream({
    channelId: CHANNEL_ID,
    after: 0,
    accept() {},
  }), /closed/);
  void pending.catch(() => {});
});
