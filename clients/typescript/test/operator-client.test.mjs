import assert from "node:assert/strict";
import test from "node:test";

import {
  createFleetdOperatorClient,
  FleetdOperatorClientError,
} from "../src/operator-client.ts";

const ORIGIN = "http://127.0.0.1:4317";
const CHANNEL_ID = "shared-1";

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function channel(overrides = {}) {
  return {
    archived_at_ms: null,
    created_at_ms: 1_787_000_000_000,
    id: CHANNEL_ID,
    kind: "shared",
    metadata: { retained: true },
    name: "Architecture",
    ...overrides,
  };
}

function member(agentId = "human-1") {
  return {
    agent_id: agentId,
    agent_name: agentId,
    channel_id: CHANNEL_ID,
    delivery_mode: agentId === "human-1" ? "stream_only" : "inbox",
    joined_at_ms: 1_787_000_000_001,
  };
}

test("uses generated operations for the complete collaboration lifecycle", async () => {
  const requests = [];
  let directRequests = 0;
  const fetch = async (input, init) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url);
    const requestText = ["POST", "PUT", "PATCH"].includes(request.method)
      ? await request.clone().text()
      : "";
    const body = requestText.length > 0 ? JSON.parse(requestText) : null;
    requests.push({
      authorization: request.headers.get("authorization"),
      body,
      method: request.method,
      path: url.pathname,
      search: url.search,
    });

    if (url.pathname === "/v1/agents") return json([]);
    if (url.pathname === "/v1/conversations") {
      return json([
        {
          ...channel(),
          latest_message_at_ms: null,
          latest_message_seq: null,
          members: [member(), member("worker-1")],
        },
      ]);
    }
    if (url.pathname === "/v1/channels" && request.method === "POST") {
      return json(channel(), 201);
    }
    if (url.pathname === `/v1/channels/${CHANNEL_ID}`) {
      return json(channel({ name: body.name }));
    }
    if (url.pathname === `/v1/channels/${CHANNEL_ID}/archive`) {
      return json(channel({ archived_at_ms: 1_787_000_000_010 }));
    }
    if (url.pathname === `/v1/channels/${CHANNEL_ID}/members`) {
      return new Response(null, { status: 204 });
    }
    if (url.pathname === "/v1/direct-conversations") {
      directRequests += 1;
      return json(
        {
          ...channel({ id: "direct-1", kind: "direct", name: "" }),
          latest_message_at_ms: null,
          latest_message_seq: null,
          members: [member(), member("worker-1")],
        },
        directRequests === 1 ? 201 : 200,
      );
    }
    if (url.pathname === "/v1/plugin-generations") return json([]);
    if (url.pathname === "/v1/session-bindings") return json([]);
    if (url.pathname === "/v1/agent-seats") return json([]);
    if (url.pathname === "/v1/agent-seat-configurations") return json([]);
    if (url.pathname === "/v1/agents/worker-1/seat-configuration") {
      return json({ agent_id: "worker-1", ...body, revision: 1, created_at_ms: 1, updated_at_ms: 1 });
    }
    if (url.pathname === "/v1/agents/worker-1/seat-restart") {
      return json({
        agent_id: "worker-1",
        profile_id: "opencode-default",
        instructions: "Build with peers.",
        desired_state: "running",
        revision: 2,
        created_at_ms: 1,
        updated_at_ms: 2,
      });
    }
    return json({ error: "not found" }, 404);
  };
  const client = createFleetdOperatorClient({
    origin: ORIGIN,
    operatorCredential: "operator-secret",
    fetch,
  });

  await client.listAgents();
  const conversations = await client.listConversations({
    include_archived: true,
  });
  await client.createSharedChannel({
    members: [
      { agent_id: "human-1", delivery_mode: "stream_only" },
      { agent_id: "worker-1", delivery_mode: "inbox" },
    ],
    name: "Architecture",
  });
  await client.renameSharedChannel(CHANNEL_ID, { name: "Design" });
  await client.archiveSharedChannel(CHANNEL_ID);
  await client.addSharedChannelMember(CHANNEL_ID, {
    agent_id: "reviewer-1",
    delivery_mode: "inbox",
  });
  const direct = await client.openDirectConversation({
    members: [
      { agent_id: "human-1", delivery_mode: "stream_only" },
      { agent_id: "worker-1", delivery_mode: "inbox" },
    ],
  });
  const reopenedDirect = await client.openDirectConversation({
    members: [
      { agent_id: "worker-1", delivery_mode: "inbox" },
      { agent_id: "human-1", delivery_mode: "stream_only" },
    ],
  });
  await client.listPluginGenerations({ agent: "worker-1" });
  await client.listSessionBindings({ agent: "worker-1" });
  await client.listAgentSeats();
  await client.listAgentSeatConfigurations();
  await client.configureAgentSeat("worker-1", {
    profile_id: "opencode-default",
    instructions: "Build with peers.",
    desired_state: "running",
  });
  await client.restartAgentSeat("worker-1");

  assert.equal(conversations[0].kind, "shared");
  assert.equal(direct.kind, "direct");
  assert.equal(reopenedDirect.id, direct.id);
  assert.deepEqual(
    requests.map(({ method, path, search }) => [method, path, search]),
    [
      ["GET", "/v1/agents", ""],
      ["GET", "/v1/conversations", "?include_archived=true"],
      ["POST", "/v1/channels", ""],
      ["PATCH", `/v1/channels/${CHANNEL_ID}`, ""],
      ["POST", `/v1/channels/${CHANNEL_ID}/archive`, ""],
      ["POST", `/v1/channels/${CHANNEL_ID}/members`, ""],
      ["POST", "/v1/direct-conversations", ""],
      ["POST", "/v1/direct-conversations", ""],
      ["GET", "/v1/plugin-generations", "?agent=worker-1"],
      ["GET", "/v1/session-bindings", "?agent=worker-1"],
      ["GET", "/v1/agent-seats", ""],
      ["GET", "/v1/agent-seat-configurations", ""],
      ["PUT", "/v1/agents/worker-1/seat-configuration", ""],
      ["POST", "/v1/agents/worker-1/seat-restart", ""],
    ],
  );
  assert.ok(
    requests.every(
      ({ authorization }) => authorization === "Bearer operator-secret",
    ),
  );
  assert.ok(
    requests.every(({ path, body }) =>
      !`${path}${JSON.stringify(body)}`.includes("operator-secret"),
    ),
  );
  assert.deepEqual(requests[6].body, {
    members: [
      { agent_id: "human-1", delivery_mode: "stream_only" },
      { agent_id: "worker-1", delivery_mode: "inbox" },
    ],
  });
  client.close();
});

test("preserves inspectable public API failures", async () => {
  const client = createFleetdOperatorClient({
    origin: ORIGIN,
    operatorCredential: "operator-secret",
    fetch: async () => json({ error: "name already exists" }, 409),
  });

  await assert.rejects(
    client.renameSharedChannel(CHANNEL_ID, { name: "Occupied" }),
    (error) => {
      assert.ok(error instanceof FleetdOperatorClientError);
      assert.equal(error.status, 409);
      assert.deepEqual(error.body, { error: "name already exists" });
      return true;
    },
  );
  client.close();
});

test("close aborts active requests and rejects later use", async () => {
  let signal;
  const fetch = (input, init) =>
    new Promise((_resolve, reject) => {
      const request = input instanceof Request ? input : new Request(input, init);
      signal = request.signal;
      signal.addEventListener(
        "abort",
        () => reject(new DOMException("aborted", "AbortError")),
        { once: true },
      );
    });
  const client = createFleetdOperatorClient({
    origin: ORIGIN,
    operatorCredential: "operator-secret",
    requestTimeoutMs: 60_000,
    fetch,
  });

  const pending = client.listAgents();
  await new Promise((resolve) => setTimeout(resolve, 0));
  client.close();

  await assert.rejects(pending, (error) => {
    assert.ok(error instanceof FleetdOperatorClientError);
    assert.equal(error.status, null);
    return true;
  });
  assert.equal(signal.aborted, true);
  await assert.rejects(client.listAgents(), /operator client is closed/);
});
