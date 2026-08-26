import { describe, expect, test } from "bun:test";

import type {
  Agent,
  PluginGeneration,
  SessionBinding,
} from "@fleetd/client/types";
import {
  agentDirectoryItem,
  agentDirectoryItems,
} from "../src/ui/workspace-view-models.ts";

const piler: Agent = {
  id: "agent-piler",
  name: "piler",
  metadata: {},
  created_at_ms: 1,
};

describe("workspace agent directory projection", () => {
  test("reports only state supported by public generation and session records", () => {
    const active = agentDirectoryItem(
      piler,
      generation({ health: "active" }),
      binding({ state: "active", active_invocation_id: "invocation-1" }),
    );
    expect(active.health).toBe("working");
    expect(active.status).toBe("Working");

    const sessionOnly = agentDirectoryItem(
      piler,
      generation({ health: "active" }),
      binding({ state: "active", active_invocation_id: null }),
    );
    expect(sessionOnly.status).toBe("Session active");
    expect(sessionOnly.description).toContain("no active invocation recorded");

    const uncertain = agentDirectoryItem(
      piler,
      generation({ health: "active" }),
      binding({ state: "uncertain" }),
    );
    expect(uncertain.health).toBe("stale");
    expect(uncertain.description).toContain("uncertain outcome");

    const absent = agentDirectoryItem(piler);
    expect(absent.health).toBe("unmanaged");
    expect(absent.status).toBe("No worker observed");
  });

  test("uses the latest exact records and omits the connected participant", () => {
    const items = agentDirectoryItems(
      [
        piler,
        { ...piler, id: "human", name: "Nic" },
        { ...piler, id: "agent-weaver", name: "weaver" },
      ],
      [
        generation({ agent_id: "agent-piler", health: "stopped", started_at_ms: 1 }),
        generation({ agent_id: "agent-piler", health: "active", started_at_ms: 2 }),
      ],
      [],
      "human",
    );
    expect(items.map((item) => item.id)).toEqual([
      "agent-piler",
      "agent-weaver",
    ]);
    expect(items[0]?.status).toBe("Worker observed");
  });
});

function generation(
  override: Partial<PluginGeneration> = {},
): PluginGeneration {
  return {
    acp_protocol_version: 1,
    acp_sdk_version: "1",
    agent_capabilities: {},
    agent_id: "agent-piler",
    compatibility_digest: "compat",
    driver_version: "1",
    health: "active",
    heartbeat_interval_ms: 1_000,
    id: "generation-1",
    interfaces: [],
    last_heartbeat_at_ms: 1,
    max_concurrent_turns: 1,
    max_frame_bytes: 1,
    plugin_id: "plugin",
    plugin_name: "OpenCode",
    plugin_version: "1",
    process_id: 1,
    profile_digest: "profile",
    raw_initialize_result: {},
    runtime_executable_digest: "runtime",
    runtime_name: "OpenCode",
    runtime_version: "1",
    started_at_ms: 1,
    state: "active",
    ...override,
  };
}

function binding(override: Partial<SessionBinding> = {}): SessionBinding {
  return {
    additional_directories: [],
    agent_id: "agent-piler",
    binding: {
      binding_generation: 1,
      binding_id: "binding-1",
      owner_epoch: 1,
    },
    compatibility_digest: "compat",
    created_at_ms: 1,
    lane_key: "lane",
    lane_policy: "channel",
    owner_instance_id: "owner",
    profile_digest: "profile",
    state: "ready",
    updated_at_ms: 1,
    working_directory: "/workspace",
    ...override,
  };
}
