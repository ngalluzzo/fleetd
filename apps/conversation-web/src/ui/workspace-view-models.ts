import type {
  Agent,
  PluginGeneration,
  SessionBinding,
} from "@fleetd/client/types";
import { displayName, shortId } from "./view-models.ts";

export type AgentHealth =
  | "working"
  | "active"
  | "stale"
  | "stopped"
  | "unmanaged";

export interface AgentDirectoryItem {
  readonly id: string;
  readonly name: string;
  readonly exactName: string;
  readonly initials: string;
  readonly health: AgentHealth;
  readonly status: string;
  readonly description: string;
}

export function agentDirectoryItems(
  agents: readonly Agent[],
  generations: readonly PluginGeneration[],
  bindings: readonly SessionBinding[],
  participantId: string,
): readonly AgentDirectoryItem[] {
  const latestGenerations = latestByAgent(
    generations,
    (generation) => generation.agent_id,
    (generation) => generation.started_at_ms,
  );
  const latestBindings = latestByAgent(
    bindings,
    (binding) => binding.agent_id,
    (binding) => binding.updated_at_ms,
  );

  return agents
    .filter((agent) => agent.id !== participantId)
    .map((agent) =>
      agentDirectoryItem(
        agent,
        latestGenerations.get(agent.id),
        latestBindings.get(agent.id),
      ),
    )
    .sort((left, right) => {
      const healthDifference = healthOrder(left.health) - healthOrder(right.health);
      return healthDifference || left.name.localeCompare(right.name);
    });
}

export function agentDirectoryItem(
  agent: Agent,
  generation?: PluginGeneration,
  binding?: SessionBinding,
): AgentDirectoryItem {
  const name = displayName(agent.name);
  const identity = `${agent.name} · ${agent.id}`;

  if (
    binding?.state === "active" &&
    binding.active_invocation_id != null &&
    generation?.health === "active"
  ) {
    return {
      id: agent.id,
      name,
      exactName: identity,
      initials: initials(name),
      health: "working",
      status: "Working",
      description: `${generation.runtime_name} has an active invocation.`,
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
      description: hasActiveSession
        ? `${generation.runtime_name} owns an active session with no active invocation recorded.`
        : `Fleetd observed an active ${generation.runtime_name} plugin generation.`,
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
    };
  }
  return {
    id: agent.id,
    name,
    exactName: identity,
    initials: initials(name),
    health: "unmanaged",
    status: "No worker observed",
    description: `Registered participant ${shortId(agent.id)} has no observed worker.`,
  };
}

function latestByAgent<T>(
  values: readonly T[],
  agentId: (value: T) => string,
  timestamp: (value: T) => number,
): Map<string, T> {
  const latest = new Map<string, T>();
  for (const value of values) {
    const key = agentId(value);
    const current = latest.get(key);
    if (!current || timestamp(value) > timestamp(current)) latest.set(key, value);
  }
  return latest;
}

function healthOrder(health: AgentHealth): number {
  return {
    working: 0,
    active: 1,
    stale: 2,
    stopped: 3,
    unmanaged: 4,
  }[health];
}

function initials(name: string): string {
  return (
    name
      .split(/\s+/)
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part[0]?.toLocaleUpperCase() ?? "")
      .join("") || "·"
  );
}
