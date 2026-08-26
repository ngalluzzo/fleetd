import type {
  Agent,
  ChannelMember,
} from "@fleetd/client/types";
import { displayName, shortId } from "./view-models.ts";
import type { AgentDirectoryItem } from "./workspace-view-models.ts";

export function renderAgentDirectory(
  container: HTMLElement,
  items: readonly AgentDirectoryItem[],
): void {
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

    const message = document.createElement("button");
    message.type = "button";
    message.className = "button";
    message.dataset.directAgentId = item.id;
    message.textContent = "Message";
    message.setAttribute("aria-label", `Message ${item.name}`);
    article.append(avatar, content, message);
    return article;
  });
  container.replaceChildren(...rows);
}

export function renderChannelMemberOptions(
  container: HTMLElement,
  agents: readonly Agent[],
  participantId: string,
): void {
  const options = agents
    .filter((agent) => agent.id !== participantId)
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((agent) => {
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

export function selectedMemberIds(container: HTMLElement): readonly string[] {
  return Array.from(
    container.querySelectorAll<HTMLInputElement>('input[name="member"]:checked'),
    (input) => input.value,
  );
}

export function renderConversationMembers(
  container: HTMLElement,
  members: readonly ChannelMember[],
  participantId: string,
): void {
  const rows = members.map((member) => {
    const row = document.createElement("div");
    row.className = "member-row";
    row.dataset.agentId = member.agent_id;
    const avatar = document.createElement("span");
    avatar.className = "member-avatar";
    avatar.setAttribute("aria-hidden", "true");
    avatar.textContent = avatarLabel(member.agent_name);
    const content = document.createElement("div");
    const name = document.createElement("strong");
    name.textContent =
      member.agent_id === participantId
        ? "You"
        : displayName(member.agent_name);
    name.title = `${member.agent_name} · ${member.agent_id}`;
    const detail = document.createElement("small");
    detail.textContent = shortId(member.agent_id);
    content.append(name, detail);
    const role = document.createElement("span");
    role.className = "member-role";
    role.textContent =
      member.delivery_mode === "stream_only" ? "Participant" : "Agent";
    row.append(avatar, content, role);
    return row;
  });
  container.replaceChildren(...rows);
}

export function renderAddMemberOptions(
  select: HTMLSelectElement,
  agents: readonly Agent[],
  members: readonly ChannelMember[],
  participantId: string,
): void {
  const memberIds = new Set(members.map((member) => member.agent_id));
  const available = agents
    .filter(
      (agent) => agent.id !== participantId && !memberIds.has(agent.id),
    )
    .sort((left, right) => left.name.localeCompare(right.name));
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent =
    available.length === 0 ? "All agents are members" : "Choose an agent";
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

function avatarLabel(name: string): string {
  return displayName(name).trim().slice(0, 1).toLocaleUpperCase() || "·";
}
