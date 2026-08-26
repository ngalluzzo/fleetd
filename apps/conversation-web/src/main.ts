import {
  ConversationSession,
  type ConversationSnapshot,
} from "@fleetd/client/conversation";
import { createBrowserConversationTransport } from "@fleetd/client/conversation-transport";
import {
  createFleetdOperatorClient,
  FleetdOperatorClientError,
  type FleetdOperatorClient,
} from "@fleetd/client/operator";
import type {
  Agent,
  ConversationSummary,
  PluginGeneration,
  SessionBinding,
} from "@fleetd/client/types";
import type { ConversationPresentationContract } from "./presentation-contract.ts";
import {
  applyComposerAvailability,
  composerAvailability,
  isComposerSendShortcut,
  resizeComposer,
} from "./ui/composer.ts";
import {
  CHANNEL_BROADCAST_TARGET,
  MessageListView,
  renderChannelHeader,
  renderConversationNavigation,
  renderConnectionStatus,
  renderEmptyConversation,
  renderMemberTargets,
} from "./ui/components.ts";
import {
  renderAddMemberOptions,
  renderAgentDirectory,
  renderChannelMemberOptions,
  renderConversationMembers,
  selectedMemberIds,
} from "./ui/workspace-components.ts";
import { agentDirectoryItems } from "./ui/workspace-view-models.ts";
import { displayName } from "./ui/view-models.ts";

interface ConnectionProfile {
  participantId: string;
  operatorCredential: string;
  participantCredential: string;
  requestKind: string;
  resultKind: string;
  channelId?: string;
}

interface PublicConversationApp {
  connect(profile: ConnectionProfile): Promise<void>;
  disconnect(): void;
  inspect(): Record<string, unknown>;
}

declare global {
  interface Window {
    __fleetdConversation?: PublicConversationApp;
  }
}

const elements = {
  connectPanel: required<HTMLElement>("connect-panel"),
  connectForm: required<HTMLFormElement>("connect-form"),
  operatorCredential: required<HTMLInputElement>("operator-credential"),
  participantCredential: required<HTMLInputElement>("participant-credential"),
  participantId: required<HTMLInputElement>("participant-id"),
  requestKind: required<HTMLInputElement>("request-kind"),
  resultKind: required<HTMLInputElement>("result-kind"),
  app: required<HTMLElement>("conversation-app"),
  status: required<HTMLElement>("connection-status"),
  channels: required<HTMLElement>("channel-list"),
  directs: required<HTMLElement>("direct-list"),
  openAgentDirectory: required<HTMLButtonElement>("open-agent-directory"),
  newChannel: required<HTMLButtonElement>("new-channel"),
  newDirectMessage: required<HTMLButtonElement>("new-direct-message"),
  channelTitle: required<HTMLElement>("channel-title"),
  channelMeta: required<HTMLElement>("channel-meta"),
  channelAvatar: required<HTMLElement>("channel-avatar"),
  messages: required<HTMLElement>("message-list"),
  empty: required<HTMLElement>("empty-conversation"),
  emptyTitle: required<HTMLElement>("empty-conversation-title"),
  emptyCopy: required<HTMLElement>("empty-conversation-copy"),
  target: required<HTMLSelectElement>("message-target"),
  composer: required<HTMLFormElement>("composer"),
  composerText: required<HTMLTextAreaElement>("composer-text"),
  send: required<HTMLButtonElement>("send-message"),
  disconnect: required<HTMLButtonElement>("disconnect"),
  openConversationDetails: required<HTMLButtonElement>(
    "open-conversation-details",
  ),
  agentDirectoryDialog: required<HTMLDialogElement>("agent-directory-dialog"),
  agentDirectoryState: required<HTMLElement>("agent-directory-state"),
  agentList: required<HTMLElement>("agent-list"),
  channelDialog: required<HTMLDialogElement>("channel-dialog"),
  channelForm: required<HTMLFormElement>("channel-form"),
  channelName: required<HTMLInputElement>("channel-name"),
  channelMemberOptions: required<HTMLElement>("channel-member-options"),
  channelFormError: required<HTMLElement>("channel-form-error"),
  createChannel: required<HTMLButtonElement>("create-channel"),
  conversationDetailsDialog: required<HTMLDialogElement>(
    "conversation-details-dialog",
  ),
  conversationDetailsKicker: required<HTMLElement>(
    "conversation-details-kicker",
  ),
  conversationDetailsTitle: required<HTMLElement>(
    "conversation-details-title",
  ),
  conversationDetailsCopy: required<HTMLElement>("conversation-details-copy"),
  renameChannelForm: required<HTMLFormElement>("rename-channel-form"),
  renameChannelName: required<HTMLInputElement>("rename-channel-name"),
  renameChannel: required<HTMLButtonElement>("rename-channel"),
  conversationMemberCount: required<HTMLElement>("conversation-member-count"),
  conversationMemberList: required<HTMLElement>("conversation-member-list"),
  addMemberForm: required<HTMLFormElement>("add-member-form"),
  addMemberAgent: required<HTMLSelectElement>("add-member-agent"),
  addMember: required<HTMLButtonElement>("add-member"),
  conversationDetailsError: required<HTMLElement>("conversation-details-error"),
  channelDangerZone: required<HTMLElement>("channel-danger-zone"),
  requestArchiveChannel: required<HTMLButtonElement>("request-archive-channel"),
  archiveChannelDialog: required<HTMLDialogElement>("archive-channel-dialog"),
  archiveChannelForm: required<HTMLFormElement>("archive-channel-form"),
  archiveChannelCopy: required<HTMLElement>("archive-channel-copy"),
  archiveChannel: required<HTMLButtonElement>("archive-channel"),
};
const connectSubmit = requiredDescendant<HTMLButtonElement>(
  elements.connectForm,
  'button[type="submit"]',
);
const connectSubmitLabel = requiredDescendant<HTMLElement>(
  connectSubmit,
  ".button-label",
);
const connectSubmitIcon = requiredDescendant<HTMLElement>(
  connectSubmit,
  ".button-icon",
);

let session: ConversationSession | undefined;
let operatorClient: FleetdOperatorClient | undefined;
let unsubscribe: (() => void) | undefined;
let contract: ConversationPresentationContract | undefined;
let latestSnapshot: ConversationSnapshot | undefined;
let renderFrame: number | undefined;
let sendInFlight = false;
let connectInFlight = false;
let appGeneration = 0;
let agents: readonly Agent[] = [];
let conversations: readonly ConversationSummary[] = [];
let pluginGenerations: readonly PluginGeneration[] = [];
let sessionBindings: readonly SessionBinding[] = [];
let workspaceError: string | undefined;
let workspaceBusy = false;
const messageList = new MessageListView(elements.messages);

const publicApp: PublicConversationApp = {
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
      direct_conversation_count: conversations.filter(
        (conversation) => conversation.kind === "direct",
      ).length,
      agent_count: agents.length,
      member_count: snapshot?.members.length ?? 0,
      message_ids: snapshot?.messages.map((message) => message.id) ?? [],
      message_sequences: snapshot?.messages.map((message) => message.seq) ?? [],
      pending_sends: snapshot?.pendingSends ?? 0,
      error_code: snapshot?.error?.code ?? null,
    };
  },
};

Object.defineProperty(window, "__fleetdConversation", {
  configurable: false,
  enumerable: false,
  writable: false,
  value: publicApp,
});
document.documentElement.dataset.fleetdConversationReady = "true";

elements.connectForm.addEventListener("submit", (event) => {
  event.preventDefault();
  if (connectInFlight) return;
  const profile: ConnectionProfile = {
    participantId: elements.participantId.value,
    operatorCredential: elements.operatorCredential.value,
    participantCredential: elements.participantCredential.value,
    requestKind: elements.requestKind.value,
    resultKind: elements.resultKind.value,
  };
  elements.operatorCredential.value = "";
  elements.participantCredential.value = "";
  connectInFlight = true;
  setConnectBusy(true);
  void connect(profile)
    .catch(() => {
      showConnectError("Check your workspace and participant keys, then try again.");
    })
    .finally(() => {
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
elements.openConversationDetails.addEventListener(
  "click",
  openConversationDetails,
);
elements.agentList.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest<HTMLButtonElement>("button[data-direct-agent-id]");
  if (!button?.dataset.directAgentId) return;
  void openDirectMessage(button.dataset.directAgentId, button);
});
elements.channelForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void createSharedChannel();
});
elements.renameChannelForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void renameSelectedChannel();
});
elements.addMemberForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void addSelectedChannelMember();
});
elements.requestArchiveChannel.addEventListener("click", confirmChannelArchive);
elements.archiveChannelForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void archiveSelectedChannel();
});
document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest<HTMLButtonElement>("button[data-close-dialog]");
  const dialogId = button?.dataset.closeDialog;
  if (!dialogId) return;
  const dialog = document.getElementById(dialogId);
  if (dialog instanceof HTMLDialogElement) dialog.close();
});
elements.composer.addEventListener("submit", (event) => {
  event.preventDefault();
  void sendComposerMessage();
});
elements.composerText.addEventListener("input", () => {
  resizeComposer(elements.composerText);
  renderComposerAvailability();
});
elements.composerText.addEventListener("keydown", (event) => {
  if (!isComposerSendShortcut(event)) return;
  event.preventDefault();
  elements.composer.requestSubmit();
});
elements.target.addEventListener("change", () => {
  renderComposerAvailability();
  renderComposerContext();
  const selected = elements.target.selectedOptions[0];
  if (selected?.title) elements.target.title = selected.title;
});
resizeComposer(elements.composerText);

async function connect(profileInput: ConnectionProfile): Promise<void> {
  disconnect();
  const generation = appGeneration;
  let profile: ConnectionProfile;
  try {
    profile = { ...validateProfile(profileInput) };
  } finally {
    clearProfileCredentials(profileInput);
  }
  required<HTMLElement>("connect-error").hidden = true;
  contract = {
    requestKind: profile.requestKind,
    resultKind: profile.resultKind,
  };
  let activeOperatorClient: FleetdOperatorClient | undefined;
  const transport = (() => {
    try {
      activeOperatorClient = createFleetdOperatorClient({
        origin: window.location.origin,
        operatorCredential: profile.operatorCredential,
      });
      return createBrowserConversationTransport({
        origin: window.location.origin,
        participantId: profile.participantId,
        operatorCredential: profile.operatorCredential,
        participantCredential: profile.participantCredential,
      });
    } finally {
      clearProfileCredentials(profile);
    }
  })();
  if (!activeOperatorClient) throw new Error("Fleetd workspace connection failed");
  operatorClient = activeOperatorClient;
  const activeSession = new ConversationSession(transport);
  session = activeSession;
  unsubscribe = activeSession.subscribe(scheduleRender);
  elements.connectPanel.hidden = true;
  elements.app.hidden = false;
  try {
    await Promise.all([
      activeSession.start(),
      refreshWorkspace(activeOperatorClient),
    ]);
    if (generation !== appGeneration || session !== activeSession) {
      throw new Error("Fleetd conversation connection was superseded");
    }
    const channelId = profile.channelId;
    if (channelId) await activeSession.selectChannel(channelId);
  } catch {
    if (generation === appGeneration && session === activeSession) disconnect();
    throw new Error("Fleetd conversation connection failed");
  }
}

function disconnect(): void {
  appGeneration += 1;
  sendInFlight = false;
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
  workspaceError = undefined;
  workspaceBusy = false;
  if (renderFrame !== undefined) cancelAnimationFrame(renderFrame);
  renderFrame = undefined;
  elements.app.hidden = true;
  elements.connectPanel.hidden = false;
  for (const dialog of [
    elements.agentDirectoryDialog,
    elements.channelDialog,
    elements.conversationDetailsDialog,
    elements.archiveChannelDialog,
  ]) {
    if (dialog.open) dialog.close();
  }
  messageList.clear();
  elements.channels.replaceChildren();
  elements.directs.replaceChildren();
  elements.target.replaceChildren();
  elements.composerText.value = "";
  elements.openConversationDetails.disabled = true;
  resizeComposer(elements.composerText);
  renderComposerAvailability();
}

function scheduleRender(snapshot: ConversationSnapshot): void {
  latestSnapshot = snapshot;
  if (renderFrame !== undefined) return;
  renderFrame = requestAnimationFrame(() => {
    renderFrame = undefined;
    if (latestSnapshot) render(latestSnapshot);
  });
}

function render(snapshot: ConversationSnapshot): void {
  renderConnectionStatus(elements.status, snapshot, sendInFlight);
  const directory = agentDirectoryItems(
    agents,
    pluginGenerations,
    sessionBindings,
    snapshot.participantId,
  );
  renderConversationNavigation(
    { channels: elements.channels, directs: elements.directs },
    conversations,
    snapshot.selectedChannelId,
    snapshot,
    new Map(directory.map((item) => [item.id, item.health])),
  );
  const selectedConversation = conversations.find(
    (conversation) => conversation.id === snapshot.selectedChannelId,
  );
  renderChannelHeader(snapshot, {
    title: elements.channelTitle,
    meta: elements.channelMeta,
    avatar: elements.channelAvatar,
  }, selectedConversation);
  renderMemberTargets(
    elements.target,
    snapshot.members,
    snapshot.participantId,
    selectedConversation?.kind === "shared",
  );
  elements.openConversationDetails.disabled = selectedConversation == null;
  renderComposerContext();
  messageList.render(snapshot, requiredContract());
  renderEmptyConversation(snapshot, {
    root: elements.empty,
    title: elements.emptyTitle,
    copy: elements.emptyCopy,
  });
  renderComposerAvailability();
}

function selectConversationFromEvent(event: Event): void {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest<HTMLButtonElement>("button[data-channel-id]");
  if (!button?.dataset.channelId || !session) return;
  void session.selectChannel(button.dataset.channelId).catch(() => {
    if (session) scheduleRender(session.snapshot);
  });
}

async function refreshWorkspace(
  client: FleetdOperatorClient = requiredOperatorClient(),
): Promise<void> {
  workspaceBusy = true;
  elements.agentList.setAttribute("aria-busy", "true");
  renderAgentDirectoryState();
  try {
    const [nextAgents, nextConversations, nextGenerations, nextBindings] =
      await Promise.all([
        client.listAgents(),
        client.listConversations({ include_archived: false }),
        client.listPluginGenerations(),
        client.listSessionBindings(),
      ]);
    if (client !== operatorClient) return;
    agents = nextAgents;
    conversations = nextConversations;
    pluginGenerations = nextGenerations;
    sessionBindings = nextBindings;
    workspaceError = undefined;
    renderAgentDirectoryState();
    if (session) scheduleRender(session.snapshot);
  } catch (error) {
    if (client === operatorClient) {
      workspaceError = operatorErrorMessage(error, "Workspace details are unavailable.");
      renderAgentDirectoryState();
    }
    throw error;
  } finally {
    if (client === operatorClient) {
      workspaceBusy = false;
      elements.agentList.setAttribute("aria-busy", "false");
      renderAgentDirectoryState();
    }
  }
}

function openAgentDirectory(): void {
  renderAgentDirectoryState();
  showDialog(elements.agentDirectoryDialog);
}

function renderAgentDirectoryState(): void {
  const snapshot = latestSnapshot;
  const items = agentDirectoryItems(
    agents,
    pluginGenerations,
    sessionBindings,
    snapshot?.participantId ?? "",
  );
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

function openCreateChannel(): void {
  const snapshot = latestSnapshot;
  if (!snapshot) return;
  elements.channelForm.reset();
  setDialogError(elements.channelFormError);
  renderChannelMemberOptions(
    elements.channelMemberOptions,
    agents,
    snapshot.participantId,
  );
  showDialog(elements.channelDialog);
  elements.channelName.focus();
}

async function createSharedChannel(): Promise<void> {
  const client = operatorClient;
  const activeSession = session;
  const snapshot = latestSnapshot;
  if (!client || !activeSession || !snapshot) return;
  const memberIds = selectedMemberIds(elements.channelMemberOptions);
  if (memberIds.length === 0) {
    setDialogError(
      elements.channelFormError,
      "Choose at least one agent for this channel.",
    );
    return;
  }
  setFormBusy(elements.channelForm, elements.createChannel, true, "Creating…");
  setDialogError(elements.channelFormError);
  try {
    const channel = await client.createSharedChannel({
      name: elements.channelName.value.trim(),
      members: [
        { agent_id: snapshot.participantId, delivery_mode: "stream_only" },
        ...memberIds.map((agentId) => ({
          agent_id: agentId,
          delivery_mode: "inbox" as const,
        })),
      ],
    });
    elements.channelDialog.close();
    await refreshAndSelectConversation(channel.id);
  } catch (error) {
    setDialogError(
      elements.channelFormError,
      operatorErrorMessage(error, "The channel could not be created."),
    );
  } finally {
    setFormBusy(elements.channelForm, elements.createChannel, false, "Create channel");
  }
}

async function openDirectMessage(
  agentId: string,
  button: HTMLButtonElement,
): Promise<void> {
  const client = operatorClient;
  const snapshot = latestSnapshot;
  if (!client || !session || !snapshot) return;
  button.disabled = true;
  button.textContent = "Opening…";
  setDialogState("Opening direct message…");
  try {
    const conversation = await client.openDirectConversation({
      members: [
        { agent_id: snapshot.participantId, delivery_mode: "stream_only" },
        { agent_id: agentId, delivery_mode: "inbox" },
      ],
    });
    elements.agentDirectoryDialog.close();
    await refreshAndSelectConversation(conversation.id);
  } catch (error) {
    setDialogState(
      operatorErrorMessage(error, "The direct message could not be opened."),
      "danger",
    );
  } finally {
    button.disabled = false;
    button.textContent = "Message";
  }
}

function openConversationDetails(): void {
  const conversation = selectedConversation();
  const snapshot = latestSnapshot;
  if (!conversation || !snapshot) return;
  const direct = conversation.kind === "direct";
  const peer = direct
    ? conversation.members.find(
        (member) => member.agent_id !== snapshot.participantId,
      )
    : undefined;
  elements.conversationDetailsKicker.textContent = direct
    ? "Direct message"
    : "Channel";
  elements.conversationDetailsTitle.textContent = peer
    ? displayName(peer.agent_name)
    : displayName(conversation.name);
  elements.conversationDetailsCopy.textContent = direct
    ? "A private conversation between two workspace participants."
    : "Manage the channel name and its members.";
  elements.renameChannelName.value = conversation.name;
  elements.renameChannelForm.hidden = direct;
  elements.addMemberForm.hidden = direct;
  elements.channelDangerZone.hidden = direct;
  elements.conversationMemberCount.textContent = String(conversation.members.length);
  renderConversationMembers(
    elements.conversationMemberList,
    conversation.members,
    snapshot.participantId,
  );
  renderAddMemberOptions(
    elements.addMemberAgent,
    agents,
    conversation.members,
    snapshot.participantId,
  );
  elements.addMember.disabled = elements.addMemberAgent.disabled;
  setDialogError(elements.conversationDetailsError);
  showDialog(elements.conversationDetailsDialog);
}

async function renameSelectedChannel(): Promise<void> {
  const client = operatorClient;
  const conversation = selectedConversation();
  if (!client || conversation?.kind !== "shared") return;
  setFormBusy(
    elements.renameChannelForm,
    elements.renameChannel,
    true,
    "Saving…",
  );
  setDialogError(elements.conversationDetailsError);
  try {
    await client.renameSharedChannel(conversation.id, {
      name: elements.renameChannelName.value.trim(),
    });
    await refreshConversationState();
    openConversationDetails();
  } catch (error) {
    setDialogError(
      elements.conversationDetailsError,
      operatorErrorMessage(error, "The channel name could not be saved."),
    );
  } finally {
    setFormBusy(
      elements.renameChannelForm,
      elements.renameChannel,
      false,
      "Save name",
    );
  }
}

async function addSelectedChannelMember(): Promise<void> {
  const client = operatorClient;
  const conversation = selectedConversation();
  const agentId = elements.addMemberAgent.value;
  if (!client || conversation?.kind !== "shared" || !agentId) return;
  setFormBusy(elements.addMemberForm, elements.addMember, true, "Adding…");
  setDialogError(elements.conversationDetailsError);
  try {
    await client.addSharedChannelMember(conversation.id, {
      agent_id: agentId,
      delivery_mode: "inbox",
    });
    await refreshConversationState();
    openConversationDetails();
  } catch (error) {
    setDialogError(
      elements.conversationDetailsError,
      operatorErrorMessage(error, "The agent could not be added."),
    );
  } finally {
    setFormBusy(elements.addMemberForm, elements.addMember, false, "Add member");
  }
}

function confirmChannelArchive(): void {
  const conversation = selectedConversation();
  if (conversation?.kind !== "shared") return;
  elements.archiveChannelCopy.textContent = `#${displayName(conversation.name)} will leave the sidebar. Its history will not be deleted.`;
  elements.conversationDetailsDialog.close();
  showDialog(elements.archiveChannelDialog);
}

async function archiveSelectedChannel(): Promise<void> {
  const client = operatorClient;
  const activeSession = session;
  const conversation = selectedConversation();
  if (!client || !activeSession || conversation?.kind !== "shared") return;
  setFormBusy(
    elements.archiveChannelForm,
    elements.archiveChannel,
    true,
    "Archiving…",
  );
  try {
    await client.archiveSharedChannel(conversation.id);
    activeSession.clearSelection();
    await refreshConversationState();
    elements.archiveChannelDialog.close();
  } catch (error) {
    elements.archiveChannelCopy.textContent = operatorErrorMessage(
      error,
      "The channel could not be archived.",
    );
  } finally {
    setFormBusy(
      elements.archiveChannelForm,
      elements.archiveChannel,
      false,
      "Archive channel",
    );
  }
}

async function refreshAndSelectConversation(channelId: string): Promise<void> {
  await refreshConversationState();
  await session?.selectChannel(channelId);
}

async function refreshConversationState(): Promise<void> {
  const activeSession = session;
  const client = operatorClient;
  if (!activeSession || !client) return;
  await Promise.all([activeSession.refreshChannels(), refreshWorkspace(client)]);
}

function selectedConversation(): ConversationSummary | undefined {
  const channelId = latestSnapshot?.selectedChannelId;
  return conversations.find((conversation) => conversation.id === channelId);
}

function requiredOperatorClient(): FleetdOperatorClient {
  if (!operatorClient) throw new Error("Fleetd workspace is disconnected");
  return operatorClient;
}

function showDialog(dialog: HTMLDialogElement): void {
  if (dialog.open) return;
  dialog.showModal();
}

function setDialogError(element: HTMLElement, message?: string): void {
  element.textContent = message ?? "";
  element.hidden = message == null;
}

function setDialogState(message: string, tone?: "danger"): void {
  elements.agentDirectoryState.textContent = message;
  elements.agentDirectoryState.hidden = false;
  if (tone) elements.agentDirectoryState.dataset.tone = tone;
  else delete elements.agentDirectoryState.dataset.tone;
}

function setFormBusy(
  form: HTMLFormElement,
  button: HTMLButtonElement,
  busy: boolean,
  label: string,
): void {
  form.setAttribute("aria-busy", String(busy));
  button.disabled = busy;
  button.textContent = label;
}

function operatorErrorMessage(error: unknown, fallback: string): string {
  if (!(error instanceof FleetdOperatorClientError)) return fallback;
  if (error.status === 401) return "The workspace key is no longer valid. Reconnect to continue.";
  if (error.status === 403) return "This action requires workspace owner access.";
  if (error.status === 404) return "This conversation no longer exists. Refresh the workspace and try again.";
  if (error.status === 409) return "The workspace changed before this action completed. Refresh and try again.";
  return fallback;
}

async function sendComposerMessage(): Promise<void> {
  const activeSession = session;
  if (!activeSession || !contract || sendInFlight) return;
  const draft = elements.composerText.value;
  const text = draft.trim();
  const recipientId = elements.target.value;
  if (!text || !recipientId) return;
  const turnId = crypto.randomUUID();
  const generation = appGeneration;
  sendInFlight = true;
  renderConnectionStatus(elements.status, activeSession.snapshot, true);
  renderComposerAvailability();
  try {
    await activeSession.send({
      idempotency_key: `fleetd-conversation/${turnId}`,
      recipient_id:
        recipientId === CHANNEL_BROADCAST_TARGET ? null : recipientId,
      kind: contract.requestKind,
      payload: { text },
      correlation_id: turnId,
      causation_id: null,
    });
    if (
      generation === appGeneration &&
      session === activeSession &&
      elements.composerText.value === draft
    ) {
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

function renderComposerAvailability(): void {
  const snapshot = latestSnapshot;
  const availability = composerAvailability({
    phase: snapshot?.phase ?? "closed",
    selectedChannelId: snapshot?.selectedChannelId ?? null,
    targetId: elements.target.value,
    draft: elements.composerText.value,
    pendingSends: snapshot?.pendingSends ?? 0,
    sending: sendInFlight,
  });
  applyComposerAvailability(availability, {
    form: elements.composer,
    textarea: elements.composerText,
    target: elements.target,
    send: elements.send,
  });
}

function renderComposerContext(): void {
  const recipient = elements.target.selectedOptions[0]?.textContent?.trim();
  elements.composerText.placeholder = recipient
    ? `Message ${recipient}…`
    : "Write a message…";
}

function validateProfile(value: ConnectionProfile): ConnectionProfile {
  if (!value || typeof value !== "object") throw new Error("profile required");
  boundedProfileField(value.participantId, "participantId", 256);
  boundedProfileField(value.operatorCredential, "operatorCredential", 4_096);
  boundedProfileField(
    value.participantCredential,
    "participantCredential",
    4_096,
  );
  boundedProfileField(value.requestKind, "requestKind", 256);
  boundedProfileField(value.resultKind, "resultKind", 256);
  if (
    value.channelId !== undefined &&
    (typeof value.channelId !== "string" ||
      value.channelId.trim().length === 0 ||
      value.channelId.length > 256)
  ) {
    throw new Error("invalid conversation profile field: channelId");
  }
  return value;
}

function boundedProfileField(
  value: string,
  name: string,
  maximumLength: number,
): void {
  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    value.length > maximumLength
  ) {
    throw new Error(`invalid conversation profile field: ${name}`);
  }
}

function clearProfileCredentials(profile: ConnectionProfile): void {
  try {
    profile.operatorCredential = "";
    profile.participantCredential = "";
  } catch {
    // A frozen native-host input cannot be scrubbed; it remains caller-owned.
  }
}

function showConnectError(message: string): void {
  const output = required<HTMLElement>("connect-error");
  output.textContent = message;
  output.hidden = false;
}

function setConnectBusy(busy: boolean): void {
  elements.connectForm.setAttribute("aria-busy", String(busy));
  connectSubmit.disabled = busy;
  connectSubmitLabel.textContent = busy
    ? "Connecting…"
    : "Continue to conversations";
  connectSubmitIcon.textContent = busy ? "…" : "→";
}

function requiredContract(): ConversationPresentationContract {
  if (!contract) throw new Error("conversation presentation is disconnected");
  return contract;
}

function required<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`missing conversation element: ${id}`);
  return element as T;
}

function requiredDescendant<T extends HTMLElement>(
  parent: ParentNode,
  selector: string,
): T {
  const element = parent.querySelector(selector);
  if (!element) throw new Error(`missing conversation element: ${selector}`);
  return element as T;
}
