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
  AgentSeatConfiguration,
  ConversationAttention,
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
  MessageListView,
  renderChannelHeader,
  renderConversationNavigation,
  renderConnectionStatus,
  renderEmptyConversation,
} from "./ui/components.ts";
import {
  applyMention,
  directRecipient,
  mentionCandidates,
  mentionQueryAt,
  mentionSelectionPresent,
  type MentionCandidate,
  type MentionQuery,
  type MentionSelection,
} from "./ui/mentions.ts";
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
  runtimeProfiles?: readonly RuntimeProfileDescriptor[];
}

interface RuntimeProfileDescriptor {
  id: string;
  label: string;
  description: string;
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
  composer: required<HTMLFormElement>("composer"),
  composerText: required<HTMLTextAreaElement>("composer-text"),
  composerAudience: required<HTMLElement>("composer-audience"),
  composerAudienceLabel: requiredDescendant<HTMLElement>(
    required<HTMLElement>("composer-audience"),
    ".composer-audience-label",
  ),
  clearMention: required<HTMLButtonElement>("clear-mention"),
  mentionSuggestions: required<HTMLElement>("mention-suggestions"),
  send: required<HTMLButtonElement>("send-message"),
  disconnect: required<HTMLButtonElement>("disconnect"),
  openConversationDetails: required<HTMLButtonElement>(
    "open-conversation-details",
  ),
  agentDirectoryDialog: required<HTMLDialogElement>("agent-directory-dialog"),
  agentDirectoryState: required<HTMLElement>("agent-directory-state"),
  agentList: required<HTMLElement>("agent-list"),
  agentSeatDialog: required<HTMLDialogElement>("agent-seat-dialog"),
  agentSeatForm: required<HTMLFormElement>("agent-seat-form"),
  agentSeatTitle: required<HTMLElement>("agent-seat-title"),
  agentSeatCopy: required<HTMLElement>("agent-seat-copy"),
  agentSeatProfileSummary: required<HTMLElement>("agent-seat-profile-summary"),
  agentSeatProfile: required<HTMLSelectElement>("agent-seat-profile"),
  agentSeatInstructions: required<HTMLTextAreaElement>("agent-seat-instructions"),
  agentSeatDesiredState: required<HTMLSelectElement>("agent-seat-desired-state"),
  agentSeatError: required<HTMLElement>("agent-seat-error"),
  restartAgentSeat: required<HTMLButtonElement>("restart-agent-seat"),
  saveAgentSeat: required<HTMLButtonElement>("save-agent-seat"),
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
let seatConfigurations: readonly AgentSeatConfiguration[] = [];
let runtimeProfiles: readonly RuntimeProfileDescriptor[] = [];
let configuredAgentId: string | undefined;
let workspaceError: string | undefined;
let workspaceBusy = false;
let mentionSelection: MentionSelection | undefined;
let mentionQuery: MentionQuery | undefined;
let mentionOptions: readonly MentionCandidate[] = [];
let activeMentionIndex = 0;
let mentionDismissed = false;
let renderedChannelId: string | null = null;
let visitFirstUnreadSeq: number | null = null;
const pendingReadAdvances = new Map<string, number>();
let readAdvanceRunning = false;
let attentionRefreshRunning = false;
let attentionRefreshTimer: number | undefined;
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
      attention: snapshot?.attention ?? [],
      first_unread_seq: visitFirstUnreadSeq,
      pending_sends: snapshot?.pendingSends ?? 0,
      composer_recipient_id: currentRecipientId(),
      mention_suggestions_open: !elements.mentionSuggestions.hidden,
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
  const manage = target.closest<HTMLButtonElement>("button[data-manage-agent-id]");
  if (manage?.dataset.manageAgentId) {
    openAgentSeatConfiguration(manage.dataset.manageAgentId);
    return;
  }
  const button = target.closest<HTMLButtonElement>("button[data-direct-agent-id]");
  if (!button?.dataset.directAgentId) return;
  void openDirectMessage(button.dataset.directAgentId, button);
});
elements.agentSeatForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void saveAgentSeatConfiguration();
});
elements.restartAgentSeat.addEventListener("click", () => {
  void restartConfiguredAgent();
});
elements.agentSeatProfile.addEventListener("change", renderSelectedProfileSummary);
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
  mentionDismissed = false;
  if (
    mentionSelection &&
    !mentionSelectionPresent(elements.composerText.value, mentionSelection)
  ) {
    mentionSelection = undefined;
  }
  refreshMentionSuggestions();
  resizeComposer(elements.composerText);
  renderComposerAvailability();
  renderComposerContext();
});
elements.composerText.addEventListener("keydown", (event) => {
  if (handleMentionKeydown(event)) return;
  if (!isComposerSendShortcut(event)) return;
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
  if (!(target instanceof Element)) return;
  const option = target.closest<HTMLButtonElement>("button[data-recipient-id]");
  const recipientId = option?.dataset.recipientId;
  if (!recipientId) return;
  const candidate = mentionOptions.find(
    (item) => item.recipientId === recipientId,
  );
  if (candidate) selectMention(candidate);
});
elements.clearMention.addEventListener("click", clearSelectedMention);
elements.messages.addEventListener("scroll", queueCurrentVisibleRead);
window.addEventListener("focus", refreshAttentionOnReturn);
window.addEventListener("resize", queueCurrentVisibleRead);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") refreshAttentionOnReturn();
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
  runtimeProfiles = profile.runtimeProfiles ?? [];
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
    scheduleAttentionRefresh();
  } catch {
    if (generation === appGeneration && session === activeSession) disconnect();
    throw new Error("Fleetd conversation connection failed");
  }
}

function disconnect(): void {
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
  if (renderFrame !== undefined) cancelAnimationFrame(renderFrame);
  renderFrame = undefined;
  elements.app.hidden = true;
  elements.connectPanel.hidden = false;
  for (const dialog of [
    elements.agentDirectoryDialog,
    elements.agentSeatDialog,
    elements.channelDialog,
    elements.conversationDetailsDialog,
    elements.archiveChannelDialog,
  ]) {
    if (dialog.open) dialog.close();
  }
  messageList.clear();
  elements.channels.replaceChildren();
  elements.directs.replaceChildren();
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
  const attention = new Map(
    snapshot.attention.map((item) => [item.channel_id, item]),
  );
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
    attention,
  );
  const selectedConversation = conversations.find(
    (conversation) => conversation.id === snapshot.selectedChannelId,
  );
  if (renderedChannelId !== snapshot.selectedChannelId) {
    resetMentionState();
    renderedChannelId = snapshot.selectedChannelId;
    visitFirstUnreadSeq = snapshot.selectedChannelId === null
      ? null
      : attention.get(snapshot.selectedChannelId)?.first_unread_seq ?? null;
  }
  renderChannelHeader(snapshot, {
    title: elements.channelTitle,
    meta: elements.channelMeta,
    avatar: elements.channelAvatar,
  }, selectedConversation);
  elements.openConversationDetails.disabled = selectedConversation == null;
  refreshMentionSuggestions();
  renderComposerContext();
  messageList.render(snapshot, requiredContract(), visitFirstUnreadSeq);
  renderEmptyConversation(snapshot, {
    root: elements.empty,
    title: elements.emptyTitle,
    copy: elements.emptyCopy,
  });
  renderComposerAvailability();
  queueVisibleReadAdvance(snapshot, attention);
}

function selectConversationFromEvent(event: Event): void {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const button = target.closest<HTMLButtonElement>("button[data-channel-id]");
  if (!button?.dataset.channelId || !session) return;
  const activeSession = session;
  const channelId = button.dataset.channelId;
  void (async () => {
    try {
      await activeSession.refreshAttention();
    } catch {
      // Selection still opens against the last exact durable attention read.
    }
    await activeSession.selectChannel(channelId);
  })().catch(() => {
    if (session === activeSession) scheduleRender(activeSession.snapshot);
  });
}

function queueVisibleReadAdvance(
  snapshot: ConversationSnapshot,
  attention: ReadonlyMap<string, ConversationAttention>,
): void {
  const channelId = snapshot.selectedChannelId;
  if (
    channelId === null ||
    snapshot.phase !== "live" ||
    snapshot.cursor <= 0 ||
    !documentIsAttended()
  ) {
    return;
  }
  const readThrough = attention.get(channelId)?.read_through_seq ?? 0;
  const throughSeq = messageList.visibleThroughSeq();
  if (throughSeq === null || throughSeq <= readThrough) return;
  pendingReadAdvances.set(
    channelId,
    Math.max(pendingReadAdvances.get(channelId) ?? 0, throughSeq),
  );
  if (!readAdvanceRunning) void drainReadAdvances();
}

function queueCurrentVisibleRead(): void {
  const snapshot = latestSnapshot;
  if (!snapshot) return;
  queueVisibleReadAdvance(
    snapshot,
    new Map(snapshot.attention.map((item) => [item.channel_id, item])),
  );
}

async function drainReadAdvances(): Promise<void> {
  if (readAdvanceRunning) return;
  readAdvanceRunning = true;
  try {
    while (pendingReadAdvances.size > 0) {
      const next = pendingReadAdvances.entries().next().value as
        | [string, number]
        | undefined;
      if (!next) break;
      const [channelId, throughSeq] = next;
      pendingReadAdvances.delete(channelId);
      const activeSession = session;
      if (!activeSession) continue;
      try {
        await activeSession.markRead(channelId, throughSeq);
      } catch {
        // The durable cursor remains unchanged and a later focus or selection
        // refresh will retry from authoritative state.
      }
    }
  } finally {
    readAdvanceRunning = false;
    if (pendingReadAdvances.size > 0) void drainReadAdvances();
  }
}

function refreshAttentionOnReturn(): void {
  const activeSession = session;
  if (!activeSession || attentionRefreshRunning || !documentIsAttended()) return;
  attentionRefreshRunning = true;
  void activeSession.refreshAttention().catch(() => {
    // Conversation transport remains usable; the next focus retries.
  }).finally(() => {
    attentionRefreshRunning = false;
    if (session === activeSession) {
      scheduleRender(activeSession.snapshot);
    }
  });
}

function scheduleAttentionRefresh(): void {
  if (!session || attentionRefreshTimer !== undefined) return;
  const generation = appGeneration;
  attentionRefreshTimer = window.setTimeout(() => {
    attentionRefreshTimer = undefined;
    if (session && generation === appGeneration) {
      refreshAttentionOnReturn();
      scheduleAttentionRefresh();
    }
  }, 5_000);
}

function documentIsAttended(): boolean {
  return document.visibilityState === "visible" && document.hasFocus();
}

async function refreshWorkspace(
  client: FleetdOperatorClient = requiredOperatorClient(),
): Promise<void> {
  workspaceBusy = true;
  elements.agentList.setAttribute("aria-busy", "true");
  renderAgentDirectoryState();
  try {
    const [
      nextAgents,
      nextConversations,
      nextGenerations,
      nextBindings,
      nextSeatConfigurations,
    ] =
      await Promise.all([
        client.listAgents(),
        client.listConversations({ include_archived: false }),
        client.listPluginGenerations(),
        client.listSessionBindings(),
        client.listAgentSeatConfigurations(),
      ]);
    if (client !== operatorClient) return;
    agents = nextAgents;
    conversations = nextConversations;
    pluginGenerations = nextGenerations;
    sessionBindings = nextBindings;
    seatConfigurations = nextSeatConfigurations;
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
    seatConfigurations,
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

function openAgentSeatConfiguration(agentId: string): void {
  const agent = agents.find((candidate) => candidate.id === agentId);
  if (!agent) return;
  configuredAgentId = agentId;
  const current = seatConfigurations.find(
    (configuration) => configuration.agent_id === agentId,
  );
  elements.agentSeatTitle.textContent = `${current ? "Manage" : "Set up"} ${displayName(agent.name)}`;
  elements.agentSeatCopy.textContent = current
    ? `This runtime follows ${displayName(agent.name)} wherever the agent participates.`
    : `Make ${displayName(agent.name)} an active collaborator on this machine.`;
  const options = runtimeProfiles.map((profile) => {
    const option = document.createElement("option");
    option.value = profile.id;
    option.textContent = profile.label;
    return option;
  });
  if (
    current &&
    !runtimeProfiles.some((profile) => profile.id === current.profile_id)
  ) {
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
  if (elements.agentDirectoryDialog.open) elements.agentDirectoryDialog.close();
  if (elements.conversationDetailsDialog.open) elements.conversationDetailsDialog.close();
  showDialog(elements.agentSeatDialog);
}

function renderSelectedProfileSummary(): void {
  const selected = runtimeProfiles.find(
    (profile) => profile.id === elements.agentSeatProfile.value,
  );
  elements.agentSeatProfileSummary.textContent = selected
    ? selected.description
    : runtimeProfiles.length === 0
      ? "This host has not published an approved runtime catalog. Configure the desktop host to activate agents."
      : "The previously selected profile is not approved on this host.";
  elements.agentSeatProfileSummary.dataset.tone = selected ? "neutral" : "warning";
}

async function saveAgentSeatConfiguration(): Promise<void> {
  const client = operatorClient;
  const agentId = configuredAgentId;
  if (!client || !agentId || !elements.agentSeatProfile.value) return;
  setFormBusy(elements.agentSeatForm, elements.saveAgentSeat, true, "Applying…");
  elements.restartAgentSeat.disabled = true;
  setDialogError(elements.agentSeatError);
  try {
    await client.configureAgentSeat(agentId, {
      profile_id: elements.agentSeatProfile.value,
      instructions: elements.agentSeatInstructions.value,
      desired_state:
        elements.agentSeatDesiredState.value === "stopped" ? "stopped" : "running",
    });
    await refreshWorkspace(client);
    elements.agentSeatDialog.close();
  } catch (error) {
    setDialogError(
      elements.agentSeatError,
      operatorErrorMessage(error, "The agent configuration could not be applied."),
    );
  } finally {
    setFormBusy(elements.agentSeatForm, elements.saveAgentSeat, false, "Save and apply");
    const current = seatConfigurations.find(
      (configuration) => configuration.agent_id === agentId,
    );
    elements.restartAgentSeat.disabled = current?.desired_state !== "running";
  }
}

async function restartConfiguredAgent(): Promise<void> {
  const client = operatorClient;
  const agentId = configuredAgentId;
  if (!client || !agentId) return;
  elements.restartAgentSeat.disabled = true;
  elements.restartAgentSeat.textContent = "Restarting…";
  setDialogError(elements.agentSeatError);
  try {
    await client.restartAgentSeat(agentId);
    await refreshWorkspace(client);
    elements.agentSeatDialog.close();
  } catch (error) {
    setDialogError(
      elements.agentSeatError,
      operatorErrorMessage(error, "The agent could not be restarted."),
    );
  } finally {
    elements.restartAgentSeat.textContent = "Restart now";
    const current = seatConfigurations.find(
      (configuration) => configuration.agent_id === agentId,
    );
    elements.restartAgentSeat.disabled = current?.desired_state !== "running";
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
    const configured = seatConfigurations.some(
      (configuration) => configuration.agent_id === agentId,
    );
    if (!configured && runtimeProfiles.length > 0) {
      openAgentSeatConfiguration(agentId);
    } else {
      openConversationDetails();
    }
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
  const recipientId = currentRecipientId();
  if (!text) return;
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

function renderComposerAvailability(): void {
  const snapshot = latestSnapshot;
  const availability = composerAvailability({
    phase: snapshot?.phase ?? "closed",
    selectedChannelId: snapshot?.selectedChannelId ?? null,
    draft: elements.composerText.value,
    pendingSends: snapshot?.pendingSends ?? 0,
    sending: sendInFlight,
  });
  applyComposerAvailability(availability, {
    form: elements.composer,
    textarea: elements.composerText,
    send: elements.send,
  });
}

function renderComposerContext(): void {
  const snapshot = latestSnapshot;
  const conversation = selectedConversation();
  const direct = conversation?.kind === "direct"
    ? directRecipient(snapshot?.members ?? [], snapshot?.participantId ?? "")
    : undefined;
  const selected = mentionSelection && mentionSelectionPresent(
    elements.composerText.value,
    mentionSelection,
  )
    ? mentionSelection
    : undefined;
  const recipientId = direct?.recipientId ?? selected?.recipientId ?? "";
  const recipientLabel = direct?.label ?? selected?.label;
  elements.composerAudience.dataset.recipientId = recipientId;
  elements.clearMention.hidden = direct !== undefined || selected === undefined;
  elements.composerAudienceLabel.textContent = !conversation
    ? "Select a conversation"
    : recipientLabel
      ? `Notifying ${recipientLabel}`
      : "Notifying everyone";
  elements.composerText.placeholder = !conversation
    ? "Write a message…"
    : conversation.kind === "direct" && recipientLabel
      ? `Message ${recipientLabel}…`
      : "Message the channel…";
}

function currentRecipientId(): string | null {
  const snapshot = latestSnapshot;
  const conversation = selectedConversation();
  if (!snapshot || !conversation) return null;
  if (conversation.kind === "direct") {
    return directRecipient(snapshot.members, snapshot.participantId)?.recipientId ?? null;
  }
  if (
    mentionSelection &&
    mentionSelectionPresent(elements.composerText.value, mentionSelection)
  ) {
    return mentionSelection.recipientId;
  }
  return null;
}

function refreshMentionSuggestions(): void {
  const snapshot = latestSnapshot;
  const conversation = selectedConversation();
  if (
    mentionDismissed ||
    snapshot?.phase !== "live" ||
    conversation?.kind !== "shared" ||
    mentionSelection !== undefined
  ) {
    hideMentionSuggestions();
    return;
  }
  const caret = elements.composerText.selectionStart;
  mentionQuery = mentionQueryAt(elements.composerText.value, caret);
  mentionOptions = mentionQuery
    ? mentionCandidates(
        snapshot.members,
        snapshot.participantId,
        mentionQuery.text,
      )
    : [];
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
    note.textContent = candidate.receivesInboxWork
      ? "will be notified"
      : "watching channel";
    option.append(name, note);
    return option;
  });
  elements.mentionSuggestions.replaceChildren(...options);
  elements.mentionSuggestions.hidden = false;
  elements.composerText.setAttribute("aria-expanded", "true");
  elements.composerText.setAttribute(
    "aria-activedescendant",
    `mention-option-${activeMentionIndex}`,
  );
}

function handleMentionKeydown(event: KeyboardEvent): boolean {
  if (elements.mentionSuggestions.hidden || mentionOptions.length === 0) {
    return false;
  }
  if (event.key === "ArrowDown" || event.key === "ArrowUp") {
    event.preventDefault();
    const direction = event.key === "ArrowDown" ? 1 : -1;
    activeMentionIndex =
      (activeMentionIndex + direction + mentionOptions.length) %
      mentionOptions.length;
    refreshMentionSuggestions();
    return true;
  }
  if (event.key === "Enter" || event.key === "Tab") {
    event.preventDefault();
    const candidate = mentionOptions[activeMentionIndex];
    if (candidate) selectMention(candidate);
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

function selectMention(candidate: MentionCandidate): void {
  if (!mentionQuery) return;
  const applied = applyMention(
    elements.composerText.value,
    mentionQuery,
    candidate,
  );
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

function clearSelectedMention(): void {
  if (!mentionSelection) return;
  const tokenIndex = elements.composerText.value.indexOf(mentionSelection.token);
  if (tokenIndex >= 0) {
    const before = elements.composerText.value.slice(0, tokenIndex);
    const after = elements.composerText.value.slice(
      tokenIndex + mentionSelection.token.length,
    );
    elements.composerText.value =
      before.endsWith(" ") && after.startsWith(" ")
        ? `${before}${after.slice(1)}`
        : `${before}${after}`;
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

function hideMentionSuggestions(): void {
  mentionQuery = undefined;
  mentionOptions = [];
  activeMentionIndex = 0;
  elements.mentionSuggestions.hidden = true;
  elements.mentionSuggestions.replaceChildren();
  elements.composerText.setAttribute("aria-expanded", "false");
  elements.composerText.removeAttribute("aria-activedescendant");
}

function resetMentionState(): void {
  mentionSelection = undefined;
  mentionDismissed = false;
  hideMentionSuggestions();
  renderComposerContext();
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
  if (value.runtimeProfiles !== undefined) {
    if (!Array.isArray(value.runtimeProfiles) || value.runtimeProfiles.length > 128) {
      throw new Error("invalid conversation profile field: runtimeProfiles");
    }
    const ids = new Set<string>();
    for (const profile of value.runtimeProfiles) {
      boundedProfileField(profile.id, "runtimeProfiles.id", 128);
      boundedProfileField(profile.label, "runtimeProfiles.label", 256);
      boundedProfileField(
        profile.description,
        "runtimeProfiles.description",
        2_048,
      );
      if (!/^[A-Za-z0-9._-]+$/u.test(profile.id) || ids.has(profile.id)) {
        throw new Error("invalid conversation profile field: runtimeProfiles.id");
      }
      ids.add(profile.id);
    }
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
