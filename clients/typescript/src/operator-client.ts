import {
  addChannelMember,
  archiveChannel,
  createChannel,
  listAgents,
  listConversations,
  listPluginGenerations,
  listSessionBindings,
  openDirectConversation,
  renameChannel,
} from "./generated/sdk.gen.ts";
import { createClient } from "./generated/client/index.ts";
import type {
  AddMember,
  Agent,
  Channel,
  ConversationSummary,
  CreateChannel,
  ErrorResponse,
  ListConversationsData,
  ListPluginGenerationsData,
  ListSessionBindingsData,
  OpenDirectConversation,
  PluginGeneration,
  RenameChannel,
  SessionBinding,
} from "./generated/types.gen.ts";
import {
  boundedCredential,
  boundedRequestTimeout,
  exactHttpOrigin,
} from "./client-options.ts";

export type ListConversationsOptions = NonNullable<
  ListConversationsData["query"]
>;

export type ListPluginGenerationsOptions = NonNullable<
  ListPluginGenerationsData["query"]
>;

export type ListSessionBindingsOptions = NonNullable<
  ListSessionBindingsData["query"]
>;

/**
 * Operator-owned collaboration lifecycle and operational read models.
 *
 * Shared conversations and direct conversations remain distinct operations.
 * Health records are returned without deriving synthetic presence or status.
 */
export interface FleetdOperatorClient {
  listAgents(): Promise<readonly Agent[]>;
  listConversations(
    options?: ListConversationsOptions,
  ): Promise<readonly ConversationSummary[]>;
  createSharedChannel(input: CreateChannel): Promise<Channel>;
  renameSharedChannel(channelId: string, input: RenameChannel): Promise<Channel>;
  archiveSharedChannel(channelId: string): Promise<Channel>;
  addSharedChannelMember(
    channelId: string,
    input: AddMember,
  ): Promise<void>;
  openDirectConversation(
    input: OpenDirectConversation,
  ): Promise<ConversationSummary>;
  listPluginGenerations(
    options?: ListPluginGenerationsOptions,
  ): Promise<readonly PluginGeneration[]>;
  listSessionBindings(
    options?: ListSessionBindingsOptions,
  ): Promise<readonly SessionBinding[]>;
  close(): void;
}

export interface FleetdOperatorClientOptions {
  origin: string;
  operatorCredential: string;
  requestTimeoutMs?: number;
  fetch?: typeof globalThis.fetch;
}

/** A public API failure with an inspectable HTTP status and Fleetd error body. */
export class FleetdOperatorClientError extends Error {
  readonly status: number | null;
  readonly body: ErrorResponse | null;

  constructor(
    operation: string,
    status: number | null,
    body: ErrorResponse | null,
    cause?: unknown,
  ) {
    super(
      status === null
        ? `Fleetd ${operation} request failed before receiving a response`
        : `Fleetd ${operation} request failed with HTTP ${status}`,
      cause === undefined ? undefined : { cause },
    );
    this.name = "FleetdOperatorClientError";
    this.status = status;
    this.body = body;
  }
}

type GeneratedResult<T> = {
  data?: T;
  error?: ErrorResponse;
  response?: Response;
};

/**
 * Creates an isolated generated Fetch client for operator collaboration work.
 * The credential is closure-owned, never enters a URL or request body, and is
 * overwritten after all active requests are aborted on `close`.
 */
export function createFleetdOperatorClient(
  options: FleetdOperatorClientOptions,
): FleetdOperatorClient {
  const origin = exactHttpOrigin(options.origin);
  let operatorCredential = boundedCredential(
    options.operatorCredential,
    "operatorCredential",
  );
  const requestTimeoutMs = boundedRequestTimeout(options.requestTimeoutMs);
  const fetchImplementation: typeof globalThis.fetch =
    options.fetch ?? ((input, init) => globalThis.fetch(input, init));
  const wireClient = createClient({
    auth: () => operatorCredential,
    baseUrl: origin,
    fetch: fetchImplementation,
  });
  const activeRequests = new Set<AbortController>();
  let closed = false;

  const execute = async <T>(
    operation: string,
    invoke: (signal: AbortSignal) => Promise<GeneratedResult<T>>,
  ): Promise<T> => {
    if (closed) throw new Error("Fleetd operator client is closed");
    const controller = new AbortController();
    activeRequests.add(controller);
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const result = await invoke(controller.signal);
      if (result.error === undefined && result.response?.ok) {
        return result.data as T;
      }
      throw new FleetdOperatorClientError(
        operation,
        result.response?.status ?? null,
        result.error ?? null,
      );
    } catch (cause) {
      if (cause instanceof FleetdOperatorClientError) throw cause;
      throw new FleetdOperatorClientError(operation, null, null, cause);
    } finally {
      clearTimeout(timeout);
      activeRequests.delete(controller);
    }
  };

  return {
    listAgents: () =>
      execute<Agent[]>("list agents", (signal) =>
        listAgents({ client: wireClient, signal }),
      ),
    listConversations: (query) =>
      execute<ConversationSummary[]>("list conversations", (signal) =>
        listConversations({ client: wireClient, query, signal }),
      ),
    createSharedChannel: (body) =>
      execute<Channel>("create shared channel", (signal) =>
        createChannel({ body, client: wireClient, signal }),
      ),
    renameSharedChannel: (channelId, body) =>
      execute<Channel>("rename shared channel", (signal) =>
        renameChannel({
          body,
          client: wireClient,
          path: { channel_id: channelId },
          signal,
        }),
      ),
    archiveSharedChannel: (channelId) =>
      execute<Channel>("archive shared channel", (signal) =>
        archiveChannel({
          client: wireClient,
          path: { channel_id: channelId },
          signal,
        }),
      ),
    addSharedChannelMember: (channelId, body) =>
      execute<void>("add shared channel member", (signal) =>
        addChannelMember({
          body,
          client: wireClient,
          path: { channel_id: channelId },
          signal,
        }),
      ),
    openDirectConversation: (body) =>
      execute<ConversationSummary>("open direct conversation", (signal) =>
        openDirectConversation({ body, client: wireClient, signal }),
      ),
    listPluginGenerations: (query) =>
      execute<PluginGeneration[]>("list plugin generations", (signal) =>
        listPluginGenerations({ client: wireClient, query, signal }),
      ),
    listSessionBindings: (query) =>
      execute<SessionBinding[]>("list session bindings", (signal) =>
        listSessionBindings({ client: wireClient, query, signal }),
      ),
    close() {
      if (closed) return;
      closed = true;
      for (const request of activeRequests) request.abort();
      activeRequests.clear();
      operatorCredential = "";
    },
  };
}
