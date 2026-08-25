import {
  BROWSER_CHANNEL_STREAM_PROTOCOL,
  openBrowserChannelStream,
} from "../clients/typescript/src/browser-channel-stream.ts";

const RESULT_ATTRIBUTE = "data-fleetd-live-conversation-qualification";

interface StreamState {
  outcome: "pending" | "closed" | "error";
  ready: boolean;
  selectedProtocol: string | null;
  cursor: number;
  frameTypes: string[];
  messages: unknown[];
  errorCode?: string;
}

function publish(state: StreamState): void {
  document.documentElement.setAttribute(
    RESULT_ATTRIBUTE,
    JSON.stringify(state),
  );
}

/**
 * Opens the production browser adapter in an actual WebView. This harness
 * retains only public frames and deliberately publishes neither credential nor
 * single-use grant into its result state.
 */
export function startLiveConversationQualification(config: {
  origin: string;
  channelId: string;
  credential: string;
  after: number;
}): void {
  const state: StreamState = {
    outcome: "pending",
    ready: false,
    selectedProtocol: null,
    cursor: config.after,
    frameTypes: [],
    messages: [],
  };
  publish(state);

  const stream = openBrowserChannelStream({
    origin: config.origin,
    channelId: config.channelId,
    credential: config.credential,
    after: config.after,
    reconnectDelaysMs: [],
    readyTimeoutMs: 10_000,
    accept(message) {
      state.messages.push(message);
      state.cursor = message.seq;
      publish(state);
    },
    createWebSocket(url, protocol) {
      const socket = new WebSocket(url, protocol);
      socket.addEventListener("open", () => {
        state.selectedProtocol = socket.protocol;
        publish(state);
      });
      socket.addEventListener("message", (event) => {
        try {
          const frame = JSON.parse(String(event.data));
          const type = typeof frame?.type === "string" ? frame.type : "unknown";
          state.frameTypes.push(type);
          if (type === "ready") state.ready = true;
        } catch {
          state.frameTypes.push("invalid");
        }
        publish(state);
      });
      return socket;
    },
  });

  stream.closed.then(
    () => {
      state.cursor = stream.cursor;
      state.outcome = "closed";
      publish(state);
    },
    (error) => {
      state.cursor = stream.cursor;
      state.outcome = "error";
      state.errorCode = error?.code ?? "unknown";
      publish(state);
    },
  );
  Reflect.set(
    document.documentElement,
    "__fleetdLiveConversationStream",
    stream,
  );
}

export function closeLiveConversationQualification(): void {
  const stream = Reflect.get(
    document.documentElement,
    "__fleetdLiveConversationStream",
  );
  stream?.close();
}

export const liveConversationQualificationConstants = {
  protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
  resultAttribute: RESULT_ATTRIBUTE,
};
