import {
  closeLiveConversationQualification,
  liveConversationQualificationConstants,
  startLiveConversationQualification,
} from "./live-conversation-webview-harness.ts";

Object.defineProperty(globalThis, "__fleetdLiveConversationQualification", {
  configurable: true,
  value: {
    ...liveConversationQualificationConstants,
    close: closeLiveConversationQualification,
    start: startLiveConversationQualification,
  },
});
