import {
  qualificationConstants,
  startAliasQualification,
  startForeignQualification,
  startSameOriginQualification,
} from "./browser-channel-stream-webview-harness.ts";

Object.defineProperty(globalThis, "__fleetdBrowserChannelStreamClient", {
  configurable: true,
  value: {
    ...qualificationConstants,
    startAlias: startAliasQualification,
    startForeign: startForeignQualification,
    startSameOrigin: startSameOriginQualification,
  },
});
