import {
  BROWSER_CHANNEL_STREAM_PATH,
  BROWSER_CHANNEL_STREAM_PROTOCOL,
  openBrowserChannelStream,
} from "../clients/typescript/src/browser-channel-stream.ts";

Object.defineProperty(globalThis, "__fleetdBrowserChannelStreamClient", {
  configurable: true,
  value: {
    path: BROWSER_CHANNEL_STREAM_PATH,
    protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
    open: openBrowserChannelStream,
  },
});
