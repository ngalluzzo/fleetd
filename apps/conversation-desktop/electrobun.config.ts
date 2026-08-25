import type { ElectrobunConfig } from "electrobun";

export default {
  app: {
    name: "Fleetd Conversation",
    identifier: "com.productcolab.fleetd-conversation",
    version: "0.1.0",
    description: "A native host for Fleetd's public conversation surface",
  },
  build: {
    mainProcess: "bun",
    bun: {
      entrypoint: "src/main.ts",
    },
    mac: {
      bundleCEF: false,
    },
    linux: {
      bundleCEF: false,
    },
    win: {
      bundleCEF: false,
    },
  },
} satisfies ElectrobunConfig;
