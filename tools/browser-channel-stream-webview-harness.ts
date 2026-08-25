import {
  BROWSER_CHANNEL_STREAM_PATH,
  BROWSER_CHANNEL_STREAM_PROTOCOL,
  openBrowserChannelStream,
} from "../clients/typescript/src/browser-channel-stream.ts";

const RESULT_ATTRIBUTE = "data-fleetd-browser-stream-qualification";

interface SecretAudit {
  setGrant(value: unknown): void;
  summarize(expectedLocation: string): Promise<Record<string, unknown>>;
}

function safeText(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Error) {
    return `${value.name}:${value.message}:${value.stack ?? ""}`;
  }
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    try {
      return String(value);
    } catch {
      return "<unrepresentable>";
    }
  }
}

function createSecretAudit(credential: string): SecretAudit {
  let grant: string | undefined;
  let inspecting = false;
  const consoleCalls: unknown[][] = [];
  const pageErrors: unknown[] = [];
  const rejectionReasons: unknown[] = [];
  const historyCalls: unknown[][] = [];
  const cookieWrites: unknown[] = [];
  const storageCalls: unknown[][] = [];
  const indexedDbCalls: unknown[][] = [];
  const cacheCalls: unknown[][] = [];
  const serviceWorkerCalls: unknown[][] = [];

  const originalConsole = new Map<string, (...args: unknown[]) => void>();
  let consoleMethodsInstrumented = 0;
  for (const method of ["debug", "error", "info", "log", "trace", "warn"]) {
    const candidate = Reflect.get(console, method);
    if (typeof candidate !== "function") continue;
    originalConsole.set(method, candidate.bind(console));
    if (
      Reflect.set(console, method, (...args: unknown[]) => {
        consoleCalls.push(args);
      })
    ) {
      consoleMethodsInstrumented += 1;
    }
  }

  addEventListener("error", (event) => {
    pageErrors.push([event.message, event.filename, event.error]);
    event.preventDefault();
  });
  addEventListener("unhandledrejection", (event) => {
    rejectionReasons.push(event.reason);
    event.preventDefault();
  });

  let historyMethodsInstrumented = 0;
  for (const method of ["pushState", "replaceState"] as const) {
    const original = history[method].bind(history);
    history[method] = ((...args: unknown[]) => {
      historyCalls.push(args);
      return Reflect.apply(original, history, args);
    }) as typeof history[typeof method];
    historyMethodsInstrumented += 1;
  }

  const storagePrototype = Reflect.getPrototypeOf(localStorage) as Storage;
  let storageMethodsInstrumented = 0;
  for (const method of ["clear", "removeItem", "setItem"] as const) {
    const original = storagePrototype[method];
    if (
      Reflect.set(storagePrototype, method, function (...args: unknown[]) {
        if (!inspecting) storageCalls.push([method, ...args]);
        return Reflect.apply(original, this, args);
      })
    ) {
      storageMethodsInstrumented += 1;
    }
  }

  let cookieSetterInstrumented = false;
  const cookieDescriptor = Object.getOwnPropertyDescriptor(
    Document.prototype,
    "cookie",
  );
  if (cookieDescriptor?.get && cookieDescriptor.set && cookieDescriptor.configurable) {
    Object.defineProperty(Document.prototype, "cookie", {
      configurable: true,
      enumerable: cookieDescriptor.enumerable,
      get: cookieDescriptor.get,
      set(value: string) {
        cookieWrites.push(value);
        return Reflect.apply(cookieDescriptor.set!, this, [value]);
      },
    });
    cookieSetterInstrumented = true;
  }

  let indexedDbInstrumented = false;
  if (typeof indexedDB !== "undefined") {
    try {
      for (const method of ["deleteDatabase", "open"] as const) {
        const original = indexedDB[method].bind(indexedDB);
        if (
          !Reflect.set(indexedDB, method, (...args: unknown[]) => {
            if (!inspecting) indexedDbCalls.push([method, ...args]);
            return Reflect.apply(original, indexedDB, args);
          })
        ) {
          throw new Error("indexedDB method is not instrumentable");
        }
      }
      indexedDbInstrumented = true;
    } catch {
      indexedDbInstrumented = false;
    }
  }

  let cacheInstrumented = false;
  if (typeof caches !== "undefined") {
    try {
      for (const method of ["delete", "match", "open"] as const) {
        const original = caches[method].bind(caches);
        if (
          !Reflect.set(caches, method, (...args: unknown[]) => {
            if (!inspecting) cacheCalls.push([method, ...args]);
            return Reflect.apply(original, caches, args);
          })
        ) {
          throw new Error("Cache API method is not instrumentable");
        }
      }
      cacheInstrumented = true;
    } catch {
      cacheInstrumented = false;
    }
  }

  let serviceWorkerInstrumented = false;
  if (navigator.serviceWorker) {
    try {
      const original = navigator.serviceWorker.register.bind(
        navigator.serviceWorker,
      );
      if (
        !Reflect.set(
          navigator.serviceWorker,
          "register",
          (...args: unknown[]) => {
          if (!inspecting) serviceWorkerCalls.push(["register", ...args]);
          return Reflect.apply(original, navigator.serviceWorker, args);
          },
        )
      ) {
        throw new Error("service worker registration is not instrumentable");
      }
      serviceWorkerInstrumented = true;
    } catch {
      serviceWorkerInstrumented = false;
    }
  }

  const containsSecret = (values: unknown[]): boolean => {
    const text = values.map(safeText).join("\n");
    return text.includes(credential) || (grant !== undefined && text.includes(grant));
  };

  const storageSnapshot = (storage: Storage) => {
    const entries: unknown[] = [];
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (key !== null) entries.push([key, storage.getItem(key)]);
    }
    return entries;
  };

  const inspectIndexedDb = async () => {
    if (typeof indexedDB === "undefined") {
      return {
        available: false,
        authoritative: false,
        reason: "indexeddb_unavailable",
        databaseCount: 0,
        secretDetected: false,
      };
    }
    const databasesMethod = Reflect.get(indexedDB, "databases");
    if (typeof databasesMethod !== "function") {
      return {
        available: true,
        authoritative: false,
        reason: "database_enumeration_unavailable",
        databaseCount: 0,
        secretDetected: false,
      };
    }
    try {
      const databases = await Reflect.apply(databasesMethod, indexedDB, []);
      return {
        available: true,
        authoritative: true,
        reason: null,
        databaseCount: databases.length,
        secretDetected: containsSecret(databases),
      };
    } catch {
      return {
        available: true,
        authoritative: false,
        reason: "database_enumeration_failed",
        databaseCount: 0,
        secretDetected: false,
      };
    }
  };

  const inspectCaches = async () => {
    if (typeof caches === "undefined" || typeof caches.keys !== "function") {
      return {
        available: false,
        authoritative: false,
        cacheCount: 0,
        secretDetected: false,
      };
    }
    try {
      const names = await caches.keys();
      const values: unknown[] = [...names];
      for (const name of names) {
        const cache = await caches.open(name);
        const requests = await cache.keys();
        for (const request of requests) {
          values.push([request.url, [...request.headers.entries()]]);
          const response = await cache.match(request);
          if (response) {
            values.push([
              response.url,
              [...response.headers.entries()],
              await response.clone().text(),
            ]);
          }
        }
      }
      return {
        available: true,
        authoritative: true,
        cacheCount: names.length,
        secretDetected: containsSecret(values),
      };
    } catch {
      return {
        available: true,
        authoritative: false,
        cacheCount: 0,
        secretDetected: false,
      };
    }
  };

  const inspectServiceWorkers = async () => {
    if (
      !navigator.serviceWorker ||
      typeof navigator.serviceWorker.getRegistrations !== "function"
    ) {
      return {
        available: false,
        authoritative: false,
        registrationCount: 0,
        secretDetected: false,
      };
    }
    try {
      const registrations = await navigator.serviceWorker.getRegistrations();
      const values = registrations.map((registration) => [
        registration.scope,
        registration.active?.scriptURL,
        registration.installing?.scriptURL,
        registration.waiting?.scriptURL,
      ]);
      return {
        available: true,
        authoritative: true,
        registrationCount: registrations.length,
        secretDetected: containsSecret(values),
      };
    } catch {
      return {
        available: true,
        authoritative: false,
        registrationCount: 0,
        secretDetected: false,
      };
    }
  };

  return {
    setGrant(value) {
      if (typeof value === "string") grant = value;
    },
    async summarize(expectedLocation) {
      inspecting = true;
      try {
        const localEntries = storageSnapshot(localStorage);
        const sessionEntries = storageSnapshot(sessionStorage);
        const indexedDb = await inspectIndexedDb();
        const cacheApi = await inspectCaches();
        const serviceWorkers = await inspectServiceWorkers();
        const locationValues = [location.href, document.referrer];
        const historyValues = [history.state, ...historyCalls];
        const cookieValues = [document.cookie, ...cookieWrites];
        const allSecretFlags = [
          containsSecret(locationValues),
          containsSecret(historyValues),
          containsSecret(cookieValues),
          containsSecret(localEntries),
          containsSecret(sessionEntries),
          containsSecret(storageCalls),
          indexedDb.secretDetected,
          containsSecret(indexedDbCalls),
          cacheApi.secretDetected,
          containsSecret(cacheCalls),
          serviceWorkers.secretDetected,
          containsSecret(serviceWorkerCalls),
          containsSecret(consoleCalls),
          containsSecret(pageErrors),
          containsSecret(rejectionReasons),
        ];
        return {
          credentialObserved: credential.length > 0,
          grantObserved: grant !== undefined,
          noSecretDetected: allSecretFlags.every((flag) => flag === false),
          instrumentation: {
            consoleMethods: consoleMethodsInstrumented,
            historyMethods: historyMethodsInstrumented,
            storageMethods: storageMethodsInstrumented,
            pageFailures: true,
          },
          location: {
            unchanged: location.href === expectedLocation,
            secretDetected: containsSecret(locationValues),
          },
          history: {
            mutationCalls: historyCalls.length,
            currentStateSecretDetected: containsSecret(historyValues),
            entryEnumerationAuthoritative: false,
          },
          cookies: {
            setterInstrumented: cookieSetterInstrumented,
            writes: cookieWrites.length,
            currentEntries: document.cookie ? document.cookie.split(";").length : 0,
            secretDetected: containsSecret(cookieValues),
          },
          localStorage: {
            entries: localEntries.length,
            writes: storageCalls.filter(([scope]) => scope === "setItem").length,
            secretDetected:
              containsSecret(localEntries) || containsSecret(storageCalls),
          },
          sessionStorage: {
            entries: sessionEntries.length,
            writes: storageCalls.filter(([scope]) => scope === "setItem").length,
            secretDetected:
              containsSecret(sessionEntries) || containsSecret(storageCalls),
          },
          indexedDb: {
            ...indexedDb,
            instrumented: indexedDbInstrumented,
            mutationCalls: indexedDbCalls.length,
          },
          cacheApi: {
            ...cacheApi,
            instrumented: cacheInstrumented,
            mutationCalls: cacheCalls.length,
          },
          serviceWorkers: {
            ...serviceWorkers,
            instrumented: serviceWorkerInstrumented,
            registrationCalls: serviceWorkerCalls.length,
          },
          console: {
            calls: consoleCalls.length,
            secretDetected: containsSecret(consoleCalls),
          },
          pageFailures: {
            errors: pageErrors.length,
            unhandledRejections: rejectionReasons.length,
            secretDetected:
              containsSecret(pageErrors) || containsSecret(rejectionReasons),
          },
        };
      } finally {
        inspecting = false;
      }
    },
  };
}

function publish(root: HTMLElement, state: Record<string, unknown>): void {
  root.setAttribute(RESULT_ATTRIBUTE, JSON.stringify(state));
}

export function startSameOriginQualification(config: {
  origin: string;
  channelId: string;
  credential: string;
  replayMessageId: string;
}): void {
  const root = document.documentElement;
  const operations: Record<string, unknown>[] = [];
  const acceptedIds: string[] = [];
  const frameTypes: string[] = [];
  const audit = createSecretAudit(config.credential);
  const state: Record<string, unknown> = {
    outcome: "pending",
    stage: "connecting",
    operations,
    acceptedIds,
    frameTypes,
    requestedProtocol: null,
    selectedProtocol: null,
    socketUrl: null,
  };
  publish(root, state);
  let finalized = false;

  const finalize = async () => {
    if (finalized) return;
    finalized = true;
    state.audit = await audit.summarize(new URL("/operator/", config.origin).href);
    state.outcome = "complete";
    publish(root, state);
  };

  const stream = openBrowserChannelStream({
    origin: config.origin,
    channelId: config.channelId,
    credential: config.credential,
    after: 0,
    reconnectDelaysMs: [],
    accept(message) {
      acceptedIds.push(message.id);
      if (message.id === config.replayMessageId) {
        state.stage = "replay_accepted";
      } else if (message.kind === "qualification.browser.live/v1") {
        state.stage = "live_accepted";
        void finalize();
      }
      publish(root, state);
    },
    async fetch(input, init) {
      const url = new URL(String(input), location.origin);
      operations.push({ kind: "fetch", method: init.method, path: url.pathname });
      publish(root, state);
      const response = await globalThis.fetch(input, init);
      if (url.pathname.endsWith("/stream-grants") && response.status === 201) {
        try {
          const body = await response.clone().json();
          audit.setGrant(body?.grant);
        } catch {
          // The actual client owns validation; the audit records only when the
          // exact grant can be observed without consuming its response.
        }
      }
      return response;
    },
    createWebSocket(url, requestedProtocol) {
      operations.push({
        kind: "websocket",
        path: new URL(url).pathname,
        protocol: requestedProtocol,
      });
      state.requestedProtocol = requestedProtocol;
      const socket = new WebSocket(url, requestedProtocol);
      socket.addEventListener("open", () => {
        state.selectedProtocol = socket.protocol;
        state.socketUrl = socket.url;
        publish(root, state);
      });
      socket.addEventListener("message", (event) => {
        try {
          frameTypes.push(JSON.parse(event.data).type ?? "unknown");
        } catch {
          frameTypes.push("invalid");
        }
        publish(root, state);
      });
      return socket;
    },
  });
  stream.closed.catch((error) => {
    if (state.outcome === "pending") {
      state.outcome = "error";
      state.errorCode = error?.code ?? "unknown";
      publish(root, state);
    }
  });
  Reflect.set(root, "__fleetdQualificationStream", stream);
}

export function startAliasQualification(config: {
  channelId: string;
  credential: string;
}): void {
  const root = document.documentElement;
  const operations: Record<string, unknown>[] = [];
  const audit = createSecretAudit(config.credential);
  const state: Record<string, unknown> = {
    outcome: "pending",
    pageOrigin: location.origin,
    operations,
    applicationFrames: 0,
    socketOpened: false,
  };
  publish(root, state);
  const stream = openBrowserChannelStream({
    origin: location.origin,
    channelId: config.channelId,
    credential: config.credential,
    after: 0,
    reconnectDelaysMs: [],
    accept() {
      state.applicationFrames = Number(state.applicationFrames) + 1;
      publish(root, state);
    },
    async fetch(input, init) {
      const url = new URL(String(input), location.origin);
      operations.push({ kind: "fetch", method: init.method, path: url.pathname });
      publish(root, state);
      const response = await globalThis.fetch(input, init);
      if (url.pathname.endsWith("/stream-grants") && response.status === 201) {
        try {
          const body = await response.clone().json();
          audit.setGrant(body?.grant);
        } catch {}
      }
      return response;
    },
    createWebSocket(url, requestedProtocol) {
      operations.push({
        kind: "websocket",
        path: new URL(url).pathname,
        protocol: requestedProtocol,
      });
      const socket = new WebSocket(url, requestedProtocol);
      socket.addEventListener("open", () => {
        state.socketOpened = true;
        publish(root, state);
      });
      socket.addEventListener("message", () => {
        state.applicationFrames = Number(state.applicationFrames) + 1;
        publish(root, state);
      });
      return socket;
    },
  });
  const finish = async (errorCode?: string) => {
    state.audit = await audit.summarize(location.href);
    state.outcome = "closed";
    if (errorCode) state.errorCode = errorCode;
    publish(root, state);
  };
  stream.closed.then(
    () => {
      void finish();
    },
    (error) => {
      void finish(error?.code ?? "unknown");
    },
  );
  Reflect.set(root, "__fleetdQualificationStream", stream);
}

export function startForeignQualification(config: {
  fleetOrigin: string;
  channelId: string;
  credential: string;
  grant: string;
}): void {
  const root = document.documentElement;
  const audit = createSecretAudit(config.credential);
  audit.setGrant(config.grant);
  const state: Record<string, unknown> = {
    outcome: "pending",
    pageOrigin: location.origin,
    adapterOperations: [],
    adapterApplicationFrames: 0,
    directSocketOpened: false,
    directApplicationFrames: 0,
    directRequestedProtocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
  };
  publish(root, state);
  let adapterTerminal = false;
  let directTerminal = false;
  let finalized = false;
  const finish = async () => {
    if (!adapterTerminal || !directTerminal) return;
    if (finalized) return;
    finalized = true;
    state.audit = await audit.summarize(location.href);
    state.outcome = "complete";
    publish(root, state);
  };

  const adapter = openBrowserChannelStream({
    origin: config.fleetOrigin,
    channelId: config.channelId,
    credential: config.credential,
    after: 0,
    reconnectDelaysMs: [],
    accept() {
      state.adapterApplicationFrames =
        Number(state.adapterApplicationFrames) + 1;
      publish(root, state);
    },
    fetch(input, init) {
      const url = new URL(String(input), location.origin);
      (state.adapterOperations as Record<string, unknown>[]).push({
        kind: "fetch",
        method: init.method,
        path: url.pathname,
      });
      publish(root, state);
      return globalThis.fetch(input, init);
    },
    createWebSocket(url, requestedProtocol) {
      (state.adapterOperations as Record<string, unknown>[]).push({
        kind: "websocket",
        path: new URL(url).pathname,
        protocol: requestedProtocol,
      });
      const socket = new WebSocket(url, requestedProtocol);
      socket.addEventListener("message", () => {
        state.adapterApplicationFrames =
          Number(state.adapterApplicationFrames) + 1;
        publish(root, state);
      });
      return socket;
    },
  });
  adapter.closed.then(
    () => {
      adapterTerminal = true;
      void finish();
    },
    () => {
      adapterTerminal = true;
      void finish();
    },
  );

  const directUrl = new URL(BROWSER_CHANNEL_STREAM_PATH, config.fleetOrigin);
  directUrl.protocol = directUrl.protocol === "https:" ? "wss:" : "ws:";
  const direct = new WebSocket(directUrl.href, BROWSER_CHANNEL_STREAM_PROTOCOL);
  direct.addEventListener("open", () => {
    state.directSocketOpened = true;
    direct.send(JSON.stringify({ type: "redeem", grant: config.grant }));
    publish(root, state);
  });
  direct.addEventListener("message", () => {
    state.directApplicationFrames = Number(state.directApplicationFrames) + 1;
    publish(root, state);
  });
  direct.addEventListener("error", () => {
    directTerminal = true;
    void finish();
  });
  direct.addEventListener("close", () => {
    directTerminal = true;
    void finish();
  });
  Reflect.set(root, "__fleetdQualificationStream", adapter);
  Reflect.set(root, "__fleetdQualificationDirectSocket", direct);
}

export const qualificationConstants = {
  path: BROWSER_CHANNEL_STREAM_PATH,
  protocol: BROWSER_CHANNEL_STREAM_PROTOCOL,
};
