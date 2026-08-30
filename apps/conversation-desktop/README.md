# Fleetd Conversation desktop host

This package puts Fleetd's existing `/conversation/` page in an Electrobun
native system-webview window. It is packaging only: the page still uses the
public Fleetd HTTP and browser-stream contracts, and the shared headless
`ConversationSession` still owns selection, cursor, convergence, and teardown.

The host reads one strict owner-only profile. That profile contains no bearer
values, only absolute paths to two distinct owner-only credential files:

- the operator credential for channel discovery; and
- the human participant credential for membership, streaming, and attributed
  sends.

Profile schema 2 also names three absolute local paths: the Fleetd
configuration, the `fleetd` executable, and a private approved worker-profile
catalog. The host starts `fleetd worker supervise` beside the window and passes
only each profile's ID, label, and description into the webview. The catalog's
worker and inference-backend blocks — executables, arguments, models, tools,
directories, and plugin configuration — never enter page memory.

Copy [`conversation-profile.example.json`](conversation-profile.example.json)
to the default location and restrict it before editing:

```sh
mkdir -p ~/.fleetd
cp apps/conversation-desktop/conversation-profile.example.json \
  ~/.fleetd/conversation-desktop.json
chmod 600 ~/.fleetd/conversation-desktop.json \
  ~/.fleetd/operator.token ~/.fleetd/human.token \
  ~/.fleetd/worker-profiles.json
```

Every configured path must be absolute. The origin must be an exact loopback
HTTP origin with a port, and both credential paths must resolve directly to
regular files owned by the current user with no group or other permissions.
Links and credential values containing whitespace are rejected.

Install the pinned Electrobun 2.0.1 devkit, test, and open the window:

```sh
cd apps/conversation-desktop
npm ci
npm run typecheck
npm test
npm run start
```

An alternate profile can be supplied as an absolute path without putting a
credential in the environment:

```sh
FLEETD_CONVERSATION_PROFILE=/absolute/path/to/conversation.json \
  npm run start
```

`--profile /absolute/path` is also accepted when the main process is invoked
directly. Packaged Electrobun launchers do not currently forward arbitrary
launcher arguments to the Bun main process, so the default path or the path-only
environment override is authoritative for a packaged app.

Build a native distributable with `npm run build`. The
host uses Electrobun's native renderer and does not bundle CEF. It restricts
navigation to the configured Fleetd conversation URL, injects the credentials
once after DOM readiness, clears its copies, and never logs them. Closing or
reloading the page does not create another protocol implementation inside the
host. The supervisor uses a database-adjacent process lock, so opening another
window cannot create a second local reconciler for the same fleet.
