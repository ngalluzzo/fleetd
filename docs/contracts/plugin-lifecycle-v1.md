# Fleetd plugin lifecycle v1

Status: implemented.

The lifecycle contract starts, identifies, health-checks, observes, and stops
one out-of-process integration. It negotiates operational wire interfaces only;
it does not advertise semantic capabilities.

## Framing

The host and plugin exchange newline-delimited JSON-RPC 2.0 on stdin/stdout.
Plugin stdout contains protocol frames only. Frames are bounded to 1 MiB.

## Initialize

The first request is `fleetd.initialize`:

```json
{
  "protocol_version": 1,
  "instance_id": "host-generated-uuid",
  "host_version": "0.1.0",
  "config": {}
}
```

The plugin returns its identity and one or more exact operational interfaces:

```json
{
  "protocol_version": 1,
  "plugin": {
    "id": "fleetd.harness.opencode",
    "name": "fleetd OpenCode harness",
    "version": "0.1.0"
  },
  "interfaces": [
    {
      "id": "fleetd.harness-acp",
      "version": "0.1.0"
    }
  ]
}
```

Interface identity and version match exactly. The supervisor rejects lifecycle
version mismatches, plugin identity mismatches, malformed or duplicate
interfaces, empty interface sets, and missing required interfaces.

An interface names a transport contract implemented by the process. It does
not imply a task, workflow, repository, review, or other semantic ability.

## Health

`fleetd.health` accepts an empty object and returns `{ "status": "ok" }`.
Health is readiness, not merely process liveness.

## Shutdown

`fleetd.shutdown` accepts an empty object and returns `{ "accepted": true }`.
The plugin must then exit within the configured shutdown deadline. Fleetd kills
the complete process group when the deadline is exceeded.

## Notifications and errors

Plugins may emit JSON-RPC notifications. The lifecycle transport preserves
unknown notifications for a typed interface client. Plugin-initiated requests
are rejected.

JSON-RPC errors retain their numeric code, message, and optional data. Framing,
malformed JSON, unknown response IDs, timeouts, and premature process exit are
supervisor failures.

Lifecycle v1 intentionally exposes no generic domain invocation method. Typed
clients such as `fleetd.harness-acp@0.1.0` define their own bounded methods.
