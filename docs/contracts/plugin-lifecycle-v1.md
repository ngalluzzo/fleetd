# Plugin lifecycle protocol v1

The transport is JSON-RPC 2.0 with one UTF-8 JSON object per line. Lines are
bounded to one MiB. Plugin standard output must contain no prose or logging.

## Initialize

The host's first request is `fleetd.initialize`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "fleetd.initialize",
  "params": {
    "protocol_version": 1,
    "instance_id": "uuid",
    "host_version": "0.1.0",
    "config": {}
  }
}
```

The plugin returns its identity and one exact GOOIR capability offer set:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocol_version": 1,
    "plugin": {
      "id": "example.harness",
      "name": "Example harness",
      "version": "1.2.0"
    },
    "capability_offers": {
      "protocol": "org.gooi.capability.offers/v1",
      "package": {
        "package": "example.harness",
        "name": "package",
        "version": "1.2.0"
      },
      "offers": [
        {
          "implementation": {
            "package": "example.harness",
            "name": "turn_execute",
            "version": "1.2.0"
          },
          "capability": {
            "package": "org.gooi.capability.agent_session",
            "name": "turn_execute",
            "version": "1.0.0"
          },
          "implementation_digest": "sha256:..."
        }
      ]
    }
  }
}
```

The supervisor rejects identity mismatches, duplicate or malformed offers,
unsupported lifecycle versions, and missing exact required capabilities.
Lifecycle compatibility does not imply capability compatibility. One package
may offer several implementations, and lifecycle transport names do not imply
semantic capabilities.

## Health

`fleetd.health` accepts an empty object and returns `{ "status": "ok" }`.
Health is readiness, not merely process liveness.

## Shutdown

`fleetd.shutdown` accepts an empty object and returns `{ "accepted": true }`.
The plugin must then exit within the configured shutdown deadline. fleetd kills
processes that overrun the deadline and records that the shutdown was forced.

## Notifications and errors

Plugins may emit JSON-RPC notifications. Unknown notifications remain opaque
to the lifecycle transport and can be consumed by a typed plugin client.
Plugin-initiated requests are not part of lifecycle v1.

Lifecycle v1 intentionally exposes no generic domain invocation method. Each
domain operation is defined by a separately versioned capability contract.

JSON-RPC errors retain their numeric code and message. Transport framing,
malformed JSON, unknown response IDs, timeouts, and premature process exit are
supervisor failures rather than capability results.
