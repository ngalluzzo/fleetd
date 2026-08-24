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

The plugin returns its identity and exact capabilities:

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
    "capabilities": [
      { "name": "harness.invoke", "version": 1 }
    ]
  }
}
```

The supervisor rejects identity mismatches, duplicate or malformed
capabilities, unsupported lifecycle versions, and missing required
capabilities.

Plugin and capability identifiers contain 1–128 bytes of lowercase ASCII
letters and digits in non-empty segments separated by `.` or `-`. Capability
versions are positive integers and are matched exactly; lifecycle compatibility
does not imply capability compatibility.

## Health

`fleetd.health` accepts an empty object and returns `{ "status": "ok" }`.
Health is readiness, not merely process liveness.

## Shutdown

`fleetd.shutdown` accepts an empty object and returns `{ "accepted": true }`.
The plugin must then exit within the configured shutdown deadline. fleetd kills
processes that overrun the deadline and records that the shutdown was forced.

## Notifications and errors

Plugins may emit JSON-RPC notifications. Unknown notifications remain opaque
to the lifecycle transport and can be consumed by a capability adapter.
Plugin-initiated requests are not part of lifecycle v1.

Lifecycle v1 intentionally exposes no generic domain invocation method. Each
domain operation is defined by a separately versioned capability contract.

JSON-RPC errors retain their numeric code and message. Transport framing,
malformed JSON, unknown response IDs, timeouts, and premature process exit are
supervisor failures rather than capability results.
