# Experimental OpenAI-compatible inference plugin contract 0.1

Status: draft, exercised by two integration packages and one fresh MLX-VLM
real-runtime qualification.

Interface identity: `fleetd.inference-openai@0.1.0`.

This is an operational transport interface. It says that one plugin-owned
process group exposes one ready OpenAI-compatible route. It says nothing about
model intelligence, quality, roles, tools, suitable work, or full compatibility
with every OpenAI API operation.

## Lifecycle

The plugin uses the common lifecycle v1 transport. `fleetd.initialize` receives
only the plugin's strict private configuration. Initialization succeeds after
all of the following are true:

1. the configured backend executable resolves to a file;
2. a bounded version probe contains the exact expected version;
3. the server child starts with an empty environment plus the integration's
   explicit allowlist;
4. the credential-free loopback health URL succeeds; and
5. the OpenAI-compatible `/v1/models` document contains the configured model
   ID exactly.

`fleetd.health` repeats the process, health, and model-route checks. A response
other than `{"status":"ok"}` makes the host retire the backend and its
dependent agent seats. `fleetd.shutdown` terminates the server child and exits;
the outer plugin host owns the complete process group and force-cleans an
overrun.

## Describe

Request:

```json
{"jsonrpc":"2.0","id":2,"method":"inference.openai.describe","params":{}}
```

Result:

```json
{
  "backend": {
    "name": "MLX-VLM",
    "version": "0.6.15",
    "executable_digest": "sha256:..."
  },
  "endpoint": {
    "base_url": "http://127.0.0.1:18082/v1",
    "model": {
      "id": "/models/qwen",
      "name": "Local Qwen",
      "revision": null
    }
  },
  "profile_digest": "sha256:...",
  "observer": {
    "url": "http://127.0.0.1:18082/metrics",
    "media_type": "application/json"
  }
}
```

Every URL must use plain HTTP with an explicit loopback IP and port and may not
contain credentials, a query, or a fragment. The observer is optional and its
document remains provider-native opaque evidence. `revision: null` means the
plugin could not establish a content-addressed model revision; it is not a zero
or an implied latest revision.

The backend executable and profile digests use lowercase SHA-256. The profile
digest covers the integration identity, executable identity, model path or
route, and every launch field whose change can affect runtime behavior.

## Composition

Worker-profile catalog schema 2 declares backend plugins once and lets several
agent profiles reference one `inference_backend` ID. The supervisor starts the
backend before its first dependent harness and injects this exact describe
result into the harness plugin's private configuration under `inference`.
Profiles may not pre-resolve that field.

The current OpenCode integration converts the resolved route to its native
`@ai-sdk/openai-compatible` provider configuration. That mapping remains
OpenCode-owned. Another harness must implement its own strict mapping rather
than asking the supervisor to write vendor configuration.

Backend-native generation defaults remain plugin configuration rather than
fields in this transport contract. For example, MLX-VLM owns its thinking-mode
and thinking-budget launch flags, while OpenCode owns the compatible
`reasoning_effort` request option. The shared interface neither advertises nor
normalizes those semantics.

## Deliberate omissions

- no provider credentials or remote endpoints;
- no normalized queue, cache, token, throughput, or cost schema;
- no generic command, arbitrary arguments, or environment map;
- no model download or marketplace operation;
- no semantic capability declarations;
- no global placement, load balancing, or remote machine transport; and
- no stability claim until MLX-VLM and llama.cpp pass fresh real-runtime
  qualifications through this exact interface.

The first real-runtime record is
[MLX-VLM with Qwen3.8 27B](../qualification/inference-mlx-vlm-qwen-2026-08-28.md).
