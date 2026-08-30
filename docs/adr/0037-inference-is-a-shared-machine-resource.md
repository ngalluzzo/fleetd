# ADR 0037: Inference is a shared machine resource behind backend plugins

- Status: accepted for experimental dogfood
- Date: 2026-08-28

## Context

An executable agent profile already selects a harness, model route, working
directory, and tools. A local model server was still an external prerequisite:
the operator started it separately, copied its URL into every harness profile,
and could not tell whether the selected identity was absent because the harness
failed or because inference never became ready.

Putting model-server branches in the worker would turn Fleetd into a switch over
MLX-VLM, llama.cpp, Ollama, vLLM, SGLang, and every backend that follows. Giving
each agent its own server would waste the machine resource these agents should
share. Treating a backend as a model capability would be worse: model quality,
roles, and suitable work are semantic claims that no lifecycle manifest can
establish.

The external ecosystem does have a useful narrow waist. MLX-VLM, llama.cpp,
Ollama, vLLM, and other servers expose at least part of an OpenAI-compatible
HTTP surface, while keeping different launch flags, readiness documents,
metrics, batching, caches, and hardware policy.

## Decision

**A machine contributes inference as a shared, process-backed resource. A
backend plugin owns that resource and returns one verified, credential-free
loopback route. Approved agent profiles reference it by local catalog ID.**

The experimental operational interface is
`fleetd.inference-openai@0.1.0`. Its typed method reports:

- observed backend name and version;
- the backend executable digest;
- one OpenAI-compatible loopback base URL;
- the exact model route exposed by `/v1/models`;
- the vendor-owned launch-profile digest; and
- an optional provider-native observer URL whose contents remain opaque.

Lifecycle readiness remains the common plugin `fleetd.health` operation. The
backend plugin does not return until its process is alive, its health endpoint
passes, and the configured model ID is present. It receives no ambient
environment or Fleetd credential, launches its model server directly without a
shell, and keeps that child inside the plugin process group.

Worker-profile catalog schema 2 has a private `inference_backends` registry.
Each agent profile may reference one registry ID. The machine supervisor starts
one backend instance for all running agents that reference that ID, injects the
resolved route into the harness plugin only after readiness, and stops the
backend after its last consumer stops. Backend failure stops dependent seats
before bounded restart. The browser still receives only agent-profile IDs,
labels, and descriptions.

MLX-VLM and llama.cpp are the first two independently identified integrations.
They share the lifecycle and route contract but keep distinct strict
configuration schemas and native launch policy. Their executable-shaped tests
exercise the full plugin/process/HTTP boundary against fixtures. MLX-VLM has
also completed a real-runtime Qwen qualification under this interface; the
llama.cpp proof remains open, so the contract remains experimental.

## Consequences

Several agents can share one expensive model load without sharing harness
sessions or agent identity. Switching an approved profile between backend
implementations changes the harness profile digest and therefore follows the
existing conservative session-compatibility rules.

Fleetd does not become a model server, model marketplace, prompt router, or
semantic model registry. It does not normalize provider metrics. Backend-native
metrics remain optional observer documents for external collectors.

The OpenAI-compatible route is a transport waist, not a promise of complete API
parity. A backend plugin establishes only the exact route that its qualification
exercises. Adding Responses, embeddings, image generation, remote providers, or
credential brokers requires a separately named interface or an explicit
version change.

Global placement is deliberately deferred. Once encrypted machine enrollment
exists, the same resource identity can participate in placement and routing.
Until then every admitted endpoint remains explicit loopback HTTP and the local
supervisor makes no cross-machine scheduling claim.
