#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
distribution_root=$(CDPATH= cd -- "${script_dir}/../.." && pwd)

if [ -x "${distribution_root}/bin/fleetd" ]; then
    default_fleetd_bin="${distribution_root}/bin/fleetd"
    default_reference_plugin="${distribution_root}/bin/fleetd-acp-reference"
else
    default_fleetd_bin="${distribution_root}/target/debug/fleetd"
    default_reference_plugin="${distribution_root}/target/debug/fleetd-acp-reference"
fi

fleetd_bin=${FLEETD_BIN:-${default_fleetd_bin}}
reference_plugin=${FLEETD_REFERENCE_PLUGIN:-${default_reference_plugin}}
python_bin=${PYTHON_BIN:-$(command -v python3)}
demo_dir=${1:-"${TMPDIR:-/tmp}/fleetd-restart-demo-$$"}
config_path="${demo_dir}/config.json"
daemon_pid=""
worker_pid=""

cleanup() {
    if [ -n "${worker_pid}" ] && kill -0 "${worker_pid}" 2>/dev/null; then
        kill -INT "${worker_pid}" 2>/dev/null || true
        wait "${worker_pid}" 2>/dev/null || true
    fi
    if [ -n "${daemon_pid}" ] && kill -0 "${daemon_pid}" 2>/dev/null; then
        kill -INT "${daemon_pid}" 2>/dev/null || true
        wait "${daemon_pid}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

if [ -e "${demo_dir}" ]; then
    echo "demo directory already exists: ${demo_dir}" >&2
    exit 2
fi
if [ ! -x "${fleetd_bin}" ] || [ ! -x "${reference_plugin}" ]; then
    echo "build fleetd and fleetd-acp-reference first, or set FLEETD_BIN and FLEETD_REFERENCE_PLUGIN" >&2
    exit 2
fi

mkdir -p "${demo_dir}/workspace"
listen_port=$(
    "${python_bin}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
)
"${fleetd_bin}" --fleet-config "${config_path}" init --listen "127.0.0.1:${listen_port}" > "${demo_dir}/init.json"

start_daemon() {
    "${fleetd_bin}" --fleet-config "${config_path}" serve > "${demo_dir}/daemon-$1.log" 2>&1 &
    daemon_pid=$!
    "${python_bin}" - "${listen_port}" <<'PY'
import sys
import time
import urllib.request

url = f"http://127.0.0.1:{sys.argv[1]}/health"
for _ in range(100):
    try:
        with urllib.request.urlopen(url, timeout=0.2) as response:
            if response.status == 200:
                raise SystemExit(0)
    except Exception:
        time.sleep(0.05)
raise SystemExit(f"daemon did not become healthy at {url}")
PY
}

start_worker() {
    "${fleetd_bin}" --fleet-config "${config_path}" worker run --config "${demo_dir}/worker.json" > "${demo_dir}/worker-$1.log" 2>&1 &
    worker_pid=$!
}

wait_for_result_count() {
    expected_count=$1
    for _attempt in $(seq 1 200); do
        "${fleetd_bin}" --fleet-config "${config_path}" --token-file "${demo_dir}/sender.token" message list \
            --channel "${channel_id}" --limit 100 > "${demo_dir}/history.json"
        if "${python_bin}" - "${demo_dir}/history.json" "${expected_count}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    history = json.load(source)
count = sum(message["kind"] == "work.result/v1" for message in history["messages"])
raise SystemExit(0 if count >= int(sys.argv[2]) else 1)
PY
        then
            return 0
        fi
        sleep 0.05
    done
    echo "timed out waiting for ${expected_count} result messages" >&2
    return 1
}

start_daemon 1
"${fleetd_bin}" --fleet-config "${config_path}" agent add --name demo-sender \
    --credential-file "${demo_dir}/sender.token" > "${demo_dir}/sender.json"
"${fleetd_bin}" --fleet-config "${config_path}" agent add --name demo-worker \
    --credential-file "${demo_dir}/worker.token" > "${demo_dir}/worker-agent.json"
sender_id=$("${python_bin}" -c 'import json,sys; print(json.load(open(sys.argv[1]))["agent"]["id"])' "${demo_dir}/sender.json")
worker_id=$("${python_bin}" -c 'import json,sys; print(json.load(open(sys.argv[1]))["agent"]["id"])' "${demo_dir}/worker-agent.json")
"${fleetd_bin}" --fleet-config "${config_path}" channel create --name restart-demo \
    --member "${sender_id}" --member "${worker_id}" > "${demo_dir}/channel.json"
channel_id=$("${python_bin}" -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "${demo_dir}/channel.json")

"${python_bin}" - "${demo_dir}/worker.json" "${worker_id}" "${demo_dir}/workspace" \
    "${reference_plugin}" "${python_bin}" "${script_dir}/mock-acp-agent.py" \
    "${demo_dir}/adoption.log" <<'PY'
import json
import sys

output, agent_id, workspace, plugin, python, mock, adoption_log = sys.argv[1:]
config = {
    "schema_version": 2,
    "agent_id": agent_id,
    "working_directory": workspace,
    "adapter": {
        "kind": "envelope",
        "inbound": {"schema_version": 1, "message_kinds": ["work.request/v1"]},
    },
    "plugin": {
        "id": "fleetd.acp-reference",
        "executable": plugin,
        "config": {
            "profile_digest": "fleetd-restart-demo/v1",
            "runtime": {
                "expected_name": "fleetd-restart-demo",
                "expected_version": "1.0.0",
                "executable": python,
                "identity_path": mock,
                "args": [mock, adoption_log],
                "environment": {},
            },
        },
    },
    "result_kind": "work.result/v1",
    "lease_duration_ms": 75000,
    "poll_interval_ms": 50,
    "restart_backoff_ms": 50,
    "pre_arm_retry_delay_ms": 50,
    "turn": {
        "idle_timeout_ms": 5000,
        "wall_timeout_ms": 10000,
        "cancel_drain_timeout_ms": 1000,
        "max_captured_output_bytes": 65536,
        "tool_budget": 8,
    },
}
with open(output, "w", encoding="utf-8") as destination:
    json.dump(config, destination, indent=2)
    destination.write("\n")
PY

start_worker 1
"${fleetd_bin}" --fleet-config "${config_path}" --token-file "${demo_dir}/sender.token" message send \
    --channel "${channel_id}" --to "${worker_id}" --kind work.request/v1 \
    --payload '{"task":"before the crash"}' --idempotency-key demo/before-crash > "${demo_dir}/first-request.json"
wait_for_result_count 1

kill -KILL "${worker_pid}"
wait "${worker_pid}" 2>/dev/null || true
worker_pid=""
kill -KILL "${daemon_pid}"
wait "${daemon_pid}" 2>/dev/null || true
daemon_pid=""

start_daemon 2
start_worker 2
"${fleetd_bin}" --fleet-config "${config_path}" --token-file "${demo_dir}/sender.token" message send \
    --channel "${channel_id}" --to "${worker_id}" --kind work.request/v1 \
    --payload '{"task":"after the crash"}' --idempotency-key demo/after-crash > "${demo_dir}/second-request.json"
wait_for_result_count 2

"${fleetd_bin}" --fleet-config "${config_path}" invocation list --agent "${worker_id}" > "${demo_dir}/invocations.json"
latest_invocation=$("${python_bin}" -c 'import json,sys; print(json.load(open(sys.argv[1]))[0]["id"])' "${demo_dir}/invocations.json")
"${fleetd_bin}" --fleet-config "${config_path}" trace --invocation "${latest_invocation}" > "${demo_dir}/trace.json"
"${fleetd_bin}" --fleet-config "${config_path}" status --agent "${worker_id}" > "${demo_dir}/status.json"

"${python_bin}" - "${demo_dir}/trace.json" "${demo_dir}/status.json" "${demo_dir}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    trace = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    status = json.load(source)
observation = trace["observation"]
session = trace["session"]
generation = status["current_plugin_generations"][0]
assert trace["invocation"]["execution_certainty"] == "outcome_known"
assert observation["owner_epoch"] >= 2
assert session["binding"]["owner_epoch"] >= 2
assert generation["health"] == "active"

# Adoption must restore the session without replaying its conversation. ACP
# obliges `session/load` to stream every prior entry before it answers, and a
# replayed entry belongs to no invocation, so asking for one would be asking
# for evidence there is no honest place to put.
adoption_log = f"{sys.argv[3]}/adoption.log"
with open(adoption_log, encoding="utf-8") as source:
    adoption = [line.strip() for line in source if line.strip()]
assert adoption, "the replacement worker never adopted the native session"
assert all(method == "session/resume" for method in adoption), (
    f"adoption replayed a transcript it cannot attribute: {adoption}"
)
print("fleetd crash/restart demonstration passed")
print(f"  session adoption: {', '.join(adoption)}")
print(f"  native session: {session['session_ref']}")
print(f"  owner epoch after restart: {observation['owner_epoch']}")
print(f"  latest invocation certainty: {trace['invocation']['execution_certainty']}")
print(f"  current plugin health: {generation['health']}")
print(f"  durable evidence: {sys.argv[3]}")
PY

# A transcript read goes through a second, short-lived plugin process while the
# seat still owns the session. It must use `session/load`, the only ACP method
# that replays, where adoption used `session/resume`, and it must leave the
# seat's ownership of that session intact.
session_ref=$("${python_bin}" -c 'import json,sys; print(json.load(open(sys.argv[1]))["session"]["session_ref"])' "${demo_dir}/trace.json")
"${fleetd_bin}" --fleet-config "${config_path}" transcript \
    --config "${demo_dir}/worker.json" --session "${session_ref}" > "${demo_dir}/transcript.json"
"${fleetd_bin}" --fleet-config "${config_path}" status --agent "${worker_id}" \
    > "${demo_dir}/status-after-read.json"

"${python_bin}" - "${demo_dir}/transcript.json" "${demo_dir}/adoption.log" \
    "${session_ref}" "${demo_dir}/status-after-read.json" <<'CHECK'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    transcript = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    adoption = [line.strip() for line in source if line.strip()]
with open(sys.argv[4], encoding="utf-8") as source:
    status = json.load(source)

assert transcript["session_ref"] == sys.argv[3], transcript["session_ref"]
turns = transcript["turns"]
classifications = [e["classification"] for turn in turns for e in turn["entries"]]
assert classifications == ["reasoning_content", "agent_message_content"], classifications
assert transcript["complete"]["entry_count"] == 2, transcript["complete"]
assert transcript["complete"]["truncated"] is False, transcript["complete"]

# The demo's mock replays no prompt, so its entries belong to no dispatched
# turn. Grouping must say so rather than inventing an attribution.
assert len(turns) == 1, turns
assert turns[0]["invocation_id"] is None, turns[0]
assert transcript["attributed_turns"] == 0, transcript["attributed_turns"]

# Adoption resumed; only the retrieval loaded. One log, two purposes.
assert adoption.count("session/load") == 1, adoption
assert adoption[-1] == "session/load", adoption
assert all(method == "session/resume" for method in adoption[:-1]), adoption

# Reading must not retire the ownership the seat still holds.
binding = status["current_session_bindings"][0]
assert binding["session_ref"] == sys.argv[3], binding
assert binding["state"] == "ready", binding

print(f"  transcript read: {transcript['complete']['entry_count']} entries in "
      f"{len(turns)} turn(s), {transcript['attributed_turns']} attributed")
print(f"  adoption then retrieval: {', '.join(adoption)}")
print(f"  seat after being read: {binding['state']}, session retained")
CHECK
