#!/usr/bin/env python3
import json
import os
import sys


def send(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


if (
    sys.argv[1:4] != ["--profile", "acp", "--patch"]
    or len(sys.argv) != 5
    or not os.path.isabs(sys.argv[4])
):
    raise RuntimeError("DeepSeek Harness must launch the official ACP profile with one generated patch")
if os.environ.get("DSH_PERMISSION_MODE") != "workspace-write":
    raise RuntimeError("DeepSeek Harness must start in workspace-write mode")
if os.environ.get("DSH_TELEMETRY_DISABLED") != "1":
    raise RuntimeError("DeepSeek Harness telemetry must be disabled")
if not os.path.isabs(os.environ.get("DSH_HOME", "")):
    raise RuntimeError("DeepSeek Harness requires an explicit DSH_HOME")
if "DEEPSEEK_API_KEY" in os.environ:
    raise RuntimeError("raw provider credentials must not cross plugin launch")

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params", {})
    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": 1,
                    "agentCapabilities": {
                        "loadSession": False,
                        "sessionCapabilities": {"resume": {}},
                        "promptCapabilities": {},
                        "mcpCapabilities": {"http": {}},
                    },
                    "agentInfo": {
                        "name": "deepseek-harness-acp",
                        "version": "0.0.1",
                    },
                },
            }
        )
    elif method == "session/new":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "sessionId": "deepseek-session-1",
                    "configOptions": [],
                },
            }
        )
    elif method == "session/resume":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"configOptions": []},
            }
        )
    elif method == "session/prompt":
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": params["sessionId"],
                    "update": {
                        "sessionUpdate": "agent_thought_chunk",
                        "content": {"type": "text", "text": "DeepSeek reasoning"},
                    },
                },
            }
        )
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": params["sessionId"],
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "DeepSeek answer"},
                    },
                },
            }
        )
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"stopReason": "end_turn"},
            }
        )
    elif method == "session/cancel":
        pass
    elif request_id is not None:
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": "method not found"},
            }
        )
