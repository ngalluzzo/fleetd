#!/usr/bin/env python3
import json
import os
import sys


def send(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


config = json.loads(os.environ["OPENCODE_CONFIG_CONTENT"])
if config != {"model": "zai-coding-plan/glm-5.3"}:
    raise RuntimeError("typed OpenCode model route was not applied")

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
                        "loadSession": True,
                        "promptCapabilities": {},
                        "mcpCapabilities": {},
                    },
                    "agentInfo": {"name": "OpenCode", "version": "1.4.0"},
                },
            }
        )
    elif method == "session/new":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"sessionId": "opencode-session-1"},
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
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "opencode-progress",
                        "content": {"type": "text", "text": "OpenCode progress"},
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
                        "messageId": "opencode-final",
                        "content": {"type": "text", "text": "OpenCode answer"},
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
    elif request_id is not None:
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": "method not found"},
            }
        )
