import json
import sys
import time

# Modes let one fixture cover both adoption paths and a runtime that cannot
# replay at all. Default advertises everything.
MODE = sys.argv[1] if len(sys.argv) > 1 else "resumable"


def send(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


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
                        "loadSession": MODE != "no-load",
                        "sessionCapabilities": (
                            {"resume": {}} if MODE == "resumable" else {}
                        ),
                        "promptCapabilities": {},
                        "mcpCapabilities": {},
                    },
                    "agentInfo": {"name": "mock-acp", "version": "1.0.0"},
                    "_meta": {"mock": {"preserved": True}},
                },
            }
        )
    elif method == "session/new":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "sessionId": "native-session-1",
                    "_meta": {"new": {"preserved": True}},
                },
            }
        )
    elif method == "session/resume":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"_meta": {"resume": {"preserved": True}}},
            }
        )
    elif method == "session/load":
        for entry in (
            {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "stored reasoning"},
            },
            {
                "sessionUpdate": "tool_call",
                "toolCallId": "stored-call-1",
                "kind": "read",
                "status": "completed",
                "rawInput": {"path": "notes.txt"},
            },
            {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "stored answer"},
            },
        ):
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": params["sessionId"],
                        "update": entry,
                    },
                }
            )
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"_meta": {"load": {"preserved": True}}},
            }
        )
    elif method == "session/prompt":
        if any(
            block.get("type") == "text" and block.get("text") == "delayed prompt"
            for block in params.get("prompt", [])
        ):
            time.sleep(0.1)
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": params["sessionId"],
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "mock answer"},
                        "mockUnknownField": {"preserved": True},
                    },
                },
            }
        )
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "stopReason": "end_turn",
                    "_meta": {"prompt": {"preserved": True}},
                },
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
