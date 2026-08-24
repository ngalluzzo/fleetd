import json
import sys


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
                        "loadSession": True,
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
    elif method == "session/load":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"_meta": {"load": {"preserved": True}}},
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
