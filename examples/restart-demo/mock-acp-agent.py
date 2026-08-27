import json
import sys

# Adoption records the method it was asked for, so run.sh can assert that
# resuming a native session does not replay its transcript. The path arrives as
# an argument because a plugin launches its runtime with an empty environment.
ADOPTION_LOG = sys.argv[1] if len(sys.argv) > 1 else None


def record(method):
    if ADOPTION_LOG:
        with open(ADOPTION_LOG, "a", encoding="utf-8") as handle:
            handle.write(method + "\n")


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
                        "sessionCapabilities": {"resume": {}},
                    },
                    "agentInfo": {"name": "fleetd-restart-demo", "version": "1.0.0"},
                },
            }
        )
    elif method == "session/new":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"sessionId": "restart-demo-native-session"},
            }
        )
    elif method == "session/resume":
        record(method)
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/load":
        record(method)
        # ACP obliges a load to replay every stored entry before it answers.
        for entry in (
            {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "stored reasoning"},
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
                    "params": {"sessionId": params["sessionId"], "update": entry},
                }
            )
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/prompt":
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": params["sessionId"],
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {
                            "type": "text",
                            "text": "restart demo completed this durable request",
                        },
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
