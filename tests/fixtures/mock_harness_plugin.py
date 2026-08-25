import json
import sys
from pathlib import Path


mode = sys.argv[1] if len(sys.argv) > 1 else "healthy"
marker_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
active_fence = None


def send(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def result(request, payload):
    send({"jsonrpc": "2.0", "id": request["id"], "result": payload})


initialize = json.loads(sys.stdin.readline())
result(
    initialize,
    {
        "protocol_version": 1,
        "plugin": {
            "id": "mock.harness",
            "name": "Mock ACP harness",
            "version": "0.1.0",
        },
        "interfaces": [{"id": "fleetd.harness-acp", "version": "0.1.0"}],
    },
)

if mode == "overflow":
    for sequence in range(300):
        send(
            {
                "jsonrpc": "2.0",
                "method": "mock.overflow",
                "params": {"sequence": sequence},
            }
        )

for line in sys.stdin:
    request = json.loads(line)
    method = request["method"]
    params = request.get("params", {})
    if method == "fleetd.health":
        result(request, {"status": "ok"})
    elif method == "harness.acp.describe":
        result(
            request,
            {
                "driver": {
                    "version": "0.1.0",
                    "acp_sdk_version": "2.0.0",
                    "acp_protocol_version": 1,
                },
                "runtime": {
                    "name": "mock-acp",
                    "version": "1.0.0",
                    "executable_digest": "sha256:mock",
                },
                "agent_capabilities": {"loadSession": True},
                "limits": {
                    "max_concurrent_turns": 1,
                    "max_frame_bytes": 1048576,
                },
                "profile_digest": "sha256:profile",
                "raw_initialize_result": {"extension": "preserved"},
            },
        )
    elif method == "harness.acp.session.open":
        if (
            mode == "fail-open-once"
            and marker_path is not None
            and not marker_path.exists()
        ):
            marker_path.touch()
            sys.exit(19)
        resumed = params["mode"]["kind"] == "resume"
        session_ref = params["mode"].get("session_ref", "mock-session")
        result(
            request,
            {
                "session_ref": session_ref,
                "profile_digest": params["profile_digest"],
                "resumed": resumed,
                "effective_config": {},
                "raw_session_result": {"extension": "preserved"},
            },
        )
    elif method == "harness.acp.turn.start":
        wall_enforcement = "soft" if mode == "weak-enforcement" else "hard"
        assistant_text = "done"
        result(
            request,
            {
                "accepted": True,
                "effective_enforcement": {
                    "wall_timeout": wall_enforcement,
                    "idle_timeout": "hard",
                    "cancel_drain_timeout": "hard",
                    "captured_output_bytes": "hard",
                    "tool_budget": "observe_then_cancel",
                    "token_budget": "unavailable",
                },
            },
        )
        fence = dict(params["fence"])
        if mode == "cancel-end-turn":
            active_fence = fence
            continue
        if mode == "wrong-fence":
            fence["owner_epoch"] += 1
        assistant_messages = []
        event_seq = 1
        message_id = None
        send(
            {
                "jsonrpc": "2.0",
                "method": "harness.acp.turn.event",
                "params": {
                    "fence": fence,
                    "event_seq": event_seq,
                    "observed_at_ms": 1,
                    "classification": "agent_message_content",
                    "raw_update": {
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": message_id,
                        "content": {"type": "text", "text": assistant_text},
                        "unknownExtension": {"preserved": True},
                    },
                },
            }
        )
        assistant_messages.append(
            {
                "message_id": message_id,
                "content": [{"type": "text", "text": assistant_text}],
                "complete": True,
                "first_event_seq": event_seq,
                "last_event_seq": event_seq,
            }
        )
        send(
            {
                "jsonrpc": "2.0",
                "method": "harness.acp.turn.terminal",
                "params": {
                    "fence": fence,
                    "last_event_seq": event_seq,
                    "stop_reason": "end_turn",
                    "execution_certainty": "outcome_known",
                    "session_quiescent": True,
                    "session_persistence": "runtime_claimed",
                    "assistant_messages": assistant_messages,
                    "usage": {},
                    "raw_prompt_response": {"stopReason": "end_turn"},
                },
            }
        )
    elif method == "harness.acp.turn.cancel":
        result(request, {"accepted": True})
        if mode == "cancel-end-turn":
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "harness.acp.turn.terminal",
                    "params": {
                        "fence": active_fence,
                        "last_event_seq": 0,
                        "stop_reason": "end_turn",
                        "execution_certainty": "outcome_known",
                        "session_quiescent": True,
                        "session_persistence": "runtime_claimed",
                        "assistant_messages": [],
                        "usage": {},
                        "raw_prompt_response": {"stopReason": "end_turn"},
                    },
                }
            )
    elif method == "harness.acp.permission.resolve":
        result(request, {"accepted": True})
    elif method == "harness.acp.session.close":
        result(
            request,
            {"ownership_retired": True, "native_resources_released": False},
        )
    elif method == "fleetd.shutdown":
        result(request, {"accepted": True})
        sys.exit(0)
    else:
        send(
            {
                "jsonrpc": "2.0",
                "id": request["id"],
                "error": {"code": -32601, "message": "method not found"},
            }
        )
