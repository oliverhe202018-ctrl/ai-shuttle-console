"""Read-only same-thread lifecycle reconstruction; never infer closure from age."""
import json
from pathlib import Path

from app.canary import same_path

ROLLOUT_TYPES = {"session_meta", "event_msg", "response_item", "turn_context", "compacted",
                 "token_usage_record", "world_state", "retained_context", "security_risk_score",
                 "inter_agent_communication", "inter_agent_communication_metadata", "dynamic_tools_updated",
                 "realtime_item"}

def _turn_id(value):
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        raise ValueError("PRODUCTION_SESSION_LIFECYCLE_UNKNOWN")
    return value


def normalized_path(value):
    text = str(value)
    if text.startswith("\\\\?\\UNC\\"):
        text = "\\\\" + text[8:]
    elif text.startswith("\\\\?\\"):
        text = text[4:]
    return Path(text)


def record_owner_thread_id(record):
    """Only schema-defined persisted ownership, never arbitrary thread-shaped data."""
    p = record.get("payload", {})
    if record.get("type") == "session_meta":
        return _turn_id(p.get("id"))
    if record.get("type") == "event_msg" and p.get("type") in ("item_started", "item_completed"):
        return _turn_id(p.get("thread_id"))
    return None


def related_thread_references(record):
    """Retain relationship/tool references without giving them ownership authority."""
    result = []
    owner_path = ("$.payload.id" if record.get("type") == "session_meta" else
                  "$.payload.thread_id" if record.get("type") == "event_msg" and
                  record.get("payload", {}).get("type") in ("item_started", "item_completed") else None)
    keys = {"thread_id", "threadId", "parent_thread_id", "forked_from_id", "sender_thread_id",
            "receiver_thread_id", "new_thread_id", "target_thread_id"}

    def walk(value, path):
        if isinstance(value, dict):
            for key, child in value.items():
                location = path + "." + key
                if key in keys and location != owner_path:
                    result.append({"classification": "RELATED_THREAD_REFERENCE", "field_path": location, "value": child})
                if key == "arguments" and isinstance(child, str):
                    try:
                        child = json.loads(child)
                    except (ValueError, TypeError):
                        continue
                walk(child, location)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, path + f"[{index}]")
    walk(record, "$")
    return result


def inspect_rollout(path, workspace, exclude_thread=None):
    """Return exact observed lifecycle evidence, or None for an unrelated thread.

    A distinct event_msg/task_started supersedes an earlier verified start in
    this rollout only. A superseded orphan has no invented terminal record.
    Evidence contradicting serial same-thread execution remains blocking.
    """
    with Path(path).open(encoding="utf-8") as stream:
        header = json.loads(stream.readline())
        if not isinstance(header, dict):
            raise ValueError("PRODUCTION_SESSION_INVENTORY_UNKNOWN")
        meta = header.get("payload", {})
        if (header.get("type") != "session_meta" or not isinstance(meta, dict)
                or any(not isinstance(meta.get(k), str) or not meta[k] for k in ("id", "cwd"))):
            raise ValueError("PRODUCTION_SESSION_INVENTORY_UNKNOWN")
        thread_id = _turn_id(meta["id"])
        if not meta["cwd"].strip():
            raise ValueError("PRODUCTION_SESSION_INVENTORY_UNKNOWN")
        if not same_path(normalized_path(meta["cwd"]), normalized_path(workspace)) or thread_id == exclude_thread:
            return None
        boundary = meta.get("subagent_history_start_ordinal")
        if boundary is not None and (type(boundary) is not int or boundary < 1 or
                meta.get("history_mode") != "paginated" or not meta.get("forked_from_id") or
                _turn_id(meta["forked_from_id"]) == thread_id or header.get("ordinal") != 0):
            raise ValueError("PRODUCTION_SESSION_INHERITED_BOUNDARY_UNKNOWN")
        references = related_thread_references(header)
        inherited = []
        previous_ordinal = 0
        turns = {}
        latest = None
        latest_started = None
        lifecycle_observed = unbound_activity = conflicting_activity = False

        def turn(tid):
            return turns.setdefault(tid, {
                "turn_id": tid, "classification": "UNKNOWN_LIVENESS",
                "start_line": None, "started_at": None, "start_verified": False,
                "terminal_line": None, "terminal_at": None,
                "terminal_reason": None, "diagnostic": None, "human_requests": {},
                "superseded_by_turn_id": None, "superseded_at_line": None})

        for line_number, line in enumerate(stream, 2):
            if not line.strip():
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("PRODUCTION_SESSION_LIFECYCLE_UNKNOWN")
            p = record.get("payload", {})
            if not isinstance(p, dict):
                raise ValueError("PRODUCTION_SESSION_LIFECYCLE_UNKNOWN")
            references.extend({**ref, "line": line_number} for ref in related_thread_references(record))
            if boundary is not None:
                ordinal = record.get("ordinal")
                if type(ordinal) is not int or ordinal != previous_ordinal + 1:
                    raise ValueError("PRODUCTION_SESSION_INHERITED_BOUNDARY_UNKNOWN")
                previous_ordinal = ordinal
                if ordinal < boundary:
                    inherited_owner = record_owner_thread_id(record)
                    if inherited_owner is not None and inherited_owner != meta["forked_from_id"]:
                        raise ValueError("PRODUCTION_SESSION_LIFECYCLE_THREAD_MISMATCH")
                    if record.get("type") == "session_meta":
                        references.append({"classification": "RELATED_THREAD_REFERENCE", "line": line_number,
                                           "field_path": "$.payload.id", "value": inherited_owner,
                                           "meaning": "declared inherited parent history"})
                    inherited.append({"line": line_number, "ordinal": ordinal, "record_type": record.get("type")})
                    continue
            owner = record_owner_thread_id(record)
            if owner is not None and owner != thread_id:
                raise ValueError("PRODUCTION_SESSION_LIFECYCLE_THREAD_MISMATCH")
            kind, tid = p.get("type"), p.get("turn_id")
            started = kind in ("task_started", "turn_started")
            terminal = kind in ("task_complete", "turn_complete", "task_aborted", "turn_aborted")
            if isinstance(tid, str) and tid in turns and not terminal and not started:
                if turns[tid]["classification"] == "SUPERSEDED_ORPHAN_TURN":
                    conflicting_activity = True

            if record.get("type") == "turn_context":
                tid = _turn_id(tid)
                turn(tid)
                latest = tid
            if started:
                tid = _turn_id(tid)
                current = turn(tid)
                verified = record.get("type") == "event_msg"
                if current["classification"] != "UNKNOWN_LIVENESS":
                    conflicting_activity = True
                if verified and not conflicting_activity:
                    for old_id, old in turns.items():
                        if (old_id != tid and old["start_verified"] and old["start_line"] < line_number
                                and old["classification"] == "UNKNOWN_LIVENESS"):
                            old.update(classification="SUPERSEDED_ORPHAN_TURN",
                                       superseded_by_turn_id=tid, superseded_at_line=line_number)
                if current["start_line"] is None or (verified and not current["start_verified"]):
                    current.update(start_line=line_number, started_at=record.get("timestamp"),
                                   start_verified=verified)
                current["classification"] = "UNKNOWN_LIVENESS"
                latest = tid
                latest_started = tid
                lifecycle_observed = True
            elif terminal:
                tid = _turn_id(tid)
                if record.get("type") != "event_msg":
                    raise ValueError("PRODUCTION_SESSION_LIFECYCLE_UNKNOWN")
                current = turn(tid)
                current.update(classification="COMPLETED" if kind in ("task_complete", "turn_complete") else "ABORTED",
                               terminal_line=line_number, terminal_at=record.get("timestamp"), terminal_reason=p.get("reason"))
                latest = latest or tid
                lifecycle_observed = True
            elif kind in ("function_call", "custom_tool_call"):
                if not any(t["classification"] == "UNKNOWN_LIVENESS" for t in turns.values()):
                    unbound_activity = True
            current = turns.get(tid if isinstance(tid, str) and tid in turns else latest_started or latest)
            if current and current["classification"] == "UNKNOWN_LIVENESS":
                if kind in ("function_call", "custom_tool_call"):
                    current["diagnostic"] = None
                    if str(p.get("name", "")).split(".")[-1] == "request_user_input" and p.get("call_id"):
                        current["human_requests"][p["call_id"]] = {"line": line_number, "kind": "request_user_input"}
                elif kind in ("function_call_output", "custom_tool_call_output"):
                    current["human_requests"].pop(p.get("call_id"), None)
                elif kind in ("request_user_input", "exec_approval_request", "apply_patch_approval_request", "request_permissions", "elicitation_request"):
                    current["human_requests"][p.get("call_id") or str(line_number)] = {"line": line_number, "kind": kind}
                elif kind == "stream_error":
                    current["diagnostic"] = "INTERRUPTED_OR_DISCONNECTED"
                elif p.get("role") == "assistant" and p.get("phase") == "final_answer":
                    current["diagnostic"] = "CRASHED_OR_INCOMPLETE"
            if record.get("type") not in ROLLOUT_TYPES:
                unbound_activity = True

        if conflicting_activity:
            for t in turns.values():
                if t["classification"] == "SUPERSEDED_ORPHAN_TURN":
                    t["classification"] = "UNKNOWN_LIVENESS"
        pending = sorted(tid for tid, t in turns.items() if t["classification"] == "UNKNOWN_LIVENESS")
        blocking = bool(pending or not lifecycle_observed or unbound_activity or conflicting_activity)
        reason = ("CONFLICTING_SAME_THREAD_ACTIVITY" if conflicting_activity else
                  "UNTERMINATED_TURN" if pending else
                  "UNBOUND_TOOL_ACTIVITY" if unbound_activity else
                  "LIFECYCLE_NOT_OBSERVED" if not lifecycle_observed else None)
        for tid in pending:
            t = turns[tid]
            if t["human_requests"]:
                t["classification"] = "ACTIVE_WAITING_HUMAN_ACTION"
            elif t["diagnostic"]:
                t["classification"] = t["diagnostic"]
        classification = turns[latest_started or latest]["classification"] if turns else "UNKNOWN_LIVENESS"
        if conflicting_activity or unbound_activity or not lifecycle_observed:
            classification = "UNKNOWN_LIVENESS"
        return {"thread_id": thread_id, "rollout_path": str(path), "turn_ids": pending,
                "classification": classification,
                "blocking": blocking, "reason": reason, "turns": list(turns.values()),
                "related_thread_references": references, "inherited_history": inherited}
