"""Read-only same-thread lifecycle reconstruction; never infer closure from age."""
import json
from pathlib import Path

from app.canary import same_path


def _turn_id(value):
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        raise ValueError("PRODUCTION_SESSION_LIFECYCLE_UNKNOWN")
    return value


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
        if not same_path(meta["cwd"], workspace) or thread_id == exclude_thread:
            return None
        turns = {}
        latest = None
        latest_started = None
        lifecycle_observed = unbound_activity = conflicting_activity = False

        def turn(tid):
            return turns.setdefault(tid, {
                "turn_id": tid, "classification": "UNKNOWN_LIVENESS",
                "start_line": None, "started_at": None, "start_verified": False,
                "terminal_line": None, "terminal_at": None,
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
            if record.get("type") == "session_meta" or any(
                    container.get(key, thread_id) != thread_id
                    for container in (record, p) for key in ("thread_id", "threadId")):
                raise ValueError("PRODUCTION_SESSION_LIFECYCLE_THREAD_MISMATCH")
            kind, tid = p.get("type"), p.get("turn_id")
            terminal = kind in ("task_complete", "task_aborted", "turn_aborted")
            if isinstance(tid, str) and tid in turns and not terminal and kind != "task_started":
                if turns[tid]["classification"] == "SUPERSEDED_ORPHAN_TURN":
                    conflicting_activity = True

            if record.get("type") == "turn_context":
                tid = _turn_id(tid)
                turn(tid)
                latest = tid
            if kind == "task_started":
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
                current.update(classification="COMPLETED" if kind == "task_complete" else "ABORTED",
                               terminal_line=line_number, terminal_at=record.get("timestamp"))
                latest = latest or tid
                lifecycle_observed = True
            elif kind in ("function_call", "custom_tool_call"):
                if not any(t["classification"] == "UNKNOWN_LIVENESS" for t in turns.values()):
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
        return {"thread_id": thread_id, "rollout_path": str(path), "turn_ids": pending,
                "classification": "UNKNOWN_LIVENESS" if blocking else turns[latest_started or latest]["classification"],
                "blocking": blocking, "reason": reason, "turns": list(turns.values())}
