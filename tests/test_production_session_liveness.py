"""Full-lifecycle quiescence regressions; only synthetic files are used."""
import json
import os
import sqlite3

import pytest

from app.transactional_c2g import active_production_sessions


def event(kind, turn="turn-one", **extra):
    return {"type": "event_msg", "payload": {"type": kind, "turn_id": turn, **extra}}


def rollout(tmp_path, events, name="one", **metadata):
    sessions = tmp_path / "sessions"
    sessions.mkdir(exist_ok=True)
    path = sessions / (name + ".jsonl")
    header = {"type": "session_meta", "payload": {
        "id": name, "cwd": str(tmp_path / "workspace"), **metadata}}
    path.write_text("\n".join(json.dumps(r) for r in [header, *events]) + "\n", encoding="utf-8")
    return path


def blockers(tmp_path):
    return active_production_sessions(tmp_path / "workspace", tmp_path / "sessions")


def test_unknown_lifecycle_does_not_become_idle(tmp_path):
    rollout(tmp_path, [event("token_count")])
    assert blockers(tmp_path)[0]["classification"] == "UNKNOWN_LIVENESS"


@pytest.mark.parametrize("field", ["id", "cwd"])
def test_incomplete_session_identity_cannot_be_skipped(tmp_path, field):
    path = rollout(tmp_path, [event("task_started"), event("task_complete")])
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    del rows[0]["payload"][field]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    with pytest.raises(ValueError, match="INVENTORY_UNKNOWN"):
        blockers(tmp_path)


def test_historical_orphan_is_not_claimed_to_be_current_execution(tmp_path):
    rollout(tmp_path, [event("task_started", "orphan"), event("task_started"), event("task_complete")])
    result = blockers(tmp_path)
    assert result[0]["turn_ids"] == ["orphan"]
    assert result[0]["classification"] == "UNKNOWN_LIVENESS"


@pytest.mark.parametrize("padding", [0, 2500])
def test_recent_completed_even_outside_tail(tmp_path, padding):
    rollout(tmp_path, [event("task_started"), event("task_complete"),
                       *[event("token_count") for _ in range(padding)]])
    assert blockers(tmp_path) == []


def test_stale_database_recency_is_not_liveness(tmp_path):
    path = rollout(tmp_path, [event("task_started"), event("task_complete")],
                   updated_at="2999-01-01", status="inProgress")
    (tmp_path / "sessions" / "index.json").write_text('{"updated_at": "2999", "status": "inProgress"}')
    os.utime(path, None)
    assert blockers(tmp_path) == []


@pytest.mark.parametrize("activity", [None, "function_call", "custom_tool_call", "request_user_input"])
def test_unfinished_execution_tool_or_human_wait_still_blocks(tmp_path, activity):
    events = [event("task_started")]
    if activity:
        events.append({"type": "response_item", "payload": {
            "type": "function_call" if activity == "request_user_input" else activity,
            "name": activity, "call_id": "call-one"}})
    rollout(tmp_path, events)
    assert blockers(tmp_path)


@pytest.mark.parametrize("terminal", ["task_aborted", "turn_aborted"])
def test_exact_aborted_turn_is_not_active(tmp_path, terminal):
    rollout(tmp_path, [event("task_started"), event(terminal)])
    assert blockers(tmp_path) == []


def test_queued_not_dispatched_does_not_invent_execution(tmp_path):
    from app.adapters import inspect_codex_queue_storage
    rollout(tmp_path, [event("task_started"), event("task_complete")])
    # Queue presence alone is not a task_started lifecycle event. The transaction
    # has no queue-dispatch capability, so a closed rollout remains closed.
    with sqlite3.connect(tmp_path / "queue_1.sqlite") as db:
        db.execute("CREATE TABLE queued_items(id,thread_id,payload_json,queue_order,created_at_ms,updated_at_ms)")
        db.execute("INSERT INTO queued_items VALUES(?,?,?,?,?,?)",
                   ("queued-one", "one", '{"UserInput":{"content":[{"text":"queued task"}]}}', 1, 1, 1))
    assert inspect_codex_queue_storage("one", codex_home=tmp_path)["count"] == 1
    assert blockers(tmp_path) == []


def test_new_turn_context_without_started_event_is_unknown(tmp_path):
    rollout(tmp_path, [event("task_started"), event("task_complete"),
                       {"type": "turn_context", "payload": {"turn_id": "new-turn"}}])
    assert blockers(tmp_path)


def test_function_activity_without_new_lifecycle_is_unknown(tmp_path):
    rollout(tmp_path, [event("task_started"), event("task_complete"),
                       {"type": "response_item", "payload": {"type": "function_call", "call_id": "unbound-call"}}])
    assert blockers(tmp_path)


@pytest.mark.parametrize("terminal", ["task_complete", "task_aborted"])
def test_unbound_terminal_does_not_prove_idle(tmp_path, terminal):
    rollout(tmp_path, [event(terminal, None)])
    with pytest.raises(ValueError, match="LIFECYCLE_UNKNOWN"):
        blockers(tmp_path)


@pytest.mark.parametrize("damage", ["missing", "corrupt", "partial"])
def test_unreadable_inventory_fails_closed(tmp_path, damage):
    if damage != "missing":
        path = rollout(tmp_path, [event("task_started")])
        if damage == "corrupt":
            path.write_text("invalid json", encoding="utf-8")
        else:
            with path.open("a", encoding="utf-8") as stream:
                stream.write('{"type":')
    with pytest.raises((ValueError, OSError)):
        blockers(tmp_path)


@pytest.mark.parametrize("unfinished", [False, True])
def test_multiple_sessions_require_every_session_closed(tmp_path, unfinished):
    rollout(tmp_path, [event("task_started"), event("task_complete")])
    rollout(tmp_path, [event("task_started")] + ([] if unfinished else [event("task_complete")]), name="two")
    result = blockers(tmp_path)
    assert bool(result) == unfinished
    if unfinished:
        assert result[0]["thread_id"] == "two"


def test_liveness_audit_has_no_sender_or_lifecycle_side_effects(tmp_path, monkeypatch):
    from app.adapters import RealChatGPTMessageSender
    from app.codex_app_server import CodexAppServerManager
    def forbidden(*args, **kwargs):
        pytest.fail("liveness audit attempted production I/O")
    monkeypatch.setattr(RealChatGPTMessageSender, "send_message", forbidden)
    for name in ("start_thread", "start_turn", "start_readonly_handoff", "resume_thread_safely"):
        monkeypatch.setattr(CodexAppServerManager, name, forbidden)
    rollout(tmp_path, [event("task_started"), event("task_complete")])
    assert blockers(tmp_path) == []
