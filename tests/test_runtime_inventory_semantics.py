"""Protocol-scoped ownership, inherited history, and conservative terminal evidence."""
import json

import pytest

from app.session_liveness import inspect_rollout
import app.session_liveness as live
from test_production_session_liveness import event, rollout, blockers


def inspect(tmp_path):
    return inspect_rollout(tmp_path / "sessions/one.jsonl", tmp_path / "workspace")


def fork_rollout(tmp_path, boundary=4):
    path = rollout(tmp_path, [], history_mode="paginated", forked_from_id="parent",
                   parent_thread_id="parent", subagent_history_start_ordinal=boundary)
    header = json.loads(path.read_text(encoding="utf-8"))
    rows = [header, {"type": "session_meta", "payload": {"id": "parent", "cwd": str(tmp_path / "workspace")}},
            event("task_started", "parent-turn"), event("task_complete", "parent-turn"),
            event("task_started", "child-turn"), event("task_complete", "child-turn")]
    for ordinal, row in enumerate(rows):
        row["ordinal"] = ordinal
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return path


def test_declared_inherited_parent_history_is_not_child_ownership(tmp_path):
    fork_rollout(tmp_path)
    report = inspect(tmp_path)
    assert report["classification"] == "COMPLETED" and not report["blocking"]
    assert [t["turn_id"] for t in report["turns"]] == ["child-turn"]
    assert any(r["classification"] == "RELATED_THREAD_REFERENCE" for r in report["related_thread_references"])


@pytest.mark.parametrize("boundary", [None, -1, True, "4"])
def test_unproven_inherited_boundary_fails_closed(tmp_path, boundary):
    fork_rollout(tmp_path, boundary)
    with pytest.raises(ValueError):
        inspect(tmp_path)


def test_parent_metadata_outside_inherited_prefix_is_true_mismatch(tmp_path):
    fork_rollout(tmp_path, 1)
    with pytest.raises(ValueError, match="THREAD_MISMATCH"):
        inspect(tmp_path)


def test_corrupt_item_owner_inside_inherited_prefix_is_not_laundered(tmp_path):
    path = fork_rollout(tmp_path)
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").split("\n") if line.strip()]
    rows[2] = {"ordinal": 2, **event("item_started", "parent-turn", thread_id="unrelated")}
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    with pytest.raises(ValueError, match="THREAD_MISMATCH"):
        inspect(tmp_path)


@pytest.mark.parametrize("unfinished", [False, True])
def test_extended_cwd_is_not_skipped_as_unrelated(tmp_path, unfinished):
    rows = [event("task_started")]
    if not unfinished:
        rows.append(event("task_complete"))
    rollout(tmp_path, rows, cwd="\\\\?\\" + str(tmp_path / "workspace"))
    assert inspect(tmp_path)["blocking"] == unfinished


def test_truncated_inherited_prefix_cannot_establish_child_idle(tmp_path):
    fork_rollout(tmp_path, 50)
    assert inspect(tmp_path)["blocking"]


@pytest.mark.parametrize("payload", [
    {"type": "collab_agent_spawn_end", "sender_thread_id": "one", "new_thread_id": "child"},
    {"type": "collab_agent_interaction_end", "sender_thread_id": "one", "receiver_thread_id": "target"},
    {"type": "thread_queue_changed", "threadId": "target"},
])
def test_related_reference_is_retained_without_owner_mismatch(tmp_path, payload):
    rollout(tmp_path, [event("task_started"), {"type": "event_msg", "payload": payload}, event("task_complete")])
    assert not inspect(tmp_path)["blocking"]
    assert inspect(tmp_path)["related_thread_references"]


def test_arbitrary_tool_argument_thread_id_is_only_a_reference(tmp_path):
    rollout(tmp_path, [event("task_started"), {"type": "response_item", "payload": {
        "type": "function_call", "name": "lookup", "call_id": "call-one",
        "arguments": json.dumps({"thread_id": "other", "nested": {"threadId": "child"}})}}, event("task_complete")])
    assert live.record_owner_thread_id({"type": "response_item", "payload": {"type": "function_call", "thread_id": "argument"}}) is None
    assert not inspect(tmp_path)["blocking"]
    assert any("arguments" in r["field_path"] for r in inspect(tmp_path)["related_thread_references"])


@pytest.mark.parametrize("owner", [None, [], "bad id"])
def test_malformed_schema_defined_owner_is_not_ignored(tmp_path, owner):
    rollout(tmp_path, [event("task_started"), event("item_started", thread_id=owner), event("task_complete")])
    with pytest.raises(ValueError, match="LIFECYCLE_UNKNOWN"):
        inspect(tmp_path)


@pytest.mark.parametrize("owner", ["one", "foreign"])
def test_schema_defined_item_owner(tmp_path, owner):
    record = event("item_started", thread_id=owner)
    assert live.record_owner_thread_id(record) == owner
    rollout(tmp_path, [event("task_started"), record, event("task_complete")])
    if owner == "one":
        assert not inspect(tmp_path)["blocking"]
    else:
        with pytest.raises(ValueError, match="THREAD_MISMATCH"):
            inspect(tmp_path)


@pytest.mark.parametrize("terminal", ["task_complete", "turn_complete"])
def test_verified_completion_aliases(tmp_path, terminal):
    rollout(tmp_path, [event("task_started"), event(terminal)])
    assert inspect(tmp_path)["classification"] == "COMPLETED"


@pytest.mark.parametrize("reason", ["interrupted", "replaced", "review_ended", "budget_limited"])
def test_abort_including_interrupt_is_not_success(tmp_path, reason):
    rollout(tmp_path, [event("task_started"), event("turn_aborted", reason=reason)])
    assert inspect(tmp_path)["classification"] == "ABORTED"
    assert inspect(tmp_path)["turns"][-1]["terminal_reason"] == reason


@pytest.mark.parametrize("closed", [False, True])
def test_stream_error_is_retry_evidence_not_success_or_idle(tmp_path, closed):
    rows = [event("task_started"), event("stream_error", codex_error_info={"response_stream_disconnected": {"http_status_code": 503}})]
    if closed:
        rows.append(event("task_complete"))
    rollout(tmp_path, rows)
    report = inspect(tmp_path)
    assert report["classification"] == ("COMPLETED" if closed else "INTERRUPTED_OR_DISCONNECTED")
    assert report["blocking"] != closed
    if not closed:
        assert report["turns"][-1]["terminal_line"] is None


def test_final_without_terminal_is_incomplete_and_blocking(tmp_path):
    rollout(tmp_path, [event("task_started"), {"type": "response_item", "payload": {
        "type": "message", "role": "assistant", "phase": "final_answer", "id": "final-one"}}])
    assert inspect(tmp_path)["classification"] == "CRASHED_OR_INCOMPLETE"
    assert inspect(tmp_path)["blocking"]


@pytest.mark.parametrize("answered", [False, True])
def test_explicit_human_request_and_correlated_response(tmp_path, answered):
    rows = [event("task_started"), {"type": "response_item", "payload": {
        "type": "function_call", "name": "request_user_input", "call_id": "human-one"}}]
    if answered:
        rows.append({"type": "response_item", "payload": {"type": "function_call_output", "call_id": "human-one", "output": "answer"}})
    rollout(tmp_path, rows)
    assert inspect(tmp_path)["classification"] == ("UNKNOWN_LIVENESS" if answered else "ACTIVE_WAITING_HUMAN_ACTION")
    assert inspect(tmp_path)["blocking"]


def test_tool_activity_remains_blocking_without_proof_of_closure(tmp_path):
    rollout(tmp_path, [event("task_started"), {"type": "response_item", "payload": {"type": "function_call", "call_id": "executing"}}])
    assert inspect(tmp_path)["blocking"]


def test_truncation_never_becomes_completion(tmp_path):
    path = rollout(tmp_path, [event("task_started")])
    with path.open("a", encoding="utf-8") as stream:
        stream.write('{"type": "event_msg"')
    with pytest.raises(ValueError):
        inspect(tmp_path)


def test_unknown_record_cannot_extend_closed_history_as_idle(tmp_path):
    rollout(tmp_path, [event("task_started"), event("task_complete"), {"type": "future_execution_record", "payload": {}}])
    assert inspect(tmp_path)["blocking"]


def test_known_context_metadata_does_not_invent_an_executing_turn(tmp_path):
    rollout(tmp_path, [event("task_started"), event("task_complete"),
                       {"type": "world_state", "payload": {}},
                       {"type": "inter_agent_communication_metadata", "payload": {"trigger_turn": False}}])
    assert not inspect(tmp_path)["blocking"]


def test_no_age_based_idle_inference(tmp_path):
    start = event("task_started")
    start["timestamp"] = "1990-01-01T00:00:00Z"
    rollout(tmp_path, [start])
    assert blockers(tmp_path)
