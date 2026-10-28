"""Same-thread Started evidence supersedes history, never current unknown work."""
import pytest

import app.transactional_c2g as tx
from test_phase2_7_production_shadow import env
from test_production_session_liveness import event, rollout, blockers
from test_transactional_c2g import atomic


def sessions(tmp_path):
    return tx.production_session_liveness(tmp_path / "workspace", tmp_path / "sessions")


def test_distinct_started_supersedes_orphan_without_fabricating_terminal(tmp_path):
    rollout(tmp_path, [event("task_started", "old"), event("task_started", "new"), event("task_complete", "new")])
    report = sessions(tmp_path)[0]
    old, new = report["turns"]
    assert old["classification"] == "SUPERSEDED_ORPHAN_TURN"
    assert old["superseded_by_turn_id"] == "new"
    assert old["superseded_at_line"] > old["start_line"]
    assert old["terminal_at"] is None and old["terminal_line"] is None
    assert new["classification"] == "COMPLETED"
    assert report["classification"] == "COMPLETED" and not report["blocking"]
    assert blockers(tmp_path) == []


def test_superseded_orphan_never_hides_latest_unresolved_turn(tmp_path):
    rollout(tmp_path, [event("task_started", "old"), event("task_started", "done"),
                       event("task_complete", "done"), event("task_started", "current")])
    report = sessions(tmp_path)[0]
    assert [t["classification"] for t in report["turns"]] == [
        "SUPERSEDED_ORPHAN_TURN", "COMPLETED", "UNKNOWN_LIVENESS"]
    assert report["turn_ids"] == ["current"]
    assert blockers(tmp_path)


def test_new_started_without_completion_still_blocks(tmp_path):
    rollout(tmp_path, [event("task_started", "old"), event("task_started", "new")])
    report = sessions(tmp_path)[0]
    assert report["turns"][0]["classification"] == "SUPERSEDED_ORPHAN_TURN"
    assert report["turn_ids"] == ["new"] and report["blocking"]


@pytest.mark.parametrize("later", [event("task_started", "old"),
    event("user_message", "old"), event("turn_steered", "old"),
    {"type": "turn_context", "payload": {"turn_id": "other"}},
    {"type": "response_item", "payload": {"type": "task_started", "turn_id": "other"}}])
def test_steer_context_or_unverified_start_cannot_supersede(tmp_path, later):
    rollout(tmp_path, [event("task_started", "old"), later])
    report = sessions(tmp_path)[0]
    assert report["turns"][0]["classification"] == "UNKNOWN_LIVENESS"
    assert report["blocking"]


def test_multiple_historical_orphans(tmp_path):
    rollout(tmp_path, [event("task_started", "a"), event("task_started", "b"),
                       event("task_started", "c"), event("task_complete", "c")])
    assert [t["classification"] for t in sessions(tmp_path)[0]["turns"]] == [
        "SUPERSEDED_ORPHAN_TURN", "SUPERSEDED_ORPHAN_TURN", "COMPLETED"]
    assert blockers(tmp_path) == []


@pytest.mark.parametrize("bad_id", [None, "", " ", "bad id", 42, [], {}])
def test_malformed_later_identity_fails_closed(tmp_path, bad_id):
    rollout(tmp_path, [event("task_started", "old"), event("task_started", bad_id)])
    with pytest.raises(ValueError, match="LIFECYCLE_UNKNOWN"):
        blockers(tmp_path)


def test_other_rollout_cannot_supersede(tmp_path):
    rollout(tmp_path, [event("task_started", "old")], name="thread-a")
    rollout(tmp_path, [event("task_started", "new"), event("task_complete", "new")], name="thread-b")
    assert [r["thread_id"] for r in blockers(tmp_path)] == ["thread-a"]


def test_explicit_cross_thread_event_fails_closed(tmp_path):
    rollout(tmp_path, [event("task_started", "old"),
                       event("item_started", "old", thread_id="different-thread"), event("task_complete", "new")])
    with pytest.raises(ValueError, match="THREAD_MISMATCH"):
        blockers(tmp_path)


def test_independent_activity_from_old_turn_prevents_supersession(tmp_path):
    rollout(tmp_path, [event("task_started", "old"), event("task_started", "new"),
        {"type": "response_item", "payload": {"type": "function_call", "turn_id": "old", "call_id": "old-call"}},
        event("task_complete", "new")])
    report = sessions(tmp_path)[0]
    assert report["blocking"] and report["reason"] == "CONFLICTING_SAME_THREAD_ACTIVITY"
    assert report["turns"][0]["classification"] == "UNKNOWN_LIVENESS"


def test_real_terminal_remains_real_after_supersession(tmp_path):
    rollout(tmp_path, [event("task_started", "old"), event("task_started", "new"),
                       event("task_aborted", "old"), event("task_complete", "new")])
    assert sessions(tmp_path)[0]["turns"][0]["classification"] == "ABORTED"
    assert blockers(tmp_path) == []


def test_unverified_original_start_cannot_be_superseded(tmp_path):
    rollout(tmp_path, [{"payload": {"type": "task_started", "turn_id": "old"}},
                       event("task_started", "new"), event("task_complete", "new")])
    assert sessions(tmp_path)[0]["turns"][0]["classification"] == "UNKNOWN_LIVENESS"
    assert blockers(tmp_path)


def test_rollout_order_overrides_timestamp_order(tmp_path):
    old = event("task_started", "old")
    new = event("task_started", "new")
    old["timestamp"], new["timestamp"] = "2999-01-01", "2000-01-01"
    rollout(tmp_path, [old, new, event("task_complete", "new")])
    assert sessions(tmp_path)[0]["turns"][0]["classification"] == "SUPERSEDED_ORPHAN_TURN"
    assert blockers(tmp_path) == []


def test_closed_old_context_cannot_override_latest_started_outcome(tmp_path):
    rollout(tmp_path, [event("task_started", "old"), event("task_complete", "old"),
                       event("task_started", "new"), event("task_aborted", "new"),
                       {"type": "turn_context", "payload": {"turn_id": "old"}}])
    assert sessions(tmp_path)[0]["classification"] == "ABORTED"


def test_audit_does_not_write_or_send(tmp_path, monkeypatch):
    from app.adapters import RealChatGPTMessageSender
    from app.codex_app_server import CodexAppServerManager
    def forbidden(*args, **kwargs):
        pytest.fail("read-only liveness inspection attempted a side effect")
    monkeypatch.setattr(RealChatGPTMessageSender, "send_message", forbidden)
    for name in ("start_thread", "start_turn", "start_readonly_handoff", "resume_thread_safely"):
        monkeypatch.setattr(CodexAppServerManager, name, forbidden)
    path = rollout(tmp_path, [event("task_started", "old"), event("task_started", "new"), event("task_complete", "new")])
    before = path.read_bytes()
    assert blockers(tmp_path) == []
    assert path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("unresolved", [False, True])
async def test_runtime_quiescence_get_has_no_authorization_or_delivery(atomic, tmp_path, monkeypatch, unresolved):
    from httpx import ASGITransport, AsyncClient
    import app.main as main
    from app.database import Database
    env = atomic
    service = env["service"]
    with env["rollout"].open("a", encoding="utf-8") as stream:
        import json
        stream.write(json.dumps(event("task_started", "historical")) + "\n")
        stream.write(json.dumps(event("task_started", "latest")) + "\n")
        if not unresolved:
            stream.write(json.dumps(event("task_complete", "latest")) + "\n")
    db = Database(tmp_path / "api.db")
    await db.init()
    await db.save_config(env["cfg"])
    monkeypatch.setattr(main, "db", db)
    monkeypatch.setattr(main, "production_shadow", service)
    async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://local") as client:
        response = await client.get("/api/production-shadow/quiescence")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == ("PRODUCTION_WORKSPACE_GENUINELY_BUSY" if unresolved else "PRODUCTION_QUIESCENCE_VERIFIED")
    assert bool(body["snapshot"]["active_sessions"]) == unresolved
    assert body["transaction_authorized"] is False
    assert service.load() is None
    with service.connect() as ledger:
        assert ledger.execute("SELECT COUNT(*) FROM atomic_c2g_parents").fetchone()[0] == 0
    env["sender"].send_message.assert_not_awaited()
    env["manager"].start_readonly_handoff.assert_not_awaited()
