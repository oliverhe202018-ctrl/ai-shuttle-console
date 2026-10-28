"""Transactional acceptance with synthetic rollouts/browser; no live calls."""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

import test_phase2_7_production_shadow as legacy
from test_phase2_7_production_shadow import env
import app.transactional_c2g as tx
from app.canonical import compute_semantic_hash
from app.models import compute_sha256
from app.codex_app_server import CodexAppServerManager

FRESH_THREAD = "44444444-4444-4444-8444-444444444444"
FRESH = "Fresh read-only handoff\nFRESH_PRODUCTION_HANDOFF_READY"


@pytest.fixture
def atomic(env):
    old = env["service"]
    manager = MagicMock()
    async def capture(workspace):
        path = Path(env["cfg"].codex.sessions_dir) / f"rollout-{FRESH_THREAD}.jsonl"
        legacy.create_handoff_rollout(path, FRESH_THREAD, workspace, FRESH)
        with Path(env["cfg"].codex.session_index_path).open("a", encoding="utf-8") as f:
            f.write("\n" + json.dumps({"id": FRESH_THREAD, "cwd": workspace}))
        return {"thread_id": FRESH_THREAD, "turn_id": "turn-test-1"}
    manager.start_readonly_handoff = AsyncMock(side_effect=capture)
    service = tx.TransactionalProductionShadow(old.path, old.config_fn, old.observer, old.browser, old.sender,
                                               handoff_manager=manager)
    env.update(service=service, manager=manager)
    async def send(**kwargs):
        assert kwargs["message"] == FRESH
        env["browser"].read_live_page.return_value["html"] = legacy.page(env["turns"] + [
            ("user", "new-user", "new-user-message", FRESH, False),
            ("assistant", "new-assistant", "new-assistant-message", legacy.PROMPT, True)])
        return {"status": "delivered", "raw_content_hash": compute_sha256(FRESH),
                "semantic_content_hash": compute_semantic_hash(FRESH), "delivery_evidence": {
                    "user_turn_id": "new-user", "user_message_id": "new-user-message",
                    "matching_turn_count": 1, "semantic_delivery_status": "SEMANTIC_DELIVERY_CONFIRMED"}}
    env["sender"].send_message.side_effect = send
    return env


async def grant(e):
    s = e["service"]
    return await s.authorize_parent(s.parent_binding(e["cfg"]), True)


async def run(e):
    a = await grant(e)
    return await e["service"].capture_and_send_atomic_c2g(a["token"])


def restart(e):
    s = e["service"]
    return tx.TransactionalProductionShadow(s.path, s.config_fn, s.observer, s.browser, s.sender,
                                           handoff_manager=e["manager"])


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["workspace", "conversation_url", "conversation_id", "config_fingerprint",
    "handoff_prompt_sha256", "implementation_sha256", "operation", "production_shadow_mode",
    "max_c2g_sends", "max_handoff_captures"])
async def test_parent_exact_protocol_binding(atomic, field):
    s = atomic["service"]
    binding = s.parent_binding(atomic["cfg"])
    assert "payload_sha256" not in binding
    binding[field] = "tampered"
    with pytest.raises(ValueError, match="BINDING_MISMATCH"):
        await s.authorize_parent(binding, True)
    atomic["manager"].start_readonly_handoff.assert_not_awaited()


@pytest.mark.asyncio
async def test_confirmation_expiry_and_protocol_change(atomic, monkeypatch):
    s = atomic["service"]
    with pytest.raises(ValueError, match="CONFIRMATION"):
        await s.authorize_parent(s.parent_binding(atomic["cfg"]), False)
    a = await grant(atomic)
    with s.connect() as db:
        db.execute("UPDATE atomic_c2g_parents SET expires_at='2000-01-01'")
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await s.capture_and_send_atomic_c2g(a["token"])
    a = await grant(atomic)
    monkeypatch.setattr(tx, "build_handoff_instruction_prompt", lambda: "arbitrary replacement")
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await s.capture_and_send_atomic_c2g(a["token"])


@pytest.mark.asyncio
async def test_arbitrary_prompt_and_manual_paths_rejected(atomic):
    s = atomic["service"]
    for method in (s.authorize, s.send, s.prepare, s.baseline, s.rebind_source, s.capture_atomic_checkpoint):
        with pytest.raises(ValueError, match="TRANSACTIONAL_C2G_REQUIRED"):
            await method(custom_prompt="arbitrary")
    with pytest.raises(TypeError):
        await s.capture_and_send_atomic_c2g("token", custom_prompt="arbitrary")


@pytest.mark.asyncio
async def test_active_session_blocks_before_capture_and_budget(atomic):
    with atomic["rollout"].open("a", encoding="utf-8") as f:
        f.write(json.dumps({"payload": {"type": "task_started", "turn_id": "active"}}) + "\n")
    r = await run(atomic)
    assert r["status"] == "ATOMIC_C2G_PRECAPTURE_BLOCKED"
    assert not r["c2g_allowance_consumed"]
    atomic["manager"].start_readonly_handoff.assert_not_awaited()
    atomic["sender"].send_message.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["branch", "composer", "inventory"])
async def test_precapture_fail_closed(atomic, failure):
    if failure == "branch":
        atomic["cfg"].codex.expected_branch = "wrong"
    elif failure == "composer":
        atomic["browser"].read_live_page.return_value["html"] = legacy.page(atomic["turns"]).replace(
            '<textarea id="prompt-textarea"></textarea>', '<textarea id="prompt-textarea">unsent text</textarea>')
    else:
        (Path(atomic["cfg"].codex.sessions_dir) / "broken.jsonl").write_text("bad")
    r = await run(atomic)
    assert r["status"] == "ATOMIC_C2G_PRECAPTURE_BLOCKED" and not r["c2g_allowance_consumed"]
    atomic["manager"].start_readonly_handoff.assert_not_awaited()


@pytest.mark.asyncio
async def test_ab_material_race_does_not_pin_or_send(atomic):
    capture = atomic["manager"].start_readonly_handoff.side_effect
    async def raced(workspace):
        result = await capture(workspace)
        (Path(workspace) / "new.py").write_text("material change")
        return result
    atomic["manager"].start_readonly_handoff.side_effect = raced
    r = await run(atomic)
    assert r["status"] == "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE"
    assert "pinned_artifact" not in r and not r["c2g_allowance_consumed"]
    atomic["sender"].send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_semantic_ab_and_dirty_stability_pass(atomic):
    (atomic["workspace"] / "tracked.txt").write_text("preexisting dirty")
    capture = atomic["manager"].start_readonly_handoff.side_effect
    async def telemetry(workspace):
        result = await capture(workspace)
        (Path(workspace) / "runtime.log").write_text("telemetry")
        return result
    atomic["manager"].start_readonly_handoff.side_effect = telemetry
    r = await run(atomic)
    assert r["checkpoint_status"] == "ATOMIC_PRODUCTION_CHECKPOINT_SEMANTICALLY_STABLE"
    assert r["status"] == "PRODUCTION_G2C_PREPARED_FOR_REVIEW"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["material", "unknown", "active"])
async def test_snapshot_c_blocks_before_child(atomic, monkeypatch, kind):
    original = tx.take_checkpoint_snapshot
    calls = 0
    def snapshot(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3 and kind != "active":
            (atomic["workspace"] / ("change.py" if kind == "material" else "change.bin")).write_text("changed")
        value = original(*args, **kwargs)
        if calls == 3 and kind == "active":
            value["active_sessions"] = [{"thread_id": "new-active"}]
        return value
    monkeypatch.setattr(tx, "take_checkpoint_snapshot", snapshot)
    r = await run(atomic)
    assert r["status"] == "ATOMIC_C2G_FINAL_FRESHNESS_FAILED"
    assert "pinned_artifact" in r and "child_authorization" not in r
    assert not r["c2g_allowance_consumed"]
    atomic["sender"].send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_stable_child_binding_exact_g2c_and_no_execution(atomic):
    r = await run(atomic)
    child = r["child_authorization"]
    assert child["parent_authorization_id"] == r["parent"]["id"]
    assert child["payload_sha256"] == r["pinned_artifact"]["sha256"] == compute_sha256(FRESH)
    assert child["char_count"] == len(FRESH) and child["direction"] == "C2G"
    assert child["snapshot_c_fingerprint"] == r["snapshot_c"]["fingerprint"]
    assert r["c2g_send_attempts"] == r["c2g_delivered_count"] == 1
    assert r["source"]["status"] == "PINNED_SOURCE_ARTIFACT_VERIFIED"
    assert r["g2c"]["prompt"] == r["g2c"]["message"] == legacy.PROMPT.strip()
    assert r["g2c"]["g2c_rewrite_count"] == 0 and "authorization" not in r["g2c"]
    assert r["codex_queue_submissions"] == r["codex_executions"] == r["thread_resume_calls"] == 0
    assert r["planner"]["completion_evidence"]["is_completed"]
    atomic["manager"].start_readonly_handoff.assert_awaited_once()
    atomic["sender"].send_message.assert_awaited_once()
    with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
        await grant(atomic)
    with pytest.raises(ValueError, match="TRANSACTIONAL"):
        await atomic["service"].send(child["id"])
    assert atomic["cfg"].codex.thread_id == legacy.THREAD


@pytest.mark.asyncio
async def test_duplicate_and_unrelated_user_detection(atomic):
    send = atomic["sender"].send_message.side_effect
    async def duplicate(**kwargs):
        result = await send(**kwargs)
        atomic["browser"].read_live_page.return_value["html"] += legacy.page([
            ("user", "duplicate-user", "duplicate-msg", FRESH, False)])
        return result
    atomic["sender"].send_message.side_effect = duplicate
    r = await run(atomic)
    assert r["status"] == "PRODUCTION_C2G_DELIVERY_UNKNOWN"
    assert r["c2g_allowance_consumed"] and r["c2g_delivered_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ConnectionError("unknown outcome"), tx.NotSentError("PRODUCTION_COMPOSER_TEXT_MISMATCH")])
async def test_consumed_attempt_never_retries(atomic, error):
    atomic["sender"].send_message.side_effect = error
    a = await grant(atomic)
    r = await atomic["service"].capture_and_send_atomic_c2g(a["token"])
    assert r["c2g_allowance_consumed"] and r["c2g_send_attempts"] == 1
    s = restart(atomic)
    await s.recover()
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await s.capture_and_send_atomic_c2g(a["token"])
    with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
        await s.authorize_parent(s.parent_binding(atomic["cfg"]), True)
    atomic["sender"].send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_interleaved_planner_is_terminal(atomic):
    a = await grant(atomic)
    r = await atomic["service"]._capture_and_deliver(a["token"])
    assert r["status"] == "PRODUCTION_C2G_DELIVERED"
    atomic["browser"].read_live_page.return_value["html"] = legacy.page(atomic["turns"] + [
        ("user", "new-user", "new-user-message", FRESH, False),
        ("user", "interloper", "interloper-msg", "another request", False),
        ("assistant", "new-assistant", "new-assistant-msg", legacy.PROMPT, True)])
    r = await atomic["service"].observe_planner()
    assert r["status"] == "PRODUCTION_CHATGPT_INTERLEAVING_DETECTED" and "g2c" not in r


@pytest.mark.asyncio
async def test_restart_before_child_requires_new_parent(atomic):
    atomic["manager"].start_readonly_handoff.side_effect = asyncio.CancelledError()
    a = await grant(atomic)
    with pytest.raises(asyncio.CancelledError):
        await atomic["service"].capture_and_send_atomic_c2g(a["token"])
    s = restart(atomic)
    r = await s.recover()
    assert r["status"] == "ATOMIC_C2G_INTERRUPTED_NEW_PARENT_REQUIRED"
    assert not r.get("send_attempted_at")
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await s.capture_and_send_atomic_c2g(a["token"])
    atomic["sender"].send_message.assert_not_awaited()
    assert atomic["manager"].start_readonly_handoff.await_count == 1


@pytest.mark.asyncio
async def test_crash_after_consumption_recovery_requires_reconciliation(atomic):
    atomic["sender"].send_message.side_effect = asyncio.CancelledError()
    a = await grant(atomic)
    with pytest.raises(asyncio.CancelledError):
        await atomic["service"].capture_and_send_atomic_c2g(a["token"])
    s = restart(atomic)
    r = await s.recover()
    assert r["status"] == "PRODUCTION_C2G_RECOVERY_REQUIRED"
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await s.capture_and_send_atomic_c2g(a["token"])
    atomic["sender"].send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_restart_after_delivery_allows_observation_only(atomic):
    a = await grant(atomic)
    await atomic["service"]._capture_and_deliver(a["token"])
    s = restart(atomic)
    assert (await s.recover())["status"] == "PRODUCTION_C2G_DELIVERED"
    assert (await s.observe_planner())["status"] == "PRODUCTION_G2C_PREPARED_FOR_REVIEW"
    atomic["sender"].send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_two_instances_and_parent_replay_cannot_capture_twice(atomic):
    capture = atomic["manager"].start_readonly_handoff.side_effect
    async def yielded(workspace):
        await asyncio.sleep(0.01)
        return await capture(workspace)
    atomic["manager"].start_readonly_handoff.side_effect = yielded
    a = await grant(atomic)
    b = await grant(atomic)
    s = restart(atomic)
    results = await asyncio.gather(atomic["service"].capture_and_send_atomic_c2g(a["token"]),
                                  s.capture_and_send_atomic_c2g(b["token"]), return_exceptions=True)
    assert any(isinstance(r, ValueError) for r in results)
    atomic["manager"].start_readonly_handoff.assert_awaited_once()
    atomic["sender"].send_message.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["baseline", "prepare", "authorize", "send", "rebind-source"])
async def test_legacy_shadow_api_cannot_bypass(atomic, monkeypatch, tmp_path, endpoint):
    import app.main as main
    from app.database import Database
    db = Database(tmp_path / "api.db")
    await db.init()
    await db.save_config(atomic["cfg"])
    monkeypatch.setattr(main, "db", db)
    monkeypatch.setattr(main, "production_shadow", atomic["service"])
    async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://local") as client:
        response = await client.post('/api/production-shadow/' + endpoint, json={})
    assert response.status_code == 409
    atomic["sender"].send_message.assert_not_awaited()
    atomic["manager"].start_readonly_handoff.assert_not_awaited()


@pytest.mark.asyncio
async def test_official_handoff_sandbox_and_execution_barrier():
    manager = CodexAppServerManager("unused")
    manager.production_shadow_mode = True
    manager.ensure_started = AsyncMock()
    manager.rpc_client.call = AsyncMock(side_effect=[
        {"thread": {"id": FRESH_THREAD, "cwd": str(Path.cwd())}}, {"turn": {"id": "observed-turn"}}])
    result = await manager.start_readonly_handoff(str(Path.cwd()))
    assert result["turn_id"] == "observed-turn"
    calls = manager.rpc_client.call.await_args_list
    assert [c.args[0] for c in calls] == ["thread/start", "turn/start"]
    assert calls[0].args[1]["sandbox"] == "read-only"
    assert calls[1].args[1]["sandboxPolicy"] == {"type": "readOnly"}
    assert calls[1].args[1]["input"][0]["text"] == tx.build_handoff_instruction_prompt()
    for method, args in [(manager.start_thread, (str(Path.cwd()),)),
                         (manager.start_turn, (FRESH_THREAD, "arbitrary")),
                         (manager.resume_thread_safely, (FRESH_THREAD,))]:
        with pytest.raises(ValueError):
            await method(*args)
    assert manager.rpc_client.call.await_count == 2


def test_unexplained_snapshot_delta_is_unknown():
    a = {"fingerprint": "a", "head": "h", "branch": "b"}
    b = {**a, "fingerprint": "b"}
    assert tx.classify_checkpoint_delta(a, b)[0] not in tx.STABLE


def test_private_storage_and_public_sanitization():
    legacy.test_private_files_are_ignored_and_untracked()
    legacy.test_real_ids_stay_out_of_public_repository()


def test_ui_dedicated_transaction_control():
    template = (Path(__file__).resolve().parents[1] / "app/templates/index.html").read_text(encoding="utf-8")
    assert "Atomic Capture &amp; Send Once" in template
    assert "C2G max sends: 1" in template and "G2C execution: OFF" in template
    assert "authorize-atomic-parent" in template and "atomic-capture-send" in template
    assert 'id="shadow-send"' not in template


@pytest.mark.asyncio
async def test_budget_and_child_committed_before_browser_io(atomic):
    send = atomic["sender"].send_message.side_effect
    async def checked_send(**kwargs):
        s = atomic["service"]
        with s.connect() as db:
            raw, attempted = db.execute("SELECT record,send_attempted FROM production_shadow_runs WHERE slot=1").fetchone()
        stored = json.loads(raw)
        assert attempted == 1 and stored["child_authorization"]["consumed_at"]
        assert stored["atomic_phase"] == "CHILD_CONSUMED"
        assert kwargs["confirmation_token"] == stored["child_authorization"]["id"]
        return await send(**kwargs)
    atomic["sender"].send_message.side_effect = checked_send
    assert (await run(atomic))["status"] == "PRODUCTION_G2C_PREPARED_FOR_REVIEW"


@pytest.mark.asyncio
async def test_failed_handoff_preserves_no_pin_and_consumes_capture_only(atomic):
    capture = atomic["manager"].start_readonly_handoff.side_effect
    async def invalid(workspace):
        result = await capture(workspace)
        path = atomic["observer"].locate_session_file(FRESH_THREAD)
        path.write_text(path.read_text(encoding="utf-8").replace("FRESH_PRODUCTION_HANDOFF_READY", "INVALID"), encoding="utf-8")
        return result
    atomic["manager"].start_readonly_handoff.side_effect = invalid
    a = await grant(atomic)
    r = await atomic["service"].capture_and_send_atomic_c2g(a["token"])
    assert r["status"] == "ATOMIC_HANDOFF_FINAL_INVALID"
    assert not r["c2g_allowance_consumed"] and "pinned_artifact" not in r
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await atomic["service"].capture_and_send_atomic_c2g(a["token"])
    with atomic["service"].connect() as db:
        history = json.loads(db.execute("SELECT record FROM atomic_c2g_history WHERE id=?", (r["shadow_run_id"],)).fetchone()[0])
    assert history["status"] == r["status"]
    atomic["sender"].send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_unresolved_legacy_authorization_blocks_capture(atomic):
    s = atomic["service"]
    s.save({"shadow_run_id": "old-run", "status": "PRODUCTION_C2G_AUTHORIZED"}, create=True)
    r = await run(atomic)
    assert r["status"] == "ATOMIC_C2G_PRECAPTURE_BLOCKED"
    atomic["manager"].start_readonly_handoff.assert_not_awaited()


@pytest.mark.asyncio
async def test_atomic_api_requires_bound_parent_and_returns_prepared_only(atomic, monkeypatch, tmp_path):
    import app.main as main
    from app.database import Database
    db = Database(tmp_path / "api.db")
    await db.init()
    await db.save_config(atomic["cfg"])
    monkeypatch.setattr(main, "db", db)
    monkeypatch.setattr(main, "production_shadow", atomic["service"])
    async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://local") as client:
        denied = await client.post('/api/production-shadow/atomic-capture-send', json={"token": "wrong"})
        assert denied.status_code == 409
        denied = await client.post('/api/production-shadow/atomic-capture-send', json={"token": "wrong", "custom_prompt": "arbitrary"})
        assert denied.status_code == 422
        response = await client.post('/api/production-shadow/authorize-atomic-parent', json={
            "binding": atomic["service"].parent_binding(atomic["cfg"]), "operator_confirmed": True})
        assert response.status_code == 200
        response = await client.post('/api/production-shadow/atomic-capture-send', json={"token": response.json()["token"]})
    assert response.status_code == 200
    r = response.json()
    assert r["status"] == "PRODUCTION_G2C_PREPARED_FOR_REVIEW" and r["codex_executions"] == 0
