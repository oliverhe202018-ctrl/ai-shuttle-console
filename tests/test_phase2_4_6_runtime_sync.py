"""Runtime/config/browser/transfer synchronization with zero external delivery."""
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

import app.main as m
import app.canary_bootstrap as bootstrap
from app.adapters import RealChatGPTBrowserObserver
from app.canary import compute_config_fingerprint, load_explicit_config, PRODUCTION_THREAD
from app.models import ShuttleStage, TransferRecord, TransferAuthorization, compute_sha256
from app.state_machine import ShuttleStateMachine
from test_phase2_4_5_pre_send_readiness import temp_canary_env


@pytest.fixture
async def runtime_app(temp_canary_env, monkeypatch):
    env = temp_canary_env
    cfg, database = env["config"], env["db"]
    await database.init()
    await database.save_config(cfg)
    source = env["tmp_path"] / "config.canary.local.A.json"
    source.write_text(cfg.model_dump_json(), encoding="utf-8")
    monkeypatch.setenv("SHUTTLE_CONFIG", str(source))
    monkeypatch.setenv("SHUTTLE_CANARY", "1")
    monkeypatch.setattr(m, "db", database)
    machine = ShuttleStateMachine(database, cfg)
    await machine.init()
    monkeypatch.setattr(m, "state_machine", machine)
    monkeypatch.setattr(m, "runtime_identity", None)
    monkeypatch.setattr(m, "runtime_browser_status", {})
    monkeypatch.setattr(m, "runtime_codex_status", {})
    monkeypatch.setattr(m, "validate_canary_session", lambda *a: {"session_file_path": "fixture.jsonl", "total_lines": 2})
    observer = RealChatGPTBrowserObserver(target_url=cfg.chatgpt.conversation_url, cdp_url=cfg.chatgpt.cdp_url)
    observer.read_live_page = AsyncMock(return_value={"url": cfg.chatgpt.conversation_url,
        "html": '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div></form>'})
    monkeypatch.setattr(m, "real_chatgpt_observer", observer)
    verified = {"verdict": "CANARY_PREFLIGHT_VERIFIED", "config_path": str(source),
                "config_fingerprint": compute_config_fingerprint(cfg), "preflight_result": {"status": "verified"}}
    await database.save_canary_bootstrap_state(verified)
    monkeypatch.setattr(bootstrap, "run_canary_bootstrap", AsyncMock(return_value=verified))
    sender, queue = AsyncMock(), AsyncMock()
    sender.send_message.side_effect = AssertionError("send must not be reached")
    queue.queue_message.side_effect = AssertionError("queue must not be reached")
    monkeypatch.setattr(m, "real_chatgpt_sender", sender)
    monkeypatch.setattr(m, "real_codex_queue", queue)
    async with AsyncClient(transport=ASGITransport(app=m.app), base_url="http://test") as client:
        yield {**env, "client": client, "source": source, "observer": observer,
               "sender": sender, "queue": queue, "verified": verified}
    sender.send_message.assert_not_called()
    queue.queue_message.assert_not_called()


async def prepare(env):
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200, res.text
    return res.json()


@pytest.mark.asyncio
async def test_bootstrap_B_cannot_authorize_runtime_A(runtime_app):
    env = runtime_app
    cfg = env["config"].model_copy(deep=True)
    cfg.codex.sessions_dir += "-changed"
    b = env["source"].with_name("config.canary.local.B.json")
    b.write_text(cfg.model_dump_json(), encoding="utf-8")
    await env["db"].save_canary_bootstrap_state({**env["verified"], "config_path": str(b),
        "config_fingerprint": compute_config_fingerprint(cfg)})
    readiness = await m.runtime_readiness(probe=True)
    assert readiness["status"] == "RUNTIME_RESTART_REQUIRED"
    assert not readiness["c2g_send_enabled"]
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 409
    assert not await env["db"].get_all_transfers()
    assert (await env["client"].get("/api/transfer/target_info")).json()["config_source"] == str(env["source"])


@pytest.mark.asyncio
async def test_modified_runtime_file_requires_restart(runtime_app):
    env = runtime_app
    cfg = env["config"].model_copy(deep=True)
    cfg.codex.sessions_dir += "-changed"
    env["source"].write_text(cfg.model_dump_json(), encoding="utf-8")
    assert (await m.runtime_readiness(probe=True))["status"] == "RUNTIME_RESTART_REQUIRED"


@pytest.mark.asyncio
async def test_discovered_target_does_not_override_disconnected_probe(runtime_app):
    env = runtime_app
    env["observer"].read_live_page.side_effect = ConnectionError("CDP disconnected")
    snapshot = (await env["client"].get("/api/runtime/refresh")).json()
    assert snapshot["bootstrap"]["verdict"] == "CANARY_PREFLIGHT_VERIFIED"
    assert snapshot["readiness"]["status"] == "BROWSER_NOT_READY"
    assert not snapshot["readiness"]["browser_status"]["connected"]
    assert snapshot["reconciliation"]["chatgpt"]["browser_readiness"] == "blocked_or_offline"


@pytest.mark.asyncio
async def test_live_probe_updates_all_panels_without_delivered_request(runtime_app):
    env = runtime_app
    res = await env["client"].post("/api/chatgpt/observe", json={})
    assert res.status_code == 200
    assert res.json()["browser_status"]["ready"]
    snapshot = (await env["client"].get("/api/runtime/refresh")).json()
    assert snapshot["readiness"]["status"] == "CANARY_READY_FOR_C2G_PREPARE"
    assert snapshot["target_info"]["config_fingerprint"] == snapshot["bootstrap"]["config_fingerprint"]
    assert snapshot["reconciliation"]["chatgpt"]["browser_readiness"] == "ready"
    assert not snapshot["reconciliation"]["reconciliation"]["transfer_eligible"]
    assert not snapshot["readiness"]["c2g_send_enabled"]
    assert not snapshot["readiness"]["g2c_queue_enabled"]
    assert not await env["db"].get_all_transfers()


@pytest.mark.asyncio
async def test_orphan_report_preview_repairs_without_fabricating_record(runtime_app):
    # Simulate a pre-upgrade persisted orphan, bypassing the new transition guard.
    cp = m.state_machine.current_checkpoint.model_copy(update={"stage": ShuttleStage.REPORT_PREVIEW, "transfer_status": "prepared"})
    m.state_machine.current_checkpoint = await runtime_app["db"].save_checkpoint(cp)
    snap = (await runtime_app["client"].get("/api/runtime/refresh")).json()
    assert snap["state"]["stage"] == "CODEX_WAITING"
    assert snap["readiness"]["status"] == "TRANSFER_STATE_INCONSISTENT"
    assert not await runtime_app["db"].get_all_transfers()
    assert any(x["event_type"] == "TRANSFER_STATE_REPAIRED" for x in await runtime_app["db"].get_recent_logs())


@pytest.mark.asyncio
async def test_force_transition_cannot_manufacture_report_preview(runtime_app):
    res = await runtime_app["client"].post("/api/stage/transition", json={"target_stage": "REPORT_PREVIEW", "force": True})
    assert res.status_code == 400
    assert "TRANSFER_STATE_INCONSISTENT" in res.json()["detail"]
    assert m.state_machine.stage == ShuttleStage.IDLE


@pytest.mark.asyncio
async def test_orphan_prepared_status_repairs_even_outside_preview(runtime_app):
    cp = m.state_machine.current_checkpoint.model_copy(update={"transfer_status": "prepared"})
    m.state_machine.current_checkpoint = await runtime_app["db"].save_checkpoint(cp)
    snapshot = (await runtime_app["client"].get("/api/runtime/refresh")).json()
    assert snapshot["state"]["transfer_status"] == "idle"
    assert snapshot["readiness"]["status"] == "TRANSFER_STATE_INCONSISTENT"


@pytest.mark.asyncio
async def test_duplicate_prepared_records_block_and_preserve_audit(runtime_app):
    data = await prepare(runtime_app)
    transfer = await runtime_app["db"].get_transfer_by_id(data["transfer_id"])
    duplicate = transfer.model_copy(update={"id": None, "transfer_id": "duplicate-prepared"})
    await runtime_app["db"].save_transfer_record(duplicate)
    assert not (await m.runtime_readiness(probe=True))["c2g_send_enabled"]
    snapshot = (await runtime_app["client"].get("/api/runtime/refresh")).json()
    assert snapshot["state"]["stage"] == "CODEX_WAITING"
    records = await runtime_app["db"].get_all_transfers()
    assert len(records) == 2 and all(t.status == "cancelled" for t in records)


@pytest.mark.asyncio
async def test_finished_cycle_does_not_claim_ready_for_new_prepare(runtime_app):
    data = await prepare(runtime_app)
    transfer = await runtime_app["db"].get_transfer_by_id(data["transfer_id"])
    transfer.status = "delivered"
    await runtime_app["db"].save_transfer_record(transfer)
    await m.state_machine.transition_to(ShuttleStage.CODEX_WAITING, transfer_status="delivered", force=True)
    assert (await m.runtime_readiness(probe=True))["status"] == "CANARY_CYCLE_ALREADY_USED"


@pytest.mark.asyncio
async def test_prepared_record_restored_with_identity_hash_nonce_and_token(runtime_app):
    data = await prepare(runtime_app)
    snap = (await runtime_app["client"].get("/api/runtime/refresh")).json()
    record = snap["readiness"]["transfers"]["c2g"]["transfer"]
    assert record["transfer_id"] == data["transfer_id"]
    assert record["content_hash"] == compute_sha256(record["message_text"])
    assert record["source_message_id"] == "canary-request:" + data["canary_nonce"]
    assert snap["readiness"]["c2g_send_enabled"]
    assert not snap["readiness"]["g2c_queue_enabled"]


@pytest.mark.parametrize("field,value", [("content_hash", ""), ("source_message_id", None),
                                         ("confirmation_token", None), ("config_fingerprint", "wrong"),
                                         ("destination_identifier", "https://chatgpt.com/c/wrong")])
@pytest.mark.asyncio
async def test_missing_bindings_disable_ui_and_backend_rejects(runtime_app, field, value):
    env = runtime_app
    data = await prepare(env)
    transfer = await env["db"].get_transfer_by_id(data["transfer_id"])
    setattr(transfer, field, value)
    await env["db"].save_transfer_record(transfer)
    assert not (await m.runtime_readiness(probe=True))["c2g_send_enabled"]
    res = await env["client"].post("/api/transfer/chatgpt/send", json={
        "transfer_id": data["transfer_id"], "confirmation_token": data["confirmation_token"]})
    assert res.status_code in (400, 409)


@pytest.mark.asyncio
async def test_expired_authorization_is_not_refreshed_by_recheck(runtime_app):
    data = await prepare(runtime_app)
    auth = await runtime_app["db"].get_transfer_authorization(data["confirmation_token"])
    auth.expires_at = datetime.utcnow() - timedelta(seconds=1)
    # Update existing authorization without issuing another token.
    import aiosqlite
    async with aiosqlite.connect(runtime_app["db"].db_path) as conn:
        await conn.execute("UPDATE transfer_authorizations SET expires_at = ?", (auth.expires_at.isoformat(),))
        await conn.commit()
    snap = (await runtime_app["client"].post("/api/canary/bootstrap/recheck")).json()["runtime_snapshot"]
    assert not snap["readiness"]["c2g_send_enabled"]
    assert "expired" in snap["readiness"]["blocker"]


@pytest.mark.asyncio
async def test_unknown_delivery_anywhere_blocks_both_directions(runtime_app):
    await prepare(runtime_app)
    await runtime_app["db"].save_transfer_record(TransferRecord(transfer_id="older-unknown", status="delivery_outcome_unknown"))
    ready = await m.runtime_readiness(probe=True)
    assert ready["status"] == "DELIVERY_OUTCOME_UNKNOWN"
    assert not ready["c2g_send_enabled"] and not ready["g2c_queue_enabled"]


@pytest.mark.asyncio
async def test_g2c_rejects_c2g_record_even_with_its_valid_token(runtime_app):
    data = await prepare(runtime_app)
    res = await runtime_app["client"].post("/api/transfer/codex_queue/submit", json={
        "transfer_id": data["transfer_id"], "confirmation_token": data["confirmation_token"]})
    assert res.status_code in (400, 409)


@pytest.mark.asyncio
async def test_production_identity_cannot_become_ready(runtime_app):
    cfg = runtime_app["config"].model_copy(deep=True)
    cfg.codex.thread_id = PRODUCTION_THREAD
    await runtime_app["db"].save_config(cfg)
    assert not (await m.runtime_readiness(probe=True))["c2g_send_enabled"]


def test_generation_reuses_full_matching_config(temp_canary_env, monkeypatch):
    env = temp_canary_env
    cfg = env["config"]
    monkeypatch.setattr(bootstrap, "BASE_DIR", env["tmp_path"])
    template = env["tmp_path"] / "config.canary.example.json"
    template.write_text(json.dumps({**cfg.model_dump(), "server": {"host": "127.0.0.1", "port": 8765}}), encoding="utf-8")
    codex = {"thread_id": cfg.codex.thread_id, "title": cfg.codex.thread_title, "cwd": cfg.codex.repo_path,
             "sessions_dir": cfg.codex.sessions_dir, "session_index_path": cfg.codex.session_index_path}
    chatgpt = {"conversation_url": cfg.chatgpt.conversation_url, "conversation_id": cfg.chatgpt.conversation_id,
               "profile_dir": cfg.chatgpt.profile_dir, "cdp_url": cfg.chatgpt.cdp_url}
    args = dict(codex_target=codex, chatgpt_target=chatgpt, cli_path=cfg.codex.cli_path)
    first = bootstrap.generate_canary_config(**args)
    before = first.read_bytes()
    assert bootstrap.generate_canary_config(**args, preferred_config_path=first) == first
    assert first.read_bytes() == before
    assert len(list(env["tmp_path"].glob("config.canary.local.*.json"))) == 1
    codex["sessions_dir"] += "-changed"
    Path(codex["sessions_dir"]).mkdir()
    with pytest.raises(ValueError, match="RUNTIME_RESTART_REQUIRED"):
        bootstrap.generate_canary_config(**args, preferred_config_path=first, reuse_only=True)
    second = bootstrap.generate_canary_config(**args, preferred_config_path=first)
    assert second != first
    assert compute_config_fingerprint(load_explicit_config(first)) != compute_config_fingerprint(load_explicit_config(second))


def test_send_buttons_default_disabled():
    from html.parser import HTMLParser
    class Buttons(HTMLParser):
        found = {}
        def handle_starttag(self, tag, attrs):
            data = dict(attrs)
            if tag == "button" and data.get("id") in ("c2g-send-button", "g2c-queue-button"):
                self.found[data["id"]] = "disabled" in data
    parser = Buttons()
    parser.feed((Path(m.TEMPLATES_DIR) / "index.html").read_text(encoding="utf-8"))
    assert parser.found == {"c2g-send-button": True, "g2c-queue-button": True}
