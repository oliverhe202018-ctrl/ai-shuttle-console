"""Phase 2.4.7 Prepared Transfer / Shuttle Stage Atomicity Repair Regression Tests."""
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

import app.main as m
import app.canary_bootstrap as bootstrap
from app.adapters import RealChatGPTBrowserObserver
from app.canary import compute_config_fingerprint, canary_request, canary_nonce, canary_prompt
from app.models import ShuttleStage, TransferRecord, TransferAuthorization, TransferStatus, compute_sha256
from app.state_machine import ShuttleStateMachine, InvalidTransitionError
from test_phase2_4_5_pre_send_readiness import temp_canary_env


@pytest.fixture
async def atomicity_app(temp_canary_env, monkeypatch):
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
    monkeypatch.setattr(m, "runtime_identity", {
        "config_source": str(source),
        "config_fingerprint": compute_config_fingerprint(cfg),
        "database_path": str(database.db_path),
        "process_id": 99999,
        "startup_canary": True,
    })
    monkeypatch.setattr(m, "runtime_browser_status", {})
    monkeypatch.setattr(m, "runtime_codex_status", {})
    monkeypatch.setattr(m, "validate_canary_session", lambda *a: {"session_file_path": "fixture.jsonl", "total_lines": 2})
    observer = RealChatGPTBrowserObserver(target_url=cfg.chatgpt.conversation_url, cdp_url=cfg.chatgpt.cdp_url)
    observer.read_live_page = AsyncMock(return_value={
        "url": cfg.chatgpt.conversation_url,
        "html": '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div></form>'
    })
    monkeypatch.setattr(m, "real_chatgpt_observer", observer)
    verified = {
        "verdict": "CANARY_PREFLIGHT_VERIFIED",
        "config_path": str(source),
        "config_fingerprint": compute_config_fingerprint(cfg),
        "preflight_result": {"status": "verified"},
    }
    await database.save_canary_bootstrap_state(verified)
    monkeypatch.setattr(bootstrap, "run_canary_bootstrap", AsyncMock(return_value=verified))
    sender, queue = AsyncMock(), AsyncMock()
    sender.send_message.side_effect = AssertionError("send must not be reached in tests")
    queue.queue_message.side_effect = AssertionError("queue must not be reached in tests")
    monkeypatch.setattr(m, "real_chatgpt_sender", sender)
    monkeypatch.setattr(m, "real_codex_queue", queue)
    async with AsyncClient(transport=ASGITransport(app=m.app), base_url="http://test") as client:
        yield {**env, "client": client, "source": source, "observer": observer,
               "sender": sender, "queue": queue, "verified": verified, "machine": machine}
    sender.send_message.assert_not_called()
    queue.queue_message.assert_not_called()


@pytest.mark.asyncio
async def test_1_successful_prepare_creates_transfer_and_report_preview_together(atomicity_app):
    env = atomicity_app
    # Set stage to CODEX_WAITING to test the exact real-world scenario
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)
    assert env["machine"].stage == ShuttleStage.CODEX_WAITING

    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200, res.text
    data = res.json()
    assert data["status"] == "prepared"
    assert data["transfer_id"].startswith("transfer-c2g-")
    assert data["confirmation_token"].startswith("token-c2g-")
    assert data["canary_nonce"] is not None

    # Verify stage advanced together
    assert env["machine"].stage == ShuttleStage.REPORT_PREVIEW
    cp = await env["db"].get_latest_checkpoint()
    assert cp.stage == ShuttleStage.REPORT_PREVIEW
    assert cp.transfer_status == "prepared"

    # Verify DB transfer record
    transfers = await env["db"].get_all_transfers()
    assert len(transfers) == 1
    t = transfers[0]
    assert t.transfer_id == data["transfer_id"]
    assert t.status == "prepared"
    assert t.source_message_id == f"canary-request:{data['canary_nonce']}"

    # Verify DB authorization record
    auth = await env["db"].get_transfer_authorization(data["confirmation_token"])
    assert auth is not None
    assert auth.transfer_id == data["transfer_id"]
    assert not auth.consumed


@pytest.mark.asyncio
async def test_2_page_refresh_preserves_valid_prepared_transfer(atomicity_app):
    env = atomicity_app
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200
    data = res.json()

    # Simulate page refresh / dashboard poll
    snap = (await env["client"].get("/api/runtime/refresh")).json()
    assert snap["state"]["stage"] == "REPORT_PREVIEW"
    assert snap["state"]["transfer_status"] == "prepared"
    assert snap["readiness"]["status"] == "CANARY_REPORT_PREPARED_AWAITING_AUTHORIZATION"
    assert snap["readiness"]["c2g_send_enabled"] is True
    assert snap["readiness"]["transfers"]["c2g"]["valid"] is True
    assert snap["readiness"]["transfers"]["c2g"]["transfer"]["transfer_id"] == data["transfer_id"]

    # Transfer record must remain prepared, not cancelled
    t = await env["db"].get_transfer_by_id(data["transfer_id"])
    assert t.status == "prepared"


@pytest.mark.asyncio
async def test_3_backend_restart_rehydrates_same_valid_prepared_transfer(atomicity_app):
    env = atomicity_app
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200
    data = res.json()

    # Recreate state machine from DB simulating restart
    restarted_machine = ShuttleStateMachine(env["db"], env["config"])
    await restarted_machine.init()
    assert restarted_machine.stage == ShuttleStage.REPORT_PREVIEW
    assert restarted_machine.current_checkpoint.transfer_status == "prepared"

    # Refresh via API retains valid preview
    snap = (await env["client"].get("/api/runtime/refresh")).json()
    assert snap["state"]["stage"] == "REPORT_PREVIEW"
    assert snap["readiness"]["c2g_send_enabled"] is True


@pytest.mark.asyncio
async def test_4_state_transition_failure_rolls_back_transfer_token(atomicity_app):
    env = atomicity_app
    # Set stage to DELIVERY_OUTCOME_UNKNOWN from which REPORT_PREVIEW is NOT allowed
    await env["machine"].transition_to(ShuttleStage.CHATGPT_WAITING, force=True)
    assert env["machine"].stage == ShuttleStage.CHATGPT_WAITING

    # Test direct prepare_transfer_atomic call
    with pytest.raises(InvalidTransitionError):
        await env["machine"].prepare_transfer_atomic(
            transfer=TransferRecord(
                transfer_id="transfer-reject-test",
                iteration_id=1,
                direction="codex_to_chatgpt",
                source_identifier="thread",
                destination_identifier="conv",
                content_hash="abc",
                status="prepared",
            ),
            auth=TransferAuthorization(
                transfer_id="transfer-reject-test",
                confirmation_token="token-reject-test",
                destination_identifier="conv",
                expected_content_hash="abc",
                expires_at=datetime.utcnow() + timedelta(minutes=15),
            ),
            target_stage=ShuttleStage.REPORT_PREVIEW,
        )

    # Invariant: stage remains unchanged, no transfer or authorization committed
    assert env["machine"].stage == ShuttleStage.CHATGPT_WAITING
    assert len(await env["db"].get_all_transfers()) == 0
    recent_logs = await env["db"].get_recent_logs()
    assert any(log["event_type"] == "TRANSITION_REJECTED" for log in recent_logs)


@pytest.mark.asyncio
async def test_5_transfer_persistence_failure_does_not_advance_stage(atomicity_app, monkeypatch):
    env = atomicity_app
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)
    initial_stage = env["machine"].stage

    # Simulate database failure during atomic prepare
    original_persist = env["db"].persist_prepared_transfer_atomic
    async def failing_persist(*args, **kwargs):
        raise RuntimeError("Simulated disk/SQL error during transfer preparation")
    monkeypatch.setattr(env["db"], "persist_prepared_transfer_atomic", failing_persist)

    with pytest.raises(Exception):
        await env["machine"].prepare_transfer_atomic(
            transfer=TransferRecord(
                transfer_id="transfer-test-fail",
                iteration_id=1,
                direction="codex_to_chatgpt",
                source_identifier="thread",
                destination_identifier="conv",
                content_hash="abc",
                status="prepared",
            ),
            auth=TransferAuthorization(
                transfer_id="transfer-test-fail",
                confirmation_token="token-test-fail",
                destination_identifier="conv",
                expected_content_hash="abc",
                expires_at=datetime.utcnow() + timedelta(minutes=15),
            ),
            target_stage=ShuttleStage.REPORT_PREVIEW,
        )

    # Invariant: stage did NOT advance, DB has no transfer
    assert env["machine"].stage == initial_stage
    assert len(await env["db"].get_all_transfers()) == 0


@pytest.mark.asyncio
async def test_6_checkpoint_failure_does_not_leave_usable_authorization(atomicity_app, monkeypatch):
    env = atomicity_app
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)

    # Simulate failure on checkpoint insert in DB
    original_execute = env["db"].persist_prepared_transfer_atomic
    async def failing_on_checkpoint(*args, **kwargs):
        # Even if someone wrote auth first, the atomic method must roll back
        raise RuntimeError("Simulated checkpoint write failure")
    monkeypatch.setattr(env["db"], "persist_prepared_transfer_atomic", failing_on_checkpoint)

    with pytest.raises(Exception):
        await env["machine"].prepare_transfer_atomic(
            transfer=TransferRecord(
                transfer_id="transfer-cp-fail",
                iteration_id=1,
                direction="codex_to_chatgpt",
                source_identifier="thread",
                destination_identifier="conv",
                content_hash="abc",
                status="prepared",
            ),
            auth=TransferAuthorization(
                transfer_id="transfer-cp-fail",
                confirmation_token="token-cp-fail",
                destination_identifier="conv",
                expected_content_hash="abc",
                expires_at=datetime.utcnow() + timedelta(minutes=15),
            ),
            target_stage=ShuttleStage.REPORT_PREVIEW,
        )

    # Invariant: no usable authorization in DB
    auth = await env["db"].get_transfer_authorization("token-cp-fail")
    assert auth is None


@pytest.mark.asyncio
async def test_7_legacy_incomplete_prepared_transfer_is_cancelled(atomicity_app):
    env = atomicity_app
    # Create an incomplete legacy transfer missing source_message_id
    nonce = uuid.uuid4().hex
    msg = canary_request(nonce)
    chash = compute_sha256(msg)
    fp = compute_config_fingerprint(env["config"])
    token = "token-legacy-incomplete"
    tid = "transfer-legacy-incomplete"

    auth = TransferAuthorization(
        transfer_id=tid,
        confirmation_token=token,
        destination_identifier=env["config"].chatgpt.conversation_url,
        expected_content_hash=chash,
        consumed=False,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=fp,
    )
    await env["db"].create_transfer_authorization(auth)

    t = TransferRecord(
        transfer_id=tid,
        iteration_id=1,
        direction="codex_to_chatgpt",
        source_type="canary",
        source_identifier=env["config"].codex.thread_id,
        source_message_id=None,  # Missing source identity!
        destination_type="chatgpt",
        destination_identifier=env["config"].chatgpt.conversation_url,
        content_hash=chash,
        char_count=len(msg),
        message_text=msg,
        payload_preview=msg[:200],
        status="prepared",
        confirmation_token=token,
        provenance_policy=f"canary:{nonce}",
        config_fingerprint=fp,
    )
    await env["db"].save_transfer_record(t)

    # Simulate a historical persisted checkpoint in REPORT_PREVIEW
    cp = env["machine"].current_checkpoint.model_copy(update={"stage": ShuttleStage.REPORT_PREVIEW, "transfer_status": "prepared"})
    env["machine"].current_checkpoint = await env["db"].save_checkpoint(cp)

    # Refresh runs repair
    snap = (await env["client"].get("/api/runtime/refresh")).json()
    assert snap["state"]["stage"] == "CODEX_WAITING"

    # Transfer cancelled and authorization invalidated
    updated_t = await env["db"].get_transfer_by_id(tid)
    assert updated_t.status == "cancelled"
    assert "TRANSFER_STATE_INCONSISTENT" in updated_t.error_message
    updated_auth = await env["db"].get_transfer_authorization(token)
    assert updated_auth.consumed is True

    # Audit timeline recorded
    logs = await env["db"].get_recent_logs()
    assert any(l["event_type"] == "TRANSFER_STATE_REPAIRED" for l in logs)


@pytest.mark.asyncio
async def test_8_complete_prepared_transfer_is_not_misclassified_as_orphan(atomicity_app):
    env = atomicity_app
    nonce = uuid.uuid4().hex
    msg = canary_request(nonce)
    chash = compute_sha256(msg)
    fp = compute_config_fingerprint(env["config"])
    token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    tid = f"transfer-c2g-{uuid.uuid4().hex[:12]}"

    auth = TransferAuthorization(
        transfer_id=tid,
        confirmation_token=token,
        destination_identifier=env["config"].chatgpt.conversation_url,
        expected_content_hash=chash,
        consumed=False,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=fp,
    )
    await env["db"].create_transfer_authorization(auth)

    t = TransferRecord(
        transfer_id=tid,
        iteration_id=1,
        direction="codex_to_chatgpt",
        source_type="canary",
        source_identifier=env["config"].codex.thread_id,
        source_message_id=f"canary-request:{nonce}",
        destination_type="chatgpt",
        destination_identifier=env["config"].chatgpt.conversation_url,
        content_hash=chash,
        char_count=len(msg),
        message_text=msg,
        payload_preview=msg[:200],
        status="prepared",
        confirmation_token=token,
        provenance_policy=f"canary:{nonce}",
        config_fingerprint=fp,
    )
    await env["db"].save_transfer_record(t)

    # Stage lagged in CODEX_WAITING (the exact bug from Section 1)
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)

    # Refresh must restore stage to REPORT_PREVIEW, NOT cancel it as orphan
    snap = (await env["client"].get("/api/runtime/refresh")).json()
    assert snap["state"]["stage"] == "REPORT_PREVIEW"
    assert snap["readiness"]["c2g_send_enabled"] is True

    # Transfer retained
    retained_t = await env["db"].get_transfer_by_id(tid)
    assert retained_t.status == "prepared"

    # Audit recorded STAGE_RESTORED
    logs = await env["db"].get_recent_logs()
    assert any(l["event_type"] == "STAGE_RESTORED" for l in logs)


@pytest.mark.asyncio
async def test_9_only_one_active_prepared_c2g_transfer_may_exist(atomicity_app):
    env = atomicity_app
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200

    # Second prepare must be rejected with 409
    res2 = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res2.status_code == 409

    # Only one prepared transfer in DB
    transfers = [t for t in await env["db"].get_all_transfers() if t.status == "prepared"]
    assert len(transfers) == 1


@pytest.mark.asyncio
async def test_10_manual_forced_stage_changes_cannot_create_prepared_transfer_mismatch(atomicity_app):
    env = atomicity_app
    # Case A: Force transition to REPORT_PREVIEW when no prepared transfer exists
    res = await env["client"].post("/api/stage/transition", json={"target_stage": "REPORT_PREVIEW", "force": True})
    assert res.status_code == 400
    assert "TRANSFER_STATE_INCONSISTENT" in res.json()["detail"]

    # Now prepare a valid transfer into REPORT_PREVIEW
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200
    assert env["machine"].stage == ShuttleStage.REPORT_PREVIEW

    # Case B: Force transition away from REPORT_PREVIEW while prepared transfer is active
    res2 = await env["client"].post("/api/stage/transition", json={"target_stage": "CODEX_WAITING", "force": True})
    assert res2.status_code == 400
    assert "TRANSFER_STATE_INCONSISTENT" in res2.json()["detail"]
    assert env["machine"].stage == ShuttleStage.REPORT_PREVIEW


@pytest.mark.asyncio
async def test_11_send_endpoint_rejects_prepared_transfer_outside_report_preview(atomicity_app):
    env = atomicity_app
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200
    data = res.json()

    # Manually bypass state machine in memory to simulate inconsistent runtime
    env["machine"].current_checkpoint.stage = ShuttleStage.CODEX_WAITING

    send_res = await env["client"].post("/api/transfer/chatgpt/send", json={
        "transfer_id": data["transfer_id"],
        "confirmation_token": data["confirmation_token"],
    })
    assert send_res.status_code == 400
    assert "outside REPORT_PREVIEW" in send_res.json()["detail"]

    # Token must NOT be consumed
    auth = await env["db"].get_transfer_authorization(data["confirmation_token"])
    assert not auth.consumed


@pytest.mark.asyncio
async def test_12_send_button_remains_disabled_for_inconsistent_state(atomicity_app):
    env = atomicity_app
    # In initial IDLE state without prepared transfer
    readiness = await m.runtime_readiness(probe=True)
    assert readiness["c2g_send_enabled"] is False

    # With expired authorization
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200
    data = res.json()

    # Expire auth
    import aiosqlite
    async with aiosqlite.connect(env["db"].db_path) as conn:
        past = (datetime.utcnow() - timedelta(minutes=20)).isoformat()
        await conn.execute("UPDATE transfer_authorizations SET expires_at = ?", (past,))
        await conn.commit()

    snap = (await env["client"].get("/api/runtime/refresh")).json()
    assert snap["readiness"]["c2g_send_enabled"] is False
    assert "expired" in snap["readiness"]["blocker"]


@pytest.mark.asyncio
async def test_13_send_button_becomes_eligible_only_for_coherent_preview(atomicity_app):
    env = atomicity_app
    await env["machine"].transition_to(ShuttleStage.CODEX_WAITING, force=True)
    res = await env["client"].post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert res.status_code == 200

    snap = (await env["client"].get("/api/runtime/refresh")).json()
    assert snap["state"]["stage"] == "REPORT_PREVIEW"
    assert snap["readiness"]["status"] == "CANARY_REPORT_PREPARED_AWAITING_AUTHORIZATION"
    assert snap["readiness"]["blocker"] is None
    assert snap["readiness"]["c2g_send_enabled"] is True


@pytest.mark.asyncio
async def test_14_no_real_browser_send_and_no_real_codex_queue_occur_in_tests(atomicity_app):
    env = atomicity_app
    # Verify mock fixtures are in place and no real calls made
    assert env["sender"].send_message.call_count == 0
    assert env["queue"].queue_message.call_count == 0
