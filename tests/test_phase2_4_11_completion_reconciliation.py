"""
Phase 2.4.11 — G2C Completion Reconciliation & Cold-Thread Dispatch Handling Regression Suite.

Covers all 10 criteria from Phase 2.4.11 specification:
1. queue accepted, no turn (CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED)
2. target thread later loads and user turn appears (CODEX_QUEUE_DISPATCHED_TASK_PENDING)
3. assistant final later appears (CODEX_QUEUE_AND_TASK_COMPLETED)
4. exact CANARY-OK reconciliation (transitions to IDLE, logs CANARY_END_TO_END_COMPLETED)
5. wrong nonce rejected
6. wrong assistant final rejected
7. duplicate queue retry remains prohibited
8. completion clears unknown-delivery blocker and reports CANARY_END_TO_END_VERIFIED
9. restart preserves completed evidence
10. no real queue or browser send in tests (strict safety mocks)
"""
import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

import app.main as m
import app.canary_bootstrap as bootstrap
from app.adapters import (
    DeliveryOutcomeUnknownError,
    RealChatGPTBrowserObserver,
    RealCodexQueueAdapter,
    ReadOnlyCodexSessionObserver,
    classify_codex_delivery_outcome,
    inspect_codex_queue_storage,
)
from app.canary import compute_config_fingerprint, canary_request, canary_nonce, canary_prompt
from app.models import (
    CodexDeliveryClassification,
    ShuttleStage,
    TransferRecord,
    TransferAuthorization,
    TransferStatus,
    compute_sha256,
)
from app.database import Database
from app.state_machine import ShuttleStateMachine
from test_phase2_4_5_pre_send_readiness import temp_canary_env


def create_temp_queue_sqlite(directory: Path, thread_id: str, nonce: str, item_id="q-001", client_id="c-001"):
    """Helper to create a temporary queue_1.sqlite with one queued item."""
    q_db = directory / "queue_1.sqlite"
    conn = sqlite3.connect(q_db)
    conn.execute(
        "CREATE TABLE queued_items ("
        "id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, payload_json TEXT NOT NULL, "
        "queue_order INTEGER NOT NULL, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);"
    )
    payload = {
        "UserInput": {
            "content": [
                {
                    "type": "text",
                    "text": f"# Canary Ack\nReturn exactly: CANARY-OK:{nonce}\nDo not inspect files.",
                }
            ],
            "client_id": client_id,
        }
    }
    conn.execute(
        "INSERT INTO queued_items VALUES (?, ?, ?, ?, ?, ?)",
        (item_id, thread_id, json.dumps(payload), 0, 1793040000000, 1793040000000),
    )
    conn.commit()
    conn.close()
    return q_db


@pytest.fixture
async def p2411_env(temp_canary_env, monkeypatch):
    """Fixture providing isolated canary application environment for Phase 2.4.11 testing."""
    env = temp_canary_env
    cfg, database = env["config"], env["db"]
    await database.init()
    await database.save_config(cfg)
    source = env["tmp_path"] / "config.canary.local.20261026-11.json"
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
    monkeypatch.setattr(m, "runtime_browser_status", {
        "connected": True,
        "ready": True,
        "human_verification_required": False,
        "current_url": cfg.chatgpt.conversation_url,
        "interstitial_reason": None,
        "config_fingerprint": compute_config_fingerprint(cfg),
        "send_control_verified": False,
        "send_control": None,
    })
    monkeypatch.setattr(m, "runtime_codex_status", {
        "verified_session_identity": True,
        "session_file": "fixture.jsonl",
    })
    monkeypatch.setattr(m, "validate_canary_session", lambda *a, **k: {"session_file_path": "fixture.jsonl", "total_lines": 30})

    observer = RealChatGPTBrowserObserver(target_url=cfg.chatgpt.conversation_url, cdp_url=cfg.chatgpt.cdp_url)
    observer.read_live_page = AsyncMock(return_value={
        "url": cfg.chatgpt.conversation_url,
        "html": '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div></form>'
    })
    monkeypatch.setattr(m, "real_chatgpt_observer", observer)

    verified = {
        "verdict": "CANARY_PREFLIGHT_VERIFIED",
        "stage": "preflight_verified",
        "config_path": str(source),
        "config_fingerprint": compute_config_fingerprint(cfg),
        "preflight_result": {"status": "verified"},
    }
    await database.save_canary_bootstrap_state(verified)
    monkeypatch.setattr(bootstrap, "run_canary_bootstrap", AsyncMock(return_value=verified))

    sender, queue = AsyncMock(), AsyncMock()
    sender.send_message.side_effect = AssertionError("Live ChatGPT send forbidden in tests")
    queue.queue_message.side_effect = AssertionError("Live Codex queue forbidden in tests")
    monkeypatch.setattr(m, "real_chatgpt_sender", sender)
    monkeypatch.setattr(m, "real_codex_queue", queue)

    async with AsyncClient(transport=ASGITransport(app=m.app), base_url="http://test") as client:
        yield {
            **env,
            "client": client,
            "source": source,
            "observer": observer,
            "sender": sender,
            "queue": queue,
            "verified": verified,
            "machine": machine,
        }

    # Strict invariant: no real send or queue in tests
    sender.send_message.assert_not_called()
    queue.queue_message.assert_not_called()


def test_criterion_1_queue_accepted_no_turn(tmp_path):
    """1. Queue item exists but no user turn appears in rollout (cold thread)."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    create_temp_queue_sqlite(tmp_path, thread_id, nonce, item_id="q-001", client_id="c-001")
    q_info = inspect_codex_queue_storage(thread_id, codex_home=tmp_path, nonce=nonce)

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {"type": "event_msg", "payload": {"type": "task_started"}},
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
        queue_storage_info=q_info,
    )

    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value
    assert result["queue_receipt_status"] == "accepted"
    assert result["turn_dispatch_status"] == "pending"
    assert result["task_completion_status"] == "pending"
    assert result["task_completed"] is False
    assert result["action_instruction"] == "Open/resume target Codex thread — do not resend"


def test_criterion_2_target_thread_later_loads_user_turn_appears(tmp_path):
    """2. Target thread loads and user turn appears with nonce, but assistant is still generating."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "turn_context",
            "payload": {"turn_id": "turn-c2-001"},
        },
        {
            "type": "response_item",
            "payload": {
                "id": "msg-u-001",
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": f"# Canary Ack\nReturn exactly: CANARY-OK:{nonce}"}],
                "turn_id": "turn-c2-001",
            },
            "timestamp": "2026-10-26T21:04:17.200Z",
        },
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
        queue_receipt_accepted=True,
    )

    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_DISPATCHED_TASK_PENDING.value
    assert result["queue_receipt_status"] == "accepted"
    assert result["turn_dispatch_status"] == "dispatched"
    assert result["task_completion_status"] == "pending"
    assert result["task_completed"] is False
    assert result["user_line"] == 2
    assert result["user_message_id"] == "msg-u-001"
    assert result["user_turn_id"] == "turn-c2-001"


def test_criterion_3_assistant_final_later_appears(tmp_path):
    """3. Assistant final response appears with CANARY-OK:{nonce} -> CODEX_QUEUE_AND_TASK_COMPLETED."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "turn_context",
            "payload": {"turn_id": "turn-c3-001"},
        },
        {
            "type": "response_item",
            "payload": {
                "id": "msg-u-002",
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": f"# Canary Ack\nReturn exactly: CANARY-OK:{nonce}\nDo not inspect files."}],
                "turn_id": "turn-c3-001",
            },
            "timestamp": "2026-10-26T21:04:17.200Z",
        },
        {
            "type": "response_item",
            "payload": {
                "id": "msg-a-002",
                "type": "message",
                "role": "assistant",
                "phase": "final_answer",
                "status": "completed",
                "content": [{"type": "output_text", "text": f"CANARY-OK:{nonce}"}],
                "turn_id": "turn-c3-001",
            },
            "timestamp": "2026-10-26T21:04:22.381Z",
        },
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
        queue_receipt_accepted=True,
    )

    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
    assert result["queue_receipt_status"] == "accepted"
    assert result["turn_dispatch_status"] == "dispatched"
    assert result["task_completion_status"] == "completed"
    assert result["task_completed"] is True
    assert result["canary_ok_verified"] is True
    assert result["nonce_matched"] is True
    assert result["user_message_id"] == "msg-u-002"
    assert result["assistant_message_id"] == "msg-a-002"
    assert result["turn_id"] == "turn-c3-001"
    assert result["assistant_reply_text"] == f"CANARY-OK:{nonce}"


@pytest.mark.asyncio
async def test_criterion_4_exact_canary_ok_reconciliation(p2411_env, monkeypatch):
    """4. Exact CANARY-OK reconciliation transitions to IDLE and logs CANARY_END_TO_END_COMPLETED."""
    env = p2411_env
    client = env["client"]
    db = env["db"]
    cfg = env["config"]
    nonce = "d05445c9c98642928640ffbcd08f9d4b"
    tid_c2g = "transfer-c2g-test01"
    tid_g2c = "transfer-g2c-test02"

    # Pre-record completed C2G transfer
    c2g_rec = TransferRecord(
        transfer_id=tid_c2g,
        iteration_id=1,
        direction="codex_to_chatgpt",
        source_identifier=cfg.codex.thread_id,
        destination_identifier=cfg.chatgpt.conversation_url,
        content_hash=compute_sha256(canary_request(nonce)),
        message_text=canary_request(nonce),
        status="delivered",
        provenance_policy=f"canary:{nonce}",
        sent_at=datetime.utcnow() - timedelta(minutes=10),
    )
    await db.save_transfer_record(c2g_rec)

    # Record G2C transfer currently in delivery_outcome_unknown
    g2c_prompt = canary_prompt(nonce)
    g2c_rec = TransferRecord(
        transfer_id=tid_g2c,
        iteration_id=1,
        direction="chatgpt_to_codex",
        source_identifier=cfg.chatgpt.conversation_url,
        destination_identifier=cfg.codex.thread_id,
        content_hash=compute_sha256(g2c_prompt),
        message_text=g2c_prompt,
        status="delivery_outcome_unknown",
        error_message="CLI exited without a correlated queue receipt",
        provenance_policy=f"canary:{nonce}",
        delivery_evidence={
            "queue_receipt_status": "accepted",
            "turn_dispatch_status": "pending",
            "task_completion_status": "pending",
            "receipt_id": "q-rec-001",
            "client_user_message_id": "c-msg-001",
            "baseline_line": 5,
        },
        confirmed_at=datetime.utcnow() - timedelta(minutes=5),
    )
    await db.save_transfer_record(g2c_rec)
    await env["machine"].transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN, transfer_status="unknown", force=True)

    # Mock codex_observer to simulate completed rollout
    mock_obs = ReadOnlyCodexSessionObserver(sessions_base_dir=env["tmp_path"])
    mock_obs.classify_delivery = lambda **kwargs: {
        "classification": "CODEX_QUEUE_AND_TASK_COMPLETED",
        "outcome_classification": "CODEX_QUEUE_AND_TASK_COMPLETED",
        "queue_receipt_status": "accepted",
        "turn_dispatch_status": "dispatched",
        "task_completion_status": "completed",
        "task_completed": True,
        "canary_ok_verified": True,
        "nonce_matched": True,
        "receipt_id": "q-rec-001",
        "client_user_message_id": "c-msg-001",
        "turn_id": "turn-live-001",
        "user_message_id": "msg-user-live-001",
        "assistant_message_id": "msg-asst-live-001",
        "assistant_reply_text": f"CANARY-OK:{nonce}",
        "user_timestamp": "2026-10-26T21:04:17.200Z",
        "assistant_timestamp": "2026-10-26T21:04:22.381Z",
        "user_content_hash": compute_sha256(g2c_prompt),
        "assistant_content_hash": compute_sha256(f"CANARY-OK:{nonce}"),
    }
    monkeypatch.setattr(m, "codex_observer", mock_obs)

    # Reconcile unknown
    recon_res = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": tid_g2c,
        "resolution": "mark_delivered",
        "notes": "Verified live completion in target rollout",
    })
    assert recon_res.status_code == 200, recon_res.text
    data = recon_res.json()
    assert data["status"] == "reconciled"
    assert data["new_stage"] == "IDLE"
    assert data["queue_receipt_status"] == "accepted"
    assert data["turn_dispatch_status"] == "dispatched"
    assert data["task_completion_status"] == "completed"
    assert data["outcome_classification"] == "CODEX_QUEUE_AND_TASK_COMPLETED"

    # Verify state machine stage
    assert env["machine"].stage == ShuttleStage.IDLE

    # Verify audit events
    events = await db.get_event_logs(limit=50)
    e2e_events = [e for e in events if e.get("event_type") == "CANARY_END_TO_END_COMPLETED"]
    assert len(e2e_events) >= 1
    det = e2e_events[0].get("details")
    if isinstance(det, str):
        det = json.loads(det)
    assert det["transfer_id"] == tid_g2c
    assert det["nonce"] == nonce
    assert det["c2g_transfer_id"] == tid_c2g
    assert det["task_completion_status"] == "completed"


def test_criterion_5_wrong_nonce_rejected():
    """5. Rollout with mismatched nonce is strictly rejected."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    expected_nonce = "d05445c9c98642928640ffbcd08f9d4b"
    wrong_nonce = "ffffffffffffffffffffffffffffffff"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "response_item",
            "payload": {
                "id": "msg-u-wrong",
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": f"# Canary Ack\nReturn CANARY-OK:{wrong_nonce}"}],
                "turn_id": "turn-wrong",
            },
        },
        {
            "type": "response_item",
            "payload": {
                "id": "msg-a-wrong",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": f"CANARY-OK:{wrong_nonce}"}],
                "turn_id": "turn-wrong",
            },
        },
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=expected_nonce,
        rollout_lines=rollout_lines,
        queue_receipt_accepted=True,
    )

    assert result["user_line"] is None
    assert result["assistant_line"] is None
    assert result["canary_ok_verified"] is False
    assert result["nonce_matched"] is False
    assert result["classification"] != CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value


def test_criterion_6_wrong_assistant_final_rejected():
    """6. Correct prompt with nonce but assistant replied with something other than CANARY-OK:{nonce}."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "response_item",
            "payload": {
                "id": "msg-u-006",
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": f"# Canary Ack\nReturn CANARY-OK:{nonce}"}],
                "turn_id": "turn-006",
            },
        },
        {
            "type": "response_item",
            "payload": {
                "id": "msg-a-006",
                "type": "message",
                "role": "assistant",
                "phase": "final_answer",
                "status": "completed",
                "content": [{"type": "output_text", "text": "I am not able to comply with the requested ack format."}],
                "turn_id": "turn-006",
            },
        },
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
        queue_receipt_accepted=True,
    )

    assert result["user_line"] == 1
    assert result["assistant_line"] == 2
    assert result["canary_ok_verified"] is False
    assert result["nonce_matched"] is False
    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_DISPATCHED_TASK_PENDING.value
    assert result["task_completion_status"] == "pending"


@pytest.mark.asyncio
async def test_criterion_7_duplicate_queue_retry_prohibited(p2411_env):
    """7. Duplicate queue retry remains prohibited while a queue/transfer exists or cycle is finished."""
    env = p2411_env
    client = env["client"]
    db = env["db"]
    cfg = env["config"]
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    # Pre-record delivered C2G and completed G2C
    await db.save_transfer_record(TransferRecord(
        transfer_id="t-c2g-007",
        direction="codex_to_chatgpt",
        source_identifier=cfg.codex.thread_id,
        destination_identifier=cfg.chatgpt.conversation_url,
        content_hash="h1",
        status="delivered",
        provenance_policy=f"canary:{nonce}",
    ))
    await db.save_transfer_record(TransferRecord(
        transfer_id="t-g2c-007",
        direction="chatgpt_to_codex",
        source_identifier=cfg.chatgpt.conversation_url,
        destination_identifier=cfg.codex.thread_id,
        content_hash="h2",
        status="delivered",
        provenance_policy=f"canary:{nonce}",
        delivery_evidence={"task_completion_status": "completed"},
    ))

    # Check readiness buttons
    refresh = (await client.get("/api/runtime/refresh")).json()
    assert refresh["readiness"]["g2c_queue_enabled"] is False
    assert refresh["readiness"]["c2g_send_enabled"] is False

    # Attempt to call queue endpoint directly without authorization token
    q_res = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": "t-g2c-007",
        "confirmation_token": "token-007",
        "thread_id": cfg.codex.thread_id,
    })
    assert q_res.status_code == 400
    assert "cannot submit" in q_res.json()["detail"]


@pytest.mark.asyncio
async def test_criterion_8_completion_clears_unknown_delivery_blocker(p2411_env, monkeypatch):
    """8. Successful completion clears unknown-delivery blocker and reports CANARY_END_TO_END_VERIFIED."""
    env = p2411_env
    client = env["client"]
    db = env["db"]
    cfg = env["config"]
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    # Create C2G delivered
    await db.save_transfer_record(TransferRecord(
        transfer_id="t-c2g-008",
        direction="codex_to_chatgpt",
        source_identifier=cfg.codex.thread_id,
        destination_identifier=cfg.chatgpt.conversation_url,
        content_hash="hash-c2g",
        status="delivered",
        provenance_policy=f"canary:{nonce}",
        sent_at=datetime.utcnow() - timedelta(minutes=15),
    ))

    # Create G2C unknown
    await db.save_transfer_record(TransferRecord(
        transfer_id="t-g2c-008",
        direction="chatgpt_to_codex",
        source_identifier=cfg.chatgpt.conversation_url,
        destination_identifier=cfg.codex.thread_id,
        content_hash="hash-g2c",
        status="delivery_outcome_unknown",
        error_message="timeout",
        provenance_policy=f"canary:{nonce}",
        delivery_evidence={"baseline_line": 0},
    ))
    await env["machine"].transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN, transfer_status="unknown", force=True)

    # Before reconciliation: blocked
    guard_before = (await client.get("/api/transfer/target_info")).json()
    assert guard_before["blocking_reason"] == "Previous delivery requires manual reconciliation"

    # Mock observer returning completed
    mock_obs = ReadOnlyCodexSessionObserver(sessions_base_dir=env["tmp_path"])
    mock_obs.classify_delivery = lambda **k: {
        "classification": "CODEX_QUEUE_AND_TASK_COMPLETED",
        "outcome_classification": "CODEX_QUEUE_AND_TASK_COMPLETED",
        "queue_receipt_status": "accepted",
        "turn_dispatch_status": "dispatched",
        "task_completion_status": "completed",
        "task_completed": True,
        "canary_ok_verified": True,
        "nonce_matched": True,
        "receipt_id": "q-888",
        "client_user_message_id": "c-888",
        "turn_id": "turn-888",
        "assistant_reply_text": f"CANARY-OK:{nonce}",
    }
    monkeypatch.setattr(m, "codex_observer", mock_obs)

    # Reconcile delivered
    recon = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": "t-g2c-008",
        "resolution": "mark_delivered",
        "notes": "E2E complete",
    })
    assert recon.status_code == 200
    assert recon.json()["new_stage"] == "IDLE"

    # After reconciliation: blocker cleared and status verified
    guard_after = (await client.get("/api/transfer/target_info")).json()
    assert guard_after["blocking_reason"] is None

    refresh_after = (await client.get("/api/runtime/refresh")).json()
    assert refresh_after["readiness"]["transfers"]["unresolved_delivery"] is False
    assert refresh_after["readiness"]["status"] == "CANARY_END_TO_END_VERIFIED"
    assert refresh_after["readiness"]["blocker"] is None


@pytest.mark.asyncio
async def test_criterion_9_restart_preserves_completed_evidence(p2411_env, monkeypatch):
    """9. Restarting the backend preserves the completed evidence and IDLE stage."""
    env = p2411_env
    db = env["db"]
    cfg = env["config"]
    nonce = "d05445c9c98642928640ffbcd08f9d4b"
    tid = "t-g2c-009"

    # Perform reconciliation in db
    await db.save_transfer_record(TransferRecord(
        transfer_id=tid,
        direction="chatgpt_to_codex",
        source_identifier=cfg.chatgpt.conversation_url,
        destination_identifier=cfg.codex.thread_id,
        content_hash="hash-009",
        status="delivery_outcome_unknown",
        provenance_policy=f"canary:{nonce}",
    ))
    await env["machine"].transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN, transfer_status="unknown", force=True)

    evidence_update = {
        "queue_receipt_status": "accepted",
        "turn_dispatch_status": "dispatched",
        "task_completion_status": "completed",
        "outcome_classification": "CODEX_QUEUE_AND_TASK_COMPLETED",
        "turn_id": "turn-009",
        "assistant_reply_text": f"CANARY-OK:{nonce}",
    }
    await db.reconcile_unknown_outcome_transaction(
        transfer_id=tid,
        resolution="mark_delivered",
        notes="Reconciled for test 9",
        delivery_evidence_update=evidence_update,
    )

    # Re-instantiate database and state machine (simulating cold process restart)
    restarted_db = Database(db.db_path)
    restarted_machine = ShuttleStateMachine(restarted_db, cfg)
    await restarted_machine.init()

    assert restarted_machine.stage == ShuttleStage.IDLE

    # Verify persisted transfer
    t = await restarted_db.get_transfer_by_id(tid)
    assert t.status == "delivered"
    assert t.queue_receipt_status == "accepted"
    assert t.turn_dispatch_status == "dispatched"
    assert t.task_completion_status == "completed"
    assert t.outcome_classification == "CODEX_QUEUE_AND_TASK_COMPLETED"

    # Verify persisted audit event
    logs = await restarted_db.get_event_logs(limit=20)
    e2e_logs = [l for l in logs if l.get("event_type") == "CANARY_END_TO_END_COMPLETED"]
    assert len(e2e_logs) >= 1


@pytest.mark.asyncio
async def test_criterion_10_no_real_queue_or_browser_send(p2411_env):
    """10. Verify that no real queue subprocess or browser send is ever triggered in automated tests."""
    env = p2411_env
    sender = env["sender"]
    queue = env["queue"]

    # Both must remain uncalled
    assert sender.send_message.call_count == 0
    assert queue.queue_message.call_count == 0
