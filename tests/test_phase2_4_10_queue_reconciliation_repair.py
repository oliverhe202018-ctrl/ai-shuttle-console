"""
Phase 2.4.10 — Codex Queue Receipt, Dispatch & Completion Reconciliation Repair Regression Suite.

Covers all 14 criteria:
1. CLI acknowledgement with current output format (`Queued message <id> for thread <id>.`)
2. exit 0 with valid receipt
3. exit 0 with no recognizable receipt
4. queue item exists but no user turn (CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED)
5. user turn exists but no assistant final (CODEX_QUEUE_DISPATCHED_TASK_PENDING)
6. completed assistant final exists (CODEX_QUEUE_AND_TASK_COMPLETED)
7. active-goal delayed queue scenario
8. interrupted-thread pending queue scenario
9. duplicate retry prohibited while queue item exists
10. client message ID correlation
11. wrong thread receipt rejected
12. wrong nonce rejected
13. queue receipt != task completion
14. no real queue subprocess in automated tests (mocking / runner injection only)
"""
import json
import sqlite3
import subprocess
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


def create_temp_goals_sqlite(directory: Path, thread_id: str, status="active", objective="Active canary goal"):
    """Helper to create a temporary goals_1.sqlite with a thread goal."""
    g_db = directory / "goals_1.sqlite"
    conn = sqlite3.connect(g_db)
    conn.execute(
        "CREATE TABLE thread_goals ("
        "thread_id TEXT PRIMARY KEY, goal_id TEXT, objective TEXT, status TEXT, "
        "token_budget INTEGER, tokens_used INTEGER, time_used_seconds INTEGER, "
        "created_at_ms INTEGER, updated_at_ms INTEGER);"
    )
    conn.execute(
        "INSERT INTO thread_goals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (thread_id, "goal-1", objective, status, None, 100, 10, 1793040000000, 1793040000000),
    )
    conn.commit()
    conn.close()
    return g_db


@pytest.fixture
async def p2410_env(temp_canary_env, monkeypatch):
    """Fixture providing isolated canary application environment for Phase 2.4.10 testing."""
    env = temp_canary_env
    cfg, database = env["config"], env["db"]
    await database.init()
    await database.save_config(cfg)
    source = env["tmp_path"] / "config.canary.local.20261026-10.json"
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
        "send_control_verified": True,
        "send_control": "button[data-testid='send-button']",
    })
    monkeypatch.setattr(m, "runtime_codex_status", {
        "verified_session_identity": True,
        "session_file": "fixture.jsonl",
    })
    monkeypatch.setattr(m, "validate_canary_session", lambda *a, **k: {
        "session_file_path": str(env["session_file"]),
        "total_lines": 17,
        "metadata_valid": True,
    })

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

    sender = AsyncMock()
    sender.send_message.side_effect = AssertionError("Live ChatGPT send forbidden in tests")
    mock_codex_obs = AsyncMock()
    mock_codex_obs.observe_thread = lambda *a, **k: {
        "validation_errors": [],
        "metadata_valid": True,
        "session_meta": {"id": cfg.codex.thread_id, "cwd": cfg.codex.repo_path},
    }
    queue_adapter = RealCodexQueueAdapter(
        cli_path="codex.exe",
        default_repo_path=cfg.codex.repo_path,
        db=None,
        observer=mock_codex_obs,
        verified_thread_id=None,
    )
    monkeypatch.setattr(m, "real_codex_queue", queue_adapter)

    async with AsyncClient(transport=ASGITransport(app=m.app), base_url="http://test") as client:
        yield {
            **env,
            "client": client,
            "source": source,
            "observer": observer,
            "sender": sender,
            "queue_adapter": queue_adapter,
            "verified": verified,
            "machine": machine,
        }

    sender.send_message.assert_not_called()


# ---------------------------------------------------------------- Tests


def test_criterion_1_cli_acknowledgement_current_format():
    """1. Installed CLI output format ('Queued message <id> for thread <id>.') is parsed correctly."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    client_id = "01a1798c-931f-74c1-b960-1f0f4065e054"
    output = f"Queued message {client_id} for thread {thread_id}."

    parsed = RealCodexQueueAdapter.parse_queue_cli_output(output, expected_thread_id=thread_id)
    assert parsed["receipt_id"] == client_id
    assert parsed["client_user_message_id"] == client_id
    assert parsed["thread_id"] == thread_id
    assert parsed["format"] == "text"


@pytest.mark.asyncio
async def test_criterion_2_exit_0_with_valid_receipt(p2410_env):
    """2. Subprocess exit 0 with recognizable receipt returns accepted queue receipt."""
    env = p2410_env
    thread_id = env["config"].codex.thread_id
    client_id = "01a1798c-931f-74c1-b960-1f0f4065e054"
    text_out = f"Queued message {client_id} for thread {thread_id}.\n"

    adapter = env["queue_adapter"]
    adapter.set_injected_runner(lambda *a, **k: subprocess.CompletedProcess(
        args=["codex", "queue"], returncode=0, stdout=text_out, stderr=""
    ))

    res = await adapter.queue_message(
        thread_id=thread_id,
        message="# Canary Ack\nReturn CANARY-OK:1234",
        repo_path=env["config"].codex.repo_path,
        check_existing_queue=False,
    )

    assert res["status"] == "queued"
    assert res["queue_receipt_status"] == "accepted"
    assert res["receipt_id"] == client_id
    assert res["client_user_message_id"] == client_id
    assert res["turn_dispatch_status"] == "pending"
    assert res["task_completion_status"] == "pending"
    assert res["task_completed"] is False
    assert res["outcome_classification"] == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value

    # Also verify JSON receipt variant
    json_out = json.dumps({
        "status": "queued",
        "id": "q-json-receipt-999",
        "client_id": "client-json-888",
        "thread_id": thread_id,
    })
    adapter.set_injected_runner(lambda *a, **k: subprocess.CompletedProcess(
        args=["codex", "queue"], returncode=0, stdout=json_out, stderr=""
    ))

    res_json = await adapter.queue_message(
        thread_id=thread_id,
        message="# Canary Ack\nReturn CANARY-OK:1234",
        repo_path=env["config"].codex.repo_path,
        check_existing_queue=False,
    )
    assert res_json["receipt_id"] == "q-json-receipt-999"
    assert res_json["client_user_message_id"] == "client-json-888"


def test_criterion_3_exit_0_with_no_recognizable_receipt():
    """3. Exit 0 with empty or unrecognizable receipt raises DeliveryOutcomeUnknownError."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"

    # Empty stdout
    with pytest.raises(DeliveryOutcomeUnknownError, match="CLI exited without a correlated queue receipt"):
        RealCodexQueueAdapter.parse_queue_cli_output("", expected_thread_id=thread_id)

    # Random text without queue receipt pattern
    with pytest.raises(DeliveryOutcomeUnknownError, match="CLI exited without a correlated queue receipt"):
        RealCodexQueueAdapter.parse_queue_cli_output("Codex session updated successfully", expected_thread_id=thread_id)


def test_criterion_4_queue_item_exists_no_user_turn(tmp_path):
    """4. Queue item exists in queue_1.sqlite but no user turn in rollout -> CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    create_temp_queue_sqlite(tmp_path, thread_id, nonce, item_id="q-001", client_id="c-001")
    q_info = inspect_codex_queue_storage(thread_id, codex_home=tmp_path, nonce=nonce)
    assert q_info["has_matching_item"] is True

    # Initial session rollout lines (no user message with canary nonce)
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
    assert result["receipt_id"] == "q-001"
    assert result["client_user_message_id"] == "c-001"
    assert result["recommended_action"] == "record_queue_accepted"


def test_criterion_5_user_turn_exists_no_assistant_final():
    """5. User turn exists in rollout but assistant reply not completed -> CODEX_QUEUE_DISPATCHED_TASK_PENDING."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": f"# Canary Ack\nReturn CANARY-OK:{nonce}"}],
                "turn_id": "turn-user-001",
            },
        },
        # No assistant final answer yet
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
    )

    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_DISPATCHED_TASK_PENDING.value
    assert result["queue_receipt_status"] == "accepted"
    assert result["turn_dispatch_status"] == "dispatched"
    assert result["task_completion_status"] == "pending"
    assert result["task_completed"] is False
    assert result["turn_id"] == "turn-user-001"
    assert result["recommended_action"] == "wait_for_completion"


def test_criterion_6_completed_assistant_final_exists():
    """6. Completed assistant final answer equals CANARY-OK:<nonce> -> CODEX_QUEUE_AND_TASK_COMPLETED."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": f"# Canary Ack\nReturn CANARY-OK:{nonce}"}],
                "turn_id": "turn-user-001",
            },
        },
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": f"CANARY-OK:{nonce}"}],
                "turn_id": "turn-user-001",
            },
        },
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
    )

    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
    assert result["queue_receipt_status"] == "accepted"
    assert result["turn_dispatch_status"] == "dispatched"
    assert result["task_completion_status"] == "completed"
    assert result["task_completed"] is True
    assert result["canary_ok_verified"] is True
    assert result["recommended_action"] == "allow_reconciliation_delivered"


def test_criterion_7_active_goal_delayed_queue_scenario(tmp_path):
    """7. Active goal on target thread is detected and noted as cause for delayed queue dispatch."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    create_temp_queue_sqlite(tmp_path, thread_id, nonce)
    create_temp_goals_sqlite(tmp_path, thread_id, status="active", objective="Long-running autonomous task")

    q_info = inspect_codex_queue_storage(thread_id, codex_home=tmp_path, nonce=nonce)
    assert q_info["has_matching_item"] is True
    assert q_info["has_active_goal"] is True
    assert q_info["active_goal"]["objective"] == "Long-running autonomous task"

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=[],
        queue_storage_info=q_info,
    )
    assert result["has_active_goal"] is True
    assert result["active_goal"]["status"] == "active"
    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value


def test_criterion_8_interrupted_thread_pending_queue_scenario(tmp_path):
    """8. Interrupted thread event is detected and reported in delivery evaluation."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    create_temp_queue_sqlite(tmp_path, thread_id, nonce)
    q_info = inspect_codex_queue_storage(thread_id, codex_home=tmp_path, nonce=nonce)

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {"type": "turn_context", "payload": {"type": "thread_interrupted", "turn_id": "turn-001"}},
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
        queue_storage_info=q_info,
    )
    assert result["thread_interrupted"] is True
    assert result["classification"] == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value


@pytest.mark.asyncio
async def test_criterion_9_duplicate_retry_prohibited_while_queue_item_exists(tmp_path, p2410_env):
    """9. Duplicate queue retry is prohibited while matching item is in queue storage."""
    env = p2410_env
    thread_id = env["config"].codex.thread_id
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    # Create matching queue storage
    create_temp_queue_sqlite(tmp_path, thread_id, nonce)

    adapter = env["queue_adapter"]
    adapter.set_injected_runner(AsyncMock())

    # Adapter-level check
    msg = f"# Canary Ack\nReturn CANARY-OK:{nonce}"
    with pytest.raises(ValueError, match="Duplicate retry prohibited: matching item is already present"):
        await adapter.queue_message(
            thread_id=thread_id,
            message=msg,
            repo_path=env["config"].codex.repo_path,
            codex_home=tmp_path,
            nonce=nonce,
            check_existing_queue=True,
        )

    # API endpoint pre-check with patched queue storage inspector
    client = env["client"]
    token = f"token-g2c-{uuid.uuid4().hex[:16]}"
    tid = f"transfer-g2c-{uuid.uuid4().hex[:12]}"
    rec = TransferRecord(
        transfer_id=tid,
        iteration_id=1,
        direction="chatgpt_to_codex",
        source_type="canary",
        source_identifier=env["config"].chatgpt.conversation_url,
        destination_identifier=thread_id,
        content_hash=compute_sha256(msg),
        message_text=msg,
        payload_preview=msg[:100],
        status=TransferStatus.PREPARED.value,
        provenance_policy=f"canary:{nonce}",
        confirmation_token=token,
    )
    await env["db"].save_transfer_record(rec)

    with patch("app.adapters.inspect_codex_queue_storage", return_value={"has_matching_item": True}):
        res = await client.post("/api/transfer/codex_queue/submit", json={
            "transfer_id": tid,
            "confirmation_token": token,
            "thread_id": thread_id,
            "repo_path": env["config"].codex.repo_path,
        })
        assert res.status_code == 400
        assert "Duplicate retry prohibited" in res.json()["detail"]


def test_criterion_10_client_message_id_correlation(tmp_path):
    """10. Client user message ID is preserved and correlated across receipt parsing and queue storage."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"
    expected_client_id = "01a1798c-931f-74c1-b960-1f0f4065e054"
    expected_queue_id = "01a1798c-9320-7f50-81a5-a87d55a88241"

    # From text CLI output
    cli_out = f"Queued message {expected_client_id} for thread {thread_id}."
    parsed = RealCodexQueueAdapter.parse_queue_cli_output(cli_out, thread_id)
    assert parsed["client_user_message_id"] == expected_client_id

    # From queue storage
    create_temp_queue_sqlite(tmp_path, thread_id, nonce, item_id=expected_queue_id, client_id=expected_client_id)
    q_info = inspect_codex_queue_storage(thread_id, codex_home=tmp_path, nonce=nonce)
    assert q_info["first_matching_item"]["client_id"] == expected_client_id
    assert q_info["first_matching_item"]["id"] == expected_queue_id

    # Through classification
    eval_res = classify_codex_delivery_outcome(thread_id, nonce, [], queue_storage_info=q_info)
    assert eval_res["client_user_message_id"] == expected_client_id
    assert eval_res["receipt_id"] == expected_queue_id


def test_criterion_11_wrong_thread_receipt_rejected():
    """11. Receipt acknowledging a mismatched thread ID is rejected with DeliveryOutcomeUnknownError."""
    expected_thread = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    wrong_thread = "01a09999-different-thread-id"

    # Text format mismatch
    text_out = f"Queued message msg-123 for thread {wrong_thread}."
    with pytest.raises(DeliveryOutcomeUnknownError, match="CLI queue receipt thread mismatch"):
        RealCodexQueueAdapter.parse_queue_cli_output(text_out, expected_thread_id=expected_thread)

    # JSON format mismatch
    json_out = json.dumps({"status": "queued", "id": "q-1", "thread_id": wrong_thread})
    with pytest.raises(DeliveryOutcomeUnknownError, match="CLI queue receipt thread mismatch"):
        RealCodexQueueAdapter.parse_queue_cli_output(json_out, expected_thread_id=expected_thread)


def test_criterion_12_wrong_nonce_rejected():
    """12. Rollout containing different nonce is not matched against expected canary nonce."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    expected_nonce = "d05445c9c98642928640ffbcd08f9d4b"
    wrong_nonce = "11111111111111111111111111111111"

    rollout_lines = [
        {"type": "session_meta", "payload": {"id": thread_id}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": f"# Canary Ack\nReturn CANARY-OK:{wrong_nonce}"}],
                "turn_id": "turn-wrong-001",
            },
        },
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": f"CANARY-OK:{wrong_nonce}"}],
                "turn_id": "turn-wrong-001",
            },
        },
    ]

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=expected_nonce,
        rollout_lines=rollout_lines,
    )

    # Must NOT claim user turn or assistant reply for expected_nonce
    assert result["user_line"] is None
    assert result["assistant_line"] is None
    assert result["canary_ok_verified"] is False
    assert result["classification"] != CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value


def test_criterion_13_queue_receipt_not_equal_to_task_completion(tmp_path):
    """13. Queue receipt accepted does not imply task completion; the three evidence levels are separate."""
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    nonce = "d05445c9c98642928640ffbcd08f9d4b"

    create_temp_queue_sqlite(tmp_path, thread_id, nonce)
    q_info = inspect_codex_queue_storage(thread_id, codex_home=tmp_path, nonce=nonce)

    result = classify_codex_delivery_outcome(
        target_thread_id=thread_id,
        expected_nonce=nonce,
        rollout_lines=[],
        queue_storage_info=q_info,
    )

    # Distinct levels:
    assert result["queue_receipt_status"] == "accepted"
    assert result["turn_dispatch_status"] == "pending"
    assert result["task_completion_status"] == "pending"
    assert result["task_completed"] is False
    assert result["canary_ok_verified"] is False


@pytest.mark.asyncio
async def test_criterion_14_no_real_queue_subprocess_in_automated_tests(p2410_env):
    """14. Strict safety invariant: no real queue subprocess is ever invoked in automated test suites."""
    env = p2410_env
    adapter = env["queue_adapter"]
    runner_called = False

    async def mock_runner(cmd, thread_id, message, repo_path, transfer_id):
        nonlocal runner_called
        runner_called = True
        assert cmd[0] == "codex.exe" or "codex" in cmd[0]
        assert cmd[1] == "queue"
        return f"Queued message client-test for thread {thread_id}."

    adapter.set_injected_runner(mock_runner)

    with patch("subprocess.Popen") as mock_popen, patch("subprocess.run") as mock_subrun:
        res = await adapter.queue_message(
            thread_id=env["config"].codex.thread_id,
            message="# Canary Ack\nReturn CANARY-OK:1234",
            repo_path=env["config"].codex.repo_path,
            check_existing_queue=False,
        )
        assert runner_called is True
        assert res["status"] == "queued"
        # Verify no real OS subprocess was spawned
        mock_popen.assert_not_called()
        mock_subrun.assert_not_called()
