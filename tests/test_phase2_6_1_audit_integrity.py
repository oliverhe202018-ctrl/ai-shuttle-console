import asyncio
import json
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from app.adapters import (
    ReadOnlyCodexSessionObserver,
    MockChatGPTMessageSender,
    MockCodexQueueAdapter,
    RealCodexQueueAdapter,
)
from app.auto_mode import (
    AutoModeCoordinator,
    CanaryIterationPayloadStrategy,
    ProductionShuttlePayloadStrategy,
    IIterationPayloadStrategy,
    classify_planner_response,
)
from app.codex_app_server import CodexAppServerManager
from app.database import Database
from app.models import (
    AppConfig,
    AutoRunIteration,
    AutoRunState,
    CodexConfig,
    ChatGPTConfig,
    PlannerClassification,
    RouteConfig,
    compute_sha256,
)


@pytest.fixture
async def test_db(tmp_path):
    db = Database(tmp_path / "test_integrity.db")
    await db.init()
    cfg = AppConfig(
        canary_mode=True,
        codex=CodexConfig(
            repo_path=str(tmp_path),
            expected_branch="master",
            cli_path="codex.exe",
            thread_id="11111111-2222-3333-4444-555555555555",
            sessions_dir=str(tmp_path / "sessions"),
        ),
        chatgpt=ChatGPTConfig(
            profile_dir=str(tmp_path / "profile"),
            conversation_url="https://chatgpt.com/c/66666666-7777-8888-9999-000000000000",
            conversation_id="66666666-7777-8888-9999-000000000000",
        ),
    )
    await db.save_config(cfg)
    return db


# 1. chatgpt_user_turn_id stores a real observed turn ID
def test_1_chatgpt_user_turn_id_stores_real_observed_id():
    rec = AutoRunIteration(
        run_id="run-1",
        iteration_number=1,
        chatgpt_user_turn_id="turn-user-real-001",
        chatgpt_user_message_id="msg-user-real-001",
    )
    assert rec.chatgpt_user_turn_id == "turn-user-real-001"
    assert rec.chatgpt_user_message_id == "msg-user-real-001"


# 2. Transfer ID cannot populate turn-ID field
def test_2_transfer_id_cannot_populate_turn_id_field():
    for bad_id in ("c2g-run-1-iter1", "g2c-run-1-iter1", "transfer-001"):
        with pytest.raises(ValidationError, match="transfer ID"):
            AutoRunIteration(
                run_id="run-1",
                iteration_number=1,
                chatgpt_user_turn_id=bad_id,
            )


# 3. Queue receipt ID, client ID, and queued item ID remain distinct
def test_3_queue_receipt_client_item_ids_remain_distinct():
    # Duplicate queued_item_id and client_user_message_id must be rejected
    with pytest.raises(ValidationError, match="cannot duplicate queue_client_user_message_id"):
        AutoRunIteration(
            run_id="run-1",
            iteration_number=1,
            queue_client_user_message_id="msg-client-001",
            queued_item_id="msg-client-001",
        )

    # Literal 'message' as queue receipt ID must be rejected
    with pytest.raises(ValidationError, match="generic literal 'message'"):
        AutoRunIteration(
            run_id="run-1",
            iteration_number=1,
            queue_receipt_id="message",
        )

    # Distinct values are valid
    rec = AutoRunIteration(
        run_id="run-1",
        iteration_number=1,
        queue_receipt_id="rcpt-001",
        queue_client_user_message_id="cli-msg-001",
        queued_item_id="item-db-001",
    )
    assert rec.queue_receipt_id != rec.queue_client_user_message_id
    assert rec.queued_item_id != rec.queue_client_user_message_id


# 4. Missing queued item ID is represented as unavailable (None), not substituted
def test_4_missing_queued_item_id_represented_as_none():
    rec = AutoRunIteration(
        run_id="run-1",
        iteration_number=1,
        queue_receipt_id="rcpt-001",
        queue_client_user_message_id="cli-msg-001",
        queued_item_id=None,
    )
    assert rec.queued_item_id is None
    assert rec.queue_client_user_message_id == "cli-msg-001"


# 5. Codex user turn ID comes from actual rollout evidence
def test_5_codex_user_turn_id_from_rollout_evidence(tmp_path):
    session_file = tmp_path / "rollout-test.jsonl"
    lines = [
        json.dumps({"type": "turn_context", "payload": {"turn_id": "turn-cdx-100"}}),
        json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "content": "Hello Codex",
                "client_user_message_id": "cli-msg-100",
                "turn_id": "turn-cdx-100",
                "id": "msg-usr-100"
            }
        }),
    ]
    session_file.write_text("\n".join(lines), encoding="utf-8")

    obs = ReadOnlyCodexSessionObserver(tmp_path)
    obs.locate_session_file = MagicMock(return_value=session_file)

    res = obs.find_dispatched_turn_and_answer(
        thread_id="test-thread",
        after_line=0,
        client_user_message_id="cli-msg-100"
    )
    assert res["dispatched"] is True
    assert res["user_turn_id"] == "turn-cdx-100"


# 6. Codex user message ID comes from actual rollout evidence
def test_6_codex_user_message_id_from_rollout_evidence(tmp_path):
    session_file = tmp_path / "rollout-test.jsonl"
    lines = [
        json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "content": "Test prompt",
                "client_user_message_id": "cli-msg-200",
                "turn_id": "turn-cdx-200",
                "id": "msg-usr-200"
            }
        }),
    ]
    session_file.write_text("\n".join(lines), encoding="utf-8")

    obs = ReadOnlyCodexSessionObserver(tmp_path)
    obs.locate_session_file = MagicMock(return_value=session_file)

    res = obs.find_dispatched_turn_and_answer(
        thread_id="test-thread",
        after_line=0,
        client_user_message_id="cli-msg-200"
    )
    assert res["user_message_id"] == "msg-usr-200"


# 7. Assistant message ID is a real rollout message ID
def test_7_assistant_message_id_is_real_rollout_id(tmp_path):
    session_file = tmp_path / "rollout-test.jsonl"
    lines = [
        json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "content": "User question",
                "turn_id": "turn-cdx-300",
                "id": "msg-usr-300"
            }
        }),
        json.dumps({
            "type": "response_item",
            "timestamp": "2026-10-28T01:00:00Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "phase": "final_answer",
                "turn_id": "turn-cdx-300",
                "id": "msg-asst-300",
                "content": "Assistant answer"
            }
        }),
    ]
    session_file.write_text("\n".join(lines), encoding="utf-8")

    obs = ReadOnlyCodexSessionObserver(tmp_path)
    obs.locate_session_file = MagicMock(return_value=session_file)

    res = obs.find_dispatched_turn_and_answer(
        thread_id="test-thread",
        after_line=0
    )
    assert res["completed"] is True
    assert res["assistant_message_id"] == "msg-asst-300"
    assert res["assistant_text"] == "Assistant answer"


# 8. Hashes never masquerade as IDs
def test_8_hashes_never_masquerade_as_ids():
    output_hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    # Prefix hash as message ID rejected
    with pytest.raises(ValidationError, match="cannot be populated from a content hash"):
        AutoRunIteration(
            run_id="run-1",
            iteration_number=1,
            codex_output_hash=output_hash,
            codex_assistant_message_id=output_hash[:16],
        )

    # Full hash as message ID rejected
    with pytest.raises(ValidationError, match="cannot be populated from a content hash"):
        AutoRunIteration(
            run_id="run-1",
            iteration_number=1,
            codex_output_hash=output_hash,
            codex_assistant_message_id=output_hash,
        )


# 9. Stale thread-level completed status cannot complete a new iteration
@pytest.mark.asyncio
async def test_9_stale_thread_level_completed_status_cannot_complete_new_iteration(tmp_path, test_db):
    cfg = AppConfig(
        canary_mode=False,
        codex=CodexConfig(
            repo_path=str(tmp_path),
            expected_branch="master",
            cli_path="codex.exe",
            thread_id="11111111-2222-3333-4444-555555555555",
            sessions_dir=str(tmp_path / "sessions"),
        ),
        chatgpt=ChatGPTConfig(
            profile_dir=str(tmp_path / "profile"),
            conversation_url="https://chatgpt.com/c/66666666-7777-8888-9999-000000000000",
            conversation_id="66666666-7777-8888-9999-000000000000",
        ),
    )
    await test_db.save_config(cfg)

    manager = CodexAppServerManager("codex.exe")
    manager.is_thread_cold = AsyncMock(return_value=(False, "loaded"))
    sender = AsyncMock()
    sender.send_message = AsyncMock(return_value={
        "status": "delivered",
        "delivery_evidence": {"user_turn_id": "turn-u-1", "user_message_id": "msg-u-1"}
    })
    queue = AsyncMock()
    queue.submit_message = AsyncMock(return_value={"receipt_id": "rcpt-1", "client_user_message_id": "cli-1"})

    # Observer reports task_completion_status == "completed" at thread level, but NO turn-level match
    observer = MagicMock()
    del observer.find_dispatched_turn_and_answer
    observer.observe_thread = MagicMock(return_value={
        "total_lines": 10,
        "task_completion_status": "completed", # Stale completed flag from older turn!
        "latest_assistant_text": "Older completion",
        "completed_turn_id": "turn-older-999", # Does NOT match current turn!
        "turns": [{"client_user_message_id": "cli-1", "turn_id": "turn-current-001", "message_id": "msg-current-001"}],
    })

    coord = AutoModeCoordinator(
        db=test_db,
        app_server_manager=manager,
        chatgpt_sender=sender,
        codex_queue=queue,
        get_config_fn=AsyncMock(return_value=cfg),
        observer_fn=lambda: observer,
        payload_strategy=ProductionShuttlePayloadStrategy(),
    )

    # Production arming is forbidden in Phase 2.7. Exercise the existing
    # correlation unit with synthetic local state, without issuing a live run.
    with pytest.raises(ValueError, match="PRODUCTION_AUTO_MODE_NOT_AUTHORIZED"):
        await coord.arm_run(max_iterations=1, operator_confirmed=True)
    from app.canary import compute_config_fingerprint
    auth = {"run_id": "synthetic-correlation-unit", "max_iterations": 1,
            "canary_mode": False, "config_fingerprint": compute_config_fingerprint(cfg)}

    long_prompt = "Perform production audit:\n" + ("- Task step detail for testing\n" * 10)
    # Patch browser observer to return completed planner response
    with patch("app.adapters.RealChatGPTBrowserObserver.read_live_page", AsyncMock(return_value={})), \
         patch("app.adapters.RealChatGPTBrowserObserver.inspect_dom_payload", MagicMock(return_value={
             "is_completed": True,
             "is_streaming": False,
             "latest_user_turn": {"turn_index": 1, "turn_id": "turn-u-1"},
             "latest_assistant_turn": {
                 "turn_index": 2,
                 "turn_id": "turn-a-1",
                 "text": f"Next prompt:\n```markdown\n{long_prompt}\n```",
                 "code_blocks": [f"```markdown\n{long_prompt}\n```"]
             }
         })):
        # Run iteration - with tight timeout on completion polling
        def adv():
            val = [0.0]
            def _f():
                val[0] += 20.0
                return val[0]
            return _f
        with patch("time.perf_counter", side_effect=adv()):
            ok, state, reason = await coord._execute_iteration(auth, iteration=1)
            # Must NOT succeed with completed status!
            assert ok is False
            assert "CODEX_COMPLETION_TIMED_OUT" in reason or "FAILED" in (state.value if state else "")


# 10. Unrelated newer/older Codex completion cannot be attributed to current iteration
def test_10_unrelated_codex_completion_not_attributed(tmp_path):
    session_file = tmp_path / "rollout-unrelated.jsonl"
    lines = [
        # Line 0: older turn & answer
        json.dumps({"type": "turn_context", "payload": {"turn_id": "turn-old"}}),
        json.dumps({"type": "response_item", "payload": {"type": "message", "role": "assistant", "turn_id": "turn-old", "id": "msg-old", "content": "Old answer"}}),
        # Line 2: baseline line count was 2. Now new user turn at line 2:
        json.dumps({"type": "turn_context", "payload": {"turn_id": "turn-new"}}),
        json.dumps({"type": "event_msg", "payload": {"type": "user_message", "turn_id": "turn-new", "client_user_message_id": "cli-new", "content": "New question"}}),
        # But NO assistant answer for turn-new yet!
    ]
    session_file.write_text("\n".join(lines), encoding="utf-8")

    obs = ReadOnlyCodexSessionObserver(tmp_path)
    obs.locate_session_file = MagicMock(return_value=session_file)

    res = obs.find_dispatched_turn_and_answer(
        thread_id="test-thread",
        after_line=2,
        client_user_message_id="cli-new"
    )
    assert res["dispatched"] is True
    assert res["user_turn_id"] == "turn-new"
    assert res["completed"] is False
    assert res["assistant_message_id"] is None


# 11. Restart recovery enters AUTO_PAUSED_RECOVERY_REQUIRED
@pytest.mark.asyncio
async def test_11_restart_recovery_enters_auto_paused_recovery_required(tmp_path, test_db):
    coord = AutoModeCoordinator(
        db=test_db,
        app_server_manager=CodexAppServerManager("codex.exe"),
    )
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    run_id = auth["run_id"]

    for active_state in (AutoRunState.AUTO_RUNNING, AutoRunState.AUTO_WAITING_CODEX, AutoRunState.AUTO_WAITING_CHATGPT):
        await test_db.update_auto_run_status(run_id, active_state.value)
        recovered = await coord.check_restart_recovery()
        assert recovered is not None
        assert recovered["status"] == AutoRunState.AUTO_PAUSED_RECOVERY_REQUIRED.value
        assert "recovery required" in recovered["stop_reason"]


# 12. No side effect is automatically replayed after restart
@pytest.mark.asyncio
async def test_12_no_side_effect_replayed_after_restart(tmp_path, test_db):
    sender = AsyncMock()
    queue = AsyncMock()
    coord = AutoModeCoordinator(
        db=test_db,
        app_server_manager=CodexAppServerManager("codex.exe"),
        chatgpt_sender=sender,
        codex_queue=queue,
    )
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    run_id = auth["run_id"]
    await test_db.update_auto_run_status(run_id, AutoRunState.AUTO_WAITING_CODEX.value)

    # Perform restart check
    recovered = await coord.check_restart_recovery()
    assert recovered["status"] == AutoRunState.AUTO_PAUSED_RECOVERY_REQUIRED.value

    # Verify NO side effects occurred
    sender.send_message.assert_not_called()
    queue.submit_message.assert_not_called()
    queue.queue_message.assert_not_called()


# 13. Canary and production payload strategies are separated
def test_13_canary_and_production_payload_strategies_separated():
    canary_strat = CanaryIterationPayloadStrategy()
    prod_strat = ProductionShuttlePayloadStrategy()

    assert isinstance(canary_strat, IIterationPayloadStrategy)
    assert isinstance(prod_strat, IIterationPayloadStrategy)
    assert type(canary_strat) is not type(prod_strat)

    nonce = secrets.token_hex(16)
    canary_payload = canary_strat.build_c2g_payload(
        run_id="run-1", iteration=1, nonce=nonce, max_iterations=3, prior_codex_output="Output 1"
    )
    assert "canary auto-shuttle iteration" in canary_payload
    assert nonce in canary_payload

    prod_payload = prod_strat.build_c2g_payload(
        run_id="run-1", iteration=1, nonce=nonce, max_iterations=3, prior_codex_output="Exact Codex Work"
    )
    assert prod_payload == "Exact Codex Work"
    assert "CANARY" not in prod_payload


# 14. Production strategy does not generate Canary instructions
def test_14_production_strategy_does_not_generate_canary_instructions():
    prod_strat = ProductionShuttlePayloadStrategy()
    nonce = secrets.token_hex(16)
    assert prod_strat.get_expected_codex_prompt("run-1", 1, nonce) is None
    assert prod_strat.get_expected_codex_output("run-1", 1, nonce) is None
    payload = prod_strat.build_c2g_payload("run-1", 1, nonce, 3, "Production Output")
    assert "CANARY" not in payload
    assert "nonce" not in payload.lower()


# 15. Planner/non-planner boundary remains intact
def test_15_planner_non_planner_boundary_intact():
    prompt_text = "Fix security audit issues in evidence persistence\n" + ("- Ensure turn IDs are preserved exactly\n" * 8)
    code_block = f"```markdown\n{prompt_text}\n```"
    planner_output = {
        "raw_text": f"Plan approved. Next step:\n{code_block}",
        "code_blocks": [code_block],
        "canvas_text": "",
    }
    classification, extracted, prov, reason = classify_planner_response(planner_output)
    assert classification == PlannerClassification.NEXT_CODEX_PROMPT_FOUND
    # Shuttle extracts the exact planner prompt without rewriting or synthesizing
    assert extracted.strip() == code_block.strip()
    assert prov == "code_block"
