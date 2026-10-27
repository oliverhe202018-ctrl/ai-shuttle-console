import asyncio
import json
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.auto_mode import AutoModeCoordinator, classify_planner_response
from app.codex_app_server import CodexAppServerManager
from app.database import Database
from app.models import (
    AppConfig,
    AutoAuthorizeRequest,
    AutoRunAuthorization,
    AutoRunIteration,
    AutoRunState,
    CodexConfig,
    ChatGPTConfig,
    PlannerClassification,
    RouteConfig,
    TransferDirection,
    TransferStatus,
    compute_sha256,
)
from app.canary import compute_config_fingerprint, auto_canary_prompt, PRODUCTION_THREAD


@pytest.fixture
async def auto_env(tmp_path):
    db = Database(tmp_path / "test_auto.db")
    await db.init()

    cfg = AppConfig(
        canary_mode=True,
        codex=CodexConfig(
            repo_path=str(tmp_path / "canary_ws"),
            expected_branch="master",
            cli_path="codex.exe",
            thread_id="01a174f9-b5d8-7c91-9a27-d8cd3989575b",
            sessions_dir=str(tmp_path / "sessions"),
            session_index_path=str(tmp_path / "index.jsonl"),
            integration_mode="cli_queue",
        ),
        chatgpt=ChatGPTConfig(
            profile_dir=str(tmp_path / ".canary-chatgpt-profile"),
            conversation_url="https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
            conversation_id="6ade6dd6-25bc-83ea-a016-86acbc909581",
            cdp_url="http://127.0.0.1:9223",
        ),
        route=RouteConfig(manual_approval_required=True),
        server_host="127.0.0.1",
        server_port=8765,
    )
    await db.save_config(cfg)

    manager = CodexAppServerManager("codex.exe")
    sender = AsyncMock()
    sender.send_message = AsyncMock(return_value={"status": "sent"})
    queue = AsyncMock()
    queue.submit_message = AsyncMock(return_value={"receipt_id": "rcpt-123", "client_user_message_id": "msg-123"})

    coord = AutoModeCoordinator(
        db=db,
        app_server_manager=manager,
        chatgpt_sender=sender,
        codex_queue=queue,
        get_config_fn=AsyncMock(return_value=cfg),
    )
    return {"db": db, "cfg": cfg, "coord": coord, "manager": manager, "sender": sender, "queue": queue}

@pytest.mark.asyncio
async def test_max_iterations_enforced(auto_env):
    coord = auto_env["coord"]
    # Rejects 0
    with pytest.raises(ValueError, match="max_iterations must be between 1 and 10"):
        await coord.arm_run(max_iterations=0, operator_confirmed=True)
    # Rejects > 10
    with pytest.raises(ValueError, match="max_iterations must be between 1 and 10"):
        await coord.arm_run(max_iterations=11, operator_confirmed=True)
    # Requires operator confirmation
    with pytest.raises(ValueError, match="Operator confirmation is required"):
        await coord.arm_run(max_iterations=3, operator_confirmed=False)

    # Valid: 3
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    assert auth["max_iterations"] == 3
    assert auth["status"] == AutoRunState.AUTO_ARMED.value


@pytest.mark.asyncio
async def test_authorization_ttl_enforced(auto_env):
    coord = auto_env["coord"]
    db = auto_env["db"]
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True, ttl_minutes=1)
    run_id = auth["run_id"]

    # Artificially expire the authorization
    expired_at = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
    await db.save_auto_run_authorization({**auth, "expires_at": expired_at})

    # When run loop starts, it detects TTL expiration immediately
    await coord.start_run(run_id)
    await asyncio.sleep(0.1)

    updated = await db.get_auto_run_authorization(run_id)
    assert updated["status"] == AutoRunState.AUTO_STOPPED.value
    assert updated["stop_reason"] == "AUTO_RUN_AUTHORIZATION_EXPIRED"


@pytest.mark.asyncio
async def test_exact_target_binding_and_production_guard(auto_env):
    coord = auto_env["coord"]
    cfg = auto_env["cfg"]

    # Production thread in canary mode must be rejected
    prod_cfg = cfg.model_copy(deep=True)
    prod_cfg.codex.thread_id = PRODUCTION_THREAD  # PRODUCTION_THREAD
    coord.get_config_fn = AsyncMock(return_value=prod_cfg)

    with pytest.raises(ValueError, match="Production targets are strictly forbidden"):
        await coord.arm_run(max_iterations=3, operator_confirmed=True)


@pytest.mark.asyncio
async def test_config_fingerprint_binding(auto_env):
    coord = auto_env["coord"]
    db = auto_env["db"]
    cfg = auto_env["cfg"]

    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    run_id = auth["run_id"]

    # Alter configuration after arming
    changed_cfg = cfg.model_copy(deep=True)
    changed_cfg.codex.repo_path = "D:\\project\\altered_path"
    coord.get_config_fn = AsyncMock(return_value=changed_cfg)

    await coord.start_run(run_id)
    await asyncio.sleep(0.1)

    updated = await db.get_auto_run_authorization(run_id)
    assert updated["status"] == AutoRunState.AUTO_FAILED.value
    assert updated["stop_reason"] == "CONFIG_FINGERPRINT_MISMATCH"


@pytest.mark.asyncio
async def test_child_authorization_derivation_and_single_use(auto_env):
    coord = auto_env["coord"]
    db = auto_env["db"]
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)

    transfer_id = "test-transfer-001"
    content_hash = "abc123hash"
    child_auth = await coord.derive_child_authorization(
        auth=auth,
        iteration_number=1,
        transfer_id=transfer_id,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        destination_identifier="https://chatgpt.com/c/test",
        content_hash=content_hash,
        config_fingerprint=auth["config_fingerprint"],
    )

    assert child_auth.parent_run_id == auth["run_id"]
    assert child_auth.iteration_number == 1
    assert child_auth.transfer_id == transfer_id
    assert child_auth.consumed is False
    assert child_auth.confirmation_token is not None

    # Verify registered in transfer_authorizations table
    parent_ta = await db.get_transfer_authorization(child_auth.confirmation_token)
    assert parent_ta is not None
    assert parent_ta.expected_content_hash == content_hash

    # Single-use consumption
    ok, err = await db.verify_and_consume_authorization(
        transfer_id=transfer_id,
        token=child_auth.confirmation_token,
        content_hash=content_hash,
        destination="https://chatgpt.com/c/test",
        expected_config_fingerprint=auth["config_fingerprint"]
    )
    assert ok is True
    assert err is None
    consumed_ta = await db.get_transfer_authorization(child_auth.confirmation_token)
    assert consumed_ta.consumed is True

def test_planner_classification_stop_marker():
    # Terminal marker SHUTTLE_STOP requested
    response = {
        "raw_text": "Good work. SHUTTLE_STOP All requested objectives are met.",
        "code_blocks": [],
        "canvas_text": ""
    }
    status, prompt, prov, reason = classify_planner_response(response)
    assert status == PlannerClassification.PLANNER_STOP_REQUESTED
    assert "SHUTTLE_STOP" in reason


def test_planner_classification_no_actionable_prompt():
    response = {
        "raw_text": "I have reviewed your progress. It looks clean and well-structured.",
        "code_blocks": [],
        "canvas_text": ""
    }
    status, prompt, prov, reason = classify_planner_response(response)
    assert status == PlannerClassification.PLANNER_NO_ACTIONABLE_PROMPT


def test_planner_classification_ambiguous_prompts():
    response = {
        "raw_text": "Here are two options to choose from:",
        "code_blocks": [
            "```markdown\n# Option 1\nRun test suite 1\n```",
            "```markdown\n# Option 2\nRun test suite 2\n```"
        ],
        "canvas_text": ""
    }
    status, prompt, prov, reason = classify_planner_response(response)
    assert status == PlannerClassification.PLANNER_RESPONSE_AMBIGUOUS
    assert "Multiple competing" in reason


def test_planner_classification_auto_canary_match():
    run_id = "auto-test1234"
    iteration = 1
    nonce = "a" * 32
    expected = auto_canary_prompt(run_id, iteration, nonce)

    response = {
        "raw_text": f"Here is the prompt:\n```markdown\n{expected}\n```",
        "code_blocks": [f"```markdown\n{expected}\n```"],
        "canvas_text": ""
    }
    status, prompt, prov, reason = classify_planner_response(response, expected_prompt=expected)
    assert status == PlannerClassification.NEXT_CODEX_PROMPT_FOUND
    assert prompt == expected
    assert prov == "auto_canary_marked_block"


@pytest.mark.asyncio
async def test_concurrency_guard_rejects_nested_runs(auto_env):
    coord = auto_env["coord"]
    # Arm first run
    auth1 = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    assert auth1["status"] == AutoRunState.AUTO_ARMED.value

    # Attempting to arm second run while first is armed/running must fail
    with pytest.raises(ValueError, match="already active"):
        await coord.arm_run(max_iterations=3, operator_confirmed=True)


@pytest.mark.asyncio
async def test_pause_and_emergency_stop_semantics(auto_env):
    coord = auto_env["coord"]
    db = auto_env["db"]
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    run_id = auth["run_id"]

    # Request pause
    await coord.request_pause(run_id, reason="Operator paused test")
    assert coord._pause_requested is True

    # Emergency stop immediately sets status to AUTO_STOPPED
    await coord.emergency_stop(run_id, reason="Operator hit emergency stop")
    updated = await db.get_auto_run_authorization(run_id)
    assert updated["status"] == AutoRunState.AUTO_STOPPED.value
    assert updated["stop_reason"] == "Operator hit emergency stop"


@pytest.mark.asyncio
async def test_service_restart_recovery(auto_env):
    coord = auto_env["coord"]
    db = auto_env["db"]
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    run_id = auth["run_id"]

    # Simulate crash while running
    await db.update_auto_run_status(run_id, AutoRunState.AUTO_WAITING_CHATGPT.value)

    # Service restarts and calls check_restart_recovery()
    recovered = await coord.check_restart_recovery()
    assert recovered is not None
    assert recovered["status"] == AutoRunState.AUTO_PAUSED_RECOVERY_REQUIRED.value
    assert "recovery required" in recovered["stop_reason"]


@pytest.mark.asyncio
async def test_human_action_required_pauses_execution(auto_env):
    coord = auto_env["coord"]
    db = auto_env["db"]
    auth = await coord.arm_run(max_iterations=3, operator_confirmed=True)
    run_id = auth["run_id"]

    # Simulate app-server needing human approval
    auto_env["manager"].adapter.pending_human_actions.append({
        "request_id": 101,
        "method": "applyPatchApproval",
        "params": {}
    })

    # When iteration encounters human action, it transitions to AUTO_HUMAN_ACTION_REQUIRED
    with patch.object(coord, "_execute_iteration", AsyncMock(return_value=(False, AutoRunState.AUTO_HUMAN_ACTION_REQUIRED, "HUMAN_ACTION_REQUIRED: applyPatchApproval"))):
        await coord.start_run(run_id)
        await asyncio.sleep(0.05)

    updated = await db.get_auto_run_authorization(run_id)
    assert updated["status"] == AutoRunState.AUTO_HUMAN_ACTION_REQUIRED.value
    assert "applyPatchApproval" in updated["stop_reason"]
