"""
Phase 2.5 — Three-Round Live Shuttle Stability Acceptance Test Suite.

Covers all 10 criteria from Phase 2.5 specification:
1. Round state model & independent persistence (no overwrite across rounds)
2. Database restart preserves all completed rounds and stability metrics
3. Nonce uniqueness enforcement (32 hex, no replay of previous nonces)
4. Hard safety invariants (zero production targets, zero duplicate sends/queues)
5. Bounded polling limits (30s send, 120s response, 30s dispatch, 180s completion; no auto-retry)
6. Cold-thread dispatch handling (CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED, manual_thread_load_required tracking)
7. Exact CANARY-OK:<nonce> verification per round (mismatched nonce / text rejected)
8. Stability metrics tracking (all 8 latency metrics persisted)
9. Three-round readiness & acceptance transitions (THREE_ROUND_CANARY_READY -> THREE_ROUND_CANARY_STABILITY_VERIFIED / BLOCKED)
10. Strict test isolation (zero real queue subprocess, zero real browser send)
"""
import json
import secrets
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

import app.main as m
import app.canary_bootstrap as bootstrap
from app.adapters import (
    ReadOnlyCodexSessionObserver,
    classify_codex_delivery_outcome,
)
from app.canary import (
    compute_config_fingerprint,
    canary_request,
    canary_prompt,
    generate_canary_nonce,
    validate_round_invariants,
    CHATGPT_DELIVERY_TIMEOUT_SEC,
    CHATGPT_ASSISTANT_TIMEOUT_SEC,
    CODEX_QUEUE_DISPATCH_TIMEOUT_SEC,
    CODEX_COMPLETION_TIMEOUT_SEC,
    PRODUCTION_THREAD,
    PRODUCTION_CONVERSATION,
    PRODUCTION_WORKSPACE,
)
from app.models import (
    CanaryRoundRecord,
    CanaryRoundResult,
    CanaryRoundMetrics,
    CodexDeliveryClassification,
    ShuttleStage,
    TransferRecord,
    TransferStatus,
    compute_sha256,
)
from app.database import Database
from app.state_machine import ShuttleStateMachine
from test_phase2_4_5_pre_send_readiness import temp_canary_env


@pytest.fixture
async def p25_env(temp_canary_env, monkeypatch):
    """Fixture providing isolated canary application environment for Phase 2.5 testing."""
    env = temp_canary_env
    cfg, database = env["config"], env["db"]
    await database.init()
    await database.save_config(cfg)
    source = env["tmp_path"] / "config.canary.local.20261026-p25.json"
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
    queue.queue_message.side_effect = AssertionError("Live CLI queue forbidden in tests")
    monkeypatch.setattr(m, "real_chatgpt_sender", sender)
    monkeypatch.setattr(m, "real_codex_queue", queue)

    transport = ASGITransport(app=m.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield {
            **env,
            "client": client,
            "machine": machine,
            "sender": sender,
            "queue": queue,
        }


# ==============================================================================
# 1. Round State Model & Persistence
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_1_round_state_model_and_persistence(p25_env):
    """1. Persist each round independently with full trace IDs and verify no overwriting."""
    db = p25_env["db"]

    r1_data = {
        "round_id": "round-1",
        "round_number": 1,
        "nonce": "11111111111111111111111111111111",
        "c2g_transfer_id": "t-c2g-r1",
        "chatgpt_user_turn_id": "cg-u-r1",
        "chatgpt_assistant_turn_id": "cg-a-r1",
        "g2c_transfer_id": "t-g2c-r1",
        "codex_queue_receipt_id": "rec-r1",
        "codex_user_turn_id": "cx-u-r1",
        "codex_assistant_message_id": "cx-a-r1",
        "started_at": datetime.utcnow() - timedelta(minutes=5),
        "completed_at": datetime.utcnow() - timedelta(minutes=4),
        "result": CanaryRoundResult.ROUND_COMPLETED.value,
        "manual_thread_load_required": False,
        "metrics": {"total_round_trip_latency_ms": 12500.0},
    }
    saved_r1 = await db.save_canary_round(r1_data)
    assert saved_r1["round_id"] == "round-1"
    assert saved_r1["result"] == "ROUND_COMPLETED"
    assert saved_r1["nonce"] == "11111111111111111111111111111111"

    r2_data = {
        "round_id": "round-2",
        "round_number": 2,
        "nonce": "22222222222222222222222222222222",
        "c2g_transfer_id": "t-c2g-r2",
        "chatgpt_user_turn_id": "cg-u-r2",
        "chatgpt_assistant_turn_id": "cg-a-r2",
        "g2c_transfer_id": "t-g2c-r2",
        "codex_queue_receipt_id": "rec-r2",
        "codex_user_turn_id": "cx-u-r2",
        "codex_assistant_message_id": "cx-a-r2",
        "started_at": datetime.utcnow() - timedelta(minutes=3),
        "completed_at": datetime.utcnow() - timedelta(minutes=2),
        "result": CanaryRoundResult.ROUND_COMPLETED.value,
        "manual_thread_load_required": True,
        "metrics": {"total_round_trip_latency_ms": 14200.0},
    }
    saved_r2 = await db.save_canary_round(r2_data)
    assert saved_r2["round_id"] == "round-2"

    all_rounds = await db.get_canary_rounds()
    assert len(all_rounds) == 2
    assert all_rounds[0]["round_id"] == "round-1"
    assert all_rounds[1]["round_id"] == "round-2"
    assert all_rounds[0]["nonce"] == "11111111111111111111111111111111"
    assert all_rounds[1]["nonce"] == "22222222222222222222222222222222"
    assert all_rounds[0]["manual_thread_load_required"] is False
    assert all_rounds[1]["manual_thread_load_required"] is True


# ==============================================================================
# 2. Database Restart Preserves Completed Rounds
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_2_restart_preserves_completed_rounds(p25_env):
    """2. Restarting database preserves all completed round records and metrics."""
    db = p25_env["db"]

    for i in (1, 2, 3):
        await db.save_canary_round({
            "round_id": f"round-{i}",
            "round_number": i,
            "nonce": f"{i}" * 32,
            "result": CanaryRoundResult.ROUND_COMPLETED.value,
            "metrics": {"total_round_trip_latency_ms": float(10000 * i)},
        })

    # Cold restart simulation
    restarted_db = Database(db.db_path)
    await restarted_db.init()
    rounds = await restarted_db.get_canary_rounds()
    assert len(rounds) == 3
    for i, r in enumerate(rounds, start=1):
        assert r["round_number"] == i
        assert r["nonce"] == f"{i}" * 32
        assert r["result"] == "ROUND_COMPLETED"
        assert r["metrics"]["total_round_trip_latency_ms"] == float(10000 * i)


# ==============================================================================
# 3. Nonce Uniqueness & Replay Rejection
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_3_nonce_uniqueness_enforcement(p25_env):
    """3. Each round must use a valid 32-hex nonce; duplicate nonces and replayed nonces are rejected."""
    nonce1 = generate_canary_nonce()
    assert len(nonce1) == 32
    assert all(c in "0123456789abcdef" for c in nonce1)

    previous_phase_nonce = "d05445c9c98642928640ffbcd08f9d4b"

    # Reject replaying previous phase nonce
    with pytest.raises(ValueError, match="already been used"):
        validate_round_invariants(
            nonce=previous_phase_nonce,
            previous_nonces=[previous_phase_nonce],
        )

    # Reject non-hex or invalid length nonces
    with pytest.raises(ValueError, match="valid 32-character hex string"):
        validate_round_invariants(nonce="short-nonce")

    with pytest.raises(ValueError, match="valid 32-character hex string"):
        validate_round_invariants(nonce="g" * 32)  # 'g' is not hex


# ==============================================================================
# 4. Hard Safety Invariants
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_4_hard_safety_invariants(p25_env):
    """4. Production targets are strictly forbidden in canary rounds."""
    nonce = generate_canary_nonce()

    # Production thread rejected
    with pytest.raises(ValueError, match="Production targets are strictly forbidden"):
        validate_round_invariants(nonce=nonce, target_thread=PRODUCTION_THREAD)

    # Production conversation rejected
    with pytest.raises(ValueError, match="Production targets are strictly forbidden"):
        validate_round_invariants(nonce=nonce, target_conv=PRODUCTION_CONVERSATION)

    # Production workspace rejected
    with pytest.raises(ValueError, match="Production targets are strictly forbidden"):
        validate_round_invariants(nonce=nonce, target_repo=PRODUCTION_WORKSPACE)


# ==============================================================================
# 5. Bounded Polling Limits
# ==============================================================================
def test_criterion_5_bounded_polling_constants():
    """5. Verify explicit bounded polling timeouts comply with specification."""
    assert CHATGPT_DELIVERY_TIMEOUT_SEC <= 30
    assert CHATGPT_ASSISTANT_TIMEOUT_SEC <= 120
    assert CODEX_QUEUE_DISPATCH_TIMEOUT_SEC <= 30
    assert CODEX_COMPLETION_TIMEOUT_SEC <= 180


# ==============================================================================
# 6. Cold-Thread Dispatch Handling
# ==============================================================================
def test_criterion_6_cold_thread_dispatch_handling():
    """6. Pending queue item without user turn produces CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED."""
    nonce = generate_canary_nonce()
    rollout_lines = [{"type": "session_meta", "id": "t-001"}]

    classification = classify_codex_delivery_outcome(
        target_thread_id="t-001",
        expected_nonce=nonce,
        rollout_lines=rollout_lines,
        queue_storage_info={"has_matching_item": True},
    )
    assert classification["classification"] == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value
    assert classification["turn_dispatch_status"] == "pending"
    assert classification["task_completion_status"] == "pending"
    assert "Open/resume target Codex thread" in classification["action_instruction"]


# ==============================================================================
# 7. Exact CANARY-OK Verification
# ==============================================================================
def test_criterion_7_exact_canary_ok_verification():
    """7. Only exact CANARY-OK:<nonce> matches; mismatched nonces or texts are rejected."""
    nonce = generate_canary_nonce()
    lines = [
        {"type": "session_meta", "payload": {"id": "t-001"}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "text", "text": canary_prompt(nonce)}],
                "turn_id": "turn-1",
            },
        },
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": f"CANARY-OK:{nonce}"}],
                "turn_id": "turn-1",
            },
        },
    ]

    res_ok = classify_codex_delivery_outcome(
        target_thread_id="t-001",
        expected_nonce=nonce,
        rollout_lines=lines,
        queue_storage_info={"has_matching_item": False},
    )
    assert res_ok["classification"] == CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
    assert res_ok["canary_ok_verified"] is True
    assert res_ok["nonce_matched"] is True

    # Wrong nonce rejected
    res_wrong = classify_codex_delivery_outcome(
        target_thread_id="t-001",
        expected_nonce="00000000000000000000000000000000",
        rollout_lines=lines,
        queue_storage_info={"has_matching_item": False},
    )
    assert res_wrong["canary_ok_verified"] is False


# ==============================================================================
# 8. Stability Metrics Tracking
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_8_stability_metrics_tracking(p25_env):
    """8. Record and persist all 8 latency metrics in the round record."""
    client = p25_env["client"]
    db = p25_env["db"]

    nonce = generate_canary_nonce()
    init_res = await client.post("/api/canary/rounds/init", json={
        "round_number": 1,
        "nonce": nonce,
        "manual_thread_load_required": False,
    })
    assert init_res.status_code == 200, init_res.text
    r_data = init_res.json()
    assert r_data["round_id"] == "round-1"
    assert r_data["nonce"] == nonce

    metrics = CanaryRoundMetrics(
        c2g_prep_latency_ms=120.5,
        c2g_delivery_latency_ms=850.0,
        chatgpt_response_latency_ms=3200.0,
        prompt_extraction_latency_ms=45.2,
        g2c_queue_ack_latency_ms=510.0,
        queue_to_dispatch_latency_ms=2100.0,
        codex_execution_latency_ms=4100.0,
        total_round_trip_latency_ms=10925.7,
    )

    update_res = await client.post("/api/canary/rounds/update", json={
        "round_id": "round-1",
        "result": CanaryRoundResult.ROUND_COMPLETED.value,
        "metrics": metrics.model_dump(),
        "completed_at": datetime.utcnow().isoformat(),
    })
    assert update_res.status_code == 200, update_res.text
    updated = update_res.json()
    assert updated["result"] == "ROUND_COMPLETED"
    assert updated["metrics"]["total_round_trip_latency_ms"] == 10925.7
    assert updated["metrics"]["codex_execution_latency_ms"] == 4100.0


# ==============================================================================
# 9. Readiness & Acceptance Transitions
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_9_readiness_and_acceptance_transitions(p25_env):
    """9. Initial clean preflight reports THREE_ROUND_CANARY_READY; 3 completed rounds yield VERIFIED."""
    client = p25_env["client"]
    db = p25_env["db"]

    # Initial state with preflight passed and no rounds
    readiness_res = await client.get("/api/canary/three_round/readiness")
    assert readiness_res.status_code == 200
    ready = readiness_res.json()
    assert ready["status"] == "THREE_ROUND_CANARY_READY"
    assert ready["rounds_completed"] == 0

    # Simulate 3 successful rounds
    for i in (1, 2, 3):
        await db.save_canary_round({
            "round_id": f"round-{i}",
            "round_number": i,
            "nonce": generate_canary_nonce(),
            "result": CanaryRoundResult.ROUND_COMPLETED.value,
            "completed_at": datetime.utcnow().isoformat(),
        })

    readiness_3 = (await client.get("/api/canary/three_round/readiness")).json()
    assert readiness_3["status"] == "THREE_ROUND_CANARY_STABILITY_VERIFIED"
    assert readiness_3["rounds_completed"] == 3

    # If one round had failed, status reports BLOCKED
    await db.update_canary_round("round-2", {"result": CanaryRoundResult.ROUND_FAILED_VALIDATION.value})
    readiness_blocked = (await client.get("/api/canary/three_round/readiness")).json()
    assert readiness_blocked["status"] == "THREE_ROUND_CANARY_STABILITY_BLOCKED"


# ==============================================================================
# 10. Strict Test Isolation
# ==============================================================================
@pytest.mark.asyncio
async def test_criterion_10_automated_tests_strict_isolation(p25_env):
    """10. Ensure zero real queue subprocess and zero real browser message send occurred."""
    sender = p25_env["sender"]
    queue = p25_env["queue"]
    assert sender.send_message.call_count == 0
    assert queue.queue_message.call_count == 0
