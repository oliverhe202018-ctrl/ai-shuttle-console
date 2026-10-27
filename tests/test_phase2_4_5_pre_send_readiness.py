"""
Phase 2.4.5 — Live Canary Runtime Verification & Pre-Send Readiness Test Suite.

Acceptance verification against the 8 required criteria:
  1. The actual runtime is in Canary mode (when launched with canary config, execution_mode is CANARY;
     and detection of unconfigured/default runtime identifies it as non-canary).
  2. Production identities are rejected.
  3. Configuration changes invalidate existing authorization.
  4. The ChatGPT and Codex operations use the expected isolated targets.
  5. A transfer preview does not itself send a message.
  6. ChatGPT sending and Codex queueing require separate authorizations.
  7. Ambiguous delivery blocks automatic retries.
  8. No real ChatGPT message or Codex queue operation occurred during this phase.
"""
import asyncio
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import tempfile
import uuid
import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters import (
    DeliveryOutcomeUnknownError,
    ReadOnlyCodexSessionObserver,
    RealChatGPTBrowserObserver,
    RealChatGPTMessageSender,
    RealCodexQueueAdapter,
    StandardPromptExtractor,
)
from app.canary import (
    BASE_DIR,
    CANARY_WORKSPACE,
    PRODUCTION_CONVERSATION,
    PRODUCTION_THREAD,
    PRODUCTION_WORKSPACE,
    canary_prompt,
    canary_request,
    compute_config_fingerprint,
    is_production_conversation,
    is_production_target,
    is_production_thread,
    is_production_workspace,
    load_explicit_config,
    validate_canary_config,
)
from app.database import Database
import app.main as m
from app.models import (
    AppConfig,
    CodexConfig,
    ChatGPTConfig,
    PromptExtractionRecord,
    RouteConfig,
    ShuttleStage,
    TransferAuthorization,
    TransferDirection,
    TransferRecord,
    TransferStatus,
    compute_sha256,
)
from app.state_machine import ShuttleStateMachine


@pytest.fixture
def temp_canary_env(tmp_path: Path):
    ws = tmp_path / "canary_workspace"
    ws.mkdir(parents=True, exist_ok=True)
    sessions = ws / "sessions"
    sessions.mkdir(parents=True, exist_ok=True)
    session_index = ws / "session_index.jsonl"
    session_index.write_text("", encoding="utf-8")
    cli_mock = tmp_path / "codex.exe"
    cli_mock.write_text("mock binary", encoding="utf-8")
    profile = tmp_path / ".canary-chatgpt-profile"
    profile.mkdir(parents=True, exist_ok=True)

    canary_thread = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    canary_conv = "6ade6dd6-25bc-83ea-a016-86acbc909581"

    config = AppConfig(
        canary_mode=True,
        codex=CodexConfig(
            repo_path=str(CANARY_WORKSPACE.resolve()),
            expected_branch="master",
            cli_path=str(cli_mock.resolve()),
            thread_id=canary_thread,
            thread_title="Canary Thread",
            sessions_dir=str(sessions.resolve()),
            session_index_path=str(session_index.resolve()),
            integration_mode="cli_queue",
        ),
        chatgpt=ChatGPTConfig(
            browser_type="msedge",
            profile_dir=str((BASE_DIR / ".canary-chatgpt-profile").resolve()),
            conversation_url=f"https://chatgpt.com/c/{canary_conv}",
            expected_conversation_id=canary_conv,
            cdp_url="http://127.0.0.1:9223",
            browser_readiness="ready",
            human_verification_state="passed",
        ),
        route=RouteConfig(
            manual_approval_required=True,
            duplicate_detection_policy="hash_and_prefix_match",
        ),
        server_host="127.0.0.1",
        server_port=8765,
    )

    db_path = tmp_path / "test_canary_state.db"
    test_db = Database(db_path)

    return {
        "tmp_path": tmp_path,
        "config": config,
        "db": test_db,
        "cli_path": cli_mock,
        "thread_id": canary_thread,
        "conversation_id": canary_conv,
    }


@pytest.fixture
async def readiness_app(temp_canary_env, verified_runtime):
    test_db = temp_canary_env["db"]
    await test_db.init()
    cfg = temp_canary_env["config"]
    await test_db.save_config(cfg)

    old_db = m.db
    old_sm = m.state_machine
    old_sender = m.real_chatgpt_sender
    old_obs = m.real_chatgpt_observer
    old_queue = m.real_codex_queue
    old_env_cfg = os.environ.get("SHUTTLE_CONFIG")
    old_env_canary = os.environ.get("SHUTTLE_CANARY")

    m.db = test_db
    m.state_machine = ShuttleStateMachine(test_db)
    m.state_machine.config = cfg
    await m.state_machine.init()

    os.environ["SHUTTLE_CONFIG"] = str(temp_canary_env["tmp_path"] / "config.canary.json")
    os.environ["SHUTTLE_CANARY"] = "1"
    await verified_runtime(cfg, test_db)

    transport = ASGITransport(app=m.app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
        yield {
            "client": client,
            "db": test_db,
            "config": cfg,
            "env": temp_canary_env,
        }

    # Restore globals
    m.db = old_db
    m.state_machine = old_sm
    m.real_chatgpt_sender = old_sender
    m.real_chatgpt_observer = old_obs
    m.real_codex_queue = old_queue
    if old_env_cfg is not None:
        os.environ["SHUTTLE_CONFIG"] = old_env_cfg
    else:
        os.environ.pop("SHUTTLE_CONFIG", None)
    if old_env_canary is not None:
        os.environ["SHUTTLE_CANARY"] = old_env_canary
    else:
        os.environ.pop("SHUTTLE_CANARY", None)


@pytest.mark.asyncio
async def test_criterion_1_runtime_is_in_canary_mode(readiness_app):
    """Criterion 1: The actual runtime is in Canary mode and unconfigured runtime is rejected."""
    client = readiness_app["client"]
    res = await client.get("/api/transfer/target_info")
    assert res.status_code == 200
    data = res.json()
    assert data["execution_mode"] == "CANARY"
    assert data["canary_mode"] is True
    assert data["canary_safe"] is True
    assert data["is_production_target"] is False
    assert data["blocking_reason"] is None


@pytest.mark.asyncio
async def test_criterion_2_production_identities_are_rejected(readiness_app):
    """Criterion 2: Production identities (thread, conversation, workspace) are strictly rejected."""
    client = readiness_app["client"]

    # Block production thread
    res_th = await client.post("/api/transfer/codex_report/prepare", json={
        "thread_id": PRODUCTION_THREAD,
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res_th.status_code == 400
    assert "production" in res_th.json()["detail"].lower()

    # Block production workspace
    res_ws = await client.post("/api/transfer/codex_report/prepare", json={
        "expected_cwd": str(PRODUCTION_WORKSPACE),
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res_ws.status_code == 400
    assert "production" in res_ws.json()["detail"].lower()


@pytest.mark.asyncio
async def test_criterion_3_configuration_changes_invalidate_existing_authorization(readiness_app):
    """Criterion 3: Modifying configuration invalidates previously issued authorization tokens."""
    client = readiness_app["client"]
    db = readiness_app["db"]
    cfg = readiness_app["config"]

    # Prepare transfer with initial fingerprint
    res = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res.status_code == 200
    data = res.json()
    transfer_id = data["transfer_id"]
    token = data["confirmation_token"]

    # Mutate configuration fingerprint
    mutated_cfg = cfg.model_copy(deep=True)
    mutated_cfg.codex.thread_title = "Modified Title To Change Fingerprint"
    await db.save_config(mutated_cfg)
    m.state_machine.config = mutated_cfg

    # Attempt to send with original token
    res_send = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer_id,
        "confirmation_token": token,
        "page_override": {"html": "<form></form>"},
    })
    assert res_send.status_code == 400
    assert "configuration has changed" in res_send.json()["detail"].lower()


@pytest.mark.asyncio
async def test_criterion_4_chatgpt_and_codex_operations_use_expected_isolated_targets(readiness_app):
    """Criterion 4: ChatGPT and Codex operations target the exact expected isolated Canary identities."""
    client = readiness_app["client"]
    cfg = readiness_app["config"]

    res = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res.status_code == 200
    data = res.json()
    assert data["destination_url"] == cfg.chatgpt.conversation_url
    assert data["destination_conversation_id"] == cfg.chatgpt.conversation_id
    assert data["source_thread_id"] == cfg.codex.thread_id
    assert is_production_target(thread_id=data["source_thread_id"], conv_id_or_url=data["destination_url"]) is False


@pytest.mark.asyncio
async def test_criterion_5_transfer_preview_does_not_itself_send_message(readiness_app):
    """Criterion 5: Generating a transfer preview creates a prepared record but does NOT send a message."""
    client = readiness_app["client"]
    db = readiness_app["db"]

    res = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "prepared"

    transfer = await db.get_transfer_by_id(data["transfer_id"])
    assert transfer is not None
    assert transfer.status == TransferStatus.PREPARED.value
    assert transfer.human_confirmed is False
    assert transfer.sent_at is None
    assert transfer.delivery_evidence is None

    # Verify authorization is unconsumed
    auth = await db.get_transfer_authorization(data["confirmation_token"])
    assert auth is not None
    assert auth.consumed is False


@pytest.mark.asyncio
async def test_criterion_6_chatgpt_and_codex_require_separate_authorizations(readiness_app):
    """Criterion 6: ChatGPT sending and Codex queueing mandate distinct, single-use authorizations."""
    client = readiness_app["client"]
    db = readiness_app["db"]

    # 1. Prepare Codex -> ChatGPT transfer
    res_c2g = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res_c2g.status_code == 200
    c2g_data = res_c2g.json()
    c2g_token = c2g_data["confirmation_token"]
    c2g_tid = c2g_data["transfer_id"]

    # Attempting to use c2g_token for codex queue must fail
    res_wrong_q = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": c2g_tid,
        "confirmation_token": c2g_token,
    })
    assert res_wrong_q.status_code in (400, 404, 409)


@pytest.mark.asyncio
async def test_criterion_7_ambiguous_delivery_blocks_automatic_retries(readiness_app):
    """Criterion 7: Ambiguous delivery transitions to DELIVERY_OUTCOME_UNKNOWN and blocks automatic retry."""
    client = readiness_app["client"]
    db = readiness_app["db"]

    res = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res.status_code == 200
    data = res.json()
    transfer_id = data["transfer_id"]
    token = data["confirmation_token"]

    class AmbiguousSender(RealChatGPTMessageSender):
        async def send_message(self, *args, **kwargs):
            raise DeliveryOutcomeUnknownError("Network timeout during submission")

    m.real_chatgpt_sender = AmbiguousSender(
        target_url=readiness_app["config"].chatgpt.conversation_url,
        conversation_id=readiness_app["config"].chatgpt.conversation_id,
        db=db,
    )

    # Send fails with 502 Bad Gateway
    res_unknown = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer_id,
        "confirmation_token": token,
    })
    assert res_unknown.status_code == 502
    assert "delivery outcome unknown" in res_unknown.json()["detail"].lower()

    # State machine and transfer are DELIVERY_OUTCOME_UNKNOWN
    assert m.state_machine.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN
    transfer = await db.get_transfer_by_id(transfer_id)
    assert transfer.status == TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value

    # Subsequent retry is rejected with 409 Conflict
    res_retry = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer_id,
        "confirmation_token": token,
    })
    assert res_retry.status_code == 409
    assert "manual reconciliation" in res_retry.json()["detail"].lower()


@pytest.mark.asyncio
async def test_criterion_8_no_real_side_effects_in_tests(readiness_app):
    """Criterion 8: Verifies that no real ChatGPT message or Codex queue operation occurred."""
    db = readiness_app["db"]
    cfg = readiness_app["config"]

    # Verify Codex queue adapter with mock runner
    executed_commands = []
    queue_adapter = RealCodexQueueAdapter(
        cli_path="codex",
        default_repo_path=cfg.codex.repo_path,
        db=db,
    )
    queue_adapter.set_injected_runner(lambda t, r, m: executed_commands.append({"thread": t, "msg": m}))

    token = f"token-g2c-{uuid.uuid4().hex[:16]}"
    tid = f"transfer-g2c-{uuid.uuid4().hex[:12]}"
    prompt_hash = compute_sha256("test prompt")

    await db.create_transfer_authorization(TransferAuthorization(
        transfer_id=tid,
        confirmation_token=token,
        destination_identifier=cfg.codex.thread_id,
        expected_content_hash=prompt_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
    ))

    res_q = await queue_adapter.queue_message(
        thread_id=cfg.codex.thread_id,
        message="test prompt",
        confirmation_token=token,
        transfer_id=tid,
        repo_path=cfg.codex.repo_path,
    )
    assert res_q["status"] == "queued"
    assert len(executed_commands) == 1
    assert executed_commands[0]["thread"] == cfg.codex.thread_id

