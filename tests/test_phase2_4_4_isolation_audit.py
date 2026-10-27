"""
Phase 2.4.4 — Canary Runtime Configuration Isolation Audit Test Suite.

Verifies the 12 explicit audit criteria to guarantee fail-closed runtime isolation
between Canary transfers and protected production resources:
  1. Canary preflight succeeds, but transfer endpoint attempts to load production config (fails closed).
  2. Canary send endpoint cannot target production conversation.
  3. Canary queue endpoint cannot target production thread or workspace.
  4. Configuration changes after authorization invalidate the authorization.
  5. Authorization token destination differs from transfer destination (fails closed).
  6. Missing or invalid Canary config fails closed.
  7. Existing config is never overwritten.
  8. Two separate authorizations remain mandatory.
  9. Stale/mismatched UI selection cannot retarget a transfer.
  10. Ambiguous delivery cannot trigger automatic retries (DELIVERY_OUTCOME_UNKNOWN).
  11. Production-mode configuration remains available for explicitly configured production operations without being silently selected by Canary actions.
  12. No real browser send or real Codex queue process executed in automated tests.
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
from app.canary_bootstrap import generate_canary_config
from app.database import Database
from app.main import app
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
def temp_workspace(tmp_path: Path):
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
    return {
        "root": tmp_path,
        "workspace": ws,
        "sessions": sessions,
        "session_index": session_index,
        "cli_path": cli_mock,
        "profile": profile,
    }


def make_production_config() -> AppConfig:
    return AppConfig(
        canary_mode=False,
        codex=CodexConfig(
            repo_path=str(PRODUCTION_WORKSPACE),
            expected_branch="synthetic-production-branch",
            cli_path="C:/OpenAI/Codex/bin/codex.exe",
            thread_id=PRODUCTION_THREAD,
            thread_title="Synthetic production thread",
            sessions_dir="C:/Users/test/.codex/sessions",
            integration_mode="cli_queue",
        ),
        chatgpt=ChatGPTConfig(
            browser_type="msedge",
            profile_dir="D:/workspace/production-repo/.supervisor/chatgpt-profile",
            conversation_url=f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}",
            expected_conversation_id=PRODUCTION_CONVERSATION,
            cdp_url="http://127.0.0.1:9222",
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


def make_canary_config(workspace_info: dict) -> AppConfig:
    canary_thread = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    canary_conv = "6ade6dd6-25bc-83ea-a016-86acbc909581"
    return AppConfig(
        canary_mode=True,
        codex=CodexConfig(
            repo_path=str(CANARY_WORKSPACE.resolve()),
            expected_branch="master",
            cli_path=str(workspace_info["cli_path"].resolve()),
            thread_id=canary_thread,
            thread_title="Canary Thread",
            sessions_dir=str(workspace_info["sessions"]),
            session_index_path=str(workspace_info["session_index"]),
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


@pytest.fixture
async def audit_env(tmp_path: Path, temp_workspace: dict, verified_runtime):
    db_file = tmp_path / "audit_state.db"
    test_db = Database(db_file)
    await test_db.init()

    # Save original globals
    old_db = m.db
    old_sm = m.state_machine
    old_sender = m.real_chatgpt_sender
    old_obs = m.real_chatgpt_observer
    old_queue = m.real_codex_queue
    old_env_cfg = os.environ.get("SHUTTLE_CONFIG")
    old_env_canary = os.environ.get("SHUTTLE_CANARY")

    canary_cfg = make_canary_config(temp_workspace)
    m.db = test_db
    m.state_machine = ShuttleStateMachine(test_db)
    await m.state_machine.init()

    m.codex_observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_workspace["sessions"],
        session_index_path=temp_workspace["session_index"],
    )
    m.real_chatgpt_sender = RealChatGPTMessageSender(
        target_url=canary_cfg.chatgpt.conversation_url,
        conversation_id=canary_cfg.chatgpt.conversation_id,
        gpt_id=canary_cfg.chatgpt.gpt_id,
        db=test_db,
    )
    m.real_chatgpt_observer = RealChatGPTBrowserObserver(
        target_url=canary_cfg.chatgpt.conversation_url,
        conversation_id=canary_cfg.chatgpt.conversation_id,
        gpt_id=canary_cfg.chatgpt.gpt_id,
        prompt_extractor=m.prompt_extractor,
    )
    m.real_codex_queue = RealCodexQueueAdapter(
        cli_path=str(temp_workspace["cli_path"]),
        default_repo_path=canary_cfg.codex.repo_path,
        db=test_db,
    )
    await verified_runtime(canary_cfg, test_db)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
        yield {
            "client": client,
            "db": test_db,
            "sm": m.state_machine,
            "temp": temp_workspace,
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

# ==============================================================================
# Audit Criteria 1 to 12
# ==============================================================================

@pytest.mark.asyncio
async def test_1_canary_preflight_succeeds_but_transfer_endpoint_blocks_production_config(audit_env):
    """
    Criterion 1: Canary preflight succeeds, but transfer endpoint attempts to load
    the production config (fails closed).
    """
    client = audit_env["client"]
    db = audit_env["db"]
    prod_cfg = make_production_config()
    await db.save_config(prod_cfg)
    m.state_machine.config = prod_cfg

    # Check target_info detects production mode & safety blocker
    res_info = await client.get("/api/transfer/target_info")
    assert res_info.status_code == 200
    info_data = res_info.json()
    assert info_data["execution_mode"] == "PRODUCTION"
    assert info_data["canary_mode"] is False
    assert info_data["canary_safe"] is False
    assert "PRODUCTION mode" in info_data["blocking_reason"]

    # 1. Prepare Canary report while running production config fails closed
    res_prep = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res_prep.status_code == 400
    assert "production configuration" in res_prep.json()["detail"].lower()

    # 2. Canary send under production config fails closed
    fake_token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    canary_transfer = TransferRecord(
        transfer_id=f"transfer-c2g-{uuid.uuid4().hex[:12]}",
        iteration_id=1,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        source_type="canary",
        source_identifier="01a174f9-b5d8-7c91-9a27-d8cd3989575b",
        destination_type="chatgpt",
        destination_identifier="https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
        content_hash="dummyhash",
        char_count=10,
        message_text="dummy message",
        status=TransferStatus.PREPARED.value,
        confirmation_token=fake_token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=compute_config_fingerprint(prod_cfg),
    )
    await db.save_transfer_record(canary_transfer)
    res_send = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": canary_transfer.transfer_id,
        "confirmation_token": fake_token,
    })
    assert res_send.status_code == 400
    assert "production configuration" in res_send.json()["detail"].lower()

    # 3. Canary queue under production config fails closed
    canary_queue_transfer = TransferRecord(
        transfer_id=f"transfer-g2c-{uuid.uuid4().hex[:12]}",
        iteration_id=1,
        direction=TransferDirection.CHATGPT_TO_CODEX.value,
        source_type="canary",
        source_identifier="https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
        destination_type="codex",
        destination_identifier="01a174f9-b5d8-7c91-9a27-d8cd3989575b",
        content_hash="dummyhash",
        char_count=10,
        message_text="dummy message",
        status=TransferStatus.PREPARED.value,
        confirmation_token=fake_token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=compute_config_fingerprint(prod_cfg),
    )
    await db.save_transfer_record(canary_queue_transfer)
    res_queue = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": canary_queue_transfer.transfer_id,
        "confirmation_token": fake_token,
        "thread_id": "01a174f9-b5d8-7c91-9a27-d8cd3989575b",
    })
    assert res_queue.status_code == 400
    assert "production configuration" in res_queue.json()["detail"].lower()


@pytest.mark.asyncio
async def test_2_canary_send_endpoint_cannot_target_production_conversation(audit_env):
    """
    Criterion 2: Canary send endpoint cannot target the production conversation.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])
    await db.save_config(canary_cfg)
    m.state_machine.config = canary_cfg

    fp = compute_config_fingerprint(canary_cfg)
    fake_token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    transfer = TransferRecord(
        transfer_id=f"transfer-c2g-{uuid.uuid4().hex[:12]}",
        iteration_id=1,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        source_type="canary",
        source_identifier=canary_cfg.codex.thread_id,
        destination_type="chatgpt",
        # Explicitly setting production conversation
        destination_identifier=f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}",
        content_hash="testhash",
        char_count=10,
        message_text="test",
        status=TransferStatus.PREPARED.value,
        confirmation_token=fake_token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=fp,
    )
    await db.save_transfer_record(transfer)

    # API send endpoint rejects production conversation
    res = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer.transfer_id,
        "confirmation_token": fake_token,
    })
    assert res.status_code == 400
    assert "production chatgpt targets are forbidden in canary mode" in res.json()["detail"].lower()

    # Direct RealChatGPTMessageSender check
    sender = RealChatGPTMessageSender(
        target_url=f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}",
        conversation_id=PRODUCTION_CONVERSATION,
        db=db,
    )
    with pytest.raises(ValueError, match="cannot be targeted in canary mode"):
        await sender.send_message(
            message="canary payload",
            confirmation_token=fake_token,
            transfer_id=transfer.transfer_id,
            is_canary=True,
        )


@pytest.mark.asyncio
async def test_3_canary_queue_endpoint_cannot_target_production_thread_or_workspace(audit_env):
    """
    Criterion 3: Canary queue endpoint cannot target the production thread or workspace.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])
    await db.save_config(canary_cfg)
    m.state_machine.config = canary_cfg

    fp = compute_config_fingerprint(canary_cfg)
    fake_token = f"token-g2c-{uuid.uuid4().hex[:16]}"

    # Transfer targeting production thread
    t_prod_thread = TransferRecord(
        transfer_id=f"transfer-g2c-{uuid.uuid4().hex[:12]}",
        iteration_id=1,
        direction=TransferDirection.CHATGPT_TO_CODEX.value,
        source_type="canary",
        source_identifier=canary_cfg.chatgpt.conversation_url,
        destination_type="codex",
        destination_identifier=PRODUCTION_THREAD,
        content_hash="hash1",
        char_count=10,
        message_text="test",
        status=TransferStatus.PREPARED.value,
        confirmation_token=fake_token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=fp,
    )
    await db.save_transfer_record(t_prod_thread)

    res_thread = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": t_prod_thread.transfer_id,
        "confirmation_token": fake_token,
        "thread_id": PRODUCTION_THREAD,
    })
    assert res_thread.status_code == 400
    assert "production codex thread and workspace are strictly forbidden in canary mode" in res_thread.json()["detail"].lower()

    # Transfer targeting production workspace
    t_prod_ws = TransferRecord(
        transfer_id=f"transfer-g2c-{uuid.uuid4().hex[:12]}",
        iteration_id=1,
        direction=TransferDirection.CHATGPT_TO_CODEX.value,
        source_type="canary",
        source_identifier=canary_cfg.chatgpt.conversation_url,
        destination_type="codex",
        destination_identifier=canary_cfg.codex.thread_id,
        content_hash="hash2",
        char_count=10,
        message_text="test",
        status=TransferStatus.PREPARED.value,
        confirmation_token=fake_token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=fp,
    )
    await db.save_transfer_record(t_prod_ws)

    res_ws = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": t_prod_ws.transfer_id,
        "confirmation_token": fake_token,
        "thread_id": canary_cfg.codex.thread_id,
        "repo_path": str(PRODUCTION_WORKSPACE),
    })
    assert res_ws.status_code == 400
    assert "production codex thread and workspace are strictly forbidden in canary mode" in res_ws.json()["detail"].lower()

    # Direct RealCodexQueueAdapter check
    adapter = RealCodexQueueAdapter(
        cli_path="codex",
        default_repo_path=str(PRODUCTION_WORKSPACE),
        db=db,
    )
    with pytest.raises(ValueError, match="Production Codex targets and workspaces are strictly forbidden in canary mode"):
        await adapter.queue_message(
            thread_id=PRODUCTION_THREAD,
            message="canary payload",
            confirmation_token=fake_token,
            transfer_id=t_prod_ws.transfer_id,
            is_canary=True,
        )


@pytest.mark.asyncio
async def test_4_configuration_changes_after_authorization_invalidate_authorization(audit_env):
    """
    Criterion 4: Configuration changes after authorization invalidate the authorization token.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])
    await db.save_config(canary_cfg)
    m.state_machine.config = canary_cfg

    fp1 = compute_config_fingerprint(canary_cfg)
    transfer_id = f"transfer-c2g-{uuid.uuid4().hex[:12]}"
    token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    content_hash = compute_sha256("test canary payload")

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=token,
        destination_identifier=canary_cfg.chatgpt.conversation_url,
        expected_content_hash=content_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=fp1,
    )
    await db.create_transfer_authorization(auth)

    transfer = TransferRecord(
        transfer_id=transfer_id,
        iteration_id=1,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        source_type="canary",
        source_identifier=canary_cfg.codex.thread_id,
        destination_type="chatgpt",
        destination_identifier=canary_cfg.chatgpt.conversation_url,
        content_hash=content_hash,
        char_count=len("test canary payload"),
        message_text="test canary payload",
        status=TransferStatus.PREPARED.value,
        confirmation_token=token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=fp1,
    )
    await db.save_transfer_record(transfer)

    # Mutate configuration
    mutated_cfg = canary_cfg.model_copy(deep=True)
    mutated_cfg.chatgpt.human_verification_state = "blocked"
    fp2 = compute_config_fingerprint(mutated_cfg)
    assert fp1 != fp2

    await db.save_config(mutated_cfg)
    m.state_machine.config = mutated_cfg

    # Database-level verification rejects changed fingerprint
    ok, err = await db.verify_and_consume_authorization(
        transfer_id=transfer_id,
        token=token,
        content_hash=content_hash,
        destination=canary_cfg.chatgpt.conversation_url,
        expected_config_fingerprint=fp2,
    )
    assert ok is False
    assert "configuration has changed since authorization was issued" in err.lower()

    # Endpoint rejects send because config fingerprint changed
    res = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer_id,
        "confirmation_token": token,
        "page_override": {"html": "<html></html>"},
    })
    assert res.status_code == 400
    assert "configuration has changed since transfer was prepared" in res.json()["detail"].lower()


@pytest.mark.asyncio
async def test_5_authorization_token_destination_mismatch_fails_closed(audit_env):
    """
    Criterion 5: Authorization token destination differs from the transfer destination (fails closed).
    """
    db = audit_env["db"]
    transfer_id = f"transfer-c2g-{uuid.uuid4().hex[:12]}"
    token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    content_hash = compute_sha256("test payload")
    dest_original = "https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581"
    dest_tampered = "https://chatgpt.com/c/00000000-0000-0000-0000-000000000099"

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=token,
        destination_identifier=dest_original,
        expected_content_hash=content_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
    )
    await db.create_transfer_authorization(auth)

    # Attempt to consume token against tampered destination
    ok, err = await db.verify_and_consume_authorization(
        transfer_id=transfer_id,
        token=token,
        content_hash=content_hash,
        destination=dest_tampered,
    )
    assert ok is False
    assert "destination mismatch" in err.lower()

    # Token remains unconsumed
    stored_auth = await db.get_transfer_authorization(token)
    assert stored_auth.consumed is False


@pytest.mark.asyncio
async def test_6_missing_or_invalid_canary_config_fails_closed(tmp_path: Path):
    """
    Criterion 6: Missing or invalid Canary config fails closed.
    """
    # 1. Missing required canary fields
    bad_cfg_file = tmp_path / "bad_canary.json"
    bad_cfg_file.write_text(json.dumps({
        "canary_mode": True,
        "codex": {
            "thread_id": "01a174f9-b5d8-7c91-9a27-d8cd3989575b",
            "repo_path": str(CANARY_WORKSPACE),
            "expected_branch": "master",
            "cli_path": "codex",
            "sessions_dir": str(CANARY_WORKSPACE / "sessions"),
            # Missing session_index_path!
            "integration_mode": "cli_queue",
        },
        "chatgpt": {
            "conversation_url": "https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
            "expected_conversation_id": "6ade6dd6-25bc-83ea-a016-86acbc909581",
            "profile_dir": str(BASE_DIR / ".canary-chatgpt-profile"),
            "cdp_url": "http://127.0.0.1:9223",
        },
        "route": {"manual_approval_required": True},
        "server": {"host": "127.0.0.1", "port": 8765},
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required canary fields"):
        load_explicit_config(bad_cfg_file, canary=True)

    # 2. Placeholders rejected
    ph_cfg_file = tmp_path / "placeholder_canary.json"
    ph_cfg_file.write_text(json.dumps({
        "canary_mode": True,
        "codex": {
            "thread_id": "<canary_thread_id>",
            "repo_path": str(CANARY_WORKSPACE),
            "expected_branch": "master",
            "cli_path": "codex",
            "sessions_dir": str(CANARY_WORKSPACE / "sessions"),
            "session_index_path": str(CANARY_WORKSPACE / "session_index.jsonl"),
            "integration_mode": "cli_queue",
        },
        "chatgpt": {
            "conversation_url": "https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
            "expected_conversation_id": "6ade6dd6-25bc-83ea-a016-86acbc909581",
            "profile_dir": str(BASE_DIR / ".canary-chatgpt-profile"),
            "cdp_url": "http://127.0.0.1:9223",
        },
        "route": {"manual_approval_required": True},
        "server": {"host": "127.0.0.1", "port": 8765},
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="Placeholder target identifiers are forbidden"):
        load_explicit_config(ph_cfg_file, canary=True)

    # 3. Production targets rejected for canary
    prod_in_canary = tmp_path / "prod_in_canary.json"
    prod_in_canary.write_text(json.dumps({
        "canary_mode": True,
        "codex": {
            "thread_id": PRODUCTION_THREAD,
            "repo_path": str(CANARY_WORKSPACE),
            "expected_branch": "master",
            "cli_path": "codex",
            "sessions_dir": str(CANARY_WORKSPACE / "sessions"),
            "session_index_path": str(CANARY_WORKSPACE / "session_index.jsonl"),
            "integration_mode": "cli_queue",
        },
        "chatgpt": {
            "conversation_url": f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}",
            "expected_conversation_id": PRODUCTION_CONVERSATION,
            "profile_dir": str(BASE_DIR / ".canary-chatgpt-profile"),
            "cdp_url": "http://127.0.0.1:9223",
        },
        "route": {"manual_approval_required": True},
        "server": {"host": "127.0.0.1", "port": 8765},
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="Production targets are forbidden for canary"):
        load_explicit_config(prod_in_canary, canary=True)


@pytest.mark.asyncio
async def test_7_existing_canary_config_never_overwritten(tmp_path: Path, temp_workspace: dict):
    """
    Criterion 7: Existing config is never overwritten.
    """
    existing_cfg = tmp_path / "config.canary.local.test.json"
    existing_cfg.write_text('{"locked": "do_not_overwrite"}', encoding="utf-8")

    codex_target = {
        "thread_id": "01a174f9-b5d8-7c91-9a27-d8cd3989575b",
        "cwd": str(CANARY_WORKSPACE),
        "title": "Canary",
        "sessions_dir": str(CANARY_WORKSPACE / "sessions"),
        "session_index_path": str(CANARY_WORKSPACE / "session_index.jsonl"),
    }
    chatgpt_target = {
        "conversation_id": "6ade6dd6-25bc-83ea-a016-86acbc909581",
        "conversation_url": "https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
        "profile_dir": str((BASE_DIR / ".canary-chatgpt-profile").resolve()),
        "cdp_url": "http://127.0.0.1:9223",
    }

    # Attempting to generate at existing path raises FileExistsError
    with pytest.raises(FileExistsError, match="refuse overwrite"):
        generate_canary_config(
            codex_target=codex_target,
            chatgpt_target=chatgpt_target,
            cli_path=str(temp_workspace["cli_path"]),
            output_path=existing_cfg,
        )

    # Ensure existing file content is unchanged
    assert existing_cfg.read_text(encoding="utf-8") == '{"locked": "do_not_overwrite"}'


@pytest.mark.asyncio
async def test_8_two_separate_authorizations_remain_mandatory(audit_env):
    """
    Criterion 8: Two separate authorizations remain mandatory.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])
    await db.save_config(canary_cfg)
    m.state_machine.config = canary_cfg

    nonce = uuid.uuid4().hex
    c2g_prep = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert c2g_prep.status_code == 200
    c2g_data = c2g_prep.json()
    nonce = c2g_data["canary_nonce"]
    token_c2g = c2g_data["confirmation_token"]
    tid_c2g = c2g_data["transfer_id"]

    # 1. token_c2g cannot be used for codex queue
    res_bad_queue = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": tid_c2g,
        "confirmation_token": token_c2g,
        "thread_id": canary_cfg.codex.thread_id,
    })
    assert res_bad_queue.status_code in (400, 404, 409)

    mock_html = '<form data-chatgpt-composer="true"><textarea id="prompt-textarea" placeholder="Send a message"></textarea><button data-testid="send-button">Send</button></form>'
    m.real_chatgpt_sender.set_injected_page_data({"html": mock_html})
    # 2. Consume token_c2g for chatgpt send
    res_send = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid_c2g,
        "confirmation_token": token_c2g,
    })
    assert res_send.status_code == 200

    # Set sent_at strictly before prompt extraction
    req_rec = await db.get_transfer_by_id(tid_c2g)
    req_rec.sent_at = datetime.utcnow() - timedelta(seconds=10)
    await db.save_transfer_record(req_rec)

    # 3. token_c2g is now consumed and cannot be reused
    res_reuse = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid_c2g,
        "confirmation_token": token_c2g,
        "page_override": {"html": mock_html},
    })
    assert res_reuse.status_code == 400

    # 4. G2C requires its own separate authorization
    ext = PromptExtractionRecord(
        source_conversation_id=canary_cfg.chatgpt.conversation_id,
        source_message_id="msg-001",
        content_hash=compute_sha256("resp"),
        extracted_prompt_hash=compute_sha256(canary_prompt(nonce)),
        extracted_prompt_preview=canary_prompt(nonce),
        provenance_policy=f"canary:{nonce}",
        is_fresh=True,
        transfer_eligible=True,
        created_at=datetime.utcnow() + timedelta(seconds=2),
    )
    await db.record_prompt_extraction(ext)

    # Transition to prompt preparation
    await m.state_machine.transition_to(ShuttleStage.CHATGPT_RESPONSE_READY, force=True)

    g2c_prep = await client.post("/api/transfer/codex_prompt/prepare", json={
        "source_extraction_id": ext.id,
        "iteration_id": 1,
    })
    assert g2c_prep.status_code == 200
    g2c_data = g2c_prep.json()
    token_g2c = g2c_data["confirmation_token"]
    tid_g2c = g2c_data["transfer_id"]

    assert token_g2c != token_c2g
    assert tid_g2c != tid_c2g

    # token_g2c can authorize codex queue
    runner_calls = []
    m.real_codex_queue.set_injected_runner(lambda *args: runner_calls.append(args))

    res_queue = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": tid_g2c,
        "confirmation_token": token_g2c,
        "thread_id": canary_cfg.codex.thread_id,
    })
    assert res_queue.status_code == 200
    assert len(runner_calls) == 1


@pytest.mark.asyncio
async def test_9_stale_or_mismatched_ui_selection_cannot_retarget_transfer(audit_env):
    """
    Criterion 9: A stale or mismatched UI selection cannot retarget a transfer.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])
    await db.save_config(canary_cfg)
    m.state_machine.config = canary_cfg

    fp = compute_config_fingerprint(canary_cfg)
    bound_thread = canary_cfg.codex.thread_id
    tampered_thread = "00000000-0000-0000-0000-000000000099"
    fake_token = f"token-g2c-{uuid.uuid4().hex[:16]}"

    transfer = TransferRecord(
        transfer_id=f"transfer-g2c-{uuid.uuid4().hex[:12]}",
        iteration_id=1,
        direction=TransferDirection.CHATGPT_TO_CODEX.value,
        source_type="canary",
        source_identifier=canary_cfg.chatgpt.conversation_url,
        destination_type="codex",
        destination_identifier=bound_thread,
        content_hash="contenthash123",
        char_count=10,
        message_text="prompt to queue",
        status=TransferStatus.PREPARED.value,
        confirmation_token=fake_token,
        provenance_policy="canary:abc1234567890abcdef1234567890abc",
        config_fingerprint=fp,
    )
    await db.save_transfer_record(transfer)

    auth = TransferAuthorization(
        transfer_id=transfer.transfer_id,
        confirmation_token=fake_token,
        destination_identifier=bound_thread,
        expected_content_hash="contenthash123",
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=fp,
    )
    await db.create_transfer_authorization(auth)

    runner_calls = []
    m.real_codex_queue.set_injected_runner(lambda t, r, m: runner_calls.append((t, r, m)))

    # Request specifying mismatched thread fails closed
    res = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": transfer.transfer_id,
        "confirmation_token": fake_token,
        "thread_id": tampered_thread,
    })
    assert res.status_code == 400
    assert "target thread mismatch" in res.json()["detail"].lower()
    assert "retargeting an authorized transfer is forbidden" in res.json()["detail"].lower()

    # Runner was NEVER called with the tampered thread
    assert len(runner_calls) == 0


@pytest.mark.asyncio
async def test_10_ambiguous_delivery_cannot_trigger_automatic_retries(audit_env):
    """
    Criterion 10: Ambiguous delivery (DeliveryOutcomeUnknownError) cannot trigger
    automatic retries.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])
    await db.save_config(canary_cfg)
    m.state_machine.config = canary_cfg

    fp = compute_config_fingerprint(canary_cfg)
    transfer_id = f"transfer-c2g-{uuid.uuid4().hex[:12]}"
    token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    content_hash = compute_sha256("test prompt")

    nonce = uuid.uuid4().hex
    canary_msg = canary_request(nonce)
    content_hash = compute_sha256(canary_msg)
    transfer = TransferRecord(
        transfer_id=transfer_id,
        iteration_id=1,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        source_type="canary",
        source_identifier=canary_cfg.codex.thread_id,
        destination_type="chatgpt",
        destination_identifier=canary_cfg.chatgpt.conversation_url,
        content_hash=content_hash,
        char_count=len(canary_msg),
        source_message_id=f"canary-request:{nonce}",
        message_text=canary_msg,
        status=TransferStatus.PREPARED.value,
        confirmation_token=token,
        provenance_policy=f"canary:{nonce}",
        config_fingerprint=fp,
    )
    await db.save_transfer_record(transfer)
    await db.create_transfer_authorization(TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=token,
        destination_identifier=canary_cfg.chatgpt.conversation_url,
        expected_content_hash=content_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=fp,
    ))

    # Mock sender to raise DeliveryOutcomeUnknownError
    class AmbiguousSender(RealChatGPTMessageSender):
        async def send_message(self, *args, **kwargs):
            raise DeliveryOutcomeUnknownError("Mid-transfer browser disconnect; outcome unknown")

    m.real_chatgpt_sender = AmbiguousSender(
        target_url=canary_cfg.chatgpt.conversation_url,
        conversation_id=canary_cfg.chatgpt.conversation_id,
        db=db,
    )

    await m.state_machine.transition_to(ShuttleStage.REPORT_PREVIEW, force=True)
    # First send encounters unknown delivery outcome
    res_unknown = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer_id,
        "confirmation_token": token,
    })
    assert res_unknown.status_code == 502
    assert "delivery outcome unknown" in res_unknown.json()["detail"].lower()

    # Verify state machine transitioned to DELIVERY_OUTCOME_UNKNOWN
    assert m.state_machine.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN

    # Any subsequent retry attempt is rejected with 409 Conflict
    res_retry = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": transfer_id,
        "confirmation_token": token,
    })
    assert res_retry.status_code == 409
    assert "manual reconciliation" in res_retry.json()["detail"].lower()

    res_queue_retry = await client.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": "other-transfer",
        "confirmation_token": "other-token",
    })
    assert res_queue_retry.status_code == 409
    assert "manual reconciliation" in res_queue_retry.json()["detail"].lower()


@pytest.mark.asyncio
async def test_11_production_mode_configuration_remains_available_without_silent_canary_selection(audit_env):
    """
    Criterion 11: Production-mode configuration remains available for explicitly
    configured production operations without being silently selected by Canary actions.
    """
    client = audit_env["client"]
    db = audit_env["db"]
    prod_cfg = make_production_config()
    await db.save_config(prod_cfg)
    m.state_machine.config = prod_cfg

    # Target info shows PRODUCTION mode
    res_info = await client.get("/api/transfer/target_info")
    data_info = res_info.json()
    assert data_info["execution_mode"] == "PRODUCTION"
    assert data_info["canary_mode"] is False
    assert data_info["canary_safe"] is False

    # 1. Canary action is strictly blocked
    res_canary = await client.post("/api/transfer/codex_report/prepare", json={
        "is_canary": True,
        "iteration_id": 1,
    })
    assert res_canary.status_code == 400
    assert "production configuration" in res_canary.json()["detail"].lower()

    # 2. Production operation preparation succeeds
    res_prod = await client.post("/api/transfer/codex_report/prepare", json={
        "report_text": "Production operator status report",
        "is_canary": False,
        "iteration_id": 1,
    })
    assert res_prod.status_code == 200
    prod_data = res_prod.json()
    assert prod_data["execution_mode"] == "PRODUCTION"
    assert prod_data["destination"] == prod_cfg.chatgpt.conversation_url
    assert prod_data["destination_conversation_id"] == PRODUCTION_CONVERSATION


@pytest.mark.asyncio
async def test_12_no_real_browser_send_or_real_codex_queue_in_tests(audit_env):
    """
    Criterion 12: No real browser send or real Codex queue process executed in automated tests.
    """
    db = audit_env["db"]
    canary_cfg = make_canary_config(audit_env["temp"])

    # Verify RealCodexQueueAdapter uses injected runner without calling subprocess
    runner_args = []
    queue_adapter = RealCodexQueueAdapter(
        cli_path="codex",
        default_repo_path=canary_cfg.codex.repo_path,
        db=db,
    )
    queue_adapter.set_injected_runner(lambda t, r, m: runner_args.append({"thread": t, "repo": r, "message": m}))

    token = f"token-g2c-{uuid.uuid4().hex[:16]}"
    tid = f"transfer-g2c-{uuid.uuid4().hex[:12]}"
    content_hash = compute_sha256("test prompt")

    await db.create_transfer_authorization(TransferAuthorization(
        transfer_id=tid,
        confirmation_token=token,
        destination_identifier=canary_cfg.codex.thread_id,
        expected_content_hash=content_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
    ))

    res_q = await queue_adapter.queue_message(
        thread_id=canary_cfg.codex.thread_id,
        message="test prompt",
        confirmation_token=token,
        transfer_id=tid,
        repo_path=canary_cfg.codex.repo_path,
    )
    assert res_q["status"] == "queued"
    assert len(runner_args) == 1
    assert runner_args[0]["thread"] == canary_cfg.codex.thread_id
    assert runner_args[0]["message"] == "test prompt"

    # Verify RealChatGPTMessageSender uses mock page data without launching Edge/Playwright
    sender = RealChatGPTMessageSender(
        target_url=canary_cfg.chatgpt.conversation_url,
        conversation_id=canary_cfg.chatgpt.conversation_id,
        db=db,
    )
    mock_composer_html = '<form data-chatgpt-composer="true"><textarea id="prompt-textarea" placeholder="Send a message"></textarea><button data-testid="send-button">Send</button></form>'
    sender.set_injected_page_data({"html": mock_composer_html})

    token_send = f"token-c2g-{uuid.uuid4().hex[:16]}"
    tid_send = f"transfer-c2g-{uuid.uuid4().hex[:12]}"
    send_hash = compute_sha256("test send")
    await db.create_transfer_authorization(TransferAuthorization(
        transfer_id=tid_send,
        confirmation_token=token_send,
        destination_identifier=canary_cfg.chatgpt.conversation_url,
        expected_content_hash=send_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
    ))

    res_s = await sender.send_message(
        message="test send",
        confirmation_token=token_send,
        transfer_id=tid_send,
    )
    assert res_s["status"] == "delivered"
    assert res_s["delivery_evidence"]["method"] == "injected_mock"
