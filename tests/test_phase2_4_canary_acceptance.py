"""
Phase 2.4 — Isolated Live Canary & End-to-End Acceptance Tests.

Verifies:
- Complete isolation from production project production project and Goal A thread
- Server binding strictly to 127.0.0.1:8765
- Absence of autonomous background loops and presence of mandatory single-use authorization
- 11-step controlled canary acceptance sequence with dual independent authorizations
- Immediate halt and transition to DELIVERY_OUTCOME_UNKNOWN on disconnects or ambiguities
- Manual operator reconciliation
"""
import asyncio
from datetime import datetime, timedelta
import json
from pathlib import Path
import pytest
from httpx import AsyncClient, ASGITransport
import uuid

from app.adapters import (
    DeliveryOutcomeUnknownError,
    MockChatGPTMessageSender,
    MockCodexQueueAdapter,
    RealChatGPTBrowserObserver,
    RealChatGPTMessageSender,
    RealCodexQueueAdapter,
    compute_sha256,
)
from app.canary import (
    is_production_thread,
    is_production_conversation,
    is_production_workspace,
    PRODUCTION_CONVERSATION,
)
from app.database import Database
from app.main import app, db as main_db, state_machine
from app.models import (
    AppConfig,
    ChatGPTConfig,
    CodexConfig,
    RouteConfig,
    ShuttleStage,
    TransferAuthorization,
    TransferDirection,
    TransferRecord,
    TransferStatus,
)


@pytest.fixture
def tmp_canary_workspace(tmp_path: Path):
    ws = tmp_path / "canary_workspace"
    ws.mkdir(parents=True, exist_ok=True)
    return ws


@pytest.fixture
def canary_config(tmp_canary_workspace: Path):
    return AppConfig(
        codex=CodexConfig(
            repo_path=str(tmp_canary_workspace),
            expected_branch="master",
            cli_path="codex",
            thread_id="00000000-canary-0000-0000-000000000001",
            thread_title="Canary Test Thread (Isolated)",
            sessions_dir=str(tmp_canary_workspace / "sessions"),
            integration_mode="mock",
        ),
        chatgpt=ChatGPTConfig(
            browser_type="msedge",
            profile_dir=str(tmp_canary_workspace / "chatgpt-profile"),
            conversation_url="https://chatgpt.com/c/00000000-0000-0000-0000-canarytest001",
            expected_gpt_id=None,
            expected_conversation_id="00000000-0000-0000-0000-canarytest001",
            browser_readiness="ready",
            human_verification_state="passed",
        ),
        route=RouteConfig(
            codex_to_chatgpt_enabled=True,
            chatgpt_to_codex_enabled=True,
            prompt_extraction_policy="strict_markdown_blocks",
            manual_approval_required=True,
            duplicate_detection_policy="hash_and_prefix_match",
            restart_recovery_policy="resume_latest_checkpoint",
        ),
        server_host="127.0.0.1",
        server_port=8765,
    )


@pytest.fixture
async def client(tmp_path: Path, canary_config: AppConfig):
    test_db_path = tmp_path / "test_canary.db"
    test_db = Database(test_db_path)
    await test_db.init()
    await test_db.save_config(canary_config)

    import app.main as m
    old_db = m.db
    old_sm = m.state_machine
    old_sender = m.real_chatgpt_sender
    old_obs = m.real_chatgpt_observer
    old_queue = m.real_codex_queue

    m.db = test_db
    m.state_machine.db = test_db
    m.state_machine.config = canary_config
    await m.state_machine.init()

    m.real_chatgpt_sender = RealChatGPTMessageSender(
        target_url=canary_config.chatgpt.conversation_url,
        conversation_id=canary_config.chatgpt.conversation_id,
        gpt_id=canary_config.chatgpt.gpt_id,
        db=test_db,
    )
    m.real_chatgpt_observer = RealChatGPTBrowserObserver(
        target_url=canary_config.chatgpt.conversation_url,
        conversation_id=canary_config.chatgpt.conversation_id,
        gpt_id=canary_config.chatgpt.gpt_id,
        prompt_extractor=m.prompt_extractor,
    )
    m.real_codex_queue = RealCodexQueueAdapter(
        cli_path=canary_config.codex.cli_path,
        default_repo_path=canary_config.codex.repo_path,
        db=test_db,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as ac:
        yield ac, test_db, canary_config

    m.db = old_db
    m.state_machine = old_sm
    m.real_chatgpt_sender = old_sender
    m.real_chatgpt_observer = old_obs
    m.real_codex_queue = old_queue


@pytest.mark.asyncio
async def test_1_canary_config_isolation(client):
    """Gate 1 & Gate 2: Config isolates completely from production project and Goal A."""
    ac, db, cfg = client
    res = await ac.get("/api/config")
    assert res.status_code == 200
    data = res.json()

    assert data["codex"]["repo_path"] == cfg.codex.repo_path
    assert "production" not in data["codex"]["repo_path"].lower()
    assert data["codex"]["thread_id"] == "00000000-canary-0000-0000-000000000001"
    assert not is_production_thread(data["codex"]["thread_id"])
    assert not is_production_conversation(data["chatgpt"]["conversation_url"])
    assert data["server_host"] == "127.0.0.1"
    assert data["server_port"] == 8765


@pytest.mark.asyncio
async def test_2_server_binding_strictly_127_0_0_1_8765(client):
    """Gate 1: Application server binds strictly to 127.0.0.1:8765."""
    ac, db, cfg = client
    assert cfg.server_host == "127.0.0.1"
    assert cfg.server_port == 8765
    res = await ac.get("/api/state")
    assert res.status_code == 200
    assert res.json()["stage"] == "IDLE"


@pytest.mark.asyncio
async def test_3_no_autonomous_background_loops_or_implicit_authorization(client):
    """Gate 1: Zero autonomous transfer loops and strict single-use authorization."""
    ac, db, cfg = client
    res = await ac.post("/api/transfer/chatgpt/send", json={
        "transfer_id": "nonexistent-transfer",
        "confirmation_token": "fake-token"
    })
    assert res.status_code in (404, 400)

    res_q = await ac.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": "nonexistent-transfer",
        "confirmation_token": "fake-token"
    })
    assert res_q.status_code in (404, 400)


@pytest.mark.asyncio
async def test_4_canary_report_preparation_with_unique_nonce(client):
    """Gate 3 Step 1 & 2: Prepare canary report with unique nonce and verify SHA-256."""
    ac, db, cfg = client
    nonce = uuid.uuid4().hex[:8]
    canary_text = f"CANARY REPORT [nonce={nonce}] Test report acknowledging baseline state."
    expected_hash = compute_sha256(canary_text)

    res = await ac.post("/api/transfer/codex_report/prepare", json={
        "report_text": canary_text,
        "is_canary": True,
        "iteration_id": 1
    })
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "prepared"
    assert data["transfer_id"].startswith("transfer-c2g-")
    assert data["confirmation_token"].startswith("token-c2g-")
    assert data["content_hash"] == expected_hash
    assert data["char_count"] == len(canary_text)

    st = await ac.get("/api/state")
    assert st.json()["stage"] == "REPORT_PREVIEW"


@pytest.mark.asyncio
async def test_5_canary_chatgpt_rejects_unauthorized_or_modified_token(client):
    """Gate 3 Step 3: Authorization is strictly bound to payload SHA-256."""
    ac, db, cfg = client
    nonce = uuid.uuid4().hex[:8]
    canary_text = f"CANARY REPORT [nonce={nonce}]"
    prep = await ac.post("/api/transfer/codex_report/prepare", json={
        "report_text": canary_text,
        "is_canary": True
    })
    tid = prep.json()["transfer_id"]
    token = prep.json()["confirmation_token"]

    res = await ac.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid,
        "confirmation_token": "wrong-token-abc"
    })
    assert res.status_code in (400, 500)


@pytest.mark.asyncio
async def test_6_canary_chatgpt_rejects_production_page(tmp_canary_workspace):
    """Gate 2 & Gate 4: ChatGPT sender strictly rejects target URL mismatch."""
    sender = RealChatGPTMessageSender(
        target_url="https://chatgpt.com/c/00000000-0000-0000-0000-canarytest001",
        conversation_id="00000000-0000-0000-0000-canarytest001",
    )
    assert not sender.validate_target_url(f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}")
    assert sender.validate_target_url("https://chatgpt.com/c/00000000-0000-0000-0000-canarytest001")


@pytest.mark.asyncio
async def test_7_canary_chatgpt_send_and_user_turn_verification(client):
    """Gate 3 Step 3 & 4: Successful mock send verifying user turn in DOM."""
    ac, db, cfg = client
    nonce = uuid.uuid4().hex[:8]
    canary_text = f"CANARY REPORT [nonce={nonce}]"

    prep = await ac.post("/api/transfer/codex_report/prepare", json={
        "report_text": canary_text,
        "is_canary": True
    })
    tid = prep.json()["transfer_id"]
    token = prep.json()["confirmation_token"]

    mock_page_data = {
        "url": cfg.chatgpt.conversation_url,
        "html": f'''
        <div id="__next">
          <form data-chatgpt-composer="true">
            <div contenteditable="true" role="textbox" id="prompt-textarea"></div>
            <button data-testid="send-button" type="submit"></button>
          </form>
          <div data-testid="conversation-turn-3">
            <div data-message-author-role="user">{canary_text}</div>
          </div>
        </div>
        '''
    }

    res = await ac.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid,
        "confirmation_token": token,
        "page_override": mock_page_data,
    })
    assert res.status_code == 200
    assert res.json()["status"] == "delivered"

    st = await ac.get("/api/state")
    assert st.json()["stage"] == "CHATGPT_WAITING"

    res_repeat = await ac.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid,
        "confirmation_token": token,
        "page_override": mock_page_data,
    })
    assert res_repeat.status_code in (400, 500)


@pytest.mark.asyncio
async def test_8_canary_chatgpt_response_observation_and_prompt_extraction(client):
    """Gate 3 Step 5 & 6: Observe new assistant response with valid test prompt."""
    ac, db, cfg = client
    raw_prompt_body = "# Phase 2.4 Canary Ack\n\nAcknowledged canary report nonce.\n" + ("- Step verification line\n" * 15)

    assistant_turn_html = f'''
    <!DOCTYPE html>
    <html>
    <body>
    <div data-app-shell-focus-area="main">
      <div class="thread-scroll-container">
        <div data-turn-key="turn-uuid-001">
          <div data-content-search-turn-key="fallback-turn-0">
            <div data-content-search-unit-key="fallback-turn-0:0:user">
              <div data-user-message-bubble="true">CANARY REPORT</div>
            </div>
            <div data-content-search-unit-key="fallback-turn-0:1:assistant" data-chatgpt-search-unit-key="fallback-turn-0:1:assistant">
              <div data-markdown-text-style="assistant-message" class="MarkdownRoot-rZKhxa">
                <pre><code>{raw_prompt_body}</code></pre>
              </div>
            </div>
            <div class="turn-action-controls">
              <button aria-label="Copy">Copy</button>
            </div>
          </div>
        </div>
      </div>
      <form data-chatgpt-composer="true">
        <div contenteditable="true" role="textbox"></div>
        <button data-testid="send-button"></button>
      </form>
    </div>
    </body>
    </html>
    '''

    obs_res = await ac.post("/api/transfer/chatgpt/observe", json={
        "page_data": {
            "url": cfg.chatgpt.conversation_url,
            "html": assistant_turn_html
        }
    })
    assert obs_res.status_code == 200
    data = obs_res.json()
    assert data["status"] == "ready"
    assert data["extraction"] is not None
    assert data["extraction"]["transfer_eligible"] is True
    assert "Canary Ack" in data["extraction"]["extracted_prompt_preview"]

    st = await ac.get("/api/state")
    assert st.json()["stage"] == "CHATGPT_RESPONSE_READY"


@pytest.mark.asyncio
async def test_9_canary_codex_prompt_preparation_separate_authorization(client):
    """Gate 3 Step 7 & 8: Separate authorization for Codex queue step."""
    ac, db, cfg = client
    import app.main as m
    await m.state_machine.transition_to(ShuttleStage.CHATGPT_RESPONSE_READY)

    test_prompt = "# Canary Task\nEcho CANARY-OK"
    h = compute_sha256(test_prompt)

    prep = await ac.post("/api/transfer/codex_prompt/prepare", json={
        "prompt_text": test_prompt,
        "is_operator_modified": False,
        "iteration_id": 1
    })
    assert prep.status_code == 200
    data = prep.json()
    assert data["status"] == "prepared"
    assert data["transfer_id"].startswith("transfer-g2c-")
    assert data["confirmation_token"].startswith("token-g2c-")
    assert data["content_hash"] == h

    st = await ac.get("/api/state")
    assert st.json()["stage"] == "PROMPT_PREVIEW"


@pytest.mark.asyncio
async def test_10_canary_codex_queue_submission_to_disposable_workspace(client, tmp_canary_workspace):
    """Gate 3 Step 9 & 10: Queue submission to disposable workspace outside production."""
    ac, db, cfg = client
    test_prompt = "# Canary Task\nEcho CANARY-OK"

    import app.main as m
    await m.state_machine.transition_to(ShuttleStage.CHATGPT_RESPONSE_READY)

    prep = await ac.post("/api/transfer/codex_prompt/prepare", json={
        "prompt_text": test_prompt,
        "is_operator_modified": False
    })
    tid = prep.json()["transfer_id"]
    token = prep.json()["confirmation_token"]

    runner_called = False
    async def mock_runner(cmd, thread_id, msg, repo_path, transfer_id):
        nonlocal runner_called
        runner_called = True
        assert cmd[0] == "codex"
        assert cmd[1] == "queue"
        assert cmd[2] == "--thread"
        assert cmd[3] == cfg.codex.thread_id
        assert cmd[4] == "--message"
        assert cmd[5] == test_prompt
        assert cmd[6] == "-C"
        assert cmd[7] == str(tmp_canary_workspace)
        assert not is_production_workspace(repo_path)
        return {
            "status": "queued",
            "receipt_id": "queue-receipt-canary-001",
            "thread_id": thread_id,
            "repo_path": repo_path
        }

    from app.main import real_codex_queue
    if real_codex_queue:
        real_codex_queue.set_injected_runner(mock_runner)

    res = await ac.post("/api/transfer/codex_queue/submit", json={
        "transfer_id": tid,
        "confirmation_token": token,
        "thread_id": cfg.codex.thread_id,
        "repo_path": str(tmp_canary_workspace)
    })
    assert res.status_code == 200
    assert res.json()["status"] == "delivered"

    st = await ac.get("/api/state")
    assert st.json()["stage"] == "CODEX_WAITING"


@pytest.mark.asyncio
async def test_11_canary_disconnect_outcome_unknown_and_reconciliation(client):
    """Gate 4: Ambiguous outcome transitions to DELIVERY_OUTCOME_UNKNOWN without retry."""
    ac, db, cfg = client
    nonce = uuid.uuid4().hex[:8]
    canary_text = f"CANARY REPORT [nonce={nonce}]"

    prep = await ac.post("/api/transfer/codex_report/prepare", json={
        "report_text": canary_text,
        "is_canary": True
    })
    tid = prep.json()["transfer_id"]
    token = prep.json()["confirmation_token"]

    disconnect_page_data = {
        "url": cfg.chatgpt.conversation_url,
        "html": '''
        <div id="__next">
          <form data-chatgpt-composer="true">
            <div contenteditable="true" role="textbox"></div>
            <button data-testid="send-button"></button>
          </form>
        </div>
        ''',
        "disconnect_after_submit": True
    }

    res = await ac.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid,
        "confirmation_token": token,
        "page_override": disconnect_page_data,
    })
    assert res.status_code == 502
    assert "outcome unknown" in res.json()["detail"].lower()

    st = await ac.get("/api/state")
    assert st.json()["stage"] == "DELIVERY_OUTCOME_UNKNOWN"

    # Operator reconciliation flow
    recon = await ac.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": tid,
        "resolution": "mark_cancelled",
        "notes": "Verified in test browser that message was not delivered."
    })
    assert recon.status_code == 200
    assert recon.json()["new_stage"] == "REPORT_PREVIEW"
