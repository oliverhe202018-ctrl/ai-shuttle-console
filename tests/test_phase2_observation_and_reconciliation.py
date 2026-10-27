"""
Phase 2 Automated Verification Suite — Real Session Observation and Message Reconciliation.
Validates:
1. Codex session reading, identity validation, event flow parsing, turn recency reconciliation, and sha256 hashing.
2. ChatGPT browser observation, target conversation URL binding, Cloudflare Turnstile/blocked detection,
   error placeholder defense ('无法加载此回复'), and multi-tier prompt extraction with provenance.
3. SQLite observations & prompt_extractions persistence and state machine transitions.
4. Strictly read-only zero-side-effects enforcement.
"""
import json
import os
import tempfile
from pathlib import Path
import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters import (
    ReadOnlyCodexSessionObserver,
    RealChatGPTBrowserObserver,
    StandardPromptExtractor,
    is_error_placeholder,
)
from app.database import Database
from app.models import (
    AppConfig,
    CodexConfig,
    ChatGPTConfig,
    ObservationRecord,
    PromptExtractionRecord,
    RouteConfig,
    ShuttleStage,
    compute_sha256,
)
from app.state_machine import ShuttleStateMachine


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as base_tmp:
        tmp_path = Path(base_tmp)
        codex_dir = tmp_path / "codex_sessions"
        codex_dir.mkdir(parents=True, exist_ok=True)
        session_index = tmp_path / "session_index.jsonl"
        db_file = tmp_path / "shuttle_test.db"
        yield {
            "root": tmp_path,
            "codex_sessions": codex_dir,
            "session_index": session_index,
            "db_file": db_file,
        }


@pytest.mark.asyncio
async def test_codex_identity_matching_and_reconciliation(temp_dirs):
    """
    Verify Codex observer identifies the exact configured thread, validates metadata,
    and accurately reconciles completed assistant answer.
    """
    thread_id = "00000000-0000-0000-0000-000000000001"
    expected_cwd = "D:/workspace/example-repo"
    expected_title = "重置跑胡子手机指导优先级"

    # Write mock session_index.jsonl
    with open(temp_dirs["session_index"], "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "id": thread_id,
            "thread_name": expected_title,
            "cwd": expected_cwd,
            "updated_at": "2026-10-25T00:52:40.398Z"
        }) + "\n")

    # Write target session JSONL with session_meta, user prompt, and completed final_answer
    session_file = temp_dirs["codex_sessions"] / f"rollout-{thread_id}.jsonl"
    events = [
        {"timestamp": "2026-10-25T00:52:11Z", "type": "session_meta", "payload": {"id": thread_id, "cwd": expected_cwd}},
        {"timestamp": "2026-10-25T00:52:12Z", "type": "turn_context", "payload": {"turn_id": "turn-001"}},
        {"timestamp": "2026-10-25T00:52:13Z", "type": "response_item", "payload": {"type": "message", "role": "user", "content": "请重置跑胡子指导优先级"}},
        {"timestamp": "2026-10-25T00:52:20Z", "type": "message", "payload": {
            "type": "message",
            "role": "assistant",
            "phase": "final_answer",
            "status": "completed",
            "turn_id": "turn-001",
            "content": [{"type": "output_text", "text": "跑胡子手机指导优先级已重置为Goal A专项锁定。"}]
        }},
    ]
    with open(session_file, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_dirs["codex_sessions"],
        session_index_path=temp_dirs["session_index"],
    )

    obs = observer.observe_thread(
        thread_id=thread_id,
        expected_cwd=expected_cwd,
        expected_title=expected_title,
    )

    assert obs["metadata_valid"] is True
    assert obs["is_completed"] is True
    assert obs["is_in_progress"] is False
    assert obs["latest_assistant_response"] == "跑胡子手机指导优先级已重置为Goal A专项锁定。"
    assert obs["latest_response_hash"] == compute_sha256("跑胡子手机指导优先级已重置为Goal A专项锁定。")
    assert obs["latest_turn_id"] == "turn-001"
    assert obs["user_turns_count"] == 1
    assert obs["assistant_turns_count"] == 1


@pytest.mark.asyncio
async def test_codex_rejects_mismatched_cwd_and_unrelated_session(temp_dirs):
    """
    Verify observer rejects observation when CWD does not match expected workspace
    or thread_id cannot be found.
    """
    thread_id = "00000000-0000-0000-0000-000000000001"
    # Write session_index with different workspace
    with open(temp_dirs["session_index"], "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "id": thread_id,
            "thread_name": "无关项目会话",
            "cwd": "D:/project/unrelated-repo",
        }) + "\n")

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_dirs["codex_sessions"],
        session_index_path=temp_dirs["session_index"],
    )

    # 1. Mismatched CWD
    obs_mismatch = observer.observe_thread(
        thread_id=thread_id,
        expected_cwd="D:/workspace/example-repo",
    )
    assert obs_mismatch["metadata_valid"] is False
    assert any("CWD mismatch" in e for e in obs_mismatch["validation_errors"])

    # 2. Unknown thread ID
    obs_unknown = observer.observe_thread(
        thread_id="non-existent-thread-id",
        expected_cwd="D:/workspace/example-repo",
    )
    assert obs_unknown["metadata_valid"] is False
    assert any("not found" in e for e in obs_unknown["validation_errors"])


@pytest.mark.asyncio
async def test_codex_turn_recency_and_in_progress_state(temp_dirs):
    """
    Verify observer correctly distinguishes in-progress turns vs historical answers:
    If a user prompt appears AFTER the latest assistant final answer, do NOT return
    the old answer as current.
    """
    thread_id = "00000000-0000-0000-0000-000000000001"
    session_file = temp_dirs["codex_sessions"] / f"rollout-{thread_id}.jsonl"

    with open(temp_dirs["session_index"], "w", encoding="utf-8") as f:
        f.write(json.dumps({"id": thread_id, "thread_name": "测试会话"}) + "\n")

    # Sequence: User Turn 1 -> Assistant Turn 1 Completed -> User Turn 2 Pending
    events = [
        {"type": "turn_context", "payload": {"turn_id": "turn-001"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": "第一问"}},
        {"type": "message", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "turn_id": "turn-001", "content": "第一答"}},
        {"type": "turn_context", "payload": {"turn_id": "turn-002"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": "第二问（用户刚刚发送）"}},
    ]
    with open(session_file, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_dirs["codex_sessions"],
        session_index_path=temp_dirs["session_index"],
    )

    obs = observer.observe_thread(thread_id=thread_id)

    # Must recognize that a new turn is in progress and NOT return "第一答"
    assert obs["is_in_progress"] is True
    assert obs["is_completed"] is False
    assert obs["latest_assistant_response"] is None
    assert obs["latest_user_prompt"] == "第二问（用户刚刚发送）"


@pytest.mark.asyncio
async def test_chatgpt_observer_url_binding_and_rejection():
    """
    Verify ChatGPT observer strictly enforces URL, GPT ID, and Conversation ID binding.
    """
    target_url = "https://chatgpt.com/g/g-p-example/c/00000000-0000-0000-0000-000000000002"
    gpt_id = "g-p-example"
    conversation_id = "00000000-0000-0000-0000-000000000002"

    observer = RealChatGPTBrowserObserver(
        target_url=target_url,
        gpt_id=gpt_id,
        conversation_id=conversation_id,
    )

    # 1. Matching target URL
    res_valid = observer.validate_url(target_url)
    assert res_valid["matches_target"] is True
    assert not res_valid["validation_errors"]

    # 2. General ChatGPT Home URL (Mismatched)
    res_home = observer.validate_url("https://chatgpt.com/")
    assert res_home["matches_target"] is False
    assert len(res_home["validation_errors"]) > 0

    # 3. Different GPT / Conversation ID
    res_other = observer.validate_url("https://chatgpt.com/g/g-other-bot/c/11111111-2222-3333-4444-555555555555")
    assert res_other["matches_target"] is False


@pytest.mark.asyncio
async def test_chatgpt_error_placeholder_defense():
    """
    Verify observer strictly detects and rejects '无法加载此回复' and other failure banners,
    refusing to mark response as eligible for transfer.
    """
    target_url = "https://chatgpt.com/g/g-p-example/c/00000000-0000-0000-0000-000000000002"
    observer = RealChatGPTBrowserObserver(target_url=target_url)

    # Test payload containing error placeholder
    payload = {
        "url": target_url,
        "text": "无法加载此回复",
        "canvas_text": "",
        "code_blocks": [],
    }

    obs = observer.inspect_dom_payload(payload)
    assert obs["has_error_placeholder"] is True
    assert obs["transfer_eligible"] is False
    assert "Error placeholder detected" in obs["ineligibility_reason"]

    # Test helper directly
    assert is_error_placeholder("无法加载此回复") is True
    assert is_error_placeholder("Something went wrong, please try reloading the conversation.") is True
    assert is_error_placeholder("Normal assistant response about Mahjong") is False


@pytest.mark.asyncio
async def test_chatgpt_prompt_extraction_with_provenance():
    """
    Verify prompt extraction hierarchy and provenance tagging:
    Canvas -> Code block -> Structured Markdown.
    """
    extractor = StandardPromptExtractor()

    # 1. Canvas with Mission/Governance
    canvas_payload = {
        "canvas_text": "## Mission\nUpdate priority rules for PaoHuZi mobile guidance according to Goal A constraints.\n" + ("x" * 300),
        "code_blocks": ["print('short code')"],
        "text": "Please see the canvas update.",
    }
    p1, prov1 = extractor.extract_with_provenance(canvas_payload)
    assert prov1 == "canvas"
    assert "Mission" in p1

    # 2. Code Block
    code_block_text = "```markdown\n# PHASE 2 IMPLEMENTATION\n" + ("rule = 1\n" * 50) + "```"
    code_payload = {
        "canvas_text": "",
        "code_blocks": [code_block_text],
        "text": "Below is the task prompt to send to Codex.",
    }
    p2, prov2 = extractor.extract_with_provenance(code_payload)
    assert prov2 == "code_block"
    assert "PHASE 2" in p2

    # 3. Structured Markdown section
    markdown_payload = {
        "canvas_text": "",
        "code_blocks": [],
        "text": "分析完成。\n三、可直接交给 Codex 的执行提示词\n请执行以下重置动作：\n" + ("锁定 Goal A。\n" * 30) + "\n四、预期产物",
    }
    p3, prov3 = extractor.extract_with_provenance(markdown_payload)
    assert prov3 == "structured_markdown"
    assert "可直接交给 Codex 的执行提示词" in p3
    assert "四、预期产物" not in p3  # Bounded properly


@pytest.mark.asyncio
async def test_sqlite_observations_and_extractions_persistence(temp_dirs):
    """
    Verify database records observations and prompt_extractions with full schema integrity.
    """
    db = Database(str(temp_dirs["db_file"]))
    await db.init()

    # 1. Save Observation
    obs = ObservationRecord(
        source_type="chatgpt",
        source_identifier="https://chatgpt.com/c/test-id",
        message_id="msg-101",
        content_hash="abc123hash",
        payload_preview="Sample preview payload",
        status="verified",
    )
    saved_obs = await db.record_observation(obs)
    assert saved_obs.id is not None

    latest_obs = await db.get_latest_observation("chatgpt")
    assert latest_obs is not None
    assert latest_obs.content_hash == "abc123hash"
    assert latest_obs.status == "verified"

    # 2. Save Prompt Extraction
    ext = PromptExtractionRecord(
        source_conversation_id="test-conv-001",
        content_hash="abc123hash",
        extracted_prompt_hash="prompt_hash_999",
        extracted_prompt_preview="## Mission Prompt Preview",
        provenance_policy="canvas",
        is_fresh=True,
        transfer_eligible=True,
    )
    saved_ext = await db.record_prompt_extraction(ext)
    assert saved_ext.id is not None

    latest_ext = await db.get_latest_prompt_extraction("test-conv-001")
    assert latest_ext is not None
    assert latest_ext.extracted_prompt_hash == "prompt_hash_999"
    assert latest_ext.provenance_policy == "canvas"
    assert latest_ext.transfer_eligible is True


@pytest.mark.asyncio
async def test_rejection_of_stale_prompt_file_and_restart_invariance(temp_dirs):
    """
    Critical Invariants:
    1. Never use initial_prompt_file as fallback for latest ChatGPT response.
    2. Never infer a prompt is current merely because it exists on disk.
    3. A previously completed response must not be reclassified as new after restart.
    """
    db = Database(str(temp_dirs["db_file"]))
    await db.init()

    # Pre-record a historical prompt extraction
    old_prompt = "## Old Historical Prompt: Run Phase 1 Mobile MVP Audit\n" + ("rule\n" * 20)
    old_hash = compute_sha256(old_prompt)
    await db.record_prompt_extraction(PromptExtractionRecord(
        source_conversation_id="test-conv-001",
        content_hash=old_hash,
        extracted_prompt_hash=old_hash,
        extracted_prompt_preview=old_prompt[:100],
        provenance_policy="code_block",
        is_fresh=True,
        transfer_eligible=True,
    ))

    # Restart scenario: New DB connection / application reboot
    db_restarted = Database(str(temp_dirs["db_file"]))
    await db_restarted.init()

    # Verify stale detection: has_seen_content_hash returns True
    assert await db_restarted.has_seen_content_hash(old_hash) is True

    # Unknown hash returns False
    new_prompt = "## Fresh Prompt: Goal A Execution Rule\n" + ("new_rule\n" * 20)
    new_hash = compute_sha256(new_prompt)
    assert await db_restarted.has_seen_content_hash(new_hash) is False


@pytest.mark.asyncio
async def test_reconciliation_api_endpoint(temp_dirs):
    """
    Verify /api/reconciliation returns structured audit details for both sources and eligibility.
    """
    from app.main import app
    import app.main as app_main

    test_db = Database(str(temp_dirs["db_file"]))
    await test_db.init()

    cfg = AppConfig(
        codex=CodexConfig(
            repo_path="D:/workspace/example-repo",
            expected_branch="codex/run_huzi_mobile_guidance",
            cli_path="C:/dummy/codex.exe",
            thread_id="00000000-0000-0000-0000-000000000001",
            thread_title="重置跑胡子手机指导优先级",
            sessions_dir=str(temp_dirs["codex_sessions"]),
            session_index_path=str(temp_dirs["session_index"]),
        ),
        chatgpt=ChatGPTConfig(
            browser_type="msedge",
            profile_dir=str(temp_dirs["root"] / "profile"),
            conversation_url="https://chatgpt.com/g/g-p-example/c/00000000-0000-0000-0000-000000000002",
        ),
        route=RouteConfig(),
    )
    await test_db.save_config(cfg)

    old_db = app_main.db
    old_sm = app_main.state_machine
    app_main.db = test_db
    app_main.state_machine = ShuttleStateMachine(test_db, cfg)
    await app_main.state_machine.init()

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
            res = await client.get("/api/reconciliation")
            assert res.status_code == 200
            data = res.json()
            assert "codex" in data
            assert "chatgpt" in data
            assert "reconciliation" in data
            assert data["codex"]["project_path"] == "D:/workspace/example-repo"
            assert data["reconciliation"]["expected_source"] == "chatgpt"
            assert data["reconciliation"]["expected_destination"] == "codex_gui"
            # Without fresh extraction, transfer_eligible must be False
            assert data["reconciliation"]["transfer_eligible"] is False
    finally:
        app_main.db = old_db
        app_main.state_machine = old_sm
@pytest.mark.asyncio
async def test_cross_source_iteration_matching_and_mismatch_defense(temp_dirs):
    """
    Gate 4 Verification:
    If ChatGPT prompt does not correlate with Codex iteration (e.g. mobile audit vs Goal A),
    mark reconciliation as MISMATCH and prohibit transfer eligibility.
    """
    from app.main import app
    import app.main as app_main

    test_db = Database(str(temp_dirs["db_file"]))
    await test_db.init()

    # Pre-record Codex observation (Goal A)
    await test_db.record_observation(ObservationRecord(
        source_type="codex",
        source_identifier="00000000-0000-0000-0000-000000000001",
        message_id="turn-goal-a",
        content_hash="hash-codex-goal-a",
        payload_preview="Goal A Critical-Response One-Ply Lookahead Checkpoint",
        status="verified",
    ))

    # Pre-record ChatGPT observation with UNRELATED/MISMATCHED prompt (e.g. mobile release audit)
    await test_db.record_observation(ObservationRecord(
        source_type="chatgpt",
        source_identifier="https://chatgpt.com/c/test",
        message_id=None,
        content_hash="hash-mismatched",
        payload_preview="Unrelated Task: Mobile Release Audit checklist",
        status="verified",
    ))
    await test_db.record_prompt_extraction(PromptExtractionRecord(
        source_conversation_id="test",
        content_hash="hash-mismatched",
        extracted_prompt_hash="hash-mismatched-p",
        extracted_prompt_preview="Please run mobile release audit and freeze champion",
        provenance_policy="code_block",
        is_fresh=True,
        transfer_eligible=True,
    ))

    old_db = app_main.db
    old_sm = app_main.state_machine
    app_main.db = test_db
    app_main.state_machine = ShuttleStateMachine(test_db)
    await app_main.state_machine.init()

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
            res = await client.get("/api/reconciliation")
            assert res.status_code == 200
            data = res.json()
            recon = data["reconciliation"]
            assert recon["match_status"] == "MISMATCH"
            assert recon["transfer_eligible"] is False
            assert "Iteration mismatch" in recon["ineligibility_reason"]
    finally:
        app_main.db = old_db
        app_main.state_machine = old_sm


@pytest.mark.asyncio
async def test_live_codex_target_session_verification():
    """
    Direct verification against the actual configured Codex session on the host machine.
    Target Thread: 00000000-0000-0000-0000-000000000001
    Expected CWD: D:/workspace/example-repo
    Expected Title: 重置跑胡子手机指导优先级
    """
    real_sessions_dir = Path.home() / ".codex" / "sessions"
    real_session_index = Path.home() / ".codex" / "session_index.jsonl"

    if not real_sessions_dir.exists() or not real_session_index.exists():
        pytest.skip("Codex user data directory not found on host")
    index_text = real_session_index.read_text(encoding="utf-8", errors="ignore")
    if "00000000-0000-0000-0000-000000000001" not in index_text:
        pytest.skip("Synthetic thread not in host session index")

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=real_sessions_dir,
        session_index_path=real_session_index,
    )

    thread_id = "00000000-0000-0000-0000-000000000001"
    obs = observer.observe_thread(
        thread_id=thread_id,
        expected_cwd="D:/workspace/example-repo",
        expected_title="重置跑胡子手机指导优先级",
    )

    assert obs["metadata_valid"] is True, f"Validation errors: {obs['validation_errors']}"
    assert obs["session_file_path"] is not None
    assert obs["total_lines"] > 0
    assert obs["is_completed"] is True or obs["is_in_progress"] is True
    if obs["is_completed"]:
        assert obs["latest_assistant_response"] is not None
        assert obs["latest_response_hash"] == compute_sha256(obs["latest_assistant_response"])
        assert obs["latest_turn_id"] is not None
    else:
        assert obs["latest_user_line"] is not None
        assert obs["latest_turn_id"] is not None


@pytest.mark.asyncio
async def test_api_chatgpt_and_codex_observation_endpoints(temp_dirs):
    """
    Test /api/codex/observe and /api/chatgpt/observe API endpoints with injected real payloads.
    """
    from app.main import app, db as main_db, state_machine as main_sm
    from app.main import codex_observer as main_codex_obs

    # Configure app with test db
    test_db = Database(str(temp_dirs["db_file"]))
    await test_db.init()

    # Save valid config into test db
    cfg = AppConfig(
        codex=CodexConfig(
            repo_path="D:/workspace/example-repo",
            expected_branch="codex/run_huzi_mobile_guidance",
            cli_path="C:/dummy/codex.exe",
            thread_id="00000000-0000-0000-0000-000000000001",
            thread_title="重置跑胡子手机指导优先级",
            sessions_dir=str(temp_dirs["codex_sessions"]),
            session_index_path=str(temp_dirs["session_index"]),
        ),
        chatgpt=ChatGPTConfig(
            browser_type="msedge",
            profile_dir=str(temp_dirs["root"] / "profile"),
            conversation_url="https://chatgpt.com/g/g-p-example/c/00000000-0000-0000-0000-000000000002",
            expected_gpt_id="g-p-example",
            expected_conversation_id="00000000-0000-0000-0000-000000000002",
        ),
        route=RouteConfig(),
    )
    await test_db.save_config(cfg)

    # Patch global app instances safely for test
    import app.main as app_main
    old_db = app_main.db
    old_sm = app_main.state_machine
    app_main.db = test_db
    app_main.state_machine = ShuttleStateMachine(test_db, cfg)
    await app_main.state_machine.init()

    app_main.codex_observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_dirs["codex_sessions"],
        session_index_path=temp_dirs["session_index"],
    )

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
            # 1. Test ChatGPT observation with simulated DOM payload containing valid Mission Prompt
            chatgpt_dom = {
                "url": cfg.chatgpt.conversation_url,
                "text": "ChatGPT 分析完成。",
                "canvas_text": "## Mission\nGoal A 专项测试执行指令：重置跑胡子指导权重。\n" + ("x" * 300),
                "code_blocks": [],
            }
            res_cg = await client.post("/api/chatgpt/observe", json={"injected_dom": chatgpt_dom})
            assert res_cg.status_code == 200
            data_cg = res_cg.json()
            assert data_cg["status"] == "ready"
            assert data_cg["latest_response"]["provenance_policy"] == "canvas"
            assert data_cg["extraction"]["transfer_eligible"] is True

            # Check state machine transition
            res_state = await client.get("/api/state")
            assert res_state.json()["stage"] == ShuttleStage.CHATGPT_RESPONSE_READY

            # 2. Test ChatGPT observation with '无法加载此回复' error placeholder
            error_dom = {
                "url": cfg.chatgpt.conversation_url,
                "text": "无法加载此回复",
                "canvas_text": "",
                "code_blocks": [],
            }
            res_err = await client.post("/api/chatgpt/observe", json={"injected_dom": error_dom})
            assert res_err.status_code == 200
            data_err = res_err.json()
            assert data_err["status"] == "waiting"

            # Check observations query endpoint
            res_obs = await client.get("/api/observations")
            assert res_obs.status_code == 200
            obs_json = res_obs.json()
            assert obs_json["chatgpt"] is not None
    finally:
        app_main.db = old_db
        app_main.state_machine = old_sm

