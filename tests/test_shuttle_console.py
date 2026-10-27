"""
Automated Test Suite for Local AI Shuttle Control Center (Phase 1).
Covers:
- Configuration validation and persistence
- Checkpoint persistence and restart recovery
- Valid and invalid state transitions
- Duplicate transfer detection
- Message hashing
- Correct thread identity matching
- JSONL parsing with historical and unrelated messages
- Browser blocked/error states using mocks
- Refusal to retry an unknown delivery outcome
- Strict isolation from external side effects
"""
import json
from pathlib import Path
import shutil
import tempfile
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.adapters import (
    MockChatGPTBrowserObserver,
    MockChatGPTMessageSender,
    MockCodexQueueAdapter,
    ReadOnlyCodexSessionObserver,
    StandardPromptExtractor,
    is_error_placeholder,
)
from app.database import Database
from app.main import app
from app.models import (
    AppConfig,
    CheckpointRecord,
    CodexConfig,
    ChatGPTConfig,
    RouteConfig,
    ShuttleStage,
    compute_sha256,
)
from app.state_machine import InvalidTransitionError, ShuttleStateMachine


@pytest.fixture
def tmp_dir():
    d = Path(tempfile.mkdtemp(prefix="test-shuttle-console-"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest_asyncio.fixture
async def test_db(tmp_dir):
    db_file = tmp_dir / "test_shuttle.db"
    db = Database(db_file)
    await db.init()
    return db


# ---------------------------------------------------------------- Test Cases


@pytest.mark.asyncio
async def test_1_configuration_validation_and_persistence(test_db):
    """1. 验证配置校验 (Pydantic models) 与数据库持久化。"""
    config = AppConfig(
        codex=CodexConfig(
            repo_path="D:/workspace/example-repo",
            expected_branch="synthetic-production-branch",
            cli_path="C:/Codex/codex.exe",
            thread_id="00000000-0000-0000-0000-000000000001",
            thread_title="跑胡子指导",
            sessions_dir="C:/Users/test/.codex/sessions",
            integration_mode="mock",
        ),
        chatgpt=ChatGPTConfig(
            browser_type="msedge",
            profile_dir="D:/project/profile",
            conversation_url="https://chatgpt.com/g/g-test/c/conv-1234",
            expected_gpt_id="g-test",
            expected_conversation_id="conv-1234",
        ),
        route=RouteConfig(),
    )

    await test_db.save_config(config)
    loaded = await test_db.get_config()

    assert loaded is not None
    assert loaded.codex.thread_id == "00000000-0000-0000-0000-000000000001"
    assert loaded.chatgpt.conversation_url == "https://chatgpt.com/g/g-test/c/conv-1234"
    assert loaded.route.manual_approval_required is True

    # 测试非法配置校验：无效 URL
    with pytest.raises(ValueError):
        ChatGPTConfig(
            browser_type="msedge",
            profile_dir="D:/project/profile",
            conversation_url="ftp://invalid-url.com",
        )


@pytest.mark.asyncio
async def test_2_checkpoint_persistence_and_restart_recovery(test_db):
    """2. 检查点在 SQLite 中的持久化与重启恢复。"""
    sm = ShuttleStateMachine(test_db)
    await sm.init()
    assert sm.stage == ShuttleStage.IDLE

    # 推进到 REPORT_PREVIEW
    await sm.transition_to(
        ShuttleStage.REPORT_PREVIEW,
        iteration_id=1,
        source_id="thread:00000000",
        dest_id="chatgpt",
        content_hash="hash-12345",
        payload_preview="Report preview content",
        transfer_status="pending",
    )
    assert sm.stage == ShuttleStage.REPORT_PREVIEW

    # 模拟应用重启：实例化新的状态机
    restarted_sm = ShuttleStateMachine(test_db)
    await restarted_sm.init()
    assert restarted_sm.stage == ShuttleStage.REPORT_PREVIEW
    assert restarted_sm.current_checkpoint.iteration_id == 1
    assert restarted_sm.current_checkpoint.content_hash == "hash-12345"


@pytest.mark.asyncio
async def test_3_valid_and_invalid_state_transitions(test_db):
    """3. 合法状态跃迁与非法倒退拦截。"""
    sm = ShuttleStateMachine(test_db)
    await sm.init()

    # 合法跃迁 IDLE -> CODEX_WAITING
    assert sm.can_transition_to(ShuttleStage.CODEX_WAITING) is True
    await sm.transition_to(ShuttleStage.CODEX_WAITING)

    # 非法跃迁 CODEX_WAITING -> CHATGPT_RESPONSE_READY (越级)
    assert sm.can_transition_to(ShuttleStage.CHATGPT_RESPONSE_READY) is False
    with pytest.raises(InvalidTransitionError):
        await sm.transition_to(ShuttleStage.CHATGPT_RESPONSE_READY)

    # 合法跃迁 CODEX_WAITING -> CODEX_RESPONSE_READY
    await sm.transition_to(ShuttleStage.CODEX_RESPONSE_READY)
    assert sm.stage == ShuttleStage.CODEX_RESPONSE_READY


@pytest.mark.asyncio
async def test_4_duplicate_transfer_detection(test_db):
    """4. 基于内容 Hash 的重复发送拦截。"""
    sm = ShuttleStateMachine(test_db)
    await sm.init()

    content = "# Audit Report\nFully passed test suite."
    h = compute_sha256(content)

    # 首次检查，无重复
    is_dup, _ = await sm.check_duplicate(content)
    assert is_dup is False

    # 记录该内容已发送
    await test_db.record_transfer(
        iteration_id=1,
        direction="codex_to_chatgpt",
        source_id="codex",
        dest_id="chatgpt",
        content_hash=h,
        payload_preview=content[:50],
        status="sent",
    )

    # 再次检查，立即识别出重复
    is_dup2, msg = await sm.check_duplicate(content)
    assert is_dup2 is True
    assert "Duplicate detected" in msg


def test_5_message_hashing():
    """5. 确定性 SHA-256 消息指纹计算。"""
    sample_text = "Standard Codex Final Answer Report"
    h1 = compute_sha256(sample_text)
    h2 = compute_sha256(sample_text)
    assert h1 == h2
    assert len(h1) == 64
    assert h1 == "2cea02dbeba3e0e3cb2e0f0fe7b4f3028943a1378ee2ab94b0d7675288eb779e"
    # 变动文本测试
    h3 = compute_sha256(sample_text + " modified")
    assert h3 != h1


@pytest.mark.asyncio
async def test_6_correct_thread_identity_matching_and_jsonl_parsing(tmp_dir):
    """6. 验证 JSONL 文件定位与多 turn 混杂时正确匹配目标 thread 和 turn。"""
    sessions_dir = tmp_dir / "sessions"
    sessions_dir.mkdir()

    thread_target = "00000000-0000-0000-0000-000000000001"
    thread_other = "99999999-0000-0000-0000-000000000000"

    # 生成包含目标 thread 的会话文件
    target_file = sessions_dir / f"rollout-{thread_target}.jsonl"
    records = [
        {"type": "turn_context", "payload": {"turn_id": "turn-old"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": "Old History"}},
        {"type": "turn_context", "payload": {"turn_id": "turn-target"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "New prompt"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": [{"type": "output_text", "text": "Target New Final Answer"}]}},
    ]
    with open(target_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    # 生成无关 thread 的会话文件
    other_file = sessions_dir / f"rollout-{thread_other}.jsonl"
    with open(other_file, "w", encoding="utf-8") as f:
        f.write(json.dumps({"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": "Unrelated Thread"}}) + "\n")

    observer = ReadOnlyCodexSessionObserver(sessions_dir)

    # 1. 精确匹配文件
    matched_path = observer.locate_session_file(thread_target)
    assert matched_path == target_file

    # 2. 从指定 offset 过滤旧回复并提取目标 final_answer
    ans, idx, turn_id = await observer.get_latest_response(thread_target, after_line=2)
    assert ans == "Target New Final Answer"
    assert idx == 4
    assert turn_id == "turn-target"


@pytest.mark.asyncio
async def test_7_browser_blocked_and_error_states():
    """7. 模拟 Cloudflare 拦截与报错占位符过滤。"""
    browser_obs = MockChatGPTBrowserObserver()

    # 初始就绪态
    st = await browser_obs.get_browser_status()
    assert st["ready"] is True
    assert st["human_verification_required"] is False

    # 模拟被 Cloudflare 验证拦截
    browser_obs.is_blocked = True
    st_blocked = await browser_obs.get_browser_status()
    assert st_blocked["human_verification_required"] is True
    assert "Cloudflare" in st_blocked["interstitial_reason"]

    # 测试错误占位符识别
    assert is_error_placeholder("无法加载此回复") is True
    assert is_error_placeholder("Something went wrong, please try reloading the conversation.") is True
    assert is_error_placeholder("正常助手回复正文") is False

    # 确保 prompt extractor 拒绝错误占位符
    extractor = StandardPromptExtractor()
    bad_parts = {"text": "无法加载此回复", "code_blocks": ["无法加载此回复"], "canvas_text": ""}
    assert extractor.extract(bad_parts) is None


@pytest.mark.asyncio
async def test_8_refusal_to_retry_unknown_delivery_outcome(test_db):
    """8. 发生网络中断导致投递结果未知时，进入 DELIVERY_OUTCOME_UNKNOWN 并不自动重发。"""
    sm = ShuttleStateMachine(test_db)
    await sm.init()

    # 进入 CHATGPT_SEND_PENDING
    await sm.transition_to(ShuttleStage.REPORT_PREVIEW)
    await sm.transition_to(ShuttleStage.CHATGPT_SEND_PENDING)

    # 模拟中途断网抛出 DELIVERY_OUTCOME_UNKNOWN
    await sm.transition_to(
        ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
        failure_details="Connection reset while sending message. Browser state indeterminate.",
        transfer_status="unknown",
    )
    assert sm.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN

    # 严格检验：状态机禁止自动跃迁回 CHATGPT_SEND_PENDING 或 CODEX_QUEUE_PENDING
    assert sm.can_transition_to(ShuttleStage.CHATGPT_SEND_PENDING) is False
    assert sm.can_transition_to(ShuttleStage.CODEX_QUEUE_PENDING) is False

    # 仅允许人工干预阶段 (HUMAN_ACTION_REQUIRED) 或重置
    assert sm.can_transition_to(ShuttleStage.HUMAN_ACTION_REQUIRED) is True


@pytest.mark.asyncio
async def test_9_no_real_external_side_effects():
    """9. 确保 Mock 适配器在测试与 Phase 1 阶段绝不调用真实外部网络或子进程。"""
    queue_mock = MockCodexQueueAdapter()
    send_mock = MockChatGPTMessageSender()

    res_q = await queue_mock.queue_message("target-thread-123", "prompt test")
    assert res_q["status"] == "queued_mock"
    assert len(queue_mock.queued_messages) == 1

    res_s = await send_mock.send_message("report test")
    assert res_s["status"] == "delivered_mock"
    assert len(send_mock.sent_messages) == 1


@pytest.mark.asyncio
async def test_10_api_endpoints_integration(test_db):
    """10. FastAPI 端点集成测试 (/api/state, /api/preview/hash, /api/stage/transition)。"""
    # 注入测试数据库
    from app import main
    main.db = test_db
    main.state_machine = ShuttleStateMachine(test_db)
    await main.state_machine.init()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
        # 1. 检查状态
        res = await client.get("/api/state")
        assert res.status_code == 200
        data = res.json()
        assert data["stage"] == "IDLE"

        # 2. 计算预览 Hash
        preview_res = await client.post("/api/preview/hash", json={"text": "Integration test report"})
        assert preview_res.status_code == 200
        p_data = preview_res.json()
        assert "sha256" in p_data
        assert p_data["length_chars"] == 23

        # 3. 触发合法状态跃迁
        trans_res = await client.post(
            "/api/stage/transition",
            json={"target_stage": "CODEX_WAITING", "reason": "Test transition"},
        )
        assert trans_res.status_code == 200
        assert trans_res.json()["stage"] == "CODEX_WAITING"

        # 4. 触发非法状态跃迁 (被 400 拦截)
        bad_trans_res = await client.post(
            "/api/stage/transition",
            json={"target_stage": "CHATGPT_RESPONSE_READY", "reason": "Illegal jump"},
        )
        assert bad_trans_res.status_code == 400
