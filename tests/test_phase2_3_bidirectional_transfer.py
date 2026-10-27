"""
Phase 2.3 — Controlled Bidirectional Shuttle Implementation Test Suite.
Covers all 20 required scenarios:
1. Correct Codex thread and project identity
2. Stale or incomplete Codex responses (rejected)
3. Exact multiline payload preservation and hashing
4. Wrong ChatGPT URL and multiple matching pages (rejected)
5. Missing composer and writing-block/composer confusion (rejected)
6. Composer content mismatch (rejected)
7. Successful mocked send and delivery receipt
8. Browser disconnect before submission (aborted cleanly)
9. Disconnect or timeout after submission, producing unknown outcome without retry (DELIVERY_OUTCOME_UNKNOWN)
10. Stale assistant response rejection
11. Streaming and completed response differentiation
12. Error placeholders (rejected)
13. Invalid or absent executable prompt (ineligible)
14. Correct prompt extraction and provenance
15. Wrong Codex destination (rejected)
16. CLI rejection and timeout handling
17. Duplicate transfer rejection
18. Confirmation bound to exact payload hash (single-use constraint)
19. Restart recovery of in-progress and unknown transfers
20. Receipt verification and reconciliation flow (end-to-end 6 human-controlled operations)
"""
import asyncio
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import tempfile
import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters import (
    DeliveryOutcomeUnknownError,
    MockChatGPTMessageSender,
    MockCodexQueueAdapter,
    ReadOnlyCodexSessionObserver,
    RealChatGPTBrowserObserver,
    RealChatGPTMessageSender,
    RealCodexQueueAdapter,
    StandardPromptExtractor,
)
from app.chatgpt_dom import ChatGPTDOMParser
from app.database import Database
from app.models import (
    AppConfig,
    CodexConfig,
    ChatGPTConfig,
    ObservationRecord,
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
from app.main import app, db as main_db, state_machine as main_sm, real_chatgpt_sender, real_codex_queue

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "chatgpt"


def load_fixture(name: str) -> str:
    fp = FIXTURES_DIR / name
    with open(fp, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def temp_env():
    with tempfile.TemporaryDirectory() as base_tmp:
        tmp_path = Path(base_tmp)
        codex_dir = tmp_path / "codex_sessions"
        codex_dir.mkdir(parents=True, exist_ok=True)
        session_index = tmp_path / "session_index.jsonl"
        db_file = tmp_path / "test_phase2_3.db"
        yield {
            "root": tmp_path,
            "codex_sessions": codex_dir,
            "session_index": session_index,
            "db_file": db_file,
        }


def make_session_file(sessions_dir: Path, thread_id: str, lines: list) -> Path:
    fp = sessions_dir / f"session_{thread_id}.jsonl"
    with open(fp, "w", encoding="utf-8") as f:
        for item in lines:
            f.write(json.dumps(item) + "\n")
    return fp


# ---------------------------------------------------------------- Tests 1 to 5


@pytest.mark.asyncio
async def test_1_codex_thread_and_project_identity(temp_env):
    """1. Validate correct Codex thread & project identity; reject mismatched cwd or title."""
    thread_id = "00000000-0000-0000-0000-000000000001"
    expected_cwd = "D:/workspace/example-repo"
    expected_title = "Audit Priority Thread"

    with open(temp_env["session_index"], "w", encoding="utf-8") as f_idx:
        f_idx.write(json.dumps({
            "id": thread_id,
            "thread_name": expected_title,
            "cwd": expected_cwd,
            "updated_at": "2026-10-25T00:52:40.398Z"
        }) + "\n")

    events = [
        {"type": "session_meta", "payload": {"id": thread_id, "cwd": expected_cwd}},
        {"type": "turn_context", "payload": {"turn_id": "turn-001"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "output_text", "text": "Start audit"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": [{"type": "output_text", "text": "Audit completed."}]}},
    ]
    make_session_file(temp_env["codex_sessions"], thread_id, events)

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_env["codex_sessions"],
        session_index_path=temp_env["session_index"],
    )

    # Success with correct credentials
    rep = observer.prepare_report(thread_id, expected_cwd=expected_cwd, expected_title=expected_title)
    assert rep["thread_id"] == thread_id
    assert rep["report_text"] == "Audit completed."
    assert rep["content_hash"] == compute_sha256("Audit completed.")

    # Mismatched CWD rejected
    with pytest.raises(ValueError, match="CWD mismatch"):
        observer.prepare_report(thread_id, expected_cwd="D:/project/other_project")

    # Mismatched Title rejected
    with pytest.raises(ValueError, match="Title mismatch"):
        observer.prepare_report(thread_id, expected_cwd=expected_cwd, expected_title="Mismatched Title")


@pytest.mark.asyncio
async def test_2_stale_or_incomplete_codex_responses(temp_env):
    """2. Reject in-progress turn, truncated stream, or empty response."""
    thread_id = "00000000-0000-0000-0000-000000000001"
    with open(temp_env["session_index"], "w", encoding="utf-8") as f_idx:
        f_idx.write(json.dumps({
            "id": thread_id,
            "thread_name": "Test Thread",
            "cwd": "D:/project/test",
            "updated_at": "2026-10-25T00:52:40.398Z"
        }) + "\n")

    # User sent a new message after assistant reply -> in-progress
    events_in_progress = [
        {"type": "session_meta", "payload": {"id": thread_id, "cwd": "D:/project/test"}},
        {"type": "turn_context", "payload": {"turn_id": "turn-001"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "Q1"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": [{"type": "output_text", "text": "A1"}]}},
        {"type": "turn_context", "payload": {"turn_id": "turn-002"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "Q2 (in progress)"}]}},
    ]
    make_session_file(temp_env["codex_sessions"], thread_id, events_in_progress)

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_env["codex_sessions"],
        session_index_path=temp_env["session_index"],
    )
    with pytest.raises(ValueError, match="in progress"):
        observer.prepare_report(thread_id, expected_cwd="D:/project/test")

    # Empty assistant reply
    events_empty = [
        {"type": "session_meta", "payload": {"id": thread_id, "cwd": "D:/project/test"}},
        {"type": "turn_context", "payload": {"turn_id": "turn-001"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "Q1"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": [{"type": "output_text", "text": "   "}]}},
    ]
    make_session_file(temp_env["codex_sessions"], thread_id, events_empty)
    with pytest.raises(ValueError, match="empty|truncated"):
        observer.prepare_report(thread_id, expected_cwd="D:/project/test")


@pytest.mark.asyncio
async def test_3_exact_multiline_payload_preservation_and_hashing(temp_env):
    """3. Multiline payload with newlines, tables, code blocks must be preserved character-for-character."""
    multiline_text = (
        "# Goal A Checkpoint Report\n\n"
        "| Stage | Status | Tests |\n"
        "|---|---|---|\n"
        "| Phase 1 | Pass | 10/10 |\n"
        "| Phase 2 | Pass | 12/12 |\n\n"
        "`python\ndef verify():\n    return True\n`\n"
    )
    thread_id = "00000000-0000-0000-0000-000000000001"
    with open(temp_env["session_index"], "w", encoding="utf-8") as f_idx:
        f_idx.write(json.dumps({
            "id": thread_id,
            "thread_name": "Multiline Test",
            "cwd": "D:/project/test",
            "updated_at": "2026-10-25T00:52:40.398Z"
        }) + "\n")

    events = [
        {"type": "session_meta", "payload": {"id": thread_id, "cwd": "D:/project/test"}},
        {"type": "turn_context", "payload": {"turn_id": "turn-001"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "Report"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": [{"type": "output_text", "text": multiline_text}]}},
    ]
    make_session_file(temp_env["codex_sessions"], thread_id, events)

    observer = ReadOnlyCodexSessionObserver(
        sessions_base_dir=temp_env["codex_sessions"],
        session_index_path=temp_env["session_index"],
    )
    rep = observer.prepare_report(thread_id, expected_cwd="D:/project/test")
    assert rep["report_text"] == multiline_text
    assert rep["char_count"] == len(multiline_text)
    assert rep["content_hash"] == compute_sha256(multiline_text)
    assert "\n| Phase 1 | Pass |" in rep["report_text"]


@pytest.mark.asyncio
async def test_4_wrong_chatgpt_url_and_multiple_matching_pages(temp_env):
    """4. Reject send when conversation URL does not match or multiple matching pages open."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    sender = RealChatGPTMessageSender(target_url=target_url)

    wrong_page = {"url": "https://chatgpt.com/g/g-p-999/c/wrong-conv-id", "html": "<html></html>"}
    with pytest.raises(ValueError, match="Target URL mismatch"):
        await sender.send_message("Test message", page_override=wrong_page)

    ambiguous_page = {"multiple_pages_open": True, "url": target_url}
    with pytest.raises(ValueError, match="Ambiguous: multiple matching"):
        await sender.send_message("Test message", page_override=ambiguous_page)


@pytest.mark.asyncio
async def test_5_missing_composer_and_writing_block_confusion(temp_env):
    """5. Missing composer and writing-block / canvas confusion rejected."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    sender = RealChatGPTMessageSender(target_url=target_url)

    no_composer_html = "<html><body><article>Only message text</article></body></html>"
    with pytest.raises(ValueError, match="Message composer not found"):
        await sender.send_message("Test message", page_override={"url": target_url, "html": no_composer_html})

    writing_block_html = load_fixture("multiple_writing_blocks.html")
    with pytest.raises(ValueError, match="Rejected: writing block"):
        await sender.send_message(
            "Test message",
            page_override={"url": target_url, "html": writing_block_html, "simulate_writing_block": True}
        )


@pytest.mark.asyncio
async def test_6_composer_content_mismatch(temp_env):
    """6. Composer populated content mismatch character-for-character rejected before submit."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    sender = RealChatGPTMessageSender(target_url=target_url)
    html = load_fixture("current_completed.html")

    page_data = {
        "url": target_url,
        "html": html,
        "composer_content_mismatch": True,
        "simulate_read_back_text": "Different content than report",
    }
    with pytest.raises(ValueError, match="Composer text mismatch"):
        await sender.send_message("Expected original report\nLine 2", page_override=page_data)


@pytest.mark.asyncio
async def test_7_successful_mocked_send_and_delivery_receipt(temp_env):
    """7. Successful mocked send: single-use auth consumed, delivery receipt returned."""
    db = Database(temp_env["db_file"])
    await db.init()

    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    sender = RealChatGPTMessageSender(target_url=target_url, db=db)

    msg = "Report message payload to ChatGPT"
    msg_hash = compute_sha256(msg)
    transfer_id = "trans-c2g-test-001"
    token = "auth-token-c2g-001"

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=token,
        destination_identifier=target_url,
        expected_content_hash=msg_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=10),
    )
    await db.create_transfer_authorization(auth)

    html = load_fixture("current_completed.html")
    page_data = {"url": target_url, "html": html}

    res = await sender.send_message(
        msg,
        confirmation_token=token,
        transfer_id=transfer_id,
        baseline_hash="base-hash-001",
        page_override=page_data,
    )
    assert res["status"] == "delivered"
    assert res["content_hash"] == msg_hash
    assert res["delivery_evidence"]["user_turn_verified"] is True
    assert res["delivery_evidence"]["baseline_hash"] == "base-hash-001"

    consumed_auth = await db.get_transfer_authorization(token)
    assert consumed_auth.consumed is True

    with pytest.raises(ValueError, match="already been consumed"):
        await sender.send_message(
            msg,
            confirmation_token=token,
            transfer_id=transfer_id,
            page_override=page_data,
        )


# ---------------------------------------------------------------- Tests 8 to 14


@pytest.mark.asyncio
async def test_8_browser_disconnect_before_submission(temp_env):
    """8. Browser disconnect before submission aborts cleanly without submit."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    sender = RealChatGPTMessageSender(target_url=target_url)

    page_data = {
        "url": target_url,
        "disconnect_before_submit": True,
    }
    with pytest.raises(ConnectionError, match="disconnected before submission"):
        await sender.send_message("Test message", page_override=page_data)


@pytest.mark.asyncio
async def test_9_disconnect_or_timeout_after_submission_unknown_outcome(temp_env):
    """9. Disconnect or timeout after submit raises DeliveryOutcomeUnknownError without retry."""
    db = Database(temp_env["db_file"])
    await db.init()
    sm = ShuttleStateMachine(db)
    await sm.init()

    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    sender = RealChatGPTMessageSender(target_url=target_url, db=db)

    msg = "Payload that times out after submit"
    msg_hash = compute_sha256(msg)
    transfer_id = "trans-c2g-timeout-001"
    token = "auth-token-c2g-timeout"

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=token,
        destination_identifier=target_url,
        expected_content_hash=msg_hash,
        expires_at=datetime.utcnow() + timedelta(minutes=10),
    )
    await db.create_transfer_authorization(auth)

    html = load_fixture("current_completed.html")
    page_data = {
        "url": target_url,
        "html": html,
        "disconnect_after_submit": True,
    }

    with pytest.raises(DeliveryOutcomeUnknownError, match="delivery outcome unknown"):
        await sender.send_message(
            msg,
            confirmation_token=token,
            transfer_id=transfer_id,
            page_override=page_data,
        )

    await sm.transition_to(ShuttleStage.CODEX_RESPONSE_READY)
    await sm.transition_to(ShuttleStage.CHATGPT_SEND_PENDING)
    await sm.transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN, failure_details="Timeout mid-transfer")
    assert sm.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN

    assert sm.can_transition_to(ShuttleStage.CHATGPT_SEND_PENDING) is False
    assert sm.can_transition_to(ShuttleStage.CODEX_QUEUE_PENDING) is False


@pytest.mark.asyncio
async def test_10_stale_assistant_response_rejection(temp_env):
    """10. Reject stale assistant response when baseline matches or turn is historical."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    observer = RealChatGPTBrowserObserver(target_url=target_url)
    html = load_fixture("stale_reply_user_turn.html")

    observer.set_baseline(turn_index=1)
    res = observer.inspect_dom_payload({"url": target_url, "html": html})
    assert res["is_stale_assistant_reply"] is True
    assert res["transfer_eligible"] is False
    assert "user turn" in res["ineligibility_reason"].lower() or "stale" in res["ineligibility_reason"].lower()


@pytest.mark.asyncio
async def test_11_streaming_and_completed_response_differentiation(temp_env):
    """11. Distinguish streaming from completed responses."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    observer = RealChatGPTBrowserObserver(target_url=target_url)

    streaming_html = load_fixture("current_streaming.html")
    res_stream = observer.inspect_dom_payload({"url": target_url, "html": streaming_html})
    assert res_stream["is_streaming"] is True
    assert res_stream["is_completed"] is False
    assert res_stream["transfer_eligible"] is False

    completed_html = load_fixture("current_completed.html")
    res_comp = observer.inspect_dom_payload({"url": target_url, "html": completed_html})
    assert res_comp["is_streaming"] is False
    assert res_comp["is_completed"] is True
    assert res_comp["transfer_eligible"] is True


@pytest.mark.asyncio
async def test_12_error_placeholders(temp_env):
    """12. Detect and reject error placeholders (Cloudflare / Unable to load response)."""
    target_url = "https://chatgpt.com/g/g-p-123/c/target-conv-id"
    observer = RealChatGPTBrowserObserver(target_url=target_url)

    err_html = load_fixture("error_placeholder.html")
    res = observer.inspect_dom_payload({"url": target_url, "html": err_html})
    assert res["has_error_placeholder"] is True
    assert res["transfer_eligible"] is False
    assert "Error placeholder detected" in res["ineligibility_reason"]


@pytest.mark.asyncio
async def test_13_invalid_or_absent_executable_prompt(temp_env):
    """13. Ordinary discussion without structured executable prompt is ineligible for transfer."""
    extractor = StandardPromptExtractor()
    chat_response = {
        "text": "Hello! How can I assist you today? Let me know what you want to work on.",
        "canvas_text": "",
        "code_blocks": [],
    }
    prompt = extractor.extract(chat_response)
    assert prompt is None


@pytest.mark.asyncio
async def test_14_correct_prompt_extraction_and_provenance(temp_env):
    """14. Extract valid structured prompt with exact text, hash, and provenance policy."""
    extractor = StandardPromptExtractor()
    raw_prompt_body = "# Goal A: Resume HuZi Priority Audit\n\n1. Run test suite\n2. Verify lookahead\n" + ("3. Step validation item\n" * 15)
    code_block_fenced = f"```markdown\n{raw_prompt_body}\n```"
    response_parts = {
        "text": f"Here is the execution prompt:\n{code_block_fenced}\nPlease execute.",
        "canvas_text": "",
        "code_blocks": [code_block_fenced],
    }
    prompt, prov = extractor.extract_with_provenance(response_parts)
    assert prompt is not None
    assert "Goal A: Resume HuZi Priority Audit" in prompt
    assert prov == "code_block"
    assert compute_sha256(prompt) is not None


# ---------------------------------------------------------------- Tests 15 to 20


@pytest.mark.asyncio
async def test_15_wrong_codex_destination(temp_env):
    """15. Queue submission to empty or invalid thread rejected."""
    adapter = RealCodexQueueAdapter(default_repo_path=str(temp_env["root"]))
    with pytest.raises(ValueError, match="Thread ID is required"):
        await adapter.queue_message(thread_id="", message="Valid prompt")


@pytest.mark.asyncio
async def test_16_cli_rejection_and_timeout_handling(temp_env):
    """16. Subprocess non-zero exit code and timeout handled cleanly without crashing."""
    adapter = RealCodexQueueAdapter(
        cli_path="codex",
        default_repo_path=str(temp_env["root"]),
    )

    # Injected runner simulates CLI failure
    async def mock_fail_runner(cmd, thread_id, message, repo, transfer_id):
        raise RuntimeError("Codex CLI queue returned non-zero code 1: session not found")

    adapter.set_injected_runner(mock_fail_runner)
    with pytest.raises(RuntimeError, match="non-zero code 1"):
        await adapter.queue_message(thread_id="00000000-0000-0000-0000-000000000001", message="Test")

    # Injected runner simulates timeout
    async def mock_timeout_runner(cmd, thread_id, message, repo, transfer_id):
        raise TimeoutError("Codex CLI queue execution timed out after 30 seconds")

    adapter.set_injected_runner(mock_timeout_runner)
    with pytest.raises(TimeoutError, match="timed out"):
        await adapter.queue_message(thread_id="00000000-0000-0000-0000-000000000001", message="Test")


@pytest.mark.asyncio
async def test_17_duplicate_transfer_rejection(temp_env):
    """17. Duplicate transfer of identical payload rejected."""
    db = Database(temp_env["db_file"])
    await db.init()
    sm = ShuttleStateMachine(db)
    await sm.init()

    msg = "Unique payload for duplicate test"
    content_hash = compute_sha256(msg)

    await db.record_transfer(
        iteration_id=1,
        direction="codex_to_chatgpt",
        source_id="codex",
        dest_id="chatgpt",
        content_hash=content_hash,
        payload_preview=msg[:50],
        status="sent",
    )

    is_dup, dup_msg = await sm.check_duplicate(msg)
    assert is_dup is True
    assert "Duplicate detected" in dup_msg


@pytest.mark.asyncio
async def test_18_confirmation_bound_to_exact_payload_hash(temp_env):
    """18. Tampering with payload after confirmation token generation rejected."""
    db = Database(temp_env["db_file"])
    await db.init()

    transfer_id = "trans-tamper-001"
    token = "auth-token-tamper-001"
    original_text = "Original authentic message"
    hash_orig = compute_sha256(original_text)

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=token,
        destination_identifier="dest-target",
        expected_content_hash=hash_orig,
        expires_at=datetime.utcnow() + timedelta(minutes=10),
    )
    await db.create_transfer_authorization(auth)

    tampered_text = "Tampered unauthorized message"
    hash_tampered = compute_sha256(tampered_text)

    ok, err = await db.verify_and_consume_authorization(
        transfer_id=transfer_id,
        token=token,
        content_hash=hash_tampered,
        destination="dest-target",
    )
    assert ok is False
    assert "Content hash mismatch" in err

    ok_valid, err_valid = await db.verify_and_consume_authorization(
        transfer_id=transfer_id,
        token=token,
        content_hash=hash_orig,
        destination="dest-target",
    )
    assert ok_valid is True
    assert err_valid is None

    ok_reuse, err_reuse = await db.verify_and_consume_authorization(
        transfer_id=transfer_id,
        token=token,
        content_hash=hash_orig,
        destination="dest-target",
    )
    assert ok_reuse is False
    assert "already been consumed" in err_reuse


@pytest.mark.asyncio
async def test_19_restart_recovery_of_transfers(temp_env):
    """19. SQLite persistence preserves transfers and unknown delivery state across restart."""
    db1 = Database(temp_env["db_file"])
    await db1.init()

    tid = "trans-persist-001"
    rec = TransferRecord(
        transfer_id=tid,
        iteration_id=2,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        source_type="codex",
        source_identifier="thread-uuid-1",
        destination_type="chatgpt",
        destination_identifier="https://chatgpt.com/test",
        content_hash=compute_sha256("persisted text"),
        char_count=len("persisted text"),
        message_text="persisted text",
        payload_preview="persisted text",
        status=TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value,
        error_message="Network dropped mid-transfer",
        delivery_evidence={"retry_count": 0, "attempted_at": "2026-10-25T12:00:00Z"},
    )
    await db1.save_transfer_record(rec)

    db2 = Database(temp_env["db_file"])
    await db2.init()

    recovered = await db2.get_transfer_by_id(tid)
    assert recovered is not None
    assert recovered.transfer_id == tid
    assert recovered.status == TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value
    assert recovered.error_message == "Network dropped mid-transfer"
    assert recovered.delivery_evidence["attempted_at"] == "2026-10-25T12:00:00Z"


@pytest.mark.asyncio
async def test_20_receipt_verification_and_reconciliation_flow(temp_env):
    """20. End-to-end integration of all 6 human-controlled transfer and reconciliation operations."""
    thread_id = "00000000-0000-0000-0000-000000000001"
    expected_cwd = str(temp_env["root"])
    expected_title = "E2E Shuttle Test"

    with open(temp_env["session_index"], "w", encoding="utf-8") as f_idx:
        f_idx.write(json.dumps({
            "id": thread_id,
            "thread_name": expected_title,
            "cwd": expected_cwd,
            "updated_at": "2026-10-25T00:52:40.398Z"
        }) + "\n")

    report_text = "# Phase 2.3 Report\nAll unit tests passed.\nReady for review."
    events = [
        {"type": "session_meta", "payload": {"id": thread_id, "cwd": expected_cwd}},
        {"type": "turn_context", "payload": {"turn_id": "turn-e2e-001"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "Generate report"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "status": "completed", "content": [{"type": "output_text", "text": report_text}]}},
    ]
    make_session_file(temp_env["codex_sessions"], thread_id, events)

    test_db = Database(str(temp_env["db_file"]))
    await test_db.init()

    import app.main as app_main
    old_db = app_main.db
    old_sm = app_main.state_machine
    old_cg_obs = app_main.real_chatgpt_observer
    old_cg_sender = app_main.real_chatgpt_sender
    old_cx_queue = app_main.real_codex_queue
    old_cx_obs = app_main.codex_observer

    app_main.db = test_db
    app_main.state_machine = ShuttleStateMachine(test_db)
    await app_main.state_machine.init()

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Configure app
            cfg = AppConfig(
                codex=CodexConfig(
                    repo_path=expected_cwd,
                    expected_branch="master",
                    cli_path="codex",
                    thread_id=thread_id,
                    thread_title=expected_title,
                    sessions_dir=str(temp_env["codex_sessions"]),
                    session_index_path=str(temp_env["session_index"]),
                ),
                chatgpt=ChatGPTConfig(
                    conversation_url="https://chatgpt.com/g/g-p-test/c/e2e-conv",
                    expected_gpt_id="g-p-test",
                    expected_conversation_id="e2e-conv",
                    profile_dir=str(Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data"),
                ),
                route=RouteConfig(),
            )
            r_cfg = await client.post("/api/config", json=cfg.model_dump(mode="json"))
            assert r_cfg.status_code == 200

            # Step 1: Prepare Codex report
            r_prep = await client.post("/api/transfer/codex_report/prepare", json={
                "thread_id": thread_id,
                "expected_cwd": expected_cwd,
                "expected_title": expected_title,
                "iteration_id": 1,
            })
            assert r_prep.status_code == 200
            prep_data = r_prep.json()
            assert prep_data["status"] == "prepared"
            c2g_tid = prep_data["transfer_id"]
            c2g_token = prep_data["confirmation_token"]
            assert prep_data["report_text"] == report_text

            # Step 2: Human confirms send to ChatGPT (with injected page fixture)
            page_fixture = {
                "url": "https://chatgpt.com/g/g-p-test/c/e2e-conv",
                "html": load_fixture("current_completed.html"),
            }
            r_send = await client.post("/api/transfer/chatgpt/send", json={
                "transfer_id": c2g_tid,
                "confirmation_token": c2g_token,
                "page_override": page_fixture,
            })
            assert r_send.status_code == 200
            send_data = r_send.json()
            assert send_data["status"] == "delivered"

            # Step 3: Observe ChatGPT response
            r_obs = await client.post("/api/transfer/chatgpt/observe", json={
                "page_data": page_fixture,
            })
            assert r_obs.status_code == 200
            obs_data = r_obs.json()
            assert obs_data["status"] == "ready"

            # Step 4: Prepare Codex prompt (operator edits prompt)
            edited_prompt = "```markdown\n# Phase 2.3 Directive\nRun verification suite.\n```"
            r_prompt = await client.post("/api/transfer/codex_prompt/prepare", json={
                "prompt_text": edited_prompt,
                "is_operator_modified": True,
                "iteration_id": 1,
            })
            assert r_prompt.status_code == 200
            prompt_data = r_prompt.json()
            assert prompt_data["status"] == "prepared"
            g2c_tid = prompt_data["transfer_id"]
            g2c_token = prompt_data["confirmation_token"]
            assert prompt_data["is_operator_modified"] is True
            assert prompt_data["content_hash"] == compute_sha256(edited_prompt)

            # Inject runner for Codex queue to avoid live CLI in test
            async def mock_queue_run(cmd, target_thread, message, repo, transfer_id):
                return {
                    "status": "queued",
                    "receipt_id": "queue-receipt-e2e-001",
                    "thread_id": target_thread,
                    "content_hash": compute_sha256(message),
                    "timestamp": datetime.utcnow().isoformat(),
                }

            if app_main.real_codex_queue:
                app_main.real_codex_queue.set_injected_runner(mock_queue_run)

            # Step 5: Submit Codex queue
            r_queue = await client.post("/api/transfer/codex_queue/submit", json={
                "transfer_id": g2c_tid,
                "confirmation_token": g2c_token,
                "thread_id": thread_id,
                "repo_path": expected_cwd,
            })
            assert r_queue.status_code == 200
            queue_data = r_queue.json()
            assert queue_data["status"] == "delivered"

            # Step 6: Test Reconcile Unknown Outcome
            unknown_tid = "trans-unknown-test-999"
            unknown_rec = TransferRecord(
                transfer_id=unknown_tid,
                iteration_id=99,
                direction=TransferDirection.CODEX_TO_CHATGPT.value,
                source_type="codex",
                source_identifier=thread_id,
                destination_type="chatgpt",
                destination_identifier="https://chatgpt.com/test",
                content_hash=compute_sha256("timeout text"),
                char_count=len("timeout text"),
                message_text="timeout text",
                payload_preview="timeout text",
                status=TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value,
                error_message="Simulated network disconnect",
            )
            await test_db.save_transfer_record(unknown_rec)
            await app_main.state_machine.transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN, failure_details="Timeout")

            r_recon = await client.post("/api/transfer/reconcile_unknown", json={
                "transfer_id": unknown_tid,
                "resolution": "mark_delivered",
                "notes": "Operator verified message manually in browser",
            })
            assert r_recon.status_code == 200
            recon_data = r_recon.json()
            assert recon_data["status"] == "reconciled"

            assert app_main.state_machine.stage == ShuttleStage.CHATGPT_WAITING
    finally:
        app_main.db = old_db
        app_main.state_machine = old_sm
        app_main.real_chatgpt_observer = old_cg_obs
        app_main.real_chatgpt_sender = old_cg_sender
        app_main.real_codex_queue = old_cx_queue
        app_main.codex_observer = old_cx_obs

