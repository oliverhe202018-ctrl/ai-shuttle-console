"""
Phase 2.2 — ChatGPT Current Layout Compatibility Repair Regression Test Suite.
Covers all 12 required scenarios adapted from Oracle PR #525 / Issue #517.
Reference: https://github.com/steipete/oracle/pull/525
"""
import os
from pathlib import Path
import pytest

from app.adapters import RealChatGPTBrowserObserver, StandardPromptExtractor
from app.chatgpt_dom import ChatGPTDOMParser, is_error_placeholder
from app.models import compute_sha256

FIX_DIR = Path(__file__).parent / "fixtures" / "chatgpt"
TARGET_URL = "https://chatgpt.com/g/g-p-example/c/00000000-0000-0000-0000-000000000002"
GPT_ID = "g-p-example"
CONV_ID = "00000000-0000-0000-0000-000000000002"


def load_fixture(name: str) -> str:
    fp = FIX_DIR / name
    with open(fp, "r", encoding="utf-8") as f:
        return f.read()


@pytest.mark.asyncio
async def test_1_legacy_chatgpt_layout():
    """1. Legacy ChatGPT layout compatibility."""
    html = load_fixture("legacy_completed.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["layout"] == "legacy"
    assert res["turns_count"] == 2
    assert res["is_completed"] is True
    assert res["is_streaming"] is False
    assert res["has_error_placeholder"] is False
    assert res["transfer_eligible"] is True
    assert res["composer"]["found"] is True
    assert res["composer"]["selector"] == "#prompt-textarea"
    assert res["composer"]["is_textarea"] is True
    assert res["extracted_prompt"] is not None
    assert res["provenance_policy"] in ("structured_markdown", "code_block")

    # Read-only async observer contract via injected page data
    observer.set_injected_page_data({"url": TARGET_URL, "html": html})
    status = await observer.get_browser_status()
    assert status["ready"] is True
    latest = await observer.get_latest_assistant_response()
    assert latest is not None
    assert latest["layout"] == "legacy"


@pytest.mark.asyncio
async def test_2_current_chatgpt_work_layout():
    """2. Current Chat/Work layout (2026-09+)."""
    html = load_fixture("current_completed.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["layout"] == "current"
    assert res["turns_count"] == 2
    assert res["is_streaming"] is False
    assert res["is_completed"] is True
    assert res["transfer_eligible"] is True
    assert res["composer"]["found"] is True
    assert res["composer"]["selector"] == 'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"]'
    assert res["composer"]["is_writing_block"] is False
    assert res["composer"]["is_content_editable"] is True
    assert res["extracted_prompt"] is not None


@pytest.mark.asyncio
async def test_3_both_current_marker_families():
    """3. Verifies feature detection for both Chat/Work marker families."""
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    # Family A: content-search-unit-key
    html_a = load_fixture("family_content_search_only.html")
    res_a = observer.inspect_dom_payload({"url": TARGET_URL, "html": html_a})
    assert res_a["layout"] == "current"
    assert res_a["turns_count"] == 2
    assert res_a["turns"][0]["role"] == "user"
    assert res_a["turns"][1]["role"] == "assistant"
    assert res_a["is_completed"] is True

    # Family B: chatgpt-search-unit-key
    html_b = load_fixture("family_chatgpt_search_only.html")
    res_b = observer.inspect_dom_payload({"url": TARGET_URL, "html": html_b})
    assert res_b["layout"] == "current"
    assert res_b["turns_count"] == 2
    assert res_b["turns"][0]["role"] == "user"
    assert res_b["turns"][1]["role"] == "assistant"
    assert res_b["is_completed"] is True


@pytest.mark.asyncio
async def test_4_multiple_editable_writing_blocks_and_genuine_composer():
    """4. Verifies isolation of genuine composer from multiple editable writing blocks."""
    html = load_fixture("multiple_writing_blocks.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["layout"] == "current"
    assert res["composer"]["found"] is True
    assert res["composer"]["is_writing_block"] is False
    assert res["composer"]["is_within_composer_form"] is True
    assert res["composer"]["selector"] == 'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"]'
    assert "genuine chat composer content" in res["composer"]["text"].lower()
    # Canvas text must not pollute the composer
    assert "canvas writing block" not in res["composer"]["text"]


@pytest.mark.asyncio
async def test_5_completed_assistant_replies():
    """5. Completed assistant replies requiring verified completion proof."""
    html_current = load_fixture("current_completed.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res_curr = observer.inspect_dom_payload({"url": TARGET_URL, "html": html_current})
    assert res_curr["is_completed"] is True
    assert res_curr["is_streaming"] is False
    assert res_curr["latest_assistant_turn"]["code_blocks"] is not None
    assert res_curr["content_hash"] is not None
    assert res_curr["extracted_prompt_hash"] is not None


@pytest.mark.asyncio
async def test_6_streaming_assistant_replies():
    """6. Streaming assistant replies rejected until streaming stops."""
    html = load_fixture("current_streaming.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["is_streaming"] is True
    assert res["is_completed"] is False
    assert res["transfer_eligible"] is False
    assert "streaming" in res["ineligibility_reason"].lower() or "thinking" in res["ineligibility_reason"].lower()


@pytest.mark.asyncio
async def test_7_stale_previous_reply_followed_by_new_user_message():
    """7. Stale previous reply followed by new user message rejected."""
    html = load_fixture("stale_reply_user_turn.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["is_stale_assistant_reply"] is True
    assert res["transfer_eligible"] is False
    assert "user turn" in res["ineligibility_reason"].lower() or "in-progress" in res["ineligibility_reason"].lower()


@pytest.mark.asyncio
async def test_8_error_placeholders():
    """8. Error placeholders detection and rejection."""
    html = load_fixture("error_placeholder.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["has_error_placeholder"] is True
    assert res["transfer_eligible"] is False
    assert "Error placeholder detected" in res["ineligibility_reason"]


@pytest.mark.asyncio
async def test_9_copy_buttons_inside_user_messages_code_blocks_and_writing_blocks():
    """9. Scoped completion controls: unscoped copy buttons do not certify completion."""
    html = load_fixture("copy_buttons_scoped.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    res = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res["is_completed"] is False
    assert res["transfer_eligible"] is False
    assert "Completion proof missing" in res["ineligibility_reason"]


@pytest.mark.asyncio
async def test_10_wrong_conversation_url_and_unresolved_layout():
    """10. Wrong conversation URL and unresolved Chat/Work mode."""
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    # Case A: Mismatched URL
    html_good = load_fixture("current_completed.html")
    res_wrong_url = observer.inspect_dom_payload({"url": "https://chatgpt.com/wrong-chat", "html": html_good})
    assert res_wrong_url["url_validation"]["matches_target"] is False
    assert res_wrong_url["transfer_eligible"] is False
    assert "does not match target" in res_wrong_url["ineligibility_reason"]

    # Case B: Unresolved layout
    html_unresolved = load_fixture("wrong_url_unresolved.html")
    res_unresolved = observer.inspect_dom_payload({"url": TARGET_URL, "html": html_unresolved})
    assert res_unresolved["layout"] == "unresolved"
    assert res_unresolved["transfer_eligible"] is False
    assert "Unresolved ChatGPT DOM layout" in res_unresolved["ineligibility_reason"]


@pytest.mark.asyncio
async def test_11_restart_persistence_and_historical_content_hash_rejection():
    """11. Restart persistence and historical content-hash rejection."""
    html = load_fixture("current_completed.html")
    observer = RealChatGPTBrowserObserver(target_url=TARGET_URL)

    # 1. First observation is eligible
    res1 = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res1["transfer_eligible"] is True
    base_hash = res1["content_hash"]

    # 2. Record baseline hash (simulating restart recovery or new submission)
    observer.set_baseline(content_hash=base_hash)

    # 3. Identical payload reobserved must be rejected as stale
    res2 = observer.inspect_dom_payload({"url": TARGET_URL, "html": html})
    assert res2["is_stale_assistant_reply"] is True
    assert res2["transfer_eligible"] is False
    assert "matches baseline hash" in res2["ineligibility_reason"]

    # 4. Verify record_baseline helper also sets turn index and message ID
    observer2 = RealChatGPTBrowserObserver(target_url=TARGET_URL)
    observer2.record_baseline({"url": TARGET_URL, "html": html})
    assert observer2.baseline_turn_index == 1
    assert observer2.baseline_hash == base_hash


@pytest.mark.asyncio
async def test_12_multiline_composer_identification_and_content_integrity():
    """12. Multiline composer identification and content-integrity validation."""
    html = load_fixture("multiline_composer.html")
    parsed = ChatGPTDOMParser.parse(html)

    comp = parsed["composer"]
    assert comp["found"] is True
    assert comp["multiline"] is True
    assert comp["is_within_composer_form"] is True
    assert comp["is_content_editable"] is True

    text = comp["text"]
    assert "Line 1 of complex prompt" in text
    assert "Line 2 with configuration rules" in text
    assert "Line 3 with final instructions" in text
    assert "\n" in text
    lines = text.split("\n")
    assert len(lines) >= 3
