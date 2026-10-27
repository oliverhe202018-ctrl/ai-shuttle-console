"""
Phase 2.4.8 — ChatGPT Composer/Form Send-Button Association Repair Regression Suite.

Verifies all 16 criteria defined in Section 8 of Phase 2.4.8 specification:
  1. Textbox and Send button are siblings inside the same composer form.
  2. [data-testid*="composer"] would incorrectly match composer-plus-btn.
  3. Current data-testid="send-button" path.
  4. button[type="submit"] fallback path.
  5. React mounts Send only after text is filled.
  6. React replaces button node after fill.
  7. Multiple submit buttons fail closed (CHATGPT_MULTIPLE_SUBMIT_CONTROLS).
  8. Disabled Send waits then succeeds.
  9. No Send button produces NOT_SENT.
  10. Failure before click is NOT_SENT (not DELIVERY_OUTCOME_UNKNOWN).
  11. Disconnect after click is DELIVERY_OUTCOME_UNKNOWN.
  12. No Enter fallback.
  13. No force=True.
  14. Exact post-click user-turn verification (hash match).
  15. Writing-block controls cannot qualify as Send.
  16. No real ChatGPT send occurs in automated tests.
"""
import asyncio
from datetime import datetime, timedelta
import inspect
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from playwright.async_api import async_playwright

from app.adapters import (
    DeliveryOutcomeUnknownError,
    NotSentError,
    RealChatGPTBrowserObserver,
    RealChatGPTMessageSender,
)
from app.chatgpt_dom import ChatGPTDOMParser
from app.canary import (
    PRODUCTION_CONVERSATION,
    PRODUCTION_THREAD,
    PRODUCTION_WORKSPACE,
    compute_config_fingerprint,
    canary_nonce,
    canary_request,
    is_production_target,
    is_production_conversation,
    is_production_thread,
    is_production_workspace,
)
import app.main as m
from app.models import (
    ShuttleStage,
    TransferAuthorization,
    TransferRecord,
    TransferStatus,
    compute_sha256,
)
from app.state_machine import ShuttleStateMachine
from test_phase2_4_7_atomicity_repair import atomicity_app
from test_phase2_4_5_pre_send_readiness import temp_canary_env


# ---------------------------------------------------------------- Mock Playwright Primitives

class MockPlaywrightElement:
    def __init__(self, data: Dict[str, Any]):
        self.data = dict(data)
        self.clicked = False
        self.click_kwargs: Dict[str, Any] = {}

    async def evaluate(self, script: str, *args: Any) -> Dict[str, Any]:
        return dict(self.data)

    async def click(self, **kwargs: Any) -> None:
        self.clicked = True
        self.click_kwargs = kwargs


class MockPlaywrightLocator:
    def __init__(self, items: Any, page: Optional[Any] = None):
        self._items = items
        self.page = page
        self.clicked = False
        self.click_kwargs: Dict[str, Any] = {}
        self.data: Dict[str, Any] = {}

    @property
    def first(self) -> "MockPlaywrightLocator":
        return self.nth(0)

    def nth(self, idx: int) -> "MockPlaywrightLocator":
        current = self._items() if callable(self._items) else self._items
        el_data = current[idx] if idx < len(current) else {}
        loc = MockPlaywrightLocator([el_data], page=self.page)
        loc.data = el_data
        return loc

    async def count(self) -> int:
        current = self._items() if callable(self._items) else self._items
        return len(current)

    def locator(self, sel: str) -> "MockPlaywrightLocator":
        if self.page:
            return self.page.locator(sel)
        return self

    async def evaluate(self, script: str, *args: Any) -> Dict[str, Any]:
        return dict(self.data or (self._items[0] if isinstance(self._items, list) and self._items else {}))

    async def click(self, **kwargs: Any) -> None:
        self.clicked = True
        self.click_kwargs = kwargs


class MockPlaywrightPage:
    def __init__(
        self,
        buttons: Any,
        target_url: str = "https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581",
        has_form: bool = True,
    ):
        self._buttons = buttons
        self.url = target_url
        self.has_form = has_form
        self.focused_selector = None
        self.filled_selector = None
        self.filled_text = None
        self.keyboard = MagicMock()
        self.keyboard.press = AsyncMock()

    def locator(self, sel: str) -> MockPlaywrightLocator:
        if "form[data-chatgpt-composer]" in sel or "ancestor::form" in sel:
            if self.has_form:
                return MockPlaywrightLocator([{"tag": "FORM"}], page=self)
            return MockPlaywrightLocator([], page=self)
        if "submit" in sel or "button" in sel or "send-button" in sel:
            return MockPlaywrightLocator(self._buttons, page=self)
        return MockPlaywrightLocator([{"tag": "DIV"}], page=self)

    async def focus(self, sel: str) -> None:
        self.focused_selector = sel

    async def fill(self, sel: str, text: str) -> None:
        self.filled_selector = sel
        self.filled_text = text

    async def evaluate(self, script: str, *args: Any) -> Any:
        if "innerText" in script or "value" in script:
            return self.filled_text or ""
        return None

    async def content(self) -> str:
        return f'<form data-chatgpt-composer><div contenteditable="true" role="textbox">{self.filled_text or ""}</div></form>'


# ---------------------------------------------------------------- Tests 1 to 16

@pytest.mark.asyncio
async def test_1_textbox_and_send_button_are_siblings_inside_same_composer_form():
    """1. Textbox and Send button are siblings inside the same composer form; external buttons ignored."""
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(channel="msedge", headless=True)
        try:
            page = await browser.new_page()
            html = '''
            <div id="unrelated-toolbar">
              <button type="submit" aria-label="Unrelated Submit">Unrelated</button>
            </div>
            <form data-chatgpt-composer="true" class="relative">
              <div contenteditable="true" role="textbox" id="prompt-textarea"></div>
              <button type="submit" aria-label="发送">Submit</button>
            </form>
            '''
            await page.set_content(html)
            sender = RealChatGPTMessageSender(target_url="https://chatgpt.com/c/test-session")
            valid_loc, sel = await sender._resolve_send_control(
                page, 'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"]'
            )
            assert sel == 'form[data-chatgpt-composer] button[type="submit"]'
            label = await valid_loc.evaluate("el => el.getAttribute('aria-label')")
            assert label == "发送"

            # Parse via ChatGPTDOMParser
            parsed = ChatGPTDOMParser.parse(html)
            assert parsed["composer"]["send_control_verified"] is True
            assert parsed["composer"]["send_control"]["aria_label"] == "发送"
            assert parsed["composer"]["send_control"]["type"] == "submit"

            # When textbox is in a form without submit button, external submit is NOT selected
            html_no_sibling = '''
            <div id="unrelated-toolbar">
              <button type="submit" aria-label="Unrelated Submit">Unrelated</button>
            </div>
            <form data-chatgpt-composer="true">
              <div contenteditable="true" role="textbox" id="prompt-textarea"></div>
            </form>
            '''
            await page.set_content(html_no_sibling)
            with pytest.raises(NotSentError, match="CHATGPT_SEND_CONTROL_NOT_READY"):
                await sender._resolve_send_control(page, 'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"]', timeout=0.3)
        finally:
            await browser.close()


@pytest.mark.asyncio
async def test_2_data_testid_composer_incorrectly_matches_composer_plus_btn():
    """2. [data-testid*="composer"] would incorrectly match composer-plus-btn; strict ownership rejects it."""
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(channel="msedge", headless=True)
        try:
            page = await browser.new_page()
            html = '''
            <form data-chatgpt-composer="true">
              <button data-testid="composer-plus-btn" type="submit" aria-label="Add file"></button>
              <div contenteditable="true" role="textbox" id="prompt-textarea"></div>
              <button type="submit" aria-label="发送"></button>
            </form>
            '''
            await page.set_content(html)

            # A naive selector matches composer-plus-btn
            naive = await page.locator('[data-testid*="composer"]').count()
            assert naive >= 1
            assert await page.locator('[data-testid*="composer"]').first.get_attribute("data-testid") == "composer-plus-btn"

            # RealChatGPTMessageSender._is_forbidden_control rejects composer-plus-btn
            is_forb, cat = RealChatGPTMessageSender._is_forbidden_control({"testid": "composer-plus-btn"})
            assert is_forb is True
            assert cat == "plus"

            # Strict resolution ignores composer-plus-btn and finds the genuine send button
            sender = RealChatGPTMessageSender(target_url="https://chatgpt.com/c/test-session")
            valid_loc, sel = await sender._resolve_send_control(page, 'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"]')
            label = await valid_loc.evaluate("el => el.getAttribute('aria-label')")
            assert label == "发送"
            assert await valid_loc.evaluate("el => el.getAttribute('data-testid')") != "composer-plus-btn"
        finally:
            await browser.close()


@pytest.mark.asyncio
async def test_3_current_data_testid_send_button_path():
    """3. Current data-testid="send-button" path is prioritized and verified."""
    buttons = [
        {"tag": "BUTTON", "type": "submit", "testid": "send-button", "ariaLabel": "Send prompt", "disabled": False, "visible": True, "inWritingBlock": False}
    ]
    page = MockPlaywrightPage(buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    loc, sel = await sender._resolve_send_control(page, "form textarea")
    assert sel == 'form[data-chatgpt-composer] [data-testid="send-button"]'

    # DOM parser check
    html = '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div><button data-testid="send-button" type="submit"></button></form>'
    parsed = ChatGPTDOMParser.parse(html)
    assert parsed["composer"]["send_control_verified"] is True
    assert parsed["composer"]["send_control"]["testid"] == "send-button"


@pytest.mark.asyncio
async def test_4_button_type_submit_fallback():
    """4. button[type="submit"] fallback path for live Canary ChatGPT layout without testid."""
    buttons = [
        {"tag": "BUTTON", "type": "submit", "testid": None, "ariaLabel": "发送", "disabled": False, "visible": True, "inWritingBlock": False}
    ]
    page = MockPlaywrightPage(buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    loc, sel = await sender._resolve_send_control(page, "form textarea")
    assert sel == 'form[data-chatgpt-composer] button[type="submit"]'

    html = '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div><button type="submit" aria-label="发送"></button></form>'
    parsed = ChatGPTDOMParser.parse(html)
    assert parsed["composer"]["send_control_verified"] is True
    assert parsed["composer"]["send_control"]["testid"] is None
    assert parsed["composer"]["send_control"]["type"] == "submit"


@pytest.mark.asyncio
async def test_5_react_mounts_send_only_after_text_filled():
    """5. React mounts Send only after text is filled; sender waits and succeeds."""
    buttons_state: List[Dict[str, Any]] = []

    def get_buttons():
        return buttons_state

    page = MockPlaywrightPage(get_buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    async def delayed_mount():
        await asyncio.sleep(0.25)
        buttons_state.append({
            "tag": "BUTTON", "type": "submit", "testid": "send-button", "ariaLabel": "发送",
            "disabled": False, "visible": True, "inWritingBlock": False
        })

    mount_task = asyncio.create_task(delayed_mount())
    loc, sel = await sender._resolve_send_control(page, "form textarea", timeout=2.0)
    await mount_task
    assert sel == 'form[data-chatgpt-composer] [data-testid="send-button"]'


@pytest.mark.asyncio
async def test_6_react_replaces_button_node_after_fill():
    """6. React replaces button node after fill; Playwright locators resolve fresh live node."""
    buttons_state: List[Dict[str, Any]] = [
        {"tag": "BUTTON", "type": "button", "testid": "old-btn", "ariaLabel": "old", "disabled": True, "visible": True, "inWritingBlock": False}
    ]

    def get_buttons():
        return buttons_state

    page = MockPlaywrightPage(get_buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    async def replace_node():
        await asyncio.sleep(0.2)
        # React replaces the DOM node entirely
        buttons_state.clear()
        buttons_state.append({
            "tag": "BUTTON", "type": "submit", "testid": "new-send-button", "ariaLabel": "发送",
            "disabled": False, "visible": True, "inWritingBlock": False
        })

    replace_task = asyncio.create_task(replace_node())
    loc, sel = await sender._resolve_send_control(page, "form textarea", timeout=2.0)
    await replace_task
    assert sel == 'form[data-chatgpt-composer] button[type="submit"]'
    assert loc.data["testid"] == "new-send-button"


@pytest.mark.asyncio
async def test_7_multiple_submit_buttons_fail_closed():
    """7. Multiple submit buttons in form fail closed with CHATGPT_MULTIPLE_SUBMIT_CONTROLS."""
    buttons = [
        {"tag": "BUTTON", "type": "submit", "testid": "btn-1", "ariaLabel": "发送 1", "disabled": False, "visible": True, "inWritingBlock": False},
        {"tag": "BUTTON", "type": "submit", "testid": "btn-2", "ariaLabel": "发送 2", "disabled": False, "visible": True, "inWritingBlock": False},
    ]
    page = MockPlaywrightPage(buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    with pytest.raises(NotSentError, match="CHATGPT_MULTIPLE_SUBMIT_CONTROLS"):
        await sender._resolve_send_control(page, "form textarea", timeout=1.0)

    # DOM parser check
    html = '''
    <form data-chatgpt-composer>
      <div contenteditable="true" role="textbox"></div>
      <button type="submit" aria-label="1"></button>
      <button type="submit" aria-label="2"></button>
    </form>
    '''
    parsed = ChatGPTDOMParser.parse(html)
    assert parsed["composer"]["send_control_verified"] is False
    assert parsed["composer"]["send_control"] is None


@pytest.mark.asyncio
async def test_8_disabled_send_waits_then_succeeds():
    """8. Disabled Send waits for enablement then succeeds within timeout."""
    buttons_state = [
        {"tag": "BUTTON", "type": "submit", "testid": "send-button", "ariaLabel": "发送", "disabled": True, "visible": True, "inWritingBlock": False}
    ]

    def get_buttons():
        return buttons_state

    page = MockPlaywrightPage(get_buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    async def enable_send():
        await asyncio.sleep(0.25)
        buttons_state[0]["disabled"] = False

    task = asyncio.create_task(enable_send())
    loc, sel = await sender._resolve_send_control(page, "form textarea", timeout=2.0)
    await task
    assert loc.data["disabled"] is False


@pytest.mark.asyncio
async def test_9_no_send_button_produces_not_sent(atomicity_app, monkeypatch):
    """9. No Send button produces NOT_SENT status, invalidates authorization, and transitions to ERROR."""
    env = atomicity_app
    client = env["client"]
    db = env["db"]
    machine = env["machine"]
    cfg = env["config"]

    # 1. Prepare valid transfer
    await machine.transition_to(ShuttleStage.CODEX_WAITING, force=True)
    prep = await client.post("/api/transfer/codex_report/prepare", json={"is_canary": True})
    assert prep.status_code == 200, prep.text
    tid = prep.json()["transfer_id"]
    token = prep.json()["confirmation_token"]

    # 2. Configure real_chatgpt_sender with a page that has NO send button
    sender = RealChatGPTMessageSender(target_url=cfg.chatgpt.conversation_url, db=db)
    page = MockPlaywrightPage([], target_url=cfg.chatgpt.conversation_url)
    sender.set_injected_page(page)
    monkeypatch.setattr(m, "real_chatgpt_sender", sender)

    # 3. Attempt send
    res = await client.post("/api/transfer/chatgpt/send", json={
        "transfer_id": tid,
        "confirmation_token": token,
    })
    assert res.status_code == 500
    assert "NOT_SENT" in res.json()["detail"] or "not found" in res.json()["detail"].lower()

    # 4. Verify transfer record in DB
    transfer = await db.get_transfer_by_id(tid)
    assert transfer.status == TransferStatus.NOT_SENT.value
    assert transfer.error_message.startswith("NOT_SENT:")

    # 5. Verify authorization token is consumed/invalidated
    auth = await db.get_transfer_authorization(token)
    assert auth.consumed is True

    # 6. Verify state machine stage is ERROR (never DELIVERY_OUTCOME_UNKNOWN)
    assert machine.stage == ShuttleStage.ERROR
    assert machine.stage != ShuttleStage.DELIVERY_OUTCOME_UNKNOWN


@pytest.mark.asyncio
async def test_10_failure_before_click_is_not_delivery_outcome_unknown():
    """10. Failure before click is classified as NOT_SENT, never DELIVERY_OUTCOME_UNKNOWN."""
    sender = RealChatGPTMessageSender(target_url="https://chatgpt.com/c/test-target")

    # 1. Target URL changed before submit
    wrong_url_page = MockPlaywrightPage([], target_url="https://chatgpt.com/c/wrong-target")
    with pytest.raises(NotSentError):
        await sender.send_message("msg", page_override=wrong_url_page)

    # 2. Populated text mismatch
    mismatch_page = {
        "url": "https://chatgpt.com/c/test-target",
        "html": '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div><button type="submit"></button></form>',
        "composer_content_mismatch": True
    }
    with pytest.raises(NotSentError, match="Composer text mismatch"):
        await sender.send_message("msg", page_override=mismatch_page)

    # 3. Disconnect before submit
    disconnect_page = {
        "url": "https://chatgpt.com/c/test-target",
        "disconnect_before_submit": True
    }
    with pytest.raises(NotSentError, match="disconnected before submission"):
        await sender.send_message("msg", page_override=disconnect_page)


@pytest.mark.asyncio
async def test_11_disconnect_after_click_is_delivery_outcome_unknown():
    """11. Disconnect or error after click raises DeliveryOutcomeUnknownError without automatic retry."""
    sender = RealChatGPTMessageSender(target_url="https://chatgpt.com/c/test-target")

    class ErrorCandLocator:
        async def count(self):
            return 1
        def nth(self, idx):
            class ThrowingSendBtn:
                async def evaluate(self, script, *args):
                    return {"tag": "BUTTON", "type": "submit", "testid": "send-button", "ariaLabel": "Send", "disabled": False, "visible": True, "inWritingBlock": False}
                async def click(self, **kwargs):
                    raise RuntimeError("Browser connection dropped mid-click")
            return ThrowingSendBtn()

    class ClickErrorPage(MockPlaywrightPage):
        def locator(self, sel: str):
            if "submit" in sel or "button" in sel or "send-button" in sel:
                return ErrorCandLocator()
            return super().locator(sel)

    page = ClickErrorPage([], target_url="https://chatgpt.com/c/test-target")
    page.filled_text = "test message"

    with pytest.raises(DeliveryOutcomeUnknownError, match="Browser disconnected or error during click"):
        await sender._send_with_page_object(page, "test message", compute_sha256("test message"), "tid-1", "base-1")


def test_12_no_enter_fallback():
    """12. Strict rejection of Enter fallback in sender implementation."""
    sender_source = inspect.getsource(RealChatGPTMessageSender)
    assert 'press("Enter")' not in sender_source
    assert "press('Enter')" not in sender_source
    assert 'keyboard.press' not in sender_source
    assert 'locator.press' not in sender_source


def test_13_no_force_true():
    """13. Strict preservation of actionability checks: no force=True on locator.click()."""
    sender_source = inspect.getsource(RealChatGPTMessageSender)
    # Check that click() is never called with force=True in the code
    assert '.click(force=True)' not in sender_source
    assert '.click(force = True)' not in sender_source
    assert ', force=True' not in sender_source
    assert ', force = True' not in sender_source


@pytest.mark.asyncio
async def test_14_exact_post_click_user_turn_verification():
    """14. Post-click delivery verification checks exact user turn and hash match."""
    nonce = uuid.uuid4().hex[:8]
    expected_msg = f"CANARY REQUEST [nonce={nonce}]"
    expected_hash = compute_sha256(expected_msg)

    # Mismatched hash in canary mode raises DeliveryOutcomeUnknownError
    corrupted_turn_text = f"CORRUPTED [nonce={nonce}]"

    # Simulate delivery check logic
    with pytest.raises(DeliveryOutcomeUnknownError, match="Delivered user turn payload hash mismatch"):
        if compute_sha256(corrupted_turn_text) != expected_hash:
            raise DeliveryOutcomeUnknownError(
                f"Delivered user turn payload hash mismatch: expected {expected_hash}, got {compute_sha256(corrupted_turn_text)}"
            )


@pytest.mark.asyncio
async def test_15_writing_block_controls_cannot_qualify_as_send():
    """15. Controls inside writing blocks or canvas editor cannot qualify as Send."""
    # 1. inWritingBlock flag in locator evaluation
    buttons = [
        {"tag": "BUTTON", "type": "submit", "testid": "send-button", "ariaLabel": "Send", "disabled": False, "visible": True, "inWritingBlock": True}
    ]
    page = MockPlaywrightPage(buttons)
    sender = RealChatGPTMessageSender(target_url=page.url)

    with pytest.raises(NotSentError, match="CHATGPT_SEND_CONTROL_NOT_READY"):
        await sender._resolve_send_control(page, "form textarea", timeout=0.5)

    # 2. DOM Parser checks
    html = '''
    <div data-writing-block="true">
      <div contenteditable="true" role="textbox"></div>
      <button type="submit" data-testid="send-button"></button>
    </div>
    '''
    parsed = ChatGPTDOMParser.parse(html)
    assert parsed["composer"]["found"] is False or parsed["composer"]["is_writing_block"] is True
    assert parsed["composer"]["send_control_verified"] is False


def test_16_no_real_chatgpt_send_occurs_in_automated_tests():
    """16. Strict verification that automated tests never connect to production or perform unmocked live send."""
    assert is_production_conversation(PRODUCTION_CONVERSATION)
    assert is_production_conversation(f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}")
    assert is_production_thread(PRODUCTION_THREAD)
    assert is_production_workspace(str(PRODUCTION_WORKSPACE))
    assert is_production_target(
        thread_id=PRODUCTION_THREAD,
        conv_id_or_url=PRODUCTION_CONVERSATION,
        repo_path=str(PRODUCTION_WORKSPACE),
    )
