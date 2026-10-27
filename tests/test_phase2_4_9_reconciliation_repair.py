"""
Phase 2.4.9 — Unknown-Delivery Reconciliation Finalization Repair Regression Suite.
Covers all 14 criteria:
1. unknown delivery -> manual mark delivered clears unresolved blocker
2. unknown delivery -> manual mark cancelled clears unresolved blocker
3. reconciliation advances to the correct next stage
4. resolved unknown delivery remains in audit history
5. resolved historical transfer does not block readiness
6. another genuinely unresolved transfer still blocks
7. multiple historical resolved unknown transfers do not block
8. reconciliation transaction failure cannot partially advance stage
9. runtime refresh reflects the resolved state
10. page reload does not restore the stale blocker
11. frontend reconciliation fields clear after success
12. system can proceed to observe the existing ChatGPT reply after manual delivery reconciliation
13. no real ChatGPT send occurs in tests (mocks only)
14. no real Codex queue occurs in tests (mocks only)
"""
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from playwright.async_api import async_playwright

import app.main as m
import app.canary_bootstrap as bootstrap
from app.adapters import RealChatGPTBrowserObserver, StandardPromptExtractor
from app.canary import compute_config_fingerprint, canary_request, canary_nonce, canary_prompt
from app.models import ShuttleStage, TransferRecord, TransferAuthorization, TransferStatus, compute_sha256
from app.state_machine import ShuttleStateMachine
from test_phase2_4_5_pre_send_readiness import temp_canary_env


@pytest.fixture
async def recon_app(temp_canary_env, monkeypatch):
    env = temp_canary_env
    cfg, database = env["config"], env["db"]
    await database.init()
    await database.save_config(cfg)
    source = env["tmp_path"] / "config.canary.local.20261026-07.json"
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
    monkeypatch.setattr(m, "validate_canary_session", lambda *a, **k: {"session_file_path": "fixture.jsonl", "total_lines": 2})

    observer = RealChatGPTBrowserObserver(target_url=cfg.chatgpt.conversation_url, cdp_url=cfg.chatgpt.cdp_url)
    observer.read_live_page = AsyncMock(return_value={
        "url": cfg.chatgpt.conversation_url,
        "html": '<form data-chatgpt-composer><div contenteditable="true" role="textbox"></div></form>'
    })
    monkeypatch.setattr(m, "real_chatgpt_observer", observer)

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
    queue.queue_message.side_effect = AssertionError("Live Codex queue forbidden in tests")
    monkeypatch.setattr(m, "real_chatgpt_sender", sender)
    monkeypatch.setattr(m, "real_codex_queue", queue)

    async with AsyncClient(transport=ASGITransport(app=m.app), base_url="http://test") as client:
        yield {**env, "client": client, "source": source, "observer": observer,
               "sender": sender, "queue": queue, "verified": verified, "machine": machine}

    # Strict invariant: no real ChatGPT send or Codex queue occurred
    sender.send_message.assert_not_called()
    queue.queue_message.assert_not_called()


async def create_unknown_delivery(env, direction="codex_to_chatgpt", tid=None, nonce=None):
    nonce = nonce or uuid.uuid4().hex
    tid = tid or f"transfer-test-{uuid.uuid4().hex[:12]}"
    content = canary_request(nonce) if direction == "codex_to_chatgpt" else canary_prompt(nonce)
    rec = TransferRecord(
        transfer_id=tid,
        iteration_id=1,
        direction=direction,
        source_identifier=env["config"].codex.thread_id if direction == "codex_to_chatgpt" else env["config"].chatgpt.conversation_url,
        destination_identifier=env["config"].chatgpt.conversation_url if direction == "codex_to_chatgpt" else env["config"].codex.thread_id,
        content_hash=compute_sha256(content),
        message_text=content,
        payload_preview=content[:100],
        status=TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value,
        error_message="Timeout waiting for user message confirmation: delivery outcome unknown",
        provenance_policy=f"canary:{nonce}",
        confirmed_at=datetime.utcnow() - timedelta(minutes=5),
        created_at=datetime.utcnow() - timedelta(minutes=6),
    )
    await env["db"].save_transfer_record(rec)
    await env["machine"].transition_to(
        ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
        transfer_status="unknown",
        failure_details="Timeout waiting for user message confirmation: delivery outcome unknown",
        force=True,
    )
    return rec, nonce


@pytest.mark.asyncio
async def test_criterion_1_mark_delivered_clears_unresolved_blocker(recon_app):
    env = recon_app
    client = env["client"]
    rec, nonce = await create_unknown_delivery(env)

    # Before reconciliation: guard and readiness must report blocker
    guard_before = (await client.get("/api/transfer/target_info")).json()
    assert guard_before["blocking_reason"] == "Previous delivery requires manual reconciliation"
    refresh_before = (await client.get("/api/runtime/refresh")).json()
    assert refresh_before["readiness"]["status"] == "DELIVERY_OUTCOME_UNKNOWN"
    assert refresh_before["readiness"]["transfers"]["unresolved_delivery"] is True

    # Execute manual reconciliation: mark_delivered
    recon_res = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
        "notes": "Verified manual delivery in browser",
    })
    assert recon_res.status_code == 200
    assert recon_res.json()["status"] == "reconciled"
    assert recon_res.json()["new_stage"] == "CHATGPT_WAITING"

    # After reconciliation: blocker is cleared
    guard_after = (await client.get("/api/transfer/target_info")).json()
    assert guard_after["blocking_reason"] is None

    refresh_after = (await client.get("/api/runtime/refresh")).json()
    assert refresh_after["readiness"]["transfers"]["unresolved_delivery"] is False
    assert refresh_after["readiness"]["status"] == "CANARY_AWAITING_CHATGPT_REPLY"
    assert env["machine"].stage == ShuttleStage.CHATGPT_WAITING

    # DB record check
    updated_rec = await env["db"].get_transfer_by_id(rec.transfer_id)
    assert updated_rec.status == "delivered"
    assert updated_rec.error_message is None
    assert updated_rec.sent_at is not None
    assert updated_rec.delivery_evidence["resolution"] == "mark_delivered"
    cp = await env["db"].get_latest_checkpoint()
    assert cp.failure_details is None
    assert cp.transfer_status == "reconciled_delivered"


@pytest.mark.asyncio
async def test_criterion_2_mark_cancelled_clears_unresolved_blocker(recon_app):
    env = recon_app
    client = env["client"]
    rec, nonce = await create_unknown_delivery(env)

    recon_res = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_cancelled",
        "notes": "Verified not delivered in browser",
    })
    assert recon_res.status_code == 200
    assert recon_res.json()["status"] == "reconciled"
    assert recon_res.json()["new_stage"] == "CODEX_WAITING"

    guard = (await client.get("/api/transfer/target_info")).json()
    assert guard["blocking_reason"] is None

    refresh = (await client.get("/api/runtime/refresh")).json()
    assert refresh["readiness"]["transfers"]["unresolved_delivery"] is False
    assert env["machine"].stage == ShuttleStage.CODEX_WAITING

    updated_rec = await env["db"].get_transfer_by_id(rec.transfer_id)
    assert updated_rec.status == "cancelled"
    assert "Cancelled by operator" in (updated_rec.error_message or "")


@pytest.mark.asyncio
async def test_criterion_3_reconciliation_advances_to_correct_next_stage(recon_app):
    env = recon_app
    client = env["client"]

    # 1. C2G mark_delivered -> CHATGPT_WAITING
    rec_c2g, _ = await create_unknown_delivery(env, direction="codex_to_chatgpt")
    r1 = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec_c2g.transfer_id,
        "resolution": "mark_delivered",
    })
    assert r1.status_code == 200
    assert r1.json()["new_stage"] == "CHATGPT_WAITING"
    assert env["machine"].stage == ShuttleStage.CHATGPT_WAITING

    # 2. G2C mark_delivered -> CODEX_WAITING
    rec_g2c, _ = await create_unknown_delivery(env, direction="chatgpt_to_codex")
    r2 = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec_g2c.transfer_id,
        "resolution": "mark_delivered",
    })
    assert r2.status_code == 200, r2.text
    assert r2.json()["new_stage"] == "CODEX_WAITING"
    assert env["machine"].stage == ShuttleStage.CODEX_WAITING


@pytest.mark.asyncio
async def test_criterion_4_resolved_unknown_delivery_remains_in_audit_history(recon_app):
    env = recon_app
    client = env["client"]
    rec, _ = await create_unknown_delivery(env)

    notes = "Special audit note for test verification"
    await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
        "notes": notes,
    })

    logs_res = await client.get("/api/logs?limit=50")
    assert logs_res.status_code == 200
    logs = logs_res.json()["logs"]

    recon_events = [l for l in logs if l["event_type"] == "UNKNOWN_OUTCOME_RECONCILED"]
    assert len(recon_events) >= 1
    details = recon_events[0]["details"]
    if isinstance(details, str):
        details = json.loads(details)
    assert details["transfer_id"] == rec.transfer_id
    assert details["resolution"] == "mark_delivered"
    assert details["notes"] == notes

    trans_events = [l for l in logs if l["event_type"] == "STAGE_TRANSITION" and "CHATGPT_WAITING" in l["message"]]
    assert len(trans_events) >= 1


@pytest.mark.asyncio
async def test_criterion_5_resolved_historical_transfer_does_not_block_readiness(recon_app):
    env = recon_app
    client = env["client"]
    rec, _ = await create_unknown_delivery(env)

    await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
    })

    refresh = (await client.get("/api/runtime/refresh")).json()
    assert refresh["readiness"]["status"] == "CANARY_AWAITING_CHATGPT_REPLY"
    assert refresh["readiness"]["blocker"] != "Previous delivery requires manual reconciliation"
    assert refresh["readiness"]["transfers"]["unresolved_delivery"] is False


@pytest.mark.asyncio
async def test_criterion_6_another_genuinely_unresolved_transfer_still_blocks(recon_app):
    env = recon_app
    client = env["client"]
    rec1, _ = await create_unknown_delivery(env, tid="tid-resolved-1")

    # Reconcile transfer 1
    await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec1.transfer_id,
        "resolution": "mark_delivered",
    })

    # Now create a fresh unresolved transfer 2
    rec2, _ = await create_unknown_delivery(env, tid="tid-unresolved-2")

    # Target info and readiness must both block again
    guard = (await client.get("/api/transfer/target_info")).json()
    assert guard["blocking_reason"] == "Previous delivery requires manual reconciliation"

    refresh = (await client.get("/api/runtime/refresh")).json()
    assert refresh["readiness"]["status"] == "DELIVERY_OUTCOME_UNKNOWN"
    assert refresh["readiness"]["transfers"]["unresolved_delivery"] is True


@pytest.mark.asyncio
async def test_criterion_7_multiple_historical_resolved_unknown_transfers_do_not_block(recon_app):
    env = recon_app
    client = env["client"]

    # Reconcile transfer 1 as delivered
    r1, _ = await create_unknown_delivery(env, tid="t-hist-1")
    await client.post("/api/transfer/reconcile_unknown", json={"transfer_id": r1.transfer_id, "resolution": "mark_delivered"})

    # Reconcile transfer 2 as cancelled
    r2, _ = await create_unknown_delivery(env, tid="t-hist-2")
    await client.post("/api/transfer/reconcile_unknown", json={"transfer_id": r2.transfer_id, "resolution": "mark_cancelled"})

    # Reconcile transfer 3 as delivered
    r3, _ = await create_unknown_delivery(env, tid="t-hist-3")
    await client.post("/api/transfer/reconcile_unknown", json={"transfer_id": r3.transfer_id, "resolution": "mark_delivered"})

    # Verify no unresolved delivery
    refresh = (await client.get("/api/runtime/refresh")).json()
    assert refresh["readiness"]["transfers"]["unresolved_delivery"] is False
    assert refresh["readiness"]["status"] != "DELIVERY_OUTCOME_UNKNOWN"
    guard = (await client.get("/api/transfer/target_info")).json()
    assert guard["blocking_reason"] != "Previous delivery requires manual reconciliation"


@pytest.mark.asyncio
async def test_criterion_8_reconciliation_transaction_failure_cannot_partially_advance(recon_app, monkeypatch):
    env = recon_app
    client = env["client"]
    rec, _ = await create_unknown_delivery(env)
    initial_stage = env["machine"].stage

    # 1. Non-existent transfer ID fails cleanly
    r_bad = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": "non-existent-transfer",
        "resolution": "mark_delivered",
    })
    assert r_bad.status_code == 404
    assert env["machine"].stage == initial_stage

    # 2. Unsupported resolution fails cleanly
    r_bad_res = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "invalid_resolution",
    })
    assert r_bad_res.status_code == 400
    assert env["machine"].stage == initial_stage

    # 3. Simulate DB exception inside transaction
    original_execute = env["db"].reconcile_unknown_outcome_transaction
    async def failing_reconcile(*args, **kwargs):
        raise RuntimeError("Simulated mid-transaction failure")
    monkeypatch.setattr(env["db"], "reconcile_unknown_outcome_transaction", failing_reconcile)

    r_fail = await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
    })
    assert r_fail.status_code == 500
    assert env["machine"].stage == initial_stage
    t = await env["db"].get_transfer_by_id(rec.transfer_id)
    assert t.status == TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value


@pytest.mark.asyncio
async def test_criterion_9_runtime_refresh_reflects_resolved_state(recon_app):
    env = recon_app
    client = env["client"]
    rec, _ = await create_unknown_delivery(env)

    await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
    })

    res = await client.get("/api/runtime/refresh")
    assert res.status_code == 200
    data = res.json()

    assert data["state"]["stage"] == "CHATGPT_WAITING"
    assert data["state"]["transfer_status"] == "reconciled_delivered"
    assert data["state"]["failure_details"] is None
    assert data["readiness"]["transfers"]["unresolved_delivery"] is False
    assert data["target_info"]["blocking_reason"] is None


@pytest.mark.asyncio
async def test_criterion_10_page_reload_does_not_restore_stale_blocker(recon_app):
    env = recon_app
    client = env["client"]
    rec, _ = await create_unknown_delivery(env)

    await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
    })

    # Simulate 3 successive reloads/polling queries
    for _ in range(3):
        html = (await client.get("/")).text
        assert "Previous delivery requires manual reconciliation" not in html
        target = (await client.get("/api/transfer/target_info")).json()
        assert target["blocking_reason"] is None
        st = (await client.get("/api/state")).json()
        assert st["stage"] == "CHATGPT_WAITING"
        assert st["failure_details"] is None
        ref = (await client.get("/api/runtime/refresh")).json()
        assert ref["readiness"]["transfers"]["unresolved_delivery"] is False


@pytest.mark.asyncio
async def test_criterion_11_frontend_reconciliation_fields_clear_after_success(recon_app):
    env = recon_app
    client = env["client"]
    rec, _ = await create_unknown_delivery(env)

    snapshot = (await client.get("/api/runtime/refresh")).json()
    html = (await client.get("/")).text

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(channel="msedge", headless=True)
        try:
            page = await browser.new_page()
            async def route_request(route):
                req_url = route.request.url
                path = req_url.split("console.test", 1)[-1]
                if path == "/":
                    await route.fulfill(body=html, content_type="text/html")
                elif path == "/api/runtime/refresh":
                    await route.fulfill(body=json.dumps(snapshot), content_type="application/json")
                elif path == "/api/transfer/reconcile_unknown":
                    await route.fulfill(body=json.dumps({
                        "status": "reconciled",
                        "transfer_id": rec.transfer_id,
                        "new_stage": "CHATGPT_WAITING"
                    }), content_type="application/json")
                elif path == "/api/logs?limit=30":
                    await route.fulfill(body=json.dumps({"logs": []}), content_type="application/json")
                else:
                    await route.abort()

            await page.route("**/*", route_request)
            page.on("dialog", lambda d: d.accept())
            await page.goto("http://console.test/")

            # Fill inputs
            await page.fill("#reconcile-transfer-id", rec.transfer_id)
            await page.fill("#reconcile-notes", "Operator confirmed manual arrival")

            # Click reconcile button
            await page.click('button[onclick="reconcileUnknownOutcome()"]')
            await page.wait_for_function("document.getElementById('reconcile-transfer-id').value === ''")

            # Assert fields were cleared
            assert await page.input_value("#reconcile-transfer-id") == ""
            assert await page.input_value("#reconcile-notes") == ""
            assert await page.input_value("#reconcile-resolution") == "mark_delivered"
        finally:
            await browser.close()


@pytest.mark.asyncio
async def test_criterion_12_can_proceed_to_observe_existing_chatgpt_reply(recon_app):
    env = recon_app
    client = env["client"]
    rec, nonce = await create_unknown_delivery(env)

    # Reconcile transfer as delivered
    await client.post("/api/transfer/reconcile_unknown", json={
        "transfer_id": rec.transfer_id,
        "resolution": "mark_delivered",
        "notes": "Delivered successfully",
    })

    # Prepare mock DOM payload for assistant turn containing the expected nonce
    expected_ack = canary_prompt(nonce)
    reply_html = f"""
    <html>
    <body>
    <div data-app-shell-focus-area="main">
      <div class="thread-scroll-container">
        <div data-turn-key="turn-uuid-001">
          <div data-content-search-turn-key="fallback-turn-0">
            <div data-content-search-unit-key="fallback-turn-0:0:user">
              <div data-user-message-bubble="true">{canary_request(nonce)}</div>
            </div>
            <div data-content-search-unit-key="fallback-turn-0:1:assistant" data-chatgpt-search-unit-key="fallback-turn-0:1:assistant">
              <div data-markdown-text-style="assistant-message" class="MarkdownRoot">
                <div class="CodeBlock"><pre><code>{expected_ack}</code></pre></div>
              </div>
            </div>
            <div class="turn-action-controls">
              <button aria-label="Copy" data-testid="copy-turn-action-button">Copy</button>
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
    """

    env["observer"].read_live_page = AsyncMock(return_value={
        "url": env["config"].chatgpt.conversation_url,
        "html": reply_html,
    })

    # Calling observe endpoint now succeeds and extracts the response
    obs_res = await client.post("/api/transfer/chatgpt/observe", json={})
    assert obs_res.status_code == 200
    data = obs_res.json()
    assert data["status"] == "ready"
    assert data["extraction"]["extracted_prompt_preview"] == expected_ack
    assert data["extraction"]["provenance_policy"] == f"canary:{nonce}"
    assert env["machine"].stage == ShuttleStage.CHATGPT_RESPONSE_READY
