"""Execute dashboard JavaScript in a real isolated DOM; every request is mocked."""
import json

import pytest
from playwright.async_api import async_playwright

from test_phase2_4_6_runtime_sync import runtime_app, temp_canary_env, prepare


@pytest.mark.asyncio
async def test_dashboard_restores_persisted_transfer_and_fails_closed(runtime_app):
    env = runtime_app
    prepared = await prepare(env)
    snapshot = (await env["client"].get("/api/runtime/refresh")).json()
    html = (await env["client"].get("/")).text
    submitted = []
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(channel="msedge", headless=True)
        try:
            page = await browser.new_page()
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            async def route_request(route):
                path = route.request.url.split("console.test", 1)[-1]
                if path == "/":
                    await route.fulfill(body=html, content_type="text/html")
                elif path == "/api/runtime/refresh":
                    await route.fulfill(body=json.dumps(snapshot), content_type="application/json")
                else:
                    submitted.append(path)
                    await route.abort()
            await page.route("**/*", route_request)
            await page.goto("http://console.test/")
            await page.wait_for_function("document.getElementById('runtime-readiness').innerText === 'CANARY_REPORT_PREPARED_AWAITING_AUTHORIZATION'")
            assert await page.locator("#active-c2g-transfer-id").inner_text() == prepared["transfer_id"]
            assert await page.locator("#active-c2g-hash").inner_text() == prepared["content_hash"]
            assert await page.locator("#active-c2g-nonce").inner_text() == prepared["canary_nonce"]
            assert await page.locator("#c2g-send-button").is_enabled()
            assert await page.locator("#g2c-queue-button").is_disabled()
            assert await page.locator("#g2c-queue-button").evaluate("el => getComputedStyle(el).cursor") == "not-allowed"
            for field in ("transfer_id", "confirmation_token", "content_hash"):
                changed = json.loads(json.dumps(snapshot))
                changed["readiness"]["transfers"]["c2g"]["transfer"][field] = None
                await page.evaluate("data => renderRuntimeSnapshot(data)", changed)
                assert await page.locator("#c2g-send-button").is_disabled()
            changed = json.loads(json.dumps(snapshot))
            changed["readiness"]["c2g_send_enabled"] = False
            changed["readiness"]["status"] = "RUNTIME_RESTART_REQUIRED"
            await page.evaluate("data => renderRuntimeSnapshot(data)", changed)
            assert await page.locator("#c2g-send-button").is_disabled()
            assert await page.locator("#runtime-readiness").inner_text() == "RUNTIME_RESTART_REQUIRED"
            assert not submitted
            assert not errors
        finally:
            await browser.close()
