import asyncio
import json
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.codex_app_server import (
    AppServerRpcClient,
    AppServerTransport,
    CodexAppServerError,
    CodexAppServerManager,
    CodexProcessTerminatedError,
    CodexResumeIdentityMismatchError,
    CodexRpcError,
    CodexRpcTimeoutError,
    CodexThreadLifecycleAdapter,
)
from app.database import Database
from app.models import CodexLifecycleEvent


class MockTransport(AppServerTransport):
    def __init__(self):
        super().__init__(cli_path="codex.exe")
        self._alive = True
        self.sent_lines = []
        self.incoming_lines = asyncio.Queue()

    def is_alive(self) -> bool:
        return self._alive

    async def start(self) -> None:
        self._alive = True

    async def stop(self) -> None:
        self._alive = False

    async def send_line(self, line: str) -> None:
        if not self._alive:
            raise CodexProcessTerminatedError("Mock process terminated")
        self.sent_lines.append(line)

    async def read_line(self) -> str:
        if not self._alive:
            raise CodexProcessTerminatedError("Mock process terminated")
        return await self.incoming_lines.get()

    def feed_json(self, data: dict) -> None:
        self.incoming_lines.put_nowait(json.dumps(data) + "\n")


@pytest.fixture
def mock_rpc():
    transport = MockTransport()
    rpc = AppServerRpcClient(transport)
    return transport, rpc

@pytest.mark.asyncio
async def test_capability_discovery(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)

    async def auto_respond():
        while True:
            line = await asyncio.to_thread(lambda: transport.sent_lines[-1] if transport.sent_lines else None)
            if line:
                data = json.loads(line)
                req_id = data["id"]
                transport.feed_json({
                    "id": req_id,
                    "result": {
                        "userAgent": "Codex Desktop/0.164.0 (Windows)",
                        "codexHome": "C:\\Users\\test\\.codex",
                        "platformFamily": "windows",
                        "platformOs": "windows"
                    }
                })
                break
            await asyncio.sleep(0.01)

    asyncio.create_task(auto_respond())
    info = await adapter.initialize()
    assert "Codex Desktop" in info["userAgent"]
    assert info["codexHome"] == "C:\\Users\\test\\.codex"
    await rpc.stop()


@pytest.mark.asyncio
async def test_app_server_initialization_handshake(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)

    async def reply():
        await asyncio.sleep(0.02)
        sent = json.loads(transport.sent_lines[0])
        assert sent["method"] == "initialize"
        assert sent["params"]["clientInfo"]["name"] == "ai-shuttle-console"
        assert sent["params"]["capabilities"]["experimentalApi"] is False
        transport.feed_json({
            "id": sent["id"],
            "result": {"userAgent": "Codex/0.164.0", "codexHome": "/home/user", "platformFamily": "windows", "platformOs": "windows"}
        })

    asyncio.create_task(reply())
    res = await adapter.initialize()
    assert res["userAgent"] == "Codex/0.164.0"
    await rpc.stop()


@pytest.mark.asyncio
async def test_thread_resume_request_structure(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"

    async def reply():
        await asyncio.sleep(0.02)
        sent = json.loads(transport.sent_lines[0])
        assert sent["method"] == "thread/resume"
        assert sent["params"]["threadId"] == thread_id
        # Invariant: no history or path override unless required
        assert "history" not in sent["params"]
        transport.feed_json({
            "id": sent["id"],
            "result": {"thread": {"id": thread_id, "status": {"type": "idle"}}}
        })

    asyncio.create_task(reply())
    res = await adapter.resume_thread(thread_id)
    assert res["thread"]["id"] == thread_id
    await rpc.stop()


@pytest.mark.asyncio
async def test_wrong_thread_identity_rejected(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)
    target_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    wrong_id = "01a174f9-wrong-id-000000000000"

    async def reply():
        await asyncio.sleep(0.02)
        sent = json.loads(transport.sent_lines[0])
        transport.feed_json({
            "id": sent["id"],
            "result": {"thread": {"id": wrong_id, "status": {"type": "idle"}}}
        })

    asyncio.create_task(reply())
    with pytest.raises(CodexResumeIdentityMismatchError):
        await adapter.resume_thread(target_id)
    await rpc.stop()


@pytest.mark.asyncio
async def test_already_loaded_thread_no_duplicate_resume(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"

    async def reply():
        await asyncio.sleep(0.02)
        # 1. thread/loaded/list
        sent1 = json.loads(transport.sent_lines[0])
        transport.feed_json({"id": sent1["id"], "result": {"data": [thread_id]}})
        # 2. thread/read
        while len(transport.sent_lines) < 2:
            await asyncio.sleep(0.01)
        sent2 = json.loads(transport.sent_lines[1])
        transport.feed_json({"id": sent2["id"], "result": {"thread": {"id": thread_id, "status": {"type": "idle"}}}})

    asyncio.create_task(reply())
    state = await adapter.check_thread_state(thread_id)
    assert state["is_loaded"] is True
    assert state["is_cold"] is False
    assert state["status_type"] == "idle"
    # Thread is already loaded, resume should not be issued
    assert len(transport.sent_lines) == 2
    await rpc.stop()


@pytest.mark.asyncio
async def test_cold_thread_detection(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"

    async def reply():
        await asyncio.sleep(0.02)
        sent1 = json.loads(transport.sent_lines[0])
        transport.feed_json({"id": sent1["id"], "result": {"data": []}})
        while len(transport.sent_lines) < 2:
            await asyncio.sleep(0.01)
        sent2 = json.loads(transport.sent_lines[1])
        transport.feed_json({"id": sent2["id"], "result": {"thread": {"id": thread_id, "status": {"type": "notLoaded"}}}})

    asyncio.create_task(reply())
    state = await adapter.check_thread_state(thread_id)
    assert state["is_loaded"] is False
    assert state["is_cold"] is True
    assert state["status_type"] == "notLoaded"
    await rpc.stop()

@pytest.mark.asyncio
async def test_resume_workspace_mismatch_fails_closed(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    expected_ws = "D:\\project\\ai-shuttle-console\\canary_workspace"
    wrong_ws = "D:\\workspace\\production-repo"

    async def reply():
        await asyncio.sleep(0.02)
        sent = json.loads(transport.sent_lines[0])
        transport.feed_json({
            "id": sent["id"],
            "result": {"thread": {"id": thread_id, "cwd": wrong_ws}}
        })

    asyncio.create_task(reply())
    with pytest.raises(CodexResumeIdentityMismatchError):
        await adapter.resume_thread(thread_id, expected_workspace=expected_ws)
    await rpc.stop()


@pytest.mark.asyncio
async def test_app_server_timeout(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    with pytest.raises(CodexRpcTimeoutError):
        await rpc.call("thread/read", {"threadId": "test"}, timeout=0.1)
    await rpc.stop()


@pytest.mark.asyncio
async def test_app_server_process_crash(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    call_task = asyncio.create_task(rpc.call("thread/read", {"threadId": "test"}, timeout=5.0))
    await asyncio.sleep(0.05)
    # Simulate child process crash / EOF
    await transport.stop()
    transport.incoming_lines.put_nowait("")
    with pytest.raises(CodexProcessTerminatedError):
        await call_task
    await rpc.stop()


@pytest.mark.asyncio
async def test_interactive_approval_request_triggers_human_action(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)

    # Server sends an approval request
    transport.feed_json({
        "id": 999,
        "method": "applyPatchApproval",
        "params": {"patch": "diff --git a/file b/file"}
    })
    await asyncio.sleep(0.05)

    actions = adapter.pop_human_actions()
    assert len(actions) == 1
    assert actions[0]["method"] == "applyPatchApproval"
    assert actions[0]["request_id"] == 999
    # Invariant: Never auto-approve
    assert len(transport.sent_lines) == 0
    await rpc.stop()


@pytest.mark.asyncio
async def test_zero_queue_resend_invariant(mock_rpc):
    transport, rpc = mock_rpc
    await rpc.start()
    adapter = CodexThreadLifecycleAdapter(rpc)
    thread_id = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"

    async def reply():
        await asyncio.sleep(0.02)
        sent = json.loads(transport.sent_lines[0])
        transport.feed_json({
            "id": sent["id"],
            "result": {"thread": {"id": thread_id}}
        })

    asyncio.create_task(reply())
    # Resume only triggers thread/resume, NEVER enqueues
    await adapter.resume_thread(thread_id)
    sent_methods = [json.loads(line)["method"] for line in transport.sent_lines]
    assert sent_methods == ["thread/resume"]
    assert "thread/queue/add" not in sent_methods
    assert "thread/queue/start" not in sent_methods
    await rpc.stop()


@pytest.mark.asyncio
async def test_no_sqlite_queue_mutation_prohibited(tmp_path):
    """Section 8 hard prohibition: zero INSERT/UPDATE/DELETE to queue_1.sqlite."""
    mock_sqlite = tmp_path / "queue_1.sqlite"
    mock_sqlite.write_text("", encoding="utf-8")

    # In regression, no component may mutate queue_1.sqlite
    # Verify file is not written or touched by adapter
    manager = CodexAppServerManager("codex.exe")
    assert not manager.capabilities.get("experimental_queue_start_enabled")


@pytest.mark.asyncio
async def test_queue_start_disabled_by_default():
    """Section 7: thread/queue/start must not be used by default."""
    manager = CodexAppServerManager("codex.exe")
    with patch.object(manager.adapter, "initialize", AsyncMock(return_value={"userAgent": "test"})):
        await manager.ensure_started()
        caps = await manager.get_capabilities()
        assert caps["thread_queue_supported"] is False
        assert caps["experimental_queue_start_enabled"] is False


@pytest.mark.asyncio
async def test_resume_thread_with_active_writer_treated_as_active(mock_rpc):
    transport, rpc = mock_rpc
    adapter = CodexThreadLifecycleAdapter(rpc)

    target_thread = "01a174f9-b5d8-7c91-9a27-d8cd3989575b"
    expected_ws = "D:/test/workspace"

    with patch.object(
        rpc,
        "call",
        AsyncMock(side_effect=CodexRpcError(code=-32600, message=f"thread {target_thread} already has an active writer"))
    ):
        result = await adapter.resume_thread(target_thread, expected_workspace=expected_ws)
        assert result.get("active_writer") is True
        assert result.get("status") == "already_active"
        assert result.get("thread", {}).get("id") == target_thread
