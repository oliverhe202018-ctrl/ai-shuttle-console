import pytest
from unittest.mock import AsyncMock, MagicMock
from httpx import AsyncClient, ASGITransport
from app.database import Database
from app.main import app
import app.main as main
from app.models import AutoRunState


@pytest.fixture
async def temp_db(tmp_path):
    db_file = tmp_path / "test.db"
    db = Database(db_file)
    await db.init()
    return db


@pytest.mark.asyncio
async def test_codex_lifecycle_endpoints(temp_db):
    main.db = temp_db

    mock_mgr = MagicMock()
    mock_mgr.get_capabilities = AsyncMock(return_value={
        "cli_path": "codex",
        "thread_resume_supported": True,
        "thread_read_supported": True,
        "thread_loaded_list_supported": True,
        "thread_queue_supported": False,
        "experimental_queue_start_enabled": False,
    })
    mock_mgr.is_thread_cold = AsyncMock(return_value=(True, "notLoaded"))
    mock_mgr.resume_thread_safely = AsyncMock(return_value={"id": "01a174f9-b5d8-7c91-9a27-d8cd3989575b", "status": "loaded"})

    main.app_server_manager = mock_mgr

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
        # Capabilities
        res = await client.get("/api/codex/capabilities")
        assert res.status_code == 200
        assert res.json()["thread_resume_supported"] is True

        # Thread Status
        res = await client.get("/api/codex/thread/status?thread_id=01a174f9-b5d8-7c91-9a27-d8cd3989575b")
        assert res.status_code == 200
        data = res.json()
        assert data["is_cold"] is True
        assert data["status_type"] == "notLoaded"

        # Resume thread
        res = await client.post("/api/codex/thread/resume", json={
            "thread_id": "01a174f9-b5d8-7c91-9a27-d8cd3989575b",
            "expected_workspace": "D:/project/ai-shuttle-console/canary_workspace"
        })
        assert res.status_code == 200
        assert res.json()["status"] == "resumed"


@pytest.mark.asyncio
async def test_auto_mode_endpoints(temp_db):
    main.db = temp_db

    mock_coord = MagicMock()
    mock_coord.is_running = False
    mock_coord.arm_run = AsyncMock(return_value={
        "run_id": "auto-test-12345",
        "status": AutoRunState.AUTO_ARMED.value,
        "max_iterations": 3,
        "operator_confirmed": True
    })
    mock_coord.start_run = AsyncMock()
    mock_coord.request_pause = AsyncMock()
    mock_coord.emergency_stop = AsyncMock()
    mock_coord.abort_run = AsyncMock()
    mock_coord.resume_after_human_action = AsyncMock()

    main.auto_coordinator = mock_coord

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8765") as client:
        # Authorize
        res = await client.post("/api/auto/authorize", json={
            "max_iterations": 3,
            "operator_confirmed": True,
            "allow_cold_resume": True,
            "ttl_minutes": 15,
        })
        assert res.status_code == 200
        assert res.json()["run_id"] == "auto-test-12345"

        # Start
        res = await client.post("/api/auto/start?run_id=auto-test-12345")
        assert res.status_code == 200
        assert res.json()["status"] == "started"

        # Status
        res = await client.get("/api/auto/status")
        assert res.status_code == 200
        assert "active_run" in res.json()

        # Pause
        res = await client.post("/api/auto/pause", json={
            "run_id": "auto-test-12345",
            "reason": "Test pause"
        })
        assert res.status_code == 200
        assert res.json()["status"] == "pause_requested"

        # Emergency Stop
        res = await client.post("/api/auto/emergency_stop", json={
            "run_id": "auto-test-12345",
            "reason": "Test emergency stop"
        })
        assert res.status_code == 200
        assert res.json()["status"] == "stopped"

        # Abort
        res = await client.post("/api/auto/abort?run_id=auto-test-12345&reason=Test+abort")
        assert res.status_code == 200
        assert res.json()["status"] == "aborted"

        # Resume Human Action
        res = await client.post("/api/auto/resume_human", json={
            "run_id": "auto-test-12345"
        })
        assert res.status_code == 200
        assert res.json()["status"] == "resumed"
