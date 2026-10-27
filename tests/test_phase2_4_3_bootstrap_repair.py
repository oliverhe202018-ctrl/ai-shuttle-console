"""
Phase 2.4.3 — Canary Bootstrap Blockage Diagnosis and Repair Tests.
Regression test suite covering:
- All 3 subsystems discovered with create_config=False re-verifies matching config without BLOCKED None.
- Actionable blocker and stable error code when no config exists yet.
- Config generation success, refusal to overwrite, and custom session/index directories.
- Production thread and conversation ID rejection.
- Invalid or missing configuration fields.
- Windows path normalization (same_path).
- Unsupported preflight CLI arguments.
- Preflight subprocess failure reporting.
- Error details survival across API endpoints.
- UI template blocker fallback preventing "Exact Blocker: None" during BLOCKED.
"""
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters import ReadOnlyCodexSessionObserver
from app.canary import (
    BASE_DIR,
    CANARY_WORKSPACE,
    PRODUCTION_CONVERSATION,
    PRODUCTION_THREAD,
    load_explicit_config,
    same_path,
    same_conversation,
    validate_canary_config,
)
from app.canary_bootstrap import (
    CANARY_BOOTSTRAP_BLOCKED,
    CANARY_PREFLIGHT_VERIFIED,
    CANARY_PROFILE,
    find_and_verify_codex_cli,
    find_matching_canary_config,
    generate_canary_config,
    run_canary_bootstrap,
    run_official_preflight,
)
from app.main import app


# 1. Defect reproduction & fix: create_config=False re-verifies matching existing config
@pytest.mark.asyncio
async def test_original_defect_all_discovered_create_config_false_handles_existing_config(tmp_path):
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    codex_t = {
        "status": "discovered",
        "thread_id": tid,
        "title": "Canary Thread",
        "cwd": str(CANARY_WORKSPACE.resolve()),
        "sessions_dir": str(Path.home() / ".codex" / "sessions"),
        "session_index_path": str(Path.home() / ".codex" / "session_index.jsonl"),
    }
    chatgpt_t = {
        "status": "discovered",
        "conversation_url": f"https://chatgpt.com/c/{cid}",
        "conversation_id": cid,
        "gpt_id": None,
        "title": "Canary Chat",
        "cdp_url": "http://127.0.0.1:9223",
        "profile_dir": str(CANARY_PROFILE.resolve()),
    }
    cli_path = "C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe"

    # Generate a matching config in tmp_path
    cfg_file = tmp_path / "config.canary.local.20261026-99.json"
    generate_canary_config(
        codex_target=codex_t,
        chatgpt_target=chatgpt_t,
        cli_path=cli_path,
        output_path=cfg_file,
    )

    with patch("app.canary_bootstrap.find_and_verify_codex_cli") as mock_cli, \
         patch("app.canary_bootstrap.check_or_launch_canary_edge") as mock_edge, \
         patch("app.canary_bootstrap.inspect_chatgpt_canary", new_callable=AsyncMock) as mock_chat, \
         patch("app.canary_bootstrap.discover_codex_canary_target") as mock_codex, \
         patch("app.canary_bootstrap.run_official_preflight") as mock_pref, \
         patch("app.canary_bootstrap.BASE_DIR", tmp_path):

        mock_cli.return_value = {"status": "verified", "cli_path": cli_path}
        mock_edge.return_value = {"status": "reused", "profile_dir": str(CANARY_PROFILE), "cdp_url": "http://127.0.0.1:9223"}
        mock_chat.return_value = chatgpt_t
        mock_codex.return_value = codex_t
        mock_pref.return_value = {"exit_code": 0, "status": "verified", "output": "{'status': 'verified'}"}

        res = await run_canary_bootstrap(auto_launch_edge=False, create_config=False)

        assert res["verdict"] == CANARY_PREFLIGHT_VERIFIED
        assert res["stage"] == "preflight_verified"
        assert res["error_code"] is None
        assert res["blocker"] is None
        assert res["config_path"] == str(cfg_file)
        assert res["preflight_result"]["status"] == "verified"


# 2. When all discovered but no config exists yet, create_config=False provides actionable blocker, never None
@pytest.mark.asyncio
async def test_all_discovered_no_config_yet_create_config_false_actionable_blocker(tmp_path):
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    codex_t = {
        "status": "discovered",
        "thread_id": tid,
        "title": "Canary Thread",
        "cwd": str(CANARY_WORKSPACE.resolve()),
    }
    chatgpt_t = {
        "status": "discovered",
        "conversation_url": f"https://chatgpt.com/c/{cid}",
        "conversation_id": cid,
        "cdp_url": "http://127.0.0.1:9223",
        "profile_dir": str(CANARY_PROFILE.resolve()),
    }
    cli_path = "C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe"

    with patch("app.canary_bootstrap.find_and_verify_codex_cli") as mock_cli, \
         patch("app.canary_bootstrap.check_or_launch_canary_edge") as mock_edge, \
         patch("app.canary_bootstrap.inspect_chatgpt_canary", new_callable=AsyncMock) as mock_chat, \
         patch("app.canary_bootstrap.discover_codex_canary_target") as mock_codex, \
         patch("app.canary_bootstrap.BASE_DIR", tmp_path):

        mock_cli.return_value = {"status": "verified", "cli_path": cli_path}
        mock_edge.return_value = {"status": "reused", "profile_dir": str(CANARY_PROFILE), "cdp_url": "http://127.0.0.1:9223"}
        mock_chat.return_value = chatgpt_t
        mock_codex.return_value = codex_t

        res = await run_canary_bootstrap(auto_launch_edge=False, create_config=False)

        assert res["verdict"] == CANARY_BOOTSTRAP_BLOCKED
        assert res["stage"] == "configuration_generation"
        assert res["error_code"] == "CONFIG_NOT_YET_GENERATED"
        assert res["blocker"] is not None
        assert "Read-Only Recheck" in res["blocker"]
        assert res["retryable"] is True


# 3. Config generation success
def test_config_generation_success(tmp_path):
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    out_file = tmp_path / "config.canary.local.success.json"
    cfg = generate_canary_config(
        codex_target={
            "thread_id": tid,
            "title": "Success Thread",
            "cwd": str(CANARY_WORKSPACE.resolve()),
        },
        chatgpt_target={
            "conversation_url": f"https://chatgpt.com/c/{cid}",
            "conversation_id": cid,
            "profile_dir": str(CANARY_PROFILE.resolve()),
            "cdp_url": "http://127.0.0.1:9223",
        },
        cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        output_path=out_file,
    )
    assert cfg.is_file()
    loaded = load_explicit_config(cfg, canary=True)
    assert loaded.canary_mode is True
    assert loaded.codex.thread_id == tid
    assert loaded.chatgpt.conversation_id == cid
    assert loaded.route.manual_approval_required is True
    validate_canary_config(loaded)


# 4. Existing config is never overwritten
def test_existing_config_never_overwritten(tmp_path):
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    out_file = tmp_path / "config.canary.local.no_overwrite.json"
    generate_canary_config(
        codex_target={
            "thread_id": tid,
            "title": "First",
            "cwd": str(CANARY_WORKSPACE.resolve()),
        },
        chatgpt_target={
            "conversation_url": f"https://chatgpt.com/c/{cid}",
            "conversation_id": cid,
            "profile_dir": str(CANARY_PROFILE.resolve()),
            "cdp_url": "http://127.0.0.1:9223",
        },
        cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        output_path=out_file,
    )
    with pytest.raises(FileExistsError):
        generate_canary_config(
            codex_target={
                "thread_id": tid,
                "title": "Second",
                "cwd": str(CANARY_WORKSPACE.resolve()),
            },
            chatgpt_target={
                "conversation_url": f"https://chatgpt.com/c/{cid}",
                "conversation_id": cid,
                "profile_dir": str(CANARY_PROFILE.resolve()),
                "cdp_url": "http://127.0.0.1:9223",
            },
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
            output_path=out_file,
        )


# 5. Production thread and conversation IDs remain rejected
def test_production_thread_and_conversation_rejection():
    # Production thread rejection
    with pytest.raises(ValueError, match="Protected production thread ID"):
        generate_canary_config(
            codex_target={
                "thread_id": PRODUCTION_THREAD,
                "title": "Prod",
                "cwd": str(CANARY_WORKSPACE.resolve()),
            },
            chatgpt_target={
                "conversation_url": f"https://chatgpt.com/c/{str(uuid.uuid4())}",
                "conversation_id": str(uuid.uuid4()),
                "profile_dir": str(CANARY_PROFILE.resolve()),
            },
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        )

    # Production conversation rejection
    with pytest.raises(ValueError, match="Protected production ChatGPT conversation"):
        generate_canary_config(
            codex_target={
                "thread_id": str(uuid.uuid4()),
                "title": "Canary",
                "cwd": str(CANARY_WORKSPACE.resolve()),
            },
            chatgpt_target={
                "conversation_url": f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}",
                "conversation_id": PRODUCTION_CONVERSATION,
                "profile_dir": str(CANARY_PROFILE.resolve()),
            },
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        )


# 6. Invalid or missing configuration fields
def test_invalid_or_missing_configuration_fields():
    cid = str(uuid.uuid4())
    tid = str(uuid.uuid4())

    # Invalid thread UUID
    with pytest.raises(ValueError, match="UUID"):
        generate_canary_config(
            codex_target={"thread_id": "not-a-uuid", "cwd": str(CANARY_WORKSPACE.resolve())},
            chatgpt_target={"conversation_url": f"https://chatgpt.com/c/{cid}", "conversation_id": cid},
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        )

    # Invalid conversation UUID
    with pytest.raises(ValueError, match="UUID"):
        generate_canary_config(
            codex_target={"thread_id": tid, "cwd": str(CANARY_WORKSPACE.resolve())},
            chatgpt_target={"conversation_url": "https://chatgpt.com/c/not-a-uuid", "conversation_id": "not-a-uuid"},
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        )

    # Mismatched CWD
    with pytest.raises(ValueError, match="does not match canary workspace"):
        generate_canary_config(
            codex_target={"thread_id": tid, "cwd": "D:/some/other/project"},
            chatgpt_target={"conversation_url": f"https://chatgpt.com/c/{cid}", "conversation_id": cid},
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        )

    # Mismatched browser profile
    with pytest.raises(ValueError, match="does not match dedicated canary profile"):
        generate_canary_config(
            codex_target={"thread_id": tid, "cwd": str(CANARY_WORKSPACE.resolve())},
            chatgpt_target={
                "conversation_url": f"https://chatgpt.com/c/{cid}",
                "conversation_id": cid,
                "profile_dir": "D:/other/profile",
            },
            cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        )


# 7. Session file and index outside default .codex location
def test_session_and_index_outside_default_location(tmp_path):
    custom_sessions = tmp_path / "DevCache" / "codex_sessions"
    custom_sessions.mkdir(parents=True, exist_ok=True)
    custom_index = tmp_path / "custom_codex" / "session_index.jsonl"
    custom_index.parent.mkdir(parents=True, exist_ok=True)
    custom_index.write_text("", encoding="utf-8")

    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    out_file = tmp_path / "config.canary.local.custom_dirs.json"

    cfg = generate_canary_config(
        codex_target={
            "thread_id": tid,
            "title": "Custom Dir Canary",
            "cwd": str(CANARY_WORKSPACE.resolve()),
            "sessions_dir": str(custom_sessions),
            "session_index_path": str(custom_index),
        },
        chatgpt_target={
            "conversation_url": f"https://chatgpt.com/c/{cid}",
            "conversation_id": cid,
            "profile_dir": str(CANARY_PROFILE.resolve()),
            "cdp_url": "http://127.0.0.1:9223",
        },
        cli_path="C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe",
        output_path=out_file,
    )
    loaded = load_explicit_config(cfg, canary=True)
    assert same_path(loaded.codex.sessions_dir, custom_sessions)
    assert same_path(loaded.codex.session_index_path, custom_index)

    # Observer instantiates with these custom paths without error
    obs = ReadOnlyCodexSessionObserver(Path(loaded.codex.sessions_dir), Path(loaded.codex.session_index_path))
    assert same_path(obs.sessions_base_dir, custom_sessions)
    assert same_path(obs.session_index_path, custom_index)


# 8. Windows path normalization
def test_windows_path_normalization():
    # Case differences
    assert same_path("c:/users/test/workspace", "C:/Users/test/workspace")
    assert same_path("d:/project/canary", "D:\\project\\canary")

    # Slashes and trailing separators
    assert same_path("D:/project/canary/", "D:\\project\\canary")
    assert same_path("D:/project/./canary", "D:/project/canary")

    # Different paths
    assert not same_path("D:/project/canary", "D:/project/other")
    assert not same_path(None, "D:/project/canary")
    assert not same_path("", "")


# 9. Unsupported preflight arguments
def test_unsupported_preflight_arguments():
    py = str(BASE_DIR / ".venv" / "Scripts" / "python.exe")
    if not Path(py).is_file():
        py = sys.executable

    # --check without --canary or --config fails with code 2
    res1 = subprocess.run([py, "run.py", "--check"], cwd=str(BASE_DIR), capture_output=True, text=True)
    assert res1.returncode == 2

    # --canary without --config fails with code 2
    res2 = subprocess.run([py, "run.py", "--canary"], cwd=str(BASE_DIR), capture_output=True, text=True)
    assert res2.returncode == 2


# 10. Preflight subprocess failure reporting
@pytest.mark.asyncio
async def test_preflight_subprocess_failure_reporting(tmp_path):
    # Ensure template exists in tmp_path
    (tmp_path / "config.canary.example.json").write_text(
        (BASE_DIR / "config.canary.example.json").read_text(encoding="utf-8"),
        encoding="utf-8"
    )
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    codex_t = {
        "status": "discovered",
        "thread_id": tid,
        "title": "Canary Thread",
        "cwd": str(CANARY_WORKSPACE.resolve()),
    }
    chatgpt_t = {
        "status": "discovered",
        "conversation_url": f"https://chatgpt.com/c/{cid}",
        "conversation_id": cid,
        "cdp_url": "http://127.0.0.1:9223",
        "profile_dir": str(CANARY_PROFILE.resolve()),
    }
    cli_path = "C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe"

    with patch("app.canary_bootstrap.find_and_verify_codex_cli") as mock_cli, \
         patch("app.canary_bootstrap.check_or_launch_canary_edge") as mock_edge, \
         patch("app.canary_bootstrap.inspect_chatgpt_canary", new_callable=AsyncMock) as mock_chat, \
         patch("app.canary_bootstrap.discover_codex_canary_target") as mock_codex, \
         patch("app.canary_bootstrap.run_official_preflight") as mock_pref, \
         patch("app.canary_bootstrap.BASE_DIR", tmp_path):

        mock_cli.return_value = {"status": "verified", "cli_path": cli_path}
        mock_edge.return_value = {"status": "reused", "profile_dir": str(CANARY_PROFILE), "cdp_url": "http://127.0.0.1:9223"}
        mock_chat.return_value = chatgpt_t
        mock_codex.return_value = codex_t
        mock_pref.return_value = {"exit_code": 1, "status": "failed", "error": "Canary browser composer is not ready"}

        res = await run_canary_bootstrap(auto_launch_edge=False, create_config=True)

        assert res["verdict"] == CANARY_BOOTSTRAP_BLOCKED
        assert res["stage"] == "preflight_check"
        assert res["error_code"] == "PREFLIGHT_FAILED"
        assert "Canary browser composer is not ready" in res["blocker"]
        assert res["retryable"] is True
        assert res["preflight_result"]["status"] == "failed"


# 11. Config generation failure reporting when targets invalid
@pytest.mark.asyncio
async def test_both_identities_discovered_but_config_generation_rejected(tmp_path):
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    # Invalid target: CWD does not match canary_workspace
    codex_t = {
        "status": "discovered",
        "thread_id": tid,
        "title": "Canary Thread",
        "cwd": "D:/wrong/path",
    }
    chatgpt_t = {
        "status": "discovered",
        "conversation_url": f"https://chatgpt.com/c/{cid}",
        "conversation_id": cid,
        "cdp_url": "http://127.0.0.1:9223",
        "profile_dir": str(CANARY_PROFILE.resolve()),
    }
    cli_path = "C:/Users/canary_user/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe"

    with patch("app.canary_bootstrap.find_and_verify_codex_cli") as mock_cli, \
         patch("app.canary_bootstrap.check_or_launch_canary_edge") as mock_edge, \
         patch("app.canary_bootstrap.inspect_chatgpt_canary", new_callable=AsyncMock) as mock_chat, \
         patch("app.canary_bootstrap.discover_codex_canary_target") as mock_codex, \
         patch("app.canary_bootstrap.BASE_DIR", tmp_path):

        mock_cli.return_value = {"status": "verified", "cli_path": cli_path}
        mock_edge.return_value = {"status": "reused", "profile_dir": str(CANARY_PROFILE), "cdp_url": "http://127.0.0.1:9223"}
        mock_chat.return_value = chatgpt_t
        mock_codex.return_value = codex_t

        res = await run_canary_bootstrap(auto_launch_edge=False, create_config=True)

        assert res["verdict"] == CANARY_BOOTSTRAP_BLOCKED
        assert res["stage"] == "configuration_generation"
        assert res["error_code"] == "CONFIG_GENERATION_FAILED"
        assert "does not match canary workspace" in res["blocker"]
        assert res["retryable"] is False


# 12. Error details survive backend-frontend response path
@pytest.mark.asyncio
async def test_error_details_survive_backend_response():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/canary/bootstrap/status")
        assert res.status_code == 200
        data = res.json()
        assert "verdict" in data
        assert "stage" in data
        assert "error_code" in data
        assert "blocker" in data
        assert "retryable" in data
        assert "subsystems" in data

        if data["verdict"] == CANARY_BOOTSTRAP_BLOCKED:
            assert data["blocker"] is not None
            assert len(data["blocker"].strip()) > 0
            assert data["error_code"] is not None


# 13. UI template cannot show BLOCKED with empty blocker
def test_ui_template_blocker_fallback():
    html_file = BASE_DIR / "app" / "templates" / "index.html"
    content = html_file.read_text(encoding="utf-8")
    assert 'id="canary-preflight-stage"' in content
    assert 'id="canary-preflight-error-code"' in content
    assert 'id="canary-preflight-blocker"' in content

    # Check updateCanaryUI handles CANARY_BOOTSTRAP_BLOCKED without showing None
    assert "Bootstrap blocked without explicit reason" in content
