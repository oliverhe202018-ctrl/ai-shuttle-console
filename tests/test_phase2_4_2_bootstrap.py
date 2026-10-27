"""
Phase 2.4.2 — Automated Canary Environment Bootstrap Tests.
Validates all deterministic local discovery, configuration generation,
fail-closed human-action stops, and console endpoints.
"""
import asyncio
from datetime import datetime
import json
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.canary import (
    BASE_DIR,
    CANARY_WORKSPACE,
    PRODUCTION_CONVERSATION,
    PRODUCTION_THREAD,
    load_explicit_config,
)
from app.canary_bootstrap import (
    ACTION_CHATGPT_LOGIN,
    ACTION_CREATE_CHATGPT_CANARY_CHAT,
    ACTION_CREATE_CODEX_CANARY_THREAD,
    CANARY_BOOTSTRAP_BLOCKED,
    CANARY_PREFLIGHT_VERIFIED,
    CANARY_PROFILE,
    HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN,
    HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT,
    HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD,
    check_or_launch_canary_edge,
    discover_codex_canary_target,
    find_and_verify_codex_cli,
    generate_canary_config,
    inspect_chatgpt_canary,
    run_canary_bootstrap,
    run_official_preflight,
)
from app.main import app


@pytest.fixture
def mock_codex_env(tmp_path):
    """Sets up an isolated mock sessions dir and session_index."""
    ws = tmp_path / "canary_workspace"
    ws.mkdir(parents=True, exist_ok=True)
    sessions_dir = tmp_path / ".codex" / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    index_file = tmp_path / ".codex" / "session_index.jsonl"
    index_file.write_text("", encoding="utf-8")
    return {
        "workspace": ws,
        "sessions_dir": sessions_dir,
        "session_index": index_file,
    }


def create_session_file(sessions_dir: Path, thread_id: str, cwd: Path, title: str = "Test Session") -> Path:
    today_dir = sessions_dir / "2026" / "10" / "26"
    today_dir.mkdir(parents=True, exist_ok=True)
    file_path = today_dir / f"rollout-2026-10-26-{thread_id}.jsonl"
    meta = {
        "type": "session_meta",
        "payload": {
            "id": thread_id,
            "session_id": thread_id,
            "cwd": str(cwd),
            "thread_name": title,
            "timestamp": "2026-10-26T10:00:00Z",
        },
    }
    file_path.write_text(json.dumps(meta) + "\n", encoding="utf-8")
    return file_path


def add_index_entry(index_file: Path, thread_id: str, title: str, cwd: Path):
    entry = {
        "id": thread_id,
        "thread_name": title,
        "cwd": str(cwd),
        "updated_at": "2026-10-26T10:00:00Z",
    }
    with open(index_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# 1. No Codex canary thread
def test_no_codex_canary_thread(mock_codex_env):
    res = discover_codex_canary_target(
        workspace=mock_codex_env["workspace"],
        sessions_dir=mock_codex_env["sessions_dir"],
        session_index_path=mock_codex_env["session_index"],
    )
    assert res["status"] == HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD
    assert "No Codex session found" in res["error"]
    assert str(mock_codex_env["workspace"].resolve()) in res["human_action"]
    assert "CANARY-THREAD-READY" in res["human_action"]


# 2. Exactly one valid Codex canary thread
def test_exactly_one_valid_codex_canary_thread(mock_codex_env):
    tid = str(uuid.uuid4())
    s_file = create_session_file(mock_codex_env["sessions_dir"], tid, mock_codex_env["workspace"], "My Canary Thread")
    add_index_entry(mock_codex_env["session_index"], tid, "My Canary Thread", mock_codex_env["workspace"])

    res = discover_codex_canary_target(
        workspace=mock_codex_env["workspace"],
        sessions_dir=mock_codex_env["sessions_dir"],
        session_index_path=mock_codex_env["session_index"],
    )
    assert res["status"] == "discovered"
    assert res["thread_id"] == tid
    assert res["title"] == "My Canary Thread"
    assert res["session_file_path"] == str(s_file.resolve())
    assert res["cwd"] == str(mock_codex_env["workspace"].resolve())


# 3. Multiple candidate Codex threads
def test_multiple_candidate_codex_threads(mock_codex_env):
    tid1 = str(uuid.uuid4())
    tid2 = str(uuid.uuid4())
    create_session_file(mock_codex_env["sessions_dir"], tid1, mock_codex_env["workspace"], "Thread 1")
    create_session_file(mock_codex_env["sessions_dir"], tid2, mock_codex_env["workspace"], "Thread 2")
    add_index_entry(mock_codex_env["session_index"], tid1, "Thread 1", mock_codex_env["workspace"])
    add_index_entry(mock_codex_env["session_index"], tid2, "Thread 2", mock_codex_env["workspace"])

    res = discover_codex_canary_target(
        workspace=mock_codex_env["workspace"],
        sessions_dir=mock_codex_env["sessions_dir"],
        session_index_path=mock_codex_env["session_index"],
    )
    assert res["status"] == CANARY_BOOTSTRAP_BLOCKED
    assert "Multiple candidate Codex sessions found" in res["error"]
    assert len(res["candidates"]) == 2
    tids = [c["thread_id"] for c in res["candidates"]]
    assert tid1 in tids and tid2 in tids


# 4. Wrong CWD ignored
def test_wrong_cwd_ignored(mock_codex_env, tmp_path):
    tid = str(uuid.uuid4())
    other_ws = tmp_path / "other_project"
    other_ws.mkdir()
    create_session_file(mock_codex_env["sessions_dir"], tid, other_ws, "Other Project")
    add_index_entry(mock_codex_env["session_index"], tid, "Other Project", other_ws)

    res = discover_codex_canary_target(
        workspace=mock_codex_env["workspace"],
        sessions_dir=mock_codex_env["sessions_dir"],
        session_index_path=mock_codex_env["session_index"],
    )
    assert res["status"] == HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD


# 5. Production thread rejection
def test_production_thread_rejection(mock_codex_env):
    # Even if someone pointed production thread to canary workspace
    create_session_file(mock_codex_env["sessions_dir"], PRODUCTION_THREAD, mock_codex_env["workspace"], "Prod Thread")
    add_index_entry(mock_codex_env["session_index"], PRODUCTION_THREAD, "Prod Thread", mock_codex_env["workspace"])

    res = discover_codex_canary_target(
        workspace=mock_codex_env["workspace"],
        sessions_dir=mock_codex_env["sessions_dir"],
        session_index_path=mock_codex_env["session_index"],
    )
    assert res["status"] == HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD


# 6. Missing Edge instance
def test_missing_edge_instance(tmp_path):
    profile = tmp_path / "profile"
    with patch("app.canary_bootstrap.is_port_listening", return_value=False):
        res = check_or_launch_canary_edge(profile_dir=profile, cdp_port=9299, auto_launch=False)
        assert res["status"] == "missing"
        assert "not listening" in res["error"]


# 7. Existing correct Edge instance reused
def test_existing_edge_instance_reused(tmp_path):
    profile = tmp_path / "profile"
    with patch("app.canary_bootstrap.is_port_listening", return_value=True), \
         patch("app.canary_bootstrap.get_pid_listening_on_port", return_value=12345), \
         patch("app.canary_bootstrap.get_process_cmdline", return_value=f"msedge.exe --user-data-dir={profile}"):
        res = check_or_launch_canary_edge(profile_dir=profile, cdp_port=9299, auto_launch=False)
        assert res["status"] == "reused"
        assert res["process_id"] == 12345
        assert res["cdp_url"] == "http://127.0.0.1:9299"


# 8. Login or verification required
@pytest.mark.asyncio
async def test_chatgpt_login_or_verification_required():
    mock_turnstile = [
        {"type": "page", "url": "https://chatgpt.com/", "title": "Just a moment... Verify you are human"}
    ]
    res1 = await inspect_chatgpt_canary(mock_pages=mock_turnstile)
    assert res1["status"] == HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN
    assert "Turnstile" in res1["error"]

    mock_auth_redirect = [
        {"type": "page", "url": "https://auth.openai.com/authorize", "title": "Log in"}
    ]
    res2 = await inspect_chatgpt_canary(mock_pages=mock_auth_redirect)
    assert res2["status"] == HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN
    assert "login required" in res2["error"].lower()


# 9. Missing ChatGPT test conversation
@pytest.mark.asyncio
async def test_missing_chatgpt_test_conversation():
    mock_landing = [
        {"type": "page", "url": "https://chatgpt.com/", "title": "ChatGPT"}
    ]
    res = await inspect_chatgpt_canary(mock_pages=mock_landing)
    assert res["status"] == HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT
    assert "No disposable ChatGPT conversation" in res["error"]
    assert res["human_action"] == ACTION_CREATE_CHATGPT_CANARY_CHAT


# 10. Production ChatGPT conversation rejection
@pytest.mark.asyncio
async def test_production_chatgpt_conversation_rejection():
    mock_prod = [
        {"type": "page", "url": f"https://chatgpt.com/c/{PRODUCTION_CONVERSATION}", "title": "Prod Chat"}
    ]
    res = await inspect_chatgpt_canary(mock_pages=mock_prod)
    assert res["status"] == CANARY_BOOTSTRAP_BLOCKED
    assert "Protected production ChatGPT conversation" in res["error"]


# 11. Real conversation identity extraction (/c/ and /g/.../c/)
@pytest.mark.asyncio
async def test_real_conversation_identity_extraction():
    cid1 = str(uuid.uuid4())
    mock_plain = [
        {"type": "page", "url": f"https://chatgpt.com/c/{cid1}", "title": "Canary Plain Chat"}
    ]
    res1 = await inspect_chatgpt_canary(mock_pages=mock_plain)
    assert res1["status"] == "discovered"
    assert res1["conversation_id"] == cid1
    assert res1["gpt_id"] is None

    cid2 = str(uuid.uuid4())
    gpt_id = "g-p-testgpt123"
    mock_gpt = [
        {"type": "page", "url": f"https://chatgpt.com/g/{gpt_id}/c/{cid2}", "title": "Canary GPT Chat"}
    ]
    res2 = await inspect_chatgpt_canary(mock_pages=mock_gpt)
    assert res2["status"] == "discovered"
    assert res2["conversation_id"] == cid2
    assert res2["gpt_id"] == gpt_id


# 12. Stale CLI path replacement
def test_stale_cli_path_replacement(tmp_path):
    stale_path = tmp_path / "stale_dir" / "codex.exe"
    # Should ignore non-existent stale path and fall back to discovering real installed CLI
    res = find_and_verify_codex_cli(preferred_path=stale_path)
    # On this system, the real CLI is present and verified
    assert res["status"] == "verified"
    assert res["cli_path"] != str(stale_path)
    assert "codex.exe" in res["cli_path"].lower()
    assert res["has_thread"] and res["has_message"] and res["has_cd"]


# 13. Explicit-config creation without overwrite
def test_generate_canary_config_no_overwrite(tmp_path):
    tid = str(uuid.uuid4())
    cid = str(uuid.uuid4())
    codex_t = {
        "thread_id": tid,
        "title": "Canary Thread",
        "cwd": "D:/project/ai-shuttle-console/canary_workspace",
    }
    chatgpt_t = {
        "conversation_url": f"https://chatgpt.com/c/{cid}",
        "conversation_id": cid,
        "gpt_id": None,
        "profile_dir": "D:/project/ai-shuttle-console/.canary-chatgpt-profile",
        "cdp_url": "http://127.0.0.1:9223",
    }
    dummy_cli = tmp_path / "codex.exe"
    dummy_cli.touch()
    cli_path = str(dummy_cli)

    out_file = tmp_path / "config.canary.local.test.json"
    cfg_path = generate_canary_config(
        codex_target=codex_t,
        chatgpt_target=chatgpt_t,
        cli_path=cli_path,
        output_path=out_file,
    )
    assert cfg_path.is_file()
    loaded = load_explicit_config(cfg_path, canary=True)
    assert loaded.canary_mode is True
    assert loaded.codex.thread_id == tid
    assert loaded.chatgpt.conversation_id == cid

    # Attempt overwrite must raise FileExistsError
    with pytest.raises(FileExistsError):
        generate_canary_config(
            codex_target=codex_t,
            chatgpt_target=chatgpt_t,
            cli_path=cli_path,
            output_path=out_file,
        )


# 14. Successful read-only preflight
def test_successful_readonly_preflight(tmp_path):
    # Mock subprocess.run for run.py --check
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="{'status': 'verified'}", stderr="")
        dummy_cfg = tmp_path / "dummy.json"
        dummy_cfg.write_text("{}", encoding="utf-8")
        res = run_official_preflight(dummy_cfg)
        assert res["status"] == "verified"
        assert res["exit_code"] == 0


# 15. Human action state persistence and console API
@pytest.mark.asyncio
async def test_human_action_state_and_console_api():
    transport = ASGITransport(app=app)
    with patch("app.canary_bootstrap.discover_codex_canary_target") as mock_disc:
        mock_disc.return_value = {
            "status": HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD,
            "workspace": str(CANARY_WORKSPACE.resolve()),
            "error": f"No Codex session found with working directory '{CANARY_WORKSPACE.resolve()}'",
            "human_action": ACTION_CREATE_CODEX_CANARY_THREAD,
            "candidates": [],
        }
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            res = await client.get("/api/canary/bootstrap/status")
            assert res.status_code == 200
            data = res.json()
            assert "verdict" in data
            assert "subsystems" in data
            assert data["verdict"] == HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD
            assert data["human_action"] is not None
            assert "canary_workspace" in data["human_action"]

            recheck = await client.post("/api/canary/bootstrap/recheck")
            assert recheck.status_code == 200
            recheck_data = recheck.json()
            assert recheck_data["verdict"] == HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD
