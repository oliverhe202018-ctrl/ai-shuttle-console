"""The regression suite must never submit to a real external destination."""
import subprocess
import sqlite3
import asyncio
from unittest.mock import AsyncMock

import pytest

from app.adapters import RealChatGPTMessageSender


@pytest.fixture(autouse=True)
def prohibit_live_delivery(monkeypatch):
    async def forbidden_send(*args, **kwargs):
        pytest.fail("Real browser delivery is forbidden in regression tests")

    original_run = subprocess.run

    def guarded_run(args, *extra, **kwargs):
        if isinstance(args, (list, tuple)) and "queue" in args and "--help" not in args:
            pytest.fail("Real Codex queue submission is forbidden in regression tests")
        return original_run(args, *extra, **kwargs)

    monkeypatch.setattr(RealChatGPTMessageSender, "_send_with_live_cdp", forbidden_send)
    monkeypatch.setattr(subprocess, "run", guarded_run)


@pytest.fixture(autouse=True)
def fast_temporary_sqlite(monkeypatch, tmp_path):
    """Keep file-backed restart tests without expensive fsyncs in disposable DBs.

    This does not change production durability settings or test application code.
    """
    original_connect = sqlite3.connect

    def connect(database, *args, **kwargs):
        connection = original_connect(database, *args, **kwargs)
        from pathlib import Path
        try:
            Path(database).resolve().relative_to(tmp_path.resolve())
        except (ValueError, TypeError):
            return connection
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA journal_mode=MEMORY")
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)


@pytest.fixture(autouse=True)
def isolate_runtime_caches(monkeypatch, tmp_path):
    import app.main as m
    import app.canary_bootstrap as bootstrap
    monkeypatch.setattr(bootstrap, "STATE_FILE", tmp_path / "bootstrap-state.json")
    monkeypatch.setattr(m, "runtime_identity", None)
    monkeypatch.setattr(m, "runtime_browser_status", {})
    monkeypatch.setattr(m, "runtime_codex_status", {})
    monkeypatch.setattr(m, "runtime_lock", asyncio.Lock())
    monkeypatch.setattr(m, "transfer_lock", asyncio.Lock())


@pytest.fixture
def verified_runtime(tmp_path, monkeypatch):
    """Supply the newly required runtime evidence at the external I/O seam."""
    async def install(config, database):
        import json
        from pathlib import Path
        import app.main as m
        from app.adapters import ReadOnlyCodexSessionObserver, RealChatGPTBrowserObserver
        from app.canary import compute_config_fingerprint
        source = tmp_path / "selected-canary.json"
        source.write_text(config.model_dump_json(), encoding="utf-8")
        monkeypatch.setenv("SHUTTLE_CONFIG", str(source))
        monkeypatch.setenv("SHUTTLE_CANARY", "1")
        sessions = Path(config.codex.sessions_dir)
        sessions.relative_to(tmp_path)
        sessions.mkdir(exist_ok=True, parents=True)
        index = Path(config.codex.session_index_path)
        if not index.exists() or not index.read_text(encoding="utf-8").strip():
            index.write_text(json.dumps({"id": config.codex.thread_id, "cwd": config.codex.repo_path}) + "\n", encoding="utf-8")
        session = sessions / f"rollout-{config.codex.thread_id}.jsonl"
        if not session.exists():
            session.write_text(json.dumps({"type": "session_meta", "payload": {
                "id": config.codex.thread_id, "cwd": config.codex.repo_path}}) + "\n", encoding="utf-8")
        monkeypatch.setattr(m, "codex_observer", ReadOnlyCodexSessionObserver(sessions, index))
        monkeypatch.setattr(RealChatGPTBrowserObserver, "read_live_page", AsyncMock(return_value={
            "url": config.chatgpt.conversation_url,
            "html": '<form data-chatgpt-composer><textarea id="prompt-textarea"></textarea></form>'}))
        await database.save_canary_bootstrap_state({"verdict": "CANARY_PREFLIGHT_VERIFIED",
            "config_path": str(source), "config_fingerprint": compute_config_fingerprint(config),
            "preflight_result": {"status": "verified"}})
    return install

@pytest.fixture(autouse=True)
def mock_codex_cli_file(monkeypatch):
    from pathlib import Path
    orig_is_file = Path.is_file

    def mock_is_file(self):
        if "codex.exe" in str(self).lower():
            return True
        return orig_is_file(self)

    monkeypatch.setattr(Path, "is_file", mock_is_file)
