"""Synthetic runtime roots are injected; tests never inspect live process roots."""
import json
import sqlite3

import pytest

from app.runtime_inventory import inspect_runtime_inventory, normalized_path
from test_phase2_7_production_shadow import env
from test_transactional_c2g import atomic


def root(tmp_path, name, workspace, unfinished=False, missing=False):
    home = tmp_path / name
    sessions = home / "sessions"
    sessions.mkdir(parents=True)
    path = sessions / "one.jsonl"
    records = [{"type": "session_meta", "payload": {"id": name, "cwd": str(workspace)}},
               {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-one"}}]
    if not unfinished:
        records.append({"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-one"}})
    if not missing:
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    with sqlite3.connect(home / "state_5.sqlite") as db:
        db.execute("CREATE TABLE threads(id,cwd,rollout_path)")
        db.execute("INSERT INTO threads VALUES(?,?,?)", (name, str(workspace), str(path)))
    return {"process_id": len(name), "home": str(home), "sqlite_home": str(home)}


def test_other_live_runtime_root_cannot_be_omitted(tmp_path):
    workspace = tmp_path / "workspace"
    first = root(tmp_path, "primary", workspace)
    other = root(tmp_path, "old-looking-relay", workspace, unfinished=True)
    r = inspect_runtime_inventory(workspace, tmp_path / "primary/sessions", processes=[first, other])
    assert {s["thread_id"] for s in r["sessions"]} == {"primary", "old-looking-relay"}
    assert [b["thread_id"] for b in r["blockers"]] == ["old-looking-relay"]
    assert r["scope"]["complete"]


def test_registered_missing_rollout_is_unknown_not_idle(tmp_path):
    workspace = tmp_path / "workspace"
    process = root(tmp_path, "primary", workspace, missing=True)
    r = inspect_runtime_inventory(workspace, tmp_path / "primary/sessions", processes=[process])
    assert not r["scope"]["complete"]
    assert r["blockers"][0]["reason"] == "REGISTERED_PRODUCTION_ROLLOUT_UNRESOLVED"


def test_readable_closed_roots_are_complete(tmp_path):
    workspace = tmp_path / "workspace"
    processes = [root(tmp_path, name, workspace) for name in ("first", "second")]
    r = inspect_runtime_inventory(workspace, tmp_path / "first/sessions", processes=processes)
    assert r["scope"]["complete"] and not r["blockers"]


def test_discovery_failure_remains_blocking(tmp_path, monkeypatch):
    import app.runtime_inventory as runtime
    workspace = tmp_path / "workspace"
    root(tmp_path, "primary", workspace)
    def unavailable():
        raise ValueError("PRODUCTION_RUNTIME_PROCESS_ACCESS_UNKNOWN")
    monkeypatch.setattr(runtime, "discover_runtime_roots", unavailable)
    r = runtime.inspect_runtime_inventory(workspace, tmp_path / "primary/sessions")
    assert not r["scope"]["complete"]
    assert any(b["reason"] == "PRODUCTION_RUNTIME_PROCESS_ACCESS_UNKNOWN" for b in r["blockers"])


def test_other_workspace_does_not_become_production(tmp_path):
    workspace = tmp_path / "workspace"
    processes = [root(tmp_path, "primary", workspace), root(tmp_path, "other", tmp_path / "different", unfinished=True)]
    r = inspect_runtime_inventory(workspace, tmp_path / "primary/sessions", processes=processes)
    assert not r["blockers"]
    assert len(r["sessions"]) == 1


def test_windows_extended_paths_use_the_same_registry_identity():
    assert normalized_path(r"\\?\C:\workspace") == normalized_path(r"C:\workspace")
    assert normalized_path(r"\\?\UNC\server\share") == normalized_path(r"\\server\share")


@pytest.mark.asyncio
async def test_transaction_and_quiescence_share_unknown_scope_barrier(atomic):
    service = atomic["service"]
    barrier = {"thread_id": None, "classification": "UNKNOWN_LIVENESS", "blocking": True,
               "reason": "REGISTERED_PRODUCTION_ROLLOUT_UNRESOLVED"}
    service.session_inventory = lambda *args: {"sessions": [], "blockers": [barrier], "scope": {"complete": False}}
    assert service._active(atomic["cfg"]) == [barrier]
    result = await service.quiescence()
    assert result["status"] == "PRODUCTION_WORKSPACE_GENUINELY_BUSY"
    assert result["reauth_status"] is None
    assert service.load() is None
    atomic["sender"].send_message.assert_not_awaited()
    atomic["manager"].start_readonly_handoff.assert_not_awaited()
