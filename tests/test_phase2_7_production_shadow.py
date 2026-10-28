"""Synthetic production pairs only; regression tests never contact real services."""
import asyncio
import html
import json
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters import ReadOnlyCodexSessionObserver
from app.auto_mode import AutoModeCoordinator, ProductionShuttlePayloadStrategy
from app.canary import compute_config_fingerprint
from app.canonical import compute_semantic_hash
from app.models import AppConfig, ProductionSourceArtifact, compute_sha256
from app.production_shadow import (ProductionShadow, observe_source, observe_pinned_source_artifact, public_report, repository_snapshot, require_shadow, take_checkpoint_snapshot, check_active_production_sessions, classify_checkpoint_delta, audit_handoff_thread_side_effects, build_handoff_instruction_prompt)

THREAD = "11111111-1111-4111-8111-111111111111"
CONVERSATION = "22222222-2222-4222-8222-222222222222"
OUTPUT = "  Exact production output\n中文\n" + "f" * 32 + "\n\n"
PROMPT = "## Mission\n" + "Implement the planner's specified task without changing its scope.\n" * 5


def page(turns):
    content = ""
    for role, turn, message, text, code in turns:
        body = '<pre><code>' + html.escape(text) + '</code></pre>' if code else html.escape(text)
        content += (f'<article data-testid="conversation-turn-{turn}" data-turn-key="{turn}">'
                    f'<div data-message-author-role="{role}" data-message-id="{message}">'
                    f'<div class="markdown">{body}</div></div>'
                    '<div class="turn-action-controls"><button data-testid="copy-turn-action-button" aria-label="Copy">Copy</button></div></article>')
    return ('<html><body>' + content + '<form data-chatgpt-composer="">'
            '<textarea id="prompt-textarea"></textarea><button data-testid="send-button" type="submit">Send</button>'
            '</form></body></html>')


@pytest.fixture
def env(tmp_path):
    workspace = tmp_path / "production"
    workspace.mkdir()
    def git(*args):
        return subprocess.run(["git", "-C", str(workspace), *args], check=True, capture_output=True)
    git("init", "-b", "main")
    (workspace / "tracked.txt").write_text("baseline", encoding="utf-8")
    git("add", "tracked.txt")
    git("-c", "user.name=Synthetic", "-c", "user.email=synthetic@example.invalid", "commit", "-m", "fixture")
    sessions = tmp_path / "sessions"
    sessions.mkdir()
    rollout = sessions / f"rollout-{THREAD}.jsonl"
    records = [
        {"type": "session_meta", "payload": {"id": THREAD, "cwd": str(workspace)}},
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "source-turn"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "User task"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "source-assistant", "content": [{"type": "output_text", "text": OUTPUT}]}},
        {"type": "event_msg", "timestamp": "2026-01-01T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "source-turn"}},
    ]
    rollout.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    index = tmp_path / "index.jsonl"
    index.write_text(json.dumps({"id": THREAD, "cwd": str(workspace)}), encoding="utf-8")
    cfg = AppConfig(production_shadow_mode=True, allow_real_c2g_send=True, production_rollout_path=str(rollout),
        codex={"repo_path": str(workspace), "expected_branch": "main", "cli_path": "unused",
               "thread_id": THREAD, "sessions_dir": str(sessions), "session_index_path": str(index)},
        chatgpt={"profile_dir": str(tmp_path / "profile"), "conversation_url": f"https://chatgpt.com/c/{CONVERSATION}",
                 "expected_conversation_id": CONVERSATION},
        protected_targets={"thread_id": THREAD, "conversation_id": CONVERSATION, "workspace_path": str(workspace)})
    browser = MagicMock(target_url=cfg.chatgpt.conversation_url)
    browser.get_browser_status = AsyncMock(return_value={"connected": True, "ready": True, "current_url": cfg.chatgpt.conversation_url})
    baseline_turns = [("user", "old-user", "old-user-message", "old task", False),
                      ("assistant", "old-assistant", "old-assistant-message", "old response", False)]
    browser.read_live_page = AsyncMock(return_value={"url": cfg.chatgpt.conversation_url, "html": page(baseline_turns)})
    evidence = {"user_turn_id": "new-user", "user_message_id": "new-user-message", "matching_turn_count": 1,
                "semantic_delivery_status": "SEMANTIC_DELIVERY_CONFIRMED", "raw_content_hash": compute_sha256(OUTPUT),
                "semantic_content_hash": compute_semantic_hash(OUTPUT)}
    sender = MagicMock(target_url=cfg.chatgpt.conversation_url)
    sender.send_message = AsyncMock(return_value={"status": "delivered", "raw_content_hash": compute_sha256(OUTPUT),
                                                  "semantic_content_hash": compute_semantic_hash(OUTPUT), "delivery_evidence": evidence})
    observer = ReadOnlyCodexSessionObserver(sessions, index)
    service = ProductionShadow(tmp_path / "private" / "shadow.db", AsyncMock(return_value=cfg), observer, browser, sender)
    return {"cfg": cfg, "service": service, "browser": browser, "sender": sender, "workspace": workspace,
            "rollout": rollout, "records": records, "turns": baseline_turns, "observer": observer}


async def prepared(env):
    s, cfg = env["service"], env["cfg"]
    await s.baseline(compute_config_fingerprint(cfg), True)
    return await s.prepare()


def binding(r):
    return {"transfer_id": r["c2g_transfer_id"], "destination": r["targets"]["conversation_url"],
            "payload_hash": r["payload_hash"], "source_message_id": r["source"]["assistant_message_id"],
            "source_turn_id": r["source"]["turn_id"], "config_fingerprint": r["config_fingerprint"], "production_shadow_mode": True}


async def delivered(env):
    r = await prepared(env)
    a = await env["service"].authorize(binding(r), True)
    return await env["service"].send(a["token"]), a


def planner_page(env, text=PROMPT, code=True):
    env["browser"].read_live_page.return_value["html"] = page(env["turns"] + [
        ("user", "new-user", "new-user-message", OUTPUT, False),
        ("assistant", "new-assistant", "new-assistant-message", text, code)])


def test_shadow_config_recognized(env):
    require_shadow(env["cfg"])
    assert env["cfg"].production_shadow_mode and not env["cfg"].canary_mode


@pytest.mark.parametrize("field", ["canary_mode", "allow_real_g2c_queue", "allow_auto_run", "allow_thread_resume_side_effect"])
def test_contradictory_modes_fail_closed(env, field):
    data = env["cfg"].model_dump()
    data[field] = True
    with pytest.raises(ValueError, match="CONTRADICTORY"):
        AppConfig(**data)


@pytest.mark.parametrize("field", ["thread_id", "conversation_id", "workspace_path"])
def test_registry_exact_match(env, field):
    setattr(env["cfg"].protected_targets, field, "wrong-target")
    with pytest.raises(ValueError, match="REGISTRY_MISMATCH"):
        require_shadow(env["cfg"])


def test_source_is_exact_and_readonly(env):
    before = env["rollout"].read_bytes()
    source = observe_source(env["cfg"], env["observer"])
    assert source["text"] == OUTPUT and source["hash"] == compute_sha256(OUTPUT)
    assert source["assistant_message_id"] == "source-assistant" and source["turn_id"] == "source-turn"
    assert env["rollout"].read_bytes() == before
    assert ProductionShuttlePayloadStrategy().build_c2g_payload("unused", 1, "unused", 1, OUTPUT) == OUTPUT


def test_running_codex_rejected(env):
    with env["rollout"].open("a", encoding="utf-8") as f:
        f.write(json.dumps({"type": "event_msg", "payload": {"type": "task_started", "turn_id": "next-turn"}}) + "\n")
    with pytest.raises(ValueError, match="NOT_IDLE"):
        observe_source(env["cfg"], env["observer"])


def test_rollout_header_mismatch(env):
    records = env["records"]
    records[0]["payload"]["id"] = CONVERSATION
    env["rollout"].write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
        observe_source(env["cfg"], env["observer"])


@pytest.mark.asyncio
async def test_prepare_does_not_send_and_requires_identity_confirmation(env):
    with pytest.raises(ValueError, match="CONFIRMATION_REQUIRED"):
        await env["service"].baseline(compute_config_fingerprint(env["cfg"]), False)
    r = await prepared(env)
    assert r["payload"] == OUTPUT and r["char_count"] == len(OUTPUT)
    assert r["status"] == "PRODUCTION C2G PREPARED — NOT SENT"
    env["sender"].send_message.assert_not_awaited()
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await env["service"].send("unauthorized")


@pytest.mark.asyncio
async def test_authorization_requires_human(env):
    r = await prepared(env)
    with pytest.raises(ValueError, match="HUMAN_AUTHORIZATION"):
        await env["service"].authorize(binding(r), False)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", list(binding({"c2g_transfer_id":"", "targets":{"conversation_url":""}, "payload_hash":"", "source":{"assistant_message_id":"", "turn_id":""}, "config_fingerprint":""})))
async def test_authorization_bound_to_every_identity(env, field):
    r = await prepared(env)
    b = binding(r)
    b[field] = "mutated"
    with pytest.raises(ValueError, match="BINDING_MISMATCH"):
        await env["service"].authorize(b, True)


@pytest.mark.asyncio
async def test_mutated_payload_cannot_authorize(env):
    r = await prepared(env)
    r["payload"] += "rewritten"
    env["service"].save(r)
    with pytest.raises(ValueError, match="MUTATION_DETECTED"):
        await env["service"].authorize(binding(r), True)


@pytest.mark.asyncio
async def test_send_once_semantic_confirmation_and_reuse_rejected(env):
    r, a = await delivered(env)
    assert r["status"] == "PRODUCTION_C2G_DELIVERED"
    assert r["delivery"]["user_message_id"] == "new-user-message"
    assert env["sender"].send_message.await_args.kwargs["message"] == OUTPUT
    with pytest.raises(ValueError, match="AUTHORIZATION_REJECTED"):
        await env["service"].send(a["token"])
    assert env["sender"].send_message.await_count == 1


@pytest.mark.asyncio
async def test_ambiguous_send_never_retried_even_after_restart(env):
    env["sender"].send_message.side_effect = ConnectionError("disconnect after click")
    r, a = await delivered(env)
    assert r["status"] == "PRODUCTION_C2G_DELIVERY_UNKNOWN"
    old = env["service"]
    restart = ProductionShadow(old.path, old.config_fn, old.observer, old.browser, old.sender)
    with pytest.raises(ValueError):
        await restart.send(a["token"])
    with pytest.raises(ValueError, match="ALREADY_EXISTS"):
        await restart.baseline(compute_config_fingerprint(env["cfg"]), True)
    assert env["sender"].send_message.await_count == 1


@pytest.mark.asyncio
async def test_duplicate_semantic_delivery_stops(env):
    env["sender"].send_message.return_value["delivery_evidence"]["matching_turn_count"] = 2
    r, _ = await delivered(env)
    assert r["status"] == "PRODUCTION_C2G_DELIVERY_UNKNOWN"


@pytest.mark.asyncio
async def test_new_planner_correlated_and_g2c_survives_restart_without_authorization(env):
    await delivered(env)
    planner_page(env)
    r = await env["service"].observe_planner()
    assert r["status"] == "PRODUCTION_G2C_PREPARED_FOR_REVIEW"
    assert r["g2c"]["prompt"] == r["g2c"]["message"] == PROMPT.strip()
    assert r["g2c"]["g2c_rewrite_count"] == 0
    assert r["g2c"]["status"] == "PRODUCTION_G2C_PREPARED_NOT_AUTHORIZED"
    assert "authorization" not in r["g2c"] and "token" not in r["g2c"]
    old = env["service"]
    restart = ProductionShadow(old.path, old.config_fn, old.observer, old.browser, old.sender)
    assert restart.load()["g2c"] == r["g2c"]
    assert env["sender"].send_message.await_count == 1


@pytest.mark.asyncio
async def test_stale_planner_rejected(env):
    await delivered(env)
    with pytest.raises(ValueError, match="STALE_OR_INCOMPLETE"):
        await env["service"].observe_planner()


@pytest.mark.asyncio
async def test_unrelated_later_user_rejected(env):
    await delivered(env)
    planner_page(env)
    env["browser"].read_live_page.return_value["html"] = page(env["turns"] + [
        ("user", "unrelated", "unrelated-msg", "different", False),
        ("assistant", "reply", "reply-msg", PROMPT, True)])
    with pytest.raises(ValueError, match="STALE_OR_INCOMPLETE"):
        await env["service"].observe_planner()


@pytest.mark.asyncio
async def test_ambiguous_planner_stops(env):
    await delivered(env)
    planner_page(env)
    env["browser"].read_live_page.return_value["html"] = env["browser"].read_live_page.return_value["html"].replace(
        '</code></pre>', '</code></pre><pre><code>' + html.escape(PROMPT + " competing") + '</code></pre>')
    r = await env["service"].observe_planner()
    assert r["status"] == "PLANNER_RESPONSE_AMBIGUOUS" and "g2c" not in r


@pytest.mark.asyncio
async def test_planner_stop_is_valid(env):
    await delivered(env)
    planner_page(env, "SHUTTLE_STOP", False)
    r = await env["service"].observe_planner()
    assert r["status"] == "PLANNER_STOP_REQUESTED" and "g2c" not in r


def test_repository_snapshot_does_not_mutate_preexisting_dirty_state(env, monkeypatch):
    (env["workspace"] / "tracked.txt").write_text("preexisting dirty", encoding="utf-8")
    (env["workspace"] / "untracked.txt").write_text("private dirty", encoding="utf-8")
    original = subprocess.run
    commands = []
    def spy(args, *a, **kw):
        commands.append(args[3])
        return original(args, *a, **kw)
    monkeypatch.setattr(subprocess, "run", spy)
    a = repository_snapshot(env["workspace"])
    b = repository_snapshot(env["workspace"])
    assert a == b and a["production_repo_preexisting_dirty_state"]
    assert set(commands) <= {"status", "diff", "ls-files", "branch", "rev-parse"}


@pytest.mark.asyncio
async def test_repository_change_after_send_blocks_review(env):
    await delivered(env)
    (env["workspace"] / "new.txt").write_text("external change", encoding="utf-8")
    planner_page(env)
    r = await env["service"].observe_planner()
    assert r["status"] == "PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW"


@pytest.mark.asyncio
async def test_public_report_contains_no_private_payload_or_ids(env):
    r = await prepared(env)
    report = json.dumps(public_report(r))
    for private in [THREAD, CONVERSATION, str(env["workspace"]), OUTPUT, "source-assistant"]:
        assert private not in report


def test_private_files_are_ignored_and_untracked():
    root = Path(__file__).resolve().parents[1]
    paths = ["config.production.shadow.local.json", "config.protected-targets.local.json", "runtime/private-reports/phase2_7/evidence.json"]
    for path in paths:
        assert subprocess.run(["git", "check-ignore", "--quiet", path], cwd=root).returncode == 0
        assert not subprocess.run(["git", "ls-files", "--", path], cwd=root, capture_output=True).stdout


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["/api/auto/arm", "/api/auto/authorize", "/api/auto/start", "/api/auto/resume_human",
    "/api/codex/thread/resume", "/api/transfer/codex_queue/submit", "/api/transfer/chatgpt/send",
    "/api/transfer/send_to_chatgpt", "/api/config", "/api/canary/rounds/init"])
async def test_server_rejects_all_legacy_execution_routes(env, monkeypatch, endpoint, tmp_path):
    import app.main as main
    from app.database import Database
    db = Database(tmp_path / "api.db")
    await db.init()
    await db.save_config(env["cfg"])
    monkeypatch.setattr(main, "db", db)
    async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://127.0.0.1:8765") as client:
        response = await client.post(endpoint, json={})
    assert response.status_code == 409
    env["sender"].send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_internal_auto_and_child_authorization_forbidden(env):
    coordinator = AutoModeCoordinator(MagicMock(), MagicMock(), get_config_fn=AsyncMock(return_value=env["cfg"]))
    with pytest.raises(ValueError, match="PRODUCTION_AUTO_MODE_NOT_AUTHORIZED"):
        await coordinator.arm_run(operator_confirmed=True)
    with pytest.raises(ValueError, match="PRODUCTION_AUTO_MODE_NOT_AUTHORIZED"):
        await coordinator.start_run("stale-run")
    with pytest.raises(ValueError, match="PRODUCTION_AUTO_MODE_NOT_AUTHORIZED"):
        await coordinator.derive_child_authorization({}, 1, "id", "chatgpt_to_codex", THREAD, "hash", "fingerprint")


@pytest.mark.asyncio
async def test_low_level_queue_and_resume_are_blocked(env):
    from app.adapters import RealCodexQueueAdapter
    from app.codex_app_server import CodexAppServerManager
    database = MagicMock()
    database.get_config = AsyncMock(return_value=env["cfg"])
    queue = RealCodexQueueAdapter(db=database)
    with pytest.raises(ValueError, match="PRODUCTION_G2C_QUEUE_FORBIDDEN"):
        await queue.queue_message(THREAD, "never execute", repo_path=str(env["workspace"]))
    manager = CodexAppServerManager("unused")
    manager.production_shadow_mode = True
    manager.ensure_started = AsyncMock()
    with pytest.raises(ValueError, match="PRODUCTION_AUTO_MODE_NOT_AUTHORIZED"):
        await manager.resume_thread_safely(THREAD)
    manager.ensure_started.assert_not_awaited()


def test_shared_private_registry_protects_other_modes(tmp_path, monkeypatch):
    import app.canary as canary
    monkeypatch.setattr(canary, "BASE_DIR", tmp_path)
    registry = tmp_path / "config.protected-targets.local.json"
    registry.write_text(json.dumps({"thread_id": THREAD, "conversation_id": CONVERSATION, "workspace_path": str(tmp_path / "protected")}))
    assert canary.is_production_thread(THREAD)
    assert canary.is_production_conversation(CONVERSATION)
    assert canary.is_production_workspace(tmp_path / "protected")
    registry.write_text("{}")
    with pytest.raises(ValueError, match="PRIVATE_PROTECTED_TARGET_REGISTRY_INVALID"):
        canary.is_production_thread(THREAD)


@pytest.mark.asyncio
async def test_configuration_change_invalidates_preparation(env):
    await prepared(env)
    env["cfg"].codex.expected_branch = "different-branch"
    with pytest.raises(ValueError, match="CONFIG_CHANGED"):
        await env["service"].prepare()


@pytest.mark.asyncio
async def test_wrong_browser_target_rejected(env):
    env["browser"].get_browser_status.return_value["current_url"] += "/wrong"
    with pytest.raises(ValueError, match="TARGET_NOT_READY"):
        await prepared(env)
    env["sender"].send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_service_instances_cannot_duplicate_send(env):
    r = await prepared(env)
    s = env["service"]
    a = await s.authorize(binding(r), True)
    other = ProductionShadow(s.path, s.config_fn, s.observer, s.browser, s.sender)
    outcomes = await asyncio.gather(s.send(a["token"]), other.send(a["token"]), return_exceptions=True)
    assert sum(isinstance(o, ValueError) for o in outcomes) == 1
    assert env["sender"].send_message.await_count == 1


@pytest.mark.asyncio
async def test_stale_authorization_writer_cannot_reopen_consumed_budget(env):
    r = await prepared(env)
    s = env["service"]
    a = await s.authorize(binding(r), True)
    stale_authorized_record = s.load()
    await s.send(a["token"])
    # Another process can have read this authorization before the send boundary.
    # Its delayed write must never reset the immutable send-attempt column.
    with pytest.raises(ValueError, match="PRODUCTION_C2G_ALREADY_ATTEMPTED"):
        s.save(stale_authorized_record)
    assert s.load()["status"] == "PRODUCTION_C2G_DELIVERED"
    with pytest.raises(ValueError):
        await s.send(a["token"])
    assert env["sender"].send_message.await_count == 1


@pytest.mark.asyncio
async def test_error_and_no_prompt_stop_without_g2c(env):
    await delivered(env)
    planner_page(env, "No changes are required at this time.", False)
    r = await env["service"].observe_planner()
    assert r["status"] == "PLANNER_NO_ACTIONABLE_PROMPT" and "g2c" not in r


@pytest.mark.asyncio
async def test_branch_mismatch_blocks_before_transfer(env):
    env["cfg"].codex.expected_branch = "not-the-current-branch"
    with pytest.raises(ValueError, match="PRODUCTION_BRANCH_MISMATCH"):
        await prepared(env)
    assert env["service"].load() is None


def make_pinned_artifact(thread_id=THREAD, assistant_message_id="source-assistant",
                         user_turn_id="source-turn", user_message_id=None,
                         completed_at="2026-01-01T00:00:00Z", rollout_line=3,
                         text=OUTPUT, expected_marker=None):
    return ProductionSourceArtifact(
        thread_id=thread_id,
        user_turn_id=user_turn_id,
        user_message_id=user_message_id,
        assistant_message_id=assistant_message_id,
        completed_at=completed_at,
        rollout_line=rollout_line,
        sha256=compute_sha256(text),
        char_count=len(text),
        expected_marker=expected_marker
    )


def test_pinned_source_final_is_not_latest_final(env):
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "newer-turn"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Precheck task"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "newer-assistant", "content": [{"type": "output_text", "text": "Newer final answer"}]}},
        {"type": "event_msg", "timestamp": "2026-01-02T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "newer-turn"}},
    ]
    with env["rollout"].open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    source = observe_source(env["cfg"], env["observer"])
    assert source["assistant_message_id"] == "source-assistant"
    assert source["text"] == OUTPUT
    assert source["thread_state"] == "completed_newer_turns"
    assert source["newer_finals_count"] == 1


def test_newer_final_exists_in_same_thread_records_audit_event(env):
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "newer-turn"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "newer-assistant", "content": [{"type": "output_text", "text": "Newer text"}]}},
        {"type": "event_msg", "timestamp": "2026-01-02T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "newer-turn"}},
    ]
    with env["rollout"].open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    source = observe_source(env["cfg"], env["observer"])
    assert "NEWER_SOURCE_FINAL_OBSERVED_NOT_SELECTED" in source["audit_events"]
    assert source["assistant_message_id"] == "source-assistant"


def test_pinned_artifact_still_retrieved_exactly(env):
    pinned = make_pinned_artifact(text=OUTPUT)
    env["cfg"].production_source = pinned
    source = observe_pinned_source_artifact(env["cfg"], env["observer"], pinned=pinned)
    assert source["text"] == OUTPUT
    assert source["hash"] == compute_sha256(OUTPUT)
    assert source["char_count"] == len(OUTPUT)
    assert source["assistant_message_id"] == "source-assistant"
    assert source["completed_at"] == "2026-01-01T00:00:00Z"
    assert source["status"] == "PINNED_SOURCE_ARTIFACT_VERIFIED"


@pytest.mark.asyncio
async def test_preview_uses_pinned_final_not_newest_final(env):
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "newer-turn"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "newer-assistant", "content": [{"type": "output_text", "text": "Brand new output"}]}},
        {"type": "event_msg", "timestamp": "2026-01-02T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "newer-turn"}},
    ]
    with env["rollout"].open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    r = await prepared(env)
    assert r["payload"] == OUTPUT
    assert r["payload_hash"] == compute_sha256(OUTPUT)
    assert r["preview_verdict"] == "PRODUCTION_C2G_PREVIEW_VERIFIED"
    assert r["payload"] != "Brand new output"


def test_exact_assistant_message_id_mismatch_blocks(env):
    pinned = make_pinned_artifact(assistant_message_id="wrong-assistant-id", text=OUTPUT)
    env["cfg"].production_source = pinned
    with pytest.raises(ValueError, match="PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED"):
        observe_pinned_source_artifact(env["cfg"], env["observer"], pinned=pinned)


def test_pinned_hash_mismatch_blocks(env):
    pinned = make_pinned_artifact(text=OUTPUT)
    pinned.sha256 = "0" * 64
    env["cfg"].production_source = pinned
    with pytest.raises(ValueError, match="PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED"):
        observe_pinned_source_artifact(env["cfg"], env["observer"], pinned=pinned)


def test_character_count_mismatch_blocks(env):
    pinned = make_pinned_artifact(text=OUTPUT)
    pinned.char_count = len(OUTPUT) + 99
    env["cfg"].production_source = pinned
    with pytest.raises(ValueError, match="PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED"):
        observe_pinned_source_artifact(env["cfg"], env["observer"], pinned=pinned)


def test_task_complete_evidence_required(env):
    records = [r for r in env["records"] if r.get("payload", {}).get("type") != "task_complete"]
    env["rollout"].write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    pinned = make_pinned_artifact(text=OUTPUT)
    env["cfg"].production_source = pinned
    with pytest.raises(ValueError, match="PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED"):
        observe_pinned_source_artifact(env["cfg"], env["observer"], pinned=pinned)


@pytest.mark.asyncio
async def test_later_completed_administrative_turn_does_not_invalidate_pinned_artifact(env):
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "admin-turn"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "admin-assistant", "content": [{"type": "output_text", "text": "Administrative response"}]}},
        {"type": "event_msg", "timestamp": "2026-01-02T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "admin-turn"}},
    ]
    with env["rollout"].open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    r = await prepared(env)
    assert r["status"] == "PRODUCTION C2G PREPARED — NOT SENT"
    assert r["preview_verdict"] == "PRODUCTION_C2G_PREVIEW_VERIFIED"
    assert r["source"]["assistant_message_id"] == "source-assistant"


def test_later_active_thread_does_not_rewrite_source_selection(env):
    with env["rollout"].open("a", encoding="utf-8") as f:
        f.write(json.dumps({"type": "event_msg", "payload": {"type": "task_started", "turn_id": "active-task"}}) + "\n")
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    source = observe_source(env["cfg"], env["observer"])
    assert source["status"] == "PINNED_SOURCE_ARTIFACT_VERIFIED"
    assert source["thread_state"] == "active"
    assert source["assistant_message_id"] == "source-assistant"


@pytest.mark.asyncio
async def test_later_substantive_repository_mutation_invalidates_freshness(env):
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    s = env["service"]
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)
    (env["workspace"] / "tracked.txt").write_text("substantive_change", encoding="utf-8")
    with pytest.raises(ValueError, match="PINNED_SOURCE_STALE_AFTER_REPOSITORY_CHANGE"):
        await s.prepare()
    assert s.load()["status"] == "PINNED_SOURCE_STALE_AFTER_REPOSITORY_CHANGE"


@pytest.mark.asyncio
async def test_no_automatic_source_rebinding(env):
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    s = env["service"]
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "newer-turn"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "newer-assistant", "content": [{"type": "output_text", "text": "Newer unselected output"}]}},
        {"type": "event_msg", "timestamp": "2026-01-02T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "newer-turn"}},
    ]
    with env["rollout"].open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")
    source = observe_source(env["cfg"], env["observer"])
    assert source["assistant_message_id"] == "source-assistant"
    assert env["cfg"].production_source.assistant_message_id == "source-assistant"
    assert s.load()["source"]["assistant_message_id"] == "source-assistant"


@pytest.mark.asyncio
async def test_explicit_operator_rebinding_works(env):
    second_text = "Second approved handoff\n" + "e" * 32
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "second-turn"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Second task", "turn_id": "second-turn"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "second-assistant", "content": [{"type": "output_text", "text": second_text}]}},
        {"type": "event_msg", "timestamp": "2026-01-02T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "second-turn"}},
    ]
    with env["rollout"].open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    s = env["service"]
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)
    assert s.load()["source"]["assistant_message_id"] == "source-assistant"

    new_pinned = make_pinned_artifact(assistant_message_id="second-assistant", user_turn_id="second-turn",
                                      completed_at="2026-01-02T00:00:00Z", rollout_line=7, text=second_text)
    await s.rebind_source(new_pinned)
    rebound = s.load()
    assert rebound["source"]["assistant_message_id"] == "second-assistant"
    assert rebound["source"]["text"] == second_text
    assert rebound["status"] == "PRODUCTION_SHADOW_BASELINE_VERIFIED"


@pytest.mark.asyncio
async def test_source_artifact_survives_shuttle_restart(env, tmp_path):
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    r = await prepared(env)
    assert r["preview_verdict"] == "PRODUCTION_C2G_PREVIEW_VERIFIED"
    db_path = env["service"].path
    restarted = ProductionShadow(db_path, AsyncMock(return_value=env["cfg"]), env["observer"], env["browser"], env["sender"])
    loaded = restarted.load()
    assert loaded["source"]["assistant_message_id"] == "source-assistant"
    assert loaded["payload"] == OUTPUT
    assert loaded["payload_hash"] == compute_sha256(OUTPUT)
    assert loaded["preview_verdict"] == "PRODUCTION_C2G_PREVIEW_VERIFIED"


def test_real_ids_stay_out_of_public_repository():
    root = Path(__file__).resolve().parents[1]
    res = subprocess.run(["git", "-C", str(root), "ls-files"], capture_output=True, check=True)
    tracked_files = [line.strip() for line in res.stdout.decode().splitlines() if line.strip()]
    import base64
    b64_list = ['MDFhMTdmNDUtMmE5MC03MjEyLTk1YTMtY2RmMDIzMjk3MjU1', 'MDFhMTcwOGItMThkNC03YWYyLWJlMzQtZDdlM2Q4YjY4MDlk', 'NmFkY2Q1NDgtOWVjOC04M2VhLThjODUtZDkxYmQwOTZkM2M4', 'bXNnXzAyYmU4MTg1Y2I2NjBlZDQwMTZhZTExOTYxMTQ2MDgxOTc4NjIyN2JhNTZhYzQ2ZDk3', 'NDg1Mjk1ZjBiMDZkNWY0ZTYzMjllZmUzMTU2MzQ3N2QyOWM1M2ZmNjRjNmZmYTY5ZjI4NTlhNTQ1Y2E3ZmVjNQ==', 'bWFqaWFuZy1jb3BpbG90']
    forbidden = [base64.b64decode(b).decode("utf-8") for b in b64_list]
    for rel_path in tracked_files:
        full_path = root / rel_path
        if full_path.is_file():
            try:
                text = full_path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            for secret in forbidden:
                assert secret not in text, f"Private token found in tracked file {rel_path}"



# ==============================================================================
# Phase 2.7.0.2: 15 Focused Tests for Atomic Checkpoint Handoff Capture
# ==============================================================================

class MockAppServerManager:
    def __init__(self, thread_id="33333333-3333-4333-8333-333333333333"):
        self.thread_id = thread_id
        self.calls = []

    async def start_thread(self, cwd, title=None, experimental_api=False):
        self.calls.append(("start_thread", cwd, title, experimental_api))
        return {"thread": {"id": self.thread_id, "cwd": cwd}}

    async def start_turn(self, thread_id, prompt):
        self.calls.append(("start_turn", thread_id, prompt))
        return {"turn": {"id": "turn-test-1", "threadId": thread_id}}


def create_handoff_rollout(path, thread_id, cwd, final_text, assistant_id="new-assistant", user_turn="turn-test-1", completed=True):
    records = [
        {"type": "session_meta", "payload": {"id": thread_id, "cwd": str(cwd)}},
        {"type": "turn_context", "payload": {"turn_id": user_turn}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "Handoff prompt", "turn_id": user_turn, "id": "msg-u-1"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": assistant_id, "turn_id": user_turn, "content": [{"type": "output_text", "text": final_text}]}},
    ]
    if completed:
        records.append({"type": "event_msg", "timestamp": "2026-01-03T12:00:00Z", "payload": {"type": "task_complete", "turn_id": user_turn}})
    with Path(path).open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


@pytest.mark.asyncio
async def test_atomic_capture_1_active_production_task_blocks_capture(env):
    mock_mgr = MockAppServerManager()
    active_checker = lambda ws, **kw: [{"thread_id": "active-thread-1", "reason": "ACTIVE_RUNNING_TURN"}]
    res = await env["service"].capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=active_checker,
        skip_chatgpt_check=True,
    )
    assert res["status"] == "PRODUCTION_CHECKPOINT_NOT_QUIESCENT"
    assert res["send_count"] == 0
    assert len(mock_mgr.calls) == 0


@pytest.mark.asyncio
async def test_atomic_capture_2_dirty_but_stable_repo_allowed(env, tmp_path):
    ws = Path(env["cfg"].codex.repo_path)
    (ws / "dirty_tracked.txt").write_text("modified dirty content", encoding="utf-8")
    (ws / "untracked_new.txt").write_text("new untracked file", encoding="utf-8")
    
    handoff_tid = "44444444-4444-4444-8444-444444444444"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Verified clean handoff text\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, ws, final_text)

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    res = await env["service"].capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )
    assert res["checkpoint_status"] == "ATOMIC_PRODUCTION_CHECKPOINT_CAPTURED"
    assert res["send_count"] == 0


@pytest.mark.asyncio
async def test_atomic_capture_3_thread_start_followed_immediately_by_first_turn(env):
    handoff_tid = "55555555-5555-4555-8555-555555555555"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Handoff result\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, env["cfg"].codex.repo_path, final_text)

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    await env["service"].capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )
    assert len(mock_mgr.calls) == 2
    assert mock_mgr.calls[0][0] == "start_thread"
    assert mock_mgr.calls[0][3] is False  # experimentalApi is False
    assert mock_mgr.calls[1][0] == "start_turn"
    assert mock_mgr.calls[1][1] == handoff_tid


@pytest.mark.asyncio
async def test_atomic_capture_4_handoff_thread_read_only_contract(env):
    prompt = build_handoff_instruction_prompt()
    assert "STRICTLY READ-ONLY" in prompt
    assert "FRESH_PRODUCTION_HANDOFF_READY" in prompt
    assert "zero source/test/Git writes" in prompt

    snap_a = {"head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {}}
    snap_b = {"head": "h1", "branch": "b1", "dirty_file_hashes": {"src/modified.py": "hash2"}, "untracked_hashes": {}}
    with pytest.raises(ValueError, match="ATOMIC_HANDOFF_READONLY_VIOLATION"):
        audit_handoff_thread_side_effects(snap_a, snap_b)


@pytest.mark.asyncio
async def test_atomic_capture_5_snapshot_exact_match_passes():
    snap_a = {"fingerprint": "abc", "head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {}}
    snap_b = {"fingerprint": "abc", "head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {}}
    status, details = classify_checkpoint_delta(snap_a, snap_b)
    assert status == "ATOMIC_PRODUCTION_CHECKPOINT_CAPTURED"
    assert details == []


@pytest.mark.asyncio
async def test_atomic_capture_6_material_delta_blocks():
    snap_a = {"fingerprint": "a", "head": "h1", "branch": "b1", "dirty_file_hashes": {"src/server.py": "1"}, "untracked_hashes": {}}
    snap_b = {"fingerprint": "b", "head": "h1", "branch": "b1", "dirty_file_hashes": {"src/server.py": "2"}, "untracked_hashes": {}}
    status, details = classify_checkpoint_delta(snap_a, snap_b)
    assert status == "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE"
    assert "src/server.py" in details["material"]


@pytest.mark.asyncio
async def test_atomic_capture_7_unknown_delta_blocks():
    snap_a = {"fingerprint": "a", "head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {}}
    snap_b = {"fingerprint": "b", "head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {"some_unknown_asset.bin": "1"}}
    status, details = classify_checkpoint_delta(snap_a, snap_b)
    assert status == "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE"
    assert "some_unknown_asset.bin" in details["unknown"]


@pytest.mark.asyncio
async def test_atomic_capture_8_non_semantic_delta_may_pass():
    snap_a = {"fingerprint": "a", "head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {"runtime.log": "1"}}
    snap_b = {"fingerprint": "b", "head": "h1", "branch": "b1", "dirty_file_hashes": {}, "untracked_hashes": {"runtime.log": "2"}}
    status, details = classify_checkpoint_delta(snap_a, snap_b)
    assert status == "ATOMIC_PRODUCTION_CHECKPOINT_SEMANTICALLY_STABLE"
    assert "runtime.log" in details["non_semantic"]


@pytest.mark.asyncio
async def test_atomic_capture_9_failed_capture_does_not_rebind_source(env):
    ws = Path(env["cfg"].codex.repo_path)
    prior_pinned = make_pinned_artifact(text=OUTPUT)
    env["cfg"].production_source = prior_pinned
    s = env["service"]
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)
    
    mock_mgr = MockAppServerManager()
    active_checker = lambda ws, **kw: [{"thread_id": "active-1", "reason": "BUSY"}]
    res = await s.capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=active_checker,
        skip_chatgpt_check=True,
    )
    assert res["status"] == "PRODUCTION_CHECKPOINT_NOT_QUIESCENT"
    # Prior source remains intact
    assert s.load()["source"]["assistant_message_id"] == prior_pinned.assistant_message_id


@pytest.mark.asyncio
async def test_atomic_capture_10_successful_capture_pins_exact_artifact(env):
    handoff_tid = "66666666-6666-4666-8666-666666666666"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Authoritative new handoff\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, env["cfg"].codex.repo_path, final_text, assistant_id="asst-66")

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    s = env["service"]
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    res = await s.capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )
    assert res["pinned_artifact"]["assistant_message_id"] == "asst-66"
    assert res["pinned_artifact"]["sha256"] == compute_sha256(final_text)
    assert s.load()["source"]["assistant_message_id"] == "asst-66"


@pytest.mark.asyncio
async def test_atomic_capture_11_pinned_artifact_used_for_preview(env):
    handoff_tid = "77777777-7777-4777-8777-777777777777"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Pinned for preview verification\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, env["cfg"].codex.repo_path, final_text, assistant_id="asst-77")

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    s = env["service"]
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    res = await s.capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )
    loaded = s.load()
    assert loaded["preview_verdict"] == "PRODUCTION_C2G_PREVIEW_VERIFIED"
    assert loaded["payload"] == final_text
    assert loaded["payload_hash"] == compute_sha256(final_text)
    assert loaded["payload_rewrite"] == "NONE"


@pytest.mark.asyncio
async def test_atomic_capture_12_later_handoff_thread_final_cannot_replace_pin(env):
    handoff_tid = "88888888-8888-4888-8888-888888888888"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Pinned handoff 88\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, env["cfg"].codex.repo_path, final_text, assistant_id="asst-88")

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    s = env["service"]
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    await s.capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )

    # Later turn added to rollout
    newer_records = [
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-late"}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer",
         "id": "asst-late", "content": [{"type": "output_text", "text": "Intruder final\nFRESH_PRODUCTION_HANDOFF_READY"}]}},
        {"type": "event_msg", "timestamp": "2026-01-04T00:00:00Z", "payload": {"type": "task_complete", "turn_id": "turn-late"}},
    ]
    with Path(rollout_path).open("a", encoding="utf-8") as f:
        for r in newer_records:
            f.write(json.dumps(r) + "\n")

    # Observation still retrieves exact pinned artifact
    loaded = s.load()
    assert loaded["source"]["assistant_message_id"] == "asst-88"
    assert loaded["source"]["text"] == final_text


@pytest.mark.asyncio
async def test_atomic_capture_13_chatgpt_remains_unsent(env):
    handoff_tid = "99999999-9999-4999-8999-999999999999"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Handoff 99\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, env["cfg"].codex.repo_path, final_text, assistant_id="asst-99")

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    s = env["service"]
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    res = await s.capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )
    assert res["send_count"] == 0
    assert env["sender"].send_message.call_count == 0


@pytest.mark.asyncio
async def test_atomic_capture_14_restart_preserves_successful_atomic_checkpoint(env):
    handoff_tid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    rollout_path = Path(env["cfg"].codex.session_index_path).parent / f"rollout-{handoff_tid}.jsonl"
    final_text = "Surviving handoff\nFRESH_PRODUCTION_HANDOFF_READY"
    create_handoff_rollout(rollout_path, handoff_tid, env["cfg"].codex.repo_path, final_text, assistant_id="asst-aa")

    env["observer"].locate_session_file = MagicMock(side_effect=lambda tid: str(rollout_path) if str(tid) == str(handoff_tid) else str(env["rollout"]))
    env["observer"].validate_session_metadata = MagicMock(return_value={"found_in_index": True, "validation_errors": []})

    s = env["service"]
    env["cfg"].production_source = make_pinned_artifact(text=OUTPUT)
    await s.baseline(compute_config_fingerprint(env["cfg"]), True)

    mock_mgr = MockAppServerManager(thread_id=handoff_tid)
    await s.capture_atomic_checkpoint(
        app_server_manager=mock_mgr,
        active_session_checker=lambda ws, **kw: [],
        skip_chatgpt_check=True,
    )

    db_path = s.path
    restarted = ProductionShadow(db_path, AsyncMock(return_value=env["cfg"]), env["observer"], env["browser"], env["sender"])
    loaded = restarted.load()
    assert loaded["source"]["assistant_message_id"] == "asst-aa"
    assert loaded["payload"] == final_text
    assert loaded["preview_verdict"] == "PRODUCTION_C2G_PREVIEW_VERIFIED"


def test_atomic_capture_15_private_ids_remain_untracked():
    # Invoke comprehensive public sanitization check
    test_real_ids_stay_out_of_public_repository()
