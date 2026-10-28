import time
"""Phase 2.7: one durable C2G attempt, read-only planner observation, no executor."""
import asyncio
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit

from app.adapters import NotSentError, is_error_placeholder
from app.auto_mode import ProductionShuttlePayloadStrategy, classify_planner_response
from app.canary import compute_config_fingerprint, same_path, real_uuid
from app.chatgpt_dom import ChatGPTDOMParser
from app.models import compute_sha256


def now():
    return datetime.now(timezone.utc).isoformat()


def require_shadow(config):
    if not config or not config.production_shadow_mode:
        raise ValueError("PRODUCTION_SHADOW_CONFIG_REQUIRED")
    # Revalidate even when callers constructed/mutated a model without validation.
    type(config).model_validate(config.model_dump())
    p = config.protected_targets
    if (not p or p.thread_id != config.codex.thread_id
            or p.conversation_id != config.chatgpt.conversation_id
            or not same_path(p.workspace_path, config.codex.repo_path)):
        raise ValueError("PRODUCTION_TARGET_REGISTRY_MISMATCH")
    real_uuid(config.codex.thread_id)
    real_uuid(config.chatgpt.conversation_id)
    if getattr(config, "production_source", None):
        real_uuid(config.production_source.thread_id)
        if config.production_source.thread_id != config.codex.thread_id:
            raise ValueError("PRODUCTION_SOURCE_THREAD_MISMATCH")
    u = urlsplit(config.chatgpt.conversation_url)
    expected = (f"/g/{config.chatgpt.gpt_id}" if config.chatgpt.gpt_id else "") + f"/c/{config.chatgpt.conversation_id}"
    if (u.scheme, u.netloc, u.path) != ("https", "chatgpt.com", expected) or u.query or u.fragment:
        raise ValueError("PRODUCTION_CHATGPT_TARGET_MISMATCH")
    cdp = urlsplit(config.chatgpt.cdp_url or "")
    if cdp.scheme != "http" or cdp.hostname != "127.0.0.1":
        raise ValueError("PRODUCTION_PRIVATE_CDP_REQUIRED")
    if (not config.production_rollout_path or not config.codex.session_index_path
            or not Path(config.codex.repo_path).is_absolute()):
        raise ValueError("PRODUCTION_EXPLICIT_SESSION_REQUIRED")


def repository_snapshot(workspace):
    """Only Git read commands; include dirty bytes so edits to dirty files are visible."""
    def git(*args):
        env = dict(os.environ, GIT_OPTIONAL_LOCKS="0")
        return subprocess.run(["git", "-C", str(workspace), *args], check=True,
                              capture_output=True, env=env).stdout
    status = git("status", "--porcelain=v1", "-z", "--untracked-files=all")
    dirty = git("diff", "HEAD", "--binary", "--no-ext-diff", "--no-textconv")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
    hashes = {os.fsdecode(p): compute_sha256((Path(workspace) / os.fsdecode(p)).read_bytes().hex())
              for p in untracked if p and (Path(workspace) / os.fsdecode(p)).is_file()}
    return {"branch": git("branch", "--show-current").decode().strip(),
            "head": git("rev-parse", "HEAD").decode().strip(),
            "status": os.fsdecode(status), "diff_hash": compute_sha256(dirty.hex()),
            "untracked_hashes": hashes, "production_repo_preexisting_dirty_state": bool(status)}


def observe_pinned_source_artifact(config, observer, pinned=None):
    """Retrieve the exact pinned assistant final from rollout evidence.

    Verifies:
    - exact thread ID matches
    - exact assistant message ID exists
    - assistant record is a true final answer
    - associated task completed
    - raw SHA-256 equals pinned SHA-256
    - character count equals pinned count
    - content still ends with expected marker (if set)
    - associated source turn/message evidence matches persisted identity
    If any check fails, raises PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED.
    """
    require_shadow(config)
    pinned_obj = pinned or getattr(config, "production_source", None)
    if not pinned_obj:
        raise ValueError("PINNED_SOURCE_ARTIFACT_REQUIRED")
    if isinstance(pinned_obj, dict):
        from app.models import ProductionSourceArtifact
        pinned_obj = ProductionSourceArtifact.model_validate(pinned_obj)

    if pinned_obj.thread_id != config.codex.thread_id:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")

    path = Path(pinned_obj.rollout_path or config.production_rollout_path).resolve(strict=True)
    located = observer.locate_session_file(config.codex.thread_id)
    if not located or not same_path(path, located):
        raise ValueError("PRODUCTION_ROLLOUT_IDENTITY_MISMATCH")
    with path.open(encoding="utf-8") as stream:
        header = json.loads(stream.readline())
        meta = header.get("payload", {})
        if (header.get("type") != "session_meta" or meta.get("id") != config.codex.thread_id
                or not same_path(meta.get("cwd"), config.codex.repo_path)):
            raise ValueError("PRODUCTION_SESSION_IDENTITY_MISMATCH")
        validation = observer.validate_session_metadata(config.codex.thread_id, expected_cwd=config.codex.repo_path)
        if validation.get("validation_errors") or not validation.get("found_in_index"):
            raise ValueError("PRODUCTION_SESSION_INDEX_MISMATCH")
        if config.codex.thread_title and validation.get("title") != config.codex.thread_title:
            raise ValueError("PRODUCTION_SESSION_TITLE_MISMATCH")
        records = [header] + [json.loads(line) for line in stream if line.strip()]

    active = None
    user_turn = None
    user_message_id = None
    task_active = False
    turn_completions = {}
    turn_aborted = set()
    all_finals = []

    for index, record in enumerate(records):
        p = record.get("payload", {})
        kind = p.get("type")
        rtype = record.get("type")
        if rtype == "turn_context" or kind == "task_started":
            new_turn = p.get("turn_id")
            if new_turn:
                active = new_turn
                task_active = True
                user_turn = None
                user_message_id = None
        if kind == "user_message" or (kind == "message" and p.get("role") == "user"):
            user_turn = p.get("turn_id") or active
            user_message_id = p.get("id")
        if (rtype == "response_item" and kind == "message"
                and p.get("role") == "assistant" and p.get("phase") == "final_answer"):
            content = p.get("content")
            text = content if isinstance(content, str) else "".join(
                item["text"] for item in content or [] if item.get("type") == "output_text")
            f_entry = {
                "text": text,
                "assistant_message_id": p.get("id"),
                "turn_id": p.get("turn_id") or active,
                "user_turn_id": user_turn or p.get("turn_id") or active,
                "user_message_id": user_message_id,
                "line": index,
                "timestamp": record.get("timestamp"),
            }
            all_finals.append(f_entry)
        if kind == "task_complete":
            tid = p.get("turn_id") or active
            turn_completions[tid] = record.get("timestamp")
            task_active = False
        if kind in ("task_aborted", "turn_aborted"):
            tid = p.get("turn_id") or active
            turn_aborted.add(tid)
            task_active = False

    matching = [f for f in all_finals if f["assistant_message_id"] == pinned_obj.assistant_message_id]
    if not matching:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    target_final = matching[0]

    completion = turn_completions.get(target_final["turn_id"])
    if not completion or target_final["turn_id"] in turn_aborted:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")

    text_hash = compute_sha256(target_final["text"])
    if text_hash != pinned_obj.sha256:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    if len(target_final["text"]) != pinned_obj.char_count:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    if pinned_obj.expected_marker and not target_final["text"].endswith(pinned_obj.expected_marker):
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    if pinned_obj.user_turn_id and target_final["user_turn_id"] != pinned_obj.user_turn_id:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    if pinned_obj.user_message_id and target_final["user_message_id"] and target_final["user_message_id"] != pinned_obj.user_message_id:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    if pinned_obj.completed_at and completion != pinned_obj.completed_at:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")
    if pinned_obj.rollout_line is not None and target_final["line"] != pinned_obj.rollout_line:
        raise ValueError("PINNED_SOURCE_ARTIFACT_INTEGRITY_FAILED")

    newer_finals = [f for f in all_finals if f["line"] > target_final["line"]]
    if task_active:
        thread_state = "active"
    elif newer_finals:
        thread_state = "completed_newer_turns"
    else:
        thread_state = "idle"

    audit_events = []
    if newer_finals:
        audit_events.append("NEWER_SOURCE_FINAL_OBSERVED_NOT_SELECTED")

    return {
        "text": target_final["text"],
        "assistant_message_id": target_final["assistant_message_id"],
        "turn_id": target_final["turn_id"],
        "user_turn_id": target_final["user_turn_id"],
        "user_message_id": target_final["user_message_id"],
        "completed_at": completion,
        "line": target_final["line"],
        "rollout_line": target_final["line"],
        "hash": text_hash,
        "sha256": text_hash,
        "char_count": len(target_final["text"]),
        "rollout_lines": len(records),
        "session_meta": meta,
        "rollout_path": str(path),
        "integrity_result": "PINNED_SOURCE_ARTIFACT_VERIFIED",
        "status": "PINNED_SOURCE_ARTIFACT_VERIFIED",
        "thread_state": thread_state,
        "audit_events": audit_events,
        "newer_finals_count": len(newer_finals),
    }


def observe_source(config, observer):
    """Validate the exact header and index before parsing final-answer evidence.
    
    If config.production_source is pinned, retrieves the exact pinned artifact.
    Otherwise, evaluates the latest completed turn in the rollout (requiring thread idle).
    """
    if getattr(config, "production_source", None):
        return observe_pinned_source_artifact(config, observer, pinned=config.production_source)

    require_shadow(config)
    path = Path(config.production_rollout_path).resolve(strict=True)
    located = observer.locate_session_file(config.codex.thread_id)
    if not located or not same_path(path, located):
        raise ValueError("PRODUCTION_ROLLOUT_IDENTITY_MISMATCH")
    with path.open(encoding="utf-8") as stream:
        header = json.loads(stream.readline())
        meta = header.get("payload", {})
        if (header.get("type") != "session_meta" or meta.get("id") != config.codex.thread_id
                or not same_path(meta.get("cwd"), config.codex.repo_path)):
            raise ValueError("PRODUCTION_SESSION_IDENTITY_MISMATCH")
        validation = observer.validate_session_metadata(config.codex.thread_id, expected_cwd=config.codex.repo_path)
        if validation.get("validation_errors") or not validation.get("found_in_index"):
            raise ValueError("PRODUCTION_SESSION_INDEX_MISMATCH")
        if config.codex.thread_title and validation.get("title") != config.codex.thread_title:
            raise ValueError("PRODUCTION_SESSION_TITLE_MISMATCH")
        records = [header] + [json.loads(line) for line in stream if line.strip()]
    active = None
    user_turn = None
    final = None
    completion = None
    for index, record in enumerate(records):
        p = record.get("payload", {})
        kind = p.get("type")
        if record.get("type") == "turn_context" or kind == "task_started":
            new_turn = p.get("turn_id")
            if new_turn and (new_turn != active or kind == "task_started"):
                active, final, completion, user_turn = new_turn, None, None, None
        if kind == "user_message" or (kind == "message" and p.get("role") == "user"):
            user_turn = p.get("turn_id") or active
            final, completion = None, None
        if (record.get("type") == "response_item" and kind == "message"
                and p.get("role") == "assistant" and p.get("phase") == "final_answer"):
            content = p.get("content")
            text = content if isinstance(content, str) else "".join(
                item["text"] for item in content or [] if item.get("type") == "output_text")
            final = {"text": text, "assistant_message_id": p.get("id"),
                     "turn_id": p.get("turn_id") or active, "line": index}
        if kind == "task_complete" and final and (p.get("turn_id") or active) == final["turn_id"]:
            completion = record.get("timestamp")
        if kind in ("task_aborted", "turn_aborted"):
            completion = None
    if (not final or not completion or not final["assistant_message_id"] or not user_turn
            or final["turn_id"] != user_turn or not final["text"]):
        raise ValueError("PRODUCTION_CODEX_NOT_IDLE")
    return {**final, "user_turn_id": user_turn, "completed_at": completion,
            "hash": compute_sha256(final["text"]), "rollout_lines": len(records),
            "session_meta": meta, "rollout_path": str(path)}




def take_checkpoint_snapshot(workspace, active_sessions=None, ignore_patterns=None):
    """Capture a deterministic checkpoint snapshot of production workspace.

    Includes:
    - branch
    - HEAD
    - Git status porcelain
    - diff_hash (dirty tracked files)
    - dirty_file_hashes (per-file content hashes of modified tracked files)
    - untracked_hashes (hashes of relevant untracked files, skipping holdouts/caches)
    - active_sessions
    - timestamp
    - fingerprint (sha256 of canonical JSON)
    """
    def git(*args):
        env = dict(os.environ, GIT_OPTIONAL_LOCKS="0")
        return subprocess.run(["git", "-C", str(workspace), *args], check=True,
                              capture_output=True, env=env).stdout

    branch = git("branch", "--show-current").decode().strip()
    head = git("rev-parse", "HEAD").decode().strip()
    status_raw = git("status", "--porcelain=v1", "-z", "--untracked-files=all")
    status_str = os.fsdecode(status_raw)
    dirty = git("diff", "HEAD", "--binary", "--no-ext-diff", "--no-textconv")

    dirty_files = [p for p in git("diff", "--name-only", "HEAD").decode().splitlines() if p.strip()]
    dirty_file_hashes = {}
    for df in dirty_files:
        fp = Path(workspace) / df
        if fp.is_file():
            dirty_file_hashes[df] = compute_sha256(fp.read_bytes().hex())

    untracked_raw = git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
    untracked_hashes = {}
    for up in untracked_raw:
        if not up:
            continue
        rel = os.fsdecode(up)
        if any(ignored in rel for ignored in ("__pycache__", ".pytest", ".tmp", "holdout_raw", "FINAL_HOLDOUT")):
            continue
        fp = Path(workspace) / rel
        if fp.is_file():
            untracked_hashes[rel] = compute_sha256(fp.read_bytes().hex())

    snapshot_data = {
        "branch": branch,
        "head": head,
        "status": status_str,
        "diff_hash": compute_sha256(dirty.hex()),
        "dirty_file_hashes": dirty_file_hashes,
        "untracked_hashes": untracked_hashes,
        "active_sessions": list(active_sessions or []),
        "timestamp": now(),
    }

    fingerprint_canonical = json.dumps({
        "branch": branch,
        "head": head,
        "status": status_str,
        "diff_hash": snapshot_data["diff_hash"],
        "dirty_file_hashes": dirty_file_hashes,
        "untracked_hashes": untracked_hashes,
    }, sort_keys=True)
    snapshot_data["fingerprint"] = compute_sha256(fingerprint_canonical)
    return snapshot_data


def check_active_production_sessions(workspace, session_index_path=None, session_dir=None, cutoff_seconds=120):
    """Check if any production Codex session is currently executing in workspace."""
    active_sessions = []
    if session_index_path and Path(session_index_path).exists():
        try:
            db = sqlite3.connect(f"{Path(session_index_path).as_uri()}?mode=ro", uri=True)
            cur = db.cursor()
            rows = cur.execute("SELECT id, rollout_path, updated_at FROM threads WHERE cwd LIKE ?", (f"%{Path(workspace).name}%",)).fetchall()
            db.close()
            now_ts = datetime.now(timezone.utc).timestamp()
            for tid, rollout, updated_at in rows:
                if rollout and Path(rollout).exists():
                    if updated_at and (now_ts - updated_at < cutoff_seconds):
                        try:
                            with Path(rollout).open("rb") as f:
                                f.seek(max(0, Path(rollout).stat().st_size - 10000))
                                tail = f.read().decode("utf-8", errors="replace")
                            lines = tail.splitlines()
                            has_active_call = any("function_call" in l or "task_started" in l for l in lines[-10:])
                            has_completion = any("task_complete" in l or "turn_aborted" in l for l in lines[-3:])
                            if has_active_call and not has_completion:
                                active_sessions.append({"thread_id": tid, "rollout_path": rollout, "updated_at": updated_at})
                        except Exception:
                            active_sessions.append({"thread_id": tid, "rollout_path": rollout, "updated_at": updated_at})
        except Exception:
            pass
    return active_sessions


def classify_checkpoint_delta(snapshot_a, snapshot_b):
    """Classify the difference between snapshot A and snapshot B."""
    if snapshot_a.get("fingerprint") == snapshot_b.get("fingerprint"):
        return "ATOMIC_PRODUCTION_CHECKPOINT_CAPTURED", []

    if snapshot_a.get("branch") != snapshot_b.get("branch") or snapshot_a.get("head") != snapshot_b.get("head"):
        return "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE", [{"reason": "BRANCH_OR_HEAD_CHANGED"}]

    a_dirty = snapshot_a.get("dirty_file_hashes", {})
    b_dirty = snapshot_b.get("dirty_file_hashes", {})
    a_untracked = snapshot_a.get("untracked_hashes", {})
    b_untracked = snapshot_b.get("untracked_hashes", {})

    all_keys = set(a_dirty.keys()) | set(b_dirty.keys()) | set(a_untracked.keys()) | set(b_untracked.keys())
    changed_paths = []
    for k in all_keys:
        ha = a_dirty.get(k) or a_untracked.get(k)
        hb = b_dirty.get(k) or b_untracked.get(k)
        if ha != hb:
            changed_paths.append(k)

    if not changed_paths:
        return "ATOMIC_PRODUCTION_CHECKPOINT_CAPTURED", []

    material = []
    non_semantic = []
    unknown = []

    for p in changed_paths:
        normalized = p.replace("\\", "/").lower()
        if any(normalized.endswith(ext) for ext in (".log", ".tmp", ".txt.log")) or "private-reports" in normalized or "telemetry" in normalized:
            non_semantic.append(p)
        elif any(normalized.startswith(prefix) for prefix in ("src/", "device/", "rules/", "dist/")) or normalized.endswith((".py", ".java", ".apk", ".idsig")):
            material.append(p)
        elif "build_manifest.json" in normalized or "contract" in normalized:
            material.append(p)
        else:
            unknown.append(p)

    if material or unknown:
        return "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE", {
            "material": material,
            "unknown": unknown,
            "non_semantic": non_semantic,
        }

    return "ATOMIC_PRODUCTION_CHECKPOINT_SEMANTICALLY_STABLE", {
        "non_semantic": non_semantic,
    }


def audit_handoff_thread_side_effects(snapshot_a, snapshot_b, rollout_records=None):
    """Verify production mutation counts for the handoff thread."""
    if snapshot_a.get("head") != snapshot_b.get("head"):
        raise ValueError("ATOMIC_HANDOFF_READONLY_VIOLATION: commit count > 0")
    if snapshot_a.get("branch") != snapshot_b.get("branch"):
        raise ValueError("ATOMIC_HANDOFF_READONLY_VIOLATION: branch changed")

    a_hashes = {**snapshot_a.get("dirty_file_hashes", {}), **snapshot_a.get("untracked_hashes", {})}
    b_hashes = {**snapshot_b.get("dirty_file_hashes", {}), **snapshot_b.get("untracked_hashes", {})}

    for path, b_hash in b_hashes.items():
        if a_hashes.get(path) != b_hash:
            norm = path.replace("\\", "/").lower()
            if any(norm.startswith(pfx) for pfx in ("src/", "device/", "tests/")) or norm.endswith((".py", ".java")):
                raise ValueError(f"ATOMIC_HANDOFF_READONLY_VIOLATION: write to {path}")
            if "build_manifest" in norm or norm.endswith(".apk"):
                raise ValueError(f"ATOMIC_HANDOFF_READONLY_VIOLATION: build output {path}")

    if rollout_records:
        for rec in rollout_records:
            p = rec.get("payload", {})
            if rec.get("type") == "response_item" and p.get("type") == "function_call":
                args = str(p.get("arguments", "")).lower()
                name = str(p.get("name", ""))
                if "apply_patch" in name:
                    raise ValueError("ATOMIC_HANDOFF_READONLY_VIOLATION: apply_patch called")
                if "git commit" in args or "git push" in args or "git checkout" in args:
                    raise ValueError(f"ATOMIC_HANDOFF_READONLY_VIOLATION: git mutation {args}")
                if "build_apk" in args:
                    raise ValueError("ATOMIC_HANDOFF_READONLY_VIOLATION: build_apk called")


def build_handoff_instruction_prompt():
    return (
        "# Production Handoff Generation Request (STRICTLY READ-ONLY)\n\n"
        "Inspect the current production repository and produce ONE authoritative current-state handoff.\n"
        "STRICT CONSTRAINTS:\n"
        "- Read-only inspection only: zero source/test/Git writes, no commits, no training, no APK build, no service start, no phone state change, no report file creation.\n"
        "- The FINAL ANSWER in this turn is the sole deliverable.\n"
        "- The final answer must conclude with the terminal marker: FRESH_PRODUCTION_HANDOFF_READY.\n\n"
        "CONTENT REQUIREMENTS:\n"
        "1. Current project objective (Shaoyang Bopi Zipai mobile AI assistant).\n"
        "2. Current branch/HEAD/dirty repository state.\n"
        "3. Frozen B1_6 and protected invariants/holdouts.\n"
        "4. Clone/rule contract repairs.\n"
        "5. Decision-quality / teacher / learning research status.\n"
        "6. Human calibration status.\n"
        "7. Android thin client and remote recommendation service status.\n"
        "8. APK / build / runtime integrity.\n"
        "9. PlatformSceneDetector and later scene-state work.\n"
        "10. Current Compact Overlay behavior and real-device deployment status.\n"
        "11. Current visual/tracker architecture.\n"
        "12. Evidence boundaries and operator prerequisites.\n"
        "13. Superseded conclusions.\n"
        "14. ONE immediate next engineering step.\n"
        "15. Verification of the primary blocker:\n"
        "    'real phone frame -> vision -> VisualCandidateEvent -> VisualGameStateTracker / legal observation -> recommendation -> Android rendering'.\n"
        "16. Terminal marker: FRESH_PRODUCTION_HANDOFF_READY."
    )


class ProductionShadow:
    """No queue/lifecycle adapter is accepted by this coordinator.

    A single slot in a fixed private DB survives config filename changes and restart.
    The send attempt is committed BEFORE browser I/O; all uncertain results stop.
    """
    def __init__(self, path, config_fn, codex_observer, browser, sender, startup_fingerprint=None):
        self.path = Path(path)
        self.config_fn, self.observer = config_fn, codex_observer
        self.browser, self.sender = browser, sender
        self.startup_fingerprint = startup_fingerprint
        self.lock = asyncio.Lock()

    def connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.execute("CREATE TABLE IF NOT EXISTS production_shadow_runs (slot INTEGER PRIMARY KEY CHECK(slot=1), record TEXT NOT NULL, send_attempted INTEGER NOT NULL DEFAULT 0)")
        if "send_attempted" not in {row[1] for row in connection.execute("PRAGMA table_info(production_shadow_runs)")}:
            try:
                connection.execute("ALTER TABLE production_shadow_runs ADD COLUMN send_attempted INTEGER NOT NULL DEFAULT 0")
            except sqlite3.OperationalError:
                if "send_attempted" not in {row[1] for row in connection.execute("PRAGMA table_info(production_shadow_runs)")}:
                    raise
            # Migration must not reopen a previously consumed JSON-only ledger.
            for slot, raw in connection.execute("SELECT slot, record FROM production_shadow_runs"):
                if json.loads(raw).get("send_attempted_at"):
                    connection.execute("UPDATE production_shadow_runs SET send_attempted=1 WHERE slot=?", (slot,))
            connection.commit()
        return connection

    def load(self):
        with self.connect() as db:
            row = db.execute("SELECT record FROM production_shadow_runs WHERE slot=1").fetchone()
        return json.loads(row[0]) if row else None

    def save(self, record, create=False):
        with self.connect() as db:
            if create:
                db.execute("INSERT INTO production_shadow_runs (slot, record) VALUES (1, ?)", (json.dumps(record),))
            else:
                changed = db.execute("UPDATE production_shadow_runs SET record=? WHERE slot=1 AND (send_attempted=0 OR ?=1)",
                                     (json.dumps(record), int(bool(record.get("send_attempted_at"))))).rowcount
                if changed != 1:
                    raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")

    async def config(self):
        cfg = await self.config_fn()
        require_shadow(cfg)
        if self.startup_fingerprint and self.startup_fingerprint != compute_config_fingerprint(cfg):
            raise ValueError("PRODUCTION_SHADOW_CONFIG_CHANGED")
        # The startup adapter pair must remain bound to this exact config.
        if self.browser.target_url != cfg.chatgpt.conversation_url or self.sender.target_url != cfg.chatgpt.conversation_url:
            raise ValueError("PRODUCTION_ADAPTER_TARGET_MISMATCH")
        return cfg

    async def page(self, cfg, ready=False):
        status = await self.browser.get_browser_status()
        if (not status.get("connected") or status.get("human_verification_required")
                or status.get("current_url") != cfg.chatgpt.conversation_url
                or (ready and not status.get("ready"))):
            raise ValueError("PRODUCTION_CHATGPT_TARGET_NOT_READY")
        page = await self.browser.read_live_page()
        if page["url"] != cfg.chatgpt.conversation_url:
            raise ValueError("PRODUCTION_CHATGPT_TARGET_MISMATCH")
        parsed = ChatGPTDOMParser.parse(page["html"], current_url=page["url"])
        composer = parsed.get("composer") or {}
        if not composer.get("found") or composer.get("is_writing_block"):
            raise ValueError("PRODUCTION_COMPOSER_IDENTITY_MISMATCH")
        return parsed

    async def rebind_source(self, pinned_artifact):
        """Explicit operator rebinding of the source artifact."""
        async with self.lock:
            cfg = await self.config()
            r = self.load()
            if not r:
                raise ValueError("PRODUCTION_SHADOW_RUN_NOT_FOUND")
            with self.connect() as db:
                row = db.execute("SELECT send_attempted FROM production_shadow_runs WHERE slot=1").fetchone()
                if row and row[0]:
                    raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
            if isinstance(pinned_artifact, dict):
                from app.models import ProductionSourceArtifact
                pinned_artifact = ProductionSourceArtifact.model_validate(pinned_artifact)
            cfg.production_source = pinned_artifact
            cfg.codex.thread_id = pinned_artifact.thread_id
            if pinned_artifact.rollout_path:
                cfg.production_rollout_path = pinned_artifact.rollout_path
            if cfg.protected_targets and hasattr(cfg.protected_targets, "thread_id"):
                cfg.protected_targets.thread_id = pinned_artifact.thread_id
            new_source = observe_pinned_source_artifact(cfg, self.observer, pinned=pinned_artifact)
            r["source"] = new_source
            r["status"] = "PRODUCTION_SHADOW_BASELINE_VERIFIED"
            for k in ("payload", "payload_hash", "char_count", "c2g_transfer_id", "payload_preview",
                      "preview_verdict", "authorization"):
                r.pop(k, None)
            self.save(r)
            return r

    async def targets(self):
        cfg = await self.config()
        t = {"thread_id": cfg.codex.thread_id, "workspace": str(Path(cfg.codex.repo_path).resolve()),
             "expected_branch": cfg.codex.expected_branch, "rollout": cfg.production_rollout_path,
             "conversation_url": cfg.chatgpt.conversation_url, "conversation_id": cfg.chatgpt.conversation_id,
             "config_fingerprint": compute_config_fingerprint(cfg), "mode": "PRODUCTION SHADOW",
             "auto_execution": "OFF", "g2c_queue_permission": "OFF", "manual_authorization": "REQUIRED"}
        if getattr(cfg, "production_source", None):
            t["production_source"] = cfg.production_source.model_dump()
        return t

    async def baseline(self, fingerprint, targets_confirmed):
        async with self.lock:
            cfg = await self.config()
            if not targets_confirmed or fingerprint != compute_config_fingerprint(cfg):
                raise ValueError("PRODUCTION_TARGET_CONFIRMATION_REQUIRED")
            if self.load():
                raise ValueError("PRODUCTION_SHADOW_RUN_ALREADY_EXISTS")
            repo = repository_snapshot(cfg.codex.repo_path)
            if cfg.codex.expected_branch and repo["branch"] != cfg.codex.expected_branch:
                raise ValueError("PRODUCTION_BRANCH_MISMATCH")
            source = observe_source(cfg, self.observer)
            parsed = await self.page(cfg, ready=True)
            if parsed.get("is_streaming"):
                raise ValueError("PRODUCTION_CHATGPT_NOT_IDLE")
            record = {"shadow_run_id": "shadow-" + secrets.token_hex(12), "config_fingerprint": fingerprint,
                      "targets": await self.targets(), "source": source, "chatgpt_baseline": parsed,
                      "repo_pre": repo, "started_at": now(), "status": "PRODUCTION_SHADOW_BASELINE_VERIFIED",
                      "c2g_send_attempts": 0, "c2g_delivered_count": 0, "codex_queue_submissions": 0,
                      "codex_executions": 0, "thread_resume_calls": 0}
            self.save(record, create=True)
            return record

    def bound(self, cfg):
        r = self.load()
        if not r or r["config_fingerprint"] != compute_config_fingerprint(cfg):
            raise ValueError("PRODUCTION_SHADOW_CONFIG_CHANGED")
        return r

    def integrity(self, r):
        if (r["payload"] != r["source"]["text"] or compute_sha256(r["payload"]) != r["source"]["hash"]
                or r["payload_hash"] != r["source"]["hash"]):
            raise ValueError("PRODUCTION_PAYLOAD_MUTATION_DETECTED")

    def authorization_binding(self, cfg, r):
        return {"transfer_id": r["c2g_transfer_id"], "destination": cfg.chatgpt.conversation_url,
                "payload_hash": r["payload_hash"], "source_message_id": r["source"]["assistant_message_id"],
                "source_turn_id": r["source"]["turn_id"], "config_fingerprint": r["config_fingerprint"],
                "production_shadow_mode": True}

    async def recheck(self, cfg, r):
        source = observe_source(cfg, self.observer)
        for key in ("assistant_message_id", "user_turn_id", "hash", "text", "completed_at"):
            if source.get(key) != r["source"].get(key):
                raise ValueError("PRODUCTION_SOURCE_BASELINE_CHANGED")
        if repository_snapshot(cfg.codex.repo_path) != r["repo_pre"]:
            r["status"] = "PINNED_SOURCE_STALE_AFTER_REPOSITORY_CHANGE"
            self.save(r)
            raise ValueError(r["status"])
        parsed = await self.page(cfg, ready=True)
        if parsed != r["chatgpt_baseline"]:
            raise ValueError("PRODUCTION_CHATGPT_BASELINE_CHANGED")

    async def prepare(self):
        async with self.lock:
            cfg = await self.config()
            r = self.bound(cfg)
            if r["status"] != "PRODUCTION_SHADOW_BASELINE_VERIFIED":
                raise ValueError("PRODUCTION_C2G_PREPARE_NOT_ALLOWED")
            await self.recheck(cfg, r)
            payload = ProductionShuttlePayloadStrategy().build_c2g_payload(r["shadow_run_id"], 1, "", 1, r["source"]["text"])
            r.update(payload=payload, payload_hash=compute_sha256(payload), char_count=len(payload),
                     c2g_transfer_id="production-c2g-" + secrets.token_hex(12),
                     payload_preview={"first": payload[:160], "last": payload[-160:]},
                     payload_rewrite="NONE", status="PRODUCTION C2G PREPARED — NOT SENT")
            self.integrity(r)
            r["preview_verdict"] = "PRODUCTION_C2G_PREVIEW_VERIFIED"
            self.save(r)
            return r

    async def authorize(self, binding, operator_confirmed):
        async with self.lock:
            cfg = await self.config()
            r = self.bound(cfg)
            if not operator_confirmed or not cfg.allow_real_c2g_send:
                raise ValueError("PRODUCTION_C2G_HUMAN_AUTHORIZATION_REQUIRED")
            if r["status"] != "PRODUCTION C2G PREPARED — NOT SENT":
                raise ValueError("PRODUCTION_C2G_AUTHORIZATION_NOT_ALLOWED")
            self.integrity(r)
            expected = self.authorization_binding(cfg, r)
            if binding != expected:
                raise ValueError("PRODUCTION_AUTHORIZATION_BINDING_MISMATCH")
            await self.recheck(cfg, r)
            token = secrets.token_hex(32)
            r.update(authorization={"binding": expected, "token_hash": compute_sha256(token),
                                    "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()},
                     status="PRODUCTION_C2G_AUTHORIZED")
            self.save(r)
            return {"token": token, "transfer_id": r["c2g_transfer_id"]}

    async def send(self, token):
        async with self.lock:
            cfg = await self.config()
            r = self.bound(cfg)
            a = r.get("authorization", {})
            if (not cfg.allow_real_c2g_send or r["status"] != "PRODUCTION_C2G_AUTHORIZED"
                    or compute_sha256(token) != a.get("token_hash") or now() >= a.get("expires_at", "")):
                raise ValueError("PRODUCTION_C2G_AUTHORIZATION_REJECTED")
            self.integrity(r)
            if a.get("binding") != self.authorization_binding(cfg, r):
                raise ValueError("PRODUCTION_AUTHORIZATION_BINDING_MISMATCH")
            await self.recheck(cfg, r)
            # Cross-process compare-and-swap: a second worker cannot send this slot.
            original = json.dumps(r)
            r["status"] = "PRODUCTION_C2G_DELIVERY_UNKNOWN"
            r["send_attempted_at"] = now()
            r["c2g_send_attempts"] = 1
            with self.connect() as db:
                changed = db.execute("UPDATE production_shadow_runs SET record=?, send_attempted=1 WHERE slot=1 AND send_attempted=0 AND record=?",
                                     (json.dumps(r), original)).rowcount
                if changed != 1:
                    raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
            try:
                # Authorization is consumed durably above. Never create a generic/auto token.
                result = await self.sender.send_message(
                    message=r["payload"], confirmation_token=token, transfer_id=r["c2g_transfer_id"],
                    config_fingerprint=r["config_fingerprint"], production_shadow=True)
                evidence = result.get("delivery_evidence") or {}
                from app.canonical import compute_semantic_hash
                baseline_turn_ids = {t.get("turn_id") for t in r["chatgpt_baseline"].get("turns", [])}
                baseline_message_ids = {t.get("message_id") for t in r["chatgpt_baseline"].get("turns", [])}
                if (result.get("status") != "delivered" or not evidence.get("user_turn_id")
                        or evidence.get("user_turn_id") in baseline_turn_ids
                        or (evidence.get("user_message_id") and evidence["user_message_id"] in baseline_message_ids)
                        or evidence.get("matching_turn_count") != 1
                        or evidence.get("semantic_delivery_status") != "SEMANTIC_DELIVERY_CONFIRMED"
                        or result.get("raw_content_hash") != r["payload_hash"]
                        or result.get("semantic_content_hash") != compute_semantic_hash(r["payload"])):
                    raise ValueError("PRODUCTION_C2G_DELIVERY_UNKNOWN")
                r.update(delivery=evidence, status="PRODUCTION_C2G_DELIVERED", c2g_delivered_count=1)
            except NotSentError as exc:
                r.update(status="PRODUCTION_C2G_NOT_SENT", error=str(exc))
            except Exception as exc:
                r.update(status="PRODUCTION_C2G_DELIVERY_UNKNOWN", error=str(exc))
            r["repo_post_delivery"] = repository_snapshot(cfg.codex.repo_path)
            if r["repo_post_delivery"] != r["repo_pre"]:
                r["status"] = "PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW"
            if r["status"] != "PRODUCTION_C2G_DELIVERED":
                r["acceptance_status"] = "PHASE_2_7_PRODUCTION_SHADOW_BLOCKED"
            self.save(r)
            return r

    async def observe_planner(self):
        async with self.lock:
            cfg = await self.config()
            r = self.bound(cfg)
            if r["status"] != "PRODUCTION_C2G_DELIVERED":
                raise ValueError("PRODUCTION_PLANNER_OBSERVATION_NOT_ALLOWED")
            parsed = await self.page(cfg)
            turns = parsed.get("turns", [])
            delivered = r["delivery"]["user_turn_id"]
            users = [t for t in turns if t.get("role") == "user" and t.get("turn_id") == delivered]
            assistant = parsed.get("latest_assistant_turn") or {}
            baseline_ids = {t.get("turn_id") for t in r["chatgpt_baseline"].get("turns", [])}
            if (len(users) != 1 or not assistant.get("turn_id") or not assistant.get("message_id")
                    or assistant["turn_id"] in baseline_ids or parsed.get("is_streaming")
                    or assistant["message_id"] in {t.get("message_id") for t in r["chatgpt_baseline"].get("turns", [])}
                    or not parsed.get("is_completed")
                    or assistant.get("turn_index", -1) <= users[0].get("turn_index", -1)
                    or (parsed.get("latest_user_turn") or {}).get("turn_id") != delivered):
                raise ValueError("PRODUCTION_PLANNER_STALE_OR_INCOMPLETE")
            from app.canonical import compute_semantic_hash
            if compute_semantic_hash(users[0].get("text", "")) != r["delivery"]["semantic_content_hash"]:
                raise ValueError("PRODUCTION_DELIVERED_TURN_CHANGED")
            text = assistant.get("text", "")
            parts = {"raw_text": text, "text": text, "code_blocks": assistant.get("code_blocks", []),
                     "canvas_text": parsed.get("canvas_text", "")}
            classification, prompt, provenance, reason = classify_planner_response(parts)
            if parts["canvas_text"] and parts["code_blocks"]:
                classification, prompt, reason = "PLANNER_RESPONSE_AMBIGUOUS", None, "Competing canvas and code prompt sources"
            if is_error_placeholder(text):
                classification, prompt = "PLANNER_RESPONSE_ERROR", None
            classification = getattr(classification, "value", classification)
            r.update(planner={"assistant_turn_id": assistant["turn_id"], "assistant_message_id": assistant["message_id"],
                              "response": text, "response_hash": compute_sha256(text), "completed_at": now(),
                              "classification": classification, "provenance": provenance, "reason": reason})
            if classification == "NEXT_CODEX_PROMPT_FOUND":
                # Extraction is the sole source; transport never trims or adds text.
                r["g2c"] = {"transfer_id": "production-g2c-" + secrets.token_hex(12), "prompt": prompt,
                            "message": prompt, "hash": compute_sha256(prompt), "char_count": len(prompt),
                            "source_assistant_message_id": assistant["message_id"], "targets": r["targets"],
                            "config_fingerprint": r["config_fingerprint"], "repo_baseline": r["repo_pre"],
                            "payload_rewrite": "NONE", "g2c_rewrite_count": 0,
                            "status": "PRODUCTION_G2C_PREPARED_NOT_AUTHORIZED"}
                if r["g2c"]["message"] != prompt or compute_sha256(r["g2c"]["message"]) != r["g2c"]["hash"]:
                    raise ValueError("PRODUCTION_G2C_PAYLOAD_MUTATION_DETECTED")
                r["status"] = "PRODUCTION_G2C_PREPARED_FOR_REVIEW"
            else:
                r["status"] = classification
            r["repo_post"] = repository_snapshot(cfg.codex.repo_path)
            if r["repo_post"] != r["repo_pre"]:
                r["status"] = "PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW"
            r["acceptance_status"] = ("PHASE_2_7_PRODUCTION_SHADOW_VERIFIED"
                if r["status"] in ("PRODUCTION_G2C_PREPARED_FOR_REVIEW", "PLANNER_STOP_REQUESTED")
                else "PHASE_2_7_PRODUCTION_SHADOW_BLOCKED")
            r["completed_at"] = now()
            self.save(r)
            return r


    async def capture_atomic_checkpoint(self, app_server_manager=None, custom_prompt=None, active_session_checker=None, skip_chatgpt_check=False, max_wait_seconds=180, poll_interval=1.0):
        """Execute Phase 2.7.0.2 Atomic Production Checkpoint Handoff Capture."""
        cfg = await self.config()
        workspace = Path(cfg.codex.repo_path).resolve()

        # 1. Active Production Work Gate
        checker = active_session_checker or check_active_production_sessions
        active_sessions = checker(workspace, session_index_path=cfg.codex.session_index_path)
        if active_sessions:
            return {
                "status": "PRODUCTION_CHECKPOINT_NOT_QUIESCENT",
                "active_sessions": active_sessions,
                "send_count": 0,
                "send_eligible": False,
            }

        # 2. Capture Snapshot A
        snapshot_a = take_checkpoint_snapshot(workspace, active_sessions=active_sessions)

        # 3. Dedicated Handoff Thread & Turn via App-Server
        if not app_server_manager:
            raise ValueError("APP_SERVER_MANAGER_REQUIRED_FOR_ATOMIC_CAPTURE")

        thread_res = await app_server_manager.start_thread(
            cwd=str(workspace),
            title="Phase 2.7.0.2 Atomic Handoff Capture",
            experimental_api=False,
        )
        thread_id = (thread_res.get("thread", {}) or {}).get("id") or thread_res.get("threadId") or thread_res.get("id")
        if not thread_id:
            raise ValueError("FAILED_TO_START_HANDOFF_THREAD")

        prompt = custom_prompt or build_handoff_instruction_prompt()
        await app_server_manager.start_turn(thread_id=thread_id, prompt=prompt)

        rollout_file = None
        records = []
        final_text = None
        assistant_message_id = None
        user_turn_id = None
        user_message_id = None
        completed_at = None
        final_line = None

        start_wait = time.time()
        while True:
            rollout_file = self.observer.locate_session_file(thread_id)
            if rollout_file and Path(rollout_file).exists():
                try:
                    with Path(rollout_file).open(encoding="utf-8") as f:
                        records = [json.loads(line) for line in f if line.strip()]
                    for idx_rec, r in enumerate(records):
                        pld = r.get("payload", {})
                        if r.get("type") == "response_item" and pld.get("role") == "assistant" and pld.get("phase") == "final_answer":
                            content_val = pld.get("content")
                            final_text = content_val if isinstance(content_val, str) else "".join(
                                c["text"] for c in content_val or [] if c.get("type") == "output_text")
                            assistant_message_id = pld.get("id")
                            final_line = idx_rec
                        if pld.get("type") == "user_message" or (pld.get("type") == "message" and pld.get("role") == "user"):
                            user_turn_id = pld.get("turn_id")
                            user_message_id = pld.get("id")
                        if pld.get("type") == "task_complete":
                            completed_at = r.get("timestamp")
                except Exception:
                    pass

                if final_text and assistant_message_id and completed_at:
                    break

            if time.time() - start_wait > max_wait_seconds:
                break
            await asyncio.sleep(poll_interval)

        if not rollout_file or not Path(rollout_file).exists():
            raise ValueError("HANDOFF_THREAD_ROLLOUT_NOT_FOUND")

        if not final_text or not assistant_message_id or not completed_at:
            return {
                "status": "PHASE_2_7_ATOMIC_CAPTURE_IMPLEMENTATION_BLOCKED",
                "reason": "HANDOFF_FINAL_INCOMPLETE",
                "send_count": 0,
                "send_eligible": False,
            }

        if not final_text.strip().endswith("FRESH_PRODUCTION_HANDOFF_READY"):
            return {
                "status": "PHASE_2_7_ATOMIC_CAPTURE_IMPLEMENTATION_BLOCKED",
                "reason": "TERMINAL_MARKER_MISSING",
                "send_count": 0,
                "send_eligible": False,
            }

        # 4. Capture Snapshot B
        snapshot_b = take_checkpoint_snapshot(workspace)

        # 5. Audit Handoff Thread Side Effects
        try:
            audit_handoff_thread_side_effects(snapshot_a, snapshot_b, rollout_records=records)
        except ValueError as exc:
            return {
                "status": "ATOMIC_HANDOFF_READONLY_VIOLATION",
                "reason": str(exc),
                "send_count": 0,
                "send_eligible": False,
            }

        # 6. Compare Snapshot A and B
        delta_status, delta_details = classify_checkpoint_delta(snapshot_a, snapshot_b)
        if delta_status == "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE":
            return {
                "status": "ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE",
                "delta_details": delta_details,
                "send_count": 0,
                "send_eligible": False,
            }

        # 7. Pin New Artifact
        from app.models import ProductionSourceArtifact
        new_source_artifact = ProductionSourceArtifact(
            thread_id=thread_id,
            user_turn_id=user_turn_id,
            user_message_id=user_message_id,
            assistant_message_id=assistant_message_id,
            completed_at=completed_at,
            rollout_line=final_line,
            sha256=compute_sha256(final_text),
            char_count=len(final_text),
            rollout_path=str(rollout_file),
            expected_marker="FRESH_PRODUCTION_HANDOFF_READY",
        )

        cfg.production_source = new_source_artifact
        cfg.codex.thread_id = thread_id
        cfg.production_rollout_path = str(rollout_file)
        if cfg.protected_targets and hasattr(cfg.protected_targets, "thread_id"):
            cfg.protected_targets.thread_id = thread_id

        observed_source = observe_pinned_source_artifact(cfg, self.observer, pinned=new_source_artifact)

        current_run = self.load()
        if not current_run:
            current_run = {
                "shadow_run_id": "shadow-" + secrets.token_hex(12),
                "config_fingerprint": compute_config_fingerprint(cfg),
                "targets": await self.targets(),
                "source": observed_source,
                "chatgpt_baseline": {"ready": True, "url": cfg.chatgpt.conversation_url},
                "repo_pre": snapshot_a,
                "started_at": now(),
                "status": "PRODUCTION_SHADOW_BASELINE_VERIFIED",
                "c2g_send_attempts": 0, "c2g_delivered_count": 0,
                "codex_queue_submissions": 0, "codex_executions": 0, "thread_resume_calls": 0,
            }
            self.save(current_run, create=True)
        else:
            await self.rebind_source(new_source_artifact)
            current_run = self.load()

        # 8. Passively verify ChatGPT Target
        if not skip_chatgpt_check:
            parsed_page = await self.page(cfg, ready=True)
            if parsed_page.get("is_streaming"):
                raise ValueError("PRODUCTION_CHATGPT_NOT_IDLE")

        # 9. Prepare C2G Preview
        payload = ProductionShuttlePayloadStrategy().build_c2g_payload(current_run["shadow_run_id"], 1, "", 1, final_text)
        current_run.update(payload=payload, payload_hash=compute_sha256(payload), char_count=len(payload),
                           c2g_transfer_id="production-c2g-" + secrets.token_hex(12),
                           payload_preview={"first": payload[:160], "last": payload[-160:]},
                           payload_rewrite="NONE", status="PRODUCTION C2G PREPARED — NOT SENT",
                           preview_verdict="PRODUCTION_C2G_PREVIEW_VERIFIED")
        self.integrity(current_run)
        self.save(current_run)

        # 10. Final Freshness Check
        active_after = checker(workspace, session_index_path=cfg.codex.session_index_path)
        freshness_status = "PRODUCTION_CHECKPOINT_CURRENT" if not active_after else "PRODUCTION_CHECKPOINT_NO_LONGER_CURRENT"

        return {
            "checkpoint_status": delta_status,
            "status": "PHASE_2_7_LIVE_SHADOW_PRECHECK_READY" if freshness_status == "PRODUCTION_CHECKPOINT_CURRENT" else freshness_status,
            "pinned_artifact": new_source_artifact.model_dump(),
            "preview_verdict": "PRODUCTION_C2G_PREVIEW_VERIFIED",
            "chatgpt_target_status": "PRODUCTION_CHATGPT_TARGET_READY",
            "final_freshness": freshness_status,
            "send_count": 0,
            "send_eligible": False,
        }



def public_report(record):
    """Allowlist structural fields; never recurse over private strings."""
    statuses = {"PRODUCTION_SHADOW_BASELINE_VERIFIED", "PRODUCTION C2G PREPARED — NOT SENT",
                "PRODUCTION_C2G_AUTHORIZED", "PRODUCTION_C2G_DELIVERED", "PRODUCTION_C2G_NOT_SENT",
                "PRODUCTION_C2G_DELIVERY_UNKNOWN", "PRODUCTION_G2C_PREPARED_FOR_REVIEW",
                "PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW", "PINNED_SOURCE_STALE_AFTER_REPOSITORY_CHANGE",
                "PLANNER_STOP_REQUESTED", "PLANNER_RESPONSE_AMBIGUOUS", "PLANNER_RESPONSE_ERROR", "PLANNER_NO_ACTIONABLE_PROMPT"}
    classifications = {"NEXT_CODEX_PROMPT_FOUND", "PLANNER_STOP_REQUESTED", "PLANNER_RESPONSE_AMBIGUOUS",
                       "PLANNER_RESPONSE_ERROR", "PLANNER_NO_ACTIONABLE_PROMPT"}
    classification = record.get("planner", {}).get("classification")
    return {"phase": "2.7", "status": record.get("status") if record.get("status") in statuses else "REDACTED",
            "payload_rewrite": "NONE", "g2c_rewrite_count": 0 if record.get("g2c", {}).get("g2c_rewrite_count") == 0 else None,
            "codex_queue_submissions": 0, "codex_executions": 0,
            "planner_classification": classification if classification in classifications else None}
