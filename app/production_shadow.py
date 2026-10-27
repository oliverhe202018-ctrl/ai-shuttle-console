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


def observe_source(config, observer):
    """Validate the exact header and index before parsing final-answer evidence."""
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

    async def targets(self):
        cfg = await self.config()
        return {"thread_id": cfg.codex.thread_id, "workspace": str(Path(cfg.codex.repo_path).resolve()),
                "expected_branch": cfg.codex.expected_branch, "rollout": cfg.production_rollout_path,
                "conversation_url": cfg.chatgpt.conversation_url, "conversation_id": cfg.chatgpt.conversation_id,
                "config_fingerprint": compute_config_fingerprint(cfg), "mode": "PRODUCTION SHADOW",
                "auto_execution": "OFF", "g2c_queue_permission": "OFF", "manual_authorization": "REQUIRED"}

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
        if source != r["source"]:
            raise ValueError("PRODUCTION_SOURCE_BASELINE_CHANGED")
        if repository_snapshot(cfg.codex.repo_path) != r["repo_pre"]:
            r["status"] = "PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW"
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


def public_report(record):
    """Allowlist structural fields; never recurse over private strings."""
    statuses = {"PRODUCTION_SHADOW_BASELINE_VERIFIED", "PRODUCTION C2G PREPARED — NOT SENT",
                "PRODUCTION_C2G_AUTHORIZED", "PRODUCTION_C2G_DELIVERED", "PRODUCTION_C2G_NOT_SENT",
                "PRODUCTION_C2G_DELIVERY_UNKNOWN", "PRODUCTION_G2C_PREPARED_FOR_REVIEW",
                "PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW", "PLANNER_STOP_REQUESTED",
                "PLANNER_RESPONSE_AMBIGUOUS", "PLANNER_RESPONSE_ERROR", "PLANNER_NO_ACTIONABLE_PROMPT"}
    classifications = {"NEXT_CODEX_PROMPT_FOUND", "PLANNER_STOP_REQUESTED", "PLANNER_RESPONSE_AMBIGUOUS",
                       "PLANNER_RESPONSE_ERROR", "PLANNER_NO_ACTIONABLE_PROMPT"}
    classification = record.get("planner", {}).get("classification")
    return {"phase": "2.7", "status": record.get("status") if record.get("status") in statuses else "REDACTED",
            "payload_rewrite": "NONE", "g2c_rewrite_count": 0 if record.get("g2c", {}).get("g2c_rewrite_count") == 0 else None,
            "codex_queue_submissions": 0, "codex_executions": 0,
            "planner_classification": classification if classification in classifications else None}
