"""One operator protocol grant -> one read-only capture -> at most one browser attempt.

This is the production coordinator. The older coordinator supplies parsers and
read-only planner extraction, but its manual execution methods are disabled here.
"""
import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import secrets
import time

from app.adapters import NotSentError
from app.canary import compute_config_fingerprint, same_path, real_uuid
from app.canonical import compute_semantic_hash
from app.models import ProductionSourceArtifact, compute_sha256
from app.production_shadow import (
    ProductionShadow, audit_handoff_thread_side_effects,
    build_handoff_instruction_prompt, classify_checkpoint_delta, now,
    observe_pinned_source_artifact, repository_snapshot, take_checkpoint_snapshot,
)

OPERATION = "ATOMIC_CAPTURE_AND_SINGLE_C2G"
STABLE = {"ATOMIC_PRODUCTION_CHECKPOINT_CAPTURED",
          "ATOMIC_PRODUCTION_CHECKPOINT_SEMANTICALLY_STABLE"}


def protocol_binding():
    # Restarting on different executable code invalidates outstanding grants.
    root = Path(__file__).parent
    files = sorted(p for p in root.rglob("*") if p.is_file() and p.suffix in (".py", ".html", ".js", ".css"))
    return {"handoff_prompt_sha256": compute_sha256(build_handoff_instruction_prompt()),
            "implementation_sha256": compute_sha256("".join(
                str(p.relative_to(root)).replace("\\", "/") + ":" + compute_sha256(p.read_bytes().hex()) for p in files))}


def production_session_liveness(workspace, sessions_dir, exclude_thread=None):
    """Inspect full rollouts; retain superseded history separately from blockers."""
    from app.session_liveness import inspect_rollout
    root = Path(sessions_dir)
    if not root.is_dir():
        raise ValueError("PRODUCTION_SESSION_INVENTORY_UNAVAILABLE")
    reports = []
    for path in root.rglob("*.jsonl"):
        report = inspect_rollout(path, workspace, exclude_thread)
        if report is not None:
            reports.append(report)
    return reports


def active_production_sessions(workspace, sessions_dir, exclude_thread=None):
    """Unknown current turns block; proven superseded orphans do not."""
    return [r for r in production_session_liveness(workspace, sessions_dir, exclude_thread) if r["blocking"]]


@contextmanager
def workflow_lock(path):
    """Process-wide advisory lock; released by the OS on crash, never by timeout."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        stream.seek(0, 2)
        if not stream.tell():
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError("ATOMIC_C2G_TRANSACTION_ALREADY_RUNNING") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class TransactionalProductionShadow(ProductionShadow):
    def __init__(self, *args, handoff_manager=None, session_inventory=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.handoff_manager = handoff_manager
        self.session_inventory = session_inventory

    def connect(self):
        db = super().connect()
        db.execute("CREATE TABLE IF NOT EXISTS atomic_c2g_parents "
                   "(id TEXT PRIMARY KEY, token_hash TEXT UNIQUE NOT NULL, binding TEXT NOT NULL, "
                   "expires_at TEXT NOT NULL, consumed INTEGER NOT NULL DEFAULT 0)")
        db.execute("CREATE TABLE IF NOT EXISTS atomic_c2g_history (id TEXT PRIMARY KEY, record TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS atomic_c2g_prechecks (id INTEGER PRIMARY KEY, parent_id TEXT, record TEXT NOT NULL)")
        return db

    def save(self, record, create=False):
        with self.connect() as db:
            if create:
                db.execute("INSERT INTO production_shadow_runs(slot,record) VALUES(1,?)", (json.dumps(record),))
            elif db.execute("UPDATE production_shadow_runs SET record=? WHERE slot=1 AND (send_attempted=0 OR ?=1)",
                            (json.dumps(record), int(bool(record.get("send_attempted_at"))))).rowcount != 1:
                raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
            db.execute("INSERT OR REPLACE INTO atomic_c2g_history(id,record) VALUES(?,?)",
                       (record["shadow_run_id"], json.dumps(record)))

    async def _manual_forbidden(self, *args, **kwargs):
        raise ValueError("PRODUCTION_TRANSACTIONAL_C2G_REQUIRED")

    baseline = prepare = authorize = send = rebind_source = capture_atomic_checkpoint = _manual_forbidden

    async def targets(self):
        targets = await super().targets()
        cfg = await self.config()
        targets["parent_binding"] = self.parent_binding(cfg)
        return targets

    async def quiescence(self):
        """One read-only inventory and fresh repository checkpoint; no grant/I/O."""
        cfg = await self.config()
        inventory = self._inventory(cfg)
        sessions, blockers = inventory["sessions"], inventory["blockers"]
        snapshot = take_checkpoint_snapshot(cfg.codex.repo_path, active_sessions=blockers)
        return {"status": "PRODUCTION_WORKSPACE_GENUINELY_BUSY" if blockers else "PRODUCTION_QUIESCENCE_VERIFIED",
                "reauth_status": None if blockers else "PHASE_2_7_TRANSACTIONAL_C2G_REAUTH_READY",
                "sessions": sessions, "snapshot": snapshot, "inventory_scope": inventory["scope"],
                "transaction_authorized": False}

    def _inventory(self, cfg, exclude_thread=None):
        if self.session_inventory is not None:
            return self.session_inventory(cfg.codex.repo_path, cfg.codex.sessions_dir, exclude_thread)
        sessions = production_session_liveness(cfg.codex.repo_path, cfg.codex.sessions_dir, exclude_thread)
        return {"sessions": sessions, "blockers": [r for r in sessions if r["blocking"]],
                "scope": {"session_roots": [cfg.codex.sessions_dir], "complete": True}}

    def parent_binding(self, cfg):
        return {"workspace": str(Path(cfg.codex.repo_path).resolve()),
                "conversation_url": cfg.chatgpt.conversation_url,
                "conversation_id": cfg.chatgpt.conversation_id,
                "config_fingerprint": compute_config_fingerprint(cfg),
                "production_shadow_mode": True, "operation": OPERATION,
                "max_c2g_sends": 1, "max_handoff_captures": 1,
                **protocol_binding()}

    async def authorize_parent(self, binding, operator_confirmed, ttl_seconds=600):
        cfg = await self.config()
        if not operator_confirmed or not cfg.allow_real_c2g_send:
            raise ValueError("ATOMIC_C2G_PARENT_CONFIRMATION_REQUIRED")
        if json.dumps(binding, sort_keys=True) != json.dumps(self.parent_binding(cfg), sort_keys=True) or not 1 <= ttl_seconds <= 900:
            raise ValueError("ATOMIC_C2G_PARENT_BINDING_MISMATCH")
        token, aid = secrets.token_hex(32), secrets.token_hex(24)
        expiry = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        with self.connect() as db:
            attempted = db.execute("SELECT send_attempted FROM production_shadow_runs WHERE slot=1").fetchone()
            if attempted and attempted[0]:
                raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
            db.execute("INSERT INTO atomic_c2g_parents(id,token_hash,binding,expires_at) VALUES(?,?,?,?)",
                       (aid, compute_sha256(token), json.dumps(binding, sort_keys=True), expiry))
        return {"token": token, "authorization_id": aid, "expires_at": expiry, "binding": binding}

    def _parent(self, cfg, token):
        with self.connect() as db:
            row = db.execute("SELECT id,binding,expires_at,consumed FROM atomic_c2g_parents WHERE token_hash=?",
                             (compute_sha256(token),)).fetchone()
        if (not row or row[3] or row[2] <= now()
                or row[1] != json.dumps(self.parent_binding(cfg), sort_keys=True)):
            raise ValueError("ATOMIC_C2G_PARENT_AUTHORIZATION_REJECTED")
        return {"id": row[0], "binding": json.loads(row[1]), "expires_at": row[2]}

    def _active(self, cfg, exclude_thread=None):
        if self.session_inventory is not None:
            return self._inventory(cfg, exclude_thread)["blockers"]
        return active_production_sessions(cfg.codex.repo_path, cfg.codex.sessions_dir, exclude_thread)

    async def _ready_page(self, cfg):
        parsed = await self.page(cfg, ready=True)
        if (parsed.get("is_streaming") or (parsed.get("composer") or {}).get("text")
                or not parsed.get("turns")):
            raise ValueError("PRODUCTION_CHATGPT_TARGET_NOT_READY")
        return parsed

    async def recover(self):
        """Only classify interrupted work. Never resume capture, derive, or send."""
        async with self.lock:
            with workflow_lock(self.path.with_suffix(".atomic.lock")):
                r = self.load()
                if r and r.get("atomic_phase") in ("CAPTURING", "PINNED", "CHILD_CONSUMED"):
                    r["status"] = ("PRODUCTION_C2G_RECOVERY_REQUIRED" if r.get("child_authorization")
                                   else "ATOMIC_C2G_INTERRUPTED_NEW_PARENT_REQUIRED")
                    r["atomic_phase"] = "INTERRUPTED"
                    r["recovered_at"] = now()
                    self.save(r)
                return r

    async def _capture_handoff(self, cfg, r, max_wait_seconds=180):
        if self.handoff_manager is None:
            raise ValueError("APP_SERVER_MANAGER_REQUIRED_FOR_ATOMIC_CAPTURE")
        # This capability accepts neither a prompt nor a pre-existing thread ID.
        result = await self.handoff_manager.start_readonly_handoff(str(Path(cfg.codex.repo_path).resolve()))
        tid, turn = result["thread_id"], result["turn_id"]
        real_uuid(tid)
        if tid == cfg.codex.thread_id:
            raise ValueError("ATOMIC_HANDOFF_NEW_THREAD_REQUIRED")
        r["handoff_thread_id"], r["handoff_turn_id"] = tid, turn
        self.save(r)
        deadline = time.monotonic() + max_wait_seconds
        while True:
            path = self.observer.locate_session_file(tid)
            if path:
                # A concurrently appended partial record is incomplete, never valid.
                try:
                    records = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
                except json.JSONDecodeError:
                    records = []
                finals, completion, completion_line, user_id, current_turn = [], None, None, None, None
                for index, record in enumerate(records):
                    p = record.get("payload", {})
                    kind = p.get("type")
                    if record.get("type") == "turn_context" or kind == "task_started":
                        current_turn = p.get("turn_id") or current_turn
                    record_turn = p.get("turn_id") or current_turn
                    if record_turn != turn:
                        continue
                    if kind in ("task_aborted", "turn_aborted"):
                        raise ValueError("ATOMIC_HANDOFF_ABORTED")
                    if kind == "message" and p.get("role") == "user" or kind == "user_message":
                        user_id = p.get("id") or user_id
                    if (record.get("type") == "response_item" and kind == "message"
                            and p.get("role") == "assistant" and p.get("phase") == "final_answer"):
                        content = p.get("content")
                        text = content if isinstance(content, str) else "".join(
                            c["text"] for c in content or [] if c.get("type") == "output_text")
                        finals.append((index, p.get("id"), text))
                    if kind == "task_complete":
                        completion = record.get("timestamp")
                        completion_line = index
                if completion:
                    if (len(finals) != 1 or not finals[0][1] or completion_line <= finals[0][0]
                            or not finals[0][2].endswith("FRESH_PRODUCTION_HANDOFF_READY")):
                        raise ValueError("ATOMIC_HANDOFF_FINAL_INVALID")
                    line, mid, text = finals[0]
                    return {"thread_id": tid, "user_turn_id": turn, "user_message_id": user_id,
                            "assistant_message_id": mid, "completed_at": completion, "rollout_line": line,
                            "sha256": compute_sha256(text), "char_count": len(text),
                            "rollout_path": str(Path(path).resolve()),
                            "expected_marker": "FRESH_PRODUCTION_HANDOFF_READY"}, records
            if time.monotonic() >= deadline:
                raise ValueError("ATOMIC_HANDOFF_COMPLETION_TIMEOUT")
            await asyncio.sleep(0.5)

    def _verify_artifact(self, cfg, artifact):
        # Bind a copy for evidence verification; never change the operator config.
        pinned = ProductionSourceArtifact.model_validate(artifact)
        evidence_cfg = cfg.model_copy(deep=True)
        evidence_cfg.codex.thread_id = pinned.thread_id
        evidence_cfg.codex.thread_title = None
        evidence_cfg.protected_targets.thread_id = pinned.thread_id
        evidence_cfg.production_source = pinned
        evidence_cfg.production_rollout_path = pinned.rollout_path
        return observe_pinned_source_artifact(evidence_cfg, self.observer, pinned)

    async def capture_and_send_atomic_c2g(self, token):
        result = await self._capture_and_deliver(token)
        if result.get("status") == "PRODUCTION_C2G_DELIVERED":
            return await self._wait_for_planner()
        return result

    async def _wait_for_planner(self, timeout_seconds=180):
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                return await self.observe_planner()
            except Exception as exc:
                if str(exc) != "PRODUCTION_PLANNER_STALE_OR_INCOMPLETE":
                    r = self.load()
                    r.update(planner_observation_status="PRODUCTION_PLANNER_OBSERVATION_BLOCKED",
                             planner_observation_error=str(exc),
                             acceptance_status="PHASE_2_7_PRODUCTION_SHADOW_BLOCKED")
                    self.save(r)
                    return r
            if time.monotonic() >= deadline:
                r = self.load()
                r["planner_observation_status"] = "PRODUCTION_PLANNER_OBSERVATION_PENDING"
                self.save(r)
                return r
            await asyncio.sleep(0.5)

    async def _capture_and_deliver(self, token):
        async with self.lock:
            with workflow_lock(self.path.with_suffix(".atomic.lock")):
                cfg = await self.config()
                parent = self._parent(cfg, token)
                # An operator grant starts one transaction, including a blocked
                # precheck. Capture/send budgets remain independently unconsumed.
                with self.connect() as db:
                    if db.execute("UPDATE atomic_c2g_parents SET consumed=1 WHERE id=? AND consumed=0 AND expires_at>?",
                                  (parent["id"], now())).rowcount != 1:
                        raise ValueError("ATOMIC_C2G_PARENT_AUTHORIZATION_REJECTED")
                r = None
                try:
                    with self.connect() as db:
                        prior = db.execute("SELECT record,send_attempted FROM production_shadow_runs WHERE slot=1").fetchone()
                    if prior and (prior[1] or json.loads(prior[0]).get("child_authorization")
                                  or json.loads(prior[0]).get("status") in (
                                      "PRODUCTION_C2G_AUTHORIZED", "PRODUCTION_C2G_DELIVERY_UNKNOWN",
                                      "PRODUCTION_C2G_RECOVERY_REQUIRED")):
                        raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
                    if self._active(cfg):
                        raise ValueError("PRODUCTION_WORKSPACE_NOT_QUIESCENT")
                    baseline = await self._ready_page(cfg)
                    a = take_checkpoint_snapshot(cfg.codex.repo_path)
                    if cfg.codex.expected_branch and a["branch"] != cfg.codex.expected_branch:
                        raise ValueError("PRODUCTION_BRANCH_MISMATCH")
                    if self.handoff_manager is None:
                        raise ValueError("APP_SERVER_MANAGER_REQUIRED_FOR_ATOMIC_CAPTURE")
                    r = {"shadow_run_id": secrets.token_hex(24), "parent": parent,
                         "atomic_phase": "CAPTURING", "status": "ATOMIC_C2G_CAPTURING",
                         "config_fingerprint": compute_config_fingerprint(cfg), "targets": await self.targets(),
                         "snapshot_a": a, "chatgpt_baseline": baseline, "started_at": now(),
                         "c2g_send_attempts": 0, "c2g_delivered_count": 0,
                         "codex_queue_submissions": 0, "codex_executions": 0, "thread_resume_calls": 0,
                         "readonly_handoff_captures": 1}
                    with self.connect() as db:
                        db.execute("BEGIN IMMEDIATE")
                        if now() >= parent["expires_at"]:
                            raise ValueError("ATOMIC_C2G_PARENT_AUTHORIZATION_REJECTED")
                        if prior:
                            changed = db.execute("UPDATE production_shadow_runs SET record=? WHERE slot=1 AND send_attempted=0 AND record=?",
                                                 (json.dumps(r), prior[0])).rowcount
                            if changed != 1:
                                raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
                        else:
                            db.execute("INSERT INTO production_shadow_runs(slot,record) VALUES(1,?)", (json.dumps(r),))
                        db.execute("INSERT INTO atomic_c2g_history(id,record) VALUES(?,?)", (r["shadow_run_id"], json.dumps(r)))
                    artifact, records = await self._capture_handoff(cfg, r)
                    b = take_checkpoint_snapshot(cfg.codex.repo_path, active_sessions=self._active(cfg))
                    r["snapshot_b"] = b
                    delta, details = classify_checkpoint_delta(a, b)
                    r["checkpoint_status"], r["checkpoint_delta"] = delta, details
                    if delta not in STABLE or b["active_sessions"]:
                        raise ValueError("ATOMIC_HANDOFF_CAPTURE_RACED_WITH_PRODUCTION_CHANGE")
                    audit_handoff_thread_side_effects(a, b, records)
                    source = self._verify_artifact(cfg, artifact)
                    r.update(pinned_artifact=artifact, source=source, payload=source["text"],
                             payload_hash=source["hash"], char_count=source["char_count"],
                             payload_rewrite="NONE", rewrite_count=0, atomic_phase="PINNED",
                             c2g_transfer_id="production-c2g-" + secrets.token_hex(12),
                             preview_verdict="PRODUCTION_C2G_PREVIEW_VERIFIED")
                    self.integrity(r)
                    self.save(r)
                    # All asynchronous readiness/config work precedes Snapshot C.
                    final_cfg = await self.config()
                    if self.parent_binding(final_cfg) != parent["binding"] or now() >= parent["expires_at"]:
                        raise ValueError("ATOMIC_C2G_PARENT_AUTHORIZATION_REJECTED")
                    if await self._ready_page(cfg) != baseline:
                        raise ValueError("PRODUCTION_CHATGPT_BASELINE_CHANGED")
                    self._verify_artifact(cfg, artifact)
                    r["repo_pre"] = repository_snapshot(cfg.codex.repo_path)
                    c = take_checkpoint_snapshot(cfg.codex.repo_path, active_sessions=self._active(cfg))
                    r["snapshot_c"] = c
                    delta, details = classify_checkpoint_delta(b, c)
                    r["final_delta"] = details
                    if delta not in STABLE or c["active_sessions"]:
                        raise ValueError("ATOMIC_C2G_FINAL_FRESHNESS_FAILED")
                    # No awaits between final snapshot and durable child/budget CAS.
                    return await self._consume_and_deliver(cfg, r)
                except Exception as exc:
                    if r and self.load() and self.load().get("shadow_run_id") == r["shadow_run_id"]:
                        if r.get("child_authorization"):
                            r["status"] = "PRODUCTION_C2G_DELIVERY_UNKNOWN"
                        else:
                            r["status"] = str(exc)
                        r.update(atomic_phase="BLOCKED", error=str(exc),
                                 acceptance_status="PHASE_2_7_PRODUCTION_SHADOW_BLOCKED",
                                 c2g_allowance_consumed=bool(r.get("send_attempted_at")))
                        self.save(r)
                        return r
                    blocked = {"status": "ATOMIC_C2G_PRECAPTURE_BLOCKED", "reason": str(exc),
                               "c2g_allowance_consumed": False, "browser_attempts": 0, "checked_at": now()}
                    with self.connect() as db:
                        db.execute("INSERT INTO atomic_c2g_prechecks(parent_id,record) VALUES(?,?)",
                                   (parent["id"], json.dumps(blocked)))
                    return blocked

    async def _consume_and_deliver(self, cfg, r):
        self.integrity(r)
        if (r["char_count"] != len(r["payload"]) or r["parent"]["binding"] != self.parent_binding(cfg)
                or now() >= r["parent"]["expires_at"]):
            raise ValueError("ATOMIC_C2G_PARENT_AUTHORIZATION_REJECTED")
        child = {"id": secrets.token_hex(24), "parent_authorization_id": r["parent"]["id"],
                 "shadow_run_id": r["shadow_run_id"], "source_message_id": r["source"]["assistant_message_id"],
                 "source_turn_id": r["source"]["turn_id"],
                 "payload_sha256": r["payload_hash"], "char_count": r["char_count"],
                 "conversation_id": cfg.chatgpt.conversation_id, "config_fingerprint": r["config_fingerprint"],
                 "snapshot_c_fingerprint": r["snapshot_c"]["fingerprint"], "direction": "C2G", "max_sends": 1,
                 "consumed_at": now()}
        persisted = self.load()
        if persisted.get("shadow_run_id") != r["shadow_run_id"] or persisted.get("atomic_phase") != "PINNED":
            raise ValueError("ATOMIC_C2G_TRANSACTION_MISMATCH")
        original = json.dumps(persisted)
        r.update(child_authorization=child, atomic_phase="CHILD_CONSUMED", status="PRODUCTION_C2G_DELIVERY_UNKNOWN",
                 send_attempted_at=now(), c2g_send_attempts=1, c2g_allowance_consumed=True)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("UPDATE production_shadow_runs SET record=?,send_attempted=1 "
                          "WHERE slot=1 AND send_attempted=0 AND record=?",
                          (json.dumps(r), original)).rowcount != 1:
                # No child was persisted and browser I/O must never start.
                r.pop("child_authorization", None)
                r.pop("send_attempted_at", None)
                r.update(c2g_send_attempts=0, c2g_allowance_consumed=False)
                raise ValueError("PRODUCTION_C2G_ALREADY_ATTEMPTED")
            db.execute("INSERT OR REPLACE INTO atomic_c2g_history(id,record) VALUES(?,?)",
                       (r["shadow_run_id"], json.dumps(r)))
        try:
            result = await self.sender.send_message(message=r["payload"], confirmation_token=child["id"],
                transfer_id=r["c2g_transfer_id"], config_fingerprint=r["config_fingerprint"], production_shadow=True)
            evidence = result.get("delivery_evidence") or {}
            if (result.get("status") != "delivered" or not evidence.get("user_turn_id")
                    or evidence.get("matching_turn_count") != 1
                    or evidence.get("semantic_delivery_status") != "SEMANTIC_DELIVERY_CONFIRMED"
                    or result.get("raw_content_hash") != r["payload_hash"]
                    or result.get("semantic_content_hash") != compute_semantic_hash(r["payload"])):
                raise ValueError("PRODUCTION_C2G_DELIVERY_UNKNOWN")
            parsed = await self.page(cfg)
            old = r["chatgpt_baseline"]["turns"]
            old_ids = {t.get("turn_id") for t in old}
            old_messages = {t.get("message_id") for t in old if t.get("message_id")}
            users = [t for t in parsed.get("turns", []) if t.get("role") == "user" and t.get("turn_id") not in old_ids]
            if (len(users) != 1 or users[0].get("turn_id") != evidence["user_turn_id"]
                    or users[0].get("message_id") in old_messages
                    or (evidence.get("user_message_id") and users[0].get("message_id") != evidence["user_message_id"])
                    or compute_semantic_hash(users[0].get("text", "")) != compute_semantic_hash(r["payload"])):
                raise ValueError("PRODUCTION_C2G_DELIVERY_UNKNOWN")
            r.update(delivery={**evidence, "raw_content_hash": r["payload_hash"],
                               "semantic_content_hash": compute_semantic_hash(r["payload"]), "observed_at": now()},
                     status="PRODUCTION_C2G_DELIVERED", atomic_phase="DELIVERED", c2g_delivered_count=1)
        except NotSentError as exc:
            r.update(status="PRODUCTION_C2G_NOT_SENT", error=str(exc), atomic_phase="ATTEMPT_TERMINAL")
        except Exception as exc:
            r.update(status="PRODUCTION_C2G_DELIVERY_UNKNOWN", error=str(exc), atomic_phase="ATTEMPT_TERMINAL")
        r["repo_post_delivery"] = repository_snapshot(cfg.codex.repo_path)
        r["external_repository_delta_observed"] = r["repo_post_delivery"] != r["repo_pre"]
        self.save(r)
        return r

    async def observe_planner(self):
        result = await super().observe_planner()
        if result.get("status") == "PRODUCTION_CHATGPT_INTERLEAVING_DETECTED":
            return result
        result["atomic_phase"] = "PLANNER_OBSERVED"
        self.save(result)
        return result
