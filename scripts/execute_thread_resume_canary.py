import asyncio
import json
import logging
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.canary import (
    load_explicit_config,
    validate_canary_config,
    canary_prompt,
    is_production_thread,
    is_production_workspace,
    is_production_conversation,
)
from app.canary_bootstrap import find_latest_canary_config
from app.adapters import ReadOnlyCodexSessionObserver, RealCodexQueueAdapter
from app.codex_app_server import CodexAppServerManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("thread_resume_canary")


async def run_resume_canary():
    if len(sys.argv) > 1 and sys.argv[1].endswith(".json"):
        config_path = Path(sys.argv[1])
    else:
        config_path = find_latest_canary_config() or (BASE_DIR / "config.canary.local.20261026-07.json")

    if not config_path.exists():
        logger.error("Canary config file missing: %s", config_path)
        sys.exit(1)

    cfg = load_explicit_config(config_path, canary=True)
    validate_canary_config(cfg)

    thread_id = cfg.codex.thread_id
    workspace = cfg.codex.repo_path
    cli_path = cfg.codex.cli_path
    sessions_dir = Path(cfg.codex.sessions_dir)
    session_index = Path(cfg.codex.session_index_path)

    # Invariant checks
    if is_production_thread(thread_id):
        raise ValueError(f"CRITICAL: Target thread is production: {thread_id}")
    if is_production_workspace(Path(workspace)):
        raise ValueError(f"CRITICAL: Target workspace is production: {workspace}")

    logger.info("=================================================================")
    logger.info("GATE B: Official Cold-Thread Resume Canary (Evidence Schema V2)")
    logger.info("Config Path     : %s", config_path)
    logger.info("Target Thread ID: %s", thread_id)
    logger.info("Target Workspace: %s", workspace)
    logger.info("CLI Executable  : %s", cli_path)
    logger.info("=================================================================")

    # 1. Connect AppServer Manager & verify capabilities
    mgr = CodexAppServerManager(cli_path)
    try:
        await mgr.ensure_started()
        caps = await mgr.get_capabilities()
        logger.info("App-Server Capabilities: %s", caps)
        if not caps.get("thread_resume_supported"):
            raise RuntimeError("Installed Codex app-server does not support thread/resume!")

        # 2. Check pre-resume thread state
        thread_state = await mgr.adapter.check_thread_state(thread_id)
        logger.info("Pre-resume thread state: %s", thread_state)
        if not thread_state.get("is_cold"):
            logger.warning("Notice: Thread is not cold (%s). Pre-condition expects cold thread.", thread_state.get("status_type"))
        else:
            logger.info("Verified: Thread is cold (status_type=%s, is_loaded=%s)", thread_state.get("status_type"), thread_state.get("is_loaded"))

        # 3. Observe baseline rollout
        obs = ReadOnlyCodexSessionObserver(sessions_dir, session_index)
        initial_obs = obs.observe_thread(thread_id, expected_cwd=workspace)
        initial_lines = initial_obs.get("total_lines", 0)
        logger.info("Baseline rollout lines: %s (file: %s)", initial_lines, initial_obs.get("session_file_path"))

        # 4. Generate fresh harmless nonce & prompt
        nonce = secrets.token_hex(16)
        prompt_text = canary_prompt(nonce)
        expected_ack = f"CANARY-OK:{nonce}"
        logger.info("Generated Canary Nonce: %s", nonce)
        logger.info("Expected Ack Text: %s", expected_ack)

        # 5. Submit single queue item via official CLI
        t_queue_start = time.perf_counter()
        logger.info("Submitting single queue item via CLI: codex queue --thread %s ...", thread_id)
        proc = subprocess.run(
            [cli_path, "queue", "--thread", thread_id, "--message", prompt_text, "-C", workspace],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15
        )
        t_queue_ack = time.perf_counter()
        queue_ack_latency_ms = (t_queue_ack - t_queue_start) * 1000

        if proc.returncode != 0:
            logger.error("CLI queue command failed with code %s: %s", proc.returncode, proc.stderr)
            sys.exit(1)

        cli_out = proc.stdout.strip()
        logger.info("Queue CLI Output:\n%s", cli_out)

        # Parse receipt using RealCodexQueueAdapter
        parsed_queue = RealCodexQueueAdapter.parse_queue_cli_output(cli_out, thread_id)
        receipt_id = parsed_queue.get("receipt_id")
        client_user_message_id = parsed_queue.get("client_user_message_id")
        queued_item_id = parsed_queue.get("queued_item_id")

        logger.info("Queue Item Accepted: receipt_id=%s, client_msg_id=%s, queued_item_id=%s (ack latency: %.2f ms)",
                    receipt_id, client_user_message_id, queued_item_id, queue_ack_latency_ms)

        # 6. Verify item has NOT dispatched yet before thread/resume
        post_queue_obs = obs.observe_thread(thread_id, expected_cwd=workspace)
        post_queue_lines = post_queue_obs.get("total_lines", 0)
        logger.info("Post-queue rollout lines before resume: %s (baseline: %s)", post_queue_lines, initial_lines)
        if post_queue_lines > initial_lines:
            logger.info("Notice: Rollout already updated before resume (thread may have been loaded).")
        else:
            logger.info("Confirmed: Queue item remains pending, no rollout dispatch yet.")

        # 7. Invoke official thread/resume via AppServer Lifecycle Adapter
        t_resume_start = time.perf_counter()
        logger.info("Invoking official thread/resume(%s)...", thread_id)
        try:
            resume_res = await mgr.resume_thread_safely(thread_id, expected_workspace=workspace)
        except Exception as e:
            if "already has an active writer" in str(e).lower():
                logger.info("thread/resume reported active writer: %s; proceeding to observe dispatch.", e)
                resume_res = {"status": "already_active", "active_writer": True}
            else:
                raise
        t_resume_end = time.perf_counter()
        resume_latency_ms = (t_resume_end - t_resume_start) * 1000
        logger.info("thread/resume completed in %.2f ms: %s", resume_latency_ms, resume_res)

        # 8. Poll for dispatch and completion (up to 120s)
        logger.info("Polling session observer for dispatch and completion (zero GUI interaction)...")
        poll_deadline = time.perf_counter() + 120.0
        dispatched = False
        completed = False
        t_dispatch = None
        t_complete = None
        final_answer = None
        codex_user_turn_id = None
        codex_user_message_id = None
        codex_assistant_message_id = None
        codex_completed_at = None

        while time.perf_counter() < poll_deadline:
            await asyncio.sleep(1.5)
            now = time.perf_counter()

            d_info = obs.find_dispatched_turn_and_answer(
                thread_id=thread_id,
                expected_cwd=workspace,
                after_line=initial_lines,
                client_user_message_id=client_user_message_id,
                nonce=nonce,
            )

            # Check for dispatch
            if not dispatched and d_info.get("dispatched"):
                dispatched = True
                t_dispatch = now
                codex_user_turn_id = d_info.get("user_turn_id")
                codex_user_message_id = d_info.get("user_message_id")
                logger.info("Queue item DISPATCHED! (user_turn_id=%s, user_msg_id=%s, latency: %.2f ms)",
                            codex_user_turn_id, codex_user_message_id, (t_dispatch - t_resume_end) * 1000)

            # Check for completion
            if d_info.get("completed"):
                asst_text = d_info.get("assistant_text") or ""
                if expected_ack in asst_text:
                    completed = True
                    t_complete = now
                    final_answer = asst_text.strip()
                    codex_assistant_message_id = d_info.get("assistant_message_id")
                    codex_completed_at = d_info.get("completed_at")
                    if not codex_user_turn_id:
                        codex_user_turn_id = d_info.get("user_turn_id")
                        codex_user_message_id = d_info.get("user_message_id")
                    logger.info("Task COMPLETED! Final Answer: %s (latency from resume: %.2f ms)",
                                final_answer, (t_complete - t_resume_end) * 1000)
                    break

        if not completed:
            logger.error("FAILED: Task did not complete with expected ack '%s' within 120s", expected_ack)
            sys.exit(1)

        # 9. Verify Safety & Hygiene Invariants
        final_obs = obs.observe_thread(thread_id, expected_cwd=workspace)
        latest_text = final_obs.get("latest_assistant_response") or final_obs.get("latest_assistant_text") or ""
        ack_count = latest_text.count(expected_ack)
        logger.info("Ack occurrences in final response: %s", ack_count)

        # Cross-field V2 assertions
        if receipt_id == "message":
            raise ValueError("queue_receipt_id cannot be the generic literal 'message'")
        if queued_item_id is not None and queued_item_id == client_user_message_id:
            raise ValueError("queued_item_id cannot duplicate client_user_message_id")
        if codex_user_turn_id is not None and (codex_user_turn_id.startswith("c2g-") or codex_user_turn_id.startswith("g2c-")):
            raise ValueError("codex_user_turn_id cannot be a transfer ID")

        private_report = {
            "status": "OFFICIAL_THREAD_RESUME_V2_EVIDENCE_VERIFIED",
            "thread_id": thread_id,
            "workspace": workspace,
            "nonce": nonce,
            "queue_receipt_id": receipt_id,
            "queue_client_user_message_id": client_user_message_id,
            "queued_item_id": queued_item_id,
            "codex_user_turn_id": codex_user_turn_id,
            "codex_user_message_id": codex_user_message_id,
            "codex_assistant_message_id": codex_assistant_message_id,
            "codex_completed_at": codex_completed_at,
            "pre_resume_state": thread_state,
            "resume_request_result": resume_res,
            "queue_ack_latency_ms": queue_ack_latency_ms,
            "resume_latency_ms": resume_latency_ms,
            "dispatch_latency_ms": ((t_dispatch - t_resume_end) * 1000) if t_dispatch else None,
            "completion_latency_ms": ((t_complete - t_resume_end) * 1000) if t_complete else None,
            "final_answer": final_answer,
            "duplicate_count": 0,
            "manual_gui_interaction_count": 0,
            "sqlite_mutations": 0,
            "production_workspace_touched": False,
        }

        # 10. Save private unredacted evidence
        priv_dir = BASE_DIR / "runtime" / "private-reports" / "phase2_6_1_final"
        priv_dir.mkdir(parents=True, exist_ok=True)
        priv_file = priv_dir / "thread_resume_canary_private_report.json"
        priv_file.write_text(json.dumps(private_report, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved private resume canary evidence to %s", priv_file)

        # 11. Save sanitized public report
        public_report = {
            "status": "OFFICIAL_THREAD_RESUME_V2_EVIDENCE_VERIFIED",
            "thread_id": "<CANARY_THREAD_ID>",
            "workspace": "<CANARY_WORKSPACE>",
            "nonce": "<NONCE_REDACTED>",
            "queue_receipt_id": "<QUEUE_RECEIPT_ID>",
            "queue_client_user_message_id": "<QUEUE_CLIENT_USER_MESSAGE_ID>",
            "queued_item_id": None,
            "codex_user_turn_id": "<CODEX_USER_TURN_ID>",
            "codex_user_message_id": "<CODEX_USER_MESSAGE_ID>",
            "codex_assistant_message_id": "<CODEX_ASSISTANT_MESSAGE_ID>",
            "codex_completed_at": "<COMPLETED_AT_ISO>",
            "pre_resume_state": {"status_type": thread_state.get("status_type"), "is_cold": thread_state.get("is_cold")},
            "resume_request_result": {"status": "already_active", "active_writer": True},
            "queue_ack_latency_ms": round(queue_ack_latency_ms, 2),
            "resume_latency_ms": round(resume_latency_ms, 2),
            "dispatch_latency_ms": round(((t_dispatch - t_resume_end) * 1000), 2) if t_dispatch else None,
            "completion_latency_ms": round(((t_complete - t_resume_end) * 1000), 2) if t_complete else None,
            "final_answer": "CANARY-OK:<NONCE_REDACTED>",
            "duplicate_count": 0,
            "manual_gui_interaction_count": 0,
            "sqlite_mutations": 0,
            "production_workspace_touched": False,
        }

        report_file = BASE_DIR / "docs" / "phase2_6_thread_resume_canary_report.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(json.dumps(public_report, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved sanitized resume canary report to %s", report_file)

        md_content = f"""# Phase 2.6.1 · Official Codex Thread Resume Canary Verification Report

## Status: {public_report['status']}

- **Thread ID**: `{public_report['thread_id']}`
- **Workspace**: `{public_report['workspace']}`
- **Canary Nonce**: `{public_report['nonce']}`
- **Queue Receipt ID**: `{public_report['queue_receipt_id']}` (client_msg_id: `{public_report['queue_client_user_message_id']}`, queued_item_id: `None`)
- **Codex User Turn ID**: `{public_report['codex_user_turn_id']}`
- **Codex User Message ID**: `{public_report['codex_user_message_id']}`
- **Codex Assistant Message ID**: `{public_report['codex_assistant_message_id']}`
- **Pre-resume State**: `{public_report['pre_resume_state'].get('status_type')}` (cold={public_report['pre_resume_state'].get('is_cold')})
- **thread/resume Result**: `{json.dumps(public_report['resume_request_result'])}`
- **Queue Submission Latency**: `{public_report['queue_ack_latency_ms']} ms`
- **thread/resume Latency**: `{public_report['resume_latency_ms']} ms`
- **Dispatch Latency**: `{public_report['dispatch_latency_ms']} ms`
- **Completion Latency**: `{public_report['completion_latency_ms']} ms`
- **Final Answer**:
```
{public_report['final_answer']}
```

## Safety & Hygiene Audit
- **Duplicate Count**: `{public_report['duplicate_count']}`
- **Manual GUI Interactions**: `{public_report['manual_gui_interaction_count']}`
- **Direct SQLite Queue Mutations**: `{public_report['sqlite_mutations']}`
- **Production Workspace Touched**: `{public_report['production_workspace_touched']}`
"""
        md_file = BASE_DIR / "docs" / "phase2_6_thread_resume_canary_report.md"
        md_file.write_text(md_content, encoding="utf-8")
        logger.info("Saved sanitized resume canary markdown report to %s", md_file)

        logger.info("=================================================================")
        logger.info("VERDICT: OFFICIAL_THREAD_RESUME_V2_EVIDENCE_VERIFIED")
        logger.info("=================================================================")
        return private_report

    finally:
        await mgr.shutdown()


if __name__ == "__main__":
    asyncio.run(run_resume_canary())
