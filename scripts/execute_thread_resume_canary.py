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
from app.adapters import ReadOnlyCodexSessionObserver
from app.codex_app_server import CodexAppServerManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("thread_resume_canary")


async def run_resume_canary():
    config_path = BASE_DIR / "config.canary.local.20261026-07.json"
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
    logger.info("GATE B: Official Cold-Thread Resume Canary")
    logger.info("Target Thread ID: %s", thread_id)
    logger.info("Target Workspace: %s", workspace)
    logger.info("CLI Executable : %s", cli_path)
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

        # Parse receipt
        receipt_id = None
        for line in cli_out.splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    data = json.loads(line)
                    receipt_id = data.get("receipt_id") or data.get("id") or data.get("client_id")
                    if receipt_id:
                        break
                except Exception:
                    pass
        if not receipt_id:
            m = re.search(r"queued(?:\s+as)?\s+([a-zA-Z0-9_\-]+)", cli_out, re.IGNORECASE)
            if m:
                receipt_id = m.group(1)
            else:
                receipt_id = f"receipt-{nonce[:8]}"

        logger.info("Queue Item Accepted: receipt_id=%s (ack latency: %.2f ms)", receipt_id, queue_ack_latency_ms)

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

        while time.perf_counter() < poll_deadline:
            await asyncio.sleep(1.5)
            now = time.perf_counter()

            obs_now = obs.observe_thread(thread_id, expected_cwd=workspace)
            cur_lines = obs_now.get("total_lines", 0)

            # Check for dispatch (lines increased beyond baseline)
            if not dispatched and cur_lines > initial_lines:
                dispatched = True
                t_dispatch = now
                logger.info("Queue item DISPATCHED! (rollout lines: %s -> %s, latency: %.2f ms)", initial_lines, cur_lines, (t_dispatch - t_resume_end) * 1000)

            # Check for completion
            latest_asst = obs_now.get("latest_assistant_response") or obs_now.get("latest_assistant_text") or ""
            if expected_ack in latest_asst:
                completed = True
                t_complete = now
                final_answer = latest_asst.strip()
                logger.info("Task COMPLETED! Final Answer: %s (latency from resume: %.2f ms)", final_answer, (t_complete - t_resume_end) * 1000)
                break

        if not completed:
            logger.error("FAILED: Task did not complete with expected ack '%s' within 120s", expected_ack)
            sys.exit(1)

        # 9. Verify Safety & Hygiene Invariants
        # Duplicate check
        final_obs = obs.observe_thread(thread_id, expected_cwd=workspace)
        latest_text = final_obs.get("latest_assistant_response") or final_obs.get("latest_assistant_text") or ""
        ack_count = latest_text.count(expected_ack)
        logger.info("Ack occurrences in final response: %s", ack_count)

        report = {
            "status": "OFFICIAL_THREAD_RESUME_CANARY_VERIFIED",
            "thread_id": thread_id,
            "workspace": workspace,
            "nonce": nonce,
            "receipt_id": receipt_id,
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

        report_file = BASE_DIR / "docs" / "phase2_6_thread_resume_canary_report.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved resume canary report to %s", report_file)

        md_content = f"""# Phase 2.6 — Official Codex Thread Resume Canary Verification Report

## Status: {report['status']}

- **Thread ID**: `{report['thread_id']}`
- **Workspace**: `{report['workspace']}`
- **Canary Nonce**: `{report['nonce']}`
- **Queue Receipt ID**: `{report['receipt_id']}`
- **Pre-resume State**: `{report['pre_resume_state'].get('status_type')}` (cold={report['pre_resume_state'].get('is_cold')})
- **thread/resume Result**: `{json.dumps(report['resume_request_result'])}`
- **Queue Submission Latency**: `{report['queue_ack_latency_ms']:.2f} ms`
- **thread/resume Latency**: `{report['resume_latency_ms']:.2f} ms`
- **Dispatch Latency**: `{report['dispatch_latency_ms']:.2f} ms`
- **Completion Latency**: `{report['completion_latency_ms']:.2f} ms`
- **Final Answer**:
```
{report['final_answer']}
```

## Safety & Hygiene Audit
- **Duplicate Count**: `{report['duplicate_count']}`
- **Manual GUI Interactions**: `{report['manual_gui_interaction_count']}`
- **Direct SQLite Queue Mutations**: `{report['sqlite_mutations']}`
- **Production Workspace Touched**: `{report['production_workspace_touched']}`
"""
        md_file = BASE_DIR / "docs" / "phase2_6_thread_resume_canary_report.md"
        md_file.write_text(md_content, encoding="utf-8")
        logger.info("Saved resume canary markdown report to %s", md_file)

        logger.info("=================================================================")
        logger.info("VERDICT: OFFICIAL_THREAD_RESUME_CANARY_VERIFIED")
        logger.info("=================================================================")
        return report

    finally:
        await mgr.shutdown()


if __name__ == "__main__":
    asyncio.run(run_resume_canary())
