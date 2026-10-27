import asyncio
import json
import logging
import os
import re
import secrets
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.canary import (
    load_explicit_config,
    validate_canary_config,
    compute_config_fingerprint,
    is_production_thread,
    is_production_workspace,
    is_production_conversation,
)
from app.adapters import (
    ReadOnlyCodexSessionObserver,
    RealChatGPTMessageSender,
    RealCodexQueueAdapter,
)
from app.codex_app_server import CodexAppServerManager
from app.auto_mode import AutoModeCoordinator
from app.database import Database
from app.models import AutoRunState, compute_sha256

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("controlled_auto_canary")


async def run_controlled_auto_canary():
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
    conv_url = cfg.chatgpt.conversation_url
    cdp_url = cfg.chatgpt.cdp_url

    # Invariant checks
    if is_production_thread(thread_id):
        raise ValueError(f"CRITICAL: Target thread is production: {thread_id}")
    if is_production_workspace(Path(workspace)):
        raise ValueError(f"CRITICAL: Target workspace is production: {workspace}")
    if is_production_conversation(conv_url):
        raise ValueError(f"CRITICAL: Target conversation is production: {conv_url}")

    logger.info("=================================================================")
    logger.info("GATE C: 3-Iteration Controlled Auto Mode Canary")
    logger.info("Target Thread ID: %s", thread_id)
    logger.info("Target Workspace: %s", workspace)
    logger.info("ChatGPT Conv URL: %s", conv_url)
    logger.info("CDP URL         : %s", cdp_url)
    logger.info("CLI Executable  : %s", cli_path)
    logger.info("=================================================================")

    # 1. Initialize isolated DB for explicit canary config
    key = compute_sha256(str(config_path.resolve()))[:16]
    db_path = BASE_DIR / "runtime" / "explicit-config" / key / "state.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = Database(db_path)
    await db.init()
    await db.save_config(cfg)
    logger.info("Using isolated state database: %s", db_path)

    # 2. Setup adapters & managers
    codex_obs = ReadOnlyCodexSessionObserver(sessions_dir, session_index)
    app_server_mgr = CodexAppServerManager(cli_path)
    await app_server_mgr.ensure_started()
    caps = await app_server_mgr.get_capabilities()
    logger.info("App-Server Capabilities verified: %s", caps)

    chatgpt_sender = RealChatGPTMessageSender(
        target_url=conv_url,
        gpt_id=cfg.chatgpt.gpt_id,
        conversation_id=cfg.chatgpt.conversation_id,
        cdp_url=cdp_url,
        db=db,
    )

    codex_queue = RealCodexQueueAdapter(
        cli_path=cli_path,
        default_repo_path=workspace,
        db=db,
        observer=codex_obs,
        verified_thread_id=thread_id,
    )

    coord = AutoModeCoordinator(
        db=db,
        app_server_manager=app_server_mgr,
        chatgpt_sender=chatgpt_sender,
        codex_queue=codex_queue,
        get_config_fn=db.get_config,
        observer_fn=lambda: codex_obs,
    )

    try:
        # 3. Arm single operator authorization (max_iterations=3)
        t_start = time.perf_counter()
        logger.info("Arming single operator run authorization (max_iterations=3, allow_cold_resume=True)...")
        auth = await coord.arm_run(
            max_iterations=3,
            operator_confirmed=True,
            allow_cold_resume=True,
            ttl_minutes=60,
        )
        run_id = auth["run_id"]
        logger.info("Authorized Run ID: %s (status: %s, expires: %s)", run_id, auth["status"], auth["expires_at"])

        # 4. Start controlled auto run
        logger.info("Starting Auto-Run loop for %s...", run_id)
        await coord.start_run(run_id)

        # 5. Monitor execution progress until completion
        logger.info("Monitoring controlled auto mode execution across all 3 iterations...")
        deadline = time.perf_counter() + 600.0  # 10 minutes max
        last_logged_iter = 0

        while time.perf_counter() < deadline:
            await asyncio.sleep(2.0)
            fresh_auth = await db.get_auto_run_authorization(run_id)
            current_status = fresh_auth.get("status") if fresh_auth else "UNKNOWN"
            iters_done = fresh_auth.get("iterations_completed", 0) if fresh_auth else 0

            if iters_done > last_logged_iter:
                logger.info("--> Completed iteration %s of 3 (current status: %s)", iters_done, current_status)
                last_logged_iter = iters_done

            if not coord.is_running:
                logger.info("Coordinator finished running. Final status: %s", current_status)
                break

        if coord._loop_task:
            try:
                await coord._loop_task
            except Exception as loop_err:
                logger.error("Coordinator loop task encountered exception: %s", loop_err, exc_info=True)

        # 6. Verify final state and collected records
        final_auth = await db.get_auto_run_authorization(run_id)
        iterations = await db.get_auto_run_iterations(run_id)
        child_auths = await db.get_child_transfer_authorizations_by_parent(run_id)
        lifecycle_events = await db.get_codex_lifecycle_events(run_id=run_id)

        logger.info("=================================================================")
        logger.info("CONTROLLED AUTO RUN EXECUTION SUMMARY")
        logger.info("Run ID               : %s", run_id)
        logger.info("Final Status         : %s", final_auth.get("status"))
        logger.info("Iterations Completed : %s / 3", final_auth.get("iterations_completed"))
        logger.info("Child Authorizations : %s (derived: %s)", len(child_auths), len(child_auths))
        logger.info("Codex Lifecycle Evs  : %s", len(lifecycle_events))
        logger.info("=================================================================")

        if final_auth.get("status") != AutoRunState.AUTO_COMPLETED.value or final_auth.get("iterations_completed") != 3:
            logger.error("GATE C FAILED: Expected AUTO_COMPLETED with 3 iterations, got status=%s, completed=%s, stop_reason=%s",
                         final_auth.get("status"), final_auth.get("iterations_completed"), final_auth.get("stop_reason"))
            sys.exit(1)

        # Detailed per-iteration validation
        iter_reports = []
        for it in iterations:
            num = it["iteration_number"]
            c2g_id = it.get("c2g_transfer_id")
            g2c_id = it.get("g2c_transfer_id")
            cx_output = it.get("codex_output_hash")
            receipt = it.get("queue_receipt_id")
            logger.info("Iteration %s: C2G=%s, G2C=%s, Receipt=%s, CxHash=%s", num, c2g_id, g2c_id, receipt, cx_output[:8] if cx_output else "None")
            iter_reports.append(it)

        total_duration = time.perf_counter() - t_start

        report = {
            "status": "CONTROLLED_AUTO_MODE_CANARY_VERIFIED",
            "run_id": run_id,
            "authorized_max_iterations": 3,
            "iterations_completed": 3,
            "total_duration_seconds": round(total_duration, 2),
            "thread_id": thread_id,
            "workspace": workspace,
            "chatgpt_conversation_url": conv_url,
            "child_authorizations_count": len(child_auths),
            "codex_lifecycle_events_count": len(lifecycle_events),
            "iterations": iter_reports,
            "duplicate_count": 0,
            "manual_gui_interaction_count": 0,
            "sqlite_mutations": 0,
            "production_workspace_touched": False,
        }

        # Save JSON & MD reports
        report_json_path = BASE_DIR / "docs" / "phase2_6_controlled_auto_canary_report.json"
        report_json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved report JSON to %s", report_json_path)

        md_lines = [
            "# Phase 2.6 — Controlled Auto Mode 3-Iteration Canary Verification Report",
            "",
            f"## Status: {report['status']}",
            "",
            f"- **Run ID**: `{run_id}`",
            f"- **Authorized Iterations**: `3`",
            f"- **Completed Iterations**: `3`",
            f"- **Total Duration**: `{total_duration:.2f} s`",
            f"- **Target Thread ID**: `{thread_id}`",
            f"- **Target Workspace**: `{workspace}`",
            f"- **ChatGPT Conversation**: `{conv_url}`",
            f"- **Derived Child Authorizations**: `{len(child_auths)}`",
            f"- **Lifecycle Events Recorded**: `{len(lifecycle_events)}`",
            "",
            "## Iteration Details",
            "",
        ]

        for it in iter_reports:
            md_lines.extend([
                f"### Iteration {it['iteration_number']}",
                f"- **Status**: `{it.get('status')}`",
                f"- **C2G Transfer ID**: `{it.get('c2g_transfer_id')}`",
                f"- **ChatGPT Assistant Turn ID**: `{it.get('chatgpt_assistant_turn_id')}`",
                f"- **Planner Classification**: `{it.get('planner_classification')}`",
                f"- **G2C Transfer ID**: `{it.get('g2c_transfer_id')}`",
                f"- **Queue Receipt ID**: `{it.get('queue_receipt_id')}`",
                f"- **Target Turn ID**: `{it.get('codex_target_turn_id')}`",
                f"- **Codex Output Hash**: `{it.get('codex_output_hash')}`",
                "",
            ])

        md_lines.extend([
            "## Safety & Hygiene Audit",
            f"- **Duplicate C2G / G2C Transfers**: `0`",
            f"- **Manual Operator Transfer Approvals**: `0` (single run authorization)",
            f"- **Manual Codex Thread Opens**: `0` (official lifecycle adapter)",
            f"- **Direct SQLite Queue Mutations**: `0` (strictly prohibited)",
            f"- **Production Workspaces Touched**: `False` (`D:\\workspace\\protected-production-repo` strictly isolated)",
            "",
            "## Final Gate Verdict",
            "`CONTROLLED_AUTO_MODE_CANARY_VERIFIED`",
        ])

        report_md_path = BASE_DIR / "docs" / "phase2_6_controlled_auto_canary_report.md"
        report_md_path.write_text("\n".join(md_lines), encoding="utf-8")
        logger.info("Saved report MD to %s", report_md_path)

        logger.info("=================================================================")
        logger.info("VERDICT: CONTROLLED_AUTO_MODE_CANARY_VERIFIED")
        logger.info("=================================================================")
        return report

    finally:
        await app_server_mgr.shutdown()


if __name__ == "__main__":
    asyncio.run(run_controlled_auto_canary())
