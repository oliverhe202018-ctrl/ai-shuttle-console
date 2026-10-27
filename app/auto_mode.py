import asyncio
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.adapters import (
    IChatGPTMessageSender,
    ICodexQueueAdapter,
    StandardPromptExtractor,
)
from app.canary import (
    auto_canary_prompt,
    auto_canary_request,
    canary_prompt,
    canary_request,
    compute_config_fingerprint,
    is_production_target,
    is_production_thread,
    is_production_workspace,
    same_path,
)
from app.codex_app_server import CodexAppServerManager, CodexResumeIdentityMismatchError
from app.database import Database
from app.models import (
    AppConfig,
    AutoRunAuthorization,
    AutoRunIteration,
    AutoRunState,
    ChildTransferAuthorization,
    CodexLifecycleEvent,
    PlannerClassification,
    ShuttleStage,
    TransferAuthorization,
    TransferDirection,
    TransferRecord,
    TransferStatus,
    compute_sha256,
)

logger = logging.getLogger("shuttle.auto_mode")


def classify_planner_response(
    response_parts: Dict[str, Any],
    expected_prompt: Optional[str] = None,
    stop_marker: str = "SHUTTLE_STOP"
) -> Tuple[PlannerClassification, Optional[str], Optional[str], Optional[str]]:
    """Strict planner response classification (Section 16 & 17).

    Returns: (classification, prompt, provenance, stop_reason)
    """
    if not response_parts:
        return PlannerClassification.PLANNER_RESPONSE_ERROR, None, None, "Empty response parts"

    raw_text = response_parts.get("raw_text") or ""
    canvas = response_parts.get("canvas_text") or ""
    code_blocks = response_parts.get("code_blocks") or []

    # Strip code blocks before checking stop_marker
    text_without_blocks = raw_text
    for block in code_blocks:
        text_without_blocks = text_without_blocks.replace(block, "")
    if stop_marker in text_without_blocks:
        return PlannerClassification.PLANNER_STOP_REQUESTED, None, None, f"Terminal marker '{stop_marker}' found in planner response"

    # Check for ambiguity: multiple competing code blocks that look like prompts
    valid_blocks = [b.strip() for b in code_blocks if len(b.strip()) >= 20]
    if len(valid_blocks) > 1:
        return PlannerClassification.PLANNER_RESPONSE_AMBIGUOUS, None, None, f"Multiple competing prompt blocks ({len(valid_blocks)}) found in planner response"

    extractor = StandardPromptExtractor(expected_auto_prompt=expected_prompt)
    extracted_prompt, provenance = extractor.extract_with_provenance(response_parts)

    if extracted_prompt:
        return PlannerClassification.NEXT_CODEX_PROMPT_FOUND, extracted_prompt, provenance, None

    if not raw_text.strip() and not canvas.strip() and not code_blocks:
        return PlannerClassification.PLANNER_RESPONSE_ERROR, None, None, "No text or blocks in response"

    return PlannerClassification.PLANNER_NO_ACTIONABLE_PROMPT, None, None, "No actionable prompt found in response"

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple, Callable, List

class IIterationPayloadStrategy(ABC):
    @abstractmethod
    def build_c2g_payload(self, run_id: str, iteration: int, nonce: str, max_iterations: int, prior_codex_output: Optional[str] = None) -> str:
        pass

    @abstractmethod
    def get_expected_codex_prompt(self, run_id: str, iteration: int, nonce: str) -> Optional[str]:
        pass

    @abstractmethod
    def get_expected_codex_output(self, run_id: str, iteration: int, nonce: str) -> Optional[str]:
        pass

class CanaryIterationPayloadStrategy(IIterationPayloadStrategy):
    def build_c2g_payload(self, run_id: str, iteration: int, nonce: str, max_iterations: int, prior_codex_output: Optional[str] = None) -> str:
        return auto_canary_request(run_id=run_id, iteration=iteration, nonce=nonce, max_iterations=max_iterations, prior_codex_output=prior_codex_output)

    def get_expected_codex_prompt(self, run_id: str, iteration: int, nonce: str) -> Optional[str]:
        return auto_canary_prompt(run_id, iteration, nonce)

    def get_expected_codex_output(self, run_id: str, iteration: int, nonce: str) -> Optional[str]:
        return f"CANARY-AUTO-OK:{run_id}:{iteration}:{nonce}"

class ProductionShuttlePayloadStrategy(IIterationPayloadStrategy):
    def build_c2g_payload(self, run_id: str, iteration: int, nonce: str, max_iterations: int, prior_codex_output: Optional[str] = None) -> str:
        return prior_codex_output or ""

    def get_expected_codex_prompt(self, run_id: str, iteration: int, nonce: str) -> Optional[str]:
        return None

    def get_expected_codex_output(self, run_id: str, iteration: int, nonce: str) -> Optional[str]:
        return None


class AutoModeCoordinator:
    """Orchestrates Controlled Auto Mode bounded multi-round shuttle."""

    def __init__(
        self,
        db: Database,
        app_server_manager: CodexAppServerManager,
        chatgpt_sender: Optional[IChatGPTMessageSender] = None,
        codex_queue: Optional[ICodexQueueAdapter] = None,
        get_config_fn: Optional[Callable[[], Any]] = None,
        observer_fn: Optional[Callable[[], Any]] = None,
        payload_strategy: Optional[IIterationPayloadStrategy] = None,
    ):
        self.db = db
        self.manager = app_server_manager
        self.chatgpt_sender = chatgpt_sender
        self.codex_queue = codex_queue
        self.get_config_fn = get_config_fn
        self.observer_fn = observer_fn
        self.payload_strategy = payload_strategy

        self._active_run_id: Optional[str] = None
        self._loop_task: Optional[asyncio.Task] = None
        self._pause_requested: bool = False
        self._stop_requested: bool = False
        self._emergency_stop_requested: bool = False
        self._stop_reason: Optional[str] = None
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._loop_task is not None and not self._loop_task.done()

    @property
    def active_run_id(self) -> Optional[str]:
        return self._active_run_id

    async def arm_run(
        self,
        max_iterations: int = 3,
        operator_confirmed: bool = False,
        allow_cold_resume: bool = True,
        ttl_minutes: int = 60,
        experimental_queue_start_enabled: bool = False,
    ) -> Dict[str, Any]:
        """Authorize a new Controlled Auto-Run."""
        if not operator_confirmed:
            raise ValueError("Operator confirmation is required to arm an Auto-Run")

        if max_iterations < 1 or max_iterations > 10:
            raise ValueError(f"max_iterations must be between 1 and 10, got {max_iterations}")

        # Invariant: Concurrency guard (Section 34)
        active_auth = await self.db.get_active_auto_run_authorization()
        if active_auth and active_auth["status"] in (
            AutoRunState.AUTO_ARMED.value,
            AutoRunState.AUTO_RUNNING.value,
            AutoRunState.AUTO_WAITING_CODEX.value,
            AutoRunState.AUTO_WAITING_CHATGPT.value,
            AutoRunState.AUTO_RESUMING_CODEX_THREAD.value,
            AutoRunState.AUTO_HUMAN_ACTION_REQUIRED.value,
            AutoRunState.AUTO_PAUSED.value,
        ):
            raise ValueError(f"Another auto-run ({active_auth['run_id']}) is already active in status {active_auth['status']}")

        cfg: AppConfig = await self.get_config_fn() if self.get_config_fn else await self.db.get_config()
        if not cfg:
            raise ValueError("Configuration not found")

        # Validate targets
        if cfg.canary_mode:
            if is_production_target(
                thread_id=cfg.codex.thread_id,
                conv_id_or_url=cfg.chatgpt.conversation_url,
                repo_path=cfg.codex.repo_path
            ):
                raise ValueError("Production targets are strictly forbidden in canary auto mode")

        run_id = f"auto-{secrets.token_hex(8)}"
        created_at = datetime.utcnow()
        expires_at = created_at + timedelta(minutes=ttl_minutes)

        auth = AutoRunAuthorization(
            run_id=run_id,
            created_at=created_at,
            expires_at=expires_at,
            status=AutoRunState.AUTO_ARMED,
            max_iterations=max_iterations,
            iterations_completed=0,
            chatgpt_conversation_url=cfg.chatgpt.conversation_url,
            chatgpt_conversation_id=cfg.chatgpt.conversation_id or "default",
            codex_thread_id=cfg.codex.thread_id,
            codex_workspace=str(Path(cfg.codex.repo_path).resolve()),
            config_fingerprint=compute_config_fingerprint(cfg),
            canary_mode=cfg.canary_mode,
            operator_confirmed=operator_confirmed,
            allow_cold_resume=allow_cold_resume,
            experimental_queue_start_enabled=experimental_queue_start_enabled,
        )

        saved = await self.db.save_auto_run_authorization(auth)
        self._active_run_id = run_id
        self._pause_requested = False
        self._stop_requested = False
        self._emergency_stop_requested = False
        self._stop_reason = None

        logger.info("Auto-Run armed successfully: %s (max_iterations=%s)", run_id, max_iterations)
        return saved

    async def start_run(self, run_id: str) -> None:
        """Launch the background orchestration loop for an armed run."""
        async with self._lock:
            if self.is_running:
                raise ValueError(f"Auto-Run loop is already running for {self._active_run_id}")

            auth = await self.db.get_auto_run_authorization(run_id)
            if not auth:
                raise ValueError(f"Auto-Run {run_id} not found")

            if auth["status"] not in (AutoRunState.AUTO_ARMED.value, AutoRunState.AUTO_PAUSED.value):
                raise ValueError(f"Cannot start run in status {auth['status']}")

            self._active_run_id = run_id
            self._pause_requested = False
            self._stop_requested = False
            self._emergency_stop_requested = False
            self._stop_reason = None

            await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_RUNNING.value, started_at=datetime.utcnow().isoformat())
            self._loop_task = asyncio.create_task(self._run_loop(run_id))

    async def request_pause(self, run_id: str, reason: str = "Operator requested pause") -> None:
        if self._active_run_id != run_id:
            raise ValueError(f"Run {run_id} is not the active run")
        self._pause_requested = True
        self._stop_reason = reason
        logger.info("Auto-Run pause requested: %s", reason)

    async def emergency_stop(self, run_id: str, reason: str = "Operator emergency stop") -> None:
        if self._active_run_id != run_id:
            raise ValueError(f"Run {run_id} is not the active run")
        self._emergency_stop_requested = True
        self._stop_requested = True
        self._stop_reason = reason
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
        await self.db.update_auto_run_status(
            run_id,
            AutoRunState.AUTO_STOPPED.value,
            stop_reason=reason,
            completed_at=datetime.utcnow().isoformat()
        )
        logger.warning("Auto-Run emergency stop executed: %s", reason)

    async def abort_run(self, run_id: str, reason: str = "Operator aborted run") -> None:
        if self._active_run_id == run_id and self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
        await self.db.update_auto_run_status(
            run_id,
            AutoRunState.AUTO_STOPPED.value,
            stop_reason=reason,
            completed_at=datetime.utcnow().isoformat()
        )
        self._active_run_id = None

    async def resume_after_human_action(self, run_id: str) -> None:
        auth = await self.db.get_auto_run_authorization(run_id)
        if not auth or auth["status"] != AutoRunState.AUTO_HUMAN_ACTION_REQUIRED.value:
            raise ValueError("Run is not currently in AUTO_HUMAN_ACTION_REQUIRED state")
        await self.start_run(run_id)

    async def derive_child_authorization(
        self,
        auth: Dict[str, Any],
        iteration_number: int,
        transfer_id: str,
        direction: str,
        destination_identifier: str,
        content_hash: str,
        config_fingerprint: str
    ) -> ChildTransferAuthorization:
        """Derive a single-use child transfer authorization bound to parent run (Section 13)."""
        child_auth_id = f"child-auth-{secrets.token_hex(8)}"
        confirmation_token = secrets.token_hex(16)

        child_auth = ChildTransferAuthorization(
            child_auth_id=child_auth_id,
            parent_run_id=auth["run_id"],
            iteration_number=iteration_number,
            transfer_id=transfer_id,
            direction=direction,
            destination_identifier=destination_identifier,
            content_hash=content_hash,
            config_fingerprint=config_fingerprint,
            confirmation_token=confirmation_token,
            consumed=False,
        )
        await self.db.save_child_transfer_authorization(child_auth)

        expires_at = datetime.utcnow() + timedelta(minutes=30)
        parent_ta = TransferAuthorization(
            transfer_id=transfer_id,
            confirmation_token=confirmation_token,
            destination_identifier=destination_identifier,
            expected_content_hash=content_hash,
            expires_at=expires_at,
            config_fingerprint=config_fingerprint,
        )
        await self.db.create_transfer_authorization(parent_ta)

        return child_auth

    async def _run_loop(self, run_id: str) -> None:
        """Main bounded Controlled Auto Mode loop (Section 33)."""
        try:
            auth = await self.db.get_auto_run_authorization(run_id)
            if not auth:
                return

            max_iterations = auth["max_iterations"]
            start_iter = auth.get("iterations_completed", 0) + 1

            for iteration in range(start_iter, max_iterations + 1):
                fresh_auth = await self.db.get_auto_run_authorization(run_id)
                if fresh_auth:
                    auth = fresh_auth
                now = datetime.utcnow()
                expires_at = datetime.fromisoformat(auth["expires_at"]) if isinstance(auth["expires_at"], str) else auth["expires_at"]
                if now >= expires_at:
                    await self._stop_run(run_id, AutoRunState.AUTO_STOPPED, "AUTO_RUN_AUTHORIZATION_EXPIRED", iteration - 1)
                    return

                if self._emergency_stop_requested:
                    await self._stop_run(run_id, AutoRunState.AUTO_STOPPED, self._stop_reason or "Emergency Stop", iteration - 1)
                    return

                if self._pause_requested:
                    await self._pause_run(run_id, self._stop_reason or "Pause requested", iteration - 1)
                    return

                iter_success, stop_state, stop_reason = await self._execute_iteration(auth, iteration)

                if not iter_success:
                    if stop_state == AutoRunState.AUTO_HUMAN_ACTION_REQUIRED:
                        await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_HUMAN_ACTION_REQUIRED.value, stop_reason=stop_reason)
                        return
                    else:
                        await self._stop_run(run_id, stop_state or AutoRunState.AUTO_FAILED, stop_reason or "Iteration failed", iteration - 1)
                        return

                await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_RUNNING.value, iterations_completed=iteration)
                auth["iterations_completed"] = iteration

            await self.db.update_auto_run_status(
                run_id,
                AutoRunState.AUTO_COMPLETED.value,
                stop_reason="All authorized iterations completed",
                iterations_completed=max_iterations,
                completed_at=datetime.utcnow().isoformat()
            )
            logger.info("Auto-Run %s completed all %s iterations!", run_id, max_iterations)

        except asyncio.CancelledError:
            logger.info("Auto-Run task cancelled for %s", run_id)
        except Exception as exc:
            logger.error("Auto-Run exception for %s: %s", run_id, exc, exc_info=True)
            await self._stop_run(run_id, AutoRunState.AUTO_FAILED, f"Unexpected error: {exc}", auth.get("iterations_completed", 0) if auth else 0)

    async def _pause_run(self, run_id: str, reason: str, completed_iters: int) -> None:
        await self.db.update_auto_run_status(
            run_id,
            AutoRunState.AUTO_PAUSED.value,
            stop_reason=reason,
            iterations_completed=completed_iters
        )
        logger.info("Auto-Run %s paused at safe checkpoint: %s", run_id, reason)

    async def _stop_run(self, run_id: str, state: AutoRunState, reason: str, completed_iters: int) -> None:
        await self.db.update_auto_run_status(
            run_id,
            state.value,
            stop_reason=reason,
            error_details=reason if state == AutoRunState.AUTO_FAILED else None,
            iterations_completed=completed_iters,
            completed_at=datetime.utcnow().isoformat()
        )
        logger.warning("Auto-Run %s entered state %s: %s", run_id, state.value, reason)

    async def check_restart_recovery(self) -> Optional[Dict[str, Any]]:
        """Recover active auto-run state after service restart (Section 35)."""
        active_auth = await self.db.get_active_auto_run_authorization()
        if not active_auth:
            return None

        status = active_auth.get("status")
        run_id = active_auth.get("run_id")

        if status in (
            AutoRunState.AUTO_RUNNING.value,
            AutoRunState.AUTO_WAITING_CODEX.value,
            AutoRunState.AUTO_WAITING_CHATGPT.value,
            AutoRunState.AUTO_RESUMING_CODEX_THREAD.value,
        ):
            logger.warning("Found unfinalized Auto-Run %s in status %s on startup. Transitioning to AUTO_PAUSED_RECOVERY_REQUIRED.", run_id, status)
            updated = await self.db.update_auto_run_status(
                run_id,
                AutoRunState.AUTO_PAUSED_RECOVERY_REQUIRED.value,
                stop_reason="Service restarted during active iteration; recovery required"
            )
            return updated

        return active_auth

    async def _execute_iteration(
        self,
        auth: Dict[str, Any],
        iteration: int
    ) -> Tuple[bool, Optional[AutoRunState], Optional[str]]:
        run_id = auth["run_id"]
        max_iterations = auth["max_iterations"]
        cfg: AppConfig = await self.get_config_fn() if self.get_config_fn else await self.db.get_config()
        current_fp = compute_config_fingerprint(cfg)

        if auth["config_fingerprint"] != current_fp:
            return False, AutoRunState.AUTO_FAILED, "CONFIG_FINGERPRINT_MISMATCH"

        t_iter_start = time.perf_counter()
        metrics: Dict[str, float] = {}

        nonce = secrets.token_hex(16)
        strategy = self.payload_strategy
        if not strategy:
            is_canary = auth.get("canary_mode", True)
            strategy = CanaryIterationPayloadStrategy() if is_canary else ProductionShuttlePayloadStrategy()

        expected_auto_prompt = strategy.get_expected_codex_prompt(run_id, iteration, nonce)
        expected_canary_output = strategy.get_expected_codex_output(run_id, iteration, nonce)

        await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_RUNNING.value)
        obs = self.observer_fn() if self.observer_fn else None
        prior_output = None
        baseline_lines = 0
        if obs:
            thread_obs = obs.observe_thread(cfg.codex.thread_id, expected_cwd=cfg.codex.repo_path)
            prior_output = thread_obs.get("latest_assistant_text") or thread_obs.get("latest_assistant_response")
            baseline_lines = thread_obs.get("total_lines", 0)

        t_c2g_start = time.perf_counter()
        c2g_transfer_id = f"c2g-{run_id}-iter{iteration}"
        c2g_text = strategy.build_c2g_payload(
            run_id=run_id,
            iteration=iteration,
            nonce=nonce,
            max_iterations=max_iterations,
            prior_codex_output=prior_output,
        )
        c2g_hash = compute_sha256(c2g_text)

        c2g_child_auth = await self.derive_child_authorization(
            auth=auth,
            iteration_number=iteration,
            transfer_id=c2g_transfer_id,
            direction=TransferDirection.CODEX_TO_CHATGPT.value,
            destination_identifier=cfg.chatgpt.conversation_url,
            content_hash=c2g_hash,
            config_fingerprint=current_fp,
        )

        is_canary_strategy = isinstance(strategy, CanaryIterationPayloadStrategy)
        c2g_record = TransferRecord(
            transfer_id=c2g_transfer_id,
            direction=TransferDirection.CODEX_TO_CHATGPT.value,
            source_identifier=cfg.codex.thread_id,
            destination_identifier=cfg.chatgpt.conversation_url,
            content_hash=c2g_hash,
            raw_content_hash=c2g_hash,
            semantic_content_hash=c2g_hash,
            payload_preview=c2g_text[:120],
            message_text=c2g_text,
            char_count=len(c2g_text),
            source_type="canary" if is_canary_strategy else "production",
            source_message_id=f"auto-iter-{iteration}",
            destination_type="chatgpt",
            provenance_policy=f"auto_canary:{run_id}:{iteration}:{nonce}" if is_canary_strategy else f"production_shuttle:{run_id}:{iteration}",
            config_fingerprint=current_fp,
            confirmation_token=c2g_child_auth.confirmation_token,
            status=TransferStatus.PREPARED.value,
        )
        await self.db.save_transfer_record(c2g_record)
        metrics["c2g_prep_latency_ms"] = (time.perf_counter() - t_c2g_start) * 1000

        t_send_start = time.perf_counter()
        if not self.chatgpt_sender:
            return False, AutoRunState.AUTO_FAILED, "CHATGPT_SENDER_UNAVAILABLE"

        cg_send_res: Optional[Dict[str, Any]] = None
        try:
            import inspect
            sig = inspect.signature(self.chatgpt_sender.send_message)
            if "message" in sig.parameters:
                cg_send_res = await self.chatgpt_sender.send_message(
                    message=c2g_text,
                    confirmation_token=c2g_child_auth.confirmation_token,
                    transfer_id=c2g_transfer_id,
                    config_fingerprint=current_fp,
                    nonce=nonce,
                    delivery_timeout_sec=45.0,
                )
            else:
                cg_send_res = await self.chatgpt_sender.send_message(
                    conversation_url=cfg.chatgpt.conversation_url,
                    message_text=c2g_text,
                    transfer_id=c2g_transfer_id,
                    confirmation_token=c2g_child_auth.confirmation_token,
                    nonce=nonce,
                    delivery_timeout_sec=45.0,
                )
            await self.db.update_transfer_status(c2g_transfer_id, TransferStatus.DELIVERED.value)
        except Exception as s_err:
            logger.error("C2G send failed for iter %s: %s", iteration, s_err)
            return False, AutoRunState.AUTO_FAILED, f"C2G_SEND_FAILED: {s_err}"

        metrics["c2g_delivery_latency_ms"] = (time.perf_counter() - t_send_start) * 1000

        cg_delivery_ev = (cg_send_res.get("delivery_evidence") or {}) if isinstance(cg_send_res, dict) else {}
        cg_user_turn_id = cg_delivery_ev.get("user_turn_id")
        cg_user_msg_id = cg_delivery_ev.get("user_message_id")
        cg_user_raw_hash = cg_delivery_ev.get("raw_content_hash") or c2g_hash
        cg_user_sem_hash = cg_delivery_ev.get("semantic_content_hash")

        await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_WAITING_CHATGPT.value)
        t_cg_wait_start = time.perf_counter()
        cg_observed = False
        cg_obs_data = None
        cg_poll_deadline = time.perf_counter() + 120.0

        from app.adapters import RealChatGPTBrowserObserver
        while time.perf_counter() < cg_poll_deadline:
            if self._emergency_stop_requested:
                return False, AutoRunState.AUTO_STOPPED, "EMERGENCY_STOP_DURING_CHATGPT_WAIT"

            await asyncio.sleep(2.0)
            try:
                browser_obs = RealChatGPTBrowserObserver(
                    target_url=cfg.chatgpt.conversation_url,
                    cdp_url=cfg.chatgpt.cdp_url,
                    prompt_extractor=StandardPromptExtractor(expected_auto_prompt=expected_auto_prompt),
                )
                page = await browser_obs.read_live_page()
                obs_res = browser_obs.inspect_dom_payload(page)
                user = obs_res.get("latest_user_turn") or {}
                asst = obs_res.get("latest_assistant_turn") or {}
                if obs_res.get("is_completed") and not obs_res.get("is_streaming"):
                    matched = False
                    if expected_auto_prompt:
                        asst_text = asst.get("text", "") or ""
                        blocks = asst.get("code_blocks", []) or []
                        comb_text = asst_text + " " + " ".join(blocks)
                        if expected_auto_prompt in comb_text:
                            matched = True
                    else:
                        if asst.get("turn_index", -1) > user.get("turn_index", -1) and not obs_res.get("is_stale_assistant_reply"):
                            matched = True
                    if matched:
                        cg_observed = True
                        cg_obs_data = obs_res
                        break
            except Exception as obs_err:
                logger.debug("ChatGPT observe polling error: %s", obs_err)

        if not cg_observed or not cg_obs_data:
            return False, AutoRunState.AUTO_FAILED, "CHATGPT_RESPONSE_TIMED_OUT"

        metrics["chatgpt_response_latency_ms"] = (time.perf_counter() - t_cg_wait_start) * 1000

        if not cg_user_turn_id:
            latest_u = cg_obs_data.get("latest_user_turn") or {}
            cg_user_turn_id = latest_u.get("turn_id")
        if not cg_user_msg_id:
            latest_u = cg_obs_data.get("latest_user_turn") or {}
            cg_user_msg_id = latest_u.get("message_id")

        t_ext_start = time.perf_counter()
        asst_turn = cg_obs_data.get("latest_assistant_turn", {})
        cg_asst_turn_id = asst_turn.get("turn_id")
        cg_asst_msg_id = asst_turn.get("message_id")

        response_parts = {
            "raw_text": asst_turn.get("text", "") or cg_obs_data.get("raw_text", ""),
            "canvas_text": asst_turn.get("canvas_text", "") or cg_obs_data.get("canvas_text", ""),
            "code_blocks": asst_turn.get("code_blocks", []) or cg_obs_data.get("code_blocks", []),
        }
        cg_asst_resp_hash = compute_sha256(response_parts.get("raw_text", ""))

        classification, extracted_prompt, provenance, stop_reason = classify_planner_response(
            response_parts,
            expected_prompt=expected_auto_prompt,
            stop_marker="SHUTTLE_STOP"
        )
        metrics["prompt_extraction_latency_ms"] = (time.perf_counter() - t_ext_start) * 1000

        if classification == PlannerClassification.PLANNER_STOP_REQUESTED:
            logger.info("Planner explicitly requested stop: %s", stop_reason)
            return False, AutoRunState.AUTO_COMPLETED, stop_reason

        if classification == PlannerClassification.PLANNER_NO_ACTIONABLE_PROMPT:
            logger.info("Planner response has no actionable prompt: %s", stop_reason)
            return False, AutoRunState.AUTO_COMPLETED, stop_reason

        if classification != PlannerClassification.NEXT_CODEX_PROMPT_FOUND or not extracted_prompt:
            logger.error("Planner response classification failed: %s (%s)", classification, stop_reason)
            return False, AutoRunState.AUTO_FAILED, f"PLANNER_{classification.value}: {stop_reason}"

        t_g2c_start = time.perf_counter()
        g2c_transfer_id = f"g2c-{run_id}-iter{iteration}"
        g2c_hash = compute_sha256(extracted_prompt)

        g2c_child_auth = await self.derive_child_authorization(
            auth=auth,
            iteration_number=iteration,
            transfer_id=g2c_transfer_id,
            direction=TransferDirection.CHATGPT_TO_CODEX.value,
            destination_identifier=cfg.codex.thread_id,
            content_hash=g2c_hash,
            config_fingerprint=current_fp,
        )

        g2c_record = TransferRecord(
            transfer_id=g2c_transfer_id,
            direction=TransferDirection.CHATGPT_TO_CODEX.value,
            source_identifier=cfg.chatgpt.conversation_url,
            destination_identifier=cfg.codex.thread_id,
            content_hash=g2c_hash,
            raw_content_hash=g2c_hash,
            semantic_content_hash=g2c_hash,
            payload_preview=extracted_prompt[:120],
            message_text=extracted_prompt,
            char_count=len(extracted_prompt),
            source_type="chatgpt",
            source_message_id=cg_asst_msg_id or f"chatgpt-iter-{iteration}",
            destination_type="codex",
            provenance_policy=f"auto_canary:{run_id}:{iteration}:{nonce}" if is_canary_strategy else f"production_shuttle:{run_id}:{iteration}",
            config_fingerprint=current_fp,
            confirmation_token=g2c_child_auth.confirmation_token,
            status=TransferStatus.PREPARED.value,
        )
        await self.db.save_transfer_record(g2c_record)

        t_queue_submit = time.perf_counter()
        if not self.codex_queue:
            return False, AutoRunState.AUTO_FAILED, "CODEX_QUEUE_ADAPTER_UNAVAILABLE"

        queue_fn = getattr(self.codex_queue, "queue_message", None) or getattr(self.codex_queue, "submit_message", None)
        if not queue_fn:
            return False, AutoRunState.AUTO_FAILED, "CODEX_QUEUE_METHOD_UNAVAILABLE"

        queue_res = await queue_fn(
            thread_id=cfg.codex.thread_id,
            message=extracted_prompt,
            transfer_id=g2c_transfer_id,
            confirmation_token=g2c_child_auth.confirmation_token,
            repo_path=cfg.codex.repo_path,
            nonce=nonce,
            config_fingerprint=current_fp,
        )
        t_queue_ack = time.perf_counter()
        metrics["g2c_queue_ack_latency_ms"] = (t_queue_ack - t_queue_submit) * 1000

        receipt_id = queue_res.get("receipt_id")
        client_user_message_id = queue_res.get("client_user_message_id")
        queued_item_id = queue_res.get("queued_item_id")
        if not receipt_id or str(receipt_id).strip().lower() == "message":
            return False, AutoRunState.AUTO_FAILED, "CODEX_QUEUE_RECEIPT_MISSING"

        await self.db.update_transfer_status(g2c_transfer_id, TransferStatus.SENT.value)

        dispatched = False
        codex_user_turn_id = None
        codex_user_msg_id = None
        t_dispatch_start = time.perf_counter()
        natural_dispatch_deadline = t_dispatch_start + 10.0
        resume_attempted = False
        resume_required = False
        resume_result = None

        while time.perf_counter() < natural_dispatch_deadline:
            await asyncio.sleep(1.0)
            if obs:
                if hasattr(obs, "find_dispatched_turn_and_answer"):
                    d_info = obs.find_dispatched_turn_and_answer(
                        thread_id=cfg.codex.thread_id,
                        expected_cwd=cfg.codex.repo_path,
                        after_line=baseline_lines,
                        client_user_message_id=client_user_message_id,
                        nonce=nonce if expected_canary_output else None,
                        expected_content_hash=g2c_hash,
                    )
                    if d_info.get("dispatched"):
                        dispatched = True
                        codex_user_turn_id = d_info.get("user_turn_id")
                        codex_user_msg_id = d_info.get("user_message_id")
                        break
                else:
                    thread_obs = obs.observe_thread(cfg.codex.thread_id, expected_cwd=cfg.codex.repo_path)
                    cur_lines = thread_obs.get("total_lines", 0)
                    if cur_lines > baseline_lines or thread_obs.get("is_in_progress"):
                        dispatched = True
                        break
                    turns = thread_obs.get("turns", [])
                    for trn in turns:
                        if trn.get("client_user_message_id") == client_user_message_id or g2c_hash in (trn.get("content_hash") or ""):
                            dispatched = True
                            codex_user_turn_id = trn.get("turn_id")
                            codex_user_msg_id = trn.get("message_id")
                            break
            if dispatched:
                break

        if not dispatched and auth.get("allow_cold_resume", True):
            is_cold, status_type = await self.manager.is_thread_cold(cfg.codex.thread_id)
            if is_cold:
                resume_required = True
                resume_attempted = True
                await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_RESUMING_CODEX_THREAD.value)
                logger.info("Target thread %s is cold (status: %s). Invoking official thread/resume...", cfg.codex.thread_id, status_type)

                ev_id = f"ev-resume-{secrets.token_hex(6)}"
                await self.db.record_codex_lifecycle_event(
                    CodexLifecycleEvent(
                        event_id=ev_id,
                        run_id=run_id,
                        iteration_number=iteration,
                        thread_id=cfg.codex.thread_id,
                        event_type="thread_resume_requested",
                        pre_state=status_type,
                        details={"expected_workspace": cfg.codex.repo_path}
                    )
                )

                t_resume_start = time.perf_counter()
                try:
                    res_obj = await self.manager.resume_thread_safely(
                        thread_id=cfg.codex.thread_id,
                        expected_workspace=cfg.codex.repo_path
                    )
                    metrics["thread_resume_latency_ms"] = (time.perf_counter() - t_resume_start) * 1000
                    resume_result = "SUCCESS"
                    await self.db.record_codex_lifecycle_event(
                        CodexLifecycleEvent(
                            event_id=f"ev-res-ok-{secrets.token_hex(6)}",
                            run_id=run_id,
                            iteration_number=iteration,
                            thread_id=cfg.codex.thread_id,
                            event_type="thread_resume_succeeded",
                            post_state="loaded",
                            details=res_obj
                        )
                    )
                except CodexResumeIdentityMismatchError as id_err:
                    logger.error("Codex resume identity mismatch: %s", id_err)
                    return False, AutoRunState.AUTO_FAILED, f"CODEX_RESUME_IDENTITY_MISMATCH: {id_err}"
                except Exception as r_err:
                    logger.error("Codex resume failed: %s", r_err)
                    return False, AutoRunState.AUTO_FAILED, f"CODEX_RESUME_FAILED: {r_err}"

        t_wait_dispatch = time.perf_counter()
        dispatch_deadline = t_wait_dispatch + 30.0
        while not dispatched and time.perf_counter() < dispatch_deadline:
            await asyncio.sleep(1.0)
            if obs:
                if hasattr(obs, "find_dispatched_turn_and_answer"):
                    d_info = obs.find_dispatched_turn_and_answer(
                        thread_id=cfg.codex.thread_id,
                        expected_cwd=cfg.codex.repo_path,
                        after_line=baseline_lines,
                        client_user_message_id=client_user_message_id,
                        nonce=nonce if expected_canary_output else None,
                        expected_content_hash=g2c_hash,
                    )
                    if d_info.get("dispatched"):
                        dispatched = True
                        codex_user_turn_id = d_info.get("user_turn_id")
                        codex_user_msg_id = d_info.get("user_message_id")
                        break
                else:
                    thread_obs = obs.observe_thread(cfg.codex.thread_id, expected_cwd=cfg.codex.repo_path)
                    cur_lines = thread_obs.get("total_lines", 0)
                    if cur_lines > baseline_lines or thread_obs.get("is_in_progress"):
                        dispatched = True
                        break
                    turns = thread_obs.get("turns", [])
                    for trn in turns:
                        if trn.get("client_user_message_id") == client_user_message_id or g2c_hash in (trn.get("content_hash") or ""):
                            dispatched = True
                            codex_user_turn_id = trn.get("turn_id")
                            codex_user_msg_id = trn.get("message_id")
                            break

        t_dispatched = time.perf_counter()
        metrics["queue_to_dispatch_latency_ms"] = (t_dispatched - t_queue_ack) * 1000

        if not dispatched:
            logger.error("Queued submission did not dispatch after thread resume (Section 7 fail-closed)")
            return False, AutoRunState.AUTO_FAILED, "CODEX_RESUMED_QUEUE_NOT_DISPATCHED"

        await self.db.update_auto_run_status(run_id, AutoRunState.AUTO_WAITING_CODEX.value)
        completion_deadline = time.perf_counter() + 180.0
        completed = False
        cx_asst_msg = None
        codex_asst_msg_id = None
        codex_completed_at = None

        while time.perf_counter() < completion_deadline:
            actions = await self.manager.check_human_actions()
            if actions:
                logger.warning("Human action requested during Codex execution: %s", actions)
                return False, AutoRunState.AUTO_HUMAN_ACTION_REQUIRED, f"HUMAN_ACTION_REQUIRED: {actions[0].get('method')}"

            await asyncio.sleep(2.0)
            if obs:
                if hasattr(obs, "find_dispatched_turn_and_answer"):
                    d_info = obs.find_dispatched_turn_and_answer(
                        thread_id=cfg.codex.thread_id,
                        expected_cwd=cfg.codex.repo_path,
                        after_line=baseline_lines,
                        client_user_message_id=client_user_message_id,
                        nonce=nonce if expected_canary_output else None,
                        expected_content_hash=g2c_hash,
                    )
                    if d_info.get("dispatched") and not codex_user_turn_id:
                        codex_user_turn_id = d_info.get("user_turn_id")
                        codex_user_msg_id = d_info.get("user_message_id")

                    if d_info.get("completed"):
                        asst_txt = d_info.get("assistant_text") or ""
                        if expected_canary_output:
                            if expected_canary_output in asst_txt:
                                completed = True
                                cx_asst_msg = asst_txt
                                codex_asst_msg_id = d_info.get("assistant_message_id")
                                codex_completed_at = d_info.get("completed_at")
                                break
                        else:
                            completed = True
                            cx_asst_msg = asst_txt
                            codex_asst_msg_id = d_info.get("assistant_message_id")
                            codex_completed_at = d_info.get("completed_at")
                            break
                else:
                    thread_obs = obs.observe_thread(cfg.codex.thread_id, expected_cwd=cfg.codex.repo_path)
                    latest_asst = thread_obs.get("latest_assistant_text") or thread_obs.get("latest_assistant_response") or ""
                    if expected_canary_output:
                        if expected_canary_output in latest_asst:
                            completed = True
                            cx_asst_msg = latest_asst
                            codex_asst_msg_id = thread_obs.get("latest_assistant_message_id")
                            break
                    else:
                        completed_turn = thread_obs.get("completed_turn_id")
                        if completed_turn and codex_user_turn_id and completed_turn == codex_user_turn_id:
                            completed = True
                            cx_asst_msg = latest_asst
                            codex_asst_msg_id = thread_obs.get("latest_assistant_message_id")
                            break

        t_complete = time.perf_counter()
        metrics["codex_execution_latency_ms"] = (t_complete - t_dispatched) * 1000
        metrics["total_round_trip_latency_ms"] = (t_complete - t_iter_start) * 1000

        if not completed:
            return False, AutoRunState.AUTO_FAILED, "CODEX_COMPLETION_TIMED_OUT"

        await self.db.update_transfer_status(g2c_transfer_id, TransferStatus.CONFIRMED.value)

        cx_dt = None
        if codex_completed_at:
            if isinstance(codex_completed_at, datetime):
                cx_dt = codex_completed_at
            elif isinstance(codex_completed_at, str):
                try:
                    cx_dt = datetime.fromisoformat(codex_completed_at.replace("Z", "+00:00"))
                except Exception:
                    cx_dt = None

        iter_record = AutoRunIteration(
            run_id=run_id,
            iteration_number=iteration,
            status="completed",
            evidence_schema_version=2,
            codex_source_turn_id=None,
            codex_source_message_id=None,
            codex_output_hash=compute_sha256(cx_asst_msg or "") if cx_asst_msg is not None else None,
            c2g_transfer_id=c2g_transfer_id,
            chatgpt_user_turn_id=cg_user_turn_id,
            chatgpt_user_message_id=cg_user_msg_id,
            chatgpt_user_raw_hash=cg_user_raw_hash,
            chatgpt_user_semantic_hash=cg_user_sem_hash,
            chatgpt_assistant_turn_id=cg_asst_turn_id,
            chatgpt_assistant_message_id=cg_asst_msg_id,
            chatgpt_assistant_response_hash=cg_asst_resp_hash,
            planner_response_hash=compute_sha256(response_parts.get("raw_text", "")),
            planner_classification=classification.value,
            extracted_prompt_hash=g2c_hash,
            g2c_transfer_id=g2c_transfer_id,
            queue_receipt_id=receipt_id,
            queue_client_user_message_id=client_user_message_id,
            queued_item_id=queued_item_id,
            codex_target_turn_id=codex_user_turn_id,
            codex_user_message_id=codex_user_msg_id,
            codex_assistant_message_id=codex_asst_msg_id,
            codex_completed_at=cx_dt,
            thread_resume_required=resume_required,
            thread_resume_attempted=resume_attempted,
            thread_resume_result=resume_result,
            started_at=datetime.fromtimestamp(t_iter_start),
            completed_at=datetime.utcnow(),
            metrics=metrics,
        )
        await self.db.save_auto_run_iteration(iter_record)
        logger.info("Iteration %s of run %s completed successfully!", iteration, run_id)
        return True, None, None
