import re
import asyncio
from functools import wraps
"""
FastAPI Application and Local Control Plane Endpoints.
"""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from datetime import datetime, timedelta
import uuid
from app.adapters import (
    DeliveryOutcomeUnknownError,
    NotSentError,
    IChatGPTMessageSender,
    ICodexQueueAdapter,
    MockChatGPTBrowserObserver,
    MockChatGPTMessageSender,
    MockCodexQueueAdapter,
    ReadOnlyCodexSessionObserver,
    RealChatGPTBrowserObserver,
    RealChatGPTMessageSender,
    RealCodexQueueAdapter,
    StandardPromptExtractor,
)
from app.database import Database
from app.models import (
    AppConfig,
    CodexConfig,
    ChatGPTConfig,
    CodexDeliveryClassification,
    ObservationRecord,
    PromptExtractionRecord,
    RouteConfig,
    ShuttleStage,
    TransferAuthorization,
    TransferDirection,
    TransferRecord,
    TransferStatus,
    compute_sha256,
    CanaryRoundRecord,
    CanaryRoundResult,
    CanaryRoundMetrics,
)
from app.state_machine import InvalidTransitionError, ShuttleStateMachine
from app.canary import (load_explicit_config, validate_canary_config, validate_canary_session,
                        canary_prompt, canary_request, canary_nonce,
                        is_production_target, is_production_thread,
                        is_production_conversation, is_production_workspace,
                        compute_config_fingerprint, same_path,
                        generate_canary_nonce, validate_round_invariants,
                        CHATGPT_DELIVERY_TIMEOUT_SEC, CHATGPT_ASSISTANT_TIMEOUT_SEC,
                        CODEX_QUEUE_DISPATCH_TIMEOUT_SEC, CODEX_COMPLETION_TIMEOUT_SEC)
from app.runtime_state import transfer_error, authorization_error

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "shuttle_console.db"
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"

db = Database(DB_PATH)
state_machine = ShuttleStateMachine(db)

# Adapter Instances
codex_observer: Optional[ReadOnlyCodexSessionObserver] = None
codex_queue = MockCodexQueueAdapter()
real_codex_queue: Optional[RealCodexQueueAdapter] = None
chatgpt_observer = MockChatGPTBrowserObserver()
real_chatgpt_observer: Optional[RealChatGPTBrowserObserver] = None
chatgpt_sender = MockChatGPTMessageSender()
real_chatgpt_sender: Optional[RealChatGPTMessageSender] = None
prompt_extractor = StandardPromptExtractor()
runtime_identity = None
runtime_browser_status = {}
runtime_codex_status = {}
runtime_lock = asyncio.Lock()
transfer_lock = asyncio.Lock()


def serialize_transfer(function):
    @wraps(function)
    async def serialized(*args, **kwargs):
        async with transfer_lock:
            if runtime_identity and runtime_identity.get("startup_canary"):
                cfg = await db.get_config()
                if not cfg or not cfg.canary_mode:
                    raise HTTPException(status_code=409, detail="RUNTIME_RESTART_REQUIRED: active Canary mode cannot be changed")
            return await function(*args, **kwargs)
    return serialized


async def check_canary_targets(config):
    """No writes or submissions: revalidate session header/index, browser URL and CLI help."""
    validate_canary_config(config)
    observer = ReadOnlyCodexSessionObserver(Path(config.codex.sessions_dir), Path(config.codex.session_index_path))
    session = validate_canary_session(config, observer)
    browser = RealChatGPTBrowserObserver(target_url=config.chatgpt.conversation_url,
                                        cdp_url=config.chatgpt.cdp_url)
    browser_status = await browser.get_browser_status()
    if not browser_status.get("connected") or not browser_status.get("ready"):
        raise ValueError("Canary browser composer is not ready: " + str(browser_status.get("interstitial_reason")))
    queue = RealCodexQueueAdapter(cli_path=config.codex.cli_path)
    await queue.verify_queue_capability()
    return {"status": "verified", "thread_id": config.codex.thread_id,
            "workspace": config.codex.repo_path, "browser_url": browser_status["current_url"],
            "session_file": session["session_file_path"], "transfers_performed": 0}


async def require_canary_targets(config):
    try:
        return await check_canary_targets(config)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Canary preflight rejected: {exc}")


async def current_canary_request():
    request = await db.get_latest_transfer(TransferDirection.CODEX_TO_CHATGPT.value)
    if not request or request.status != TransferStatus.DELIVERED.value:
        raise HTTPException(status_code=400, detail="Current canary request has no verified ChatGPT delivery")
    try:
        nonce = canary_nonce(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if request.message_text != canary_request(nonce):
        raise HTTPException(status_code=400, detail="Canary request contract mismatch")
    return request, nonce


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global db, state_machine, runtime_identity
    explicit_path = os.environ.get("SHUTTLE_CONFIG")
    require_canary = os.environ.get("SHUTTLE_CANARY") == "1"
    if require_canary and not explicit_path:
        raise ValueError("Canary requires explicit SHUTTLE_CONFIG; fallback forbidden")
    explicit_config = load_explicit_config(Path(explicit_path), canary=require_canary) if explicit_path else None
    if explicit_path:
        # A separate DB per selected file preserves the pre-existing default DB/config/checkpoints.
        key = compute_sha256(str(Path(explicit_path).resolve()))[:16]
        db = Database(BASE_DIR / "runtime" / "explicit-config" / key / "state.db")
        state_machine = ShuttleStateMachine(db)
    await db.init()
    config = explicit_config or await db.get_config()
    if explicit_config:
        saved = await db.get_config()
        if saved and saved != explicit_config:
            raise ValueError("Explicit configuration differs from saved isolated state; use a new config filename")
        await db.save_config(explicit_config)
    if not config:
        # Load default configuration
        example_config = BASE_DIR / "config.example.json"
        if example_config.exists():
            try:
                data = json.loads(example_config.read_text(encoding="utf-8"))
                config = AppConfig(
                    codex=CodexConfig(**data["codex"]),
                    chatgpt=ChatGPTConfig(**data["chatgpt"]),
                    route=RouteConfig(**data.get("route", {})),
                    server_host=data.get("server", {}).get("host", "127.0.0.1"),
                    server_port=data.get("server", {}).get("port", 8765),
                )
                await db.save_config(config)
            except Exception as e:
                print("Failed loading default config:", e)

    state_machine.config = config
    runtime_identity = {
        "config_source": str(Path(explicit_path).resolve()) if explicit_path else "database/default",
        "config_fingerprint": compute_config_fingerprint(config),
        "database_path": str(db.db_path),
        "process_id": os.getpid(),
        "startup_canary": require_canary,
    }
    await state_machine.init()
    if config and config.canary_mode:
        interrupted = False
        for transfer in await db.get_all_transfers(limit=100):
            if transfer.status == TransferStatus.SENDING.value:
                transfer.status = TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value
                transfer.error_message = "Interrupted submission recovered on startup; reconcile manually, do not retry"
                await db.save_transfer_record(transfer)
                interrupted = True
        if interrupted:
            await state_machine.transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                transfer_status="unknown", failure_details="Interrupted canary submission", force=True)

    global codex_observer, real_chatgpt_observer, real_chatgpt_sender, real_codex_queue
    if config:
        if config.codex.sessions_dir:
            codex_observer = ReadOnlyCodexSessionObserver(
                Path(config.codex.sessions_dir),
                session_index_path=Path(config.codex.session_index_path) if config.codex.session_index_path else None,
            )
        real_chatgpt_observer = RealChatGPTBrowserObserver(
            target_url=config.chatgpt.conversation_url,
            gpt_id=config.chatgpt.gpt_id,
            conversation_id=config.chatgpt.conversation_id,
            user_data_dir=Path(config.chatgpt.user_data_dir) if config.chatgpt.user_data_dir else None,
            cdp_url=config.chatgpt.cdp_url,
            prompt_extractor=prompt_extractor,
        )
        real_chatgpt_sender = RealChatGPTMessageSender(
            target_url=config.chatgpt.conversation_url,
            gpt_id=config.chatgpt.gpt_id,
            conversation_id=config.chatgpt.conversation_id,
            cdp_url=config.chatgpt.cdp_url,
            db=db,
        )
        real_codex_queue = RealCodexQueueAdapter(
            cli_path=config.codex.cli_path,
            default_repo_path=config.codex.repo_path,
            db=db,
            observer=codex_observer,
            verified_thread_id=config.codex.thread_id if config.canary_mode else None,
        )

    if config and config.canary_mode:
        await transfer_snapshot(config, repair=True)
    yield
    # Shutdown logic if any


app = FastAPI(title="Local AI Shuttle Control Center", lifespan=lifespan)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


async def runtime_config_error(cfg):
    """Bootstrap evidence may only authorize the exact startup configuration."""
    if not cfg or not cfg.canary_mode:
        return "CANARY_RUNTIME_REQUIRED"
    try:
        validate_canary_config(cfg)
        fp = compute_config_fingerprint(cfg)
        if compute_config_fingerprint(state_machine.config) != fp:
            return "RUNTIME_RESTART_REQUIRED: database and runtime configuration differ"
        identity = runtime_identity or {}
        if identity and identity.get("config_fingerprint") != fp:
            return "RUNTIME_RESTART_REQUIRED: startup configuration differs"
        source = identity.get("config_source") or os.environ.get("SHUTTLE_CONFIG")
        if not source or compute_config_fingerprint(load_explicit_config(Path(source), canary=True)) != fp:
            return "RUNTIME_RESTART_REQUIRED: selected configuration file differs"
        bootstrap = await db.get_canary_bootstrap_state()
        if not bootstrap:
            return "CANARY_PREFLIGHT_REQUIRED"
        path = bootstrap.get("config_path")
        if path and compute_config_fingerprint(load_explicit_config(Path(path), canary=True)) != fp:
            return "RUNTIME_RESTART_REQUIRED: bootstrap verified a different configuration"
        if bootstrap.get("verdict") == "RUNTIME_RESTART_REQUIRED":
            return "RUNTIME_RESTART_REQUIRED: bootstrap configuration differs"
        if (bootstrap.get("verdict") != "CANARY_PREFLIGHT_VERIFIED"
                or bootstrap.get("config_fingerprint") != fp
                or bootstrap.get("preflight_result", {}).get("status") != "verified"):
            return bootstrap.get("blocker") or "CANARY_PREFLIGHT_REQUIRED"
    except (ValueError, OSError) as exc:
        return f"RUNTIME_RESTART_REQUIRED: {exc}"
    return None


async def probe_runtime_targets(cfg):
    """Observe the configured browser/session; no extraction, sends or queueing."""
    global runtime_browser_status, runtime_codex_status
    observer = real_chatgpt_observer
    if not observer or observer.target_url != cfg.chatgpt.conversation_url or observer.cdp_url != cfg.chatgpt.cdp_url:
        observer = RealChatGPTBrowserObserver(target_url=cfg.chatgpt.conversation_url, cdp_url=cfg.chatgpt.cdp_url)
    runtime_browser_status = await observer.get_browser_status()
    runtime_browser_status["observed_at"] = datetime.utcnow().isoformat()
    runtime_browser_status["config_fingerprint"] = compute_config_fingerprint(cfg)
    try:
        session = validate_canary_session(cfg, codex_observer)
        runtime_codex_status = {"verified_session_identity": True,
                                "session_file": session.get("session_file_path")}
    except Exception as exc:
        runtime_codex_status = {"verified_session_identity": False, "blocker": str(exc)}
    await db.record_observation(ObservationRecord(
        source_type="chatgpt", source_identifier=cfg.chatgpt.conversation_url, content_hash="",
        status="browser_ready" if runtime_browser_status.get("ready") else "not_ready",
        error_details=runtime_browser_status.get("interstitial_reason")))


async def transfer_snapshot(cfg, repair=False):
    # Query all records: unresolved delivery must never disappear behind a UI limit.
    unresolved_records = await db.get_unresolved_deliveries()
    unresolved = len(unresolved_records) > 0
    records = await db.get_all_transfers(limit=-1)
    result = {"unresolved_delivery": unresolved, "state_repaired": False, "inconsistent": False,
              "cycle_started": any(t.status != "cancelled" for t in records)}
    for key, direction in (("c2g", "codex_to_chatgpt"), ("g2c", "chatgpt_to_codex")):
        prepared = [t for t in records if t.direction == direction and t.status == "prepared"]
        transfer = prepared[0] if len(prepared) == 1 else None
        error = transfer_error(transfer, cfg, direction)
        preview = ShuttleStage.REPORT_PREVIEW if key == "c2g" else ShuttleStage.PROMPT_PREVIEW
        prior = ShuttleStage.CODEX_WAITING if key == "c2g" else ShuttleStage.CHATGPT_WAITING
        orphan_prepared_status = (key == "c2g" and state_machine.current_checkpoint
                                  and state_machine.current_checkpoint.transfer_status == "prepared"
                                  and not any(t.status == "prepared" for t in records))

        if len(prepared) > 1:
            result["inconsistent"] = True
            if repair and not unresolved:
                for invalid in prepared:
                    invalid.status = "cancelled"
                    invalid.error_message = "TRANSFER_STATE_INCONSISTENT: Multiple active prepared transfers found"
                    await db.save_transfer_record(invalid)
                    if invalid.confirmation_token:
                        await db.invalidate_transfer_authorization(invalid.confirmation_token)
                if state_machine.stage != prior:
                    await state_machine.transition_to(prior, transfer_status="idle", force=True, clear_payload=True)
                await db.log_event(event_type="TRANSFER_STATE_REPAIRED", severity="warning", stage=prior,
                                   message="Multiple active prepared transfers found",
                                   details={"direction": direction, "cancelled": [t.transfer_id for t in prepared]})
                result["state_repaired"] = True
                transfer = None
        elif len(prepared) == 1:
            auth = await db.get_transfer_authorization(transfer.confirmation_token) if transfer.confirmation_token else None
            auth_err = authorization_error(auth, transfer, compute_config_fingerprint(cfg))
            effective_error = error
            if state_machine.stage != preview and auth_err:
                effective_error = effective_error or f"Prepared transfer authorization is invalid: {auth_err}"

            if effective_error is not None:
                result["inconsistent"] = True
                if repair and not unresolved:
                    for invalid in prepared:
                        invalid.status = "cancelled"
                        invalid.error_message = f"TRANSFER_STATE_INCONSISTENT: {effective_error}"
                        await db.save_transfer_record(invalid)
                        if invalid.confirmation_token:
                            await db.invalidate_transfer_authorization(invalid.confirmation_token)
                    if state_machine.stage != prior:
                        await state_machine.transition_to(prior, transfer_status="idle", force=True, clear_payload=True)
                    await db.log_event(event_type="TRANSFER_STATE_REPAIRED", severity="warning", stage=prior,
                                       message=effective_error,
                                       details={"direction": direction, "cancelled": [t.transfer_id for t in prepared]})
                    result["state_repaired"] = True
                    transfer = None
            else:
                # Fully valid prepared transfer with all required source and transfer fields
                if state_machine.stage != preview:
                    if repair and not unresolved:
                        await state_machine.transition_to(
                            preview,
                            iteration_id=transfer.iteration_id,
                            content_hash=transfer.content_hash,
                            payload_preview=transfer.payload_preview,
                            transfer_status="prepared",
                            force=True,
                        )
                        await db.log_event(
                            event_type="STAGE_RESTORED",
                            severity="info",
                            stage=preview,
                            message=f"Restored stage to {preview.value} for verified prepared transfer {transfer.transfer_id}",
                            details={"transfer_id": transfer.transfer_id, "direction": direction},
                        )
                        result["state_repaired"] = True
                    else:
                        result["inconsistent"] = True
        else:
            # len(prepared) == 0
            if state_machine.stage == preview or orphan_prepared_status:
                result["inconsistent"] = True
                if repair and not unresolved:
                    if state_machine.stage != prior or (state_machine.current_checkpoint and state_machine.current_checkpoint.transfer_status == "prepared"):
                        await state_machine.transition_to(prior, transfer_status="idle", force=True, clear_payload=True)
                    await db.log_event(event_type="TRANSFER_STATE_REPAIRED", severity="warning", stage=prior,
                                       message="Orphan preview stage without active prepared transfer",
                                       details={"direction": direction})
                    result["state_repaired"] = True

        # Re-fetch auth for result
        auth = await db.get_transfer_authorization(transfer.confirmation_token) if transfer and transfer.confirmation_token else None
        auth_error = authorization_error(auth, transfer, compute_config_fingerprint(cfg))
        result[key] = {"transfer": transfer.model_dump(mode="json") if transfer else None,
                       "valid": transfer is not None and error is None,
                       "blocker": error or auth_error,
                       "authorization_ready": transfer is not None and error is None and auth_error is None}
    return result


async def runtime_readiness(probe=False, repair=False):
    cfg = await db.get_config()
    if not cfg or not cfg.canary_mode:
        return {"status": "CANARY_RUNTIME_REQUIRED", "blocker": "Explicit Canary runtime required",
                "c2g_send_enabled": False, "g2c_queue_enabled": False}
    if probe:
        await probe_runtime_targets(cfg)
    binding = await runtime_config_error(cfg)
    transfers = await transfer_snapshot(cfg, repair=repair)
    browser = runtime_browser_status
    browser_ready = bool(browser.get("connected") and browser.get("ready")
                         and browser.get("current_url") == cfg.chatgpt.conversation_url
                         and browser.get("config_fingerprint") == compute_config_fingerprint(cfg)
                         and not browser.get("human_verification_required"))
    blocker = binding
    status = "RUNTIME_RESTART_REQUIRED" if binding and "RUNTIME_RESTART_REQUIRED" in binding else "CANARY_PREFLIGHT_REQUIRED"
    if transfers["unresolved_delivery"] or state_machine.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN:
        status, blocker = "DELIVERY_OUTCOME_UNKNOWN", "Previous delivery requires manual reconciliation"
    elif not binding:
        if not browser_ready:
            status, blocker = "BROWSER_NOT_READY", browser.get("interstitial_reason") or "No live browser probe"
        elif not runtime_codex_status.get("verified_session_identity"):
            status, blocker = "CODEX_TARGET_UNVERIFIED", runtime_codex_status.get("blocker")
        elif transfers["state_repaired"] or transfers["inconsistent"]:
            status, blocker = "TRANSFER_STATE_INCONSISTENT", "Invalid preview repaired; refresh to inspect the prior state"
        elif transfers["c2g"]["valid"]:
            if state_machine.stage != ShuttleStage.REPORT_PREVIEW:
                status, blocker = "TRANSFER_STATE_INCONSISTENT", "Prepared report exists outside REPORT_PREVIEW"
            elif browser.get("send_control_verified"):
                status = "CANARY_C2G_SEND_CONTROL_VERIFIED"
                blocker = None
            else:
                status = "CANARY_REPORT_PREPARED_AWAITING_AUTHORIZATION"
                blocker = transfers["c2g"]["blocker"]
        elif transfers["g2c"]["valid"]:
            status, blocker = "CANARY_PROMPT_PREPARED_AWAITING_AUTHORIZATION", transfers["g2c"]["blocker"]
            if state_machine.stage != ShuttleStage.PROMPT_PREVIEW:
                status, blocker = "TRANSFER_STATE_INCONSISTENT", "Prepared prompt exists outside PROMPT_PREVIEW"
        elif state_machine.stage in (ShuttleStage.CHATGPT_WAITING, ShuttleStage.CHATGPT_RESPONSE_READY):
            status, blocker = "CANARY_AWAITING_CHATGPT_REPLY", "Observe the reply to the current delivered canary request"
        elif transfers["cycle_started"]:
            all_transfers = await db.get_all_transfers(limit=-1)
            g2c_delivered = [t for t in all_transfers if t.direction == "chatgpt_to_codex" and t.status == "delivered"]
            c2g_delivered = [t for t in all_transfers if t.direction == "codex_to_chatgpt" and t.status == "delivered"]
            if g2c_delivered and c2g_delivered:
                latest_g2c_rec = g2c_delivered[0]
                ev = latest_g2c_rec.delivery_evidence if isinstance(latest_g2c_rec.delivery_evidence, dict) else {}
                if (
                    ev.get("task_completion_status") == "completed"
                    or ev.get("outcome_classification") == CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
                ):
                    rounds = await db.get_canary_rounds()
                    completed_rounds = [r for r in rounds if r.get("result") == CanaryRoundResult.ROUND_COMPLETED.value]
                    if len(completed_rounds) >= 3:
                        status, blocker = "THREE_ROUND_CANARY_STABILITY_VERIFIED", None
                    elif any(r.get("result") in (CanaryRoundResult.ROUND_FAILED_VALIDATION.value, CanaryRoundResult.ROUND_DELIVERY_UNKNOWN.value) for r in rounds):
                        status, blocker = "THREE_ROUND_CANARY_STABILITY_BLOCKED", "One or more canary rounds failed validation"
                    elif len(rounds) > 0:
                        status, blocker = "THREE_ROUND_CANARY_READY", None
                    else:
                        status, blocker = "CANARY_END_TO_END_VERIFIED", None
                else:
                    status, blocker = "CANARY_CYCLE_ALREADY_USED", "Inspect the existing cycle; no automatic reauthorization or retry"
            else:
                status, blocker = "CANARY_CYCLE_ALREADY_USED", "Inspect the existing cycle; no automatic reauthorization or retry"
        else:
            status, blocker = "CANARY_READY_FOR_C2G_PREPARE", None
    common = (not binding and browser_ready and runtime_codex_status.get("verified_session_identity")
              and not transfers["unresolved_delivery"] and not transfers["inconsistent"]
              and state_machine.stage != ShuttleStage.DELIVERY_OUTCOME_UNKNOWN)
    c2g = bool(common and transfers["c2g"]["authorization_ready"] and state_machine.stage == ShuttleStage.REPORT_PREVIEW)
    g2c = bool(common and transfers["g2c"]["authorization_ready"] and state_machine.stage == ShuttleStage.PROMPT_PREVIEW)
    if g2c:
        t = transfers["g2c"]["transfer"]
        ext = await db.get_latest_prompt_extraction(cfg.chatgpt.conversation_id)
        request = await db.get_latest_transfer("codex_to_chatgpt")
        g2c = bool(ext and ext.is_fresh and ext.transfer_eligible and ext.source_message_id
                   and ext.source_message_id == t["source_message_id"]
                   and ext.extracted_prompt_hash == t["content_hash"] and ext.extracted_prompt_preview == t["message_text"]
                   and ext.provenance_policy == t["provenance_policy"] and request and request.status == "delivered"
                   and request.sent_at and ext.created_at > request.sent_at
                   and request.provenance_policy == t["provenance_policy"])
        if not g2c and not blocker:
            blocker = "Current delivered request and fresh extracted prompt do not match the prepared transfer"
    return {"status": status, "blocker": blocker, "config_fingerprint": compute_config_fingerprint(cfg),
            "browser_status": browser, "codex_status": runtime_codex_status, "transfers": transfers,
            "c2g_send_enabled": c2g, "g2c_queue_enabled": g2c}


async def require_runtime_transfer(direction, transfer=None, token=None):
    cfg = await db.get_config()
    if not cfg or not cfg.canary_mode:
        return
    ready = await runtime_readiness(probe=True)
    if transfer is None:
        if ready["status"] not in ("CANARY_READY_FOR_C2G_PREPARE", "CANARY_END_TO_END_VERIFIED", "THREE_ROUND_CANARY_READY") and direction == "codex_to_chatgpt":
            raise HTTPException(status_code=409, detail=ready["blocker"] or ready["status"])
        if (await runtime_config_error(cfg) or not ready.get("browser_status", {}).get("ready")
                or not ready.get("codex_status", {}).get("verified_session_identity")
                or ready["transfers"]["unresolved_delivery"] or ready["transfers"]["inconsistent"]):
            raise HTTPException(status_code=409, detail=ready["blocker"] or ready["status"])
        return
    error = transfer_error(transfer, cfg, direction)
    auth = await db.get_transfer_authorization(token) if token else None
    error = error or authorization_error(auth, transfer, compute_config_fingerprint(cfg), token)
    enabled = ready["c2g_send_enabled" if direction == "codex_to_chatgpt" else "g2c_queue_enabled"]
    if error or not enabled:
        raise HTTPException(status_code=409, detail=error or ready["blocker"] or ready["status"])


# ---------------------------------------------------------------- HTML Pages


@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    config = await db.get_config()
    checkpoint = state_machine.current_checkpoint
    logs = await db.get_recent_logs(limit=30)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "config": config,
            "checkpoint": checkpoint,
            "logs": logs,
            "all_stages": [s.value for s in ShuttleStage],
        },
    )


# ---------------------------------------------------------------- API Endpoints


@app.get("/api/state")
async def get_state():
    cp = state_machine.current_checkpoint
    return {
        "stage": state_machine.stage.value,
        "iteration_id": cp.iteration_id if cp else 0,
        "source_identifier": cp.source_identifier if cp else "",
        "destination_identifier": cp.destination_identifier if cp else "",
        "transfer_status": cp.transfer_status if cp else "idle",
        "content_hash": cp.content_hash if cp else None,
        "failure_details": cp.failure_details if cp else None,
        "updated_at": cp.updated_at.isoformat() if cp else None,
    }


@app.get("/api/transfer/target_info")
async def get_transfer_target_info():
    cfg = await db.get_config()
    current_fp = compute_config_fingerprint(cfg)
    is_canary = bool(cfg and cfg.canary_mode)
    is_prod_target = bool(cfg and is_production_target(
        thread_id=cfg.codex.thread_id,
        conv_id_or_url=cfg.chatgpt.conversation_url,
        repo_path=cfg.codex.repo_path,
    ))
    binding_error = await runtime_config_error(cfg) if is_canary else None
    blocking_reason = (
        "Server running in PRODUCTION mode; cannot run Canary transfers"
        if not is_canary
        else ("Canary configuration contains production targets" if is_prod_target else binding_error)
    )
    if is_canary and not blocking_reason:
        unresolved = await db.get_unresolved_deliveries()
        if unresolved or state_machine.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN:
            blocking_reason = "Previous delivery requires manual reconciliation"
    return {
        "execution_mode": "CANARY" if is_canary else "PRODUCTION",
        "config_source": (runtime_identity or {}).get("config_source", os.environ.get("SHUTTLE_CONFIG") or "database/default"),
        "database_path": str(db.db_path),
        "runtime_identity": runtime_identity,
        "config_fingerprint": current_fp,
        "canary_mode": is_canary,
        "is_production_target": is_prod_target,
        "chatgpt": {
            "conversation_url": cfg.chatgpt.conversation_url if cfg else None,
            "conversation_id": cfg.chatgpt.conversation_id if cfg else None,
            "gpt_id": cfg.chatgpt.gpt_id if cfg else None,
        },
        "codex": {
            "thread_id": cfg.codex.thread_id if cfg else None,
            "repo_path": cfg.codex.repo_path if cfg else None,
            "thread_title": cfg.codex.thread_title if cfg else None,
        },
        "canary_safe": is_canary and not is_prod_target and not binding_error,
        "blocking_reason": blocking_reason,
    }


@app.get("/api/config")
async def get_configuration():
    config = await db.get_config()
    if not config:
        raise HTTPException(status_code=404, detail="Configuration not initialized")
    return config.model_dump(mode="json")


@app.post("/api/config")
async def update_configuration(config_data: AppConfig):
    current = await db.get_config()
    if (current and current.canary_mode) or os.environ.get("SHUTTLE_CONFIG"):
        raise HTTPException(status_code=409, detail="Explicit configuration is locked; stop server and select a new config file")
    if config_data.canary_mode:
        raise HTTPException(status_code=400, detail="Canary requires explicit --config --canary startup")
    await db.save_config(config_data)
    state_machine.config = config_data
    global codex_observer, real_chatgpt_observer, real_chatgpt_sender, real_codex_queue
    if config_data.codex.sessions_dir:
        codex_observer = ReadOnlyCodexSessionObserver(
            Path(config_data.codex.sessions_dir),
            session_index_path=Path(config_data.codex.session_index_path) if config_data.codex.session_index_path else None,
        )
    real_chatgpt_observer = RealChatGPTBrowserObserver(
        target_url=config_data.chatgpt.conversation_url,
        gpt_id=config_data.chatgpt.gpt_id,
        conversation_id=config_data.chatgpt.conversation_id,
        user_data_dir=Path(config_data.chatgpt.user_data_dir) if config_data.chatgpt.user_data_dir else None,
        cdp_url=config_data.chatgpt.cdp_url,
        prompt_extractor=prompt_extractor,
    )
    real_chatgpt_sender = RealChatGPTMessageSender(
        target_url=config_data.chatgpt.conversation_url,
        gpt_id=config_data.chatgpt.gpt_id,
        conversation_id=config_data.chatgpt.conversation_id,
        cdp_url=config_data.chatgpt.cdp_url,
        db=db,
    )
    real_codex_queue = RealCodexQueueAdapter(
        cli_path=config_data.codex.cli_path,
        default_repo_path=config_data.codex.repo_path,
        db=db,
        observer=codex_observer,
    )

    await db.log_event(
        event_type="CONFIG_UPDATED",
        stage=state_machine.stage,
        message="Configuration updated by operator",
    )
    return {"status": "success", "message": "Configuration saved"}


class TransitionRequest(BaseModel):
    target_stage: ShuttleStage
    reason: Optional[str] = None
    force: bool = False


@app.post("/api/stage/transition")
@serialize_transfer
async def request_stage_transition(req: TransitionRequest):
    try:
        updated = await state_machine.transition_to(
            target_stage=req.target_stage,
            failure_details=req.reason if req.target_stage in (ShuttleStage.ERROR, ShuttleStage.HUMAN_ACTION_REQUIRED) else None,
            force=req.force,
        )
        return {"status": "success", "stage": updated.stage.value}
    except InvalidTransitionError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/logs")
async def get_logs(limit: int = 50):
    logs = await db.get_recent_logs(limit=limit)
    return {"logs": logs}


class PreviewRequest(BaseModel):
    text: str


@app.post("/api/preview/hash")
async def compute_hash_preview(req: PreviewRequest):
    h = compute_sha256(req.text)
    preview = req.text[:200] + ("..." if len(req.text) > 200 else "")
    return {
        "sha256": h,
        "length_chars": len(req.text),
        "preview": preview,
    }


class ObserveCodexRequest(BaseModel):
    thread_id: Optional[str] = None
    after_line: int = 0


@app.post("/api/codex/observe")
async def observe_codex_thread(req: ObserveCodexRequest):
    cfg = await db.get_config()
    if cfg and cfg.canary_mode:
        if req.thread_id and req.thread_id != cfg.codex.thread_id:
            raise HTTPException(status_code=400, detail="Canary observer requires the configured test thread")
        validate_canary_session(cfg, codex_observer)
        current = await db.get_latest_transfer(TransferDirection.CHATGPT_TO_CODEX.value)
        if current:
            return await canary_completion(current.transfer_id)
        return {"status": "identity_verified", "thread_id": cfg.codex.thread_id,
                "task_completion_status": "no_current_task"}
    target_thread = req.thread_id or (cfg.codex.thread_id if cfg else None)
    if not target_thread:
        raise HTTPException(status_code=400, detail="No thread_id specified or configured")

    if not codex_observer:
        raise HTTPException(status_code=500, detail="Codex observer not initialized")

    expected_cwd = cfg.codex.project_cwd if cfg else None
    expected_title = cfg.codex.conversation_title if cfg else None

    # Full structured reconciliation
    obs_detail = codex_observer.observe_thread(
        target_thread,
        expected_cwd=expected_cwd,
        expected_title=expected_title,
        after_line=req.after_line,
    )

    ans = obs_detail["latest_assistant_response"]
    line_idx = obs_detail["latest_assistant_line"] if obs_detail["latest_assistant_line"] is not None else -1
    turn_id = obs_detail["latest_turn_id"]

    # Record observation to SQLite
    status = "verified" if (obs_detail["metadata_valid"] and obs_detail["is_completed"]) else (
        "in_progress" if obs_detail["is_in_progress"] else "unverified"
    )
    if obs_detail["validation_errors"]:
        status = "error"

    content_hash = obs_detail["latest_response_hash"] or (compute_sha256(ans) if ans else "")
    obs_record = ObservationRecord(
        source_type="codex",
        source_identifier=target_thread,
        message_id=turn_id,
        event_offset=line_idx,
        content_hash=content_hash,
        payload_preview=ans[:200] if ans else None,
        raw_payload_snippet=ans[:1000] if ans else None,
        status=status,
        error_details="; ".join(obs_detail["validation_errors"]) if obs_detail["validation_errors"] else None,
    )
    await db.record_observation(obs_record)

    if ans and obs_detail["is_completed"]:
        if state_machine.can_transition_to(ShuttleStage.CODEX_RESPONSE_READY):
            await state_machine.transition_to(
                ShuttleStage.CODEX_RESPONSE_READY,
                source_id=f"thread:{target_thread}",
                dest_id="chatgpt",
                content_hash=content_hash,
                payload_preview=ans[:200],
            )
        return {
            "status": "found",
            "turn_id": turn_id,
            "line_index": line_idx,
            "length": len(ans),
            "sha256": content_hash,
            "preview": ans[:300],
            "metadata_valid": obs_detail["metadata_valid"],
            "validation_errors": obs_detail["validation_errors"],
            "total_lines": obs_detail["total_lines"],
            "user_turns_count": obs_detail["user_turns_count"],
            "assistant_turns_count": obs_detail["assistant_turns_count"],
        }
    return {
        "status": "waiting" if not obs_detail["validation_errors"] else "validation_failed",
        "message": "No completed final_answer ready for target thread" if not obs_detail["validation_errors"] else "; ".join(obs_detail["validation_errors"]),
        "in_progress": obs_detail["is_in_progress"],
        "metadata_valid": obs_detail["metadata_valid"],
        "validation_errors": obs_detail["validation_errors"],
    }


class ObserveChatGPTRequest(BaseModel):
    injected_dom: Optional[Dict[str, Any]] = None


@app.post("/api/chatgpt/observe")
async def observe_chatgpt_conversation(req: Optional[ObserveChatGPTRequest] = None):
    """
    Read-only observation of the configured ChatGPT browser conversation.
    Strictly zero mutation, zero sending.
    """
    cfg = await db.get_config()
    if cfg and cfg.canary_mode:
        if req and req.injected_dom is not None:
            raise HTTPException(status_code=400, detail="Canary live observation overrides forbidden")
        await probe_runtime_targets(cfg)
        return {"status": "ready" if runtime_browser_status.get("ready") else "waiting",
                "browser_status": runtime_browser_status, "latest_response": None, "extraction": None}
    target_url = cfg.chatgpt.conversation_url if cfg else None

    if real_chatgpt_observer and (not target_url or real_chatgpt_observer.target_url == target_url):
        observer = real_chatgpt_observer
    else:
        observer = RealChatGPTBrowserObserver(
            target_url=target_url,
            gpt_id=cfg.chatgpt.gpt_id if cfg else None,
            conversation_id=cfg.chatgpt.conversation_id if cfg else None,
            user_data_dir=Path(cfg.chatgpt.user_data_dir) if (cfg and cfg.chatgpt.user_data_dir) else None,
            cdp_url=cfg.chatgpt.cdp_url if cfg else None,
            prompt_extractor=prompt_extractor,
        )

    if req and req.injected_dom:
        observer.set_injected_page_data(req.injected_dom)

    status_info = await observer.get_browser_status()
    latest_resp = await observer.get_latest_assistant_response()

    # Record observation to SQLite
    obs_status = "verified" if latest_resp else ("blocked" if status_info.get("human_verification_required") else "not_ready")
    content_hash = latest_resp.get("content_hash", "") if latest_resp else ""
    obs_record = ObservationRecord(
        source_type="chatgpt",
        source_identifier=target_url or "unknown",
        message_id=None,
        event_offset=None,
        content_hash=content_hash,
        payload_preview=latest_resp.get("text", "")[:200] if latest_resp else None,
        raw_payload_snippet=latest_resp.get("text", "")[:1000] if latest_resp else None,
        status=obs_status,
        error_details=status_info.get("interstitial_reason"),
    )
    await db.record_observation(obs_record)

    # If prompt was extracted, record extraction
    extraction_rec = None
    if latest_resp and latest_resp.get("extracted_prompt"):
        extracted_p = latest_resp["extracted_prompt"]
        prompt_hash = latest_resp.get("extracted_prompt_hash") or compute_sha256(extracted_p)

        # Critical Stale-Prompt Protection: Check if already observed in SQLite
        is_already_seen = await db.has_seen_content_hash(prompt_hash)
        is_fresh = not is_already_seen
        transfer_eligible = is_fresh
        ineligibility_reason = None if is_fresh else "Stale response: prompt hash matches a previously observed response (restart recovery invariant preserved)"

        ext = PromptExtractionRecord(
            source_conversation_id=cfg.chatgpt.conversation_id if (cfg and cfg.chatgpt.conversation_id) else (target_url or "chatgpt"),
            source_message_id=None,
            content_hash=content_hash,
            extracted_prompt_hash=prompt_hash,
            extracted_prompt_preview=extracted_p[:200],
            raw_response_snippet=latest_resp.get("text", "")[:500],
            provenance_policy=latest_resp.get("provenance_policy") or "unknown",
            is_fresh=is_fresh,
            transfer_eligible=transfer_eligible,
            ineligibility_reason=ineligibility_reason,
        )
        extraction_rec = await db.record_prompt_extraction(ext)

        # Transition stage if appropriate and eligible
        if transfer_eligible and state_machine.can_transition_to(ShuttleStage.CHATGPT_RESPONSE_READY):
            await state_machine.transition_to(
                ShuttleStage.CHATGPT_RESPONSE_READY,
                source_id="chatgpt",
                dest_id="codex_gui",
                content_hash=ext.extracted_prompt_hash,
                payload_preview=extracted_p[:200],
            )

    return {
        "status": "ready" if latest_resp else "waiting",
        "browser_status": status_info,
        "latest_response": latest_resp,
        "extraction": extraction_rec.model_dump(mode="json") if extraction_rec else None,
    }


@app.get("/api/observations")
async def get_latest_observations(source_type: Optional[str] = None):
    if source_type:
        obs = await db.get_latest_observation(source_type)
        return {"observation": obs.model_dump(mode="json") if obs else None}
    obs_codex = await db.get_latest_observation("codex")
    obs_chatgpt = await db.get_latest_observation("chatgpt")
    return {
        "codex": obs_codex.model_dump(mode="json") if obs_codex else None,
        "chatgpt": obs_chatgpt.model_dump(mode="json") if obs_chatgpt else None,
    }


@app.get("/api/extractions")
async def get_latest_extraction(source_conversation_id: Optional[str] = None):
    ext = await db.get_latest_prompt_extraction(source_conversation_id)
    return {"extraction": ext.model_dump(mode="json") if ext else None}


@app.get("/api/reconciliation")
async def get_reconciliation_summary():
    """
    Returns unified reconciliation status for Codex, ChatGPT, and transfer eligibility.
    Read-only audit summary.
    """
    cfg = await db.get_config()
    obs_codex = await db.get_latest_observation("codex")
    obs_chatgpt = await db.get_latest_observation("chatgpt")
    ext = await db.get_latest_prompt_extraction()

    # Determine codex status
    codex_status = "unverified"
    if obs_codex:
        if obs_codex.status == "verified":
            codex_status = "historical" if (state_machine.stage not in [ShuttleStage.CODEX_RESPONSE_READY, ShuttleStage.REPORT_PREVIEW]) else "new"
        elif obs_codex.status == "in_progress":
            codex_status = "incomplete"
        elif obs_codex.status == "error":
            codex_status = "unverified"

    # Determine chatgpt status
    chatgpt_status = "unverified"
    if obs_chatgpt:
        if obs_chatgpt.status == "verified":
            chatgpt_status = "historical" if (ext and not ext.is_fresh) else "new"
        elif obs_chatgpt.status in ["blocked", "not_ready"]:
            chatgpt_status = "incomplete"

    # Determine transfer eligibility and reason
    transfer_eligible = bool(ext and ext.transfer_eligible and ext.is_fresh)
    ineligibility_reason = ext.ineligibility_reason if (ext and not transfer_eligible) else (
        "No fresh prompt extracted from ChatGPT" if not transfer_eligible else None
    )

    # Check iteration correlation / matching
    # If Codex is not verified or ChatGPT is blocked/incomplete, mark MISMATCH / UNVERIFIED
    reconciliation_match = "UNVERIFIED"
    if not obs_codex or obs_codex.status == "error":
        reconciliation_match = "UNVERIFIED_CODEX"
    elif not obs_chatgpt or obs_chatgpt.status != "verified":
        reconciliation_match = "UNVERIFIED_CHATGPT"
    else:
        # Both sides observed: check iteration pairing
        # E.g. If Codex latest turn is Goal A checkpoint, ChatGPT prompt must correlate to Goal A continuation
        if ext and ext.extracted_prompt_preview:
            # Check if prompt contains keywords matching the iteration
            if "GOAL A" in ext.extracted_prompt_preview.upper() or "RUN HUZI" in ext.extracted_prompt_preview.upper() or "LOOKAHEAD" in ext.extracted_prompt_preview.upper():
                reconciliation_match = "MATCHED"
            else:
                reconciliation_match = "MISMATCH"
                transfer_eligible = False
                ineligibility_reason = "Iteration mismatch: ChatGPT prompt does not correlate with Codex current iteration"
        else:
            reconciliation_match = "MISMATCH"
            transfer_eligible = False
            ineligibility_reason = "No valid prompt extracted to pair with Codex iteration"

    if not transfer_eligible and not ineligibility_reason:
        ineligibility_reason = "No fresh prompt extracted from ChatGPT or iteration correlation incomplete"

    result = {
        "codex": {
            "project_path": cfg.codex.repo_path if cfg else None,
            "branch": cfg.codex.expected_branch if cfg else None,
            "thread_id": cfg.codex.thread_id if cfg else None,
            "session_title": cfg.codex.thread_title if cfg else None,
            "verified_session_identity": bool(obs_codex and obs_codex.status != "error"),
            "latest_completed_response": obs_codex.payload_preview if obs_codex else None,
            "turn_id": obs_codex.message_id if obs_codex else None,
            "timestamp": obs_codex.observed_at.isoformat() if obs_codex else None,
            "content_hash": obs_codex.content_hash if obs_codex else None,
            "observation_status": codex_status,
        },
        "chatgpt": {
            "conversation_url": cfg.chatgpt.conversation_url if cfg else None,
            "expected_gpt_id": cfg.chatgpt.gpt_id if cfg else None,
            "expected_conversation_id": cfg.chatgpt.conversation_id if cfg else None,
            "verified_identity": bool(obs_chatgpt and obs_chatgpt.status != "blocked"),
            "browser_readiness": "ready" if (obs_chatgpt and obs_chatgpt.status == "verified") else "blocked_or_offline",
            "latest_completed_response": obs_chatgpt.payload_preview if obs_chatgpt else None,
            "timestamp": obs_chatgpt.observed_at.isoformat() if obs_chatgpt else None,
            "content_hash": obs_chatgpt.content_hash if obs_chatgpt else None,
            "observation_status": chatgpt_status,
        },
        "reconciliation": {
            "expected_source": "chatgpt",
            "expected_destination": "codex_gui",
            "match_status": reconciliation_match,
            "extracted_prompt_preview": ext.extracted_prompt_preview if ext else None,
            "extracted_prompt_hash": ext.extracted_prompt_hash if ext else None,
            "provenance_policy": ext.provenance_policy if ext else None,
            "is_fresh": ext.is_fresh if ext else False,
            "transfer_eligible": transfer_eligible,
            "ineligibility_reason": ineligibility_reason,
        },
    }
    if cfg and cfg.canary_mode:
        ready = await runtime_readiness()
        browser = ready.get("browser_status", {})
        result["codex"]["verified_session_identity"] = bool(runtime_codex_status.get("verified_session_identity"))
        if runtime_codex_status.get("verified_session_identity"):
            result["codex"]["observation_status"] = "identity_verified"
        result["chatgpt"].update(verified_identity=bool(browser.get("connected") and browser.get("current_url") == cfg.chatgpt.conversation_url),
                                 browser_readiness="ready" if browser.get("ready") else "blocked_or_offline",
                                 browser_status=browser)
        result["chatgpt"]["observation_status"] = "browser_ready" if browser.get("ready") else "not_ready"
        request = await db.get_latest_transfer("codex_to_chatgpt")
        matched = bool(ext and request and request.status == "delivered" and request.sent_at
                       and ext.created_at > request.sent_at and ext.is_fresh and ext.transfer_eligible
                       and ext.provenance_policy == request.provenance_policy)
        result["reconciliation"].update(match_status="MATCHED" if matched else "AWAITING_CURRENT_CANARY_REPLY",
                                         transfer_eligible=matched,
                                         ineligibility_reason=None if matched else "No verified reply to the current delivered canary request")
        result["readiness"] = ready

    # 4. Codex Delivery Three-Level Evidence Chain
    latest_g2c = await db.get_latest_transfer("chatgpt_to_codex")
    queue_receipt = "none"
    turn_dispatch = "none"
    task_completion = "none"
    outcome_classification = None
    receipt_id = None
    client_user_message_id = None
    evidence = {}
    canary_ok_verified = False
    assistant_reply_text = None
    turn_id = None
    user_message_id = None
    assistant_message_id = None
    action_instruction = None

    if latest_g2c:
        evidence = latest_g2c.delivery_evidence if isinstance(latest_g2c.delivery_evidence, dict) else {}
        queue_receipt = evidence.get("queue_receipt_status") or ("accepted" if latest_g2c.status == "delivered" else "none")
        turn_dispatch = evidence.get("turn_dispatch_status") or "none"
        task_completion = evidence.get("task_completion_status") or "none"
        outcome_classification = evidence.get("outcome_classification") or (
            CodexDeliveryClassification.CODEX_DELIVERY_OUTCOME_UNKNOWN.value
            if latest_g2c.status == "delivery_outcome_unknown"
            else None
        )
        receipt_id = evidence.get("receipt_id")
        client_user_message_id = evidence.get("client_user_message_id")

        if cfg and cfg.canary_mode and codex_observer:
            nonce_to_check = None
            if latest_g2c.provenance_policy and latest_g2c.provenance_policy.startswith("canary:"):
                nonce_to_check = latest_g2c.provenance_policy.split("canary:", 1)[1]
            if nonce_to_check:
                try:
                    baseline = evidence.get("baseline_line", 0)
                    live_eval = codex_observer.classify_delivery(
                        thread_id=latest_g2c.destination_identifier,
                        nonce=nonce_to_check,
                        after_line=baseline,
                        queue_receipt_accepted=(queue_receipt == "accepted"),
                    )
                    outcome_classification = live_eval.get("outcome_classification") or outcome_classification
                    queue_receipt = live_eval.get("queue_receipt_status") or queue_receipt
                    turn_dispatch = live_eval.get("turn_dispatch_status") or turn_dispatch
                    task_completion = live_eval.get("task_completion_status") or task_completion
                    if live_eval.get("receipt_id"):
                        receipt_id = live_eval.get("receipt_id")
                    if live_eval.get("client_user_message_id"):
                        client_user_message_id = live_eval.get("client_user_message_id")
                    canary_ok_verified = live_eval.get("canary_ok_verified", False)
                    assistant_reply_text = live_eval.get("assistant_reply_text")
                    turn_id = live_eval.get("turn_id")
                    user_message_id = live_eval.get("user_message_id")
                    assistant_message_id = live_eval.get("assistant_message_id")
                    action_instruction = live_eval.get("action_instruction")
                except Exception:
                    pass

    codex_delivery = {
        "transfer_id": latest_g2c.transfer_id if latest_g2c else None,
        "transfer_status": latest_g2c.status if latest_g2c else "none",
        "queue_receipt_status": queue_receipt,
        "turn_dispatch_status": turn_dispatch,
        "task_completion_status": task_completion,
        "outcome_classification": outcome_classification,
        "receipt_id": receipt_id,
        "client_user_message_id": client_user_message_id,
        "turn_id": turn_id if 'turn_id' in locals() and turn_id else evidence.get("turn_id"),
        "user_message_id": user_message_id if 'user_message_id' in locals() and user_message_id else evidence.get("user_message_id"),
        "assistant_message_id": assistant_message_id if 'assistant_message_id' in locals() and assistant_message_id else evidence.get("assistant_message_id"),
        "final_answer": assistant_reply_text if 'assistant_reply_text' in locals() and assistant_reply_text else (evidence.get("assistant_reply_text") or evidence.get("final_answer")),
        "canary_ok_verified": canary_ok_verified if 'canary_ok_verified' in locals() else evidence.get("canary_ok_verified", False),
        "nonce_matched": canary_ok_verified if 'canary_ok_verified' in locals() else evidence.get("canary_ok_verified", False),
        "action_instruction": action_instruction if 'action_instruction' in locals() and action_instruction else (
            "Open/resume target Codex thread — do not resend" if outcome_classification == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value else None
        ),
    }
    result["codex_delivery"] = codex_delivery
    result["reconciliation"]["queue_receipt_status"] = queue_receipt
    result["reconciliation"]["turn_dispatch_status"] = turn_dispatch
    result["reconciliation"]["task_completion_status"] = task_completion
    result["reconciliation"]["outcome_classification"] = outcome_classification
    result["reconciliation"]["receipt_id"] = receipt_id
    result["reconciliation"]["client_user_message_id"] = client_user_message_id
    result["reconciliation"]["turn_id"] = codex_delivery["turn_id"]
    result["reconciliation"]["final_answer"] = codex_delivery["final_answer"]
    result["reconciliation"]["canary_ok_verified"] = codex_delivery["canary_ok_verified"]
    result["reconciliation"]["nonce_matched"] = codex_delivery["nonce_matched"]
    result["reconciliation"]["action_instruction"] = codex_delivery["action_instruction"]
    return result


class MockTransferRequest(BaseModel):
    report_text: str
    iteration_id: int = 1


@app.post("/api/transfer/send_to_chatgpt")
async def send_report_to_chatgpt(req: MockTransferRequest):
    """Safe Phase 1 Mock Transfer with duplicate detection."""
    # 1. Duplicate check
    is_dup, dup_msg = await state_machine.check_duplicate(req.report_text)
    if is_dup:
        raise HTTPException(status_code=409, detail=dup_msg)

    # 2. State transition to SEND_PENDING
    if not state_machine.can_transition_to(ShuttleStage.CHATGPT_SEND_PENDING):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot initiate transfer while in stage {state_machine.stage}",
        )

    content_hash = compute_sha256(req.report_text)
    await state_machine.transition_to(
        ShuttleStage.CHATGPT_SEND_PENDING,
        iteration_id=req.iteration_id,
        content_hash=content_hash,
        payload_preview=req.report_text[:200],
        transfer_status="in_progress",
    )

    try:
        # Mock send
        res = await chatgpt_sender.send_message(req.report_text)
        await db.record_transfer(
            iteration_id=req.iteration_id,
            direction="codex_to_chatgpt",
            source_id="codex_gui",
            dest_id="chatgpt_mock",
            content_hash=content_hash,
            payload_preview=req.report_text[:200],
            status="sent",
        )
        await state_machine.transition_to(
            ShuttleStage.CHATGPT_WAITING,
            transfer_status="completed",
        )
        return {"status": "success", "result": res}
    except Exception as exc:
        err = str(exc)
        if "delivery outcome unknown" in err.lower():
            await state_machine.transition_to(
                ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                failure_details=err,
                transfer_status="unknown",
            )
            raise HTTPException(status_code=502, detail=f"Transfer outcome unknown: {err}")
        else:
            await state_machine.transition_to(
                ShuttleStage.ERROR,
                failure_details=err,
                transfer_status="failed",
            )
            raise HTTPException(status_code=500, detail=f"Transfer failed: {err}")


# ---------------------------------------------------------------- Phase 2.3 Controlled Transfer Endpoints


class PrepareCodexReportRequest(BaseModel):
    thread_id: Optional[str] = None
    expected_cwd: Optional[str] = None
    expected_title: Optional[str] = None
    after_line: int = 0
    iteration_id: int = 1
    report_text: Optional[str] = None
    is_canary: bool = False
    round_number: Optional[int] = None
    nonce: Optional[str] = None


@app.post("/api/transfer/codex_report/prepare")
@serialize_transfer
async def prepare_codex_report(req: PrepareCodexReportRequest):
    cfg = await db.get_config()
    nonce = None
    if req.is_canary:
        if cfg and is_production_target(thread_id=cfg.codex.thread_id, conv_id_or_url=cfg.chatgpt.conversation_url, repo_path=cfg.codex.repo_path):
            raise HTTPException(status_code=400, detail="Canary transfer requested but server is running under production configuration. Restart server with --config <canary-config> --canary")
        if is_production_target(thread_id=req.thread_id, conv_id_or_url=cfg.chatgpt.conversation_url if cfg else None, repo_path=req.expected_cwd):
            raise HTTPException(status_code=400, detail="Production targets are strictly forbidden in canary mode")

    if cfg and cfg.canary_mode:
        if not req.is_canary:
            raise HTTPException(status_code=400, detail="Server is in canary mode; only verified canary transfers are permitted")
        if is_production_target(thread_id=cfg.codex.thread_id, conv_id_or_url=cfg.chatgpt.conversation_url, repo_path=cfg.codex.repo_path):
            raise HTTPException(status_code=400, detail="Production targets are forbidden in canary configuration")
        if ((req.thread_id and req.thread_id != cfg.codex.thread_id)
                or (req.expected_cwd and not same_path(req.expected_cwd, cfg.codex.repo_path))):
            raise HTTPException(status_code=400, detail="Canary source differs from effective runtime")
        if req.report_text:
            raise HTTPException(status_code=400, detail="Canary request must be generated from the current nonce; report overrides forbidden")
        await require_runtime_transfer("codex_to_chatgpt")
        if not req.report_text:
            previous = await db.get_latest_transfer()
            if previous and previous.status != "cancelled":
                active_rounds = await db.get_canary_rounds()
                is_multi_round = bool(req.round_number and req.round_number >= 1) or bool(active_rounds and len(active_rounds) < 3)
                if is_multi_round:
                    if previous.status != "delivered" or state_machine.stage != ShuttleStage.IDLE:
                        raise HTTPException(status_code=409, detail="Previous canary cycle not completed or stage not IDLE")
                else:
                    raise HTTPException(status_code=409, detail="This isolated canary state already has a cycle; use a new config filename")
            nonce = req.nonce or generate_canary_nonce()
            req.report_text = canary_request(nonce)
        else:
            match = re.search(r"nonce:\s*([0-9a-f]{32})", req.report_text)
            if match:
                nonce = match.group(1)

    target_thread = req.thread_id or (cfg.codex.thread_id if cfg else None)
    target_cwd = req.expected_cwd or (cfg.codex.repo_path if cfg else None)
    target_title = req.expected_title or (cfg.codex.thread_title if cfg else None)

    if req.is_canary or (cfg and cfg.canary_mode):
        if is_production_target(thread_id=target_thread, repo_path=target_cwd):
            raise HTTPException(status_code=400, detail="Production targets are strictly forbidden in canary mode")

    if req.report_text and req.report_text.strip():
        report_text = req.report_text.strip()
        content_hash = compute_sha256(report_text)
        char_count = len(report_text)
        turn_id = f"canary-request:{nonce}" if nonce else (f"canary-turn-{uuid.uuid4().hex[:8]}" if req.is_canary else "operator-report-turn")
        report_info = {
            "report_text": report_text,
            "content_hash": content_hash,
            "char_count": char_count,
            "turn_id": turn_id,
        }
    else:
        if not target_thread:
            raise HTTPException(status_code=400, detail="Target thread ID is required")

        if not codex_observer:
            raise HTTPException(status_code=500, detail="Codex observer not initialized")

        try:
            report_info = codex_observer.prepare_report(
                thread_id=target_thread,
                expected_cwd=target_cwd,
                expected_title=target_title,
                after_line=req.after_line,
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        report_text = report_info["report_text"]
        content_hash = report_info["content_hash"]
        char_count = report_info["char_count"]

    # Duplicate check
    is_dup, dup_msg = await state_machine.check_duplicate(report_text)
    if is_dup:
        raise HTTPException(status_code=409, detail=dup_msg)

    transfer_id = f"transfer-c2g-{uuid.uuid4().hex[:12]}"
    confirmation_token = f"token-c2g-{uuid.uuid4().hex[:16]}"
    dest = cfg.chatgpt.conversation_url if cfg else "chatgpt"

    config_fingerprint = compute_config_fingerprint(cfg)

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=confirmation_token,
        destination_identifier=dest,
        expected_content_hash=content_hash,
        consumed=False,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=config_fingerprint,
    )

    from app.canonical import compute_semantic_hash
    raw_content_hash = content_hash
    semantic_content_hash = compute_semantic_hash(report_text)

    transfer_rec = TransferRecord(
        transfer_id=transfer_id,
        iteration_id=req.iteration_id,
        direction=TransferDirection.CODEX_TO_CHATGPT.value,
        source_type="canary" if (nonce or req.is_canary) else "codex",
        source_identifier=target_thread,
        source_message_id=report_info.get("turn_id"),
        destination_type="chatgpt",
        destination_identifier=dest,
        content_hash=content_hash,
        char_count=char_count,
        message_text=report_text,
        payload_preview=report_text[:200],
        status=TransferStatus.PREPARED.value,
        human_confirmed=False,
        confirmation_token=confirmation_token,
        is_operator_modified=False,
        provenance_policy=f"canary:{nonce}" if nonce else (
            "canary_fixture_only" if req.is_canary else "codex_session_observer"),
        config_fingerprint=config_fingerprint,
        raw_content_hash=raw_content_hash,
        semantic_content_hash=semantic_content_hash,
    )

    try:
        await state_machine.prepare_transfer_atomic(
            transfer=transfer_rec,
            auth=auth,
            target_stage=ShuttleStage.REPORT_PREVIEW,
        )
    except InvalidTransitionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return {
        "status": "prepared",
        "transfer_id": transfer_id,
        "confirmation_token": confirmation_token,
        "report_text": report_text,
        "content_hash": content_hash,
        "raw_content_hash": raw_content_hash,
        "semantic_content_hash": semantic_content_hash,
        "char_count": char_count,
        "turn_id": report_info.get("turn_id"),
        "destination": dest,
        "canary_nonce": nonce,
        "expected_prompt": canary_prompt(nonce) if nonce else None,
        "execution_mode": "CANARY" if ((cfg and cfg.canary_mode) or nonce or req.is_canary) else "PRODUCTION",
        "config_fingerprint": config_fingerprint,
        "config_source": os.environ.get("SHUTTLE_CONFIG") or "database/default",
        "destination_identifier": dest,
        "destination_conversation_id": cfg.chatgpt.conversation_id if cfg else None,
        "destination_url": cfg.chatgpt.conversation_url if cfg else None,
        "source_thread_id": target_thread,
        "source_workspace": target_cwd,
    }


class SendToChatGPTRequest(BaseModel):
    transfer_id: str
    confirmation_token: str
    page_override: Optional[Any] = None


@app.post("/api/transfer/chatgpt/send")
@serialize_transfer
async def send_to_chatgpt(req: SendToChatGPTRequest):
    if state_machine.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN:
        raise HTTPException(status_code=409, detail="Unknown delivery requires manual reconciliation; no retry")
    transfer = await db.get_transfer_by_id(req.transfer_id)
    if not transfer:
        raise HTTPException(status_code=404, detail=f"Transfer record '{req.transfer_id}' not found")

    if transfer.status not in (TransferStatus.PREPARED.value, "prepared"):
        raise HTTPException(
            status_code=400,
            detail=f"Transfer '{req.transfer_id}' is in status '{transfer.status}', cannot send"
        )

    cfg = await db.get_config()
    current_fp = compute_config_fingerprint(cfg)
    if transfer.config_fingerprint and transfer.config_fingerprint != current_fp:
        raise HTTPException(status_code=400, detail="Configuration has changed since transfer was prepared; authorization invalidated")

    is_canary_transfer = (
        transfer.source_type == "canary"
        or (transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"))
        or (transfer.provenance_policy == "canary_fixture_only")
        or (cfg and cfg.canary_mode)
    )

    if transfer.provenance_policy == "canary_fixture_only" and req.page_override is None:
        raise HTTPException(status_code=400, detail="Live canary requires explicit --config --canary; fixture-only record cannot be sent")

    if is_canary_transfer:
        if cfg and is_production_target(thread_id=cfg.codex.thread_id, conv_id_or_url=cfg.chatgpt.conversation_url, repo_path=cfg.codex.repo_path):
            raise HTTPException(status_code=400, detail="Canary transfer cannot execute under production configuration")
        if is_production_conversation(transfer.destination_identifier) or (cfg and is_production_conversation(cfg.chatgpt.conversation_url)):
            raise HTTPException(status_code=400, detail="Production ChatGPT targets are forbidden in canary mode")
        if cfg and transfer.destination_identifier != cfg.chatgpt.conversation_url:
            raise HTTPException(status_code=400, detail="Transfer destination does not match active canary configuration")
        if transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"):
            try:
                nonce = canary_nonce(transfer)
                if (transfer.message_text != canary_request(nonce)
                        or transfer.destination_identifier != cfg.chatgpt.conversation_url):
                    raise HTTPException(status_code=400, detail="Canary request or destination mismatch")
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

    if state_machine.stage != ShuttleStage.REPORT_PREVIEW:
        raise HTTPException(
            status_code=400,
            detail=f"Transfer cannot be sent outside REPORT_PREVIEW (current stage: {state_machine.stage.value if hasattr(state_machine.stage, 'value') else state_machine.stage})"
        )
    if cfg and cfg.canary_mode:
        if req.page_override is not None:
            raise HTTPException(status_code=400, detail="Canary live send overrides forbidden")
        await require_runtime_transfer("codex_to_chatgpt", transfer, req.confirmation_token)
        if real_chatgpt_sender is None:
            raise HTTPException(status_code=409, detail="Canary sender unavailable; no mock fallback")
    if state_machine.can_transition_to(ShuttleStage.CHATGPT_SEND_PENDING):
        await state_machine.transition_to(
            ShuttleStage.CHATGPT_SEND_PENDING,
            iteration_id=transfer.iteration_id,
            content_hash=transfer.content_hash,
            payload_preview=transfer.payload_preview,
            transfer_status="sending",
        )

    transfer.status = TransferStatus.SENDING.value
    transfer.human_confirmed = True
    transfer.confirmed_at = datetime.utcnow()
    await db.save_transfer_record(transfer)

    sender = real_chatgpt_sender or chatgpt_sender
    try:
        if getattr(sender, "db", None) is None:
            ok, err = await db.verify_and_consume_authorization(
                transfer_id=req.transfer_id,
                token=req.confirmation_token,
                content_hash=transfer.content_hash,
                destination=transfer.destination_identifier,
                expected_config_fingerprint=current_fp,
            )
            if not ok:
                raise ValueError(f"Transfer authorization rejected: {err}")

        active_canary_nonce = None
        if is_canary_transfer and transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"):
            active_canary_nonce = transfer.provenance_policy.split("canary:", 1)[1]

        res = await sender.send_message(
            message=transfer.message_text,
            confirmation_token=req.confirmation_token,
            transfer_id=req.transfer_id,
            baseline_hash=transfer.baseline_hash,
            page_override=req.page_override,
            config_fingerprint=current_fp,
            is_canary=is_canary_transfer,
            nonce=active_canary_nonce,
        )

        transfer.status = TransferStatus.DELIVERED.value
        transfer.sent_at = datetime.utcnow()
        transfer.delivery_evidence = res.get("delivery_evidence")
        if res.get("raw_content_hash"):
            transfer.raw_content_hash = res.get("raw_content_hash")
        if res.get("semantic_content_hash"):
            transfer.semantic_content_hash = res.get("semantic_content_hash")
        await db.save_transfer_record(transfer)

        if state_machine.can_transition_to(ShuttleStage.CHATGPT_WAITING):
            await state_machine.transition_to(
                ShuttleStage.CHATGPT_WAITING,
                iteration_id=transfer.iteration_id,
                content_hash=transfer.content_hash,
                transfer_status="delivered",
            )

        return {"status": "delivered", "transfer_id": req.transfer_id, "result": res}
    except DeliveryOutcomeUnknownError as exc:
        err = str(exc)
        transfer.status = TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value
        transfer.error_message = err
        await db.save_transfer_record(transfer)
        if state_machine.can_transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN):
            await state_machine.transition_to(
                ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                failure_details=err,
                transfer_status="unknown",
            )
        else:
            await state_machine.transition_to(
                ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                failure_details=err,
                transfer_status="unknown",
                force=True,
            )
        raise HTTPException(status_code=502, detail=f"Transfer delivery outcome unknown: {err}")
    except NotSentError as exc:
        err = str(exc)
        transfer.status = TransferStatus.NOT_SENT.value
        transfer.error_message = f"NOT_SENT: {err}"
        await db.save_transfer_record(transfer)
        if transfer.confirmation_token:
            await db.invalidate_transfer_authorization(transfer.confirmation_token)
        if state_machine.can_transition_to(ShuttleStage.ERROR):
            await state_machine.transition_to(
                ShuttleStage.ERROR,
                failure_details=f"NOT_SENT: {err}",
                transfer_status="not_sent",
            )
        else:
            await state_machine.transition_to(
                ShuttleStage.ERROR,
                failure_details=f"NOT_SENT: {err}",
                transfer_status="not_sent",
                force=True,
            )
        raise HTTPException(status_code=500, detail=f"Transfer not sent: {err}")
    except Exception as exc:
        err = str(exc)
        if "delivery outcome unknown" in err.lower():
            transfer.status = TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value
            transfer.error_message = err
            await db.save_transfer_record(transfer)
            if state_machine.can_transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN):
                await state_machine.transition_to(
                    ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                    failure_details=err,
                    transfer_status="unknown",
                )
            raise HTTPException(status_code=502, detail=f"Transfer delivery outcome unknown: {err}")

        transfer.status = TransferStatus.FAILED.value
        transfer.error_message = err
        await db.save_transfer_record(transfer)
        if state_machine.can_transition_to(ShuttleStage.ERROR):
            await state_machine.transition_to(
                ShuttleStage.ERROR,
                failure_details=err,
                transfer_status="failed",
            )
        raise HTTPException(status_code=500, detail=f"Transfer failed: {err}")


class ObserveChatGPTResponseRequest(BaseModel):
    page_data: Optional[Any] = None
    baseline_hash: Optional[str] = None


@app.post("/api/transfer/chatgpt/observe")
async def observe_chatgpt_response(req: ObserveChatGPTResponseRequest):
    cfg = await db.get_config()
    if cfg and cfg.canary_mode:
        if is_production_conversation(cfg.chatgpt.conversation_url):
            raise HTTPException(status_code=400, detail="Production ChatGPT targets are forbidden in canary mode")
        if req.page_data is not None:
            raise HTTPException(status_code=400, detail="Canary live observation overrides forbidden")
        if req.page_data is None:
            request, nonce = await current_canary_request()
            engine = real_chatgpt_observer or RealChatGPTBrowserObserver(
                target_url=cfg.chatgpt.conversation_url,
                cdp_url=cfg.chatgpt.cdp_url,
                prompt_extractor=StandardPromptExtractor(expected_canary_nonce=nonce)
            )
            if hasattr(engine, "extractor"):
                engine.extractor = StandardPromptExtractor(expected_canary_nonce=nonce)
            page = await engine.read_live_page()
            obs = engine.inspect_dom_payload(page)
            user = obs.get("latest_user_turn") or {}
            assistant = obs.get("latest_assistant_turn") or {}
            user_text = (user.get("text") or "").strip()
            expected_text = (request.message_text or "").strip()
            user_matches = (
                user_text == expected_text
                or user_text == expected_text.replace("```markdown\n", "").replace("\n```", "").strip()
                or (f"Current nonce: {nonce}" in user_text and canary_prompt(nonce) in user_text)
            )
            if (not obs.get("transfer_eligible") or not user_matches
                    or assistant.get("turn_index", -1) <= user.get("turn_index", -1)):
                raise HTTPException(status_code=400, detail="Current canary reply missing, historical, or nonce/marked prompt mismatch")
            ext = PromptExtractionRecord(source_conversation_id=cfg.chatgpt.conversation_id,
                        source_message_id=assistant.get("message_id"), content_hash=obs["content_hash"],
                        extracted_prompt_hash=obs["extracted_prompt_hash"],
                        extracted_prompt_preview=obs["extracted_prompt"],
                        provenance_policy=f"canary:{nonce}", is_fresh=True, transfer_eligible=True)
            await db.record_prompt_extraction(ext)
            if state_machine.can_transition_to(ShuttleStage.CHATGPT_RESPONSE_READY):
                await state_machine.transition_to(ShuttleStage.CHATGPT_RESPONSE_READY)
            return {"status": "ready", "observation": obs, "extraction": ext.model_dump(mode="json")}
    obs_engine = real_chatgpt_observer or chatgpt_observer
    if req.baseline_hash and hasattr(obs_engine, "set_baseline"):
        obs_engine.set_baseline(content_hash=req.baseline_hash)

    if req.page_data is not None and hasattr(obs_engine, "inspect_dom_payload"):
        obs = obs_engine.inspect_dom_payload(req.page_data)
    elif hasattr(obs_engine, "get_latest_assistant_response"):
        resp = await obs_engine.get_latest_assistant_response()
        if resp and hasattr(obs_engine, "inspect_dom_payload"):
            obs = obs_engine.inspect_dom_payload(resp)
        else:
            obs = {
                "is_valid": bool(resp),
                "is_completed": True,
                "is_streaming": False,
                "has_error_placeholder": False,
                "raw_text": resp.get("text") if resp else None,
                "content_hash": resp.get("content_hash") if resp else None,
                "extracted_prompt": resp.get("extracted_prompt") if resp else None,
                "extracted_prompt_hash": resp.get("extracted_prompt_hash") if resp else None,
                "transfer_eligible": resp.get("transfer_eligible", False) if resp else False,
            }
    else:
        obs = {"is_valid": False, "ineligibility_reason": "No observer available"}

    cfg = await db.get_config()
    conv_id = cfg.chatgpt.conversation_id if cfg else "chatgpt"

    # Persist observation to SQLite
    obs_rec = ObservationRecord(
        source_type="chatgpt",
        source_identifier=cfg.chatgpt.conversation_url if cfg else "chatgpt",
        content_hash=obs.get("content_hash") or "",
        payload_preview=(obs.get("raw_text") or "")[:200] if obs.get("raw_text") else None,
        status="verified" if (obs.get("is_valid") and obs.get("is_completed")) else ("in_progress" if obs.get("is_streaming") else "unverified"),
        error_details=obs.get("ineligibility_reason"),
    )
    await db.record_observation(obs_rec)

    # Persist extraction if present
    ext_rec = None
    if obs.get("extracted_prompt"):
        ext_rec = PromptExtractionRecord(
            source_conversation_id=conv_id or "chatgpt",
            content_hash=obs.get("content_hash") or "",
            extracted_prompt_hash=obs.get("extracted_prompt_hash") or compute_sha256(obs["extracted_prompt"]),
            extracted_prompt_preview=obs["extracted_prompt"],
            provenance_policy=obs.get("provenance_policy") or "chatgpt_observer",
            is_fresh=not obs.get("is_stale_assistant_reply", False),
            transfer_eligible=obs.get("transfer_eligible", False),
            ineligibility_reason=obs.get("ineligibility_reason"),
        )
        await db.record_prompt_extraction(ext_rec)

    if obs.get("is_valid") and obs.get("is_completed") and obs.get("transfer_eligible"):
        if state_machine.can_transition_to(ShuttleStage.CHATGPT_RESPONSE_READY):
            await state_machine.transition_to(
                ShuttleStage.CHATGPT_RESPONSE_READY,
                content_hash=obs.get("content_hash"),
                payload_preview=(obs.get("raw_text") or "")[:200],
            )

    return {
        "status": "ready" if obs.get("is_valid") and obs.get("is_completed") else "pending_or_ineligible",
        "observation": obs,
        "extraction": ext_rec.model_dump(mode="json") if ext_rec else None,
    }


class PrepareCodexPromptRequest(BaseModel):
    source_extraction_id: Optional[int] = None
    prompt_text: Optional[str] = None
    is_operator_modified: bool = False
    iteration_id: int = 1


@app.post("/api/transfer/codex_prompt/prepare")
@serialize_transfer
async def prepare_codex_prompt(req: PrepareCodexPromptRequest):
    cfg = await db.get_config()
    dest_thread = cfg.codex.thread_id if cfg else "codex"

    effective_text = None
    is_modified = req.is_operator_modified

    canary_source = None
    if cfg and cfg.canary_mode:
        await require_runtime_transfer("chatgpt_to_codex")
        if is_production_target(thread_id=dest_thread, repo_path=cfg.codex.repo_path):
            raise HTTPException(status_code=400, detail="Production targets are forbidden in canary mode")
        request, nonce = await current_canary_request()
        canary_source = await db.get_latest_prompt_extraction(cfg.chatgpt.conversation_id)
        latest_g2c = await db.get_latest_transfer(TransferDirection.CHATGPT_TO_CODEX.value)
        if latest_g2c and (
            latest_g2c.provenance_policy == f"canary:{nonce}"
            or (request.sent_at and latest_g2c.created_at >= request.sent_at)
            or latest_g2c.status in (TransferStatus.PREPARED.value, TransferStatus.SENDING.value)
        ):
            raise HTTPException(status_code=409, detail="Canary Codex task already prepared; no reauthorization or automatic retry")
        if (req.prompt_text is not None or req.is_operator_modified or not canary_source
                or not canary_source.is_fresh or not canary_source.transfer_eligible
                or canary_source.provenance_policy != f"canary:{nonce}"
                or canary_source.extracted_prompt_preview != canary_prompt(nonce)
                or not request.sent_at or canary_source.created_at <= request.sent_at
                or (req.source_extraction_id is not None and req.source_extraction_id != canary_source.id)):
            raise HTTPException(status_code=400, detail="No fresh current canary extraction; overrides and production fallback forbidden")
    else:
        latest_ext = await db.get_latest_prompt_extraction()
        if latest_ext and latest_ext.provenance_policy and latest_ext.provenance_policy.startswith("canary:"):
            raise HTTPException(status_code=400, detail="Canary extraction cannot be prepared under production configuration")

    if req.prompt_text and req.prompt_text.strip():
        effective_text = req.prompt_text.strip()
        is_modified = True
    else:
        ext = canary_source or await db.get_latest_prompt_extraction()
        if not ext:
            raise HTTPException(status_code=400, detail="No prompt extraction record found")
        if not ext.transfer_eligible:
            raise HTTPException(status_code=400, detail=f"Prompt is not transfer-eligible: {ext.ineligibility_reason}")
        effective_text = ext.extracted_prompt_preview

    if not effective_text:
        raise HTTPException(status_code=400, detail="Prompt text cannot be empty")

    content_hash = compute_sha256(effective_text)

    # Duplicate check
    is_dup, dup_msg = await state_machine.check_duplicate(effective_text)
    if is_dup:
        raise HTTPException(status_code=409, detail=dup_msg)

    transfer_id = f"transfer-g2c-{uuid.uuid4().hex[:12]}"
    confirmation_token = f"token-g2c-{uuid.uuid4().hex[:16]}"

    config_fingerprint = compute_config_fingerprint(cfg)

    auth = TransferAuthorization(
        transfer_id=transfer_id,
        confirmation_token=confirmation_token,
        destination_identifier=dest_thread,
        expected_content_hash=content_hash,
        consumed=False,
        expires_at=datetime.utcnow() + timedelta(minutes=15),
        config_fingerprint=config_fingerprint,
    )

    transfer_rec = TransferRecord(
        transfer_id=transfer_id,
        iteration_id=req.iteration_id,
        direction=TransferDirection.CHATGPT_TO_CODEX.value,
        source_type="canary" if (canary_source and canary_source.provenance_policy.startswith("canary:")) else "chatgpt",
        source_identifier=cfg.chatgpt.conversation_url if cfg else "chatgpt",
        source_message_id=canary_source.source_message_id if canary_source else None,
        destination_type="codex",
        destination_identifier=dest_thread,
        content_hash=content_hash,
        char_count=len(effective_text),
        message_text=effective_text,
        payload_preview=effective_text[:200],
        status=TransferStatus.PREPARED.value,
        human_confirmed=False,
        confirmation_token=confirmation_token,
        is_operator_modified=is_modified,
        provenance_policy=canary_source.provenance_policy if canary_source else (
            "chatgpt_extracted_prompt" if not is_modified else "operator_modified_prompt"),
        config_fingerprint=config_fingerprint,
    )

    try:
        await state_machine.prepare_transfer_atomic(
            transfer=transfer_rec,
            auth=auth,
            target_stage=ShuttleStage.PROMPT_PREVIEW,
        )
    except InvalidTransitionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return {
        "status": "prepared",
        "transfer_id": transfer_id,
        "confirmation_token": confirmation_token,
        "prompt_text": effective_text,
        "content_hash": content_hash,
        "char_count": len(effective_text),
        "is_operator_modified": is_modified,
        "destination_thread_id": dest_thread,
        "destination": dest_thread,
        "destination_identifier": dest_thread,
        "execution_mode": "CANARY" if ((cfg and cfg.canary_mode) or (canary_source and canary_source.provenance_policy.startswith("canary:"))) else "PRODUCTION",
        "config_fingerprint": config_fingerprint,
        "config_source": os.environ.get("SHUTTLE_CONFIG") or "database/default",
        "destination_workspace": cfg.codex.repo_path if cfg else None,
        "source_conversation_url": cfg.chatgpt.conversation_url if cfg else None,
        "source_conversation_id": cfg.chatgpt.conversation_id if cfg else None,
    }


class SubmitCodexQueueRequest(BaseModel):
    transfer_id: str
    confirmation_token: str
    thread_id: Optional[str] = None
    repo_path: Optional[str] = None


@app.post("/api/transfer/codex_queue/submit")
@serialize_transfer
async def submit_codex_queue(req: SubmitCodexQueueRequest):
    if state_machine.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN:
        raise HTTPException(status_code=409, detail="Unknown delivery requires manual reconciliation; no retry")
    transfer = await db.get_transfer_by_id(req.transfer_id)
    if not transfer:
        raise HTTPException(status_code=404, detail=f"Transfer record '{req.transfer_id}' not found")

    if transfer.status not in (TransferStatus.PREPARED.value, "prepared"):
        raise HTTPException(
            status_code=400,
            detail=f"Transfer '{req.transfer_id}' is in status '{transfer.status}', cannot submit"
        )

    cfg = await db.get_config()
    current_fp = compute_config_fingerprint(cfg)
    if transfer.config_fingerprint and transfer.config_fingerprint != current_fp:
        raise HTTPException(status_code=400, detail="Configuration has changed since transfer was prepared; authorization invalidated")

    target_thread = req.thread_id or transfer.destination_identifier or (cfg.codex.thread_id if cfg else None)
    target_repo = req.repo_path or (cfg.codex.repo_path if cfg else None)

    # Stale/mismatched selection check: Retargeting an authorized transfer is strictly forbidden
    if req.thread_id and req.thread_id.strip().lower() != transfer.destination_identifier.strip().lower():
        raise HTTPException(
            status_code=400,
            detail=f"Target thread mismatch: transfer is bound to {transfer.destination_identifier}, but request specified {req.thread_id}. Retargeting an authorized transfer is forbidden; prepare a new transfer."
        )

    if target_thread.strip().lower() != transfer.destination_identifier.strip().lower():
        raise HTTPException(
            status_code=400,
            detail=f"Target thread mismatch: transfer is bound to {transfer.destination_identifier}, got {target_thread}"
        )

    if not target_thread:
        raise HTTPException(status_code=400, detail="Target thread ID is required for Codex queue")

    # Duplicate retry check: prohibit queue submission if matching item already in queue
    try:
        from app.adapters import inspect_codex_queue_storage
        m_nonce = None
        if transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"):
            m_nonce = transfer.provenance_policy.split("canary:", 1)[1]
        q_info = inspect_codex_queue_storage(target_thread, nonce=m_nonce, expected_content_hash=transfer.content_hash)
        if q_info.get("has_matching_item"):
            raise HTTPException(
                status_code=400,
                detail="Duplicate retry prohibited: matching item is already present in Codex queue storage",
            )
    except HTTPException:
        raise
    except Exception:
        pass

    is_canary_transfer = (
        transfer.source_type == "canary"
        or (transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"))
        or (cfg and cfg.canary_mode)
    )

    queue_baseline = None
    if is_canary_transfer:
        if cfg and is_production_target(thread_id=cfg.codex.thread_id, conv_id_or_url=cfg.chatgpt.conversation_url, repo_path=cfg.codex.repo_path):
            raise HTTPException(status_code=400, detail="Canary transfer cannot execute under production configuration")
        if is_production_thread(target_thread) or is_production_workspace(target_repo):
            raise HTTPException(status_code=400, detail="Production Codex thread and workspace are strictly forbidden in canary mode")
        if target_thread.lower() != cfg.codex.thread_id.lower():
            raise HTTPException(status_code=400, detail="Queue destination differs from verified canary thread")
        if not same_path(target_repo, cfg.codex.repo_path):
            raise HTTPException(status_code=400, detail="Queue workspace differs from verified canary workspace")
        if cfg.canary_mode:
            await require_runtime_transfer("chatgpt_to_codex", transfer, req.confirmation_token)
            if real_codex_queue is None:
                raise HTTPException(status_code=409, detail="Canary queue adapter unavailable; no mock fallback")
        if transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"):
            try:
                _, nonce = await current_canary_request()
                if transfer.message_text != canary_prompt(nonce) or transfer.provenance_policy != f"canary:{nonce}":
                    raise HTTPException(status_code=400, detail="Canary queue target/workspace/prompt mismatch; no fallback")
            except HTTPException:
                raise
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
        try:
            if codex_observer:
                queue_baseline = validate_canary_session(cfg, codex_observer)["total_lines"]
                transfer.delivery_evidence = {"baseline_line": queue_baseline, "task_completion_status": "pending"}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Canary session verification failed: {exc}")

    if state_machine.can_transition_to(ShuttleStage.CODEX_QUEUE_PENDING):
        await state_machine.transition_to(
            ShuttleStage.CODEX_QUEUE_PENDING,
            iteration_id=transfer.iteration_id,
            content_hash=transfer.content_hash,
            payload_preview=transfer.payload_preview,
            transfer_status="sending",
        )

    transfer.status = TransferStatus.SENDING.value
    transfer.human_confirmed = True
    transfer.confirmed_at = datetime.utcnow()
    await db.save_transfer_record(transfer)

    queue_adapter = real_codex_queue or codex_queue
    try:
        if getattr(queue_adapter, "db", None) is None:
            ok, err = await db.verify_and_consume_authorization(
                transfer_id=req.transfer_id,
                token=req.confirmation_token,
                content_hash=transfer.content_hash,
                destination=target_thread,
                expected_config_fingerprint=current_fp,
            )
            if not ok:
                raise ValueError(f"Transfer authorization rejected: {err}")

        res = await queue_adapter.queue_message(
            thread_id=target_thread,
            message=transfer.message_text,
            confirmation_token=req.confirmation_token,
            transfer_id=req.transfer_id,
            repo_path=target_repo,
            config_fingerprint=current_fp,
            is_canary=is_canary_transfer,
        )

        if cfg and cfg.canary_mode:
            if (res.get("status") != "queued" or not res.get("receipt_id")
                    or res.get("thread_id") != target_thread
                    or (res.get("content_hash") and res.get("content_hash") != transfer.content_hash)):
                raise DeliveryOutcomeUnknownError("Uncorrelated canary queue receipt")
            res = {
                **res,
                "baseline_line": queue_baseline,
                "queue_receipt_status": res.get("queue_receipt_status", "accepted"),
                "turn_dispatch_status": res.get("turn_dispatch_status", "pending"),
                "task_completion_status": res.get("task_completion_status", "pending"),
                "outcome_classification": res.get("outcome_classification", CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value),
                "task_completed": False,
            }

        transfer.status = TransferStatus.DELIVERED.value
        transfer.sent_at = datetime.utcnow()
        transfer.delivery_evidence = res
        await db.save_transfer_record(transfer)

        if state_machine.can_transition_to(ShuttleStage.CODEX_WAITING):
            await state_machine.transition_to(
                ShuttleStage.CODEX_WAITING,
                iteration_id=transfer.iteration_id,
                content_hash=transfer.content_hash,
                transfer_status="delivered",
            )

        return {
            "status": "delivered",
            "queue_receipt_status": res.get("queue_receipt_status", "accepted"),
            "turn_dispatch_status": res.get("turn_dispatch_status", "pending"),
            "task_completion_status": res.get("task_completion_status", "pending"),
            "outcome_classification": res.get("outcome_classification", CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value),
            "receipt_id": res.get("receipt_id"),
            "client_user_message_id": res.get("client_user_message_id"),
            "transfer_id": req.transfer_id,
            "result": res,
        }
    except (DeliveryOutcomeUnknownError, TimeoutError, ConnectionError) as exc:
        err = str(exc)
        evidence = transfer.delivery_evidence or {}
        evidence.update({
            "queue_receipt_status": "unknown",
            "turn_dispatch_status": "unknown",
            "task_completion_status": evidence.get("task_completion_status", "pending"),
            "outcome_classification": CodexDeliveryClassification.CODEX_DELIVERY_OUTCOME_UNKNOWN.value,
            "error": err,
        })
        transfer.status = TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value
        transfer.error_message = err
        transfer.delivery_evidence = evidence
        await db.save_transfer_record(transfer)
        if state_machine.can_transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN):
            await state_machine.transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                failure_details=err, transfer_status="unknown")
        else:
            await state_machine.transition_to(ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
                failure_details=err, transfer_status="unknown", force=True)
        raise HTTPException(status_code=502, detail=f"Codex delivery outcome unknown; do not retry: {err}")
    except ValueError as exc:
        err = str(exc)
        transfer.status = TransferStatus.FAILED.value
        transfer.error_message = err
        await db.save_transfer_record(transfer)
        raise HTTPException(status_code=400, detail=err)
    except Exception as exc:
        err = str(exc)
        transfer.status = TransferStatus.FAILED.value
        transfer.error_message = err
        await db.save_transfer_record(transfer)
        if state_machine.can_transition_to(ShuttleStage.ERROR):
            await state_machine.transition_to(
                ShuttleStage.ERROR,
                failure_details=err,
                transfer_status="failed",
            )
        raise HTTPException(status_code=500, detail=f"Codex queue failed: {err}")


@app.get("/api/canary/bootstrap/status")
async def canary_bootstrap_status():
    snapshot = await refresh_runtime_snapshot(create_config=False)
    return {**snapshot["bootstrap"], "runtime_snapshot": snapshot}


@app.post("/api/canary/bootstrap/recheck")
async def canary_bootstrap_recheck():
    snapshot = await refresh_runtime_snapshot(create_config=True)
    return {**snapshot["bootstrap"], "runtime_snapshot": snapshot}


async def refresh_runtime_snapshot(create_config=False):
    """One serialized read-only observation cycle for every dashboard panel."""
    from app.canary_bootstrap import run_canary_bootstrap
    async with runtime_lock:
        cfg = await db.get_config()
        source = (runtime_identity or {}).get("config_source") or os.environ.get("SHUTTLE_CONFIG")
        kwargs = {"active_config_path": Path(source)} if cfg and cfg.canary_mode and source else {}
        try:
            res = await run_canary_bootstrap(auto_launch_edge=False, create_config=create_config, **kwargs)
        except (ValueError, OSError) as exc:
            res = {"verdict": "RUNTIME_RESTART_REQUIRED", "stage": "runtime_restart_required",
                   "blocker": str(exc), "error_code": "RUNTIME_RESTART_REQUIRED"}
        await db.save_canary_bootstrap_state(res)
        async with transfer_lock:
            ready = await runtime_readiness(probe=True, repair=True)
            guard = await get_transfer_target_info()
            if cfg and cfg.canary_mode:
                if ready.get("status") in ("DELIVERY_OUTCOME_UNKNOWN", "RUNTIME_RESTART_REQUIRED", "CANARY_RUNTIME_REQUIRED", "TRANSFER_STATE_INCONSISTENT"):
                    guard["blocking_reason"] = guard.get("blocking_reason") or ready.get("blocker")
                elif not guard.get("blocking_reason"):
                    guard["blocking_reason"] = None
            reconciliation = await get_reconciliation_summary()
            if cfg and cfg.canary_mode:
                reconciliation["readiness"] = ready
            return {"observed_at": datetime.utcnow().isoformat(), "bootstrap": res,
                    "readiness": ready, "target_info": guard, "state": await get_state(),
                    "reconciliation": reconciliation}


@app.get("/api/runtime/refresh")
async def refresh_runtime():
    return await refresh_runtime_snapshot()


@app.get("/api/canary/preflight")
async def canary_preflight():
    cfg = await db.get_config()
    if not cfg or not cfg.canary_mode:
        raise HTTPException(status_code=400, detail="Explicit canary configuration required")
    if is_production_target(thread_id=cfg.codex.thread_id, conv_id_or_url=cfg.chatgpt.conversation_url, repo_path=cfg.codex.repo_path):
        raise HTTPException(status_code=400, detail="Production targets are forbidden in canary configuration")
    return await require_canary_targets(cfg)


@app.get("/api/canary/completion/{transfer_id}")
async def canary_completion(transfer_id: str):
    cfg = await db.get_config()
    if not cfg or not cfg.canary_mode:
        raise HTTPException(status_code=400, detail="Explicit canary configuration required")
    transfer = await db.get_transfer_by_id(transfer_id)
    if (not transfer or transfer.direction != TransferDirection.CHATGPT_TO_CODEX.value
            or transfer.destination_identifier != cfg.codex.thread_id):
        raise HTTPException(status_code=400, detail="Not the intended canary task/thread")
    nonce = canary_nonce(transfer)
    evidence = transfer.delivery_evidence or {}
    if transfer.status != TransferStatus.DELIVERED.value or "baseline_line" not in evidence:
        raise HTTPException(status_code=400, detail="No independent accepted queue receipt/boundary; reconcile manually")
    validate_canary_session(cfg, codex_observer)
    obs = codex_observer.observe_thread(cfg.codex.thread_id, expected_cwd=cfg.codex.repo_path,
                                       after_line=evidence["baseline_line"])
    completed = (obs["metadata_valid"] and obs["is_completed"] and obs.get("completion_proven")
                 and obs.get("latest_user_prompt") == transfer.message_text == canary_prompt(nonce)
                 and (obs.get("latest_assistant_response") or "").strip() == f"CANARY-OK:{nonce}"
                 and obs.get("latest_assistant_turn_id") == obs.get("latest_user_turn_id")
                 and bool(obs.get("latest_user_turn_id"))
                 and obs.get("latest_assistant_line", -1) > (obs.get("latest_user_line") if obs.get("latest_user_line") is not None else -1))
    if completed:
        evidence["task_completion_status"] = "completed"
        evidence["outcome_classification"] = CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
        evidence["completion"] = {"thread_id": cfg.codex.thread_id, "transfer_id": transfer_id,
            "nonce": nonce, "turn_id": obs["latest_assistant_turn_id"],
            "line": obs["latest_assistant_line"], "response_hash": obs["latest_response_hash"]}
        transfer.delivery_evidence = evidence
        transfer.verified_at = datetime.utcnow()
        await db.save_transfer_record(transfer)
        if state_machine.can_transition_to(ShuttleStage.IDLE):
            await state_machine.transition_to(
                ShuttleStage.IDLE,
                iteration_id=transfer.iteration_id,
                content_hash=transfer.content_hash,
                transfer_status="reconciled_delivered",
            )
        else:
            await state_machine.transition_to(
                ShuttleStage.IDLE,
                iteration_id=transfer.iteration_id,
                content_hash=transfer.content_hash,
                transfer_status="reconciled_delivered",
                force=True,
            )
    return {"transfer_id": transfer_id, "queue_receipt_status": "accepted",
            "task_completion_status": "completed" if completed else "pending_or_unverified",
            "completion": evidence.get("completion") if completed else None}


class ReconcileUnknownRequest(BaseModel):
    transfer_id: str
    resolution: str  # 'mark_delivered' or 'mark_cancelled'
    notes: Optional[str] = None
    delivery_evidence: Optional[Dict[str, Any]] = None


@app.post("/api/transfer/reconcile_unknown")
async def reconcile_unknown_transfer(req: ReconcileUnknownRequest):
    if req.resolution not in ("mark_delivered", "mark_cancelled"):
        raise HTTPException(status_code=400, detail=f"Unsupported resolution '{req.resolution}'")
    try:
        delivery_evidence_update = dict(req.delivery_evidence) if req.delivery_evidence else {}
        if req.resolution == "mark_delivered":
            t_rec = await db.get_transfer_by_id(req.transfer_id)
            if t_rec and t_rec.direction == TransferDirection.CODEX_TO_CHATGPT.value:
                delivery_evidence_update.setdefault("raw_content_hash", t_rec.content_hash)
                sem_hash = getattr(t_rec, "semantic_content_hash", None)
                if not sem_hash and t_rec.message_text:
                    from app.canonical import compute_semantic_hash
                    sem_hash = compute_semantic_hash(t_rec.message_text)
                if sem_hash:
                    delivery_evidence_update.setdefault("semantic_content_hash", sem_hash)
                delivery_evidence_update.setdefault("semantic_delivery_status", "SEMANTIC_DELIVERY_CONFIRMED")
                delivery_evidence_update.setdefault("matching_turn_count", 1)
                delivery_evidence_update.setdefault("audit_trail", "delivery initially ambiguous -> independently verified delivered")

                browser_obs = real_chatgpt_observer or chatgpt_observer
                if hasattr(browser_obs, "classify_live_delivery") and t_rec.message_text:
                    nonce = None
                    if t_rec.provenance_policy and t_rec.provenance_policy.startswith("canary:"):
                        nonce = t_rec.provenance_policy.split("canary:", 1)[1]
                    try:
                        eval_res = await browser_obs.classify_live_delivery(
                            expected_message=t_rec.message_text,
                            nonce=nonce,
                        )
                        if eval_res.get("verified"):
                            delivery_evidence_update.setdefault("user_turn_id", eval_res.get("user_turn_id"))
                            delivery_evidence_update.setdefault("user_message_id", eval_res.get("user_message_id"))
                            delivery_evidence_update.setdefault("assistant_turn_id", eval_res.get("assistant_turn_id"))
                            delivery_evidence_update.setdefault("assistant_message_id", eval_res.get("assistant_message_id"))
                            delivery_evidence_update.setdefault("layout", eval_res.get("layout"))
                    except Exception:
                        pass
            elif t_rec and t_rec.direction == TransferDirection.CHATGPT_TO_CODEX.value and codex_observer:
                nonce = None
                if t_rec.provenance_policy and t_rec.provenance_policy.startswith("canary:"):
                    nonce = t_rec.provenance_policy.split("canary:", 1)[1]
                if nonce:
                    ev = t_rec.delivery_evidence or {}
                    baseline = ev.get("baseline_line", 0)
                    try:
                        live_eval = codex_observer.classify_delivery(
                            thread_id=t_rec.destination_identifier,
                            nonce=nonce,
                            after_line=baseline,
                            queue_receipt_accepted=True,
                        )
                        if live_eval.get("canary_ok_verified"):
                            delivery_evidence_update.setdefault("queue_receipt_status", "accepted")
                            delivery_evidence_update.setdefault("turn_dispatch_status", "dispatched")
                            delivery_evidence_update.setdefault("task_completion_status", "completed")
                            delivery_evidence_update.setdefault("outcome_classification", CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value)
                            delivery_evidence_update.setdefault("turn_id", live_eval.get("turn_id"))
                            delivery_evidence_update.setdefault("user_message_id", live_eval.get("user_message_id"))
                            delivery_evidence_update.setdefault("assistant_message_id", live_eval.get("assistant_message_id"))
                            delivery_evidence_update.setdefault("assistant_reply_text", live_eval.get("assistant_reply_text"))
                            delivery_evidence_update.setdefault("user_timestamp", live_eval.get("user_timestamp"))
                            delivery_evidence_update.setdefault("assistant_timestamp", live_eval.get("assistant_timestamp"))
                            delivery_evidence_update.setdefault("user_content_hash", live_eval.get("user_content_hash"))
                            delivery_evidence_update.setdefault("assistant_content_hash", live_eval.get("assistant_content_hash"))
                            delivery_evidence_update.setdefault("canary_ok_verified", True)
                    except Exception:
                        pass

        transfer, new_checkpoint = await db.reconcile_unknown_outcome_transaction(
            transfer_id=req.transfer_id,
            resolution=req.resolution,
            notes=req.notes,
            delivery_evidence_update=delivery_evidence_update if delivery_evidence_update else None,
        )
        state_machine.current_checkpoint = new_checkpoint
        return {
            "status": "reconciled",
            "transfer_id": req.transfer_id,
            "new_stage": new_checkpoint.stage.value,
            "queue_receipt_status": transfer.queue_receipt_status,
            "turn_dispatch_status": transfer.turn_dispatch_status,
            "task_completion_status": transfer.task_completion_status,
            "outcome_classification": transfer.outcome_classification,
        }
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Reconciliation transaction failed: {exc}")


@app.get("/api/transfers")
async def list_transfers(limit: int = 50):
    records = await db.get_all_transfers(limit=limit)
    return {"transfers": [r.model_dump(mode="json") for r in records]}


@app.get("/api/transfers/{transfer_id}")
async def get_transfer_record(transfer_id: str):
    record = await db.get_transfer_by_id(transfer_id)
    if not record:
        raise HTTPException(status_code=404, detail="Transfer record not found")
    return {"transfer": record.model_dump(mode="json")}


@app.get("/api/authorizations/{token}")
async def get_authorization_record(token: str):
    auth = await db.get_transfer_authorization(token)
    if not auth:
        raise HTTPException(status_code=404, detail="Authorization token not found")
    return {"authorization": auth.model_dump(mode="json")}


class InitCanaryRoundRequest(BaseModel):
    round_number: int
    nonce: Optional[str] = None
    manual_thread_load_required: bool = False


class UpdateCanaryRoundRequest(BaseModel):
    round_id: str
    result: Optional[str] = None
    c2g_transfer_id: Optional[str] = None
    chatgpt_user_turn_id: Optional[str] = None
    chatgpt_assistant_turn_id: Optional[str] = None
    g2c_transfer_id: Optional[str] = None
    codex_queue_receipt_id: Optional[str] = None
    codex_user_turn_id: Optional[str] = None
    codex_assistant_message_id: Optional[str] = None
    manual_thread_load_required: Optional[bool] = None
    metrics: Optional[Dict[str, Any]] = None
    error_details: Optional[str] = None
    completed_at: Optional[datetime] = None


@app.get("/api/canary/three_round/readiness")
async def three_round_readiness():
    cfg = await db.get_config()
    ready = await runtime_readiness(probe=True)
    rounds = await db.get_canary_rounds()
    completed_rounds = [r for r in rounds if r.get("result") == CanaryRoundResult.ROUND_COMPLETED.value]
    recovered_rounds = [r for r in rounds if r.get("result") == CanaryRoundResult.ROUND_RECOVERED_AFTER_FALSE_UNKNOWN.value or r.get("is_recovered")]
    failed_unrecovered_rounds = [
        r for r in rounds
        if r.get("result") in (CanaryRoundResult.ROUND_FAILED_VALIDATION.value, CanaryRoundResult.ROUND_DELIVERY_UNKNOWN.value)
        and not r.get("is_recovered")
    ]

    if len(completed_rounds) >= 3:
        status = "THREE_ROUND_CANARY_STABILITY_VERIFIED"
        blocker = None
    elif failed_unrecovered_rounds:
        status = "THREE_ROUND_CANARY_STABILITY_BLOCKED"
        blocker = "One or more canary rounds failed validation"
    elif ready["status"] in ("CANARY_END_TO_END_VERIFIED", "THREE_ROUND_CANARY_READY", "CANARY_READY_FOR_C2G_PREPARE") and not ready.get("blocker"):
        status = "THREE_ROUND_CANARY_READY"
        blocker = None
    else:
        status = "THREE_ROUND_CANARY_STABILITY_BLOCKED"
        blocker = ready.get("blocker") or ready.get("status")

    return {
        "status": status,
        "blocker": blocker,
        "rounds_completed": len(completed_rounds),
        "rounds_recovered": len(recovered_rounds),
        "total_rounds_configured": 3,
        "rounds": rounds,
        "canary_safe": bool(ready.get("browser_status", {}).get("ready") and ready.get("codex_status", {}).get("verified_session_identity")),
        "readiness_summary": ready,
    }


@app.get("/api/canary/rounds")
async def get_canary_rounds_endpoint():
    rounds = await db.get_canary_rounds()
    completed = [r for r in rounds if r.get("result") == CanaryRoundResult.ROUND_COMPLETED.value]
    recovered = [r for r in rounds if r.get("result") == CanaryRoundResult.ROUND_RECOVERED_AFTER_FALSE_UNKNOWN.value or r.get("is_recovered")]
    failed_unrecovered = [
        r for r in rounds
        if r.get("result") in (CanaryRoundResult.ROUND_FAILED_VALIDATION.value, CanaryRoundResult.ROUND_DELIVERY_UNKNOWN.value)
        and not r.get("is_recovered")
    ]
    return {
        "rounds": rounds,
        "total_rounds": len(rounds),
        "completed_count": len(completed),
        "recovered_count": len(recovered),
        "stability_status": "THREE_ROUND_CANARY_STABILITY_VERIFIED" if len(completed) >= 3 else (
            "THREE_ROUND_CANARY_STABILITY_BLOCKED" if failed_unrecovered else "THREE_ROUND_CANARY_READY"
        ),
    }


@app.post("/api/canary/rounds/init")
async def init_canary_round_endpoint(req: InitCanaryRoundRequest):
    cfg = await db.get_config()
    if not cfg or not cfg.canary_mode:
        raise HTTPException(status_code=400, detail="Server is not in canary mode")

    existing_rounds = await db.get_canary_rounds()
    used_nonces = [r["nonce"] for r in existing_rounds]

    nonce = req.nonce or generate_canary_nonce()
    validate_round_invariants(
        nonce=nonce,
        target_thread=cfg.codex.thread_id,
        target_conv=cfg.chatgpt.conversation_url,
        target_repo=cfg.codex.repo_path,
        previous_nonces=used_nonces,
    )

    round_id = f"round-{req.round_number}"
    round_data = {
        "round_id": round_id,
        "round_number": req.round_number,
        "nonce": nonce,
        "result": "ROUND_IN_PROGRESS",
        "manual_thread_load_required": req.manual_thread_load_required,
        "started_at": datetime.utcnow().isoformat(),
    }
    saved = await db.save_canary_round(round_data)
    return saved


@app.post("/api/canary/rounds/update")
async def update_canary_round_endpoint(req: UpdateCanaryRoundRequest):
    updates = req.model_dump(exclude_unset=True)
    round_id = updates.pop("round_id")
    updated = await db.update_canary_round(round_id, updates)
    if not updated:
        raise HTTPException(status_code=404, detail=f"Round '{round_id}' not found")
    return updated
