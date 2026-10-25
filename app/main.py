"""
FastAPI Application and Local Control Plane Endpoints.
"""
from contextlib import asynccontextmanager
import json
from pathlib import Path
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.adapters import (
    MockChatGPTBrowserObserver,
    MockChatGPTMessageSender,
    MockCodexQueueAdapter,
    ReadOnlyCodexSessionObserver,
    StandardPromptExtractor,
)
from app.database import Database
from app.models import (
    AppConfig,
    CodexConfig,
    ChatGPTConfig,
    RouteConfig,
    ShuttleStage,
    compute_sha256,
)
from app.state_machine import InvalidTransitionError, ShuttleStateMachine

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "shuttle_console.db"
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"

db = Database(DB_PATH)
state_machine = ShuttleStateMachine(db)

# Adapter Instances
codex_observer: Optional[ReadOnlyCodexSessionObserver] = None
codex_queue = MockCodexQueueAdapter()
chatgpt_observer = MockChatGPTBrowserObserver()
chatgpt_sender = MockChatGPTMessageSender()
prompt_extractor = StandardPromptExtractor()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await db.init()
    config = await db.get_config()
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
    await state_machine.init()

    global codex_observer
    if config and config.codex.sessions_dir:
        codex_observer = ReadOnlyCodexSessionObserver(Path(config.codex.sessions_dir))

    yield
    # Shutdown logic if any


app = FastAPI(title="Local AI Shuttle Control Center", lifespan=lifespan)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------- HTML Pages


@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    config = await db.get_config()
    checkpoint = state_machine.current_checkpoint
    logs = await db.get_recent_logs(limit=30)
    return templates.TemplateResponse(
        "index.html",
        {
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


@app.get("/api/config")
async def get_configuration():
    config = await db.get_config()
    if not config:
        raise HTTPException(status_code=404, detail="Configuration not initialized")
    return config.model_dump(mode="json")


@app.post("/api/config")
async def update_configuration(config_data: AppConfig):
    await db.save_config(config_data)
    state_machine.config = config_data
    global codex_observer
    if config_data.codex.sessions_dir:
        codex_observer = ReadOnlyCodexSessionObserver(Path(config_data.codex.sessions_dir))

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
    target_thread = req.thread_id or (cfg.codex.thread_id if cfg else None)
    if not target_thread:
        raise HTTPException(status_code=400, detail="No thread_id specified or configured")

    if not codex_observer:
        raise HTTPException(status_code=500, detail="Codex observer not initialized")

    ans, line_idx, turn_id = await codex_observer.get_latest_response(
        target_thread, after_line=req.after_line
    )
    if ans:
        content_hash = compute_sha256(ans)
        # Advance checkpoint stage if valid
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
        }
    return {"status": "waiting", "message": "No new final_answer found for target thread"}


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
