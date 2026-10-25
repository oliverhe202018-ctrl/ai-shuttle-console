"""
Local AI Shuttle Control Center - Models and Schema Definitions.
"""
from datetime import datetime
from enum import Enum
import hashlib
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class ShuttleStage(str, Enum):
    IDLE = "IDLE"
    CODEX_WAITING = "CODEX_WAITING"
    CODEX_RESPONSE_READY = "CODEX_RESPONSE_READY"
    REPORT_PREVIEW = "REPORT_PREVIEW"
    CHATGPT_SEND_PENDING = "CHATGPT_SEND_PENDING"
    CHATGPT_WAITING = "CHATGPT_WAITING"
    CHATGPT_RESPONSE_READY = "CHATGPT_RESPONSE_READY"
    PROMPT_PREVIEW = "PROMPT_PREVIEW"
    CODEX_QUEUE_PENDING = "CODEX_QUEUE_PENDING"
    HUMAN_ACTION_REQUIRED = "HUMAN_ACTION_REQUIRED"
    DELIVERY_OUTCOME_UNKNOWN = "DELIVERY_OUTCOME_UNKNOWN"
    ERROR = "ERROR"


# State machine linear ordering / transition rules
# Normal progressive stages have strictly ordered ranks
STAGE_RANKS: Dict[ShuttleStage, int] = {
    ShuttleStage.IDLE: 0,
    ShuttleStage.CODEX_WAITING: 10,
    ShuttleStage.CODEX_RESPONSE_READY: 20,
    ShuttleStage.REPORT_PREVIEW: 30,
    ShuttleStage.CHATGPT_SEND_PENDING: 40,
    ShuttleStage.CHATGPT_WAITING: 50,
    ShuttleStage.CHATGPT_RESPONSE_READY: 60,
    ShuttleStage.PROMPT_PREVIEW: 70,
    ShuttleStage.CODEX_QUEUE_PENDING: 80,
    # Terminal/Exception states can be entered from any active stage
    ShuttleStage.HUMAN_ACTION_REQUIRED: 900,
    ShuttleStage.DELIVERY_OUTCOME_UNKNOWN: 950,
    ShuttleStage.ERROR: 999,
}

VALID_TRANSITIONS: Dict[ShuttleStage, List[ShuttleStage]] = {
    ShuttleStage.IDLE: [
        ShuttleStage.CODEX_WAITING,
        ShuttleStage.CODEX_RESPONSE_READY,
        ShuttleStage.REPORT_PREVIEW,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CODEX_WAITING: [
        ShuttleStage.CODEX_RESPONSE_READY,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CODEX_RESPONSE_READY: [
        ShuttleStage.REPORT_PREVIEW,
        ShuttleStage.CHATGPT_SEND_PENDING,
        ShuttleStage.ERROR
    ],
    ShuttleStage.REPORT_PREVIEW: [
        ShuttleStage.CHATGPT_SEND_PENDING,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CHATGPT_SEND_PENDING: [
        ShuttleStage.CHATGPT_WAITING,
        ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CHATGPT_WAITING: [
        ShuttleStage.CHATGPT_RESPONSE_READY,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CHATGPT_RESPONSE_READY: [
        ShuttleStage.PROMPT_PREVIEW,
        ShuttleStage.CODEX_QUEUE_PENDING,
        ShuttleStage.ERROR
    ],
    ShuttleStage.PROMPT_PREVIEW: [
        ShuttleStage.CODEX_QUEUE_PENDING,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CODEX_QUEUE_PENDING: [
        ShuttleStage.CODEX_WAITING,
        ShuttleStage.IDLE,
        ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.ERROR
    ],
    ShuttleStage.HUMAN_ACTION_REQUIRED: [
        ShuttleStage.IDLE,
        ShuttleStage.CODEX_WAITING,
        ShuttleStage.REPORT_PREVIEW,
        ShuttleStage.CHATGPT_SEND_PENDING,
        ShuttleStage.CHATGPT_WAITING,
        ShuttleStage.PROMPT_PREVIEW,
        ShuttleStage.CODEX_QUEUE_PENDING,
        ShuttleStage.ERROR
    ],
    ShuttleStage.DELIVERY_OUTCOME_UNKNOWN: [
        # Must NOT auto retry or transition automatically to send/queue.
        # Only human intervention can transition back to inspection or reset.
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.IDLE,
        ShuttleStage.ERROR
    ],
    ShuttleStage.ERROR: [
        ShuttleStage.IDLE,
        ShuttleStage.HUMAN_ACTION_REQUIRED
    ]
}


class CodexConfig(BaseModel):
    repo_path: str = Field(..., description="Absolute path to target git repository")
    expected_branch: str = Field(..., description="Expected git branch name")
    cli_path: str = Field(..., description="Path to Codex CLI executable")
    thread_id: str = Field(..., description="Codex GUI thread UUID")
    thread_title: Optional[str] = Field(None, description="Expected title of the GUI conversation")
    sessions_dir: str = Field(..., description="Directory containing session JSONL files")
    integration_mode: str = Field("mock", description="Integration mode: 'mock', 'cli_queue', or 'app_server'")

    @field_validator("thread_id")
    @classmethod
    def validate_thread_id(cls, v: str) -> str:
        v_clean = v.strip()
        if len(v_clean) < 10:
            raise ValueError("thread_id must be a valid identifier (min 10 chars)")
        return v_clean


class ChatGPTConfig(BaseModel):
    browser_type: str = Field("msedge", description="Browser engine: 'msedge', 'chromium', or 'chrome'")
    profile_dir: str = Field(..., description="Path to dedicated browser profile directory")
    conversation_url: str = Field(..., description="Target ChatGPT conversation URL")
    expected_gpt_id: Optional[str] = Field(None, description="Expected GPT custom assistant ID")
    expected_conversation_id: Optional[str] = Field(None, description="Expected conversation UUID")
    browser_readiness: str = Field("ready", description="'ready', 'closed', or 'not_found'")
    human_verification_state: str = Field("passed", description="'passed', 'blocked', or 'unknown'")

    @field_validator("conversation_url")
    @classmethod
    def validate_conversation_url(cls, v: str) -> str:
        if not v.startswith("http://") and not v.startswith("https://"):
            raise ValueError("conversation_url must be a valid HTTP or HTTPS URL")
        if "chatgpt.com" not in v and "openai.com" not in v and "localhost" not in v and "127.0.0.1" not in v:
            raise ValueError("conversation_url must point to ChatGPT domain or local test host")
        return v


class RouteConfig(BaseModel):
    codex_to_chatgpt_enabled: bool = True
    chatgpt_to_codex_enabled: bool = True
    prompt_extraction_policy: str = "strict_markdown_blocks"
    manual_approval_required: bool = True
    duplicate_detection_policy: str = "hash_and_prefix_match"
    restart_recovery_policy: str = "resume_latest_checkpoint"


class AppConfig(BaseModel):
    codex: CodexConfig
    chatgpt: ChatGPTConfig
    route: RouteConfig = Field(default_factory=RouteConfig)
    server_host: str = "127.0.0.1"
    server_port: int = 8765


class CheckpointRecord(BaseModel):
    id: Optional[int] = None
    stage: ShuttleStage
    iteration_id: int = 0
    source_identifier: str = ""
    destination_identifier: str = ""
    content_hash: Optional[str] = None
    payload_preview: Optional[str] = None
    transfer_status: str = "pending"  # "pending", "in_progress", "completed", "failed", "unknown"
    failure_details: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class EventLogRecord(BaseModel):
    id: Optional[int] = None
    event_type: str
    severity: str = "info"  # "debug", "info", "warning", "error"
    source: str = "system"
    stage: ShuttleStage
    message: str
    details: Optional[Dict[str, Any]] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


def compute_sha256(text: str) -> str:
    """Compute standard SHA-256 hash for content verification."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
