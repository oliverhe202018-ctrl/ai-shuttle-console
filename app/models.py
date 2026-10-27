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
        ShuttleStage.CHATGPT_WAITING,
        ShuttleStage.CHATGPT_RESPONSE_READY,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CODEX_WAITING: [
        ShuttleStage.CODEX_RESPONSE_READY,
        ShuttleStage.REPORT_PREVIEW,
        ShuttleStage.IDLE,
        ShuttleStage.HUMAN_ACTION_REQUIRED,
        ShuttleStage.DELIVERY_OUTCOME_UNKNOWN,
        ShuttleStage.ERROR
    ],
    ShuttleStage.CODEX_RESPONSE_READY: [
        ShuttleStage.REPORT_PREVIEW,
        ShuttleStage.CHATGPT_SEND_PENDING,
        ShuttleStage.ERROR
    ],
    ShuttleStage.REPORT_PREVIEW: [
        ShuttleStage.CHATGPT_SEND_PENDING,
        ShuttleStage.CODEX_WAITING,
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
        ShuttleStage.PROMPT_PREVIEW,
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
        ShuttleStage.CHATGPT_WAITING,
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
        ShuttleStage.CHATGPT_WAITING,
        ShuttleStage.CODEX_WAITING,
        ShuttleStage.REPORT_PREVIEW,
        ShuttleStage.PROMPT_PREVIEW,
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
    session_index_path: Optional[str] = Field(None, description="Path to session_index.jsonl")
    integration_mode: str = Field("mock", description="Integration mode: 'mock', 'cli_queue', or 'app_server'")

    @property
    def project_cwd(self) -> str:
        return self.repo_path

    @property
    def conversation_title(self) -> Optional[str]:
        return self.thread_title

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
    cdp_url: Optional[str] = Field("http://127.0.0.1:9222", description="Chrome DevTools Protocol port/url")

    @property
    def gpt_id(self) -> Optional[str]:
        return self.expected_gpt_id

    @property
    def conversation_id(self) -> Optional[str]:
        return self.expected_conversation_id

    @property
    def user_data_dir(self) -> str:
        return self.profile_dir

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


class ProtectedTargetsConfig(BaseModel):
    thread_id: Optional[str] = Field(None, description="Protected production Codex thread UUID")
    conversation_id: Optional[str] = Field(None, description="Protected production ChatGPT conversation UUID or URL")
    workspace_path: Optional[str] = Field(None, description="Protected production workspace directory path")
    extra_thread_ids: List[str] = Field(default_factory=list, description="Additional protected thread UUIDs")
    extra_conversation_ids: List[str] = Field(default_factory=list, description="Additional protected conversation UUIDs or URLs")
    extra_workspaces: List[str] = Field(default_factory=list, description="Additional protected workspace paths")


class AppConfig(BaseModel):
    canary_mode: bool = False
    codex: CodexConfig
    chatgpt: ChatGPTConfig
    route: RouteConfig = Field(default_factory=RouteConfig)
    protected_targets: Optional[ProtectedTargetsConfig] = Field(default=None, description="Protected production targets")
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


class ObservationRecord(BaseModel):
    id: Optional[int] = None
    source_type: str  # 'codex' or 'chatgpt'
    source_identifier: str  # thread_id or conversation_url / gpt_id
    message_id: Optional[str] = None
    event_offset: Optional[int] = None
    content_hash: str
    payload_preview: Optional[str] = None
    raw_payload_snippet: Optional[str] = None
    status: str = "verified"  # 'verified', 'unverified', 'error', 'historical', 'new'
    error_details: Optional[str] = None
    observed_at: datetime = Field(default_factory=datetime.utcnow)


class PromptExtractionRecord(BaseModel):
    id: Optional[int] = None
    source_conversation_id: str
    source_message_id: Optional[str] = None
    observation_timestamp: datetime = Field(default_factory=datetime.utcnow)
    content_hash: str
    extracted_prompt_hash: str
    extracted_prompt_preview: str
    raw_response_snippet: Optional[str] = None
    provenance_policy: str
    is_fresh: bool = True
    transfer_eligible: bool = False
    ineligibility_reason: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)




class TransferDirection(str, Enum):
    CODEX_TO_CHATGPT = "codex_to_chatgpt"
    CHATGPT_TO_CODEX = "chatgpt_to_codex"


class TransferStatus(str, Enum):
    PENDING = "pending"
    PREPARED = "prepared"
    CONFIRMED = "confirmed"
    SENDING = "sending"
    SENT = "sent"
    DELIVERED = "delivered"
    DELIVERY_OUTCOME_UNKNOWN = "delivery_outcome_unknown"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NOT_SENT = "not_sent"


class CodexDeliveryClassification(str, Enum):
    CODEX_QUEUE_AND_TASK_COMPLETED = "CODEX_QUEUE_AND_TASK_COMPLETED"
    CODEX_QUEUE_DISPATCHED_TASK_PENDING = "CODEX_QUEUE_DISPATCHED_TASK_PENDING"
    CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED = "CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED"
    CODEX_NOT_SENT = "CODEX_NOT_SENT"
    CODEX_DELIVERY_OUTCOME_UNKNOWN = "CODEX_DELIVERY_OUTCOME_UNKNOWN"


class TransferAuthorization(BaseModel):
    id: Optional[int] = None
    transfer_id: str
    confirmation_token: str
    destination_identifier: str
    expected_content_hash: str
    expected_baseline_hash: Optional[str] = None
    consumed: bool = False
    expires_at: datetime
    created_at: datetime = Field(default_factory=datetime.utcnow)
    config_fingerprint: Optional[str] = None


class TransferRecord(BaseModel):
    id: Optional[int] = None
    transfer_id: str
    iteration_id: int = 0
    direction: str = TransferDirection.CODEX_TO_CHATGPT.value
    source_type: str = "codex"
    source_identifier: str = ""
    source_message_id: Optional[str] = None
    destination_type: str = "chatgpt"
    destination_identifier: str = ""
    content_hash: str = ""
    char_count: int = 0
    message_text: Optional[str] = None
    payload_preview: Optional[str] = None
    baseline_message_id: Optional[str] = None
    baseline_hash: Optional[str] = None
    status: str = TransferStatus.PENDING.value
    delivery_evidence: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    human_confirmed: bool = False
    confirmation_token: Optional[str] = None
    is_operator_modified: bool = False
    provenance_policy: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    confirmed_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    verified_at: Optional[datetime] = None
    config_fingerprint: Optional[str] = None
    raw_content_hash: Optional[str] = None
    semantic_content_hash: Optional[str] = None

    @property
    def queue_receipt_status(self) -> Optional[str]:
        if isinstance(self.delivery_evidence, dict):
            return self.delivery_evidence.get("queue_receipt_status")
        return None

    @property
    def turn_dispatch_status(self) -> Optional[str]:
        if isinstance(self.delivery_evidence, dict):
            return self.delivery_evidence.get("turn_dispatch_status")
        return None

    @property
    def task_completion_status(self) -> Optional[str]:
        if isinstance(self.delivery_evidence, dict):
            return self.delivery_evidence.get("task_completion_status")
        return None

    @property
    def outcome_classification(self) -> Optional[str]:
        if isinstance(self.delivery_evidence, dict):
            return self.delivery_evidence.get("outcome_classification")
        return None


def compute_sha256(text: str) -> str:
    """Compute standard SHA-256 hash for content verification."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class CanaryRoundResult(str, Enum):
    ROUND_COMPLETED = "ROUND_COMPLETED"
    ROUND_BLOCKED_CHATGPT = "ROUND_BLOCKED_CHATGPT"
    ROUND_QUEUE_ACCEPTED_NOT_DISPATCHED = "ROUND_QUEUE_ACCEPTED_NOT_DISPATCHED"
    ROUND_BLOCKED_CODEX = "ROUND_BLOCKED_CODEX"
    ROUND_DELIVERY_UNKNOWN = "ROUND_DELIVERY_UNKNOWN"
    ROUND_FAILED_VALIDATION = "ROUND_FAILED_VALIDATION"
    ROUND_RECOVERED_AFTER_FALSE_UNKNOWN = "ROUND_RECOVERED_AFTER_FALSE_UNKNOWN"


class CanaryRoundMetrics(BaseModel):
    c2g_prep_latency_ms: Optional[float] = None
    c2g_delivery_latency_ms: Optional[float] = None
    chatgpt_response_latency_ms: Optional[float] = None
    prompt_extraction_latency_ms: Optional[float] = None
    g2c_queue_ack_latency_ms: Optional[float] = None
    queue_to_dispatch_latency_ms: Optional[float] = None
    codex_execution_latency_ms: Optional[float] = None
    total_round_trip_latency_ms: Optional[float] = None


class CanaryRoundRecord(BaseModel):
    id: Optional[int] = None
    round_id: str
    round_number: int
    nonce: str
    c2g_transfer_id: Optional[str] = None
    chatgpt_user_turn_id: Optional[str] = None
    chatgpt_assistant_turn_id: Optional[str] = None
    g2c_transfer_id: Optional[str] = None
    codex_queue_receipt_id: Optional[str] = None
    codex_user_turn_id: Optional[str] = None
    codex_assistant_message_id: Optional[str] = None
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    result: CanaryRoundResult = CanaryRoundResult.ROUND_COMPLETED
    manual_thread_load_required: bool = False
    metrics: Optional[Dict[str, Any]] = None
    error_details: Optional[str] = None
    is_recovered: bool = False
    recovery_notes: Optional[str] = None
    recovered_at: Optional[datetime] = None


# Phase 2.6 Models: Controlled Auto Mode & Official Codex Thread Lifecycle

class AutoRunState(str, Enum):
    AUTO_DRAFT = "AUTO_DRAFT"
    AUTO_ARMED = "AUTO_ARMED"
    AUTO_RUNNING = "AUTO_RUNNING"
    AUTO_WAITING_CODEX = "AUTO_WAITING_CODEX"
    AUTO_WAITING_CHATGPT = "AUTO_WAITING_CHATGPT"
    AUTO_RESUMING_CODEX_THREAD = "AUTO_RESUMING_CODEX_THREAD"
    AUTO_HUMAN_ACTION_REQUIRED = "AUTO_HUMAN_ACTION_REQUIRED"
    AUTO_PAUSED = "AUTO_PAUSED"
    AUTO_COMPLETED = "AUTO_COMPLETED"
    AUTO_STOPPED = "AUTO_STOPPED"
    AUTO_FAILED = "AUTO_FAILED"
    AUTO_PAUSED_RECOVERY_REQUIRED = "AUTO_PAUSED_RECOVERY_REQUIRED"


class PlannerClassification(str, Enum):
    NEXT_CODEX_PROMPT_FOUND = "NEXT_CODEX_PROMPT_FOUND"
    PLANNER_STOP_REQUESTED = "PLANNER_STOP_REQUESTED"
    PLANNER_NO_ACTIONABLE_PROMPT = "PLANNER_NO_ACTIONABLE_PROMPT"
    PLANNER_RESPONSE_AMBIGUOUS = "PLANNER_RESPONSE_AMBIGUOUS"
    PLANNER_RESPONSE_ERROR = "PLANNER_RESPONSE_ERROR"


class AutoRunAuthorization(BaseModel):
    id: Optional[int] = None
    run_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime
    status: AutoRunState = AutoRunState.AUTO_ARMED
    max_iterations: int = 3
    iterations_completed: int = 0
    chatgpt_conversation_url: str
    chatgpt_conversation_id: str
    codex_thread_id: str
    codex_workspace: str
    config_fingerprint: str
    canary_mode: bool = True
    authorization_scope_version: str = "v1"
    operator_confirmed: bool = False
    allow_cold_resume: bool = True
    experimental_queue_start_enabled: bool = False
    stop_reason: Optional[str] = None
    error_details: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AutoRunIteration(BaseModel):
    id: Optional[int] = None
    run_id: str
    iteration_number: int
    status: str = "running"
    codex_source_turn_id: Optional[str] = None
    codex_source_message_id: Optional[str] = None
    codex_output_hash: Optional[str] = None
    c2g_transfer_id: Optional[str] = None
    chatgpt_user_turn_id: Optional[str] = None
    chatgpt_assistant_turn_id: Optional[str] = None
    planner_response_hash: Optional[str] = None
    planner_classification: Optional[str] = None
    extracted_prompt_hash: Optional[str] = None
    g2c_transfer_id: Optional[str] = None
    queue_receipt_id: Optional[str] = None
    queued_item_id: Optional[str] = None
    codex_target_turn_id: Optional[str] = None
    codex_assistant_message_id: Optional[str] = None
    thread_resume_required: bool = False
    thread_resume_attempted: bool = False
    thread_resume_result: Optional[str] = None
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    metrics: Optional[Dict[str, Any]] = None
    stop_reason: Optional[str] = None
    error_details: Optional[str] = None


class ChildTransferAuthorization(BaseModel):
    id: Optional[int] = None
    child_auth_id: str
    parent_run_id: str
    iteration_number: int
    transfer_id: str
    direction: str
    destination_identifier: str
    content_hash: str
    config_fingerprint: str
    confirmation_token: str
    consumed: bool = False
    consumed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CodexLifecycleEvent(BaseModel):
    id: Optional[int] = None
    event_id: str
    run_id: Optional[str] = None
    iteration_number: Optional[int] = None
    thread_id: str
    event_type: str
    pre_state: Optional[str] = None
    post_state: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AutoAuthorizeRequest(BaseModel):
    max_iterations: int = 3
    operator_confirmed: bool = False
    allow_cold_resume: bool = True
    ttl_minutes: int = 60
    experimental_queue_start_enabled: bool = False


class AutoPauseRequest(BaseModel):
    run_id: str
    reason: Optional[str] = "Operator requested pause"


class AutoEmergencyStopRequest(BaseModel):
    run_id: str
    reason: Optional[str] = "Operator emergency stop"


class AutoResumeHumanRequest(BaseModel):
    run_id: str
    action_resolved: bool = True
    notes: Optional[str] = None


class CodexResumeRequest(BaseModel):
    thread_id: str
    expected_workspace: Optional[str] = None
