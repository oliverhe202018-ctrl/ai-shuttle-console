"""Fail-closed canary contracts. This module never submits anything."""
import json
import os
import re
from typing import Any, Dict, List, Optional
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from app.models import AppConfig

BASE_DIR = Path(__file__).resolve().parent.parent
CANARY_WORKSPACE = BASE_DIR / "canary_workspace"
# Generic synthetic defaults for open source public repository (no production secrets)
DEFAULT_PROTECTED_THREAD = "00000000-0000-0000-0000-000000000001"
DEFAULT_PROTECTED_CONVERSATION = "00000000-0000-0000-0000-000000000002"
DEFAULT_PROTECTED_WORKSPACE = Path("D:/workspace/production-repo")

PRODUCTION_THREAD = os.environ.get("SHUTTLE_PROTECTED_THREAD_ID", DEFAULT_PROTECTED_THREAD)
PRODUCTION_CONVERSATION = os.environ.get("SHUTTLE_PROTECTED_CONVERSATION_ID", DEFAULT_PROTECTED_CONVERSATION)
PRODUCTION_WORKSPACE = Path(os.environ.get("SHUTTLE_PROTECTED_WORKSPACE", str(DEFAULT_PROTECTED_WORKSPACE)))

_DYNAMIC_PROTECTED_TARGETS: Dict[str, Any] = {
    "thread_ids": set(),
    "conversation_ids": set(),
    "workspaces": set(),
}


def register_protected_targets(
    thread_ids: Optional[Any] = None,
    conversation_ids: Optional[Any] = None,
    workspaces: Optional[Any] = None,
) -> None:
    """Register protected production targets dynamically at runtime or in tests."""
    if thread_ids:
        items = thread_ids if isinstance(thread_ids, (list, tuple, set)) else [thread_ids]
        for t in items:
            if t:
                _DYNAMIC_PROTECTED_TARGETS["thread_ids"].add(str(t).strip().lower())
    if conversation_ids:
        items = conversation_ids if isinstance(conversation_ids, (list, tuple, set)) else [conversation_ids]
        for c in items:
            if c:
                _DYNAMIC_PROTECTED_TARGETS["conversation_ids"].add(str(c).strip().lower())
    if workspaces:
        items = workspaces if isinstance(workspaces, (list, tuple, set)) else [workspaces]
        for w in items:
            if w:
                _DYNAMIC_PROTECTED_TARGETS["workspaces"].add(Path(w))


def get_protected_targets(config: Optional[Any] = None) -> Dict[str, Any]:
    """Resolve all active protected targets from defaults, environment, registry, and config."""
    threads = set()
    conversations = set()
    workspaces = set()

    # Module-level defaults & environment overrides
    if PRODUCTION_THREAD:
        threads.add(PRODUCTION_THREAD.strip().lower())
    if PRODUCTION_CONVERSATION:
        conversations.add(PRODUCTION_CONVERSATION.strip().lower())
    if PRODUCTION_WORKSPACE:
        workspaces.add(PRODUCTION_WORKSPACE)

    env_t = os.environ.get("SHUTTLE_PROTECTED_THREAD_ID")
    if env_t:
        threads.add(env_t.strip().lower())
    env_c = os.environ.get("SHUTTLE_PROTECTED_CONVERSATION_ID")
    if env_c:
        conversations.add(env_c.strip().lower())
    env_w = os.environ.get("SHUTTLE_PROTECTED_WORKSPACE")
    if env_w:
        workspaces.add(Path(env_w))

    # Dynamic registrations
    threads.update(_DYNAMIC_PROTECTED_TARGETS["thread_ids"])
    conversations.update(_DYNAMIC_PROTECTED_TARGETS["conversation_ids"])
    workspaces.update(_DYNAMIC_PROTECTED_TARGETS["workspaces"])

    # From explicit config if provided
    if config:
        pt = getattr(config, "protected_targets", None)
        if isinstance(pt, dict):
            if pt.get("thread_id"):
                threads.add(str(pt["thread_id"]).strip().lower())
            if pt.get("conversation_id"):
                conversations.add(str(pt["conversation_id"]).strip().lower())
            if pt.get("workspace_path"):
                workspaces.add(Path(pt["workspace_path"]))
            for t in pt.get("extra_thread_ids", []):
                if t:
                    threads.add(str(t).strip().lower())
            for c in pt.get("extra_conversation_ids", []):
                if c:
                    conversations.add(str(c).strip().lower())
            for w in pt.get("extra_workspaces", []):
                if w:
                    workspaces.add(Path(w))
        elif pt is not None:
            if getattr(pt, "thread_id", None):
                threads.add(str(pt.thread_id).strip().lower())
            if getattr(pt, "conversation_id", None):
                conversations.add(str(pt.conversation_id).strip().lower())
            if getattr(pt, "workspace_path", None):
                workspaces.add(Path(pt.workspace_path))
            for t in getattr(pt, "extra_thread_ids", []):
                if t:
                    threads.add(str(t).strip().lower())
            for c in getattr(pt, "extra_conversation_ids", []):
                if c:
                    conversations.add(str(c).strip().lower())
            for w in getattr(pt, "extra_workspaces", []):
                if w:
                    workspaces.add(Path(w))

        # If config is production mode (canary_mode is False), its own targets are protected from canary
        if not getattr(config, "canary_mode", False):
            if hasattr(config, "codex") and getattr(config.codex, "thread_id", None):
                threads.add(str(config.codex.thread_id).strip().lower())
            if hasattr(config, "codex") and getattr(config.codex, "repo_path", None):
                workspaces.add(Path(config.codex.repo_path))
            if hasattr(config, "chatgpt") and getattr(config.chatgpt, "conversation_id", None):
                conversations.add(str(config.chatgpt.conversation_id).strip().lower())
            if hasattr(config, "chatgpt") and getattr(config.chatgpt, "conversation_url", None):
                conversations.add(str(config.chatgpt.conversation_url).strip().lower())

    return {
        "threads": threads,
        "conversations": conversations,
        "workspaces": workspaces,
    }


def is_production_thread(thread_id: Any, config: Optional[Any] = None) -> bool:
    if not thread_id or not isinstance(thread_id, str):
        return False
    tid = thread_id.strip().lower()
    targets = get_protected_targets(config)
    return tid in targets["threads"]


def is_production_conversation(conv_id_or_url: Any, config: Optional[Any] = None) -> bool:
    if not conv_id_or_url or not isinstance(conv_id_or_url, str):
        return False
    val = conv_id_or_url.strip().lower()
    targets = get_protected_targets(config)
    for c in targets["conversations"]:
        if c in val:
            return True
    return False


def is_production_workspace(repo_path: Any, config: Optional[Any] = None) -> bool:
    if not repo_path:
        return False
    targets = get_protected_targets(config)
    for w in targets["workspaces"]:
        if same_path(repo_path, w):
            return True
    return False


def is_production_target(
    thread_id: Any = None,
    conv_id_or_url: Any = None,
    repo_path: Any = None,
    config: Optional[Any] = None,
) -> bool:
    return (
        is_production_thread(thread_id, config)
        or is_production_conversation(conv_id_or_url, config)
        or is_production_workspace(repo_path, config)
    )

def compute_config_fingerprint(config: Any) -> str:
    if not config:
        return "none"
    from app.models import compute_sha256
    if hasattr(config, "model_dump"):
        data = config.model_dump(mode="json")
    elif isinstance(config, dict):
        data = config
    else:
        return "none"
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return compute_sha256(serialized)



def real_uuid(value: str) -> str:
    try:
        parsed = UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Real session UUID required; placeholders are forbidden")
    if str(parsed) != value.lower() or parsed.int == 0:
        raise ValueError("Real session UUID required; placeholders are forbidden")
    return value


def same_path(p1: Any, p2: Any) -> bool:
    """Case-insensitive, trailing-separator-safe, symlink/junction-resolved path equality."""
    if not p1 or not p2:
        return False
    try:
        norm1 = os.path.normcase(os.path.normpath(str(Path(p1).resolve())))
        norm2 = os.path.normcase(os.path.normpath(str(Path(p2).resolve())))
        return norm1 == norm2
    except Exception:
        return False


def same_conversation(target: str, actual: str) -> bool:
    """Compare origin and complete path, never substrings or GPT-only matches."""
    if not target or not actual:
        return False
    t, a = urlsplit(target), urlsplit(actual)
    return (t.scheme, t.netloc.lower(), t.path.rstrip("/")) == (
        a.scheme, a.netloc.lower(), a.path.rstrip("/"))


def reject_placeholder_targets(thread_id: str, url: str):
    for value in (thread_id, url):
        if not value or re.search(r"canary|placeholder|replace|example|<|>", value, re.I):
            raise ValueError("Placeholder target identifiers are forbidden for live transfer")


def validate_canary_config(config: AppConfig):
    reject_placeholder_targets(config.codex.thread_id, config.chatgpt.conversation_url)
    real_uuid(config.codex.thread_id)
    conv = real_uuid(config.chatgpt.conversation_id)
    url = urlsplit(config.chatgpt.conversation_url)
    expected_path = (f"/g/{config.chatgpt.gpt_id}" if config.chatgpt.gpt_id else "") + f"/c/{conv}"
    if url.scheme != "https" or url.netloc != "chatgpt.com" or url.path.rstrip("/") != expected_path:
        raise ValueError("Canary URL must identify the exact configured ChatGPT conversation")
    if is_production_thread(config.codex.thread_id, config) or is_production_conversation(conv, config):
        raise ValueError("Production targets are forbidden for canary")
    if is_production_workspace(config.codex.repo_path, config):
        raise ValueError("Production workspace is forbidden for canary")
        raise ValueError("Production targets are forbidden for canary")
    required_workspace = (BASE_DIR / "canary_workspace").resolve()
    if not same_path(config.codex.repo_path, required_workspace):
        raise ValueError("Canary CWD must resolve to the disposable canary_workspace")
    profile = Path(config.chatgpt.profile_dir).resolve()
    if not same_path(profile, (BASE_DIR / ".canary-chatgpt-profile").resolve()):
        raise ValueError("Canary requires the isolated .canary-chatgpt-profile")
    if not config.codex.session_index_path or not Path(config.codex.session_index_path).is_file():
        raise ValueError("Explicit existing session_index_path required")
    if not Path(config.codex.sessions_dir).is_dir():
        raise ValueError("Existing sessions_dir required")
    if not Path(config.codex.cli_path).is_file() or config.codex.integration_mode != "cli_queue":
        raise ValueError("Canary requires an explicit existing CLI and cli_queue mode")
    if not config.chatgpt.cdp_url or not config.route.manual_approval_required:
        raise ValueError("Canary requires CDP and manual approval")
    cdp = urlsplit(config.chatgpt.cdp_url)
    if cdp.scheme != "http" or cdp.hostname != "127.0.0.1":
        raise ValueError("Canary CDP must use loopback HTTP")
    if config.server_host != "127.0.0.1" or config.server_port != 8765:
        raise ValueError("Server must bind exclusively to 127.0.0.1:8765")


def load_explicit_config(path: Path, *, canary: bool = False) -> AppConfig:
    data = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    server = data.pop("server", {})
    config = AppConfig(**data, **{
        key: value for key, value in {
            "server_host": server.get("host", data.get("server_host", "127.0.0.1")),
            "server_port": server.get("port", data.get("server_port", 8765)),
        }.items() if key not in data
    })
    if canary and not config.canary_mode:
        raise ValueError("--canary requires canary_mode=true in explicit configuration")
    if config.canary_mode:
        for section, fields in {
            "codex": ("repo_path", "cli_path", "thread_id", "sessions_dir", "session_index_path", "integration_mode"),
            "chatgpt": ("profile_dir", "conversation_url", "expected_conversation_id", "cdp_url"),
        }.items():
            if any(not data.get(section, {}).get(field) for field in fields):
                raise ValueError(f"Missing required canary fields in {section}; no configuration fallback")
        validate_canary_config(config)
    if config.server_host != "127.0.0.1" or config.server_port != 8765:
        raise ValueError("Server must bind exclusively to 127.0.0.1:8765")
    return config


def validate_canary_session(config, observer):
    validate_canary_config(config)
    if observer is None:
        raise ValueError("Canary session observer unavailable")
    obs = observer.observe_thread(config.codex.thread_id, expected_cwd=config.codex.repo_path)
    header = obs.get("session_meta") or {}
    if (obs["validation_errors"] or not obs["metadata_valid"]
            or header.get("id") != config.codex.thread_id or not header.get("cwd")
            or not same_path(header["cwd"], CANARY_WORKSPACE.resolve())):
        raise ValueError("Canary session identity/index/header/CWD verification failed: " + "; ".join(obs["validation_errors"]))
    return obs


def canary_prompt(nonce: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise ValueError("Canary nonce must be a generated 32-character hex value")
    return (f"# Canary Ack\nReturn exactly: CANARY-OK:{nonce}\n"
            "Do not inspect or modify files, execute commands, or perform any other action.")


def canary_request(nonce: str) -> str:
    return (f"Isolated harmless canary request. Current nonce: {nonce}\n"
            "Reply with exactly one fenced markdown code block containing the following Codex test prompt. "
            "Copy it verbatim, including the nonce. Do not execute it or add commentary.\n\n"
            f"```markdown\n{canary_prompt(nonce)}\n```")


def canary_nonce(record):
    policy = record.provenance_policy or ""
    if not policy.startswith("canary:"):
        raise ValueError("Current canary provenance is missing; no fallback allowed")
    nonce = policy.split(":", 1)[1]
    canary_prompt(nonce)
    return nonce

# Phase 2.5 Bounded Polling Constants
CHATGPT_DELIVERY_TIMEOUT_SEC = 30
CHATGPT_ASSISTANT_TIMEOUT_SEC = 120
CODEX_QUEUE_DISPATCH_TIMEOUT_SEC = 30
CODEX_COMPLETION_TIMEOUT_SEC = 180


def generate_canary_nonce() -> str:
    import secrets
    return secrets.token_hex(16)


def validate_round_invariants(
    nonce: str,
    target_thread: Optional[str] = None,
    target_conv: Optional[str] = None,
    target_repo: Optional[Any] = None,
    previous_nonces: Optional[List[str]] = None,
) -> None:
    if not nonce or not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise ValueError("Canary nonce must be a valid 32-character hex string")
    if previous_nonces and nonce in previous_nonces:
        raise ValueError(f"Canary nonce {nonce} has already been used; replaying nonces is strictly forbidden")
    if is_production_target(thread_id=target_thread, conv_id_or_url=target_conv, repo_path=target_repo):
        raise ValueError("Production targets are strictly forbidden in canary rounds")


def auto_canary_prompt(run_id: str, iteration: int, nonce: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise ValueError("Canary nonce must be a generated 32-character hex value")
    return (
        f"# Canary Auto Iteration\n"
        f"Return exactly: CANARY-AUTO-OK:{run_id}:{iteration}:{nonce}\n"
        f"Do not inspect or modify files, execute commands, or perform any other action."
    )


def auto_canary_request(
    run_id: str,
    iteration: int,
    nonce: str,
    max_iterations: int = 3,
    prior_codex_output: Optional[str] = None
) -> str:
    prior_note = f"\nPrior Codex final response received: {prior_codex_output}\n" if prior_codex_output else ""
    return (
        f"Isolated harmless canary auto-shuttle iteration {iteration} of {max_iterations}. Run ID: {run_id}. Nonce: {nonce}.{prior_note}\n"
        f"Reply with exactly one fenced markdown code block containing the following Codex test prompt. "
        f"Copy it verbatim, including the run_id, iteration, and nonce. Do not execute it or add commentary.\n\n"
        f"```markdown\n{auto_canary_prompt(run_id, iteration, nonce)}\n```"
    )
