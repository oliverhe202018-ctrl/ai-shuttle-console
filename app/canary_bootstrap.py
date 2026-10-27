"""
Phase 2.4.2 — Automated Canary Environment Bootstrap.
Automates safe local discovery, configuration, and preflight for the isolated live canary.
Fail-closed: stops when manual operator action is genuinely required.
"""
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit
from uuid import UUID

from app.canary import (
    BASE_DIR,
    CANARY_WORKSPACE,
    PRODUCTION_CONVERSATION,
    PRODUCTION_THREAD,
    is_production_thread,
    is_production_conversation,
    is_production_workspace,
    load_explicit_config,
    real_uuid,
    validate_canary_config,
    same_path,
    same_conversation,
    compute_config_fingerprint,
)
from app.models import AppConfig

CANARY_PROFILE = BASE_DIR / ".canary-chatgpt-profile"
STATE_FILE = BASE_DIR / "runtime" / "canary_bootstrap_state.json"


def save_bootstrap_state(state: Dict[str, Any]) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass


def load_bootstrap_state() -> Optional[Dict[str, Any]]:
    if not STATE_FILE.is_file():
        return None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def find_latest_canary_config(base_dir: Optional[Path] = None) -> Optional[Path]:
    target_dir = base_dir or BASE_DIR
    configs = sorted(target_dir.glob("config.canary.local.*.json"), reverse=True)
    for cfg in configs:
        if cfg.is_file():
            return cfg
    return None

def find_matching_canary_config(
    codex_target: Dict[str, Any],
    chatgpt_target: Dict[str, Any],
    base_dir: Optional[Path] = None,
) -> Optional[Path]:
    target_dir = base_dir or BASE_DIR
    target_tid = (codex_target.get("thread_id") or "").strip().lower()
    target_cwd = codex_target.get("cwd")
    target_curl = (chatgpt_target.get("conversation_url") or "").strip()
    target_cid = (chatgpt_target.get("conversation_id") or "").strip().lower()

    configs = sorted(target_dir.glob("config.canary.local.*.json"), reverse=True)
    for cfg_file in configs:
        if not cfg_file.is_file():
            continue
        try:
            cfg = load_explicit_config(cfg_file, canary=True)
            if not cfg.canary_mode:
                continue
            if cfg.codex.thread_id.lower() != target_tid:
                continue
            if target_cid and cfg.chatgpt.conversation_id.lower() != target_cid:
                continue
            if not same_conversation(cfg.chatgpt.conversation_url, target_curl):
                continue
            if target_cwd and not same_path(cfg.codex.repo_path, target_cwd):
                continue
            return cfg_file
        except Exception:
            continue
    return None


# Bootstrap Verdicts
CANARY_PREFLIGHT_VERIFIED = "CANARY_PREFLIGHT_VERIFIED"
HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD = "HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD"
HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN = "HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN"
HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT = "HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT"
CANARY_BOOTSTRAP_BLOCKED = "CANARY_BOOTSTRAP_BLOCKED"

# Human Action Messages
ACTION_CREATE_CODEX_CANARY_THREAD = (
    "Create a new Codex GUI conversation using working directory:\n"
    f"{CANARY_WORKSPACE}\n"
    "Send a harmless initialization message such as:\n"
    "This is the isolated AI Shuttle canary thread. Reply exactly CANARY-THREAD-READY and do not modify files or execute commands.\n"
    "After doing this, rerun the bootstrap to automatically discover the newly created thread."
)

ACTION_CHATGPT_LOGIN = (
    "Open the already-running canary Edge window and manually complete ChatGPT login/verification. "
    "Do not close the browser. After that, rerun the bootstrap."
)

ACTION_CREATE_CHATGPT_CANARY_CHAT = (
    "In the already-running canary Edge window, create a new blank ChatGPT conversation and leave it open. "
    "After that, rerun the bootstrap."
)

CHATGPT_URL_REGEX = re.compile(
    r"^https://chatgpt\.com(?:/g/([^/?#]+))?/c/([0-9a-fA-F-]+)(?:[?#].*)?$"
)


# --- Section 6: Resolve the Current Codex CLI ---

def find_and_verify_codex_cli(preferred_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Discover and verify installed Codex CLI binary.
    Verifies:
    - File exists
    - codex --help succeeds (exit code 0)
    - codex queue --help exposes --thread, --message, and -C / --cd
    Never runs codex queue.
    """
    candidates: List[Path] = []
    if preferred_path:
        candidates.append(Path(preferred_path))

    which_codex = shutil.which("codex")
    if which_codex:
        candidates.append(Path(which_codex))

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        app_data_bin = Path(local_app_data) / "OpenAI" / "Codex" / "bin"
        if app_data_bin.is_dir():
            candidates.extend(sorted(app_data_bin.glob("*/codex.exe"), reverse=True))

    home = Path.home()
    candidates.append(home / ".codex" / "bin" / "codex.exe")
    candidates.append(home / ".cargo" / "bin" / "codex.exe")

    checked: List[str] = []
    for candidate in candidates:
        cand_str = str(candidate)
        if cand_str in checked:
            continue
        checked.append(cand_str)

        if not candidate.is_file():
            continue

        try:
            help_res = subprocess.run(
                [str(candidate), "--help"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if help_res.returncode != 0:
                continue

            queue_res = subprocess.run(
                [str(candidate), "queue", "--help"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if queue_res.returncode != 0:
                continue

            q_out = queue_res.stdout
            has_thread = "--thread" in q_out
            has_message = "--message" in q_out
            has_cd = ("-C" in q_out or "--cd" in q_out)

            if has_thread and has_message and has_cd:
                return {
                    "status": "verified",
                    "cli_path": str(candidate.resolve()),
                    "has_thread": True,
                    "has_message": True,
                    "has_cd": True,
                }
        except Exception:
            continue

    return {
        "status": "not_found",
        "cli_path": None,
        "error": "No valid Codex CLI binary with queue capability found on system",
    }


# --- Section 3: Prepare the Dedicated Canary Edge Environment ---

def is_port_listening(host: str = "127.0.0.1", port: int = 9223) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex((host, port)) == 0
    except Exception:
        return False


def get_pid_listening_on_port(port: int = 9223) -> Optional[int]:
    try:
        out = subprocess.check_output(["netstat", "-ano", "-p", "tcp"], text=True, errors="ignore")
        for line in out.splitlines():
            if f":{port} " in line and "LISTENING" in line:
                parts = line.split()
                return int(parts[-1])
    except Exception:
        pass
    return None


def get_process_cmdline(pid: int) -> Optional[str]:
    try:
        ps_cmd = f'(Get-CimInstance Win32_Process -Filter "ProcessId={pid}").CommandLine'
        res = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return res.stdout.strip() if res.returncode == 0 else None
    except Exception:
        return None


def find_edge_executable() -> Optional[Path]:
    candidates = [
        Path(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("PROGRAMFILES", r"C:\Program Files")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]
    for c in candidates:
        if c.is_file():
            return c
    which_edge = shutil.which("msedge")
    if which_edge and Path(which_edge).is_file():
        return Path(which_edge)
    return None


def check_or_launch_canary_edge(
    profile_dir: Optional[Path] = None,
    cdp_port: int = 9223,
    auto_launch: bool = True,
) -> Dict[str, Any]:
    """
    Check if Edge with canary profile is listening on cdp_port.
    If yes, reuse it without launching a duplicate.
    If no, launch one headed Edge instance with the exact canary flags.
    Never terminates existing user processes or kills msedge.exe.
    """
    target_profile = (profile_dir or CANARY_PROFILE).resolve()
    target_profile.mkdir(parents=True, exist_ok=True)

    if is_port_listening("127.0.0.1", cdp_port):
        pid = get_pid_listening_on_port(cdp_port)
        cmdline = get_process_cmdline(pid) if pid else None
        
        # Verify it uses the canary profile
        norm_profile_str = str(target_profile).lower()
        if cmdline and norm_profile_str in cmdline.lower():
            return {
                "status": "reused",
                "cdp_url": f"http://127.0.0.1:{cdp_port}",
                "profile_dir": str(target_profile),
                "process_id": pid,
                "cmdline": cmdline,
            }
        elif cmdline:
            return {
                "status": "port_conflict",
                "error": f"Port {cdp_port} is in use by another process with different profile: {cmdline}",
                "cdp_url": f"http://127.0.0.1:{cdp_port}",
                "process_id": pid,
            }
        else:
            # Listening, assume reused if cmdline unavailable
            return {
                "status": "reused",
                "cdp_url": f"http://127.0.0.1:{cdp_port}",
                "profile_dir": str(target_profile),
                "process_id": pid,
            }

    if not auto_launch:
        return {
            "status": "missing",
            "error": f"Canary Edge instance is not listening on 127.0.0.1:{cdp_port}",
            "profile_dir": str(target_profile),
        }

    edge_bin = find_edge_executable()
    if not edge_bin:
        return {
            "status": "blocked",
            "error": "Microsoft Edge executable (msedge.exe) not found on system",
            "profile_dir": str(target_profile),
        }

    cmd = [
        str(edge_bin),
        "--remote-debugging-address=127.0.0.1",
        f"--remote-debugging-port={cdp_port}",
        f"--user-data-dir={target_profile}",
        "https://chatgpt.com/",
    ]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import time
        for _ in range(25):
            time.sleep(0.2)
            if is_port_listening("127.0.0.1", cdp_port):
                return {
                    "status": "launched",
                    "cdp_url": f"http://127.0.0.1:{cdp_port}",
                    "profile_dir": str(target_profile),
                    "process_id": proc.pid,
                }
        return {
            "status": "launch_timeout",
            "error": f"Launched Edge PID {proc.pid} but port {cdp_port} did not listen within 5s",
            "profile_dir": str(target_profile),
        }
    except Exception as exc:
        return {
            "status": "launch_failed",
            "error": f"Failed to launch canary Edge instance: {exc}",
            "profile_dir": str(target_profile),
        }


# --- Section 4 & 5: Detect ChatGPT Human Prerequisites & Identity ---

async def inspect_chatgpt_canary(
    cdp_url: str = "http://127.0.0.1:9223",
    profile_dir: Optional[Path] = None,
    mock_pages: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Inspect canary Edge instance via CDP.
    Determines whether:
    - ChatGPT is authenticated
    - Cloudflare/Turnstile verification is present
    - A usable ChatGPT page is open
    - A disposable conversation already exists in this canary profile
    Strictly rejects protected production conversation.
    """
    target_profile = (profile_dir or CANARY_PROFILE).resolve()
    
    pages: List[Dict[str, Any]] = []
    if mock_pages is not None:
        pages = mock_pages
    else:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=3.0) as client:
                res = await client.get(f"{cdp_url}/json/list")
                if res.status_code == 200:
                    pages = res.json()
        except Exception as exc:
            return {
                "status": CANARY_BOOTSTRAP_BLOCKED,
                "error": f"Failed to query CDP list at {cdp_url}: {exc}",
                "human_action": None,
            }

    chat_pages = [p for p in pages if p.get("type") == "page"]
    if not chat_pages:
        return {
            "status": HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN,
            "error": "No browser page targets found in canary Edge instance",
            "human_action": ACTION_CHATGPT_LOGIN,
        }

    # Detect Turnstile / Cloudflare challenges
    for p in chat_pages:
        title = (p.get("title") or "").lower()
        url = p.get("url") or ""
        if "turnstile" in title or "verify you are human" in title or "just a moment" in title:
            return {
                "status": HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN,
                "error": "Cloudflare / Turnstile security verification required",
                "human_action": ACTION_CHATGPT_LOGIN,
                "page_url": url,
            }

    # Detect login / auth redirects
    for p in chat_pages:
        url = p.get("url") or ""
        if "/auth/login" in url or "auth.openai.com" in url:
            return {
                "status": HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN,
                "error": "ChatGPT authentication / login required in canary profile",
                "human_action": ACTION_CHATGPT_LOGIN,
                "page_url": url,
            }

    conversation_candidates: List[Dict[str, Any]] = []
    for p in chat_pages:
        url = p.get("url") or ""
        match = CHATGPT_URL_REGEX.match(url)
        if match:
            gpt_id = match.group(1)
            conv_id = match.group(2)
            # Strictly reject production conversation
            if conv_id.lower() == PRODUCTION_CONVERSATION.lower():
                return {
                    "status": CANARY_BOOTSTRAP_BLOCKED,
                    "error": f"Protected production ChatGPT conversation {PRODUCTION_CONVERSATION} detected in canary profile! Forbidden.",
                    "human_action": None,
                }
            conversation_candidates.append({
                "url": url,
                "conversation_id": conv_id,
                "gpt_id": gpt_id,
                "title": p.get("title"),
                "target_id": p.get("id"),
            })

    if not conversation_candidates:
        has_chatgpt_root = any("chatgpt.com" in (p.get("url") or "") for p in chat_pages)
        if has_chatgpt_root:
            return {
                "status": HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT,
                "error": "No disposable ChatGPT conversation (/c/<uuid>) found in canary Edge instance",
                "human_action": ACTION_CREATE_CHATGPT_CANARY_CHAT,
            }
        else:
            return {
                "status": HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN,
                "error": "ChatGPT is not open in canary Edge instance",
                "human_action": ACTION_CHATGPT_LOGIN,
            }

    if len(conversation_candidates) > 1:
        return {
            "status": CANARY_BOOTSTRAP_BLOCKED,
            "error": f"Multiple ChatGPT conversations open ({len(conversation_candidates)}); exactly one required for canary",
            "candidates": conversation_candidates,
            "human_action": "Close extra ChatGPT tabs in the canary browser so only the single test conversation remains open.",
        }

    conv = conversation_candidates[0]
    return {
        "status": "discovered",
        "conversation_url": conv["url"],
        "conversation_id": conv["conversation_id"],
        "gpt_id": conv["gpt_id"],
        "title": conv.get("title"),
        "cdp_url": cdp_url,
        "profile_dir": str(target_profile),
    }


# --- Section 2: Discover or Prepare the Canary Codex Target ---

def discover_codex_canary_target(
    workspace: Optional[Path] = None,
    sessions_dir: Optional[Path] = None,
    session_index_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Search real Codex session index and session metadata for a thread whose recorded
    working directory exactly resolves to the disposable canary workspace.
    If 0 matches: returns HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD.
    If multiple matches: returns CANARY_BOOTSTRAP_BLOCKED with candidate table.
    If exactly 1 match: verifies index maps same UUID and header cwd matches, returns discovered.
    """
    target_ws = (workspace or CANARY_WORKSPACE).resolve()
    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    s_dir = (sessions_dir or (codex_home / "sessions")).resolve()
    idx_path = (session_index_path or (codex_home / "session_index.jsonl")).resolve()

    index_map: Dict[str, Dict[str, Any]] = {}
    if idx_path.is_file():
        try:
            with open(idx_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("id"):
                            index_map[entry["id"]] = entry
                    except Exception:
                        continue
        except Exception:
            pass

    if not s_dir.is_dir():
        return {
            "status": CANARY_BOOTSTRAP_BLOCKED,
            "error": f"Codex sessions directory does not exist: {s_dir}",
            "candidates": [],
        }

    matched_sessions: List[Dict[str, Any]] = []
    for s_file in s_dir.glob("**/*.jsonl"):
        try:
            with open(s_file, "r", encoding="utf-8", errors="ignore") as f:
                first_line = f.readline()
                if not first_line:
                    continue
                rec = json.loads(first_line)
                meta = rec.get("payload") if rec.get("type") == "session_meta" else rec.get("session_meta")
                if not meta:
                    continue
                cwd = meta.get("cwd")
                if not cwd:
                    continue
                try:
                    resolved_cwd = Path(cwd).resolve()
                except Exception:
                    continue

                if same_path(resolved_cwd, target_ws):
                    thread_id = meta.get("id") or meta.get("session_id")
                    if not thread_id:
                        continue

                    # Reject production thread
                    if is_production_thread(thread_id):
                        continue

                    idx_entry = index_map.get(thread_id, {})
                    title = idx_entry.get("thread_name") or idx_entry.get("title") or meta.get("thread_name")
                    updated_at = idx_entry.get("updated_at") or meta.get("timestamp")

                    matched_sessions.append({
                        "thread_id": thread_id,
                        "title": title or "(Untitled)",
                        "cwd": str(resolved_cwd),
                        "session_file_path": str(s_file.resolve()),
                        "updated_at": updated_at,
                        "in_index": thread_id in index_map,
                    })
        except Exception:
            continue

    if len(matched_sessions) == 0:
        action = (
            "Create a new Codex GUI conversation using working directory:\n"
            f"{target_ws}\n"
            "Send a harmless initialization message such as:\n"
            "This is the isolated AI Shuttle canary thread. Reply exactly CANARY-THREAD-READY and do not modify files or execute commands.\n"
            "After doing this, rerun the bootstrap to automatically discover the newly created thread."
        )
        return {
            "status": HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD,
            "workspace": str(target_ws),
            "error": f"No Codex session found with working directory '{target_ws}'",
            "human_action": action,
            "candidates": [],
        }

    if len(matched_sessions) > 1:
        return {
            "status": CANARY_BOOTSTRAP_BLOCKED,
            "workspace": str(target_ws),
            "error": f"Multiple candidate Codex sessions found ({len(matched_sessions)}) for workspace '{target_ws}'",
            "candidates": matched_sessions,
            "human_action": "Multiple threads match the canary workspace. Remove extra sessions or select an exact thread ID.",
        }

    target = matched_sessions[0]
    try:
        real_uuid(target["thread_id"])
    except ValueError as exc:
        return {
            "status": CANARY_BOOTSTRAP_BLOCKED,
            "error": f"Invalid thread UUID '{target['thread_id']}': {exc}",
            "candidates": matched_sessions,
        }

    return {
        "status": "discovered",
        "thread_id": target["thread_id"],
        "title": target["title"],
        "cwd": target["cwd"],
        "session_file_path": target["session_file_path"],
        "sessions_dir": str(s_dir),
        "session_index_path": str(idx_path),
        "updated_at": target["updated_at"],
        "in_index": target["in_index"],
    }


# --- Section 7: Generate the Real Local Canary Configuration ---

def generate_canary_config(
    template_path: Optional[Path] = None,
    codex_target: Optional[Dict[str, Any]] = None,
    chatgpt_target: Optional[Dict[str, Any]] = None,
    cli_path: Optional[str] = None,
    output_path: Optional[Path] = None,
    preferred_config_path: Optional[Path] = None,
    reuse_only: bool = False,
) -> Path:
    """
    Generate a NEW gitignored local config file once both real canary identities exist.
    Never overwrites an existing local config.
    """
    if not codex_target or not chatgpt_target or not cli_path:
        raise ValueError("Cannot generate canary configuration without real Codex and ChatGPT targets and CLI path")

    # Codex target validation
    thread_id = codex_target.get("thread_id")
    if not thread_id:
        raise ValueError("Missing 'thread_id' in codex_target")
    real_uuid(thread_id)
    if is_production_thread(thread_id):
        raise ValueError(f"Protected production thread ID {thread_id} cannot be used for canary")

    cwd = codex_target.get("cwd")
    if not cwd or not same_path(cwd, CANARY_WORKSPACE.resolve()):
        raise ValueError(f"Codex target cwd '{cwd}' does not match canary workspace '{CANARY_WORKSPACE.resolve()}'")

    # ChatGPT target validation
    conv_id = chatgpt_target.get("conversation_id")
    if not conv_id:
        raise ValueError("Missing 'conversation_id' in chatgpt_target")
    real_uuid(conv_id)
    if is_production_conversation(conv_id) or is_production_conversation(chatgpt_target.get("conversation_url") or ""):
        raise ValueError(f"Protected production ChatGPT conversation {conv_id} cannot be used for canary")

    conv_url = chatgpt_target.get("conversation_url")
    if not conv_url or not CHATGPT_URL_REGEX.match(conv_url):
        raise ValueError(f"Invalid ChatGPT conversation URL format: {conv_url}")

    profile_dir = chatgpt_target.get("profile_dir", str(CANARY_PROFILE))
    if not same_path(profile_dir, CANARY_PROFILE.resolve()):
        raise ValueError(f"ChatGPT target profile '{profile_dir}' does not match dedicated canary profile '{CANARY_PROFILE.resolve()}'")

    # CLI path validation
    cli_p = Path(cli_path)
    if not cli_p.is_file():
        raise ValueError(f"Codex CLI executable '{cli_path}' does not exist")

    # Load template
    tmpl = (template_path or preferred_config_path or (BASE_DIR / "config.canary.example.json")).resolve()
    with open(tmpl, "r", encoding="utf-8") as f:
        data = json.load(f)

    data["canary_mode"] = True

    # Fill Codex fields
    data["codex"]["thread_id"] = thread_id
    data["codex"]["thread_title"] = codex_target.get("title") or "Canary Thread"
    data["codex"]["repo_path"] = str(Path(cwd).resolve()).replace(chr(92), "/")
    data["codex"]["expected_branch"] = "master"
    data["codex"]["cli_path"] = str(cli_p.resolve()).replace(chr(92), "/")
    data["codex"]["integration_mode"] = "cli_queue"

    sessions_dir = codex_target.get("sessions_dir") or str(Path.home() / ".codex" / "sessions")
    session_index_path = codex_target.get("session_index_path") or str(Path.home() / ".codex" / "session_index.jsonl")
    data["codex"]["sessions_dir"] = str(Path(sessions_dir).resolve()).replace(chr(92), "/")
    data["codex"]["session_index_path"] = str(Path(session_index_path).resolve()).replace(chr(92), "/")

    # Fill ChatGPT fields
    data["chatgpt"]["conversation_url"] = conv_url
    data["chatgpt"]["expected_conversation_id"] = conv_id
    data["chatgpt"]["expected_gpt_id"] = chatgpt_target.get("gpt_id")
    data["chatgpt"]["profile_dir"] = str(Path(profile_dir).resolve()).replace(chr(92), "/")
    data["chatgpt"]["cdp_url"] = chatgpt_target.get("cdp_url", "http://127.0.0.1:9223")
    data["chatgpt"]["browser_type"] = "msedge"
    data["chatgpt"]["browser_readiness"] = "ready"
    data["chatgpt"]["human_verification_state"] = "passed"

    # Route and server fields
    data["route"]["manual_approval_required"] = True
    data["route"]["duplicate_detection_policy"] = "hash_and_prefix_match"
    data["route"]["restart_recovery_policy"] = "resume_latest_checkpoint"
    data["server"] = {"host": "127.0.0.1", "port": 8765, "log_level": "info"}

    # Compare the complete immutable configuration, not only the two UUIDs.
    # Prefer the running server's file so an equivalent newer file cannot
    # change the isolated database selected at startup.
    if output_path is None:
        expected = AppConfig(**data)
        candidates = ([Path(preferred_config_path)] if preferred_config_path else [])
        candidates += sorted(BASE_DIR.glob("config.canary.local.*.json"), reverse=True)
        for candidate in candidates:
            try:
                existing = load_explicit_config(candidate, canary=True)
                if compute_config_fingerprint(existing) == compute_config_fingerprint(expected):
                    return candidate
            except (OSError, ValueError):
                continue
        if reuse_only:
            raise ValueError("RUNTIME_RESTART_REQUIRED: no existing configuration matches live targets/settings")

    # Refuse overwrite
    if output_path is None:
        today_str = datetime.now().strftime("%Y%m%d")
        counter = 1
        while True:
            candidate = BASE_DIR / f"config.canary.local.{today_str}-{counter:02d}.json"
            if not candidate.exists():
                output_path = candidate
                break
            counter += 1
    else:
        output_path = Path(output_path)
        if output_path.exists():
            raise FileExistsError(f"Destination config file '{output_path}' already exists; refuse overwrite")

    # Validate before writing: failed discovery must not leave invalid config files.
    validate_canary_config(AppConfig(**data))
    with open(output_path, "x", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Validate output
    loaded = load_explicit_config(output_path, canary=True)
    validate_canary_config(loaded)
    return output_path


# --- Section 8: Run the Official Read-Only Preflight Automatically ---

def run_official_preflight(config_path: Path, python_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute: python.exe run.py --canary --config <new-local-config> --check
    """
    py = python_path or str(BASE_DIR / ".venv" / "Scripts" / "python.exe")
    if not Path(py).is_file():
        py = sys.executable

    try:
        res = subprocess.run(
            [py, "run.py", "--canary", "--config", str(config_path), "--check"],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=60,
        )

        if res.returncode == 0:
            return {
                "exit_code": 0,
                "status": "verified",
                "output": res.stdout.strip(),
            }
        else:
            return {
                "exit_code": res.returncode,
                "status": "failed",
                "error": res.stderr.strip() or res.stdout.strip(),
            }
    except Exception as exc:
        return {
            "exit_code": -1,
            "status": "failed",
            "error": str(exc),
        }


# --- Overall Bootstrap Coordinator ---

async def run_canary_bootstrap(
    workspace: Optional[Path] = None,
    profile_dir: Optional[Path] = None,
    sessions_dir: Optional[Path] = None,
    session_index_path: Optional[Path] = None,
    cdp_port: int = 9223,
    auto_launch_edge: bool = True,
    create_config: bool = True,
    mock_chatgpt_pages: Optional[List[Dict[str, Any]]] = None,
    mock_cli_info: Optional[Dict[str, Any]] = None,
    active_config_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Execute all safe local bootstrap steps:
    1. Resolve Codex CLI
    2. Ensure isolated Canary Edge environment
    3. Detect ChatGPT human prerequisites and identity
    4. Discover Codex canary target
    5. If all ready, generate local config (or re-verify existing) and run preflight
    """
    result: Dict[str, Any] = {
        "verdict": CANARY_BOOTSTRAP_BLOCKED,
        "stage": "cli_resolution",
        "error_code": None,
        "blocker": None,
        "human_action": None,
        "retryable": False,
        "diagnostic_details": {},
        "automated_steps": [],
        "manual_prerequisites": [],
        "discovered_identities": {},
        "config_path": None,
        "preflight_result": None,
        "subsystems": {
            "codex": {},
            "chatgpt": {},
            "cli": {},
            "edge": {},
        },
    }

    # Step 1: Codex CLI
    result["stage"] = "cli_resolution"
    active = load_explicit_config(active_config_path, canary=True) if active_config_path else None
    if active:
        sessions_dir = Path(active.codex.sessions_dir)
        session_index_path = Path(active.codex.session_index_path)
        profile_dir = Path(active.chatgpt.profile_dir)
        cdp_port = urlsplit(active.chatgpt.cdp_url).port
    cli_info = mock_cli_info or await asyncio.to_thread(find_and_verify_codex_cli, *([Path(active.codex.cli_path)] if active else []))
    result["subsystems"]["cli"] = cli_info
    if cli_info.get("status") != "verified":
        result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
        result["error_code"] = "CLI_NOT_FOUND" if cli_info.get("status") == "not_found" else "CLI_CAPABILITY_MISSING"
        result["blocker"] = cli_info.get("error", "Codex CLI could not be verified")
        result["retryable"] = True
        result["diagnostic_details"] = {"status": cli_info.get("status")}
        save_bootstrap_state(result)
        return result
    result["automated_steps"].append(f"Resolved and verified Codex CLI binary: {cli_info['cli_path']}")
    result["discovered_identities"]["cli_path"] = cli_info["cli_path"]

    # Step 2: Edge instance & profile
    result["stage"] = "browser_environment"
    edge_info = await asyncio.to_thread(check_or_launch_canary_edge, profile_dir=profile_dir, cdp_port=cdp_port, auto_launch=auto_launch_edge)
    result["subsystems"]["edge"] = edge_info
    if edge_info.get("status") in ("port_conflict", "blocked", "launch_failed", "launch_timeout", "missing"):
        result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
        status_name = str(edge_info.get("status", "error")).upper()
        result["error_code"] = f"EDGE_{status_name}"
        result["blocker"] = edge_info.get("error") or f"Edge browser check failed: {edge_info.get('status')}"
        result["retryable"] = True
        result["diagnostic_details"] = {"status": edge_info.get("status"), "profile_dir": edge_info.get("profile_dir")}
        save_bootstrap_state(result)
        return result
    result["automated_steps"].append(f"Verified canary Edge browser environment (status: {edge_info.get('status')})")
    result["discovered_identities"]["browser_profile"] = edge_info["profile_dir"]
    result["discovered_identities"]["cdp_url"] = edge_info.get("cdp_url", f"http://127.0.0.1:{cdp_port}")

    # Step 3: ChatGPT Canary Identity & Prerequisites
    result["stage"] = "chatgpt_discovery"
    chatgpt_info = await inspect_chatgpt_canary(
        cdp_url=result["discovered_identities"]["cdp_url"],
        profile_dir=profile_dir,
        mock_pages=mock_chatgpt_pages,
    )
    result["subsystems"]["chatgpt"] = chatgpt_info
    if chatgpt_info.get("status") == HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN:
        result["verdict"] = HUMAN_ACTION_REQUIRED_CHATGPT_LOGIN
        result["error_code"] = "CHATGPT_LOGIN_REQUIRED"
        result["blocker"] = chatgpt_info.get("error", "ChatGPT login required in canary browser")
        result["human_action"] = chatgpt_info.get("human_action") or ACTION_CHATGPT_LOGIN
        result["manual_prerequisites"].append(result["human_action"])
        result["retryable"] = True
        result["diagnostic_details"] = {"cdp_url": result["discovered_identities"]["cdp_url"]}
        save_bootstrap_state(result)
        return result
    elif chatgpt_info.get("status") == HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT:
        result["verdict"] = HUMAN_ACTION_REQUIRED_CREATE_CHATGPT_CANARY_CHAT
        result["error_code"] = "CHATGPT_CONVERSATION_REQUIRED"
        result["blocker"] = chatgpt_info.get("error", "New blank ChatGPT conversation required in canary browser")
        result["human_action"] = chatgpt_info.get("human_action") or ACTION_CREATE_CHATGPT_CANARY_CHAT
        result["manual_prerequisites"].append(result["human_action"])
        result["retryable"] = True
        result["diagnostic_details"] = {"cdp_url": result["discovered_identities"]["cdp_url"]}
        save_bootstrap_state(result)
        return result
    elif chatgpt_info.get("status") != "discovered":
        result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
        err_msg = chatgpt_info.get("error") or "ChatGPT canary inspection failed"
        if "Multiple" in err_msg:
            result["error_code"] = "CHATGPT_MULTIPLE_CONVERSATIONS"
        elif "production" in err_msg.lower():
            result["error_code"] = "CHATGPT_PRODUCTION_CONVERSATION_REJECTED"
        else:
            result["error_code"] = "CHATGPT_INSPECTION_FAILED"
        result["blocker"] = err_msg
        result["human_action"] = chatgpt_info.get("human_action")
        result["retryable"] = True
        result["diagnostic_details"] = {"error": err_msg, "candidates": chatgpt_info.get("candidates")}
        save_bootstrap_state(result)
        return result

    result["automated_steps"].append(f"Discovered and verified ChatGPT canary conversation: {chatgpt_info['conversation_url']}")
    result["discovered_identities"]["chatgpt"] = chatgpt_info

    # Step 4: Codex Canary Target
    result["stage"] = "codex_discovery"
    codex_info = await asyncio.to_thread(discover_codex_canary_target,
        workspace=workspace,
        sessions_dir=sessions_dir,
        session_index_path=session_index_path,
    )
    result["subsystems"]["codex"] = codex_info
    if codex_info.get("status") == HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD:
        result["verdict"] = HUMAN_ACTION_REQUIRED_CREATE_CODEX_CANARY_THREAD
        result["error_code"] = "CODEX_CANARY_THREAD_REQUIRED"
        result["blocker"] = codex_info.get("error", "Codex canary thread required")
        result["human_action"] = codex_info.get("human_action") or ACTION_CREATE_CODEX_CANARY_THREAD
        result["manual_prerequisites"].append(result["human_action"])
        result["retryable"] = True
        result["diagnostic_details"] = {"workspace": codex_info.get("workspace")}
        save_bootstrap_state(result)
        return result
    elif codex_info.get("status") != "discovered":
        result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
        err_msg = codex_info.get("error") or "Codex canary target discovery failed"
        if "Multiple" in err_msg:
            result["error_code"] = "CODEX_MULTIPLE_SESSIONS"
        elif "Invalid" in err_msg:
            result["error_code"] = "CODEX_INVALID_THREAD_UUID"
        elif "directory does not exist" in err_msg:
            result["error_code"] = "CODEX_SESSIONS_DIR_MISSING"
        else:
            result["error_code"] = "CODEX_DISCOVERY_FAILED"
        result["blocker"] = err_msg
        result["human_action"] = codex_info.get("human_action")
        result["candidates"] = codex_info.get("candidates")
        result["retryable"] = True
        result["diagnostic_details"] = {"error": err_msg, "candidates": codex_info.get("candidates")}
        save_bootstrap_state(result)
        return result

    result["automated_steps"].append(f"Discovered and verified Codex canary thread: {codex_info['thread_id']} (title: {codex_info.get('title')})")
    result["discovered_identities"]["codex"] = codex_info

    # Step 5: Generate Local Canary Configuration & Run Preflight
    if create_config or active:
        result["stage"] = "configuration_generation"
        try:
            cfg_path = generate_canary_config(
                codex_target=codex_info,
                chatgpt_target=chatgpt_info,
                cli_path=cli_info["cli_path"],
                preferred_config_path=active_config_path,
                reuse_only=not create_config,
            )
            result["config_path"] = str(cfg_path)
            result["config_fingerprint"] = compute_config_fingerprint(load_explicit_config(cfg_path, canary=True))
            result["automated_steps"].append(f"Selected matching local canary configuration: {cfg_path.name}")

            result["stage"] = "preflight_check"
            preflight = await asyncio.to_thread(run_official_preflight, cfg_path)
            result["preflight_result"] = preflight
            if preflight.get("status") == "verified":
                result["verdict"] = CANARY_PREFLIGHT_VERIFIED
                result["stage"] = "preflight_verified"
                result["error_code"] = None
                result["blocker"] = None
                result["retryable"] = False
                result["automated_steps"].append("Official preflight verification passed with status: verified")
            else:
                result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
                result["stage"] = "preflight_check"
                result["error_code"] = "PREFLIGHT_FAILED"
                result["blocker"] = f"Official preflight check failed: {preflight.get('error')}"
                result["retryable"] = True
                result["diagnostic_details"] = preflight
        except Exception as exc:
            result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
            result["error_code"] = "CONFIG_GENERATION_FAILED"
            result["blocker"] = f"Configuration generation failed: {exc}"
            result["retryable"] = False
            result["diagnostic_details"] = {"exception": str(exc)}
    else:
        # Read-only status check: inspect whether a matching configuration already exists
        matching_cfg = find_matching_canary_config(codex_info, chatgpt_info)
        if matching_cfg:
            result["config_path"] = str(matching_cfg)
            result["stage"] = "preflight_check"
            preflight = await asyncio.to_thread(run_official_preflight, matching_cfg)
            result["preflight_result"] = preflight
            if preflight.get("status") == "verified":
                result["verdict"] = CANARY_PREFLIGHT_VERIFIED
                result["stage"] = "preflight_verified"
                result["error_code"] = None
                result["blocker"] = None
                result["retryable"] = False
                result["automated_steps"].append(f"Verified existing matching canary configuration: {matching_cfg.name}")
                result["automated_steps"].append("Official preflight verification passed with status: verified")
            else:
                result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
                result["stage"] = "preflight_check"
                result["error_code"] = "PREFLIGHT_FAILED"
                result["blocker"] = f"Official preflight check failed on existing config {matching_cfg.name}: {preflight.get('error')}"
                result["retryable"] = True
                result["diagnostic_details"] = preflight
        else:
            result["verdict"] = CANARY_BOOTSTRAP_BLOCKED
            result["stage"] = "configuration_generation"
            result["error_code"] = "CONFIG_NOT_YET_GENERATED"
            result["blocker"] = "All canary targets discovered, but local configuration has not been generated yet. Click 'Read-Only Recheck' to generate local configuration and verify preflight."
            result["retryable"] = True
            result["diagnostic_details"] = {"discovered_identities": result["discovered_identities"]}

    # Fail-safe: A result must NEVER claim CANARY_BOOTSTRAP_BLOCKED with blocker=None or empty
    if result.get("config_path") and not result.get("config_fingerprint"):
        result["config_fingerprint"] = compute_config_fingerprint(load_explicit_config(Path(result["config_path"]), canary=True))
    if active and result.get("config_fingerprint") != compute_config_fingerprint(active):
        result.update(verdict="RUNTIME_RESTART_REQUIRED", error_code="RUNTIME_RESTART_REQUIRED",
                      blocker="Bootstrap configuration differs from the effective runtime; restart with an explicitly selected config",
                      stage="runtime_restart_required")
    if result["verdict"] == CANARY_BOOTSTRAP_BLOCKED:
        if not result.get("blocker"):
            result["blocker"] = f"Bootstrap blocked at stage '{result.get('stage')}' without explicit blocker"
        if not result.get("error_code"):
            result["error_code"] = "BOOTSTRAP_BLOCKED"

    save_bootstrap_state(result)
    return result

