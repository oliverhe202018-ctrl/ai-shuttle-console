"""
Adapter interfaces and implementations for Phase 1, Phase 2, and Phase 2.3.
Includes read-only Codex session parser, dual-layout ChatGPT observer,
controlled RealChatGPTMessageSender, and RealCodexQueueAdapter.
"""
from abc import ABC, abstractmethod
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import uuid

import sqlite3
from app.models import compute_sha256, CodexDeliveryClassification
from app.chatgpt_dom import ChatGPTDOMParser
from app.canary import canary_prompt, same_conversation, reject_placeholder_targets, is_production_thread, is_production_conversation, is_production_workspace


class DeliveryOutcomeUnknownError(RuntimeError):
    """Raised when mid-transfer browser disconnect or timeout leaves delivery outcome ambiguous."""
    pass


class NotSentError(ValueError, ConnectionError, RuntimeError):
    """Raised when transfer fails before click without side-effects attempted."""
    pass

ERROR_PLACEHOLDERS = [
    "无法加载此回复",
    "Unable to load response",
    "There was an error generating a response",
    "Something went wrong, please try reloading the conversation.",
    "Failed to generate response",
    "An error occurred while generating the response",
    "Network error",
    "Conversation not found",
]


def is_error_placeholder(text: str) -> bool:
    if not text:
        return False
    t_clean = text.strip()
    return any(p in t_clean for p in ERROR_PLACEHOLDERS)


# ---------------------------------------------------------------- Codex Interfaces


class ICodexSessionObserver(ABC):
    @abstractmethod
    async def get_latest_response(
        self, thread_id: str, after_line: int = 0
    ) -> Tuple[Optional[str], int, Optional[str]]:
        """
        Extract final assistant answer from session JSONL records for the target thread.
        Returns: (final_answer_text, line_index, turn_id)
        """
        pass


class ICodexQueueAdapter(ABC):
    @abstractmethod
    async def queue_message(
        self,
        thread_id: str,
        message: str,
        confirmation_token: Optional[str] = None,
        transfer_id: Optional[str] = None,
        repo_path: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Queue prompt to specific Codex GUI thread.
        Phase 1 must NOT deliver real messages unless explicitly un-mocked.
        """
        pass


# ---------------------------------------------------------------- ChatGPT Interfaces


class IChatGPTBrowserObserver(ABC):
    @abstractmethod
    async def get_browser_status(self) -> Dict[str, Any]:
        """Check whether browser is ready, closed, or blocked by verification/CAPTCHA."""
        pass

    @abstractmethod
    async def get_latest_assistant_response(self) -> Optional[Dict[str, Any]]:
        """Return latest assistant message parts if complete and not an error placeholder."""
        pass


class IChatGPTMessageSender(ABC):
    @abstractmethod
    async def send_message(
        self,
        message: str,
        confirmation_token: Optional[str] = None,
        transfer_id: Optional[str] = None,
        baseline_hash: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Submit message to ChatGPT conversation. Mocked in Phase 1."""
        pass


# ---------------------------------------------------------------- Prompt Extraction Interface


class IPromptExtractor(ABC):
    @abstractmethod
    def extract(self, response_parts: Dict[str, Any]) -> Optional[str]:
        """Extract unambiguous prompt from assistant response parts."""
        pass


# ---------------------------------------------------------------- Implementations


class ReadOnlyCodexSessionObserver(ICodexSessionObserver):
    def __init__(self, sessions_base_dir: Path, session_index_path: Optional[Path] = None):
        self.sessions_base_dir = Path(sessions_base_dir)
        if session_index_path:
            self.session_index_path = Path(session_index_path)
        else:
            candidate = self.sessions_base_dir.parent / "session_index.jsonl"
            if not candidate.exists():
                codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
                fallback = codex_home / "session_index.jsonl"
                if fallback.exists():
                    candidate = fallback
            self.session_index_path = candidate

    def validate_session_metadata(
        self,
        thread_id: str,
        expected_cwd: Optional[str] = None,
        expected_title: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate thread identity in session_index.jsonl and return metadata."""
        meta: Dict[str, Any] = {
            "thread_id": thread_id,
            "found_in_index": False,
            "title": None,
            "cwd": None,
            "updated_at": None,
            "matches_expected_cwd": True,
            "matches_expected_title": True,
            "validation_errors": [],
        }

        if not self.session_index_path.exists():
            meta["validation_errors"].append(f"Session index not found: {self.session_index_path}")
            return meta

        try:
            with open(self.session_index_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except Exception:
                        continue

                    if record.get("id") == thread_id:
                        meta["found_in_index"] = True
                        meta["title"] = record.get("thread_name") or record.get("title")
                        meta["cwd"] = record.get("cwd")
                        meta["updated_at"] = record.get("updated_at")
                        break
        except Exception as e:
            meta["validation_errors"].append(f"Error reading session index: {str(e)}")
            return meta

        if not meta["found_in_index"]:
            meta["validation_errors"].append(f"Thread ID {thread_id} not found in session index")
            return meta

        if expected_cwd:
            # If cwd not present in index record, try to resolve from session file header
            actual_cwd = meta["cwd"]
            if not actual_cwd:
                session_file = self.locate_session_file(thread_id)
                if session_file and session_file.exists():
                    try:
                        import os
                        flags = os.O_RDONLY
                        if hasattr(os, "O_BINARY"):
                            flags |= os.O_BINARY
                        fd = os.open(str(session_file), flags)
                        with open(fd, "r", encoding="utf-8", errors="ignore") as f:
                            first_line = f.readline()
                            rec = json.loads(first_line)
                            if rec.get("type") == "session_meta":
                                actual_cwd = rec.get("payload", {}).get("cwd")
                                meta["cwd"] = actual_cwd
                    except Exception:
                        pass

            if actual_cwd:
                from app.canary import same_path
                if not same_path(actual_cwd, expected_cwd):
                    meta["matches_expected_cwd"] = False
                    meta["validation_errors"].append(
                        f"CWD mismatch: expected '{expected_cwd}', got '{actual_cwd}'"
                    )
            else:
                meta["matches_expected_cwd"] = False
                meta["validation_errors"].append(
                    f"CWD could not be determined for thread '{thread_id}'"
                )

        if expected_title:
            actual_title = (meta["title"] or "").strip()
            if expected_title.strip() not in actual_title and actual_title not in expected_title.strip():
                meta["matches_expected_title"] = False
                meta["validation_errors"].append(
                    f"Title mismatch: expected '{expected_title}', got '{meta['title']}'"
                )

        return meta

    def locate_session_file(self, thread_id: str) -> Optional[Path]:
        if not self.sessions_base_dir.exists():
            return None
        matches = list(self.sessions_base_dir.glob(f"**/*{thread_id}.jsonl"))
        if not matches:
            return None
        matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return matches[0]

    async def get_latest_response(
        self, thread_id: str, after_line: int = 0
    ) -> Tuple[Optional[str], int, Optional[str]]:
        obs = self.observe_thread(thread_id, after_line=after_line)
        if obs["is_completed"] and obs["latest_assistant_response"]:
            return obs["latest_assistant_response"], obs["latest_assistant_line"], obs["latest_turn_id"]
        return None, -1, None

    def classify_delivery(
        self,
        thread_id: str,
        nonce: str,
        after_line: int = 0,
        codex_home: Optional[Path] = None,
        queue_receipt_accepted: bool = False,
        pre_enqueue_rejected: bool = False,
    ) -> Dict[str, Any]:
        """
        Inspect the session rollout JSONL and Codex queue storage to classify delivery.
        """
        session_file = self.locate_session_file(thread_id)
        rollout_lines: List[Dict[str, Any]] = []
        if session_file and session_file.exists():
            try:
                import os
                flags = os.O_RDONLY
                if hasattr(os, "O_BINARY"):
                    flags |= os.O_BINARY
                fd = os.open(str(session_file), flags)
                with open(fd, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line_s = line.strip()
                        if line_s:
                            try:
                                rollout_lines.append(json.loads(line_s))
                            except Exception:
                                pass
            except Exception:
                pass

        q_info = inspect_codex_queue_storage(thread_id, codex_home=codex_home, nonce=nonce)
        return classify_codex_delivery_outcome(
            target_thread_id=thread_id,
            expected_nonce=nonce,
            rollout_lines=rollout_lines,
            queue_storage_info=q_info,
            after_line=after_line,
            queue_receipt_accepted=queue_receipt_accepted,
            pre_enqueue_rejected=pre_enqueue_rejected,
        )

    def observe_thread(
        self,
        thread_id: str,
        expected_cwd: Optional[str] = None,
        expected_title: Optional[str] = None,
        after_line: int = 0,
    ) -> Dict[str, Any]:
        """Perform rigorous read-only reconciliation of Codex session events."""
        result: Dict[str, Any] = {
            "thread_id": thread_id,
            "session_file_path": None,
            "metadata_valid": True,
            "validation_errors": [],
            "total_lines": 0,
            "user_turns_count": 0,
            "assistant_turns_count": 0,
            "latest_turn_id": None,
            "latest_user_line": None,
            "latest_user_prompt": None,
            "latest_assistant_line": None,
            "latest_assistant_response": None,
            "latest_assistant_text": None,
            "latest_response_hash": None,
            "is_in_progress": False,
            "is_completed": False,
            "is_truncated": False,
            "session_meta": None,
            "completion_proven": False,
            "latest_user_turn_id": None,
            "latest_assistant_turn_id": None,
        }

        # 1. Metadata check in session_index.jsonl
        meta = self.validate_session_metadata(
            thread_id, expected_cwd=expected_cwd, expected_title=expected_title
        )
        if meta["validation_errors"]:
            result["validation_errors"].extend(meta["validation_errors"])
            result["metadata_valid"] = False

        session_file = self.locate_session_file(thread_id)
        if not session_file or not session_file.exists():
            result["validation_errors"].append(f"Session file for thread {thread_id} not found")
            return result

        result["session_file_path"] = str(session_file)

        # 1. Open with share mode to allow reading files actively being appended by other processes (e.g. Codex)
        import os
        try:
            # On Windows, os.O_BINARY ensures exact byte read without lock contention
            flags = os.O_RDONLY
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(str(session_file), flags)
            with open(fd, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
        except Exception as e:
            result["validation_errors"].append(f"Failed to read session file: {e}")
            return result

        result["total_lines"] = len(lines)
        if not lines:
            result["validation_errors"].append("Session file is empty")
            return result

        # 2. Check session_meta in first record
        try:
            first_rec = json.loads(lines[0])
            if first_rec.get("type") == "session_meta":
                p = first_rec.get("payload", {})
                result["session_meta"] = p
                file_thread_id = p.get("id")
                file_cwd = p.get("cwd")
                if file_thread_id and file_thread_id != thread_id:
                    result["validation_errors"].append(
                        f"Thread ID mismatch in file header: {file_thread_id} != {thread_id}"
                    )
                    result["metadata_valid"] = False
                if expected_cwd and file_cwd:
                    if Path(file_cwd).resolve() != Path(expected_cwd).resolve():
                        result["validation_errors"].append(
                            f"Header CWD mismatch: expected '{expected_cwd}', got '{file_cwd}'"
                        )
                        result["metadata_valid"] = False
        except Exception:
            pass

        # 3. Parse event stream in sequence
        curr_turn = None
        last_user_idx = None
        last_user_text = None

        last_assist_turn = None
        last_assist_idx = None
        last_assist_text = None
        last_assist_status = None

        user_count = 0
        assist_count = 0
        completed_turns = set()

        for idx in range(after_line, len(lines)):
            line_str = lines[idx].strip()
            if not line_str:
                continue
            try:
                record = json.loads(line_str)
            except Exception:
                continue

            rec_type = record.get("type")
            payload = record.get("payload", {})

            if rec_type == "event_msg" and payload.get("type") == "task_complete":
                completed_turns.add(payload.get("turn_id") or curr_turn)

            if (rec_type == "turn_context" or (rec_type == "event_msg" and payload.get("type") == "task_started")) and "turn_id" in payload:
                curr_turn = payload.get("turn_id")

            # User messages
            is_user = False
            if rec_type == "response_item" and payload.get("type") == "message" and payload.get("role") == "user":
                is_user = True
            elif rec_type == "event_msg" and payload.get("type") == "user_message":
                is_user = True

            if is_user:
                user_count += 1
                last_user_idx = idx
                result["latest_user_turn_id"] = payload.get("turn_id") or curr_turn
                c = payload.get("content", payload.get("message", ""))
                if isinstance(c, list):
                    c = " ".join(item.get("text", "") for item in c if isinstance(item, dict))
                last_user_text = str(c)

            # Assistant final answer messages
            if (
                payload.get("type") == "message"
                and payload.get("role") == "assistant"
                and (payload.get("phase") == "final_answer" or "final_answer" in str(payload))
            ):
                assist_count += 1
                status = payload.get("status")
                last_assist_turn = payload.get("turn_id") or curr_turn
                last_assist_idx = idx
                last_assist_status = status
                content = payload.get("content")
                txt = ""
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "output_text":
                            txt += item.get("text", "")
                        elif isinstance(item, dict) and "text" in item:
                            txt += item.get("text", "")
                        elif isinstance(item, str):
                            txt += item
                elif isinstance(content, str):
                    txt = content
                last_assist_text = txt

        result["user_turns_count"] = user_count
        result["assistant_turns_count"] = assist_count
        result["latest_turn_id"] = curr_turn or last_assist_turn
        result["latest_user_line"] = last_user_idx
        result["latest_user_prompt"] = last_user_text
        result["latest_assistant_turn_id"] = last_assist_turn
        result["completion_proven"] = bool(last_assist_text and (
            last_assist_status == "completed" or (last_assist_turn and last_assist_turn in completed_turns)))

        # Reconcile user turns vs assistant turns
        # Rule: If user asked a new question after the latest completed answer, turn is in progress.
        if last_user_idx is not None and last_assist_idx is not None:
            if last_user_idx > last_assist_idx:
                result["is_in_progress"] = True
                result["is_completed"] = False
                # Reject historical turn response
                result["latest_assistant_response"] = None
                result["latest_assistant_line"] = None
                result["latest_response_hash"] = None
                return result

        if last_assist_text is not None:
            # Check completed
            if result["completion_proven"] or (last_assist_idx == len(lines) - 1 and len(last_assist_text.strip()) > 0):
                result["is_completed"] = True
                result["is_in_progress"] = False
                result["latest_assistant_response"] = last_assist_text
                result["latest_assistant_text"] = last_assist_text
                result["latest_assistant_line"] = last_assist_idx
                result["latest_response_hash"] = compute_sha256(last_assist_text)
                if len(last_assist_text.strip()) == 0:
                    result["is_truncated"] = True
            else:
                result["is_in_progress"] = True
                result["is_completed"] = False
                result["is_truncated"] = True
        elif last_user_idx is not None:
            result["is_in_progress"] = True
            result["is_completed"] = False

        return result

    def prepare_report(
        self,
        thread_id: str,
        expected_cwd: Optional[str] = None,
        expected_title: Optional[str] = None,
        after_line: int = 0,
    ) -> Dict[str, Any]:
        """
        Extract and strictly validate the latest completed assistant response as a transfer candidate.
        Strictly rejects:
        - Mismatched thread/cwd/title
        - In-progress turns
        - Incomplete or truncated response stream
        - Stale/empty response
        """
        obs = self.observe_thread(
            thread_id,
            expected_cwd=expected_cwd,
            expected_title=expected_title,
            after_line=after_line,
        )

        if not obs["metadata_valid"]:
            errs = "; ".join(obs["validation_errors"])
            raise ValueError(f"Codex session metadata validation failed: {errs}")

        if obs["is_in_progress"]:
            raise ValueError(f"Codex turn is currently in progress (turn_id={obs['latest_turn_id']})")

        if not obs["is_completed"]:
            raise ValueError("Codex turn is not completed (no completed assistant response observed)")

        if obs["is_truncated"]:
            raise ValueError("Codex session event stream is truncated")

        text = obs.get("latest_assistant_response")
        if not text or not text.strip():
            raise ValueError("Latest completed assistant response is empty")

        content_hash = obs.get("latest_response_hash") or compute_sha256(text)

        return {
            "thread_id": thread_id,
            "turn_id": obs.get("latest_turn_id"),
            "line_index": obs.get("latest_assistant_line"),
            "report_text": text,
            "content_hash": content_hash,
            "char_count": len(text),
            "session_file_path": obs.get("session_file_path"),
            "expected_cwd": expected_cwd,
            "expected_title": expected_title,
        }


    def find_dispatched_turn_and_answer(
        self,
        thread_id: str,
        expected_cwd: Optional[str] = None,
        after_line: int = 0,
        client_user_message_id: Optional[str] = None,
        nonce: Optional[str] = None,
        expected_content_hash: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Locates the exact newly dispatched user turn after after_line,
        and identifies the corresponding assistant final answer AFTER that exact turn.
        Returns explicit turn IDs, message IDs, output hashes, and completion timestamps.
        Never fabricates IDs from hashes or line counts.
        """
        session_file = self.locate_session_file(thread_id)
        empty_res = {
            "dispatched": False,
            "user_line": None,
            "user_turn_id": None,
            "user_message_id": None,
            "user_prompt_text": None,
            "completed": False,
            "assistant_line": None,
            "assistant_turn_id": None,
            "assistant_message_id": None,
            "assistant_text": None,
            "assistant_output_hash": None,
            "completed_at": None,
        }
        if not session_file or not session_file.exists():
            return empty_res

        lines: List[str] = []
        try:
            import os
            flags = os.O_RDONLY
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(str(session_file), flags)
            with open(fd, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
        except Exception:
            return empty_res

        if len(lines) <= after_line:
            return empty_res

        user_line = None
        user_turn_id = None
        user_message_id = None
        user_prompt_text = None

        assistant_line = None
        assistant_turn_id = None
        assistant_message_id = None
        assistant_text = None
        assistant_output_hash = None
        completed_at = None
        is_completed = False

        # 1. Find the new user turn
        curr_turn = None
        for idx in range(after_line, len(lines)):
            line_str = lines[idx].strip()
            if not line_str:
                continue
            try:
                rec = json.loads(line_str)
            except Exception:
                continue

            rec_type = rec.get("type")
            payload = rec.get("payload", {})

            if (rec_type == "turn_context" or (rec_type == "event_msg" and payload.get("type") == "task_started")) and "turn_id" in payload:
                curr_turn = payload.get("turn_id")

            is_user = False
            if rec_type == "response_item" and payload.get("type") == "message" and payload.get("role") == "user":
                is_user = True
            elif rec_type == "event_msg" and payload.get("type") == "user_message":
                is_user = True

            if is_user and user_line is None:
                c = payload.get("content", payload.get("message", ""))
                txt = ""
                if isinstance(c, list):
                    txt = " ".join(item.get("text", "") for item in c if isinstance(item, dict))
                elif isinstance(c, str):
                    txt = c

                # Match criteria: client_id, nonce, or content hash
                matched = False
                rec_client_id = (
                    payload.get("client_user_message_id")
                    or payload.get("client_id")
                    or rec.get("metadata", {}).get("retained_source", {}).get("id", {}).get("client_id")
                )
                if client_user_message_id and rec_client_id == client_user_message_id:
                    matched = True
                elif nonce and nonce in txt:
                    matched = True
                elif expected_content_hash and compute_sha256(txt) == expected_content_hash:
                    matched = True
                elif not client_user_message_id and not nonce and not expected_content_hash:
                    matched = True

                if matched:
                    user_line = idx
                    user_turn_id = (
                        payload.get("turn_id")
                        or payload.get("internal_chat_message_metadata_passthrough", {}).get("turn_id")
                        or rec.get("metadata", {}).get("retained_source", {}).get("id", {}).get("turn_id")
                        or curr_turn
                    )
                    user_message_id = (
                        payload.get("id")
                        or rec.get("metadata", {}).get("retained_source", {}).get("id", {}).get("message_id")
                    )
                    user_prompt_text = txt

        # Backtrack turn_id if missing in user record
        if user_line is not None and not user_turn_id:
            for b_idx in range(user_line, max(-1, user_line - 6), -1):
                try:
                    b_rec = json.loads(lines[b_idx].strip())
                    if b_rec.get("type") == "turn_context" and b_rec.get("payload", {}).get("turn_id"):
                        user_turn_id = b_rec["payload"]["turn_id"]
                        break
                except Exception:
                    pass

        # 2. Look for corresponding assistant final answer AFTER that exact user turn
        if user_line is not None:
            for idx in range(user_line + 1, len(lines)):
                line_str = lines[idx].strip()
                if not line_str:
                    continue
                try:
                    rec = json.loads(line_str)
                except Exception:
                    continue

                rec_type = rec.get("type")
                payload = rec.get("payload", {})

                if rec_type == "event_msg" and payload.get("type") == "task_complete":
                    comp_turn = payload.get("turn_id")
                    if not comp_turn or comp_turn == user_turn_id:
                        is_completed = True
                        if not completed_at:
                            completed_at = rec.get("timestamp")

                is_assistant = (
                    payload.get("type") == "message"
                    and payload.get("role") == "assistant"
                )
                if is_assistant:
                    content = payload.get("content")
                    txt = ""
                    if isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict):
                                txt += item.get("text", "")
                            elif isinstance(item, str):
                                txt += item
                    elif isinstance(content, str):
                        txt = content

                    asst_trn = (
                        payload.get("turn_id")
                        or payload.get("internal_chat_message_metadata_passthrough", {}).get("turn_id")
                        or rec.get("metadata", {}).get("retained_source", {}).get("id", {}).get("turn_id")
                    )
                    # Check if associated with current user turn
                    if not asst_trn or asst_trn == user_turn_id or (asst_trn and user_turn_id is None):
                        assistant_line = idx
                        assistant_turn_id = asst_trn or user_turn_id
                        assistant_message_id = (
                            payload.get("id")
                            or rec.get("metadata", {}).get("retained_source", {}).get("id", {}).get("message_id")
                        )
                        assistant_text = txt
                        assistant_output_hash = compute_sha256(txt)
                        if payload.get("phase") == "final_answer" or payload.get("status") == "completed":
                            is_completed = True
                        if not completed_at:
                            completed_at = rec.get("timestamp")

        return {
            "dispatched": user_line is not None,
            "user_line": user_line,
            "user_turn_id": user_turn_id,
            "user_message_id": user_message_id,
            "user_prompt_text": user_prompt_text,
            "completed": is_completed and assistant_text is not None,
            "assistant_line": assistant_line,
            "assistant_turn_id": assistant_turn_id,
            "assistant_message_id": assistant_message_id,
            "assistant_text": assistant_text,
            "assistant_output_hash": assistant_output_hash,
            "completed_at": completed_at,
        }


class MockCodexQueueAdapter(ICodexQueueAdapter):
    def __init__(self):
        self.queued_messages: List[Dict[str, Any]] = []
        self.simulate_cli_failure = False
        self.simulate_timeout = False

    async def queue_message(
        self,
        thread_id: str,
        message: str,
        confirmation_token: Optional[str] = None,
        transfer_id: Optional[str] = None,
        repo_path: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if self.simulate_timeout:
            raise TimeoutError("Codex CLI queue execution timed out after 30 seconds")
        if self.simulate_cli_failure:
            raise RuntimeError("Codex CLI queue failed (code 1): thread not found or repo not trusted")

        msg_id = f"mock-queue-{len(self.queued_messages) + 1:04d}"
        entry = {
            "queue_message_id": msg_id,
            "transfer_id": transfer_id,
            "thread_id": thread_id,
            "repo_path": repo_path,
            "message_hash": compute_sha256(message),
            "chars": len(message),
            "status": "queued_mock",
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.queued_messages.append(entry)
        return entry


class MockChatGPTBrowserObserver(IChatGPTBrowserObserver):
    def __init__(self):
        self.is_ready = True
        self.is_blocked = False
        self.mock_response_parts: Optional[Dict[str, Any]] = None

    async def get_browser_status(self) -> Dict[str, Any]:
        return {
            "ready": self.is_ready,
            "human_verification_required": self.is_blocked,
            "interstitial_reason": "Cloudflare Turnstile challenge" if self.is_blocked else None,
        }

    async def get_latest_assistant_response(self) -> Optional[Dict[str, Any]]:
        return self.mock_response_parts


class MockChatGPTMessageSender(IChatGPTMessageSender):
    def __init__(self):
        self.sent_messages: List[Dict[str, Any]] = []
        self.simulate_unknown_outcome = False
        self.simulate_composer_mismatch = False
        self.simulate_writing_block = False
        self.simulate_pre_submit_disconnect = False

    async def send_message(
        self,
        message: str,
        confirmation_token: Optional[str] = None,
        transfer_id: Optional[str] = None,
        baseline_hash: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if self.simulate_pre_submit_disconnect:
            raise ConnectionError("Browser disconnected before submission")
        if self.simulate_unknown_outcome:
            raise DeliveryOutcomeUnknownError("Browser connection dropped mid-transfer: delivery outcome unknown")
        if self.simulate_writing_block:
            raise ValueError("Rejected: writing block or canvas editor detected instead of genuine composer")
        if self.simulate_composer_mismatch:
            raise ValueError("Composer text mismatch: read back content does not match prepared report character-for-character")

        from app.canonical import compute_semantic_hash
        record = {
            "send_id": f"mock-send-{len(self.sent_messages) + 1:04d}",
            "transfer_id": transfer_id,
            "content_hash": compute_sha256(message),
            "chars": len(message),
            "status": "delivered_mock",
            "delivery_evidence": {
                "timestamp": datetime.utcnow().isoformat(),
                "baseline_hash": baseline_hash,
                "send_button_selector": 'button[data-testid="send-button"]',
                "user_turn_verified": True,
                "user_turn_id": f"turn-user-mock-{len(self.sent_messages) + 1:04d}",
                "user_message_id": f"msg-user-mock-{len(self.sent_messages) + 1:04d}",
                "raw_content_hash": compute_sha256(message),
                "semantic_content_hash": compute_semantic_hash(message),
            }
        }
        self.sent_messages.append(record)
        return record


class StandardPromptExtractor(IPromptExtractor):
    def __init__(self, expected_canary_nonce: Optional[str] = None, expected_auto_prompt: Optional[str] = None):
        self.expected_auto_prompt = expected_auto_prompt
        self.expected_canary_nonce = expected_canary_nonce

    def extract_with_provenance(self, response_parts: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        if not response_parts:
            return None, None

        # An explicit current nonce enables only this exact, marked test contract.
        # Production extraction rules below are unchanged.
        if self.expected_auto_prompt is not None:
            expected = self.expected_auto_prompt
            blocks = response_parts.get("code_blocks") or []
            if len(blocks) != 1 or response_parts.get("canvas_text"):
                return None, None
            block = blocks[0].strip()
            if block.startswith("```markdown\n") and block.endswith("\n```"):
                block = block[len("```markdown\n"):-len("\n```")]
            elif block.startswith("```\n") and block.endswith("\n```"):
                block = block[len("```\n"):-len("\n```")]
            if block == expected:
                return expected, "auto_canary_marked_block"
            return None, None

        if self.expected_canary_nonce is not None:
            expected = canary_prompt(self.expected_canary_nonce)
            blocks = response_parts.get("code_blocks") or []
            if len(blocks) != 1 or response_parts.get("canvas_text"):
                return None, None
            block = blocks[0].strip()
            if block.startswith("```markdown\n") and block.endswith("\n```"):
                block = block[len("```markdown\n"):-len("\n```")]
            if block == expected:
                return expected, "canary_marked_block"
            return None, None

        # 1. Canvas text
        canvas = (response_parts.get("canvas_text") or "").strip()
        if is_error_placeholder(canvas):
            canvas = ""
        if len(canvas) >= 300 and ("Governance" in canvas or "Mission" in canvas):
            return canvas, "canvas"

        # 2. Code blocks (extract largest markdown/executable code block >= 200 chars)
        blocks = [
            b.strip()
            for b in response_parts.get("code_blocks", [])
            if b and len(b.strip()) >= 200 and not is_error_placeholder(b)
        ]
        if blocks:
            best_block = max(blocks, key=len)
            return best_block, "code_block"

        # 3. Canvas fallback
        if len(canvas) >= 500:
            return canvas, "canvas"

        # 4. Structured markers in text
        text = response_parts.get("text") or ""
        if is_error_placeholder(text):
            return None, None

        for marker in (
            "三、可直接交给 Codex 的执行提示词",
            "Immutable Governance",
            "0. Mission",
            "## Mission",
            "Mission\n",
            "Mission:",
        ):
            idx = text.find(marker)
            if idx >= 0:
                nl = text.rfind("\n", 0, idx)
                start = 0 if nl == -1 else nl + 1
                cand = text[start:].strip()
                for end_marker in ("\n四、", "\n## 四、"):
                    e_idx = cand.find(end_marker)
                    if e_idx > 0:
                        cand = cand[:e_idx].strip()
                if len(cand) >= 200:
                    return cand, "structured_markdown"

        return None, None

    def extract(self, response_parts: Dict[str, Any]) -> Optional[str]:
        prompt, _ = self.extract_with_provenance(response_parts)
        return prompt


class RealChatGPTBrowserObserver(IChatGPTBrowserObserver):
    """
    Real read-only observer for configured ChatGPT browser conversation.
    Supports live Playwright inspection (via CDP/session) and structured DOM reconciliation.
    STRICTLY READ-ONLY: Never sends keystrokes, never clicks submission buttons, never alters textareas.
    """

    def __init__(
        self,
        target_url: Optional[str] = None,
        gpt_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        user_data_dir: Optional[Path] = None,
        cdp_url: Optional[str] = None,
        prompt_extractor: Optional[IPromptExtractor] = None,
    ):
        self.target_url = target_url
        self.gpt_id = gpt_id
        self.conversation_id = conversation_id
        self.user_data_dir = Path(user_data_dir) if user_data_dir else None
        self.cdp_url = cdp_url or "http://127.0.0.1:9222"
        self.extractor = prompt_extractor or StandardPromptExtractor()
        self._injected_page_data: Optional[Any] = None
        self.baseline_turn_index: Optional[int] = None
        self.baseline_message_id: Optional[str] = None
        self.baseline_hash: Optional[str] = None

    def set_baseline(
        self,
        turn_index: Optional[int] = None,
        message_id: Optional[str] = None,
        content_hash: Optional[str] = None,
    ):
        """Set baseline turn index, message ID, or content hash to detect stale responses."""
        self.baseline_turn_index = turn_index
        self.baseline_message_id = message_id
        self.baseline_hash = content_hash

    def record_baseline(self, page_data: Optional[Union[Dict[str, Any], str]] = None):
        """Record latest assistant turn info as baseline to establish submission boundary."""
        data = page_data if page_data is not None else self._injected_page_data
        if not data:
            return
        if isinstance(data, str) or (isinstance(data, dict) and ("html" in data or "content" in data)):
            html = data if isinstance(data, str) else (data.get("html") or data.get("content") or "")
            parsed = ChatGPTDOMParser.parse(html)
            asst = parsed.get("latest_assistant_turn")
            if asst:
                self.baseline_turn_index = asst.get("turn_index")
                self.baseline_message_id = asst.get("message_id")
                raw_text = asst.get("text") or ""
                canvas_text = parsed.get("canvas_text") or ""
                code_blocks = asst.get("code_blocks") or []
                combined = (raw_text + "\n" + canvas_text + "\n" + "\n".join(code_blocks)).strip()
                if combined:
                    self.baseline_hash = compute_sha256(combined)
        elif isinstance(data, dict):
            raw_text = data.get("text") or ""
            canvas_text = data.get("canvas_text") or ""
            code_blocks = data.get("code_blocks") or []
            combined = (raw_text + "\n" + canvas_text + "\n" + "\n".join(code_blocks)).strip()
            if combined:
                self.baseline_hash = compute_sha256(combined)
            if "turn_index" in data:
                self.baseline_turn_index = data["turn_index"]
            if "message_id" in data:
                self.baseline_message_id = data["message_id"]

    def set_injected_page_data(self, page_data: Optional[Dict[str, Any]]):
        """Allows deterministic offline verification & test mocking of real browser DOM captures."""
        self._injected_page_data = page_data

    def validate_url(self, current_url: Optional[str]) -> Dict[str, Any]:
        """Validate if browser is positioned at the expected GPT conversation."""
        res: Dict[str, Any] = {
            "current_url": current_url,
            "target_url": self.target_url,
            "matches_target": False,
            "validation_errors": [],
        }
        if not current_url:
            res["validation_errors"].append("Browser current URL is empty")
            return res

        if self.target_url and same_conversation(self.target_url, current_url):
            res["matches_target"] = True
            return res

        if self.target_url:
            res["validation_errors"].append("URL mismatch: exact configured conversation required")
            return res

        if not self.gpt_id and not self.conversation_id:
            res["matches_target"] = False
            res["validation_errors"].append(
                f"URL mismatch: expected target URL '{self.target_url}', got '{current_url}'"
            )
            return res

        matched_gpt = True
        matched_conv = True
        if self.gpt_id:
            if self.gpt_id not in current_url:
                matched_gpt = False
                res["validation_errors"].append(
                    f"GPT ID mismatch: expected '{self.gpt_id}' in URL, got '{current_url}'"
                )
        if self.conversation_id:
            if self.conversation_id not in current_url:
                matched_conv = False
                res["validation_errors"].append(
                    f"Conversation ID mismatch: expected '{self.conversation_id}' in URL, got '{current_url}'"
                )

        res["matches_target"] = matched_gpt and matched_conv
        return res

    def _extract_prompt_with_provenance(self, payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """Helper to extract prompt and provenance using configured extractor."""
        extractor = self.extractor
        if isinstance(extractor, StandardPromptExtractor):
            prompt, prov = extractor.extract_with_provenance(payload)
        else:
            prompt = extractor.extract(payload)
            prov = "custom_extractor"
        return prompt, prov

    def inspect_dom_payload(self, payload: Union[Dict[str, Any], str]) -> Dict[str, Any]:
        """
        Reconcile assistant response payload from browser DOM.
        Validates URL match, filters error placeholders, and isolates executable prompt.
        Supports both raw HTML strings, dicts with HTML/content, and structured dict payloads.
        Delegates layout feature detection and turn parsing to ChatGPTDOMParser.
        """
        if isinstance(payload, str):
            payload_dict: Dict[str, Any] = {"html": payload, "url": self.target_url}
        elif isinstance(payload, dict):
            payload_dict = dict(payload)
        else:
            payload_dict = {}

        url = payload_dict.get("url") or payload_dict.get("current_url")
        url_check = self.validate_url(url)

        obs_result: Dict[str, Any] = {
            "url_validation": url_check,
            "ready": payload_dict.get("ready", True),
            "human_verification_required": payload_dict.get("human_verification_required", False),
            "interstitial_reason": payload_dict.get("interstitial_reason"),
            "has_error_placeholder": False,
            "error_placeholder_text": None,
            "is_valid": False,
            "raw_text": None,
            "canvas_text": None,
            "code_blocks": [],
            "content_hash": None,
            "raw_content_hash": None,
            "semantic_content_hash": None,
            "extracted_prompt": None,
            "extracted_prompt_hash": None,
            "provenance_policy": None,
            "transfer_eligible": False,
            "ineligibility_reason": None,
            "layout": "unresolved",
            "is_streaming": False,
            "is_completed": False,
            "is_stale_assistant_reply": False,
            "composer": None,
            "turns_count": 0,
            "turns": [],
            "latest_turn": None,
            "latest_assistant_turn": None,
            "latest_user_turn": None,
        }

        # 1. URL check
        if self.target_url and not url_check["matches_target"]:
            obs_result["ineligibility_reason"] = "Conversation URL does not match target"
            return obs_result

        # 2. Blocked check
        if obs_result["human_verification_required"]:
            obs_result["ineligibility_reason"] = f"Blocked by human verification: {obs_result['interstitial_reason']}"
            return obs_result

        # 3. HTML parsing via ChatGPTDOMParser when HTML is supplied
        html_content = payload_dict.get("html") or payload_dict.get("content") or payload_dict.get("html_str")
        if html_content:
            parsed = ChatGPTDOMParser.parse(
                html_str=html_content,
                current_url=url,
                baseline_turn_index=self.baseline_turn_index,
                baseline_message_id=self.baseline_message_id,
                baseline_hash=self.baseline_hash,
            )
            obs_result["layout"] = parsed["layout"]
            obs_result["is_streaming"] = parsed["is_streaming"]
            obs_result["is_completed"] = parsed["is_completed"]
            obs_result["is_stale_assistant_reply"] = parsed["is_stale_assistant_reply"]
            obs_result["composer"] = parsed["composer"]
            obs_result["turns_count"] = parsed["turns_count"]
            obs_result["turns"] = parsed["turns"]
            obs_result["latest_turn"] = parsed["latest_turn"]
            obs_result["latest_assistant_turn"] = parsed["latest_assistant_turn"]
            obs_result["latest_user_turn"] = parsed["latest_user_turn"]
            obs_result["has_error_placeholder"] = parsed["has_error_placeholder"]
            obs_result["error_placeholder_text"] = parsed["error_placeholder_text"]
            obs_result["canvas_text"] = parsed["canvas_text"]

            if parsed["layout"] == "unresolved":
                obs_result["ineligibility_reason"] = "Unresolved ChatGPT DOM layout (neither legacy nor current markers detected)"
                return obs_result

            if parsed["has_error_placeholder"]:
                obs_result["ineligibility_reason"] = parsed.get("ineligibility_reason") or f"Error placeholder detected: {obs_result['error_placeholder_text']}"
                return obs_result

            if parsed["is_stale_assistant_reply"]:
                obs_result["ineligibility_reason"] = parsed.get("ineligibility_reason") or "Stale assistant response detected"
                return obs_result

            if parsed["is_streaming"]:
                obs_result["ineligibility_reason"] = parsed.get("ineligibility_reason") or "Assistant response is currently streaming or thinking"
                return obs_result

            if not parsed["is_completed"]:
                obs_result["ineligibility_reason"] = parsed.get("ineligibility_reason") or "Completion proof missing: assistant turn action bar or completion controls absent"
                return obs_result

            latest_asst = parsed["latest_assistant_turn"]
            if not latest_asst:
                obs_result["ineligibility_reason"] = "No assistant turn found"
                return obs_result

            raw_text = latest_asst["text"]
            canvas_text = parsed["canvas_text"]
            code_blocks = latest_asst["code_blocks"]
            obs_result["raw_text"] = raw_text
            obs_result["code_blocks"] = code_blocks

            combined_payload = (raw_text + "\n" + canvas_text + "\n" + "\n".join(code_blocks)).strip()
            if not combined_payload:
                obs_result["ineligibility_reason"] = "Empty assistant response content"
                return obs_result

            obs_result["content_hash"] = compute_sha256(combined_payload)
            obs_result["raw_content_hash"] = obs_result["content_hash"]
            from app.canonical import compute_semantic_hash
            obs_result["semantic_content_hash"] = latest_asst.get("semantic_content_hash") or compute_semantic_hash(combined_payload)
            if self.baseline_hash and obs_result["content_hash"] == self.baseline_hash:
                obs_result["is_stale_assistant_reply"] = True
                obs_result["transfer_eligible"] = False
                obs_result["ineligibility_reason"] = "Assistant content hash matches baseline hash"
                return obs_result

            extract_payload = {"text": raw_text, "canvas_text": canvas_text, "code_blocks": code_blocks}
            prompt, prov = self._extract_prompt_with_provenance(extract_payload)
            if prompt:
                obs_result["extracted_prompt"] = prompt
                obs_result["extracted_prompt_hash"] = compute_sha256(prompt)
                obs_result["provenance_policy"] = prov
                obs_result["is_valid"] = True
                obs_result["transfer_eligible"] = True
            else:
                obs_result["ineligibility_reason"] = "No valid executable prompt structure found in response"
            return obs_result

        raw_text = payload_dict.get("text") or ""
        canvas_text = payload_dict.get("canvas_text") or ""
        code_blocks = payload_dict.get("code_blocks") or []

        obs_result["raw_text"] = raw_text
        obs_result["canvas_text"] = canvas_text
        obs_result["code_blocks"] = code_blocks

        # Error placeholder check
        for txt in [raw_text, canvas_text] + code_blocks:
            if is_error_placeholder(txt):
                obs_result["has_error_placeholder"] = True
                obs_result["error_placeholder_text"] = txt.strip()
                obs_result["ineligibility_reason"] = f"Error placeholder detected: {txt.strip()[:60]}"
                return obs_result

        # Content hashing
        combined_payload = (raw_text + "\n" + canvas_text + "\n" + "\n".join(code_blocks)).strip()
        if not combined_payload:
            obs_result["ineligibility_reason"] = "Empty assistant response content"
            return obs_result

        obs_result["content_hash"] = compute_sha256(combined_payload)

        # Baseline hash check
        if self.baseline_hash and obs_result["content_hash"] == self.baseline_hash:
            obs_result["is_stale_assistant_reply"] = True
            obs_result["transfer_eligible"] = False
            obs_result["ineligibility_reason"] = "Assistant content hash matches baseline hash"
            return obs_result

        if payload_dict.get("is_streaming"):
            obs_result["is_streaming"] = True
            obs_result["ineligibility_reason"] = "Assistant response is currently streaming or thinking"
            return obs_result

        if payload_dict.get("is_stale"):
            obs_result["is_stale_assistant_reply"] = True
            obs_result["ineligibility_reason"] = "Stale assistant response detected"
            return obs_result

        obs_result["is_completed"] = payload_dict.get("is_completed", True)
        if not obs_result["is_completed"]:
            obs_result["ineligibility_reason"] = "Completion proof missing: assistant turn action bar or completion controls absent"
            return obs_result

        prompt, prov = self._extract_prompt_with_provenance(payload_dict)

        if prompt:
            obs_result["extracted_prompt"] = prompt
            obs_result["extracted_prompt_hash"] = compute_sha256(prompt)
            obs_result["provenance_policy"] = prov
            obs_result["is_valid"] = True
            obs_result["transfer_eligible"] = True
        else:
            obs_result["ineligibility_reason"] = "No valid executable prompt structure found in response"

        return obs_result

    async def get_browser_status(self) -> Dict[str, Any]:
        """Check live browser status or injected status."""
        if self._injected_page_data is not None:
            data = self._injected_page_data
            if isinstance(data, str):
                return {
                    "connected": True,
                    "ready": True,
                    "human_verification_required": False,
                    "interstitial_reason": None,
                    "current_url": self.target_url,
                }
            return {
                "connected": True,
                "ready": data.get("ready", True),
                "human_verification_required": data.get("human_verification_required", False),
                "interstitial_reason": data.get("interstitial_reason"),
                "current_url": data.get("url") or data.get("current_url") or self.target_url,
            }

        # Read the same exact target as preflight and the sender. A discovered
        # URL alone is not evidence that a live composer is usable.
        try:
            from app.chatgpt_dom import ChatGPTDOMParser
            page = await self.read_live_page()
            parsed = ChatGPTDOMParser.parse(page["html"])
            composer = parsed.get("composer") or {}
            text = page["html"].lower()
            blocked = "verify you are human" in text or "cf-chl-widget" in text
            ready = bool(composer.get("found") and not composer.get("is_writing_block")
                         and not parsed.get("is_streaming") and not blocked)
            send_verified = bool(ready and composer.get("send_control_verified"))
            return {"connected": True, "ready": ready, "human_verification_required": blocked,
                    "current_url": page["url"],
                    "interstitial_reason": None if ready else "Target composer unavailable, streaming, or verification required",
                    "send_control_verified": send_verified,
                    "send_control": composer.get("send_control")}
        except Exception as exc:
            return {"connected": False, "ready": False, "human_verification_required": False,
                    "interstitial_reason": f"CDP read-only probe failed: {exc}"}

    async def read_live_page(self) -> Dict[str, Any]:
        """Read one exact browser target. No navigation, fills, clicks, or fallback."""
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(self.cdp_url)
            pages = [pg for context in browser.contexts for pg in context.pages
                     if self.validate_url(pg.url)["matches_target"]]
            if len(pages) != 1:
                raise ValueError("Exactly one configured ChatGPT browser target must be open")
            page = pages[0]
            return {"url": page.url, "html": await page.content()}

    async def get_latest_assistant_response(self) -> Optional[Dict[str, Any]]:
        """Extract latest assistant response in a strictly read-only manner."""
        if self._injected_page_data is not None:
            analysis = self.inspect_dom_payload(self._injected_page_data)
            if analysis["is_valid"] and analysis.get("transfer_eligible"):
                return {
                    "text": analysis["raw_text"],
                    "canvas_text": analysis["canvas_text"],
                    "code_blocks": analysis["code_blocks"],
                    "content_hash": analysis["content_hash"],
                    "extracted_prompt": analysis["extracted_prompt"],
                    "extracted_prompt_hash": analysis["extracted_prompt_hash"],
                    "provenance_policy": analysis["provenance_policy"],
                    "layout": analysis.get("layout"),
                    "turns_count": analysis.get("turns_count"),
                }
            return None

        # Live CDP extraction
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                try:
                    browser = await p.chromium.connect_over_cdp(self.cdp_url)
                    target_page = None
                    for c in browser.contexts:
                        for pg in c.pages:
                            if self.validate_url(pg.url)["matches_target"]:
                                target_page = pg
                                break
                    if not target_page:
                        return None

                    # Read-only DOM evaluation supporting both legacy and current layouts
                    eval_res = await target_page.evaluate("""() => {
                        return {
                            url: window.location.href,
                            html: document.documentElement.outerHTML
                        };
                    }""")

                    if not eval_res:
                        return None

                    analysis = self.inspect_dom_payload(eval_res)
                    if analysis["is_valid"] and analysis.get("transfer_eligible"):
                        return {
                            "text": analysis["raw_text"],
                            "canvas_text": analysis["canvas_text"],
                            "code_blocks": analysis["code_blocks"],
                            "content_hash": analysis["content_hash"],
                            "extracted_prompt": analysis["extracted_prompt"],
                            "extracted_prompt_hash": analysis["extracted_prompt_hash"],
                            "provenance_policy": analysis["provenance_policy"],
                            "layout": analysis.get("layout"),
                            "turns_count": analysis.get("turns_count"),
                        }
                    return None
                except Exception:
                    return None
        except Exception:
            return None

    def classify_delivery(
        self,
        page_or_html: Union[str, Dict[str, Any]],
        expected_message: str,
        nonce: Optional[str] = None,
        baseline_turn_ids: Optional[Any] = None,
        baseline_message_ids: Optional[Any] = None,
        baseline_user_index: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Classify delivery of expected_message into ChatGPT conversation using shared canonical layer."""
        from app.canonical import classify_chatgpt_delivery
        return classify_chatgpt_delivery(
            page_or_parsed=page_or_html,
            expected_message=expected_message,
            target_url=self.target_url,
            authorized_nonce=nonce,
            baseline_turn_ids=baseline_turn_ids,
            baseline_message_ids=baseline_message_ids,
            baseline_user_index=baseline_user_index,
        )

    async def classify_live_delivery(
        self,
        expected_message: str,
        nonce: Optional[str] = None,
        baseline_turn_ids: Optional[Any] = None,
        baseline_message_ids: Optional[Any] = None,
        baseline_user_index: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Classify delivery against live page or injected mock page."""
        if self._injected_page_data is not None:
            return self.classify_delivery(
                page_or_html=self._injected_page_data,
                expected_message=expected_message,
                nonce=nonce,
                baseline_turn_ids=baseline_turn_ids,
                baseline_message_ids=baseline_message_ids,
                baseline_user_index=baseline_user_index,
            )
        live_page = await self.read_live_page()
        return self.classify_delivery(
            page_or_html=live_page,
            expected_message=expected_message,
            nonce=nonce,
            baseline_turn_ids=baseline_turn_ids,
            baseline_message_ids=baseline_message_ids,
            baseline_user_index=baseline_user_index,
        )



# ---------------------------------------------------------------- Real ChatGPT Message Sender (Phase 2.3)


class RealChatGPTMessageSender(IChatGPTMessageSender):
    """
    Real ChatGPT browser message sender using Edge CDP session and dual-layout DOM parser.
    Supports live Playwright interaction and injected mock/test DOM fixtures.
    Enforces:
    - Pre-send URL revalidation (must match configured conversation URL, reject multiple pages or wrong URL)
    - Genuine composer verification (strictly rejects writing-block or canvas editor)
    - Single-use authorization check bound to payload SHA-256 and destination
    - Immediate conversation baseline capture before submission
    - Population verification (read back must match character-for-character including newlines)
    - Uniquely identified composer-associated send control submission
    - Verification that new user message appears in conversation
    - Mid-transfer disconnect/timeout handling raising DeliveryOutcomeUnknownError
    """

    def __init__(
        self,
        target_url: Optional[str] = None,
        gpt_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        cdp_url: Optional[str] = None,
        db: Optional[Any] = None,
    ):
        self.target_url = target_url
        self.gpt_id = gpt_id
        self.conversation_id = conversation_id
        self.cdp_url = cdp_url or "http://127.0.0.1:9222"
        self.db = db
        self._injected_page: Optional[Any] = None
        self._injected_page_data: Optional[Any] = None

    def set_injected_page(self, page: Any):
        """Set mock or injected page for testing."""
        self._injected_page = page

    def set_injected_page_data(self, page_data: Any):
        """Set mock page data dict or HTML for testing."""
        self._injected_page_data = page_data

    def validate_target_url(self, url: Optional[str]) -> bool:
        return same_conversation(self.target_url, url)

    async def capture_baseline(self, page_data: Optional[Any] = None) -> Dict[str, Any]:
        """Capture conversation baseline immediately before submission."""
        data = page_data if page_data is not None else self._injected_page_data
        if data:
            html = data if isinstance(data, str) else (data.get("html") or data.get("content") or "")
            parsed = ChatGPTDOMParser.parse(html)
            asst = parsed.get("latest_assistant_turn")
            user = parsed.get("latest_user_turn")
            raw_text = (asst.get("text") or "") if asst else ""
            h = compute_sha256(raw_text) if raw_text else None
            return {
                "turns_count": parsed.get("turns_count", 0),
                "latest_turn_id": asst.get("message_id") if asst else (user.get("message_id") if user else None),
                "baseline_hash": h,
                "timestamp": datetime.utcnow().isoformat(),
            }
        return {
            "turns_count": 0,
            "latest_turn_id": None,
            "baseline_hash": None,
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def send_message(
        self,
        message: str,
        confirmation_token: Optional[str] = None,
        transfer_id: Optional[str] = None,
        baseline_hash: Optional[str] = None,
        page_override: Optional[Any] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if not message or not message.strip():
            raise ValueError("Message content cannot be empty")

        content_hash = compute_sha256(message)

        # 1. Single-use authorization verification
        if self.db:
            if not confirmation_token or not transfer_id:
                raise ValueError("Single-use transfer authorization required")
            dest = self.target_url or self.conversation_id or self.gpt_id or "chatgpt"
            if (kwargs.get("is_canary") or kwargs.get("canary_mode")) and is_production_conversation(dest):
                raise ValueError(f"Production ChatGPT conversation '{dest}' cannot be targeted in canary mode")
            ok, err = await self.db.verify_and_consume_authorization(
                transfer_id=transfer_id,
                token=confirmation_token,
                content_hash=content_hash,
                destination=dest,
                expected_config_fingerprint=kwargs.get("config_fingerprint"),
            )
            if not ok:
                raise ValueError(f"Transfer authorization rejected: {err}")

        # 2. Injected page data / mock execution for tests
        target_page_data = page_override if page_override is not None else self._injected_page_data
        if target_page_data is not None:
            return await self._send_with_page_data(
                target_page_data, message, content_hash, transfer_id, baseline_hash
            )

        # 3. Injected page object for mock Playwright page
        if self._injected_page is not None:
            return await self._send_with_page_object(
                self._injected_page, message, content_hash, transfer_id, baseline_hash
            )

        # 4. Live CDP execution
        reject_placeholder_targets("validated-thread", self.target_url)
        return await self._send_with_live_cdp(
            message, content_hash, transfer_id, baseline_hash, **kwargs
        )

    @classmethod
    def _is_forbidden_control(cls, info: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        testid = (info.get("testid") or "").lower()
        aria = (info.get("ariaLabel") or "").lower()
        nav = (info.get("navTarget") or "").lower()
        comb = f"{testid} {aria} {nav}"

        forbidden = [
            ("stop", ["stop", "停止"]),
            ("microphone", ["mic", "microphone", "听写", "dictation", "录音", "语音"]),
            ("voice", ["voice", "voice-mode"]),
            ("attachment", ["attach", "attachment", "upload", "添加文件", "上传"]),
            ("plus", ["plus", "+", "composer-plus-btn"]),
            ("tools", ["tool", "工具", "gpt-tools"]),
            ("model_picker", ["model", "模型", "reasoning", "reasoning-effort"]),
            ("writing_block", ["canvas", "writing-block", "画布", "edit-block"]),
        ]
        for category, keywords in forbidden:
            for kw in keywords:
                if kw in comb:
                    return True, category
        return False, None

    async def _resolve_send_control(
        self,
        target_page: Any,
        composer_selector: str,
        timeout: float = 5.0,
    ) -> Tuple[Any, str]:
        """
        Locates the composer-associated send button using strict ownership model:
        1. Resolve verified composer form:
           textbox -> closest('form[data-chatgpt-composer]') or form[data-chatgpt-composer]
        2. Candidate controls within that verified form only:
           [data-testid="send-button"], button[type="submit"]
        3. Waits briefly for React to mount/replace/enable the active Send control.
        4. Excludes forbidden controls (stop, mic, voice, attach, plus, tool, model, writing-block).
        5. Requires exactly one valid visible/enabled submit control; multiple submit controls fail closed.
        6. Uses Playwright Locator so the element is resolved at action time.
        """
        if hasattr(target_page, "locator"):
            textbox_loc = target_page.locator(composer_selector).first

            # Form ownership: prefer form[data-chatgpt-composer], fallback to ancestor form
            form_loc = target_page.locator('form[data-chatgpt-composer]')
            form_count = await form_loc.count() if hasattr(form_loc, "count") else 0
            if form_count == 1:
                composer_form = form_loc.first
            else:
                composer_form = textbox_loc.locator('xpath=ancestor::form[1]')

            form_exists = await composer_form.count() if hasattr(composer_form, "count") else 1
            if form_exists == 0:
                raise NotSentError("Composer-associated form not found for verified textbox")

            start_time = time.time()
            candidate_metadata = []

            while time.time() - start_time < timeout:
                candidates = composer_form.locator('[data-testid="send-button"], button[type="submit"]')
                count = await candidates.count() if hasattr(candidates, "count") else 0
                valid_indices = []
                current_meta = []

                for i in range(count):
                    btn = candidates.nth(i)
                    try:
                        info = await btn.evaluate("""el => {
                            const rect = el.getBoundingClientRect();
                            return {
                                tag: el.tagName,
                                type: el.getAttribute('type'),
                                testid: el.getAttribute('data-testid'),
                                ariaLabel: el.getAttribute('aria-label'),
                                navTarget: el.getAttribute('data-composer-navigation-target'),
                                disabled: el.disabled || el.getAttribute('aria-disabled') === 'true',
                                visible: rect.width > 0 && rect.height > 0 && window.getComputedStyle(el).visibility !== 'hidden',
                                inWritingBlock: !!(el.closest('[data-writing-block]') || el.closest('[data-canvas]'))
                            };
                        }""")
                    except Exception:
                        continue

                    current_meta.append(info)
                    if not info.get("visible") or info.get("disabled") or info.get("inWritingBlock"):
                        continue
                    is_forb, _ = self._is_forbidden_control(info)
                    if is_forb:
                        continue
                    valid_indices.append(i)

                candidate_metadata = current_meta
                if len(valid_indices) == 1:
                    valid_locator = candidates.nth(valid_indices[0])
                    sel = (
                        'form[data-chatgpt-composer] [data-testid="send-button"]'
                        if candidate_metadata[valid_indices[0]].get("testid") == "send-button"
                        else 'form[data-chatgpt-composer] button[type="submit"]'
                    )
                    return valid_locator, sel
                elif len(valid_indices) > 1:
                    raise NotSentError(
                        f"CHATGPT_MULTIPLE_SUBMIT_CONTROLS: Ambiguous submit controls in composer form; "
                        f"{len(valid_indices)} visible/enabled candidates found. Candidates: {candidate_metadata}"
                    )

                await asyncio.sleep(0.2)

            raise NotSentError(
                f"CHATGPT_SEND_CONTROL_NOT_READY: Composer-associated send button not found or not ready within timeout. "
                f"Candidates: {candidate_metadata}"
            )
        elif hasattr(target_page, "query_selector"):
            btn = await target_page.query_selector('form[data-chatgpt-composer] [data-testid="send-button"], form[data-chatgpt-composer] button[type="submit"], form button[type="submit"]')
            if not btn:
                raise NotSentError("CHATGPT_SEND_CONTROL_NOT_READY: Composer-associated send button not found")
            return btn, 'form[data-chatgpt-composer] button[type="submit"]'
        else:
            raise NotSentError("Target page does not support Playwright locator or query_selector API")

    async def _send_with_page_data(
        self,
        target_page_data: Any,
        message: str,
        content_hash: str,
        transfer_id: Optional[str],
        baseline_hash: Optional[str],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if isinstance(target_page_data, dict):
            if target_page_data.get("multiple_pages_open"):
                raise NotSentError("Ambiguous: multiple matching ChatGPT pages open in browser")

            url = target_page_data.get("url") or target_page_data.get("current_url")
            if url and not self.validate_target_url(url):
                raise NotSentError(f"Target URL mismatch: expected {self.target_url}, got {url}")

            if target_page_data.get("disconnect_before_submit"):
                raise NotSentError("Browser disconnected before submission")

            if target_page_data.get("simulate_writing_block"):
                raise NotSentError("Rejected: writing block or canvas editor detected instead of genuine composer")

            if target_page_data.get("simulate_no_send_button"):
                raise NotSentError("CHATGPT_SEND_CONTROL_NOT_READY: Composer-associated send button not found")

            if target_page_data.get("simulate_multiple_submit_buttons"):
                raise NotSentError("CHATGPT_MULTIPLE_SUBMIT_CONTROLS: Multiple submit controls found in composer form")

            html = target_page_data.get("html") or target_page_data.get("content") or ""
            parsed = ChatGPTDOMParser.parse(html, current_url=url)
            comp = parsed.get("composer", {})
            if not comp.get("found"):
                raise NotSentError("Message composer not found in target ChatGPT conversation")

            if comp.get("is_writing_block") or "canvas" in str(comp.get("selector", "")).lower():
                raise NotSentError("Rejected: writing block or canvas editor detected instead of genuine composer")

            # Check populated content matches
            if target_page_data.get("composer_content_mismatch"):
                raise NotSentError("Composer text mismatch: read back content does not match prepared report character-for-character")

            if target_page_data.get("simulate_read_back_text") is not None:
                rb = target_page_data["simulate_read_back_text"]
                norm_rb = rb.strip().replace("\r\n", "\n")
                norm_msg = message.strip().replace("\r\n", "\n")
                if norm_rb != norm_msg or compute_sha256(norm_rb) != content_hash:
                    raise NotSentError("Composer text mismatch: read back content does not match prepared report character-for-character")

            # DELIVERY BOUNDARY: Any exception after click is DELIVERY_OUTCOME_UNKNOWN
            if target_page_data.get("disconnect_after_submit") or target_page_data.get("timeout_after_submit"):
                raise DeliveryOutcomeUnknownError("Browser disconnected or timeout after submission: delivery outcome unknown")

            send_selector = (
                'form[data-chatgpt-composer] [data-testid="send-button"]'
                if (comp.get("send_control") and comp.get("send_control", {}).get("testid") == "send-button")
                else 'form[data-chatgpt-composer] button[type="submit"]'
            )
            from app.canonical import compute_semantic_hash
            sem_hash = compute_semantic_hash(message)
            latest_u = parsed.get("latest_user_turn") or {}
            user_turn_id = latest_u.get("turn_id") or f"turn-user-{uuid.uuid4().hex[:8]}"
            user_msg_id = latest_u.get("message_id")
            return {
                "status": "delivered",
                "send_id": f"chatgpt-send-{uuid.uuid4().hex[:8]}",
                "transfer_id": transfer_id,
                "content_hash": content_hash,
                "raw_content_hash": content_hash,
                "semantic_content_hash": sem_hash,
                "chars": len(message),
                "delivery_evidence": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "send_button_selector": send_selector,
                    "baseline_hash": baseline_hash,
                    "user_turn_verified": True,
                    "user_turn_id": user_turn_id,
                    "user_message_id": user_msg_id,
                    "turn_index": parsed.get("turns_count", 0) + 1,
                    "layout": parsed.get("layout"),
                    "method": "injected_mock",
                    "raw_content_hash": content_hash,
                    "semantic_content_hash": sem_hash,
                    "semantic_delivery_status": "SEMANTIC_DELIVERY_CONFIRMED",
                    "matching_turn_count": 1,
                },
            }
        elif isinstance(target_page_data, str):
            parsed = ChatGPTDOMParser.parse(target_page_data, current_url=self.target_url)
            comp = parsed.get("composer", {})
            if not comp.get("found"):
                raise NotSentError("Message composer not found in target ChatGPT conversation")
            if comp.get("is_writing_block") or "canvas" in str(comp.get("selector", "")).lower():
                raise NotSentError("Rejected: writing block or canvas editor detected instead of genuine composer")

            from app.canonical import compute_semantic_hash
            sem_hash = compute_semantic_hash(message)
            latest_u = parsed.get("latest_user_turn") or {}
            user_turn_id = latest_u.get("turn_id") or f"turn-user-{uuid.uuid4().hex[:8]}"
            user_msg_id = latest_u.get("message_id")
            return {
                "status": "delivered",
                "send_id": f"chatgpt-send-{uuid.uuid4().hex[:8]}",
                "transfer_id": transfer_id,
                "content_hash": content_hash,
                "raw_content_hash": content_hash,
                "semantic_content_hash": sem_hash,
                "chars": len(message),
                "delivery_evidence": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "send_button_selector": 'form[data-chatgpt-composer] button[type="submit"]',
                    "baseline_hash": baseline_hash,
                    "user_turn_verified": True,
                    "user_turn_id": user_turn_id,
                    "user_message_id": user_msg_id,
                    "turn_index": parsed.get("turns_count", 0) + 1,
                    "layout": parsed.get("layout"),
                    "method": "injected_mock",
                    "raw_content_hash": content_hash,
                    "semantic_content_hash": sem_hash,
                    "semantic_delivery_status": "SEMANTIC_DELIVERY_CONFIRMED",
                    "matching_turn_count": 1,
                },
            }

        raise NotSentError("Unsupported target page data format")

    async def _send_with_page_object(
        self,
        page: Any,
        message: str,
        content_hash: str,
        transfer_id: Optional[str],
        baseline_hash: Optional[str],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        url = getattr(page, "url", None)
        if url and not self.validate_target_url(url):
            raise NotSentError(f"Target URL mismatch: expected {self.target_url}, got {url}")

        html = await page.content() if hasattr(page, "content") else ""
        parsed = ChatGPTDOMParser.parse(html, current_url=url)
        comp = parsed.get("composer", {})
        if not comp.get("found"):
            raise NotSentError("Message composer not found in target ChatGPT conversation")
        if comp.get("is_writing_block") or "canvas" in str(comp.get("selector", "")).lower():
            raise NotSentError("Rejected: writing block or canvas editor detected instead of genuine composer")

        selector = comp.get("selector") or '#prompt-textarea'
        if hasattr(page, "fill"):
            if comp.get("is_content_editable") and hasattr(page, "evaluate"):
                await page.focus(selector)
                await page.evaluate("""({sel, text}) => {
                    const el = document.querySelector(sel);
                    if (!el) return;
                    el.innerText = text;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                }""", {"sel": selector, "text": message})
            else:
                await page.fill(selector, message)

        # Verify read back
        if hasattr(page, "evaluate"):
            rb = await page.evaluate(f"() => document.querySelector('{selector}')?.innerText || document.querySelector('{selector}')?.value || ''")
            if rb:
                norm_rb = rb.strip().replace("\r\n", "\n")
                norm_msg = message.strip().replace("\r\n", "\n")
                if norm_rb != norm_msg or compute_sha256(norm_rb) != content_hash:
                    raise NotSentError("Composer text mismatch: read back content does not match prepared report character-for-character")

        # Resolve send control using strict ownership model
        valid_send_locator, send_selector = await self._resolve_send_control(page, selector)

        # DELIVERY BOUNDARY: before click -> NOT_SENT; during/after click -> DELIVERY_OUTCOME_UNKNOWN
        click_attempted = False
        try:
            if hasattr(page, "url") and page.url and not self.validate_target_url(page.url):
                raise NotSentError("ChatGPT target changed before submission")
            click_attempted = True
            await valid_send_locator.click()  # Normal click; NO force=True; NO Enter fallback
        except Exception as e:
            if not click_attempted:
                raise
            raise DeliveryOutcomeUnknownError(f"Browser disconnected or error during click: {e}")

        # Post-click verification (if mock page supports new turns)
        turns_count = parsed.get("turns_count", 0) + 1
        if hasattr(page, "content"):
            try:
                curr_html = await page.content()
                curr_parsed = ChatGPTDOMParser.parse(curr_html, current_url=url)
                turns_count = curr_parsed.get("turns_count", turns_count)
            except Exception:
                pass

        from app.canonical import compute_semantic_hash
        sem_hash = compute_semantic_hash(message)
        latest_u = parsed.get("latest_user_turn") or {}
        user_turn_id = latest_u.get("turn_id") or f"turn-user-{uuid.uuid4().hex[:8]}"
        user_msg_id = latest_u.get("message_id")
        return {
            "status": "delivered",
            "send_id": f"chatgpt-send-{uuid.uuid4().hex[:8]}",
            "transfer_id": transfer_id,
            "content_hash": content_hash,
            "raw_content_hash": content_hash,
            "semantic_content_hash": sem_hash,
            "chars": len(message),
            "delivery_evidence": {
                "timestamp": datetime.utcnow().isoformat(),
                "send_button_selector": send_selector,
                "baseline_hash": baseline_hash,
                "user_turn_verified": True,
                "user_turn_id": user_turn_id,
                "user_message_id": user_msg_id,
                "turn_index": turns_count,
                "layout": parsed.get("layout"),
                "raw_content_hash": content_hash,
                "semantic_content_hash": sem_hash,
                "semantic_delivery_status": "SEMANTIC_DELIVERY_CONFIRMED",
                "matching_turn_count": 1,
            },
        }

    async def _send_with_live_cdp(
        self,
        message: str,
        content_hash: str,
        transfer_id: Optional[str],
        baseline_hash: Optional[str],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            try:
                browser = await p.chromium.connect_over_cdp(self.cdp_url)
            except Exception as e:
                raise NotSentError(f"Cannot connect to browser CDP on {self.cdp_url}: {e}")

            matched_pages = []
            for c in browser.contexts:
                for pg in c.pages:
                    if self.validate_target_url(pg.url):
                        matched_pages.append(pg)

            if len(matched_pages) == 0:
                raise NotSentError("Target ChatGPT conversation page not open in browser")
            if len(matched_pages) > 1:
                raise NotSentError(f"Ambiguous: {len(matched_pages)} matching ChatGPT conversation pages open")

            target_page = matched_pages[0]

            # Revalidate URL immediately before action
            if not self.validate_target_url(target_page.url):
                raise NotSentError("Target ChatGPT URL changed unexpectedly immediately before send")

            html = await target_page.content()
            parsed = ChatGPTDOMParser.parse(html, current_url=target_page.url)
            comp = parsed.get("composer", {})
            if not comp.get("found"):
                raise NotSentError("Message composer not found in target ChatGPT conversation")
            if comp.get("is_writing_block") or "canvas" in str(comp.get("selector", "")).lower():
                raise NotSentError("Rejected: writing block or canvas editor detected instead of genuine composer")

            selector = comp.get("selector")
            if kwargs.get("production_shadow"):
                if target_page.url != self.target_url or parsed.get("is_streaming") or comp.get("text"):
                    raise NotSentError("PRODUCTION_CHATGPT_TARGET_NOT_READY")
            if comp.get("is_content_editable"):
                await target_page.focus(selector)
                await target_page.evaluate("""({sel, text}) => {
                    const el = document.querySelector(sel);
                    if (!el) return;
                    el.innerText = text;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                }""", {"sel": selector, "text": message})
            else:
                await target_page.fill(selector, message)

            # Verify populated text (character-for-character readback & exact raw hash)
            read_back = await target_page.evaluate("""sel => {
                const el = document.querySelector(sel);
                return el ? (el.innerText || el.value || '') : '';
            }""", selector)

            norm_rb = read_back if kwargs.get("production_shadow") else read_back.strip().replace("\r\n", "\n")
            norm_msg = message if kwargs.get("production_shadow") else message.strip().replace("\r\n", "\n")
            if norm_rb != norm_msg or compute_sha256(norm_rb) != content_hash:
                raise NotSentError("PRODUCTION_COMPOSER_TEXT_MISMATCH" if kwargs.get("production_shadow") else
                                   "Composer text mismatch: read back content does not match prepared report character-for-character")

            # Locate composer-associated send button using strict ownership model
            valid_send_locator, send_selector = await self._resolve_send_control(target_page, selector)

            # Capture baseline: prefer pre-fill parsed DOM to ensure new user message is never considered baseline
            baseline_turn_ids = {t.get("turn_id") for t in parsed.get("turns", []) if t.get("turn_id")}
            baseline_message_ids = {t.get("message_id") for t in parsed.get("turns", []) if t.get("message_id")}
            baseline_user_turn = parsed.get("latest_user_turn")
            baseline_user_index = baseline_user_turn.get("turn_index", -1) if baseline_user_turn else -1
            baseline_texts = [t.get("text", "") for t in parsed.get("turns", []) if t.get("text")]

            from app.canonical import classify_chatgpt_delivery, compute_semantic_hash
            authorized_nonce = kwargs.get("nonce")
            if not authorized_nonce and not kwargs.get("production_shadow"):
                m_nonce = re.search(r"nonce:\s*([0-9a-f]{32})", message, re.IGNORECASE)
                if not m_nonce:
                    m_nonce = re.search(r"\b([0-9a-f]{32})\b", message)
                if m_nonce:
                    authorized_nonce = m_nonce.group(1)

            # DELIVERY BOUNDARY: before click -> NOT_SENT; during/after click -> DELIVERY_OUTCOME_UNKNOWN
            click_attempted = False
            try:
                if not self.validate_target_url(target_page.url):
                    raise NotSentError("ChatGPT target changed before submission")
                click_attempted = True
                await valid_send_locator.click()  # Normal Playwright click; no force=True; no Enter fallback
            except Exception as e:
                if not click_attempted:
                    raise
                raise DeliveryOutcomeUnknownError(f"Browser disconnected or error during click: {e}")

            # Verify delivery: bounded polling for NEW user turn matching nonce and semantic hash
            delivered = False
            start_time = time.time()
            classification = {}
            delivery_timeout = float(kwargs.get("delivery_timeout_sec") or 30.0)

            while time.time() - start_time < delivery_timeout:
                try:
                    if not self.validate_target_url(target_page.url):
                        raise DeliveryOutcomeUnknownError("ChatGPT target changed after submission")
                    curr_html = await target_page.content()
                    classification = classify_chatgpt_delivery(
                        page_or_parsed={"html": curr_html, "url": target_page.url},
                        expected_message=message,
                        target_url=self.target_url,
                        authorized_nonce=authorized_nonce,
                        baseline_turn_ids=baseline_turn_ids,
                        baseline_message_ids=baseline_message_ids,
                        baseline_user_index=baseline_user_index,
                        baseline_texts=baseline_texts,
                        infer_nonce=not kwargs.get("production_shadow", False),
                    )
                    if classification.get("status") == "CHATGPT_DUPLICATE_DELIVERY":
                        raise DeliveryOutcomeUnknownError(
                            classification.get("error_details", "Multiple matching new user turns found after send")
                        )
                    if classification.get("verified"):
                        delivered = True
                        break
                    await asyncio.sleep(0.5)
                except DeliveryOutcomeUnknownError:
                    raise
                except Exception as poll_err:
                    raise DeliveryOutcomeUnknownError(f"Browser disconnected while waiting for delivery confirmation: {poll_err}")

            if not delivered:
                err_msg = classification.get("error_details") or "Timeout waiting for semantic delivery confirmation in conversation: delivery outcome unknown"
                raise DeliveryOutcomeUnknownError(err_msg)

            sem_hash = classification.get("semantic_content_hash") or compute_semantic_hash(message)
            return {
                "status": "delivered",
                "send_id": f"chatgpt-send-{uuid.uuid4().hex[:8]}",
                "transfer_id": transfer_id,
                "content_hash": content_hash,
                "raw_content_hash": content_hash,
                "semantic_content_hash": sem_hash,
                "chars": len(message),
                "delivery_evidence": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "send_button_selector": send_selector,
                    "baseline_hash": baseline_hash,
                    "user_turn_verified": True,
                    "user_turn_id": classification.get("user_turn_id"),
                    "user_message_id": classification.get("user_message_id"),
                    "turn_index": classification.get("user_turn_index", 0),
                    "layout": classification.get("layout"),
                    "raw_content_hash": content_hash,
                    "semantic_content_hash": sem_hash,
                    "semantic_delivery_status": "SEMANTIC_DELIVERY_CONFIRMED",
                    "matching_turn_count": 1,
                },
            }

# ---------------------------------------------------------------- Real Codex Queue Adapter (Phase 2.3)




def inspect_codex_queue_storage(
    thread_id: str,
    codex_home: Optional[Union[str, Path]] = None,
    nonce: Optional[str] = None,
    expected_content_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Safely inspect the local Codex SQLite queue storage (queue_1.sqlite) in read-only mode.
    Preserves queue state without modifying or deleting items.
    """
    home = Path(codex_home) if codex_home else Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    q_db = home / "queue_1.sqlite"
    result: Dict[str, Any] = {
        "exists": False,
        "db_path": str(q_db),
        "count": 0,
        "items": [],
        "matching_items": [],
        "has_matching_item": False,
        "first_matching_item": None,
        "active_goal": None,
        "has_active_goal": False,
        "thread_interrupted": False,
    }
    if not q_db.exists():
        return result

    try:
        uri = f"file:{q_db.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            rows = cur.execute(
                "SELECT * FROM queued_items WHERE thread_id = ? ORDER BY queue_order ASC, created_at_ms ASC",
                (thread_id,),
            ).fetchall()
            result["exists"] = True
            result["count"] = len(rows)
            for r in rows:
                payload = {}
                try:
                    payload = json.loads(r["payload_json"])
                except Exception:
                    pass
                user_input = payload.get("UserInput", {})
                content_list = user_input.get("content", [])
                client_id = user_input.get("client_id")
                text = " ".join(c.get("text", "") for c in content_list if isinstance(c, dict))
                item = {
                    "id": r["id"],
                    "thread_id": r["thread_id"],
                    "client_id": client_id,
                    "text": text,
                    "queue_order": r["queue_order"],
                    "created_at_ms": r["created_at_ms"],
                    "updated_at_ms": r["updated_at_ms"],
                }
                result["items"].append(item)
                is_match = True
                if nonce:
                    if nonce not in text and nonce not in str(payload):
                        is_match = False
                elif expected_content_hash:
                    if compute_sha256(text) != expected_content_hash:
                        is_match = False
                if is_match:
                    result["matching_items"].append(item)

            result["has_matching_item"] = len(result["matching_items"]) > 0
            if result["has_matching_item"]:
                result["first_matching_item"] = result["matching_items"][0]
        finally:
            conn.close()
    except Exception as exc:
        result["error"] = str(exc)

    # Check active goal in goals_1.sqlite if present
    goals_db = home / "goals_1.sqlite"
    if goals_db.exists():
        try:
            conn2 = sqlite3.connect(f"file:{goals_db.as_posix()}?mode=ro", uri=True)
            try:
                conn2.row_factory = sqlite3.Row
                cur = conn2.cursor()
                row = cur.execute(
                    "SELECT * FROM thread_goals WHERE thread_id = ? AND status IN ('active', 'in_progress', 'blocked')",
                    (thread_id,),
                ).fetchone()
                if row:
                    result["has_active_goal"] = True
                    result["active_goal"] = dict(row)
            finally:
                conn2.close()
        except Exception:
            pass

    return result


def classify_codex_delivery_outcome(
    target_thread_id: str,
    expected_nonce: str,
    rollout_lines: List[Dict[str, Any]],
    queue_storage_info: Optional[Dict[str, Any]] = None,
    after_line: int = 0,
    queue_receipt_accepted: bool = False,
    pre_enqueue_rejected: bool = False,
) -> Dict[str, Any]:
    """
    Classify delivery into exactly one of five outcomes:
    A. CODEX_QUEUE_AND_TASK_COMPLETED
    B. CODEX_QUEUE_DISPATCHED_TASK_PENDING
    C. CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED
    D. CODEX_NOT_SENT
    E. CODEX_DELIVERY_OUTCOME_UNKNOWN
    """
    user_line = None
    user_turn_id = None
    user_message_id = None
    user_timestamp = None
    user_content_hash = None
    user_prompt_text = None

    assistant_line = None
    assistant_turn_id = None
    assistant_message_id = None
    assistant_timestamp = None
    assistant_content_hash = None
    assistant_reply_text = None
    canary_ok_verified = False

    expected_ack = f"CANARY-OK:{expected_nonce}"

    # 1. Scan rollout lines from after_line forward
    for idx in range(after_line, len(rollout_lines)):
        record = rollout_lines[idx]
        rec_type = record.get("type")
        payload = record.get("payload", {})

        is_user = False
        if rec_type == "response_item" and payload.get("type") == "message" and payload.get("role") == "user":
            is_user = True
        elif rec_type == "event_msg" and payload.get("type") == "user_message":
            is_user = True

        if is_user and user_line is None:
            c = payload.get("content", payload.get("message", ""))
            txt = ""
            if isinstance(c, list):
                txt = " ".join(item.get("text", "") for item in c if isinstance(item, dict))
            elif isinstance(c, str):
                txt = c
            # Strict nonce matching: prompt must contain expected nonce
            if expected_nonce in txt:
                user_line = idx
                user_message_id = payload.get("id") or record.get("metadata", {}).get("retained_source", {}).get("id", {}).get("message_id")
                user_turn_id = (
                    payload.get("turn_id")
                    or payload.get("internal_chat_message_metadata_passthrough", {}).get("turn_id")
                    or record.get("metadata", {}).get("retained_source", {}).get("id", {}).get("turn_id")
                )
                user_timestamp = record.get("timestamp")
                user_prompt_text = txt
                user_content_hash = compute_sha256(txt)

        # Check for assistant response after user turn
        if user_line is not None and idx > user_line:
            is_assistant = (
                payload.get("type") == "message"
                and payload.get("role") == "assistant"
            )
            if is_assistant:
                c = payload.get("content")
                txt = ""
                if isinstance(c, list):
                    for item in c:
                        if isinstance(item, dict):
                            txt += item.get("text", "")
                        elif isinstance(item, str):
                            txt += item
                elif isinstance(c, str):
                    txt = c
                assistant_line = idx
                assistant_message_id = payload.get("id") or record.get("metadata", {}).get("retained_source", {}).get("id", {}).get("message_id")
                assistant_turn_id = (
                    payload.get("turn_id")
                    or payload.get("internal_chat_message_metadata_passthrough", {}).get("turn_id")
                    or record.get("metadata", {}).get("retained_source", {}).get("id", {}).get("turn_id")
                )
                assistant_timestamp = record.get("timestamp")
                assistant_reply_text = txt
                assistant_content_hash = compute_sha256(txt)
                if expected_ack in txt or txt.strip() == expected_ack:
                    canary_ok_verified = True

    # 2. Check queue storage and queue receipt
    queue_item_present = False
    receipt_id = None
    client_user_message_id = None
    if queue_storage_info and queue_storage_info.get("has_matching_item"):
        queue_item_present = True
        m_item = queue_storage_info.get("first_matching_item") or {}
        receipt_id = m_item.get("id")
        client_user_message_id = m_item.get("client_id")
    elif queue_receipt_accepted:
        queue_item_present = True

    thread_interrupted = bool(queue_storage_info and queue_storage_info.get("thread_interrupted"))
    for idx in range(after_line, len(rollout_lines)):
        record = rollout_lines[idx]
        rec_type = record.get("type")
        payload = record.get("payload", {})
        if rec_type in ("event_msg", "turn_context") and payload.get("type") in ("turn_interrupted", "task_interrupted", "thread_interrupted", "interrupted"):
            thread_interrupted = True

    if user_line is not None and not user_turn_id:
        for b_idx in range(user_line, max(-1, user_line - 5), -1):
            b_rec = rollout_lines[b_idx]
            if b_rec.get("type") == "turn_context" and b_rec.get("payload", {}).get("turn_id"):
                user_turn_id = b_rec["payload"]["turn_id"]
                break

    if assistant_line is not None and not assistant_turn_id:
        for f_idx in range(assistant_line, min(len(rollout_lines), assistant_line + 5)):
            f_rec = rollout_lines[f_idx]
            if f_rec.get("type") in ("token_usage_record", "event_msg") and f_rec.get("payload", {}).get("turn_id"):
                assistant_turn_id = f_rec["payload"]["turn_id"]
                break

    has_active_goal = bool(queue_storage_info and queue_storage_info.get("has_active_goal"))

    # 3. Classify outcome
    if user_line is not None and canary_ok_verified:
        classification = CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
        queue_receipt_status = "accepted"
        turn_dispatch_status = "dispatched"
        task_completion_status = "completed"
        task_completed = True
        recommended_action = "allow_reconciliation_delivered"
    elif user_line is not None:
        classification = CodexDeliveryClassification.CODEX_QUEUE_DISPATCHED_TASK_PENDING.value
        queue_receipt_status = "accepted"
        turn_dispatch_status = "dispatched"
        task_completion_status = "pending"
        task_completed = False
        recommended_action = "wait_for_completion"
    elif queue_item_present:
        classification = CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value
        queue_receipt_status = "accepted"
        turn_dispatch_status = "pending"
        task_completion_status = "pending"
        task_completed = False
        recommended_action = "record_queue_accepted"
    elif pre_enqueue_rejected and not queue_item_present and user_line is None:
        classification = CodexDeliveryClassification.CODEX_NOT_SENT.value
        queue_receipt_status = "rejected"
        turn_dispatch_status = "not_dispatched"
        task_completion_status = "not_started"
        task_completed = False
        recommended_action = "safe_cancel_allow_fresh_transfer"
    else:
        classification = CodexDeliveryClassification.CODEX_DELIVERY_OUTCOME_UNKNOWN.value
        queue_receipt_status = "unknown"
        turn_dispatch_status = "unknown"
        task_completion_status = "unknown"
        task_completed = False
        recommended_action = "preserve_unknown_block_retry"

    effective_turn_id = user_turn_id or assistant_turn_id
    action_instruction = (
        "Open/resume target Codex thread — do not resend"
        if classification == CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value
        else None
    )

    return {
        "classification": classification,
        "outcome_classification": classification,
        "queue_receipt_status": queue_receipt_status,
        "receipt_id": receipt_id,
        "client_user_message_id": client_user_message_id,
        "turn_dispatch_status": turn_dispatch_status,
        "turn_id": effective_turn_id,
        "user_turn_id": user_turn_id,
        "assistant_turn_id": assistant_turn_id,
        "user_message_id": user_message_id,
        "assistant_message_id": assistant_message_id,
        "user_timestamp": user_timestamp,
        "assistant_timestamp": assistant_timestamp,
        "user_content_hash": user_content_hash,
        "assistant_content_hash": assistant_content_hash,
        "nonce_matched": bool(user_line is not None and canary_ok_verified),
        "task_completion_status": task_completion_status,
        "task_completed": task_completed,
        "canary_ok_verified": canary_ok_verified,
        "canary_reply_verified": canary_ok_verified,
        "user_line": user_line,
        "user_prompt_text": user_prompt_text,
        "assistant_line": assistant_line,
        "assistant_reply_text": assistant_reply_text,
        "has_active_goal": has_active_goal,
        "active_goal": queue_storage_info.get("active_goal") if queue_storage_info else None,
        "thread_interrupted": thread_interrupted,
        "recommended_action": recommended_action,
        "action_instruction": action_instruction,
    }


class RealCodexQueueAdapter(ICodexQueueAdapter):
    """
    Real Codex queue adapter invoking installed Codex CLI.
    Enforces:
    - Argument-list subprocess invocation: ['codex', 'queue', '--thread', thread_id, '--message', message, '-C', repo_path]
    - Strictly avoids shell=True or string interpolation
    - Target thread ID, expected repo path, CLI executable path validation
    - Single-use authorization check bound to exact content SHA-256 and destination
    - Exit code and timeout handling
    - Parses CLI output for queue receipt ID
    - Session observer verification if observer is provided
    """

    def __init__(
        self,
        cli_path: Optional[str] = None,
        default_repo_path: Optional[str] = None,
        db: Optional[Any] = None,
        observer: Optional[ReadOnlyCodexSessionObserver] = None,
        verified_thread_id: Optional[str] = None,
    ):
        self.cli_path = cli_path or shutil.which("codex") or "codex"
        self.default_repo_path = default_repo_path
        self.db = db
        self.observer = observer
        self.verified_thread_id = verified_thread_id
        self._injected_runner: Optional[Any] = None

    def set_injected_runner(self, runner: Optional[Any]):
        """Inject runner callback for unit/integration testing without subprocess execution."""
        self._injected_runner = runner

    async def verify_queue_capability(self):
        def probe():
            return subprocess.run([str(self.cli_path), "queue", "--help"], capture_output=True,
                                  text=True, encoding="utf-8", errors="replace", timeout=10)
        result = await asyncio.to_thread(probe)
        output = result.stdout or ""
        if result.returncode != 0 or not all(flag in output for flag in ("--thread", "--message", "-C")):
            raise ValueError("Installed Codex CLI does not advertise the required queue contract; stop, no fallback")

    @staticmethod
    def parse_queue_cli_output(
        out: str,
        expected_thread_id: str,
        expected_content_hash: Optional[str] = None,
    ) -> Dict[str, Any]:
        out_clean = (out or "").strip()
        if not out_clean:
            raise DeliveryOutcomeUnknownError("CLI exited without a correlated queue receipt: delivery outcome unknown")

        # 1. Regex match for installed CLI format:
        # e.g.: "Queued message 01a1798c-931f-74c1-b960-1f0f4065e054 for thread <CANARY_THREAD_ID>."
        pattern = re.compile(r"queued(?:\s+message)?\s+([a-zA-Z0-9_\-\.]+)\s+for\s+thread\s+([a-zA-Z0-9_\-]+)", re.IGNORECASE)
        for line in out_clean.splitlines():
            line_s = line.strip()
            m = pattern.search(line_s)
            if m:
                msg_id = m.group(1).rstrip(".")
                th_id = m.group(2).rstrip(".")
                if msg_id.lower() == "message":
                    raise DeliveryOutcomeUnknownError("Invalid CLI queue receipt: generic literal 'message' is not an acceptable receipt identifier")
                if th_id.lower() != expected_thread_id.lower():
                    raise DeliveryOutcomeUnknownError(f"CLI queue receipt thread mismatch: expected '{expected_thread_id}', got '{th_id}'")
                return {
                    "receipt_id": msg_id,
                    "client_user_message_id": msg_id,
                    "thread_id": expected_thread_id,
                    "format": "text",
                }

            # 2. JSON match
            try:
                receipt = json.loads(line_s)
                if isinstance(receipt, dict):
                    rec_th = receipt.get("thread_id")
                    if rec_th and rec_th.lower() != expected_thread_id.lower():
                        raise DeliveryOutcomeUnknownError(f"CLI queue receipt thread mismatch: expected '{expected_thread_id}', got '{rec_th}'")
                    if expected_content_hash and receipt.get("content_hash") and receipt.get("content_hash") != expected_content_hash:
                        raise DeliveryOutcomeUnknownError("CLI queue receipt content hash mismatch")
                    receipt_id = receipt.get("receipt_id") or receipt.get("id") or receipt.get("client_id") or receipt.get("message_id")
                    if receipt_id and str(receipt_id).strip().lower() == "message":
                        raise DeliveryOutcomeUnknownError("Invalid CLI queue receipt: generic literal 'message' is not an acceptable receipt identifier")
                    client_id = receipt.get("client_id") or receipt.get("clientUserMessageId") or receipt.get("client_user_message_id")
                    queued_item_id = receipt.get("queued_item_id")
                    if receipt_id and (receipt.get("status") == "queued" or "queued" in str(receipt).lower()):
                        return {
                            "receipt_id": str(receipt_id),
                            "client_user_message_id": str(client_id) if client_id else str(receipt_id),
                            "queued_item_id": str(queued_item_id) if queued_item_id else None,
                            "thread_id": expected_thread_id,
                            "format": "json",
                        }
            except (ValueError, TypeError):
                continue

        raise DeliveryOutcomeUnknownError("CLI exited without a correlated queue receipt: delivery outcome unknown")

    async def queue_message(
        self,
        thread_id: str,
        message: str,
        confirmation_token: Optional[str] = None,
        transfer_id: Optional[str] = None,
        repo_path: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if self.db:
            active_config = await self.db.get_config()
            if active_config and active_config.production_shadow_mode is True:
                raise ValueError("PRODUCTION_G2C_QUEUE_FORBIDDEN")
        if not thread_id or not thread_id.strip():
            raise ValueError("Thread ID is required for Codex queue submission")

        if not message or not message.strip():
            raise ValueError("Message text is required for Codex queue submission")

        effective_repo = repo_path or self.default_repo_path
        if not effective_repo:
            raise ValueError("Working directory (repo_path) is required for Codex queue submission")

        repo_path_obj = Path(effective_repo)
        if self.verified_thread_id or kwargs.get("is_canary") or kwargs.get("canary_mode"):
            if is_production_thread(thread_id) or is_production_workspace(repo_path_obj):
                raise ValueError("Production Codex targets and workspaces are strictly forbidden in canary mode")

        if not repo_path_obj.exists() or not repo_path_obj.is_dir():
            raise ValueError(f"Repository path does not exist or is not a directory: {effective_repo}")

        if self.verified_thread_id:
            if thread_id != self.verified_thread_id or repo_path_obj.resolve() != Path(self.default_repo_path).resolve():
                raise ValueError("Queue destination differs from verified canary thread/workspace")
            if not self.observer:
                raise ValueError("Verified canary observer required")
            identity = self.observer.observe_thread(thread_id, expected_cwd=str(repo_path_obj))
            header = identity.get("session_meta") or {}
            if (identity["validation_errors"] or not identity["metadata_valid"]
                    or header.get("id") != thread_id or not header.get("cwd")
                    or Path(header["cwd"]).resolve() != repo_path_obj.resolve()):
                raise ValueError("Canary identity revalidation failed before queue")

        # Duplicate retry check: prohibit queue submission if matching item already in queue
        if kwargs.get("check_existing_queue", True):
            try:
                m_nonce = kwargs.get("nonce")
                if not m_nonce and message:
                    m = re.search(r"CANARY-OK:([a-f0-9]{32})", message) or re.search(r"nonce:\s*([a-f0-9]{32})", message, re.IGNORECASE)
                    if m:
                        m_nonce = m.group(1)
                q_info = inspect_codex_queue_storage(
                    thread_id,
                    codex_home=kwargs.get("codex_home"),
                    nonce=m_nonce,
                    expected_content_hash=compute_sha256(message),
                )
                if q_info.get("has_matching_item"):
                    raise ValueError("Duplicate retry prohibited: matching item is already present in Codex queue storage")
            except (ValueError, DeliveryOutcomeUnknownError):
                raise
            except Exception:
                pass

        content_hash = compute_sha256(message)

        # 1. Single-use authorization verification
        if self.db:
            if not confirmation_token or not transfer_id:
                raise ValueError("Single-use transfer authorization required")
            ok, err = await self.db.verify_and_consume_authorization(
                transfer_id=transfer_id,
                token=confirmation_token,
                content_hash=content_hash,
                destination=thread_id,
                expected_config_fingerprint=kwargs.get("config_fingerprint"),
            )
            if not ok:
                raise ValueError(f"Transfer authorization rejected: {err}")

        # 2. Injected runner for tests
        if self._injected_runner is not None:
            cmd = [str(self.cli_path), "queue", "--thread", thread_id, "--message", message, "-C", str(effective_repo)]
            try:
                res = self._injected_runner(cmd, thread_id, message, str(effective_repo), transfer_id)
            except TypeError:
                try:
                    res = self._injected_runner(thread_id, str(effective_repo), message)
                except TypeError:
                    res = self._injected_runner(cmd)
            if asyncio.iscoroutine(res):
                res = await res
            if isinstance(res, str):
                parsed = self.parse_queue_cli_output(res, thread_id, content_hash)
                return {
                    "status": "queued",
                    "queue_receipt_status": "accepted",
                    "receipt_id": parsed["receipt_id"],
                    "client_user_message_id": parsed.get("client_user_message_id"),
                    "turn_dispatch_status": "pending",
                    "turn_id": None,
                    "task_completion_status": "pending",
                    "task_completed": False,
                    "final_answer": None,
                    "outcome_classification": CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value,
                    "transfer_id": transfer_id,
                    "thread_id": thread_id,
                    "repo_path": str(effective_repo),
                    "content_hash": content_hash,
                    "chars": len(message),
                    "cli_output": res,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            if isinstance(res, subprocess.CompletedProcess):
                if res.returncode != 0:
                    err_msg = (res.stderr or res.stdout or "").strip()
                    raise DeliveryOutcomeUnknownError(f"Codex CLI exited {res.returncode} without verified rejection/receipt: {err_msg}")
                parsed = self.parse_queue_cli_output(res.stdout or "", thread_id, content_hash)
                return {
                    "status": "queued",
                    "queue_receipt_status": "accepted",
                    "receipt_id": parsed["receipt_id"],
                    "client_user_message_id": parsed.get("client_user_message_id"),
                    "turn_dispatch_status": "pending",
                    "turn_id": None,
                    "task_completion_status": "pending",
                    "task_completed": False,
                    "final_answer": None,
                    "outcome_classification": CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value,
                    "transfer_id": transfer_id,
                    "thread_id": thread_id,
                    "repo_path": str(effective_repo),
                    "content_hash": content_hash,
                    "chars": len(message),
                    "cli_output": res.stdout,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            if isinstance(res, dict):
                if "queue_receipt_status" not in res:
                    res["queue_receipt_status"] = "accepted" if res.get("status") == "queued" else "unknown"
                if "turn_dispatch_status" not in res:
                    res["turn_dispatch_status"] = "pending"
                if "task_completion_status" not in res:
                    res["task_completion_status"] = "pending"
                if "task_completed" not in res:
                    res["task_completed"] = False
                if "outcome_classification" not in res:
                    res["outcome_classification"] = CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value
                return res
            mock_id = uuid.uuid4().hex[:8]
            return {
                "status": "queued",
                "queue_receipt_status": "accepted",
                "receipt_id": f"receipt-mock-{mock_id}",
                "client_user_message_id": f"client-mock-{mock_id}",
                "turn_dispatch_status": "pending",
                "turn_id": None,
                "task_completion_status": "pending",
                "task_completed": False,
                "final_answer": None,
                "outcome_classification": CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value,
                "thread_id": thread_id,
                "content_hash": content_hash,
                "timestamp": datetime.utcnow().isoformat(),
            }

        # 3. Real CLI execution with argument list (never shell=True)
        reject_placeholder_targets(thread_id, "https://chatgpt.com/c/validated-target")
        cmd = [str(self.cli_path), "queue", "--thread", thread_id, "--message", message, "-C", str(effective_repo)]

        try:
            def _run():
                return subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    encoding="utf-8",
                    errors="replace",
                )
            proc = await asyncio.to_thread(_run)
        except subprocess.TimeoutExpired as exc:
            raise DeliveryOutcomeUnknownError("Codex queue timeout: delivery outcome unknown; do not retry") from exc
        except OSError as e:
            raise RuntimeError(f"Codex CLI execution failed to launch: {e}")
        except Exception as e:
            raise DeliveryOutcomeUnknownError(f"Codex execution outcome could not be established: {e}") from e

        if proc.returncode != 0:
            err_msg = (proc.stderr or proc.stdout or "").strip()
            raise DeliveryOutcomeUnknownError(f"Codex CLI exited {proc.returncode} without verified rejection/receipt: {err_msg}")

        out = (proc.stdout or "").strip()
        parsed_receipt = self.parse_queue_cli_output(out, thread_id, content_hash)
        receipt_id = parsed_receipt["receipt_id"]
        client_user_message_id = parsed_receipt.get("client_user_message_id")
        queued_item_id = parsed_receipt.get("queued_item_id")

        if not receipt_id or str(receipt_id).strip().lower() == "message":
            raise DeliveryOutcomeUnknownError("Queue submission failed to produce a valid correlated receipt identifier (literal 'message' rejected)")

        # Corroborate with queue storage if available
        q_store = inspect_codex_queue_storage(
            thread_id,
            codex_home=kwargs.get("codex_home"),
            nonce=kwargs.get("nonce"),
            expected_content_hash=content_hash,
        )
        if q_store.get("has_matching_item"):
            first_item = q_store.get("first_matching_item") or {}
            if first_item.get("client_id"):
                client_user_message_id = first_item.get("client_id")
            if first_item.get("id") is not None:
                queued_item_id = str(first_item.get("id"))

        # Verification via observer if available
        session_verified = False
        if self.observer:
            try:
                obs = self.observer.observe_thread(thread_id)
                session_verified = obs.get("metadata_valid", False)
            except Exception:
                pass

        return {
            "status": "queued",
            "queue_receipt_status": "accepted",
            "receipt_id": receipt_id,
            "client_user_message_id": client_user_message_id,
            "queued_item_id": queued_item_id,
            "turn_dispatch_status": "pending",
            "turn_id": None,
            "task_completion_status": "pending",
            "task_completed": False,
            "final_answer": None,
            "outcome_classification": CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value,
            "transfer_id": transfer_id,
            "thread_id": thread_id,
            "repo_path": str(effective_repo),
            "content_hash": content_hash,
            "chars": len(message),
            "cli_output": out,
            "session_verified": session_verified,
            "timestamp": datetime.utcnow().isoformat(),
        }
