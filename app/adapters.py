"""
Adapter interfaces and implementations for Phase 1.
Includes read-only Codex session parser and mock adapters for side-effecting operations.
"""
from abc import ABC, abstractmethod
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

from app.models import compute_sha256

ERROR_PLACEHOLDERS = [
    "无法加载此回复",
    "There was an error generating a response",
    "Something went wrong, please try reloading the conversation.",
    "Failed to generate response",
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
        self, thread_id: str, message: str
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
    async def send_message(self, message: str) -> Dict[str, Any]:
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
    def __init__(self, sessions_base_dir: Path):
        self.sessions_base_dir = Path(sessions_base_dir)

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
        session_file = self.locate_session_file(thread_id)
        if not session_file or not session_file.exists():
            return None, -1, None

        with open(session_file, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        final_text = None
        final_idx = -1
        found_turn_id = None
        curr_turn = None

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

            if rec_type == "turn_context" and "turn_id" in payload:
                curr_turn = payload.get("turn_id")

            if (
                payload.get("type") == "message"
                and payload.get("role") == "assistant"
                and payload.get("phase") == "final_answer"
            ):
                if payload.get("status") == "completed" or idx == len(lines) - 1:
                    content = payload.get("content")
                    txt = ""
                    if isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict) and item.get("type") == "output_text":
                                txt += item.get("text", "")
                            elif isinstance(item, str):
                                txt += item
                    elif isinstance(content, str):
                        txt = content

                    final_text = txt
                    final_idx = idx
                    found_turn_id = curr_turn or payload.get("turn_id")

        return final_text, final_idx, found_turn_id


class MockCodexQueueAdapter(ICodexQueueAdapter):
    def __init__(self):
        self.queued_messages: List[Dict[str, Any]] = []

    async def queue_message(self, thread_id: str, message: str) -> Dict[str, Any]:
        msg_id = f"mock-queue-{len(self.queued_messages) + 1:04d}"
        entry = {
            "queue_message_id": msg_id,
            "thread_id": thread_id,
            "message_hash": compute_sha256(message),
            "chars": len(message),
            "status": "queued_mock",
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

    async def send_message(self, message: str) -> Dict[str, Any]:
        if self.simulate_unknown_outcome:
            raise RuntimeError("Browser connection dropped mid-transfer: delivery outcome unknown")

        record = {
            "send_id": f"mock-send-{len(self.sent_messages) + 1:04d}",
            "content_hash": compute_sha256(message),
            "chars": len(message),
            "status": "delivered_mock",
        }
        self.sent_messages.append(record)
        return record


class StandardPromptExtractor(IPromptExtractor):
    def extract(self, response_parts: Dict[str, Any]) -> Optional[str]:
        if not response_parts:
            return None

        # 1. Canvas text
        canvas = (response_parts.get("canvas_text") or "").strip()
        if is_error_placeholder(canvas):
            canvas = ""
        if len(canvas) >= 300 and ("Governance" in canvas or "Mission" in canvas):
            return canvas

        # 2. Code blocks (extract largest markdown/executable code block >= 200 chars)
        blocks = [
            b.strip()
            for b in response_parts.get("code_blocks", [])
            if b and len(b.strip()) >= 200 and not is_error_placeholder(b)
        ]
        if blocks:
            return max(blocks, key=len)

        # 3. Canvas fallback
        if len(canvas) >= 500:
            return canvas

        # 4. Structured markers in text
        text = response_parts.get("text") or ""
        if is_error_placeholder(text):
            return None

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
                    return cand

        return None
