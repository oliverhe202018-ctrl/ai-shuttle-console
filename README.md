# Local AI Shuttle Control Center — Phase 1

A local web application for configuring, monitoring, and safely orchestrating message transfers between a specific Codex GUI conversation and a specific ChatGPT browser conversation.

## Features (Phase 1)

1. **Dashboard & Control Plane**:
   - Modern, responsive local control dashboard bound strictly to `127.0.0.1:8765`.
   - Real-time shuttle state machine visualization and safe manual resume/transition controls.
   - Live message preview and SHA-256 integrity hash verification.
   - Comprehensive event timeline and audit logging.

2. **Persistent State Machine**:
   - 12 typed and validated stages: `IDLE`, `CODEX_WAITING`, `CODEX_RESPONSE_READY`, `REPORT_PREVIEW`, `CHATGPT_SEND_PENDING`, `CHATGPT_WAITING`, `CHATGPT_RESPONSE_READY`, `PROMPT_PREVIEW`, `CODEX_QUEUE_PENDING`, `HUMAN_ACTION_REQUIRED`, `DELIVERY_OUTCOME_UNKNOWN`, `ERROR`.
   - SQLite backed durable checkpoints: auto-resumes latest checkpoint on restart without rolling back progress.
   - Monotonic transition rules with strict protection against premature side-effects.

3. **Safety & Zero Side-Effects**:
   - Refusal to automatically retry unknown delivery outcomes (`DELIVERY_OUTCOME_UNKNOWN` transitions exclusively to manual action or error inspection).
   - Rejection of browser error placeholders (e.g. `无法加载此回复`, `There was an error generating a response`).
   - Mock adapters for all external side effects in Phase 1 (no real queueing or browser submission).
   - Read-only Codex session JSONL parser with thread-matching and turn-boundary extraction.

4. **Database Architecture**:
   - SQLite tables: `schema_version`, `configurations`, `checkpoints`, `transfers`, `event_logs`.
   - Indexes on content hash, timestamp, and stages.

---

## Windows Setup & Launch Instructions

### 1. Requirements
- Python 3.11+
- Git

### 2. Virtual Environment Setup
Open Git Bash or Command Prompt in `D:\project\ai-shuttle-console`:

```bash
cd D:/project/ai-shuttle-console
uv venv .venv --python python3.11
# Or using standard python:
# python -m venv .venv

# Activate venv:
source .venv/Scripts/activate  # In bash
# or .venv\Scripts\activate    # In cmd / powershell

# Install dependencies:
uv pip install -r requirements.txt
```

### 3. Run Automated Tests
```bash
.venv/Scripts/python.exe -m pytest tests/test_shuttle_console.py -v -o asyncio_mode=auto
```
All 10 test cases will execute and verify the state machine, mock adapters, duplicate prevention, and API contracts.

### 4. Launch the Dashboard
```bash
.venv/Scripts/python.exe run.py
```
Open your browser at:
`http://127.0.0.1:8765`

---

## Configuration Reference (`config.example.json`)

```json
{
  "codex": {
    "repo_path": "D:/workspace/example-repo",
    "expected_branch": "feature/example-branch",
    "cli_path": "C:/Users/<USER>/AppData/Local/OpenAI/Codex/bin/codex.exe",
    "thread_id": "00000000-0000-0000-0000-000000000000",
    "thread_title": "Example Thread Title",
    "sessions_dir": "C:/Users/<USER>/.codex/sessions",
    "integration_mode": "mock"
  },
  "chatgpt": {
    "browser_type": "msedge",
    "profile_dir": "D:/workspace/.browser-profile/chatgpt-profile",
    "conversation_url": "https://chatgpt.com/g/g-p-example/c/00000000-0000-0000-0000-000000000000",
    "expected_gpt_id": "g-p-example",
    "expected_conversation_id": "00000000-0000-0000-0000-000000000000",
    "browser_readiness": "ready",
    "human_verification_state": "passed"
  },
  "route": {
    "codex_to_chatgpt_enabled": true,
    "chatgpt_to_codex_enabled": true,
    "prompt_extraction_policy": "strict_markdown_blocks",
    "manual_approval_required": true,
    "duplicate_detection_policy": "hash_and_prefix_match",
    "restart_recovery_policy": "resume_latest_checkpoint"
  },
  "server": {
    "host": "127.0.0.1",
    "port": 8765,
    "log_level": "info"
  }
}
```

---

## Interface Status Matrix (Phase 1)

| Interface / Component | Implementation Status | Note |
|---|---|---|
| `ReadOnlyCodexSessionObserver` | **REAL (READ-ONLY)** | Real file scanning, JSONL deserialization, turn matching |
| `MockCodexQueueAdapter` | **MOCKED** | Simulates queuing with hashes; no real `codex queue` invocation |
| `MockChatGPTBrowserObserver` | **MOCKED** | Mock browser state & verification challenge detection |
| `MockChatGPTMessageSender` | **MOCKED** | Simulates report submission; catches indeterminate errors |
| `StandardPromptExtractor` | **REAL** | Parses canvas, markdown blocks, and governance prompts |
| `ShuttleStateMachine` | **REAL (PRODUCTION)** | SQLite persistent state engine with monotonic transition guards |
