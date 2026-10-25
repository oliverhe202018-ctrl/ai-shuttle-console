# Architecture, State Machine & Database Schema Documentation

## 1. Database Schema & Migration Strategy

The storage layer uses SQLite with asynchronous access via `aiosqlite`.
All schema objects and migrations are managed within `app/database.py`.

### Schema Version Table
```sql
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Configurations Table
Key-value storage for typed application configuration (`codex`, `chatgpt`, `route`, `server`).
```sql
CREATE TABLE IF NOT EXISTS configurations (
    key TEXT PRIMARY KEY,
    value JSON NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Checkpoints Table
Stores the authoritative monotonic state machine checkpoints.
```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    iteration_id INTEGER NOT NULL DEFAULT 0,
    source_identifier TEXT NOT NULL,
    destination_identifier TEXT NOT NULL,
    content_hash TEXT,
    payload_preview TEXT,
    transfer_status TEXT NOT NULL DEFAULT 'pending',
    failure_details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_created ON checkpoints(created_at DESC);
```

### Transfers Table
Records every message transfer attempt, content SHA-256, and delivery outcome.
```sql
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    iteration_id INTEGER NOT NULL DEFAULT 0,
    direction TEXT NOT NULL, -- 'codex_to_chatgpt' or 'chatgpt_to_codex'
    source_identifier TEXT NOT NULL,
    destination_identifier TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload_preview TEXT,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'sent', 'confirmed', 'failed', 'unknown'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_transfers_hash ON transfers(content_hash);
```

### Event Logs Table
Structured audit trails for state changes, operator actions, and errors.
```sql
CREATE TABLE IF NOT EXISTS event_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    source TEXT NOT NULL DEFAULT 'system',
    stage TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_event_logs_stage ON event_logs(stage);
```

---

## 2. State Machine Design

The shuttle state machine implements 12 explicit stages:

```
                  +----------------------------------------------+
                  |                     IDLE                     |<-------------------+
                  +----------------------------------------------+                    |
                                         |                                            |
                                         v                                            |
                            +--------------------------+                              |
                            |      CODEX_WAITING       |                              |
                            +--------------------------+                              |
                                         |                                            |
                                         v                                            |
                            +--------------------------+                              |
                            |   CODEX_RESPONSE_READY   |                              |
                            +--------------------------+                              |
                                         |                                            |
                                         v                                            |
                            +--------------------------+                              |
                            |      REPORT_PREVIEW      |                              |
                            +--------------------------+                              |
                                         |                                            |
                                         v                                            |
                            +--------------------------+                              |
                            |   CHATGPT_SEND_PENDING   |                              |
                            +--------------------------+                              |
                                   /            \                                     |
                         [Normal] /              \ [Indeterminate Network Loss]       |
                                 v                v                                   |
                    +--------------------+    +--------------------------+            |
                    |  CHATGPT_WAITING   |    | DELIVERY_OUTCOME_UNKNOWN |            |
                    +--------------------+    +--------------------------+            |
                               |                           |                          |
                               v                           v                          |
                    +-----------------------+   +------------------------+            |
                    | CHATGPT_RESPONSE_READY|   | HUMAN_ACTION_REQUIRED  |------------+
                    +-----------------------+   +------------------------+
                               |                           ^
                               v                           |
                    +--------------------+                 |
                    |   PROMPT_PREVIEW   |                 |
                    +--------------------+                 |
                               |                           |
                               v                           |
                    +--------------------+                 |
                    | CODEX_QUEUE_PENDING|                 |
                    +--------------------+                 |
                               |                           |
                   [Loop Back] +---------------------------+
```

### Stage Transition Rules & Invariants
1. **Monotonic Forward Progression**: Under normal operation, a checkpoint cannot regress to an earlier stage within the same iteration.
2. **Delivery Outcome Protection**: If network connectivity is lost while sending a message to ChatGPT, the state immediately transitions to `DELIVERY_OUTCOME_UNKNOWN`. Automated retry is strictly forbidden. The system transitions only to `HUMAN_ACTION_REQUIRED` for operator verification.
3. **Error Placeholder Filtering**: Error strings such as `无法加载此回复` or `There was an error generating a response` are treated as non-responses, preventing false progression to `PROMPT_PREVIEW`.
4. **Duplicate Prevention**: Transfers check historical SHA-256 hashes in `transfers` table. Any duplicate submission is blocked with HTTP 409.

---

## 3. Adapters & Interface Abstractions

All external communication is decoupled into clean interfaces (`app/adapters.py`):

1. **`ICodexSessionObserver`**:
   - `ReadOnlyCodexSessionObserver`: Scans session directories (`~/.codex/sessions/**`), locates files matching the thread ID, parses JSONL event streams, tracks `turn_context`, and extracts final assistant answers.
2. **`ICodexQueueAdapter`**:
   - `MockCodexQueueAdapter` (Phase 1): Computes SHA-256 hashes and generates mock queue message IDs without executing external commands.
3. **`IChatGPTBrowserObserver`**:
   - `MockChatGPTBrowserObserver` (Phase 1): Simulates browser readiness and Cloudflare/Turnstile verification challenges.
4. **`IChatGPTMessageSender`**:
   - `MockChatGPTMessageSender` (Phase 1): Simulates message submission and delivery failure modes (including ambiguous drops).
5. **`IPromptExtractor`**:
   - `StandardPromptExtractor`: Multi-tier extraction prioritizing canvas text, large markdown code blocks (`>= 200` chars), and governance sections (`Immutable Governance`, `0. Mission`).
