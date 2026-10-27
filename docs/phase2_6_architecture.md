# Phase 2.6 Architecture: Official Codex Thread Lifecycle Automation & Controlled Auto Shuttle Mode

## 1. Overview & Core Mission

The AI Shuttle Console Phase 2.6 upgrades the Local AI Shuttle Control Center from:
`stable human-authorized bidirectional transport`
to:
`bounded operator-authorized multi-round automatic shuttle`

While doing so, it strictly maintains the central architectural invariant:
> **The REAL existing ChatGPT conversation remains the sole planner.**
> The Shuttle Console must never become the planner, rewrite prompts, synthesize next steps, or substitute an API/model for the real ChatGPT project conversation.

---

## 2. App-Server Transport Architecture (`app/codex_app_server.py`)

Rather than relying on fragile GUI automation or mutating local SQLite databases, Phase 2.6 communicates directly with the official Codex application server over standard input/output.

### 2.1 Transport Layer (`AppServerTransport`)
- **Process Management**: Launches official `codex app-server --stdio` as an isolated subprocess using explicit `argv`.
- **Process Ownership**: Clean PID tracking, asynchronous stdout/stdin streams with line-delimited UTF-8 framing.
- **Security Isolation**: Stdio-based communication eliminates the need for open listening TCP ports, preventing public or network socket exposure.
- **Graceful Teardown**: Two-stage shutdown via SIGTERM with bounded timeout (3.0s), falling back to SIGKILL if unresponsive.

### 2.2 Protocol Layer (`AppServerRpcClient`)
- **JSON-RPC 2.0 Compliance**: Bidirectional JSON-RPC 2.0 client supporting request/response multiplexing with unique incrementing integer IDs.
- **Asynchronous Dispatch**: Continuous event-driven read loop that routes:
  1. Responses correlated back to pending client futures via `call_id`.
  2. Server requests to client (e.g. tool execution approvals, permission requests).
  3. Server notifications (e.g. thread updates, turn events) to registered handlers.
- **Timeout & Failure Handling**: Configurable per-call timeout (default 30s) throwing `CodexRpcTimeoutError`; process crashes immediately cancel pending futures with `CodexProcessTerminatedError`.

---

## 3. Official Codex Thread Lifecycle Model (`CodexThreadLifecycleAdapter`)

Phase 2.6 leverages the official Codex lifecycle APIs to monitor and activate target threads.

### 3.1 Lifecycle States
- `notLoaded`: Thread exists on disk/session index but is cold (not active in memory).
- `loaded`: Thread is loaded in memory and monitored by the app-server.
- `running` / `active_writer`: Thread has an active turn running.

### 3.2 Thread Activation Sequence
1. **Queue Acceptance**: G2C queue submission is accepted into `queue_1.sqlite` by the existing verified adapter.
2. **State Inspection**: `check_thread_state(thread_id)` calls `thread/loaded/list` and `thread/read`.
3. **Cold Resume**: If the thread is `notLoaded` or absent from `thread/loaded/list`, invoke official `thread/resume(threadId)`.
4. **Active Writer Handling**: If `thread/resume` returns `-32600: thread <id> already has an active writer`, the adapter safely treats it as `already_active` without error.
5. **Dispatch & Completion**: The official Codex process automatically picks up the pending queue entry and executes it.
6. **Zero Duplicate Invariant**: No duplicate submissions are ever inserted; SQLite queue tables are never manually modified.

---

## 4. Stable vs. Experimental API Policy

- **Strict Production Default**: The initialization handshake explicitly sets:
  ```json
  "capabilities": {
    "experimentalApi": false
  }
  ```
- **Stable APIs**:
  - `initialize`: Standard handshake establishing client identity and feature negotiation.
  - `thread/read`: Safe inspection of thread metadata, status, and turn states.
  - `thread/resume`: Official cold-thread activation mechanism.
  - `thread/loaded/list`: Verified thread residency inventory.
- **Experimental APIs**:
  - `thread/queue/start`: Marked experimental and strictly disabled by default. The Shuttle relies on standard queue polling and observer confirmation.

---

## 5. Exactly-Once Delivery & Delivery Verification

Phase 2.6 inherits and hardens the Phase 2.5 delivery guarantees across both transport directions.

### 5.1 C2G (Codex -> ChatGPT)
- **Nonce-Anchored Artifacts**: Each Codex output includes an unambiguous canary nonce.
- **Pre-Fill Baseline DOM Capture**: Baseline message IDs are recorded before sending user text, preventing self-echo false matches.
- **Semantic Confirmation**: Delivery is confirmed only when the ChatGPT assistant turn explicitly appears, matches provenance, and is classified.

### 5.2 G2C (ChatGPT -> Codex)
- **Receipt Isolation**: Queue submission returns an official queue receipt ID.
- **Dispatch Verification**: Read-only session observer tracks the exact turn transition from queue dispatch to completion.
- **Three-Tier Evidence**: Turn ID, completed timestamp, and SHA-256 output hash must all be verified before an iteration completes.

---

## 6. Controlled Auto-Run Authorization Model (`app/auto_mode.py`)

Multi-round execution requires explicit operator authorization bound to cryptographic constraints.

### 6.1 Parent Run Authorization (`AutoRunAuthorization`)
- **Single-Use Operator Confirmation**: Requires explicit `operator_confirmed=True`.
- **Iteration Ceiling**: Hard-capped between 1 and 10 iterations (default 3).
- **Time-to-Live (TTL)**: Authorization expires automatically (default 60 minutes).
- **Target Immutability**: Cryptographically bound to:
  - Exact ChatGPT Conversation URL and ID.
  - Exact Codex Thread ID and canonicalized Workspace Path.
  - SHA-256 Config Fingerprint (`compute_config_fingerprint(cfg)`).
- **Production Guard**: Fails closed if any target matches protected production repository (e.g. `D:\workspace\protected-production-repo`).

### 6.2 Derived Child Authorizations (`ChildTransferAuthorization`)
- For each round, the coordinator derives exactly one `C2G` and one `G2C` child authorization.
- Child authorizations are bound to the parent `run_id`, iteration index, and direction.
- Single-use consumption enforces that no child token can be reused.

---

## 7. Fail-Closed Matrix

| Condition / Trigger | Observed Indicator | Coordinator Action | Resulting State |
| :--- | :--- | :--- | :--- |
| **Production Target Breach** | Target matches protected production repository | Immediate abort before start | `AUTO_FAILED` (Security Exception) |
| **Config Drift / Mismatch** | Fingerprint differs from parent auth | Refuse execution | `AUTO_FAILED` |
| **Authorization Expired** | Current time > `expires_at` | Abort active loop | `AUTO_FAILED` / `AUTO_STOPPED` |
| **Max Iterations Reached** | Completed == authorized | Clean termination | `AUTO_COMPLETED` |
| **Planner Stop Marker** | `SHUTTLE_STOP` found in planner output | Clean termination | `AUTO_COMPLETED` |
| **Ambiguous Planner Output** | Multiple competing prompt blocks | Halt iteration | `AUTO_STOPPED` |
| **Missing Actionable Prompt** | Planner provides no prompt | Halt iteration | `AUTO_STOPPED` |
| **Human Approval Required** | Codex requests patch/command approval | Pause loop, alert operator | `AUTO_HUMAN_ACTION_REQUIRED` |
| **Transport / RPC Failure** | Child process crash or timeout | Terminate run, cleanup | `AUTO_FAILED` |
| **Operator Pause** | `/api/auto/pause` requested | Complete current round, pause | `AUTO_PAUSED` |
| **Operator Emergency Stop** | `/api/auto/emergency-stop` requested | Immediate halt, kill tasks | `AUTO_EMERGENCY_STOPPED` |

---

## 8. Service Restart & Recovery Semantics

- **Persistent State**: Authorizations, iterations, child tokens, and lifecycle events are committed to SQLite.
- **Fail-Closed on Interruption**: If the Shuttle Console crashes or restarts while a run is in progress (`AUTO_RUNNING`, `AUTO_WAITING_*`, `AUTO_RESUMING_*`), the startup audit detects the interrupted run and transitions it to `AUTO_PAUSED_RECOVERY_REQUIRED`.
- **Zero Automatic Replay**: Stale or partially completed rounds are never automatically replayed or resumed after restart without operator reconciliation and fresh authorization.

---

## 9. Planner vs. Non-Planner Boundary

```
+-----------------------------------------------------------------------------------+
|                            ChatGPT Project Conversation                           |
|                                (Sole Planner)                                     |
|  - Determines next action, test strategy, or bug fix                              |
|  - Formats actionable prompt block or issues `SHUTTLE_STOP`                       |
+-----------------------------------------------------------------------------------+
                                        |  ^
                 Classified Prompt      |  |  Verified Output
                                        v  |
+-----------------------------------------------------------------------------------+
|                           AI Shuttle Console (Phase 2.6)                          |
|                       (Strict Non-Planner Bidirectional Bridge)                   |
|  - Bounded multi-round state machine                                              |
|  - Exact-once C2G / G2C transport                                                 |
|  - Official Codex `thread/resume` lifecycle adapter                               |
|  - Hard fail-closed safety barriers & operator controls                           |
+-----------------------------------------------------------------------------------+
                                        |  ^
                 Queue Submission       |  |  Completed Turn
                                        v  |
+-----------------------------------------------------------------------------------+
|                                Codex Local Engine                                 |
|                                (Code Execution)                                   |
|  - Executes tests, patches files in canary workspace                              |
|  - Produces verified output nonce & results                                       |
+-----------------------------------------------------------------------------------+
```

The Shuttle console acts strictly as an authenticated courier. It does not parse, modify, augment, or generate prompts.
