# Phase 2.6.1 · Final Live V2 Evidence Revalidation Public Report

## Status: PHASE_2_6_1_FINAL_CANARY_VERIFIED

## Executive Summary
This report records the completion of Phase 2.6.1 Final Live V2 Evidence Revalidation.
All operations were executed in the isolated Canary environment with:
- Zero manual Codex GUI interaction
- Zero SQLite direct queue mutation
- Zero production project access or file modification
- End-to-end Evidence Schema V2 identity verification across official cold-thread resume and Controlled Auto Mode (2 iterations)

---

## 1. Official Cold-Thread Resume Revalidation
- **Status**: `OFFICIAL_THREAD_RESUME_V2_EVIDENCE_VERIFIED`
- **Initial State**: genuinely cold (`status_type = notLoaded`, `is_cold = True`)
- **Queue Submission**: exactly one harmless item submitted via official CLI
- **Pre-resume State**: item remained pending without early rollout update
- **Resume Adapter**: official `thread/resume(threadId)` invoked via app-server
- **Resume Latency**: `99.15 ms`
- **Queue-to-Dispatch Latency**: `1502.15 ms`
- **Completion Latency**: `15270.38 ms`
- **GUI Interaction Count**: `0`
- **Duplicate Count**: `0`
- **Evidence Schema V2 Integrity**:
  - `queue_receipt_id`: REAL (`<QUEUE_RECEIPT_ID>`)
  - `queue_client_user_message_id`: REAL (`<QUEUE_CLIENT_USER_MESSAGE_ID>`)
  - `queued_item_id`: `NULL` (explicitly None; no substitution)
  - `codex_user_turn_id`: REAL (`<CODEX_USER_TURN_ID>`)
  - `codex_user_message_id`: REAL (`<CODEX_USER_MESSAGE_ID>`)
  - `codex_assistant_message_id`: REAL (`<CODEX_ASSISTANT_MESSAGE_ID>`)
  - `final_answer`: exact match `CANARY-OK:<NONCE_REDACTED>`

---

## 2. Controlled Auto Mode Revalidation (2 Iterations)
- **Status**: `CONTROLLED_AUTO_MODE_CANARY_VERIFIED`
- **Run ID**: `<AUTO_RUN_ID>`
- **Authorized Iterations**: `2`
- **Completed Iterations**: `2`
- **Operator Approvals**: single run authorization (zero per-transfer prompts)
- **Total Duration**: `105.74 s`
- **Duplicate C2G Count**: `0`
- **Duplicate G2C Count**: `0`
- **Unknown Count**: `0`
- **Unresolved Transfers**: `0`

### Iteration 1
- **Status**: `completed`
- **Evidence Schema Version**: `2`
- **ChatGPT User Turn ID**: REAL (`<CHATGPT_USER_TURN_ID_1>`, verified in live DOM)
- **ChatGPT User Message ID**: REAL (`<CHATGPT_USER_MESSAGE_ID_1>`, verified in live DOM)
- **ChatGPT Assistant Turn ID**: REAL (`<CHATGPT_ASSISTANT_TURN_ID_1>`, verified in live DOM)
- **ChatGPT Assistant Message ID**: REAL (`<CHATGPT_ASSISTANT_MESSAGE_ID_1>`, verified in live DOM)
- **Planner Classification**: `NEXT_CODEX_PROMPT_FOUND`
- **Queue Receipt ID**: REAL (`<QUEUE_RECEIPT_ID_1>`)
- **Queue Client User Message ID**: REAL (`<QUEUE_CLIENT_USER_MESSAGE_ID_1>`)
- **Queued Item ID**: `NULL` (explicitly None; not duplicated)
- **Codex Target Turn ID**: REAL (`<CODEX_TARGET_TURN_ID_1>`, verified in rollout JSONL)
- **Codex User Message ID**: REAL (`<CODEX_USER_MESSAGE_ID_1>`, verified in rollout JSONL)
- **Codex Assistant Message ID**: REAL (`<CODEX_ASSISTANT_MESSAGE_ID_1>`, verified in rollout JSONL)
- **Codex Output Hash**: verified

### Iteration 2
- **Status**: `completed`
- **Evidence Schema Version**: `2`
- **ChatGPT User Turn ID**: REAL (`<CHATGPT_USER_TURN_ID_2>`, verified in live DOM)
- **ChatGPT User Message ID**: REAL (`<CHATGPT_USER_MESSAGE_ID_2>`, verified in live DOM)
- **ChatGPT Assistant Turn ID**: REAL (`<CHATGPT_ASSISTANT_TURN_ID_2>`, verified in live DOM)
- **ChatGPT Assistant Message ID**: REAL (`<CHATGPT_ASSISTANT_MESSAGE_ID_2>`, verified in live DOM)
- **Planner Classification**: `NEXT_CODEX_PROMPT_FOUND`
- **Queue Receipt ID**: REAL (`<QUEUE_RECEIPT_ID_2>`)
- **Queue Client User Message ID**: REAL (`<QUEUE_CLIENT_USER_MESSAGE_ID_2>`)
- **Queued Item ID**: `NULL` (explicitly None; not duplicated)
- **Codex Target Turn ID**: REAL (`<CODEX_TARGET_TURN_ID_2>`, verified in rollout JSONL)
- **Codex User Message ID**: REAL (`<CODEX_USER_MESSAGE_ID_2>`, verified in rollout JSONL)
- **Codex Assistant Message ID**: REAL (`<CODEX_ASSISTANT_MESSAGE_ID_2>`, verified in rollout JSONL)
- **Codex Output Hash**: verified

---

## 3. Evidence Schema V2 Field Audit Summary
| Field | Iteration 1 | Iteration 2 | Audit Rule / Verdict |
|---|---|---|---|
| ChatGPT User Turn ID | REAL | REAL | Non-null, matches live DOM, != transfer ID, != run ID |
| ChatGPT User Message ID | REAL | REAL | Non-null, matches live DOM |
| ChatGPT Assistant Turn ID | REAL | REAL | Non-null, matches live DOM |
| ChatGPT Assistant Message ID | REAL | REAL | Non-null, matches live DOM |
| Queue Receipt ID | REAL | REAL | Non-null, != "message" |
| Queue Client User Message ID | REAL | REAL | Non-null, distinct from queued_item_id |
| Queued Item ID | NULL | NULL | Explicitly NULL (not available in CLI text output; zero fabrication) |
| Codex Target Turn ID | REAL | REAL | Non-null, correlated to current queued prompt in rollout |
| Codex User Message ID | REAL | REAL | Non-null, correlated to client user message in rollout |
| Codex Assistant Message ID | REAL | REAL | Non-null, matches assistant message after current turn |

---

## 4. Restart-Recovery Read-Only Verification
- **Test State**: staged active unfinished state (`AUTO_WAITING_CODEX`)
- **Service Recovery Result**: transitioned to `AUTO_PAUSED_RECOVERY_REQUIRED`
- **Replay Count**: `0`
- **Automatic ChatGPT Sends**: `0`
- **Automatic Codex Queue Calls**: `0`
- **Automatic Thread Resumes**: `0`
- **Verdict**: fail-closed restart recovery verified

---

## 5. Production Strategy Static Verification
- **Strategy Class**: `ProductionShuttlePayloadStrategy`
- **C2G Payload Contract**: equals exact prior Codex completed output
- **Nonce Injection**: none (no Canary nonce)
- **Task Synthesis**: none (zero synthesis)
- **Rewrite**: none (zero rewrite)
- **Planner Role**: ChatGPT remains sole planner
- **Verdict**: `PRODUCTION_STRATEGY_STATIC_CONTRACT_VERIFIED`

---

## 6. Private Evidence Storage
- **Unredacted Private Evidence Path**: `runtime/private-reports/phase2_6_1_final/phase2_6_1_final_evidence.json`
- **Ignored by Git**: verified (`runtime/` is covered by `.gitignore`)
- **Public Report Sanitization**: complete (zero raw IDs, UUIDs, nonces, local paths, or usernames published)

---

## Final Verdict
`PHASE_2_6_1_FINAL_CANARY_VERIFIED`
