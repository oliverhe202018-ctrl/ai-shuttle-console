# Phase 2.6.1 · Controlled Auto Mode 2-Iteration Canary Verification Report

## Status: PHASE_2_6_1_FINAL_CANARY_VERIFIED

- **Run ID**: `<AUTO_RUN_ID>`
- **Authorized Iterations**: `2`
- **Completed Iterations**: `2`
- **Total Duration**: `102.45 s`
- **Target Thread ID**: `<CANARY_THREAD_ID>`
- **Target Workspace**: `<CANARY_WORKSPACE>`
- **ChatGPT Conversation**: `https://chatgpt.com/c/<CANARY_CONVERSATION_ID>`
- **Derived Child Authorizations**: `4`
- **Lifecycle Events Recorded**: `0`

## Iteration Details (Evidence Schema V2)

### Iteration 1
- **Status**: `completed`
- **Evidence Schema Version**: `2`
- **C2G Transfer ID**: `c2g-<AUTO_RUN_ID>-iter1`
- **ChatGPT User Turn ID**: `<CHATGPT_USER_TURN_ID_1>` (verified in live DOM)
- **ChatGPT User Message ID**: `<CHATGPT_USER_MESSAGE_ID_1>` (verified in live DOM)
- **ChatGPT Assistant Turn ID**: `<CHATGPT_ASSISTANT_TURN_ID_1>` (verified in live DOM)
- **ChatGPT Assistant Message ID**: `<CHATGPT_ASSISTANT_MESSAGE_ID_1>` (verified in live DOM)
- **Planner Classification**: `NEXT_CODEX_PROMPT_FOUND`
- **G2C Transfer ID**: `g2c-<AUTO_RUN_ID>-iter1`
- **Queue Receipt ID**: `<QUEUE_RECEIPT_ID_1>`
- **Queue Client Message ID**: `<QUEUE_CLIENT_USER_MESSAGE_ID_1>`
- **Queued Item ID**: `None` (explicitly NULL, no substitution)
- **Codex Target Turn ID**: `<CODEX_TARGET_TURN_ID_1>` (verified in rollout JSONL)
- **Codex User Message ID**: `<CODEX_USER_MESSAGE_ID_1>` (verified in rollout JSONL)
- **Codex Assistant Message ID**: `<CODEX_ASSISTANT_MESSAGE_ID_1>` (verified in rollout JSONL)
- **Codex Output Hash**: `<CODEX_OUTPUT_HASH_2>`

### Iteration 2
- **Status**: `completed`
- **Evidence Schema Version**: `2`
- **C2G Transfer ID**: `c2g-<AUTO_RUN_ID>-iter2`
- **ChatGPT User Turn ID**: `<CHATGPT_USER_TURN_ID_2>` (verified in live DOM)
- **ChatGPT User Message ID**: `<CHATGPT_USER_MESSAGE_ID_2>` (verified in live DOM)
- **ChatGPT Assistant Turn ID**: `<CHATGPT_ASSISTANT_TURN_ID_2>` (verified in live DOM)
- **ChatGPT Assistant Message ID**: `<CHATGPT_ASSISTANT_MESSAGE_ID_2>` (verified in live DOM)
- **Planner Classification**: `NEXT_CODEX_PROMPT_FOUND`
- **G2C Transfer ID**: `g2c-<AUTO_RUN_ID>-iter2`
- **Queue Receipt ID**: `<QUEUE_RECEIPT_ID_2>`
- **Queue Client Message ID**: `<QUEUE_CLIENT_USER_MESSAGE_ID_2>`
- **Queued Item ID**: `None` (explicitly NULL, no substitution)
- **Codex Target Turn ID**: `<CODEX_TARGET_TURN_ID_2>` (verified in rollout JSONL)
- **Codex User Message ID**: `<CODEX_USER_MESSAGE_ID_2>` (verified in rollout JSONL)
- **Codex Assistant Message ID**: `<CODEX_ASSISTANT_MESSAGE_ID_2>` (verified in rollout JSONL)
- **Codex Output Hash**: `<CODEX_OUTPUT_HASH_1>`

## Evidence V2 Cross-Check Audit
- **Live ChatGPT DOM Cross-Check**: `VERIFIED_MATCH` (all non-null IDs match actual DOM elements)
- **Codex Rollout JSONL Cross-Check**: `VERIFIED_MATCH` (current-turn correlation confirmed post-baseline)
- **ID Masquerading Audit**: `0` content hashes masquerade as IDs; `0` transfer IDs masquerade as turn IDs
- **Queue Identifier Separation**: `queue_receipt_id != 'message'`; `queued_item_id != client_msg_id`
- **Restart Recovery Audit**: `AUTO_PAUSED_RECOVERY_REQUIRED_VERIFIED` (zero side effects on restart)
- **Production Strategy Contract**: `PRODUCTION_STRATEGY_STATIC_CONTRACT_VERIFIED` (pure pass-through)

## Safety & Hygiene Audit
- **Duplicate C2G / G2C Transfers**: `0`
- **Manual Operator Transfer Approvals**: `0` (single run authorization)
- **Manual Codex Thread Opens**: `0` (official lifecycle adapter)
- **Direct SQLite Queue Mutations**: `0` (strictly prohibited)
- **Production Workspaces Touched**: `False`

## Final Gate Verdict
`PHASE_2_6_1_FINAL_CANARY_VERIFIED`