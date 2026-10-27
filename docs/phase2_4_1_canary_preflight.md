# Phase 2.4.1 — Canary preflight and operator runbook

Date: 2026-10-26 (Asia/Shanghai). This repair performed no ChatGPT sends, Codex submissions, browser send clicks, or autonomous transfers. No live canary success is claimed.

## Current readiness

**STOP: real canary identities are not configured.** The example intentionally retains recognizable placeholders and cannot pass preflight. A syntactically plausible UUID is not identity evidence.

Read-only host inspection found:

- CDP `http://127.0.0.1:9222/json/list` exposed one page, the protected production conversation ending in `<PROTECTED_CONVERSATION_ID>`. It was not navigated, edited, or sent anything.
- No `session_meta.cwd` in the configured `C:/Users/<USER>/.codex/sessions` resolved to `D:/project/ai-shuttle-console/canary_workspace`. No other thread was substituted.
- The example CLI path under `8925276705115dea` does not exist. PATH resolved to `C:/Users/<USER>/AppData/Local/OpenAI/Codex/bin/2993767bd1dfb009/codex.exe`; its `--help` and `queue --help` were read without a thread or message argument. Help advertises `--thread`, `--message`, and `-C`. Reconfirm the installed path before a future run.
- The disposable repository already exists. The protected production repository has existing changes; it is not claimed clean. This repair does not write there.

## Configuration and isolation

Previously, startup read `data/shuttle_console.db`, then `config.example.json` if no stored configuration existed. Copying a file to `config.json` did not select it. That legacy no-argument behavior is retained for existing work, and must not be used for canary operation.

The explicit launcher is:

```powershell
.\.venv\Scripts\python.exe run.py --canary --config .\config.canary.local.20261026-01.json
```

`--canary` requires `--config`, and that file must contain `canary_mode: true`. Explicit paths are checked before startup can initialize a database. A missing file, missing required canary field, placeholder, wrong URL/UUID, production target, non-isolated workspace/profile, absent CLI/index, disabled manual approval, or wrong binding is rejected. There is no fallback to `config.json`, a default example, an existing default database, a different conversation, or another thread.

Explicit-file state is stored separately at `runtime/explicit-config/<first-16-hex-of-SHA256-of-absolute-config-path>/state.db`. The existing default DB, configuration files, checkpoints, and pending authorizations remain untouched. The selected configuration is locked against API edits. Restarting with the same filename resumes its state; changing its contents after initialization is rejected. A separate filename creates separate state, and must never be used to bypass an unresolved delivery.

The launcher always calls Uvicorn with `host="127.0.0.1", port=8765`. Canary configuration rejects other bindings. Launch through `run.py`; an operator invoking Uvicorn directly is responsible for its CLI binding flags.

## Manual prerequisites before a later live operation

1. Manually create a dedicated disposable Codex chat whose recorded working directory is `D:/project/ai-shuttle-console/canary_workspace`. Obtain its actual UUID, actual sessions directory, and actual session index from that chat's metadata. The index must contain the exact ID, and the JSONL header must contain the same ID and disposable CWD. Do not reuse the production thread or this repair chat.
2. Manually create a dedicated ChatGPT test conversation in the isolated `D:/project/ai-shuttle-console/.canary-chatgpt-profile` browser profile. Complete login/verification manually. Obtain the actual conversation URL and UUID from its address bar. A `/g/.../c/...` URL also requires the corresponding `expected_gpt_id`; a plain `/c/...` URL uses null. Open exactly one matching page via loopback CDP. Do not use the production profile or production conversation.
3. Prepare a new local configuration without overwriting an existing file:

   ```powershell
   Set-Location -LiteralPath D:\project\ai-shuttle-console
   New-Item -ItemType File -Path .\config.canary.local.20261026-01.json -Value (Get-Content -LiteralPath .\config.canary.example.json -Raw) -ErrorAction Stop
   ```

   Replace the two placeholder IDs, exact URL, any custom GPT ID, current CLI path, actual sessions directory/index, and optional expected title. Retain `canary_mode: true`, `integration_mode: "cli_queue"`, the disposable workspace, isolated profile, explicit loopback CDP, manual approval, and `127.0.0.1:8765`. These local configurations and browser profile are gitignored. No real IDs have been invented or inserted by this repair.
4. Run the read-only preflight first:

   ```powershell
   .\.venv\Scripts\python.exe run.py --canary --config .\config.canary.local.20261026-01.json --check
   ```

   Require exit code 0 and `status: verified`, with the expected actual thread, workspace, browser URL, and session file. This command reads metadata/DOM and CLI help; it performs no transfer and starts no server. Until it passes, stop.

## Subsequent single-cycle procedure

For canary readiness and completion, use the dedicated canary endpoints below. The legacy general reconciliation panel retains Phase 2 keyword correlation and may label a harmless canary prompt as a mismatch.

The operations in this section are for the operator's separately authorized live phase. They were **not executed** during this repair. Use the existing dashboard or make one explicit API request at each step. Do not automate this sequence into a loop.

1. Launch with the exact explicit command above. Open `http://127.0.0.1:8765`. `GET /api/canary/preflight` repeats the read-only checks.
2. Prepare one request: `POST /api/transfer/codex_report/prepare` with `{"is_canary":true,"iteration_id":1}`. Do not supply a thread/CWD/title/report override. The server generates a fresh 32-character hex nonce, a harmless request, its SHA-256, an expected prompt, a transfer ID, and a single-use confirmation token. Review and save the full response locally. A second cycle in the same state DB is rejected.
3. Review the complete request and exact ChatGPT destination. Only then submit `POST /api/transfer/chatgpt/send` with that transfer ID and token. Canary mode requires the real configured browser and rejects injected DOM/page overrides or mock fallback. Delivery means a new exact user message was observed; it does not mean ChatGPT answered.
4. After ChatGPT finishes, call `POST /api/transfer/chatgpt/observe` with `{}`. If pending or rejected, inspect manually; read-only observation can be repeated. It requires a completed assistant turn after the exact current nonce-bearing request and exactly one marked code block containing:

   ```text
   # Canary Ack
   Return exactly: CANARY-OK:<the-generated-32-character-hex-nonce>
   Do not inspect or modify files, execute commands, or perform any other action.
   ```

   This short contract is enabled only with the current expected nonce. Production extraction thresholds/markers are unchanged. Ordinary discussion, another nonce, an unmarked prompt, multiple blocks, historical answers, and production instructions are rejected. Nothing is loaded from a production prompt file as a fallback.
5. Prepare the extracted prompt: `POST /api/transfer/codex_prompt/prepare` with `{"source_extraction_id":<the-returned-extraction-id>,"iteration_id":1}`. Omit `prompt_text`; do not mark it operator-modified. Require the exact expected prompt and a second, independently generated authorization token. There is only one Codex task preparation per canary state, including after uncertain submission.
6. Review the exact real test thread UUID, disposable workspace, prompt, nonce, and SHA-256. Only then submit `POST /api/transfer/codex_queue/submit` with its transfer ID and confirmation token; omit destination overrides. The adapter rechecks the verified thread/header/index/CWD and uses an argument list, never a shell command.
7. An accepted queue receipt returns `queue_receipt_status: accepted` and `task_completion_status: pending`. Transfer status `delivered` describes transport receipt. It is **not task completion**. No synthetic queue receipt is generated when CLI output is empty or unrecognized. Correlated JSON receipts must identify the exact thread and content hash; the actual installed CLI's live output format has not been exercised. A different format safely becomes unknown and requires manual reconciliation.
8. Independently inspect completion using `GET /api/canary/completion/<Codex-transfer-id>`. Completion requires valid target metadata, events after the persisted pre-submission line boundary, the exact current prompt/user turn, an explicit completed final answer or matching `task_complete`, matching user/assistant turn IDs, and exactly `CANARY-OK:<nonce>`. Queue acceptance, EOF alone, another thread/task/nonce, or an older answer cannot satisfy this check. Completed evidence is persisted separately inside `delivery_evidence.completion`, and `verified_at` is set only at this step. The model/API assumptions must match the actual disposable session's recorded event format; unsupported or incomplete metadata remains unverified.
9. Stop after inspecting the one acknowledgement. Retain the two transfer records, tokens' consumed status, nonce/hash, queue receipt, completion evidence, and logs. No next iteration is started automatically.

## Ambiguity and recovery

A timeout/disconnect after possible queue submission, nonzero CLI result without proof of rejection, empty/unrecognized output, or mismatched receipt becomes `DELIVERY_OUTCOME_UNKNOWN`. No automatic retry occurs. The transfer's status and consumed authorization persist across restart; another submit or authorization preparation is blocked.

Stop and inspect the exact thread/session and browser manually. Do not resend, switch destinations, reset the stage, create another token, rename the configuration, or delete state to escape ambiguity. If the operator can prove delivery, the existing `/api/transfer/reconcile_unknown` operation can record `mark_delivered` with concrete notes; it preserves the original boundary and leaves task completion independent. If delivery cannot be proved, retain unknown status. Cancelling requires an explicit operator decision and does not establish completion.

## Verification record

| Check | Result | Exit code |
| --- | --- | --- |
| Original suite with documented `asyncio_mode=auto` | 65 passed (432.41 s) | 0 |
| Final targeted preflight regressions | 58 passed (425.33 s) | 0 |
| Final complete suite, default pytest configuration | 123 passed, 0 failures/errors/skips (867.24 s) | 0 |
| Example configuration with `--canary --config ... --check` | Placeholder rejected before transfer | 1 (expected rejection) |
| Python compilation, dashboard JavaScript syntax, `git diff --check` | Passed | 0 |

The initial default pytest run had 55 passes and 10 async fixture errors; `pytest.ini` now selects auto mode. The targeted red run had 13 failures and 3 passes before repairs. A later fixture iteration had 45 passes and 3 failures caused by missing completion controls and skipped state transitions in the offline fixtures; these were corrected before final verification. Obsolete test processes were stopped before the final sequential runs.

See `phase2_4_1_test_results.json` for exact commands, JUnit counts, changed-file scope, and full Git status. Existing Phase 1–2.4 uncommitted work is retained. Read-only production checks found 71 existing changed/untracked files whose content hashes remained unchanged; an additional production status entry appeared during the audit and is recorded separately. This repair performed no writes in the production repository.

The historical Phase 2.4 fixtures use synthetic identifiers only with injected transports; they are not evidence of real target verification or a live canary success. Real target verification and the live transfer remain pending the manual prerequisites above.

