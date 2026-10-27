# Phase 2.7 — Production Shadow implementation

Status: `PHASE_2_7_SHADOW_IMPLEMENTATION_READY`. Regression: **397 passed, 1 skipped**, including **52 Shadow tests**. Live acceptance has not run. See [the implementation report](phase2_7_implementation_report.md).

The planner remains the configured ChatGPT conversation. The Shuttle transports the exact latest completed Codex final response, without an envelope, nonce, summary, or generated instruction. A returned prompt is extracted using the existing strict planner classifier and is prepared for review without execution.

## Mode and configuration

Start the local server with `python run.py --config config.production.shadow.local.json`. The file must be ignored by Git. Production Shadow requires an explicit private configuration; database/default fallback is rejected. It uses its own state DB and a fixed private one-send ledger at `runtime/private-reports/phase2_7/shadow.db`.

Required controls:

```json
{
  "canary_mode": false,
  "production_shadow_mode": true,
  "allow_real_c2g_send": true,
  "allow_real_g2c_queue": false,
  "allow_auto_run": false,
  "allow_thread_resume_side_effect": false
}
```

The private config supplies the exact Codex thread, title, canonical workspace, expected branch, session directory/index, rollout path, CLI, browser profile, loopback CDP endpoint, exact ChatGPT URL/conversation/project IDs, and `protected_targets`. The configuration fingerprint is computed from the complete validated configuration. Contradictory controls fail closed.

`config.protected-targets.local.json` is a separate ignored shared registry with `thread_id`, `conversation_id`, and `workspace_path`. Canary guards read it even when their own configuration has no production identities. A malformed or incomplete registry fails closed. No production identities belong in source, fixtures, examples, or this document.

## Server restrictions

- Shadow rejects legacy transfer and configuration mutations. Production observation must use the dedicated Shadow flow.
- Auto arm/authorize/start/resume, child authorization, iteration execution, Codex queue submission, and thread resume are rejected. These restrictions exist in server routing and execution components.
- Controlled Auto authorization remains available for Canary configurations. Shadow never creates an Auto child authorization or a G2C token.
- The coordinator receives no Codex queue or lifecycle adapter.
- Production Git operations are limited to status, diff, ls-files, branch inspection, and rev-parse with optional index locking disabled. No checkout, reset, stash, clean, commit, or branch switch runs against production.

## Private API and operator sequence

1. `GET /api/production-shadow/status` displays exact private target identities and the configuration fingerprint in the separate Production Shadow UI.
2. `POST /api/production-shadow/baseline` requires target confirmation and the displayed fingerprint. It verifies the exact registry pair, rollout/header/index/title/cwd, branch, completed source turn/message, and genuine browser composer on the exact authenticated page. Running/incomplete Codex turns fail with `PRODUCTION_CODEX_NOT_IDLE`. The repository baseline includes preexisting dirty paths and dirty-content hashes.
3. `POST /api/production-shadow/prepare` creates `PRODUCTION C2G PREPARED — NOT SENT`. Preview evidence includes source IDs/time, exact payload/hash/character count, first/last preview, destination, config fingerprint, repository HEAD, and `Payload rewrite: NONE`. Source, repository, and ChatGPT baselines must remain unchanged.
4. `POST /api/production-shadow/authorize` requires a human confirmation and an exact binding to transfer ID, destination, payload hash, source message/turn IDs, fingerprint, and Shadow mode. It returns an expiring one-use token. It does not send.
5. `POST /api/production-shadow/send` is a separate operator action. It rechecks all baselines and integrity. A durable SQLite compare-and-swap consumes the sole send attempt before browser I/O. The live sender checks raw composer equality and SHA-256 before one click, then verifies one new semantic user delivery. Unknown delivery is terminal and never resent. Config filename changes and service restarts cannot reset the ledger.
6. `POST /api/production-shadow/observe` reads a completed new assistant response after the delivered user turn, rejecting stale message IDs, streaming, intervening users, and missing completion evidence. Classification is `NEXT_CODEX_PROMPT_FOUND`, `PLANNER_STOP_REQUESTED`, `PLANNER_NO_ACTIONABLE_PROMPT`, `PLANNER_RESPONSE_AMBIGUOUS`, or `PLANNER_RESPONSE_ERROR`.
7. A single extracted prompt is stored with exact provenance/hash/character count and an identical prepared G2C message. Its state is `PRODUCTION_G2C_PREPARED_NOT_AUTHORIZED`, with `g2c_rewrite_count = 0`. The run stops at `PRODUCTION_G2C_PREPARED_FOR_REVIEW`. No queue or execution endpoint exists in this flow.
8. Repository postchecks compare branch, HEAD, status, and dirty bytes. New changes are classified `PRODUCTION_REPOSITORY_CHANGED_DURING_SHADOW` for review, without attributing them to Shuttle.

The persistent private record contains source and destination turn/message IDs, timestamps, raw/semantic hashes, planner classification, exact payloads, transfer IDs, fingerprint, and pre/post repository evidence. Restart only reads these records. An attempted send cannot resume automatically; a prepared G2C remains unauthorized.

## Evidence and acceptance

All real configuration, browser discovery, target verification, raw prompts, and execution evidence stay under ignored local files or `runtime/private-reports/phase2_7/`. Public reports use an allowlist of structural statuses and metrics. Tests use synthetic identities and prohibit real browser delivery and Codex submission.

Implementation readiness does not imply live acceptance. Current private readiness and any branch/browser prerequisites are recorded in `runtime/private-reports/phase2_7/configuration-readiness.json`.

Live acceptance requires separate operator authorization, the intended branch already active, an authenticated browser open on exactly one configured production conversation, an idle/completed Codex turn, a fresh read-only baseline, and review of the exact prepared C2G payload. The operator then authorizes one C2G and sends it once. Observe the planner, prepare G2C if actionable, and stop. Phase 2.7.1 requires separate review and authorization and is outside this implementation.
