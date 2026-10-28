# Phase 2.7 — Production Shadow implementation

Status: transactional Capture-to-C2G implemented. Live transactional execution has not run. See [the transaction contract](phase2_7_transactional_c2g.md) for authorization, recovery, and validation.

The planner remains the configured ChatGPT conversation. The Shuttle transports the exact pinned Codex final response, without an envelope, nonce, summary, or generated instruction. A returned prompt is extracted using the existing strict planner classifier and is prepared for review without execution.

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

The private config supplies the exact Codex thread, title, canonical workspace, expected branch, session directory/index, rollout path, CLI, browser profile, loopback CDP endpoint, exact ChatGPT URL/conversation/project IDs, `production_source` artifact pin, and `protected_targets`. The configuration fingerprint is computed from the complete validated configuration. Contradictory controls fail closed.

`config.protected-targets.local.json` is a separate ignored shared registry with `thread_id`, `conversation_id`, and `workspace_path`. Canary guards read it even when their own configuration has no production identities. A malformed or incomplete registry fails closed. No production identities belong in source, fixtures, examples, or this document.

## Phase 2.7.0 — Immutable Production Source Artifact Pinning

Phase 2.7.0 replaces dynamic "latest final answer in source thread" resolution with immutable source artifact pinning:

1. **Immutable Artifact Schema (`ProductionSourceArtifact`)**:
   Pinned sources are bound to an explicit immutable tuple:
   - `thread_id`: synthetic source thread UUID
   - `user_turn_id`: synthetic source user turn UUID
   - `user_message_id`: synthetic source user message ID
   - `assistant_message_id`: pinned assistant final message ID
   - `completed_at`: task completion ISO timestamp
   - `rollout_line`: line/index of assistant final answer
   - `sha256`: raw text SHA-256
   - `char_count`: character count
   - `expected_marker`: terminal marker (`FRESH_PRODUCTION_HANDOFF_READY`)

   Example synthetic configuration:
   ```json
   {
     "production_source": {
       "thread_id": "11111111-1111-4111-8111-111111111111",
       "user_turn_id": "33333333-3333-4333-8333-333333333333",
       "user_message_id": "msg_user_synthetic",
       "assistant_message_id": "msg_assistant_synthetic",
       "completed_at": "2026-01-01T00:00:00Z",
       "rollout_line": 203,
       "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
       "char_count": 13578,
       "expected_marker": "FRESH_PRODUCTION_HANDOFF_READY"
     }
   }
   ```

2. **Exact Artifact Retrieval (`observe_pinned_source_artifact`)**:
   Scans session records and verifies that the exact assistant message ID exists, represents a true `final_answer` phase, has associated task-complete evidence, matches the pinned SHA-256 and char count, and ends with the expected marker. It does not return the latest final simply because newer turns exist.

3. **Decoupled Artifact Integrity vs. Thread Runtime State**:
   `PINNED_SOURCE_ARTIFACT_VERIFIED` is decoupled from thread runtime state (`idle`, `active`, `completed_newer_turns`). Subsequent completed administrative/precheck turns in the source thread do not invalidate the approved pinned artifact.

4. **Prevention of Auto-Rebinding & Audit Logging**:
   Discovery of newer finals in the thread records an audit event (`NEWER_SOURCE_FINAL_OBSERVED_NOT_SELECTED`) and never overwrites `production_source`. Rebinding requires explicit operator action via `POST /api/production-shadow/rebind-source`.

5. **Repository Freshness Gate**:
   Even with immutable artifact pinning, if the repository changes post-handoff, the handoff is considered stale and emits `PINNED_SOURCE_STALE_AFTER_REPOSITORY_CHANGE`.

6. **UI Boundary Warning**:
   A prominent banner is displayed in the operator console:
   > Production source threads are evidence sources, not Shuttle control threads. Shuttle implementation/precheck instructions should be executed from the `ai-shuttle-console` development/control context, not inside the selected production source thread.

## Server restrictions

- Shadow rejects legacy transfer and configuration mutations. Production observation must use the dedicated Shadow flow.
- Auto arm/authorize/start/resume, child authorization, iteration execution, Codex queue submission, and thread resume are rejected. These restrictions exist in server routing and execution components.
- Controlled Auto authorization remains available for Canary configurations. Shadow never creates an Auto child authorization or a G2C token.
- The coordinator receives no Codex queue. Its lifecycle capability only creates a new read-only handoff with the fixed protocol; generic development starts and resumes are denied.
- Production Git operations are limited to status, diff, ls-files, branch inspection, and rev-parse with optional index locking disabled. No checkout, reset, stash, clean, commit, or branch switch runs against production.

## Private API and operator sequence

1. `GET /api/production-shadow/status` displays private exact targets, configuration fingerprint, and the deterministic parent binding.
2. `POST /api/production-shadow/authorize-atomic-parent` requires exact binding and explicit operator confirmation. The expiring one-use grant binds the controlled capture protocol, with no pre-existing payload hash.
3. `POST /api/production-shadow/atomic-capture-send` validates the parent, checks readiness, captures A, generates one new read-only handoff, checks B, verifies its immutable pin, and checks final Snapshot C. Only then does it atomically derive/consume a C2G-only child and the fixed durable send budget. The sender verifies composer contents and clicks once. There is no automatic retry.
4. The same operation observes the new planner response read-only. If completion takes longer than the observation window, `POST /api/production-shadow/observe` permits read-only reconciliation after confirmed delivery. It cannot resend.
5. Strict extraction prepares an identical G2C message with provenance and zero rewrites, without executable authorization. Stop/no-action classifications finish without invented work; ambiguity and errors block.
6. Repository postchecks record external changes without attributing them to Shuttle. Old baseline/prepare/authorize/send/rebind endpoints are blocked in Production Shadow. Dedicated capture cannot produce a payload that an old manual route can send.

The persistent private record contains source and destination turn/message IDs, timestamps, raw/semantic hashes, planner classification, exact payloads, transfer IDs, fingerprint, and pre/post repository evidence. Restart only reads these records. An attempted send cannot resume automatically; a prepared G2C remains unauthorized.

## Evidence and acceptance

All real configuration, browser discovery, target verification, raw prompts, and execution evidence stay under ignored local files or `runtime/private-reports/phase2_7/`. Public reports use an allowlist of structural statuses and metrics. Tests use synthetic identities and prohibit real browser delivery and Codex submission.

Implementation readiness does not imply live acceptance. Current private readiness and any branch/browser prerequisites are recorded in `runtime/private-reports/phase2_7/configuration-readiness.json` and `runtime/private-reports/phase2_7/fresh-handoff-binding/precheck.json`.

Live acceptance requires separate operator parent authorization, the intended branch already active, and an authenticated browser open on exactly one configured production conversation with an empty genuine composer. Use **Atomic Capture & Send Once** after confirming the exact targets. The transaction produces and verifies its own fresh pin before the final freshness gate. Phase 2.7.1 and execution of any returned prompt require separate review and authorization.
