# Phase 2.7 Implementation Report

Status: `PHASE_2_7_PINNED_SOURCE_READY`.

Production Shadow provides a separate operator UI, an explicit private configuration, exact target guards, immutable source artifact pinning, a read-only baseline, zero-rewrite C2G preview, bound manual authorization, one durable send attempt, correlated planner observation, and an unauthorized G2C preparation. The flow ends before Codex execution.

## Phase 2.7.0 Additions

- **Immutable Source Model Added**: Added `ProductionSourceArtifact` schema to `app/models.py`, capturing `thread_id`, `user_turn_id`, `user_message_id`, `assistant_message_id`, `completed_at`, `rollout_line`, `sha256`, `char_count`, `rollout_path`, and `expected_marker`. Added `production_source: Optional[ProductionSourceArtifact]` to `AppConfig`.
- **Exact Artifact Retrieval Implemented**: Implemented `observe_pinned_source_artifact` in `app/production_shadow.py`. The function verifies exact assistant message ID, `final_answer` phase, task completion evidence, SHA-256 match, character count match, and terminal marker.
- **Latest-Final Dependency Removed**: Decoupled artifact validity (`PINNED_SOURCE_ARTIFACT_VERIFIED`) from thread runtime state (`idle`, `active`, `completed_newer_turns`). Subsequent administrative/precheck turns in the source thread do not invalidate the pinned artifact.
- **Source Auto-Rebinding Prevented**: Discovery of newer finals in the thread records an audit event (`NEWER_SOURCE_FINAL_OBSERVED_NOT_SELECTED`) without modifying the pinned source. Added explicit operator rebinding route `POST /api/production-shadow/rebind-source` and method `rebind_source(...)`.
- **Repository Freshness Gate**: Enforces that substantive repository mutations post-handoff emit `PINNED_SOURCE_STALE_AFTER_REPOSITORY_CHANGE` and block preparation/sending.
- **UI Boundary Banner**: Added prominent operator notice in `app/templates/index.html`: "Production source threads are evidence sources, not Shuttle control threads."

## Changed files

- Configuration and models: `app/models.py`.
- Shadow coordinator and API/UI: `app/production_shadow.py`, `app/main.py`, `app/templates/index.html`.
- Synthetic tests and privacy invariants: `tests/test_phase2_7_production_shadow.py`.
- Public documentation: `docs/phase2_7_production_shadow.md`, `docs/phase2_7_implementation_report.md`.

## Mode guards

Shadow and Canary are mutually exclusive. Shadow rejects G2C queue permission, Auto permission, resume side effects, and removal of manual approval. Configuration and authorization fingerprints bind the complete validated config. The operator confirms the exact private target identities before baseline/preparation.

`PRODUCTION_AUTO_MODE_NOT_AUTHORIZED` rejects Auto/lifecycle execution. `PRODUCTION_G2C_QUEUE_FORBIDDEN` rejects queue submission. Legacy transfer routes cannot bypass the dedicated Shadow APIs. Queue and resume adapters also enforce Shadow restrictions. Auto arming/starting is restricted to Canary configurations.

Payload equality and raw SHA-256 are checked before authorization and again before sending. The browser checks exact raw composer text before one normal click and uses existing semantic confirmation for one new user turn. A fixed private ledger consumes the sole attempt atomically before browser I/O. An independent send-attempt column prevents delayed writes from another worker from reopening the budget. Ambiguous delivery never retries. No G2C authorization or Auto child authorization is created by Shadow. Prepared G2C evidence persists safely across restart.

## Private configuration and read-only readiness

Private configuration: `config.production.shadow.local.json`. Shared protection registry: `config.protected-targets.local.json`. Both configurations and `runtime/private-reports/phase2_7/` are ignored and excluded from the Git index.

The approved fresh handoff artifact was migrated from existing private evidence into the `production_source` structure in `config.production.shadow.local.json` without regeneration.

A read-only precheck was executed from the `ai-shuttle-console` development/control context:
1. **Pinned Source Verification**: Exact assistant message ID, task completion, character count, and SHA-256 were verified (`PINNED_SOURCE_ARTIFACT_VERIFIED`).
2. **Repository Freshness**: The repository freshness check identified newer substantive files created after the handoff timestamp (~22:43 UTC). Accurately evaluated and reported as `FRESH_HANDOFF_ALREADY_STALE`.
3. **Target Readiness**: Passive CDP probe confirmed exact target count = 1, composer found and ready, non-streaming, no login/challenge forms (`PRODUCTION_CHATGPT_TARGET_READY`).
4. **C2G Preview**: Prepared C2G payload equality was verified against raw final text with zero rewriting (`PRODUCTION_C2G_PREVIEW_VERIFIED`).
5. **Precheck Gate**: Because the repository has post-handoff modifications, the overall live shadow precheck status is safely blocked (`PHASE_2_7_LIVE_SHADOW_PRECHECK_BLOCKED`), preventing any stale handoff from being sent.
6. **Zero Mutations**: All mutation counts remained strictly zero (production ChatGPT sends: 0, production G2C queues: 0, production thread resumes: 0, production Codex tasks started: 0, production Git writes: 0).

## Validation

- **Targeted Tests**: 67 passed in `tests/test_phase2_7_production_shadow.py` (including all 15 new Phase 2.7.0 unit/integration tests).
- **Public Repository Sanitization**: `test_real_ids_stay_out_of_public_repository` verified that zero private IDs, real message IDs, real hashes, or private workspace paths exist in any tracked file.
- **Full Suite Regression**: 411 passed, 1 skipped.

## Live Shadow Report

Not run. No live C2G message was sent.