# Phase 2.7 Implementation Report

Status: `PHASE_2_7_SHADOW_IMPLEMENTATION_READY`.

Production Shadow provides a separate operator UI, an explicit private configuration, exact target guards, a read-only baseline, zero-rewrite C2G preview, bound manual authorization, one durable send attempt, correlated planner observation, and an unauthorized G2C preparation. The flow ends before Codex execution.

## Changed files

- Configuration/privacy: `.gitignore`, `app/models.py`, `app/canary.py`.
- Shadow coordinator and API/UI: `app/production_shadow.py`, `app/main.py`, `app/templates/index.html`.
- Execution and delivery guards: `app/adapters.py`, `app/auto_mode.py`, `app/canonical.py`, `app/codex_app_server.py`.
- Synthetic tests and existing regression fixtures: `tests/test_phase2_7_production_shadow.py`, `tests/test_phase2_6_1_audit_integrity.py`, `tests/test_phase2_4_4_isolation_audit.py`, `tests/test_phase2_observation_and_reconciliation.py`, `tests/test_shuttle_console.py`.
- Public documentation: this report, `docs/phase2_7_production_shadow.md`, `docs/phase2_real_observation_report.md`, `docs/phase2_test_results.json`.

Historical public fixtures/reports contained production labels. Their current versions now use synthetic titles and branches. Original copies stay only in ignored private evidence. The correlation regression continues to test stale-completion rejection using synthetic state while asserting that production Auto arming is forbidden.

## Mode guards

Shadow and Canary are mutually exclusive. Shadow rejects G2C queue permission, Auto permission, resume side effects, and removal of manual approval. Configuration and authorization fingerprints bind the complete validated config. The operator confirms the exact private target identities before baseline/preparation.

`PRODUCTION_AUTO_MODE_NOT_AUTHORIZED` rejects Auto/lifecycle execution. `PRODUCTION_G2C_QUEUE_FORBIDDEN` rejects queue submission. Legacy transfer routes cannot bypass the dedicated Shadow APIs. Queue and resume adapters also enforce Shadow restrictions. Auto arming/starting is restricted to Canary configurations.

Payload equality and raw SHA-256 are checked before authorization and again before sending. The browser checks exact raw composer text before one normal click and uses existing semantic confirmation for one new user turn. A fixed private ledger consumes the sole attempt atomically before browser I/O. An independent send-attempt column prevents delayed writes from another worker from reopening the budget. Ambiguous delivery never retries. No G2C authorization or Auto child authorization is created by Shadow. Prepared G2C evidence persists safely across restart.

## Private configuration and read-only readiness

Private configuration: `config.production.shadow.local.json`. Shared protection registry: `config.protected-targets.local.json`. Both configurations and `runtime/private-reports/phase2_7/` are ignored and excluded from the Git index.

The local CLI, session directory/index, and CDP/profile parameters were discovered from the existing local environment. Exact production thread, canonical cwd, title/index metadata, and a unique rollout matched. The actual repository branch differs from the configured expected branch. CDP is connected, but the exact production conversation is not open; authentication and composer readiness for that page remain unverified.

The repository has a preexisting dirty state. Its branch, HEAD, and status matched the initial read-only implementation snapshot on recheck. Shuttle issued no production queue, resume, task, browser message, or repository mutation.

Actual identities, paths, payloads, and raw readiness evidence remain in local private files. Public-tree and staged-addition scans showed zero configured private-value matches; no private config or evidence file is tracked.

## Validation

Full regression: **397 passed, 1 skipped, 3 warnings**, including **52 Production Shadow tests**. The warnings are in existing Windows transport/mock tests. All previous test files ran; no live Canary was repeated. Command: `python -m pytest -p pytest_asyncio.plugin -vv -o faulthandler_timeout=45 --durations=8` with plugin autoload disabled and the asyncio plugin explicitly loaded.

The synthetic browser UI check passed with zero page-script errors, visible prepared status, and sending disabled without its separate authorization token. Python compilation and Git whitespace checks passed. Nineteen generic source/test/documentation files changed.

## Live Shadow Report

Not run. No live acceptance status is claimed, and Phase 2.7.1 has not begun.

Before live acceptance, the operator must resolve the branch discrepancy, review the recorded preexisting dirty state, open exactly one configured production conversation in the configured authenticated CDP browser, and ensure Codex is idle/completed. Separate live authorization then permits a fresh baseline and exact C2G preview; a further explicit bound operator action authorizes one real C2G send. Observe the new planner response, extract strictly, prepare G2C unchanged if actionable, and stop without queueing.

See [the implementation and operator contract](phase2_7_production_shadow.md) for endpoint details. Commit SHA and publication branch are reported with the final delivery.
