# Phase 2.4.6 — Canary runtime synchronization and send gates

Final status: **CANARY_PRE_SEND_BLOCKED**

Verified on 2026-10-26 (Asia/Shanghai). Source repair and regression verification are complete. The existing server, PID **10284**, has not loaded the repaired backend: automatic approval rejected the attempt to stop that process and restart it with the same `-07` configuration (`blocked by policy`, no further reason supplied). No alternate process-control mechanism was used.

The repaired application was additionally verified against the real Canary browser and Codex session using a **disposable copy of the active database**. That instance reaches `CANARY_READY_FOR_C2G_PREPARE`. This is not claimed as deployment or successful verification of the original running backend.

## Effective configuration: independent evidence

| Item | Actual running server, before and after | Repaired application verification |
| --- | --- | --- |
| Mode | CANARY | CANARY |
| Selected file | `D:\project\ai-shuttle-console\config.canary.local.20261026-07.json` | Same file; no switch to `-08` |
| Config fingerprint | `2323bd9a9c15effb666601bd5126cd283f05ae941641b52794821856b31dd794` | Same fingerprint in bootstrap, guard and runtime |
| Database | `D:\project\ai-shuttle-console\runtime\explicit-config\8d3e7111864373f0\state.db` | `D:\project\ai-shuttle-console\runtime\phase246-readonly\runtime\explicit-config\8d3e7111864373f0\state.db` |
| Stage | `REPORT_PREVIEW`, transfer status `prepared` | `CODEX_WAITING`, transfer status `idle` after audited repair |
| UI readiness | `RUNTIME_RESTART_REQUIRED` on a fresh page load | `CANARY_READY_FOR_C2G_PREPARE` |
| C2G / G2C buttons | Both disabled | Both disabled: no active prepared transfer or valid authorization |

The source trace is `run.py` arguments → `SHUTTLE_CONFIG` / `SHUTTLE_CANARY` → `lifespan()` loading the explicit configuration → SHA-256 of its resolved filename selecting the isolated DB → persisted config and `state_machine.config` → adapters and transfer endpoints → `/api/transfer/target_info` → frontend rendering. The active API returned the `-07` source and its fingerprint, matching the configuration in that isolated database. Reading the original process's OS command line was unavailable; the determination does not rely on the bootstrap card or an assumed command line.

The new startup identity exposes the source, fingerprint, database path, process ID and Canary startup flag. Authorization gates compare persisted configuration, startup configuration, selected file and verified bootstrap configuration. Config drift fails closed; no runtime configuration is silently replaced.

`-08` has fingerprint `e965956ea91da48051d62274307bcad0c6551547435fbe30928a66a4ee7781b1`. Its `codex.sessions_dir` is `C:/Users/<USER>/.codex/sessions`; `-07` uses `D:/DevCache/codex_sessions`. Thus these two files are not identical effective configurations.

## Root causes and repairs

1. **Configuration drift:** bootstrap Recheck called configuration generation unconditionally, independent of the server's selected file. Discovery fell back to the default sessions path, explaining the `-07` / `-08` difference. Generation now prefers the explicitly active file, reuses a full matching configuration, validates before writing, and refuses overwrite. Read-only status cannot create a config. Different immutable settings produce `RUNTIME_RESTART_REQUIRED` and cannot authorize transfers under another configuration. Repeated live read-only checks left the eight existing local config files unchanged; no `-09` was created.

2. **Discovered browser versus disconnected UI:** bootstrap discovered a CDP target and ran preflight independently. The Canary `/api/chatgpt/observe` path instead required an already delivered request before observing a reply. Before sending, that path could not provide the `browser_status` expected by the UI; the UI rendered missing fields as false. Browser readiness is now probed independently of reply extraction, using the exact configured live target and composer parser. A discovered URL cannot substitute for a live browser check. Preflight and runtime readiness share the same browser-status method.

3. **Preview versus missing frontend transfer:** the active database actually contains `transfer-c2g-a658fded3465`, with nonce, payload, hash, destination, fingerprint and an authorization token. The apparent absence was caused by JavaScript-only active-transfer variables that were never hydrated after reload. The record also has `source_message_id=null`, violating the stronger invariant requested in this phase. The repair preserves that record as cancelled, returns to `CODEX_WAITING`, clears stale checkpoint payload/failure fields and records `TRANSFER_STATE_REPAIRED`. It never manufactures a missing transfer or backfills a fabricated source identity. This repair was observed on the disposable database copy; the original active record is still prepared until the repaired backend starts.

4. **Authorization and button state:** the HTML had neither default `disabled` attributes nor readiness-derived gating. The buttons now start disabled, are visibly greyed out, and are enabled only from the unified backend readiness result plus restored ID/token/hash. Failure, offline state, wrong fingerprints, missing source/payload/destination, expired or consumed authorization, mismatched G2C extraction and unresolved delivery all fail closed. Send and queue endpoints repeat the validation before mutating a transfer or invoking an adapter. Operations are serialized; manual forced transitions cannot create an orphan preview. Canary adapters cannot fall back to mocks, and request-supplied live observation/send overrides are rejected.

5. **Stale reconciliation:** independent frontend timers fetched bootstrap, checkpoint, guard and reconciliation at different times. `/api/runtime/refresh` now returns one observation snapshot for those panels, including restored persisted transfers and the two authorization gates. Recheck returns the same snapshot structure. Browser readiness is separate from whether a fresh, nonce-correlated reply exists. A completed or already-used cycle is not advertised as ready for another preparation. Unknown delivery is checked across all records rather than a truncated history window.

## Verification

- Final full suite: **193 passed in 30.89 seconds**, no failures or skips.
- Focused Phase 2.4.6 runtime/DOM suite: **22 passed** (included in the final full suite).
- Real DOM tests use an isolated headless Edge instance with every page request intercepted. They verify rehydrated transfer ID/hash/nonce, default and dynamic disabled states, missing token/hash/ID, restart-required gating, disabled cursor styling and no JavaScript errors. They do not click send controls.
- Backend tests cover mismatched bootstrap/runtime configs, existing-config reuse, immutable changes, disconnected CDP despite discovery, successful read-only readiness, orphan previews and orphan prepared status, duplicate prepared records, forced-transition rejection, expired authorization, unresolved delivery, G2C direction/extraction binding and production-target rejection.
- The suite explicitly forbids real browser delivery and real `codex queue` subprocess submission. Existing adapter tests use injected page data, fake senders or injected queue runners. CLI `--help` capability checks remain read-only.
- Disposable SQLite tests retain file-backed persistence while disabling expensive filesystem synchronization only for databases under each test's temporary directory. Runtime database durability settings are unchanged.
- `git diff --check` passed. Git emitted existing LF/CRLF conversion notices, not whitespace errors.

Final command:

```powershell
.\.venv\Scripts\python.exe -m pytest -q --disable-warnings --tb=short --basetemp=runtime\phase246-complete --junitxml=runtime\phase246-tests.xml
```

The system Python initially lacked `aiosqlite` / `pytest-asyncio`; all reported successful runs use the repository `.venv`.

## Live read-only evidence

The verification harness opens the original SQLite database read-only for backup, runs the repaired lifespan against a separate copy, probes real targets, performs two read-only refreshes, and renders both the repaired snapshot and the actual running console. All send/queue methods are trapped in the verification process.

- Codex target: `01a174f9-b5d8-7c91-9a27-d8cd3989575b`; workspace `D:\project\ai-shuttle-console\canary_workspace`; session/header/index validation succeeds.
- ChatGPT target: `https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581`; CDP `http://127.0.0.1:9223`; actual read-only probe reports **connected=true, ready=true, blocked=false**.
- Repaired bootstrap selects `-07` and reports `CANARY_PREFLIGHT_VERIFIED`; its fingerprint equals the effective runtime and transfer guard fingerprint.
- Repaired snapshot has no unresolved delivery, no active prepared transfer, both action buttons disabled and no JavaScript errors.
- Original live transfer ID, status, hash, fingerprint and `sent_at=null` are unchanged before/after verification.
- **Real ChatGPT messages sent: 0. Real Codex queue submissions: 0.** No files in the protected `D:\workspace\production-repo` project were edited or reset.

Local artifacts:

- `runtime/phase246-tests.xml` — final full-suite JUnit results.
- `runtime/phase246-readonly/evidence.json` — complete configuration/observation/state/audit evidence, with token values excluded from transfer summaries.
- `runtime/phase246-readonly/active-ui.png` — actual running console showing `RUNTIME_RESTART_REQUIRED`.
- `runtime/phase246-readonly/repaired-ui.png` — repaired snapshot showing `CANARY_READY_FOR_C2G_PREPARE`.
- `scripts/verify_phase246_readonly.py` — reproducible read-only harness; no server restart and no submission.

## Git and remaining action

The working tree was already dirty on entry and remains uncommitted. Existing work was preserved; no commit, reset, clean or production-tree operation was performed. This phase changes `app/main.py`, `app/adapters.py`, `app/canary_bootstrap.py`, `app/state_machine.py`, `app/templates/index.html`, adds `app/runtime_state.py`, adds Phase 2.4.6 runtime/UI tests and the read-only harness, and updates earlier test fixtures for the stricter gates. Local config, DB and screenshot artifacts remain gitignored.

The remaining action is to restart the **original console through its owning shell/supervisor**, explicitly retaining `config.canary.local.20261026-07.json`, then run Read-Only Recheck. The startup repair will operate on the original isolated database at that time. Example launch from `D:\project\ai-shuttle-console`, after the existing listener has been stopped by its owner:

```powershell
.\.venv\Scripts\python.exe run.py --canary --config D:\project\ai-shuttle-console\config.canary.local.20261026-07.json
```

This report does not authorize a live transfer. Final status remains **CANARY_PRE_SEND_BLOCKED** because the active backend restart was rejected by automatic approval.

