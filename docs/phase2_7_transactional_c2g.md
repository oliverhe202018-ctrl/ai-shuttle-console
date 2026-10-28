# Phase 2.7.0.3 — Transactional Atomic Capture-to-C2G

The production coordinator is `TransactionalProductionShadow`. One explicit operator parent grant authorizes the fixed read-only handoff protocol and at most one C2G delivery. This implementation task does not run the live transaction.

## API and UI

`GET /api/production-shadow/status` returns exact private targets and `parent_binding`. Confirm those targets, then issue `POST /api/production-shadow/authorize-atomic-parent` with that binding, `operator_confirmed: true`, and `ttl_seconds` (1–900, default 600). Submit the returned token once to `POST /api/production-shadow/atomic-capture-send`.

The **Atomic Capture & Send Once** control performs both calls under one operator confirmation. It displays C2G max sends = 1, G2C execution OFF, and Auto Mode OFF. It does not store the parent token across refreshes or retry a failed request. Intermediate capture evidence is private history, never a reusable manual-send authorization.

The parent binds canonical workspace, exact conversation URL/ID, complete configuration fingerprint, deterministic handoff prompt hash, implementation fingerprint, Shadow mode, operation `ATOMIC_CAPTURE_AND_SINGLE_C2G`, capture/send limits, expiration, and a random authorization ID/token. The implementation fingerprint covers application Python, templates, JavaScript, and CSS. There is no payload hash in the parent because the handoff does not yet exist.

## Guarded sequence

1. Acquire an OS advisory lock shared by all coordinator instances using the fixed ledger. Validate the parent and reject unresolved or consumed production sends. Check exact destination readiness, empty composer, branch policy, and production session lifecycle evidence. Unreadable/missing inventory fails closed; age never proves idleness.
2. Capture Snapshot A. The parent is atomically claimed at transaction entry, including when a precheck blocks; capture/send budgets remain separate. The parent can capture only once, even if capture fails or the process crashes.
3. The narrow lifecycle capability calls official `thread/start` immediately followed by `turn/start`. It accepts no prompt or existing thread ID. The thread and turn request read-only sandboxing and never request approvals. Generic development thread/turn starts and resume are denied in Shadow mode. Capture count is tracked separately from production development execution, which remains zero.
4. Wait for exactly one final answer and a later task-complete event for the observed new turn. Capture B, compare A/B, audit side effects, and only then verify/persist an immutable source artifact using exact rollout identity, source turn/message, completion time, hash, character count, and terminal marker. The operator configuration is never rebound.
5. Build the exact C2G payload from the verified pin: no prefix, suffix, wrapper, rewrite, or nonce. Recheck configuration/protocol TTL, pin integrity, and unchanged conversation baseline. Capture Snapshot C, including active sessions; compare B/C. MATERIAL or UNKNOWN changes or active production tasks block before child creation and before the send allowance is consumed.
6. Create a C2G-only child bound to parent/run, assistant message, payload hash/count, exact destination, config fingerprint, and C fingerprint. One SQLite transaction persists child consumption and compare-and-swaps the fixed durable send-attempt budget. No asynchronous work intervenes between the final gate and this commit. The child is never exposed as a general transfer, G2C, or Auto token.
7. The existing browser sender checks exact URL, idle/empty genuine composer, populates the exact text, verifies raw readback/hash, and clicks one normal send control. It never retries or falls back to a keyboard submission. Confirm exactly one new matching user turn against the conversation baseline and persist observed IDs and hashes. Unknown delivery is terminal.
8. Observe only the correlated new completed planner response. Intervening user turns block attribution. Existing strict extraction prepares exact G2C text with provenance and zero rewrites, or records stop/no-action/ambiguity/error. No G2C authorization, queue, development execution, resume, or Auto operation is available.

Dirty repositories are allowed. A/B and B/C use the existing conservative semantic delta classification: known telemetry may be stable; unexplained fingerprint changes fail closed. The final gate detects observed concurrent changes; it does not freeze external tools after Snapshot C. Repository postchecks retain any subsequently observed changes without attributing them to Shuttle.

## Consumption and recovery

| Failure point | Parent/capture | C2G budget | Restart behavior |
| --- | --- | --- | --- |
| Pre-capture readiness blocked | Parent consumed; capture not started; private precheck recorded | Unconsumed | New explicit operator parent required |
| A/B race, invalid final/pin, B/C change, or expired protocol | Parent consumed; one capture used | Unconsumed; no child | New operator parent/transaction only; old pin is historical |
| Crash before child | Parent cannot replay | Unconsumed | `ATOMIC_C2G_INTERRUPTED_NEW_PARENT_REQUIRED`; no continuation |
| Composer mismatch or known pre-click rejection after consumption | Parent and child consumed | Consumed | No resend, even if no click occurred |
| Crash after child consumption before confirmed delivery | Parent and child consumed | Consumed | `PRODUCTION_C2G_RECOVERY_REQUIRED`; reconciliation only |
| Confirmed delivery before planner completion | Parent and child consumed | Consumed | Read-only `/observe` permitted; no resend |

The fixed private SQLite ledger and OS lock survive service/config filename changes. Per-run history and precheck outcomes stay in ignored private storage. Restart recovery only classifies interrupted work; it never starts a thread, derives a child, populates a composer, or sends. A planner response still incomplete after the bounded observation window records `PRODUCTION_PLANNER_OBSERVATION_PENDING`; later observation remains read-only.

## Verification and publication

Focused synthetic tests cover parent/protocol integrity, arbitrary prompt rejection, readiness/session gates, A/B and B/C races, semantic stability, pin timing, child binding, durable pre-I/O consumption, concurrency, exact G2C preparation, interleaving, ambiguous delivery, restart boundaries, API bypass prevention, sandbox parameters, and private-data exclusion. The test suite forbids live sender/queue calls. CLI-generated local schema was inspected for read-only sandbox request compatibility without starting a production thread.

Publish only generic code, synthetic tests, and documentation. Live IDs, exact prompts/responses, real hashes, and private paths remain ignored. Implementation readiness is separate from live acceptance.

Validation on the final implementation: **43 transactional tests + 82 existing Production Shadow tests passed**. Full regression: **470 passed, 1 skipped, 0 failed**. Public sanitization and UI JavaScript syntax checks passed. The local CLI schema confirmed the read-only thread/turn sandbox parameters. No live handoff capture, production C2G, or G2C execution was performed. Status: `PHASE_2_7_TRANSACTIONAL_C2G_READY`.
