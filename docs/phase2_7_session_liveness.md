# Phase 2.7.0.4 — Production session liveness

Quiescence is a safety decision, not a claim that every unmatched historical
start represents a currently executing turn. The transactional detector reads
the entire workspace-matching rollout and matches terminal events to exact turn
IDs. It uses neither file age, an index update timestamp, a tail window, nor a
database `inProgress` flag to decide whether the workspace is idle.

The read-only audit prefers current state from the app-server that owns a loaded
thread. A separate observer's `notLoaded` state does **not** establish global
idleness. Starting or resuming a production thread to discover its state is not
permitted. When owner state is unavailable, lifecycle evidence and correlated
queue/process/tool observations retain their individual scope and limitations.

| Classification | Quiescence policy |
| --- | --- |
| `ACTIVE_EXECUTING` | Block; requires positive current execution evidence. |
| `ACTIVE_WAITING_HUMAN_ACTION` | Block if the same turn could continue and write. |
| `QUEUED_NOT_DISPATCHED` | Queue presence alone does not prove execution. A closed rollout need not block when the transaction cannot dispatch the queue. |
| `COMPLETED` | Exact terminal lifecycle is nonblocking. |
| `ABORTED` | Exact abort lifecycle is nonblocking. |
| `STALE_ACTIVE_METADATA` | A stale flag alone does not block when closure is independently established. |
| `SUPERSEDED_ORPHAN_TURN` | A later distinct verified Started turn in the same thread proves the orphan is no longer current. Nonblocking historical defect; not a successful completion. |
| `UNKNOWN_LIVENESS` | Always block. |

The local transactional scanner has no connection to the desktop owning
app-server. Consequently, its returned list contains **quiescence blockers**,
explicitly classified `UNKNOWN_LIVENESS`, rather than assertions of current
execution. An unmatched old start remains unknown unless exact same-thread
protocol evidence proves supersession. Completion of another turn, a context
record, normal user input, same-ID steering, recency, and database flags do not
provide that evidence.

## Distinct Started evidence (Phase 2.7.0.5)

The official app-server implementation inspected at upstream commit
`ce2d47bdc4dd2513d5b3567090593b07b0fdfa0a` calls `start_or_steer_turn` and
distinguishes `TurnInputSubmission::Started` from `Steered`:
[turn processor](https://github.com/openai/codex/blob/ce2d47bdc4dd2513d5b3567090593b07b0fdfa0a/codex-rs/app-server/src/request_processors/turn_processor.rs#L638),
[Core admission](https://github.com/openai/codex/blob/ce2d47bdc4dd2513d5b3567090593b07b0fdfa0a/codex-rs/core/src/session/turn_input.rs).
[Official app-server documentation](https://learn.chatgpt.com/docs/app-server#steer-an-active-turn)
also specifies that steering preserves the active turn and does not emit a new
started notification. The same-thread supersession rule is the inference from
this serial lifecycle; it is not a claim that a historical turn succeeded.

The parser requires valid session metadata and an exact, nonempty turn ID on a
strictly later `event_msg/task_started` in that same rollout. Only a different
turn ID can supersede an earlier verified start. A start in another rollout or
an explicitly different thread cannot clear it. Evidence of old-turn activity
after supersession prevents the serial inference and remains blocking.

Every observed turn retains its start line, actual terminal line/time (or NULL),
classification, and the observed superseding turn/line. No terminal event is
invented. The latest unresolved turn remains UNKNOWN and blocking, even when
all historical orphans are superseded. A real terminal still takes precedence
for that exact turn. The upstream source snapshot is an implementation reference,
not an attestation that an installed binary has the same source revision.

Hardening also closes unsafe idle results for workspace-matching rollouts with
no lifecycle evidence, a new unclosed turn context, or unbound tool calls after
closure. Missing/corrupt inventories and terminal events without exact IDs
continue to fail closed. Session headers missing the exact thread identity or
workspace are rejected before workspace filtering or exclusion. Known completed
or aborted rollouts remain nonblocking,
including when completion is far outside the tail or timestamps are recent.

Audit records, exact identities, repository correlations, prompts, and API
responses belong only in ignored private storage. A missing historical repository
baseline prevents proving who wrote a file from timestamps alone. Unchanged
samples establish observed stability only during their sampling interval.

The audit does not create a parent, start a handoff, send C2G, submit G2C, resume
production, or run Auto. A consumed earlier parent stays consumed. A subsequent
live transaction requires a new operator parent authorization and a running
implementation whose identity is independently approved; testing or publishing
this change does not authorize that transaction.

`GET /api/production-shadow/quiescence` performs one read-only session inventory
and a fresh repository checkpoint with that blocker set. It creates no grant,
handoff, child authorization, browser operation, or queue item. A quiet result
is eligibility for later reauthorization, not a fresh handoff or an authorized
send. The normal transaction still applies every target, budget, branch, and
freshness gate. `/status` exposes startup-fixed implementation commit/protocol
fingerprints so a restarted service can be checked against the published code.

Phase 2.7.0.4 validation: 24 synthetic liveness regressions, 67 combined liveness/transactional
tests, and full regression of **494 passed, 1 skipped, 0 failed**. The full run
also reported three warnings in existing asynchronous test/transport cleanup.
These results validate the generic detector; they do not establish live
production quiescence or authorize a send.

Phase 2.7.0.5 validation: 27 supersession/runtime regressions, 94 combined
supersession/liveness/transactional tests, and full regression of **521 passed,
1 skipped, 0 failed**, with three existing asynchronous cleanup warnings.

## Schema-aware inventory and terminal audit (Phase 2.7.0.6)

Installed CLI schema generation is read-only and does not start an app-server
session. The audited installation identifies itself as `0.165.0-alpha.1.2`;
its generated `TurnStatus` enum contains `completed`, `interrupted`, `failed`,
and `inProgress`. API notifications and persisted rollout events are separate
representations. The inspected official protocol also defines `turn_started`
and `turn_complete` as aliases of the persisted `task_started` and
`task_complete` events. Exact turn attribution is still required for closure.

| Persisted field | Meaning and policy |
| --- | --- |
| Current `session_meta.payload.id` | Thread ownership; a different owner fails closed. |
| `session_meta.payload.session_id` | Shared root session identity, not necessarily this thread. |
| `forked_from_id`, `parent_thread_id` | Relationships, retained as references. |
| `item_started` / `item_completed` `payload.thread_id` | Schema-defined item ownership; malformed or foreign owners fail closed. |
| Collaboration sender/receiver/new-thread fields, queue resource IDs, tool arguments | Related resources, not inferred record ownership. |

An explicitly declared paginated subagent prefix has different ownership
semantics. With valid fork provenance, an explicit positive
`subagent_history_start_ordinal`, and contiguous observed ordinals starting at
zero, the parser retains earlier parent records as `RELATED_THREAD_REFERENCE`
and inherited context. It does not project their starts or terminals into the
child. Foreign metadata outside that prefix remains a real mismatch. An
unproven, malformed, or truncated prefix cannot prove child idleness.

This follows the inspected official
[SessionMeta contract](https://github.com/openai/codex/blob/ce2d47bdc4dd2513d5b3567090593b07b0fdfa0a/codex-rs/protocol/src/protocol.rs#L3429)
and [projection boundary](https://github.com/openai/codex/blob/ce2d47bdc4dd2513d5b3567090593b07b0fdfa0a/codex-rs/thread-store/src/local/thread_history_materialization.rs#L230).
The official source revision is a semantic reference; it is not asserted to be
the exact source revision of the installed desktop binary. Private evidence
binds the installed executable hash, generated schemas, and observed records.

A `turn_aborted` with reason `interrupted` remains an abort, not success.
`stream_error` is documented as a disconnect/error being handled, possibly by
retry. At an unclosed EOF it yields blocking `INTERRUPTED_OR_DISCONNECTED`;
it never supplies an invented terminal. A later real terminal overrides that
diagnostic. A final answer without a terminal yields blocking
`CRASHED_OR_INCOMPLETE`: incompleteness is observed, its cause is not proven.
An unanswered explicit human request is blocking `ACTIVE_WAITING_HUMAN_ACTION`.
Quiet timestamps never resolve these states. Unknown outer record types and
truncated JSON remain blocking; known context/communication metadata does not
invent execution.

Production startup installs the same runtime inventory provider for both the
transaction and `/quiescence`. It enumerates current Codex app-server processes
on supported x64 Windows hosts, reads only command/root parameters without
attaching a debugger or executing target code, and retains only root variables.
Every positively identified home/session root is inspected, including the
configured root. Process-profile defaults are explicit; directory names and
age do not exclude a root. Registries are opened with SQLite `mode=ro` and
checked for production-cwd sessions missing from readable rollouts. Windows
extended path spellings are normalized for that registry correlation.

Unreadable process parameters, uncorrelated roots, corrupt inventories, and
unresolved registered production rollouts produce `UNKNOWN_LIVENESS` blockers.
The returned `inventory_scope.complete` distinguishes complete enumeration from
an incomplete scope. No file is discarded as historical merely because it
looks archived, is old, or has an empty queue. Exclusion requires positive
evidence not currently supplied by an independent observer's `notLoaded` result.
Synthetic tests inject runtime roots rather than inspect the operator's machine.
