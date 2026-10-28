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
| `UNKNOWN_LIVENESS` | Always block. |

The local transactional scanner has no connection to the desktop owning
app-server. Consequently, its returned list contains **quiescence blockers**,
explicitly classified `UNKNOWN_LIVENESS`, rather than assertions of current
execution. An unmatched old start remains unknown even when other later turns
completed: completion of one turn is not invented completion of another. This
retains the existing conservative gate without calling stale evidence active.

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

Validation: 24 synthetic liveness regressions, 67 combined liveness/transactional
tests, and full regression of **494 passed, 1 skipped, 0 failed**. The full run
also reported three warnings in existing asynchronous test/transport cleanup.
These results validate the generic detector; they do not establish live
production quiescence or authorize a send.
