# ADR 0004: Task lifecycle and execution model

## Status

Accepted

This is the M1 decision. The schema, the `Task` record, claim, the
guarded complete, fail, and cancel writes, `TaskExecutor` (claim,
handler, one finish write), and the worker pool are implemented. The
versioned REST API is not. See "Current implementation versus planned".

Roadmap M2 (the worker runtime) is merged into M1. Leases, recovery, and
retries stay in M3-M5. This is the only ADR for the M1 lifecycle and
execution model.

## Context

Claiming a task is not executing it. A claim moves a row from
`PENDING` to `RUNNING`. Without a finish write, a row can sit in
`RUNNING` with no record of whether a handler ran, crashed, or never
started.

M1 has to close that gap without pretending the engine can do things Java
and PostgreSQL cannot do:

- A handler that fails is not the same thing as a worker that disappears.
- An interrupt during shutdown is not a task failure.
- A `CHECK` constraint cannot see the previous status, so it cannot make
  a terminal state absorbing.
- `worker_id` is not a fencing token once the same worker can claim a task
  again. In M1 it cannot, which is why the M1 guard is allowed to stop at
  `worker_id`. ADR 0003 still applies when leases and re-claims exist.

The API and the worker are one milestone. The executor needs complete and
fail, and a lifecycle API is not demoable without a worker.

## Decision

### States, and the `WHERE` clause as the state machine

```text
PENDING  --claim-->   RUNNING  --complete (owner)-->  COMPLETED
   |                     |
   +--cancel--> CANCELLED +--fail (owner)-->          FAILED
```

There is no engine transition from `RUNNING` back to `PENDING`. The
roadmap reserves that edge for lease recovery (M4) and retries (M5).

Legal transitions are the `WHERE` clause of a single `UPDATE`. Java
classifies the result. It does not check the status and then write.

```sql
-- complete (owning worker only)
UPDATE tasks
SET status = 'COMPLETED', result = CAST(:result AS jsonb),
    finished_at = now(), updated_at = now()
WHERE id = :id AND status = 'RUNNING' AND worker_id = :workerId;

-- fail (owning worker only; error truncated in SQL)
UPDATE tasks
SET status = 'FAILED', error = left(:error, 4000),
    finished_at = now(), updated_at = now()
WHERE id = :id AND status = 'RUNNING' AND worker_id = :workerId;

-- cancel (pending only)
UPDATE tasks
SET status = 'CANCELLED', finished_at = now(), updated_at = now()
WHERE id = :id AND status = 'PENDING';
```

`finished_at` is the terminal timestamp for `COMPLETED`, `FAILED`, and
`CANCELLED`. The name is `finished_at`, not `completed_at`, because
failure and cancellation are terminal too.

On the worker path, zero updated rows is a normal result, not an
exception (the same rule as ADR 0003). The service re-reads the row only
to classify the outcome: applied, already applied (same worker and same
target status), or rejected. The re-read never changes the decision, and
a zero-row update is not retried.

Cancel of an already-`CANCELLED` task is idempotent (`200` once the API
exists). Cancel of `RUNNING`, or of any other terminal status, does not
apply (`409`). A missing id is not found.

#### Terminal states are not absorbing at the `CHECK` layer

`tasks_status_check` only limits which status strings exist.
`tasks_finished_at_iff_terminal_check` requires `finished_at` exactly
when the status is terminal. Neither constraint can see the previous row.
An `UPDATE` that sets `status = 'PENDING'` and `finished_at = NULL` in the
same statement satisfies both checks.

Terminal states are absorbing because each statement's `WHERE` requires
the pre-state (`RUNNING` plus the owning `worker_id`, or `PENDING`). A
statement that does not have that guard can still move a terminal row.

Manual requeue is that edge. It is documentation only in M1, not an API.
It is safe for engine state when it pins the worker that was observed:

```sql
UPDATE tasks
SET status = 'PENDING', worker_id = NULL, claimed_at = NULL,
    updated_at = now()
WHERE id = :id
  AND status = 'RUNNING'
  AND worker_id = :observedWorkerId;
```

The handler may already have run, and it may run again. That is not
exactly-once execution, and it is not the at-least-once recovery in
ADR 0002.

### Cancel versus claim under `READ COMMITTED`

PostgreSQL's default isolation level is `READ COMMITTED`. M1 relies on
that. Methods are not pinned to another isolation level.

The two statements lock differently:

- **Claim** selects with `FOR UPDATE SKIP LOCKED`. A row locked by another
  transaction is skipped, not waited on. The selecting CTE requires
  `status = 'PENDING'`, and the outer `UPDATE` requires it again
  (`AND t.status = 'PENDING'`). If a claim ever waited on a lock (which
  is what happens when `SKIP LOCKED` is removed), `READ COMMITTED`
  re-evaluates that predicate against the committed row, so a row that
  became `RUNNING` is not claimed.
- **Cancel** updates one id and does not use `SKIP LOCKED`. If a claim
  transaction holds that row, cancel waits. When the lock is released,
  `READ COMMITTED` re-evaluates `status = 'PENDING'`:
  - the claim committed → the row is `RUNNING` → cancel updates zero rows
  - the claim rolled back → the row is still `PENDING` → cancel applies

Claim does not block behind cancel or behind another claim. Cancel does
not apply a decision it made before it acquired the lock.

### Three short transactions, one finish attempt

A worker cycle is three steps. Only the first and the third are
transactions. The executor itself is not `@Transactional`. It calls the
service through the Spring proxy so those transactions actually start
and end.

1. **Claim** (short transaction). `PENDING` becomes `RUNNING`, or there is
   nothing to do.
2. **Execute** the handler with no transaction and no pooled connection.
3. **One** finish write (short transaction): complete on success, or fail
   on a failed outcome, a thrown exception, a missing handler, or a null
   return.

If the finish write throws, the engine logs and leaves the row `RUNNING`.
There is no SQLState classifier, no second finish attempt, and no fallback
write of the other outcome.

`RUNNING` means claimed and not finished. The database cannot tell a live
handler from a crash, an interrupt, or a failed finish write.

### Failure is terminal

A handler that returns failure, throws an exception, is missing, or
returns null is recorded as `FAILED` by that one finish write. `FAILED`
is terminal in M1. The engine does not put the task back to `PENDING`.

That limit is deliberate. Automatic retry without an attempt counter, a
maximum, and a delay would re-run poison tasks immediately. Retries are
M5. Lease recovery is M4. Until then, a failed task stays failed, and a
claimed task whose finish never commits stays `RUNNING`.

### Interruption is not failure

Stopping the process must not record a guess.

- If the handler is interrupted while it is still running, the loop
  restores the interrupt flag and does not write `FAILED`. The row stays
  `RUNNING` with the same `worker_id`. This only works for handlers that
  honor interrupts.
- If the handler returned an outcome, that outcome still gets its one
  finish write. The interrupt flag is cleared around that write and then
  restored so the loop can exit.
- After stop begins, loops claim nothing new. A task already claimed is
  still executed. Handlers still running when the shutdown deadline
  passes are interrupted and left `RUNNING`.

There is no handler timeout. Marking the task `FAILED` while the handler
kept running would record a lie, and Java cannot kill that thread. Leases
and a cooperative cancel signal are M3.

### Why `worker_id` is a sufficient fence in M1

The finish guard is:

```sql
WHERE id = :id AND status = 'RUNNING' AND worker_id = :workerId
```

`worker_id` alone is enough for M1 because all three of these hold:

- One loop has one thread and one `worker_id` for the life of that loop.
- That loop never claims the same task again. M1 never moves `RUNNING`
  back to `PENDING`, and there is no timeout that would free the row for
  another claim by the same id.
- Nothing else finishes the task on behalf of that id.

ADR 0003 is unchanged. Once a lease can expire and the same `worker_id`
can claim the task again, a stale execution and the new one would carry
the same id. M3 adds `attempt`, incremented only by the claim statement,
to this guard. M1 does not reject a stale writer from a later claim by
the same `worker_id`. That situation cannot arise under M1's own rules,
and the engine does not yet defend against it.

### Guarantees M1 will make, and will not make

These are the claims the tests support. The finish guards are
implemented: one applied complete or fail per claim, only from the
owning `worker_id`, and cancel only from `PENDING`. `TaskExecutorTest`
covers no connection during execute, one finish attempt, and an
interrupt left as `RUNNING`. `WorkerPoolTest` covers the concurrency
cap, one handler call per drained task, shutdown that finishes claimed
work without new claims, an interrupt at the shutdown deadline left as
`RUNNING`, and a crashed claim that stays `RUNNING` while a later task
completes.

M1 provides:

1. This process claims a given `PENDING` row at most once and never puts
   it back to `PENDING`. A crash after claim and before execute leaves
   `RUNNING` with zero handler calls.
2. At most one applied `COMPLETED` or `FAILED` write per claim, and only
   from the claiming loop (`WHERE id AND status = 'RUNNING' AND
   worker_id`). Cancel applies only to `PENDING`.
3. Named checks: `finished_at` is set exactly when the status is terminal;
   `RUNNING` has an owner (`worker_id` and `claimed_at`); `PENDING` has
   neither; `task_type` matches `^[A-Z][A-Z0-9_]{0,63}$`.
4. No transaction and no pooled connection while a handler runs.
5. In-flight handlers never exceed the configured concurrency. Each loop
   pulls one task at a time and does not prefetch.
6. After stop: no new claims; a claimed task is always executed; a
   returned outcome gets one finish attempt; handlers still running at
   the deadline are interrupted and left `RUNNING` if they honor
   interrupts.
7. `RUNNING` means claimed and not finished.
8. The manual requeue above is safe for engine state when it pins the
   observed worker. The handler may already have run and may run again.

M1 does not provide, and must not be described as providing:

- Exactly-once execution.
- At-least-once execution. That remains ADR 0002, after recovery (M4).
- Automatic recovery or a reaper.
- A promise that a returned outcome always commits. `RUNNING` after a
  failed finish write is allowed.
- Rejection of a stale writer from a later claim by the same `worker_id`
  (ADR 0003, via `attempt`, in M3).
- Handler timeouts or detection of a hung worker.
- Task-level retries.

"The handler was invoked at most once" describes this process's control
flow, not durability. A manual requeue, and a future reaper, are second
invocations.

## Current implementation versus planned

**Implemented today:**

- Create a task (`POST /tasks`) in `PENDING` with `task_type = 'NOOP'`.
  The service method is `createTask(taskType, payload)` and returns the
  inserted row. JSON columns are text at the repository boundary.
- `TaskStatus` with `isTerminal()` only. `Task` carries `taskType`,
  `status`, `payload`, `result`, `error`, `createdAt`, `updatedAt`,
  `claimedAt`, `finishedAt`, and `workerId`.
- Claim one `PENDING` row (`POST /tasks/claim` or `TaskService.claimTask`):
  `FOR UPDATE SKIP LOCKED`, then `UPDATE ... RETURNING`, inside one
  transaction. The outer update requires `status = 'PENDING'` again, sets
  `updated_at = now()` along with `worker_id` and `claimed_at`, and
  returns the mapped columns (not `RETURNING *`). A null `workerId` is
  rejected before that write. The row becomes `RUNNING`.
- Complete and fail (`TaskService.completeTask` / `failTask`): one
  `UPDATE ... RETURNING` whose `WHERE` is `id`, `status = 'RUNNING'`, and
  `worker_id`. The statement sets `finished_at` and `updated_at`. Fail
  strips NUL bytes in Java and stores `left(error, 4000)`. A null
  `workerId` is rejected before the write. Zero rows is not an exception
  and is not retried. The same transaction re-reads the row only to
  classify it: `Applied`, `AlreadyApplied` (same worker and same target
  status), or `Rejected`. A second complete with a different result leaves
  the first result and the timestamps unchanged.
- Cancel (`TaskService.cancelTask`): one update whose `WHERE` is `id` and
  `status = 'PENDING'`. `Cancelled`, `AlreadyCancelled`, and
  `NotCancellable` are values. A missing id is `TaskNotFoundException`.
  There is no HTTP mapping yet.
- The manual requeue in this ADR is not a repository method. A test runs
  that statement, then shows the old worker's complete is rejected and
  the new worker's complete applies.
- `V4` adds `task_type`, `result`, `error`, and `finished_at`, backfills
  existing rows, and adds the named checks: `tasks_task_type_format_check`,
  `tasks_finished_at_iff_terminal_check`, `tasks_running_has_owner_check`,
  and `tasks_pending_has_no_owner_check`. `tasks_status_check` remains.
- Concurrent claim tests: a task is not claimed twice, and a locked row is
  skipped rather than waited on. `TransitionLockHoldTest` holds an
  uncommitted claim: cancel waits, then loses if the claim commits and
  applies if it rolls back. A second claim returns empty, and the
  committed worker is the first one.
- `TaskHandler`, `TaskContext`, and `TaskOutcome` (`Succeeded` /
  `Failed`). `TaskExecutor.runOnce` is not transactional. It claims
  through the `TaskService` proxy, runs the handler with no transaction
  and no pooled connection, then makes one complete or fail call.
  `NOOP`, `ECHO`, `FAIL`, and `SLEEP` are always registered. `SLEEP`
  reads `millis` from the payload and caps it at 60 seconds; that cap
  is not a handler timeout. Handlers are indexed in a
  `Map<String, TaskHandler>` bean. Startup fails on a duplicate type or
  a type that fails `^[A-Z][A-Z0-9_]{0,63}$`.
- A missing handler, a thrown exception, or a null return is one fail
  write (`no handler for type X`, the exception class and message, or
  `handler returned null`). An `Error` propagates and the row stays
  `RUNNING`. An interrupt during execute, including an
  `InterruptedException` in the cause chain or the interrupt flag set
  when the handler throws, restores the flag and leaves the row
  `RUNNING`. A returned outcome, including `Failed`, still gets its one
  finish write: the interrupt flag is cleared around that write and
  then restored. `Applied` and `AlreadyApplied` are success. Anything
  else is logged. If the finish write throws, it is logged, the row
  stays `RUNNING`, and `runOnce` returns true. An empty queue returns
  false. MDC holds `taskId`, `workerId`, and `taskType` for the cycle.
- `WorkerPool` is a `SmartLifecycle`. It starts when
  `engine.worker.enabled` is true (the default; tests set it false) and
  runs `engine.worker.concurrency` platform threads (default 4). Each
  loop has one UUID for its lifetime and a thread name
  `host-pid-loopN`. The loop calls `runOnce`. It reads the stop flag
  only before that call, so a task already claimed is still executed.
  An empty claim waits `poll-interval` on a latch that shutdown opens.
  `DataAccessException` and `TransactionException` are logged, then the
  loop waits `error-backoff` and continues. Any other `Exception` is
  logged and the loop continues. An `Error` kills that loop only.
- Shutdown sets the stop flag, wakes idle loops, and waits
  `shutdown-timeout` (default 20s). That duration must be shorter than
  `spring.lifecycle.timeout-per-shutdown-phase` (Boot default 30s). It
  is not a limit on handler runtime while the process is up. When the
  wait ends, handlers still running are interrupted. An interrupt leaves
  the task `RUNNING`. The pool logs those task ids as still running at
  the deadline; a late finish is still possible. There is no startup
  check that Hikari `maximum-pool-size` is at least concurrency plus
  API headroom. That rule is documented in `application.yaml`.

**Not implemented:**

- The versioned REST API. There is no GET, no cancel endpoint, and no
  `ProblemDetail` error model. Complete and fail are not HTTP operations.
  `POST /tasks` and `POST /tasks/claim` are still the HTTP API.

The named checks hold. The finish guards hold. `TaskExecutorTest` covers
one finish attempt, no connection during execute, and interruption left
as `RUNNING`. `WorkerPoolTest` covers the concurrency cap and the stop
behavior. A crash after claim leaves the task `RUNNING` with no further
progress. That is a lost task, not at-least-once execution (ADR 0002).

## Consequences

- Docs may describe this model as the M1 target. They must not describe
  it as current behavior until the tests exist.
- Handlers written for M1 must still tolerate a later second run. M1 does
  not promise they run only once for the life of the row.
- `FAILED` in M1 is terminal. Callers cannot expect an automatic retry.
- An operator who requeues by hand must pin `worker_id` and must accept a
  repeated side effect.
- Hikari's maximum pool size must be at least worker concurrency plus
  headroom for the API. No connection is held during execute, so a test
  in which a handler holds a connection does not describe this design.
  There is no startup check that enforces the size.
- One loop, one platform thread, one UUID. Later fencing uses `attempt`,
  not a process-wide worker id.

## Future implications

- M3 adds `attempt` on the claim statement and on the finish guard, and
  replaces `tasks_running_has_owner_check` with one check that also
  requires the lease. It does not add a second overlapping check.
  Cooperative cancel of a `RUNNING` task, and any handler timeout, also
  wait until M3.
- When M4's reaper returns a task to `PENDING`, it must clear
  `worker_id`, `lease_expires_at`, and `claimed_at`. A `PENDING` row that
  still has an owner would violate `tasks_pending_has_no_owner_check`.
- M5 adds retries (`RUNNING` to `PENDING` with a delay). That is the
  first engine-owned edge back to `PENDING`, other than the reaper.
- `V4` will not check that `result` and `error` are mutually exclusive.
  Those checks would block `last_error` (M5) and a failure that still has
  an HTTP body (M8).
