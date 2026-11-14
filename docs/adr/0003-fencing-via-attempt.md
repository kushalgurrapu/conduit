# ADR 0003: Fencing via the attempt counter

## Status

Accepted

The `attempt` fence is implemented on lease extension, complete, and
fail (M3). Since M4, the reaper requeues a task whose lease expired, and
the M4 tests show the fence rejects the stale worker after a later
claim. See "Current implementation versus planned".

## Context

Once leases exist, a worker can lose ownership of a task without knowing it:

1. Worker A claims the task.
2. A stalls (a long garbage-collection pause, a network partition, a
   stuck call), and its lease expires. A reaper then requeues the task.
   An expired lease alone does not let anyone else claim it.
3. Worker B claims the same task. This is a newer attempt.
4. A wakes up, believes it still owns the task, and writes a result.

Without protection, A's stale write overwrites or conflicts with B's work.

Checking the lease in Java before writing does not fix this. A can pass
the check and then pause before the write, so the lease can expire between
the check and the write. The check has to happen at the point of the
write, atomically with it. This is the idea of a fencing token.

## Decision

Each claim of a task will get a number, the **`attempt`**, and that number
will be the fencing token.

- `attempt` is an integer on the task. It is incremented only by the claim
  statement, so each successful claim gets a higher value than the one
  before it.
- The claim returns the `attempt` to the worker. The worker keeps it for
  the whole execution.
- Every state-changing write by a worker (extend the lease, complete,
  fail) will be one guarded statement of this shape:

  ```sql
  UPDATE tasks
  SET ...
  WHERE id = :id
    AND status = 'RUNNING'
    AND worker_id = :workerId
    AND attempt = :attempt
  ```

- If it updates zero rows, the worker is stale. It must discard its result
  and stop, and must not retry the write.

`worker_id` alone is not enough, because the same worker id can claim a
task again after its earlier lease expired. A stale execution from before
and the new execution would then look identical. The pair
`(worker_id, attempt)` tells them apart.

### Alternatives considered

- **Check the lease in application code before writing**: races as
  described above.
- **A generic version column bumped on every update**: also detects
  stale writes, but it changes on every heartbeat and carries no meaning.
  `attempt` changes only when ownership changes, and doubles as the retry
  count.
- **An external lock service**: adds infrastructure and still needs
  fencing at the write.

## Current implementation versus planned

**Implemented today (M3):**

- V5 adds `attempt int not null default 0`. The claim statement sets
  `attempt = attempt + 1` in the same `UPDATE` that sets `RUNNING`, the
  owner, and the lease. No other production statement changes
  `attempt`. Create, extend, complete, fail, and cancel leave it as it is.
- `TaskService.extendLease(taskId, workerId, attempt)` is one `UPDATE`
  guarded by `id`, `status = 'RUNNING'`, `worker_id`, and `attempt`. It
  sets `lease_expires_at` and `last_heartbeat_at` and returns `false`
  when it matches nothing. It does not require `lease_expires_at >
  NOW()`, so a lease that is already in the past still extends. An old
  timestamp does not mean a new owner exists: an expired lease makes the
  row eligible for requeue, and ownership ends only when a requeue or the
  owner's own finish commits (ADR 0005).
- Complete and fail use the same guard. Zero rows is still a value and
  is not retried. `AlreadyApplied` means this worker **and this attempt**
  already reached the target status. A different attempt is `Rejected`,
  even when the worker and the status match. Complete and fail do not
  require an unexpired lease.
- `TaskExecutor` passes the attempt from the claimed row to the handler's
  `TaskContext`, to the heartbeat, and to the finish write. It does not
  read the attempt again.
- When an extension returns `false`, the heartbeat sets
  `TaskContext.leaseLost()`. That asks the handler to stop. It does not
  stop it. A handler that returns anyway still gets its one finish
  write, and the guard rejects that write if the attempt no longer
  matches. A heartbeat that throws an exception (for example a
  data-access error) is not lease loss. The flag stays `false`, and the
  next tick tries again.

Nothing checks the lease in Java before a write. The check is in the
write itself, which is the point of this ADR. The heartbeat does not
compare `lease_expires_at` with a JVM clock. It treats only a `false`
extension as lease loss.

**What the tests show:** `LeaseExtensionTest` uses the same `worker_id`
with a different `attempt` on a row that is still `RUNNING`, and extend,
complete, and fail change nothing. In the same class, test-only SQL
requeues a claimed task without touching `attempt`, and the same worker
claims it again. The old attempt's extend and complete are rejected, and
the new attempt completes. `LeaseHeartbeatIntegrationTest` bumps
`attempt` in SQL while a handler is blocked. The handler sees
`leaseLost()`, returns success anyway, and its complete matches no row.

**What the M4 tests add.** M4 found this guard already present and did
not change it. It added the reaper (ADR 0005), the first production path
to a later claim of the same row, and these tests of the fence against
that claim:

- `RecoveryIntegrationTest` reaps worker A's attempt and then checks A's
  extend and finish in every state the row can reach: not claimed again
  (`Rejected(PENDING, null)`), held by B (`Rejected(RUNNING, B)`, and
  A's extend returns `false` with B's lease unchanged), finished by B
  (`Rejected(COMPLETED, B)` or `Rejected(FAILED, B)`), cancelled after
  the reap (`Rejected(CANCELLED, null)`), and claimed again by A's own
  id (`Rejected(RUNNING, A)`, then `Rejected(COMPLETED, A)`, never
  `AlreadyApplied`). Each rejected finish leaves the row and its history
  byte-for-byte unchanged, and the stored result is the later attempt's.
- `RecoverySoakTest` checks every write in a seeded random walk against
  the row read just before it: it applies exactly when the row is
  `RUNNING` with that worker and attempt, and otherwise changes nothing.
- `RecoveryChaosTest` releases every crashed handler after all tasks
  completed. Each of their finishes is `Rejected(COMPLETED, <final
  worker>)`, and the database does not change.

## Consequences

- Fencing is only as good as the database check. It is correct because
  the check and the write are one statement in PostgreSQL, so it relies on
  ADR 0001.
- Every worker write has to handle "zero rows updated" as a normal
  outcome, not an error.
- Fencing protects the engine's state. It does **not** stop a stale worker
  from causing side effects outside the engine. That is the job of
  idempotency (ADR 0002).
- There is no HTTP claim endpoint. Each worker loop has one UUID for its
  lifetime (ADR 0004). That id is still not a fence against a later claim
  of the same row. Fencing needs `attempt`.

## Future implications

These are the milestones this decision constrains. The `attempt` column,
the lease, and the guarded `UPDATE` on extend, complete, and fail exist
(M3).

- Recovery of abandoned tasks (M4) puts a task back to `PENDING`
  without resetting `attempt`, so the next claim produces a higher
  value. The reaper clears `worker_id`, `claimed_at`,
  `lease_expires_at`, and `last_heartbeat_at`. This is done (ADR 0005).
- Retry limits can be expressed against `attempt`.
- A history of attempts is recorded separately in `task_attempts`, one
  row per `(task_id, attempt)` (M4, ADR 0005).
- Workflow advancement (moving to the next step) must happen only if the
  guarded update succeeded.
- Tests for this should reproduce the stale-worker scenario above and
  assert that A's write is rejected and B's is kept.
