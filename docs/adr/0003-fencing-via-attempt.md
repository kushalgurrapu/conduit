# ADR 0003: Fencing via the attempt counter

## Status

Accepted

This is a **planned** decision. It is **not implemented**. The `tasks`
table has no `attempt` column, claiming does not count attempts, and no
write checks for a stale worker.

## Context

Once leases exist, a worker can lose ownership of a task without knowing it:

1. Worker A claims the task.
2. A stalls (a long garbage-collection pause, a network partition, a
   stuck call), and its lease expires.
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

## Consequences

- Fencing is only as good as the database check. It is correct because
  the check and the write are one statement in PostgreSQL, so it relies on
  ADR 0001.
- Every worker write has to handle "zero rows updated" as a normal
  outcome, not an error.
- Fencing protects the engine's state. It does **not** stop a stale worker
  from causing side effects outside the engine. That is the job of
  idempotency (ADR 0002).
- Today `POST /tasks/claim` uses a random `worker_id` per call. A stable,
  per-worker identity belongs to the worker loop work.

## Future implications

These are the milestones this decision constrains. None of it exists yet.

- The lease milestone adds the `attempt` column and lease expiry. The claim
  statement then increments `attempt`, and heartbeats, completion, and
  failure use the guarded `UPDATE`.
- Recovery of abandoned tasks must put a task back to `PENDING` without
  resetting `attempt`. The next claim then produces a higher value.
- Retry limits can be expressed against `attempt`.
- A history of attempts could be recorded separately if needed.
- Workflow advancement (moving to the next step) must happen only if the
  guarded update succeeded.
- Tests for this should reproduce the stale-worker scenario above and
  assert that A's write is rejected and B's is kept.
