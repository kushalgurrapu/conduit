# ADR 0005: Lease recovery with a reaper

## Status

Accepted, implemented

This is the M4 decision. Sub-parts M4.2 through M4.8 built it, and M4
is complete (2026-11-13). "Current implementation versus planned"
describes the code as built. M4 takes ADR number 0005. The AWS
deployment ADR planned for M15 takes the next free number when M15
starts.

It changes one M1 rule in ADR 0004: a handler `Error` that is not a
`VirtualMachineError`, and `StackOverflowError`, become a failed attempt. It also changes one M3
class: `LeaseHeartbeat` is no longer stopped when a test context is
paused. M4.7 updated ADRs 0002, 0003, and 0004 to match.

## Context

Since M3, a claim is a lease. The claim statement sets
`lease_expires_at` and increments `attempt`, and `LeaseHeartbeat`
extends the lease while the handler runs. Before M4, nothing read an
expired lease. A crash after claim, an interrupt at shutdown, or a
finish write that threw left the row `RUNNING` for good. That is a lost task, not
at-least-once execution (ADR 0002).

M4 has to act on an expired lease without pretending the engine knows
more than it does:

- **Slow is not dead.** A lease expires when its heartbeats stop landing.
  A crashed process and a live one in a long garbage-collection pause,
  a stalled heartbeat thread, or a slow JDBC call look the same from the
  database. The engine cannot tell them apart, and Java cannot stop a
  handler that is still running.
- **Expired is not lost.** M3's extend, complete, and fail do not check
  expiry. A worker whose lease is in the past still owns the row until
  something else takes it. In M3 nothing did, so an expired owner could
  still extend, complete, or fail.
- **Terminal rows carry past leases.** Complete and fail do not clear
  `lease_expires_at`, so every `COMPLETED` and `FAILED` row has a lease
  that is now in the past. `lease_expires_at < now()` alone does not
  identify a task that needs recovery. The status must be `RUNNING` too.

The `attempt` fence already exists (ADR 0003). Claim increments
`attempt`, and extend, complete, and fail require it. M4 does not
re-implement the fence. It adds the first production path that produces
a later claim of the same row, and its tests show that the fence holds
against that later claim.

## Decision

### States and the ownership rule

```text
PENDING --claim (attempt + 1, lease, opens an attempt row)--> RUNNING
RUNNING --extend (worker + attempt; a past lease still extends)--> RUNNING
RUNNING --complete / fail (worker + attempt; closes the attempt row)--> COMPLETED / FAILED
RUNNING --reap (lease_expires_at < now(); attempt unchanged; closes the row LEASE_EXPIRED)--> PENDING
PENDING --cancel (also after a reap, while the old handler may still run)--> CANCELLED
```

A worker owns attempt `n` while the row is `RUNNING` with that
`worker_id` and `attempt = n`.

- An expired lease makes the row **eligible** for requeue. It does not
  end ownership.
- Ownership ends when one of these commits: a requeue (by the reaper, or
  by the operator procedure below), or the owner's own finish.
- After a requeue, nothing can make the old `(worker_id, attempt)` match
  again. `attempt` only increases, and terminal states stay terminal.

Claim, extend, complete, fail, cancel, and reap are each one guarded
`UPDATE` on `tasks`. Claim and an applied finish add one history
statement in the same `@Transactional` method. The reap writes both
tables in one statement. Handlers still run with no transaction and no
pooled connection.

### An explicit reaper

A `LeaseReaper` bean returns expired `RUNNING` rows to `PENDING`. The
claim statement is not changed. The reaper runs in every process where
it is enabled, including processes with workers disabled, so API-only
processes recover tasks too. M5 gets one place to put its expiry policy.

### The reap statement

One statement, in one transaction per batch:

```sql
WITH expired AS MATERIALIZED (
    SELECT id, worker_id, claimed_at
    FROM tasks
    WHERE status = 'RUNNING'
      AND lease_expires_at < NOW()
    ORDER BY lease_expires_at
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED
),
requeued AS (
    UPDATE tasks t
    SET status = 'PENDING',
        worker_id = NULL,
        claimed_at = NULL,
        lease_expires_at = NULL,
        last_heartbeat_at = NULL,
        updated_at = NOW()
    FROM expired e
    WHERE t.id = e.id
      AND t.status = 'RUNNING'
      AND t.lease_expires_at < NOW()
    RETURNING t.id, t.attempt, e.worker_id, e.claimed_at
),
recorded AS (
    INSERT INTO task_attempts (task_id, attempt, worker_id, started_at, ended_at, outcome)
    SELECT id, attempt, worker_id, claimed_at, NOW(), 'LEASE_EXPIRED'
    FROM requeued
    ON CONFLICT (task_id, attempt) DO UPDATE
        SET ended_at = EXCLUDED.ended_at,
            outcome = EXCLUDED.outcome
        WHERE task_attempts.outcome IS NULL
    RETURNING task_id
)
SELECT r.id, r.attempt, r.worker_id, (c.task_id IS NOT NULL) AS recorded
FROM requeued r
LEFT JOIN recorded c ON c.task_id = r.id
ORDER BY r.id
```

The requeue clears `worker_id`, `claimed_at`, `lease_expires_at`, and
`last_heartbeat_at`, sets `updated_at`, and does not write `attempt`.
The next claim makes it `attempt + 1`. The cleared owner fields satisfy
`tasks_pending_has_no_owner_check`. `finished_at` stays null, and
`PENDING` is not terminal, so `tasks_finished_at_iff_terminal_check`
holds. `tasks_running_has_owner_check` no longer applies to the row.

The outer `UPDATE` repeats `status = 'RUNNING'` and the expiry
predicate. That is redundant while the row lock is held, and it protects
the statement if `SKIP LOCKED` or the lock is ever removed. The CTE form
matches `claimTask` and is evaluated once. The roadmap's
`IN (subquery)` form is not used.

**`LIMIT` and row locks.** The plan on PostgreSQL 16 is
`Limit -> LockRows -> Index Scan using idx_tasks_running_lease`.
`LockRows` passes up only rows it locked and that passed the
`READ COMMITTED` re-check. Rows skipped by `SKIP LOCKED`, and rows
dropped by the re-check, do not count against `LIMIT`. A batch is
therefore short only when fewer than `batchSize` expired rows could be
locked. The rest are held by in-flight transactions and are considered
again on the next tick.

**`MATERIALIZED`.** PostgreSQL 16 never inlines a CTE that has a locking
clause, so the plans with `MATERIALIZED` and `NOT MATERIALIZED` are the
same. The keyword has no effect on the plan. It stays as documentation
that `expired` is evaluated once.

`ORDER BY r.id` only makes the result deterministic for tests. Callers
must not depend on that order for anything else.

### Concurrent writers under `READ COMMITTED`

- **A heartbeat, complete, fail, or another reap that has not
  committed** holds the row lock. The reaper skips the row and does not
  wait.
- **A heartbeat that committed after the reaper's snapshot** is seen by
  the `FOR UPDATE` re-check. A fresh lease fails `< NOW()`, so the row is
  not reaped. A complete or fail that committed is seen as a terminal
  row, and another reap that committed is seen as `PENDING`. Neither is
  reaped. Each expired attempt is requeued once.
- **A claim** never touches a reaper candidate. The statuses are
  disjoint.
- **A finish that arrives while a reap holds the lock** waits, then
  matches nothing. `classifyFinish` returns `Rejected(PENDING, null)`,
  and no history is written. **A heartbeat** in the same position waits,
  then returns `false`, so `leaseLost()` is set.
- **A cancel that arrives while a reap is uncommitted** sees `RUNNING` in
  its snapshot, so its `UPDATE` matches nothing and does not wait. Its
  re-read can see `PENDING`, so the client gets a `409` that reports
  `PENDING`.

A `true` from `extendLease` promises nothing about the future. If the
extend statement was delayed past the lease, the lease it writes is
already past, and the next reap may take the row.

### No expiry check on finish or extend

Extend, complete, and fail keep M3's guard: `id`, `RUNNING`,
`worker_id`, and `attempt`. None of them requires
`lease_expires_at > NOW()`. An expired but unreaped attempt can still
extend, complete, or fail, and the reaper then leaves the row alone. The
check would only discard results that nobody else owns and cause more
duplicates.

### The clock: `NOW()` and one batch per transaction

The statement uses `NOW()` everywhere, and each batch is its own
transaction. Leases are written with `NOW()` too, so both sides use the
database clock. `NOW()` is `STABLE`, so it can bound the index scan. If
another statement ran earlier in the same transaction, the cutoff is
slightly earlier, which can only under-reap.

The database clock can step. A backward step only delays a reap: a row
that looked expired is not reaped until `NOW()` passes its lease again.
That happened on the development machine, where the Docker Desktop VM's
clock stepped backwards between two transactions. The soak and recovery
tests tolerate it (see "Tests").

### Several reapers

There is no leader election and no advisory lock. Every process may run
a reaper. `SKIP LOCKED` gives concurrent reapers disjoint rows, the
re-check drops rows another reaper already requeued, and the history
upsert's `WHERE task_attempts.outcome IS NULL` guard closes each attempt
once. The cost is one indexed probe per process per interval.

### `LeaseReaper` lifecycle

`LeaseReaper` lives in the `worker` package and is a `SmartLifecycle`
with phase `0`. It owns one `ScheduledThreadPoolExecutor(1)` daemon
thread named `lease-reaper` that runs
`scheduleWithFixedDelay(interval, interval)`. It has a public
`@Autowired` constructor `(TaskService, ReaperProperties)`, which
validates the properties and builds the scheduler, and a package-private
constructor that also takes a `ScheduledExecutorService` for tests.

**Why phase `0`.** The heartbeat is phase `-1` and `WorkerPool` is
`Integer.MAX_VALUE`. The reaper starts after the heartbeat and before
the pool, and stops after the pool and before the heartbeat.
`WorkerPool.stop()` is synchronous, so the reaper keeps running while
the pool drains. Leases of tasks still in flight during the drain stay
fresh because of the **heartbeat's** phase `-1`, not because of the
reaper's phase. A handler that honors the shutdown interrupt closes its
watch in `finally`, so its lease stops moving at the interrupt. This
process's reaper may then requeue it during a later shutdown phase, such
as graceful web shutdown. That is benign. An asynchronous
`WorkerPool.stop` would weaken this argument, because it relies on the
pool having drained when its `stop()` returns.

**`start()` and `stop()`.** When the reaper is disabled, already
running, or already stopped once, `start()` does nothing. After a
`stop()`, `start()` logs a WARN, does not throw, and leaves
`isRunning()` false. The bean is not restartable, the same as the
heartbeat. `stop()` sets a stopping flag, then calls `shutdown()`, waits
two seconds, and calls `shutdownNow()`. An interrupt during a JDBC read
is probably ignored by pgjdbc. While waiting for a Hikari connection, it
throws. A batch still running when the `DataSource` closes is aborted
and rolled back by the server. Every outcome is atomic per batch.

**One tick.** A tick is one `reapOnce()` call. `reapOnce()` calls
`taskService.reapExpiredLeases(batchSize)` through the proxy, one
transaction per batch, and returns the total number of rows requeued.
It stops after a short batch, when the stopping flag is set, or when the
thread is interrupted. It checks the stopping flag, not `isRunning()`,
so a test can call it on a bean that was never started. It logs one
INFO line per non-empty batch, one DEBUG line per task, and a WARN for
each row whose history was not recorded.

**Context pausing.** spring-test 7.0.9 pauses inactive cached test
contexts by default (`ON_CONTEXT_SWITCH`) and restarts them on reuse.
Pausing stops every `SmartLifecycle` whose `isPauseable()` is true.

- `LeaseHeartbeat.isPauseable()` returns `false`. Its `stop()` shuts the
  scheduler down permanently, so a resumed context would run handlers
  without heartbeats. An idle daemon thread in a paused context costs
  nothing. `start()` and `stop()` keep their M3 behavior.
- `LeaseReaper` stays pauseable. A paused reaper stays stopped, and its
  restart is the WARN-logged no-op above. That is wanted: a reaper left
  running in a paused context would requeue other test classes'
  fixtures.

### Tick errors and the transaction timeout

What escapes `reapOnce()` decides what the tick does:

- **An `Exception`** is logged at WARN, and the tick returns. The next
  interval is the retry.
- **A `VirtualMachineError`** is logged at ERROR with the text "lease
  reaping has stopped in this process until restart", then rethrown. The
  scheduler suppresses later runs.
- **Any other `Error`** is logged at ERROR, and the tick returns, so the
  schedule survives.

This differs from the heartbeat, which rethrows every `Error`. The
heartbeat loses one watch on an `Error`. The reaper would lose all
recovery in that process.

`TaskService.reapExpiredLeases` is `@Transactional(timeout = 5)`. A reap
blocked on a table-level lock fails within about five seconds and is
tried again on the next tick. The timeout does not cover a half-open TCP
connection (see "Consequences").

### Configuration

`ReaperProperties` binds `engine.reaper`:

- `enabled`, default `true`.
- `interval`, default `10s`. It must be non-null and at least 100 ms.
- `batch-size`, default `100`. It must be between 1 and 1000 inclusive.

`TaskService.reapExpiredLeases` itself rejects only a batch size below
1. The reaper is independent of `engine.worker.enabled`. The Hikari
sizing rule becomes worker concurrency plus API headroom plus one for
the heartbeat thread plus one for the reaper thread. The maximum stays
10.

### Attempt history: `task_attempts`

V6 adds one row per claim:

```sql
alter table tasks
    add constraint tasks_attempt_nonnegative_check check (attempt >= 0);

create table task_attempts (
    task_id    uuid        not null,
    attempt    int         not null,
    worker_id  uuid        not null,
    started_at timestamptz not null,
    ended_at   timestamptz,
    outcome    text,
    error      text,
    constraint task_attempts_pkey primary key (task_id, attempt),
    constraint task_attempts_task_id_fkey foreign key (task_id)
        references tasks (id) on delete cascade,
    constraint task_attempts_attempt_check check (attempt >= 0),
    constraint task_attempts_outcome_check
        check (outcome in ('COMPLETED', 'FAILED', 'LEASE_EXPIRED')),
    constraint task_attempts_ended_iff_outcome_check
        check ((outcome is null) = (ended_at is null))
);

insert into task_attempts (task_id, attempt, worker_id, started_at)
select id, attempt, worker_id, claimed_at
from tasks
where status = 'RUNNING';
```

- **Key.** The primary key is `(task_id, attempt)`. There is no
  surrogate id and no separate `task_id` index.
- **Outcomes.** `outcome` and `ended_at` are null while the attempt is
  open. `LEASE_EXPIRED` means no finish was recorded for the attempt
  before its lease ran out. There is no `CANCELLED` outcome: cancel
  applies only to `PENDING`, so the attempt has already ended when a
  task is cancelled.
- **Writes.** A claim opens the row in the claim transaction
  (`ON CONFLICT (task_id, attempt) DO NOTHING`). An applied complete or
  fail closes it in the finish transaction with `ended_at = finished_at`
  and, for a failure, the stored `error`. That write is close-or-create:
  it closes the open row, or creates a closed one when there is none
  (test fixtures, legacy rows, and the old manual requeue). Its upsert is
  guarded by `WHERE task_attempts.outcome IS NULL`, so a closed row is
  never overwritten. The reap closes the row as `LEASE_EXPIRED` in the
  reap statement. `AlreadyApplied`, `Rejected`, and cancel write no
  history. The outcome is bound as `outcome.name()`.
- **Backfill.** V6 backfills only `RUNNING` rows, as open attempts. V5's
  legacy rows are included at attempt 0, are reaped on first start, and
  are claimed again at attempt 1. Terminal and `PENDING` rows are not
  backfilled, because their history is unknown. A `tasks` row set to a
  negative attempt by hand fails the migration. Apply V6 with the
  process stopped.
- **Deletes.** `ON DELETE CASCADE`: an attempt row means nothing without
  its task. `DELETE FROM tasks` in tests keeps working, and M12 retention
  removes history with its tasks.

**`tasks` is the source of truth, and history is the record of it.** A
history write that cannot be made is logged at WARN, and the `tasks`
change commits. A claim whose history row already exists still commits.
A finish or reap whose row is already closed still commits, and the reap
reports it as `recorded = false`. An infrastructure error, such as a
dropped connection, rolls back the state change and its history
together. For every `tasks` state that V1 through V6 allow, and every
committed history row, no history statement can fail with a constraint
or conflict error. Engine writers always lock `tasks` first, so engine
paths cannot deadlock on history.

**Never write `task_attempts` by hand while the engine runs.** A
hand-written insert that has not committed behaves differently. If it
lands before a claim, its foreign-key lock makes the claim skip that
task. If it lands after the claim locked the row, it deadlocks with the
claim (`40P01`), and PostgreSQL may roll back the claim once. The worker
loop backs off as it does for any `DataAccessException`. That cannot
stall the queue, but it is not a supported operation.

### `GET /api/v1/tasks/{id}/attempts`

The endpoint returns `200` with a wrapper,
`{ taskId, attempts: [ { attempt, workerId, startedAt, endedAt, outcome,
error } ] }`, ordered by attempt. An open attempt has a null `endedAt`
and a null `outcome`. A task with no attempts returns `attempts: []`. A
missing task is a `404` `ProblemDetail`, a bad id is `400`, and a
database error is the generic `500`. `GET /api/v1/tasks/{id}` does not
change. After a reap it shows `PENDING`, a null `workerId`, the same
`attempt`, and a null `leaseExpiresAt`.

### What a reaped worker sees

The old handler is not told directly. The reaper does not interrupt
handlers, cancel watches, or wake worker loops.

1. **Its next heartbeat tick.** `extendLease(id, A, n)` matches nothing,
   so the tick returns `false`, `leaseLost()` becomes true, and the watch
   closes.
2. **Its finish matches nothing.** `classifyFinish` returns one of:
   - `Rejected(PENDING, null)`: not claimed again yet;
   - `Rejected(RUNNING, B)`: worker B holds the task;
   - `Rejected(COMPLETED, B)` or `Rejected(FAILED, B)`: B has finished;
   - `Rejected(CANCELLED, null)`: the task was cancelled after the reap;
   - `Rejected(RUNNING | COMPLETED | FAILED, A)`: A's own id claimed it
     again as attempt `n + 1`. `AlreadyApplied` is impossible here,
     because it requires the same attempt.

Zero rows stays a value, as in ADR 0003. There is no
`LeaseLostException`.

### A handler `Error` is a failed attempt

M1 let every handler `Error` propagate, which left the row `RUNNING` and
killed the worker loop. With a reaper, that row is requeued, claimed
again first in line, and kills the next loop. M4 changes the rule:

- **One `Failed` finish:** an `Error` that is not a
  `VirtualMachineError` (for example `AssertionError`, `LinkageError`,
  or `NoClassDefFoundError`), and `StackOverflowError`. By the time the
  executor sees a `StackOverflowError`, the stack has already unwound.
  The reason uses the same format as `TaskExecutor.failureMessage` for
  exceptions: the class name, then `": "` and the message, or the class
  name alone when there is no message. The watch is still closed in
  `finally` before the finish. `FAILED` is terminal, and the loop
  survives.
- **Still propagates:** `OutOfMemoryError` and every other
  `VirtualMachineError` (`InternalError`, `UnknownError`). The loop dies
  and the row stays `RUNNING` until it is reaped, as in M1.

The existing failure messages for exceptions do not change.

### No crash-loop guard until M5

There is no attempt limit, no crash-loop guard, and no reaper-side
`FAILED` before M5. A task that kills its JVM, wedges its heartbeat, or
throws a `VirtualMachineError` other than `StackOverflowError` is
requeued and claimed again indefinitely, normally first in line. It is
visible through `attempt`, `/attempts`, and the reaper logs. The
operator's tool for it is the quarantine statement below.

A hung handler whose heartbeats still succeed is never reaped. There is
no wall-clock handler timeout.

### Operator procedures

**Requeue a stuck `RUNNING` task.** This replaces the M1 manual requeue
in ADR 0004. Expire the lease, pinning the observed owner and attempt:

```sql
UPDATE tasks SET lease_expires_at = now()
WHERE id = :id AND status = 'RUNNING' AND worker_id = :observedWorker AND attempt = :observedAttempt;
```

The next reap requeues the row and records `LEASE_EXPIRED`. A live
owner's next heartbeat can still keep the row, which is correct. The
old manual requeue left an open attempt row on a `PENDING` row, which no
engine path does, so it is retired.

**Quarantine a poison task.** Use this for a task that crash-loops: one
that kills its JVM, wedges its heartbeat, or throws a
`VirtualMachineError` on every attempt. It stays in the docs until M5:

```sql
UPDATE tasks
SET status = 'FAILED', finished_at = now(), updated_at = now(),
    error = 'quarantined by operator'
WHERE id = :id AND status IN ('PENDING', 'RUNNING');
```

`FAILED` is terminal and has no owner check, and `finished_at` is set,
so the row satisfies V4's checks. A zombie's later finish is rejected,
because the status is no longer `RUNNING`. The open attempt row, if
there is one, stays open. The statement does not clear `worker_id`,
`claimed_at`, or the lease of a `RUNNING` row.

M4.7 ran this statement verbatim once against a test database
(2026-11-13). On a claimed `RUNNING` row, the row became `FAILED` with
`finished_at` set and kept its worker, attempt, and lease. Every
`CHECK` on `tasks` and `task_attempts` held, the attempt row stayed
open, and a later `completeTask` from the old worker and attempt
returned `Rejected(FAILED, <old worker>)` and changed nothing. On a
`PENDING` row, the row became `FAILED` with no owner fields set. The
test was not kept.

### Tests

- **No `FaultInjection` class.** Crashes are simulated in-process: a
  heartbeat on a hand-fired scheduler that never fires, a handler blocked
  on a latch, and a recording `TaskService` subclass in test code that
  can throw once from `completeTask`. Production code gets no
  fault-injection hooks.
- **`RecoveryIntegrationTest`** (twelve cases) builds every executor by
  hand, each with its own hand-fired scheduler, and drives the reaper
  only through `reapOnce()`. Leases expire naturally on the database
  clock. It covers the three tested causes of recovery and the fence in
  every case listed under "What a reaped worker sees", each with its
  exact `Rejected` value and a byte-for-byte check that the row and its
  history did not change.
- **Both soak tests run in the default `verify`, with no tag.**
  `RecoverySoakTest` (a seeded single-threaded random walk over claims,
  heartbeats, finishes, deaths, zombies, expiries, and reaps, with a
  replayable SHA-256 of its trace) and `RecoveryChaosTest` (four worker
  slots and two reapers with seeded crashes) print the seed on failure.
  Together they took about 5.7 s inside `verify` on the development
  machine, far under the 60 s budget. "Deterministic" means no
  unreported randomness, and every assertion holds for every
  interleaving.
- **The 20-run gate passed (2026-11-13).** `RecoveryChaosTest` and
  `RecoveryIntegrationTest` ran 20 consecutive times with chaos seeds 1
  to 20, and every run passed. Full `verify` passed three times in a
  row.
- **Clock steps.** "Time passes" in the soak sets leases to fixed past
  timestamps, and `RecoveryIntegrationTest` retries a reap that returned
  zero after the lease was seen to expire, so a backward step of the
  database clock cannot fail them.

The other classes are `V6MigrationTest`, `TaskAttemptHistoryTest`,
`LeaseReapTest`, `ReaperLockTest`, `ConcurrentReaperTest`,
`ReaperPropertiesTest`, `LeaseReaperTest`,
`ReaperSchedulingIntegrationTest`, `RecordingTaskServiceTest`,
`TaskAttemptsApiTest`, and new cases in `TaskExecutorTest`,
`LeaseHeartbeatTest`, `WorkerPoolTest`, and `TaskControllerTest`. Every
Spring test context has the reaper off except
`ReaperSchedulingIntegrationTest`'s, which is closed after its class,
and `PostgresIntegrationTest` checks that before each test.

## Alternatives

- **Lazy reclaim in the claim query.** The claim, the hottest
  statement, would become an `OR` across two partial indexes. Recovery
  would happen only when some worker claims, so an API-only process
  would never recover anything, and M5 would have no single place for
  its policy. Rejected.
- **Leader election, advisory locks, or a "reaper lease" row.** Not
  needed. `SKIP LOCKED`, the re-check, and the history guard already make
  several reapers safe, and a leader adds a way for recovery to stop
  when the leader is gone. Rejected.
- **An expiry check on complete, fail, or extend.** An expired lease
  means eligible, not lost. The check would discard results that nobody
  else owns and cause more duplicates. Rejected.
- **`@Scheduled`.** It needs `@EnableScheduling`, which turns on
  application-wide scheduling. A bean with its own one-thread scheduler
  follows the `LeaseHeartbeat` pattern, controls its lifecycle phase, and
  can be driven by a hand-fired scheduler in tests. Rejected.
- **`LeaseLostException`.** Zero rows is a normal outcome (ADR 0003).
  A finish returns `Rejected`, and an extend returns `false` and sets
  `leaseLost()`. An exception would turn a value into control flow.
  Rejected.
- **`clock_timestamp()` in the reap, or one transaction across several
  batches.** `clock_timestamp()` is volatile. It cannot bound the index
  scan, and its value differs between rows and between the CTE and the
  outer `UPDATE` in one statement. That would not be unsafe, but it is
  harder to reason about for no gain. One transaction across batches
  would freeze `NOW()` at the first batch and hold every batch's locks
  until the end, so finishes and heartbeats would wait longer. Rejected.
- **Rethrowing every `Error` in the reaper, as the heartbeat does.** The
  scheduler catches what a periodic task throws and only cancels the
  schedule, so the rethrow reaches no handler. One `AssertionError`
  would silently end all recovery in a single-process deployment.
  Rejected.
- **Treating every handler `Error` as a failure, including
  `OutOfMemoryError`.** A `VirtualMachineError` says the JVM itself may
  no longer work correctly, and the cause may be other work in the same
  process. The engine does not try to record a result after one.
  Rejected. `StackOverflowError` is the exception, because the stack has
  unwound by the time the executor sees it.
- **A datasource `socketTimeout` and `tcpKeepAlive`.** These would bound
  a half-open connection, but they change every connection, including
  the heartbeat's and the API's, and a value has to be chosen against
  the longest legitimate statement. Declined for M4 (P3). The
  hung-connection limitation is documented instead, and pgjdbc
  `socketTimeout` and `tcpKeepAlive` are revisited for the RDS
  deployment in M15.
- **A reaper warm-up after a failed run** (skip one reap after a failed
  tick, so live owners re-extend first). Declined for M4 (P4). Outage
  duplicates are documented instead. A warm-up is an M14 candidate if
  the chaos tests show that outage duplicates matter.

## Consequences

- **At-least-once, scoped.** A task is requeued and run again when its
  attempt's lease stops being extended. That is the only at-least-once
  claim M4 makes.
  - **Tested causes** (`RecoveryIntegrationTest`): a stopped heartbeat
    with a handler that never returns (a crash or a pause), a handler
    interrupted at shutdown, and a finish write that did not commit.
    `RecoveryChaosTest` shows the first cause under concurrency.
  - **Argued from the same mechanism, not tested:** the process is killed
    after the claim commits. No test kills a JVM.
  - It holds when **all** of these are true: at least one process runs a
    reaper; at least one worker loop claims; and the handler does not
    take its worker down on every attempt. After M4, an ordinary handler
    `Error` is a `Failed` finish, but a `VirtualMachineError` other than
    `StackOverflowError` still kills the loop.
- **Concurrent duplicates.** A requeued task may run twice, possibly at
  the same time. A handler that ignores `leaseLost()`, or ignores the
  shutdown interrupt, keeps running after its row is reaped and can run
  alongside the new owner. Its finish is rejected. Its side effects
  outside the engine are not undone (ADR 0002, M6). Nothing stops a
  zombie.
- **Outage duplicates.** After a database outage or pool exhaustion
  longer than `engine.lease.duration`, every in-flight lease has
  expired. Reapers in every process race the heartbeats, and most
  in-flight tasks run twice, possibly at the same time. Hikari's default
  `connectionTimeout` (30 s) equals the default lease, so one exhausted
  pool can be enough. There is no protection against this in M4.
- **Crash loops.** A task that kills its JVM or wedges its heartbeat is
  requeued and claimed again indefinitely, normally first in line, until
  M5. **A task that throws a `VirtualMachineError` other than
  `StackOverflowError` on every attempt can still kill loops one after
  another until M5 adds `max_attempts`.** It is reaped, claimed again,
  and kills the next loop, until one process has lost every loop, and a
  restart does not help. Use the quarantine procedure.
- **Hung connections.** pgjdbc has no `socketTimeout` and no
  `tcpKeepAlive` configured, so a half-open TCP connection can hang the
  reaper or the heartbeat thread until the operating system gives up on
  it. The 5 s reap timeout does not cover it.
- **A dead reaper is only logged.** After a `VirtualMachineError`, the
  reaper stops until restart, and only the ERROR line shows it. In a
  single-process deployment that stops all recovery. M11 adds health.
- **Cancel after a reap.** A reaped row is `PENDING`, so cancel applies
  even though the old handler may still be running and causing side
  effects. `CANCELLED` no longer means "never ran", and cancel does not
  stop a running zombie. The attempt history shows the `LEASE_EXPIRED`
  attempt. A cancel that races an uncommitted reap gets a `409` that
  reports `PENDING`, and a retry can still be refused if a worker claims
  the task first.
- **Recovery latency is an estimate, not a bound.** With the defaults, at
  least one live and idle loop, and a live reaper, recovery takes
  roughly the lease (30 s) plus the reaper interval (10 s) plus
  `max-idle-interval` (30 s), about 70 seconds. Each of these adds to
  it: the drain time of earlier batches, because the delay is fixed; one
  extra interval when a batch is short; Hikari pool exhaustion, up to
  30 s per call; and an older `PENDING` backlog. No test establishes a
  limit on it, and the docs must not call it a guarantee.
- **First start after V6.** Legacy and stuck `RUNNING` rows are reaped
  and run again. Running M3 and M4 processes at the same time is
  untested. Apply V6 with the process stopped. Starting once with
  `engine.reaper.enabled=false` lets an operator inspect first.
- **Handlers must tolerate a second run,** possibly at the same time as
  the first. A finish is rejected once ownership has ended.
- **Not provided, and must not be claimed:**
  - running a task only once, or "effectively once" (idempotency is M6);
  - that a zombie stops;
  - that a `CANCELLED` task never ran, or that cancel stops a running
    zombie;
  - retries after `FAILED`, or a limit on attempts (M5);
  - detection of a hung handler whose heartbeats still succeed;
  - that an expired lease means another owner exists;
  - that a finish after expiry is rejected;
  - any time limit on recovery;
  - protection against duplicates after an outage longer than the lease;
  - that a successful heartbeat guarantees future ownership;
  - durability (the tests run PostgreSQL with `fsync=off`);
  - history for tasks that finished before V6.

## Current implementation versus planned

**Implemented today:** every item in "Decision".

- **V6** (M4.2) adds `tasks_attempt_nonnegative_check` and
  `task_attempts` exactly as above, and backfills one open row per
  `RUNNING` task. `TaskAttemptRepository` opens a row on claim
  (`recordStart`, `ON CONFLICT DO NOTHING`) and closes or creates one on
  an applied finish (`recordFinish`, binding `outcome.name()`).
  `TaskService.claimTask`, `completeTask`, and `failTask` call it in the
  same transaction and log a WARN when it returns `false`.
  `TaskService.getAttempts` returns the rows ordered by attempt.
- **The reap** (M4.3) is `TaskRepository.reapExpiredLeases`, the
  statement above, returning one `ReapedTask(taskId, attempt,
  previousWorkerId, attemptRecorded)` per row.
  `TaskService.reapExpiredLeases(batchSize)` is
  `@Transactional(timeout = 5)` and rejects a batch size below 1. The
  reap uses `idx_tasks_running_lease`.
- **`LeaseReaper`** (M4.4) is the bean described above: phase `0`, one
  daemon thread named `lease-reaper`, the tick error rules, the
  start-after-stop no-op, and pauseable. `ReaperProperties` binds
  `engine.reaper` with the defaults and bounds above, and
  `HandlerConfiguration` enables it. `application.yaml` has an
  `engine.reaper` block (`enabled: true`, `interval: 10s`,
  `batch-size: 100`) whose comment calls the latency an estimate, and
  its Hikari comment counts one connection each for the heartbeat and
  the reaper. The maximum is still 10, and Hikari's `connectionTimeout`
  keeps its default of 30 s.
- **`LeaseHeartbeat.isPauseable()`** returns `false` (M4.4). Nothing else
  in the class changed.
- **`TaskExecutor`** (M4.8) catches `Error` from the handler. A
  `VirtualMachineError` that is not a `StackOverflowError` is rethrown,
  so the loop dies and the row stays `RUNNING` until it is reaped. Any
  other `Error` is logged and is one fail write with the
  `failureMessage` format, for example
  `java.lang.AssertionError: handler error`. `failureMessage` takes a
  `Throwable`, and the messages for exceptions did not change.
- **`GET /api/v1/tasks/{id}/attempts`** (M4.5) returns
  `TaskAttemptsResponse`, as described above.
- **Tests** (M4.6a, M4.6b, and each sub-part's own classes), as listed
  under "Tests". `PostgresIntegrationTest` sets
  `engine.reaper.enabled=false` and checks before each test that the
  reaper is not running in a context that disables it.

The suite has 232 tests. On 2026-11-13, `.\mvnw.cmd -B verify` passed
with 0 failures.

**Not implemented, by decision:** an attempt limit or crash-loop guard
(M5), reaper health and metrics (M11), a datasource `socketTimeout` or
`tcpKeepAlive` (declined, revisited in M15), and a reaper warm-up after a
failed run (declined, an M14 candidate). The limitations those leave are
under "Consequences": the `VirtualMachineError` loop killer until M5
with the quarantine procedure, hung connections, and outage duplicates.

## Future implications

- M5 puts its policy (`max_attempts` and backoff) in the reap statement,
  the one place where an expired attempt is handled. `attempt` is
  already the attempt count, and `task_attempts` already records each
  one.
- M6 uses `(task_id, attempt)`, the history's primary key, to identify
  an attempt for idempotency.
- M11 adds reaper metrics and a health indicator, so a stopped reaper is
  visible without reading logs.
- M12 retention removes history with its tasks through
  `ON DELETE CASCADE`. The `/attempts` list is unbounded until M5 and
  M12.
- M14 may add a reaper warm-up if the chaos tests show that outage
  duplicates matter. M15 revisits pgjdbc `socketTimeout` and
  `tcpKeepAlive` for the RDS deployment.
- The phase argument depends on `WorkerPool.stop()` being synchronous.
  A change to an asynchronous stop must revisit the reaper's and the
  heartbeat's phases.
