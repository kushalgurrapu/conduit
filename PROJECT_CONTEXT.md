Project:

Durable Workflow Execution Engine

Goal:

Build a production-oriented workflow execution engine demonstrating

distributed-systems/backend engineering concepts.

Primary stack:

- Java 25
- Spring Boot
- PostgreSQL
- Spring JDBC / JdbcClient
- Flyway
- Docker / Docker Compose (local PostgreSQL)
- Maven
- JUnit 5 and Testcontainers (integration tests)

Current architecture:

HTTP Controller

```
↓
```

Service

```
↓
```

Repository

```
↓
```

PostgreSQL

Task lifecycle:

Implemented:

-   PENDING → RUNNING (claim, which sets a lease and increments attempt)
-   RUNNING → RUNNING (extend the lease), only for the owning worker and
    attempt
-   RUNNING → COMPLETED or FAILED, and only for the owning worker and
    attempt
-   PENDING → CANCELLED, also after a reap, while the old handler may
    still run
-   RUNNING → PENDING (the reap), when lease_expires_at < NOW(). It
    clears worker_id, claimed_at, lease_expires_at, and
    last_heartbeat_at, keeps attempt, and closes the attempt row as
    LEASE_EXPIRED

A claimed task stays RUNNING until complete or fail, or until its lease
expires and a reaper requeues it. An expired lease makes the row
eligible for requeue. It does not end ownership: until the reap
commits, the worker can still extend, complete, or fail. The M1 manual
requeue is retired. The operator procedures (expire the lease, and
quarantine a poison task) are in the README and ADR 0005, and are not
an API.

Current implementation:

- POST /api/v1/tasks creates a task. The body is taskType plus a JSON
  payload. taskType is format-checked only. GET /api/v1/tasks/{id} returns
  the task. POST /api/v1/tasks/{id}/cancel returns 200 for PENDING or
  already CANCELLED, 409 for RUNNING or another terminal status, and 404
  when the id is missing. Errors are ProblemDetail. There is no HTTP
  claim, complete, or fail. TaskResponse includes workerId, attempt, and
  leaseExpiresAt. It does not include lastHeartbeatAt.
  GET /api/v1/tasks/{id}/attempts returns { taskId, attempts } ordered by
  attempt, or 404 for a missing id
- TaskRepository and TaskAttemptRepository use JdbcClient
- TaskService owns transaction boundaries, including the reap. Claim and
  an applied complete or fail write task_attempts in the same
  transaction. tasks is the source of truth: a history write that
  cannot be made is logged at WARN and the tasks change commits
- Task claiming uses FOR UPDATE SKIP LOCKED
- The claim update re-checks status PENDING, and sets worker_id, claimed_at,
  and updated_at (claimed_at and updated_at share the same now()). It also
  sets lease_expires_at = NOW() + (:leaseMicros * INTERVAL '1 microsecond')
  from engine.lease.duration, sets attempt = attempt + 1, and clears
  last_heartbeat_at. Nothing else increments attempt
- A null worker id is rejected before the claim write
- Claim returns the updated Task, or null when no pending row is available
- Task.status is a TaskStatus enum. isTerminal() is the only method.
  Transitions are not decided in Java
- extendLease is one UPDATE guarded by id, RUNNING, worker_id, and
  attempt. It moves lease_expires_at and sets last_heartbeat_at, and
  returns false when it matches nothing. A lease already in the past
  still extends. It does not change updated_at
- complete, fail, and cancel are one UPDATE each. The WHERE clause is
  the transition. Complete and fail also require the owning worker_id
  and the attempt. They do not require an unexpired lease.
  Zero rows is classified (applied, already applied, rejected, cancelled,
  already cancelled, or not cancellable) and is not retried. Already
  applied means this worker and this attempt; a different attempt is
  rejected. A missing id
  on cancel throws TaskNotFoundException. A null worker id is rejected
  before complete or fail. Fail strips NUL bytes in Java and truncates
  the error to 4000 characters in SQL
- Schema is managed by Flyway V1-V6: timestamptz columns, task_type, result,
  error, finished_at, updated_at, attempt, lease_expires_at,
  last_heartbeat_at, a CHECK constraint on status, named checks for
  task_type format, finished_at, owner fields, and a non-negative
  attempt, a partial index on pending tasks, and a partial index on
  running leases (idx_tasks_running_lease, used by the reap). V5
  backfills a RUNNING row's lease from claimed_at and replaces
  tasks_running_has_owner_check so RUNNING also requires
  lease_expires_at. V6 adds tasks_attempt_nonnegative_check and the
  task_attempts table (primary key (task_id, attempt), ON DELETE
  CASCADE), backfills one open attempt per RUNNING row, and fails on a
  negative attempt. Apply V5 and V6 with the process stopped
- The reap (TaskService.reapExpiredLeases) is one statement in its own
  transaction with a 5 s timeout: FOR UPDATE SKIP LOCKED over expired
  RUNNING rows, then back to PENDING, with the attempt closed as
  LEASE_EXPIRED. LeaseReaper runs it from one daemon thread at phase 0,
  every engine.reaper.interval (default 10s, at least 100 ms), in
  batches of engine.reaper.batch-size (default 100, 1 to 1000). It is
  independent of engine.worker.enabled. It survives an Exception and an
  Error that is not a VirtualMachineError; a VirtualMachineError stops
  reaping in that process until restart. No leader election
- Datasource is configured with DB_URL, DB_USERNAME and DB_PASSWORD;
  defaults match the PostgreSQL in docker-compose.yml

TaskExecutor.runOnce claims one pending task, runs the handler for its
task_type with no transaction and no pooled connection, and writes the
outcome once. NOOP, ECHO, FAIL, and SLEEP are always registered. SLEEP
uses the payload's millis and caps it at 60 seconds. An unknown type, a
thrown exception, or a null return is one fail write. A handler Error
that is not a VirtualMachineError, and StackOverflowError, are one fail
write too (class name, then ": " and the message). Any other
VirtualMachineError propagates, kills that loop, and leaves the task
RUNNING until it is reaped. An interrupt during the handler leaves the
task RUNNING until it is reaped. A returned outcome is still written
once. A finish write that throws is logged and not retried; the task
stays RUNNING until it is reaped. Startup fails on a duplicate handler
type or a type that fails the task_type check.

WorkerPool, when engine.worker.enabled is true, calls runOnce from a
fixed number of platform threads (engine.worker.concurrency, default 4).
Each loop has one worker id. An empty queue waits a delay that starts at
poll-interval and doubles up to max-idle-interval, with equal jitter.
A claim resets that delay. Shutdown stops new claims, finishes a task
already claimed, and interrupts a handler still running at
shutdown-timeout. That timeout must be shorter than
spring.lifecycle.timeout-per-shutdown-phase. It is not a handler
timeout while the process is up. An interrupt leaves the task RUNNING
until it is reaped. A loop that exits from an Error stays dead;
/actuator/health then reports the workerPool component DOWN. Hikari
maximum-pool-size must be at least concurrency plus API headroom plus
one for the heartbeat thread plus one for the reaper thread. Tests set
engine.worker.enabled=false and build their own pool.

LeaseHeartbeat extends the lease of every in-flight task from one
scheduler thread, with one fixed-delay tick per task every
engine.lease.heartbeat-interval. Defaults: engine.lease.duration 30s,
heartbeat-interval 10s. Startup rejects a value under one microsecond
and an interval that is not shorter than the duration. Heartbeat every
lease/3 is a heuristic, not a proved constant. A false extend while the
watch is open sets TaskContext.leaseLost() and stops that task's ticks.
It is a request the handler may ignore. The built-in handlers do not
check it. A data-access error from a tick is not lease loss; the next
tick tries again. The executor closes the watch on every exit from
execute, before the finish write, so a finished task's late tick does
not flag loss. A returned outcome is still finished once with the
claimed attempt. LeaseHeartbeat's lifecycle phase is -1, so it stops
after WorkerPool, and its isPauseable() is false, so a paused test
context keeps its heartbeats.

A reaped worker's next heartbeat is rejected and sets leaseLost(). Its
finish gets Rejected with the row's current status and worker:
PENDING before a new claim, RUNNING or the finished status after one
(also when the new claim is by the same worker id), or CANCELLED after
a cancel. The reaper does not interrupt handlers.

At-least-once is claimed only as scoped in ADR 0005 and the README: a
task is requeued and run again when its attempt's lease stops being
extended. Tested causes: a stopped heartbeat with a handler that never
returns, a handler interrupted at shutdown, and a finish write that did
not commit. A process killed after the claim is argued, not tested. It
holds only while some process runs a reaper, some loop claims, and the
handler does not take its worker down on every attempt.

Limits: one slow JDBC call on the heartbeat thread delays every tick.
A GC pause can let a lease expire while the handler is alive. Recovery
latency is an estimate (about 70 s with the defaults), not a bound. A
requeued task may run twice, possibly at the same time, and nothing
stops a zombie. A CANCELLED task may have run. Until M5, a task that
kills its JVM, wedges its heartbeat, or throws a VirtualMachineError
other than StackOverflowError on every attempt is requeued
indefinitely and can kill loops one after another; use the quarantine
statement. After an outage longer than the lease, most in-flight tasks
run twice. No datasource socketTimeout or tcpKeepAlive is set, so a
half-open connection can hang the reaper or heartbeat thread. A dead
reaper is only logged.

Not implemented yet: retries, an attempt limit, idempotency, and
workflow definitions. GitHub Actions runs `./mvnw verify`.

Verified:

- PostgreSQL task creation works
- Flyway migrations apply to a fresh database
- A task can be successfully claimed
- Claimed task changes from PENDING → RUNNING
- claimed_at, updated_at, and worker_id are populated on claim
- Concurrent workers do not claim the same task (ConcurrentTaskClaimTest)
- A task locked by another transaction is skipped, not waited on (SkipLockedClaimTest)
- Complete and fail apply only for the owning worker. A rejected write leaves
  the row unchanged. Cancel applies only to a pending task
  (GuardedTransitionTest, TransitionLockHoldTest)
- TaskExecutor.runOnce completes ECHO and NOOP, fails FAIL, a thrown
  exception, an unknown type, and a null return, and does not write when
  the queue is empty (TaskExecutorTest)
- A handler runs with no transaction and no borrowed connection
- An interrupt during SLEEP leaves the task RUNNING with the same worker.
  A returned outcome is still completed or failed once
- All integration tests share one Testcontainers PostgreSQL instance and
  do not need a local database
- WorkerPool drains tasks on four loops with one handler call each, keeps
  in-flight work within concurrency, survives database errors, finishes
  claimed work on shutdown without claiming more, leaves a handler
  interrupted at the deadline RUNNING, and does not finish a crashed
  claim (WorkerPoolTest)
- Idle backoff doubles until max-idle-interval and resets after a claim
  (IdleBackoffTest, WorkerPoolTest). Four loops complete 1,000 tasks
  once each (WorkerPoolTest)
- Worker-pool health is UP when workers are disabled, and UP when the
  pool is running, no loop has exited unexpectedly, and liveLoops equals
  concurrency. That equality is worker-loop readiness only: the loops
  are inside run, including idle waits and database backoff. It does not
  mean a handler is making progress, that the database is reachable, or
  that tasks are being processed. The component is OUT_OF_SERVICE when
  the pool is stopped or stopping, and when it is running with fewer
  live loops than concurrency and no unexpected exit. It is DOWN when a
  loop dies while the pool is still started (WorkerPoolTest,
  WorkerPoolHealthIndicatorTest)
- POST /api/v1/tasks returns 201 and Location. GET returns the task.
  Cancel of a pending task returns 200, cancel again stays 200, and
  cancel of a running task returns 409 and leaves the row RUNNING
  (TaskControllerTest, TaskApiTest). A type with no handler is accepted.
  A bad type, JSON null, a NUL byte, and unreadable JSON are 400
  ProblemDetail. A missing id is 404. A database error is a generic 500
- V5 applies to a V4 database: attempt 0, a running lease backfilled
  from claimed_at, one replaced owner check, idx_tasks_running_lease,
  and a RUNNING insert with no lease fails (V5MigrationTest)
- Claim sets the lease from the configured duration and takes attempt
  from 0 to 1 (ClaimUpdatedAtTest). Lease settings validate
  (LeasePropertiesTest)
- extendLease, complete, and fail require the worker and the attempt.
  A past lease still extends. The same worker with another attempt
  cannot extend, complete, or fail a row that is still RUNNING. A
  test-only requeue and re-claim by the same worker rejects the old
  attempt. Only claim changes attempt (LeaseExtensionTest)
- The heartbeat tick, lease loss, and closing the watch on every exit
  from execute, on a hand-fired scheduler (LeaseHeartbeatTest)
- With a 1s lease, heartbeats keep a task RUNNING for three durations.
  Without heartbeats, and with the reaper off, the lease expires and the
  task stays RUNNING. A handler that ignores leaseLost() still finishes,
  and the write matches nothing after attempt changed
  (LeaseHeartbeatIntegrationTest)
- GET shows attempt and leaseExpiresAt, not lastHeartbeatAt. Cancel of
  RUNNING stays 409 and leaves the lease (TaskApiTest, TaskControllerTest)
- V6 applies to a V5 database, backfills RUNNING rows only, and fails on
  a negative attempt (V6MigrationTest). V5MigrationTest stops at V5
- A claim opens one attempt row, an applied finish closes it in the same
  transaction, and rejected finishes, cancel, and rollbacks write no
  history (TaskAttemptHistoryTest)
- The reap requeues expired RUNNING rows, keeps attempt, records
  LEASE_EXPIRED, and leaves PENDING, terminal, and unexpired rows alone,
  including a lease at NOW(). The expire-the-lease procedure works
  verbatim (LeaseReapTest). Locked rows are skipped without waiting,
  waiting writers are rejected after a reap, cancel does not wait, and
  the 5 s timeout fires (ReaperLockTest). Three reapers requeue each of
  1,000 tasks exactly once (ConcurrentReaperTest)
- The reaper bean's tick, error rules, lifecycle, and settings
  (LeaseReaperTest, ReaperPropertiesTest), and the scheduled reaper
  against PostgreSQL (ReaperSchedulingIntegrationTest)
- A handler Error that is not a VirtualMachineError, and
  StackOverflowError, are one fail write and the loop survives; an
  OutOfMemoryError propagates and leaves the row RUNNING
  (TaskExecutorTest, LeaseHeartbeatTest, WorkerPoolTest)
- GET /attempts, including after a reap and a cancel
  (TaskAttemptsApiTest, TaskControllerTest)
- A crashed, interrupted, or failed-finish attempt is reaped and
  completed by a later attempt. The old attempt's extend and finish are
  rejected with the exact Rejected value in every case (not claimed
  again, held by another worker, finished, cancelled, or claimed again
  by the same worker id) and change nothing (RecoveryIntegrationTest)
- A seeded random walk keeps the fence and the history and ends with
  every task COMPLETED (RecoverySoakTest). Seeded crashes under
  concurrency end with every task completed once, from its last attempt
  (RecoveryChaosTest; passed 20 runs, chaos seeds 1 to 20)
- `.\mvnw.cmd -B verify`: 232 tests, 0 failures (2026-11-13)

Source of truth:

- Code and tests override every document.
- Milestone order and the next milestone: `PROJECT_ROADMAP.md`, section
  "Current state" and its milestone table. Section 0 of that file is the
  2026-10-02 audit. It still mentions `POST /tasks` and a placeholder
  `TaskWorker`. Those are gone.
- Lifecycle rules: `docs/adr/0004-task-lifecycle-and-execution-model.md`.
  Lease recovery: `docs/adr/0005-lease-recovery-with-a-reaper.md`.
- `.plans/` is gitignored. The M1, M3, and M4 plans there are finished
  historical material. A fresh clone will not contain them. Do not
  implement from them.

Current milestone:

M0 is complete, including GitHub Actions CI.

M1 is complete, including former roadmap M2 (the worker runtime) and the
later idle-backoff and `workerPool` health work. Do not rebuild the task
API, the executor, the pool, idle backoff, or the health indicator.

M3, leases and heartbeats, is complete (M3.1 through M3.5). V5 adds
`attempt`, `lease_expires_at`, and `last_heartbeat_at`. Claim sets the
lease and increments `attempt`. `extendLease`, complete, and fail
require the attempt. `TaskResponse` has `attempt` and `leaseExpiresAt`.
`LeaseHeartbeat` extends leases while handlers run and sets
`TaskContext.leaseLost()` when an extension is rejected. Do not rebuild
any of that. The deviations from the original roadmap text are in the
M3 deviations table in `PROJECT_ROADMAP.md`.

M4, crash recovery and fencing, is complete (M4.1 through M4.8). V6
adds `task_attempts` and `tasks_attempt_nonnegative_check`. The reap
statement and `LeaseReaper` return expired `RUNNING` rows to `PENDING`.
`GET /api/v1/tasks/{id}/attempts` returns the history. A handler `Error`
that is not a `VirtualMachineError`, and `StackOverflowError`, are one
failed finish. `LeaseHeartbeat` is not pauseable. `RecoveryIntegrationTest`,
`RecoverySoakTest`, and `RecoveryChaosTest` show recovery and the fence
against a later claim. Do not rebuild any of that. ADR 0005 is the
decision record, and the deviations are in the M4 deviations table in
`PROJECT_ROADMAP.md`.

Current milestone is M5, retries, exponential backoff, and
dead-lettering. It has not started. M5 puts its expiry policy
(`max_attempts`) in the reap statement, the one place where an expired
attempt is handled. The next migration is V7. Do not edit V1–V6.

Still not in the tree: retries, `available_at`, `max_attempts`, a
crash-loop guard, idempotency, workflows, a wall-clock handler timeout,
reaper health and metrics, or changing cancel so a `RUNNING` task
succeeds. Cancel of `RUNNING` stays `409`.

The M5 section of `PROJECT_ROADMAP.md` is the starting point. Re-inspect
the tree first; code and tests override it.

Numbering was not changed in the 2026-10-12 reconciliation. From the
first committed roadmap, M2 was the worker runtime (later merged into
M1), M3 was leases and heartbeats, and M4 was crash recovery and fencing.
No committed roadmap defines M2 as leases. README phase headings are not
milestone ids.

Later, after M5: idempotency (M6), workflow runs (M7), then the rest of
`PROJECT_ROADMAP.md`. Do not start those inside M5.

Design decisions are recorded in `docs/adr/`. ADRs 0002 and 0003 are
accepted. Their current-implementation notes match the code: leases,
heartbeats, the attempt fence, recovery by the reaper, and the zombie
tests exist, and at-least-once holds only as scoped in ADR 0005. ADR
0004's current-implementation section matches the code, including idle
backoff, `workerPool` health, the M3 lease and heartbeat behavior, and
the M4 changes. ADR 0005 (lease recovery with a reaper) is accepted and
implemented.

Packages:

- `api`: `TaskController`, `CreateTaskRequest`, `TaskResponse`,
  `TaskAttemptResponse`, `TaskAttemptsResponse`, `TaskPayloadValidator`,
  `ApiExceptionHandler`, `IllegalTaskTransitionException`
- `task`: `Task`, `TaskStatus`, `TaskService`, `TaskRepository`,
  `TaskAttempt`, `TaskAttemptRepository`, `AttemptOutcome`,
  `ReapedTask`, `TransitionResult`, `CancelResult`,
  `TaskNotFoundException`, `LeaseProperties`
- `worker`: `WorkerPool`, `WorkerProperties`, `IdleBackoff`,
  `WorkerPoolHealthIndicator`, `TaskExecutor`, `LeaseHeartbeat`,
  `LeaseReaper`, `ReaperProperties`, `HandlerConfiguration`,
  `TaskHandler`, `TaskContext`, `TaskOutcome`, and the `NOOP`, `ECHO`,
  `FAIL`, and `SLEEP` handlers

Inspect those, plus `application.yaml` and `src/main/resources/db/migration`,
before changing behavior. There is one controller. Claim, complete, and
fail are not HTTP operations.

Tests:

```text
.\mvnw.cmd -B verify
```

Use `./mvnw -B verify` off Windows. Docker must be running for
Testcontainers. The Compose database is not required. Spring tests extend
`PostgresIntegrationTest`: one shared PostgreSQL container, workers and
the reaper disabled, `DELETE FROM tasks` before each test, and a check
before each test that the reaper is not running unless the context
enables it. `V4MigrationTest`, `V5MigrationTest`, and `V6MigrationTest`
use their own databases and do not start the Spring context.
`LeaseHeartbeatIntegrationTest` has its own Spring context with a 1s
lease. The recovery tests extend `RecoveryContextTest` (1s lease, 300ms
heartbeat), which forbids subclass configuration, imports, mock beans,
and properties so the context stays shared. `RecoverySoakTest` has its
own context with a 1h lease. Only `ReaperSchedulingIntegrationTest`
enables the reaper, and `@DirtiesContext` closes its context after the
class. Do not put a short lease on the shared context. Do not turn the
shared application pool or the reaper on inside those tests. A new
public `TaskService` method needs an override in `RecordingTaskService`,
or `RecordingTaskServiceTest` fails.

A worker-loop exception that is not a database or transaction failure is
logged and the loop continues with no `error-backoff`. That is current
behavior. It did not block M4, and it does not block M5. Do not widen
M5 to "fix" it.



## AI Development Rule

Before making architectural or non-trivial implementation changes, explain

the proposed change, identify the relevant files, explain why the change is

needed, and wait for approval when the change could materially affect the

architecture.

For routine implementation tasks, inspect the existing code before editing it.

Do not introduce new technologies, dependencies, services, or abstractions

unless they are justified by the current milestone.

The developer is using this project to learn backend engineering and

distributed systems. Explanations should prioritize understanding over

simply generating code.
