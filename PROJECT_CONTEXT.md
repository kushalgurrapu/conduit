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

-   PENDING → RUNNING (claim)
-   RUNNING → COMPLETED or FAILED, and only for the owning worker
-   PENDING → CANCELLED

A claimed task stays RUNNING until complete or fail is called. Nothing
in the engine puts a running task back to PENDING. The manual requeue
statement in ADR 0004 is documentation and a test, not an API.

Current implementation:

- POST /api/v1/tasks creates a task. The body is taskType plus a JSON
  payload. taskType is format-checked only. GET /api/v1/tasks/{id} returns
  the task. POST /api/v1/tasks/{id}/cancel returns 200 for PENDING or
  already CANCELLED, 409 for RUNNING or another terminal status, and 404
  when the id is missing. Errors are ProblemDetail. There is no HTTP
  claim, complete, or fail
- TaskRepository uses JdbcClient
- TaskService owns transaction boundaries
- Task claiming uses FOR UPDATE SKIP LOCKED
- The claim update re-checks status PENDING, and sets worker_id, claimed_at,
  and updated_at (claimed_at and updated_at share the same now())
- A null worker id is rejected before the claim write
- Claim returns the updated Task, or null when no pending row is available
- Task.status is a TaskStatus enum. isTerminal() is the only method.
  Transitions are not decided in Java
- complete, fail, and cancel are one UPDATE each. The WHERE clause is
  the transition. Complete and fail also require the owning worker_id.
  Zero rows is classified (applied, already applied, rejected, cancelled,
  already cancelled, or not cancellable) and is not retried. A missing id
  on cancel throws TaskNotFoundException. A null worker id is rejected
  before complete or fail. Fail strips NUL bytes in Java and truncates
  the error to 4000 characters in SQL
- Schema is managed by Flyway V1-V4: timestamptz columns, task_type, result,
  error, finished_at, updated_at, a CHECK constraint on status, named checks
  for task_type format, finished_at, and owner fields, and a partial index
  on pending tasks
- Datasource is configured with DB_URL, DB_USERNAME and DB_PASSWORD;
  defaults match the PostgreSQL in docker-compose.yml

TaskExecutor.runOnce claims one pending task, runs the handler for its
task_type with no transaction and no pooled connection, and writes the
outcome once. NOOP, ECHO, FAIL, and SLEEP are always registered. SLEEP
uses the payload's millis and caps it at 60 seconds. An unknown type, a
thrown exception, or a null return is one fail write. An interrupt during
the handler leaves the task RUNNING. A returned outcome is still written
once. A finish write that throws is logged and not retried. Startup fails
on a duplicate handler type or a type that fails the task_type check.

WorkerPool, when engine.worker.enabled is true, calls runOnce from a
fixed number of platform threads (engine.worker.concurrency, default 4).
Each loop has one worker id. An empty queue waits a delay that starts at
poll-interval and doubles up to max-idle-interval, with equal jitter.
A claim resets that delay. Shutdown stops new claims, finishes a task
already claimed, and interrupts a handler still running at
shutdown-timeout. That timeout must be shorter than
spring.lifecycle.timeout-per-shutdown-phase. It is not a handler
timeout while the process is up. An interrupt leaves the task RUNNING.
A loop that exits from an Error stays dead; /actuator/health then
reports the workerPool component DOWN. Hikari maximum-pool-size must
be at least concurrency plus API headroom. Tests set
engine.worker.enabled=false and build their own pool.

Not implemented yet: attempt counter, leases, heartbeats, crash recovery,
fencing, retries, idempotency, and workflow definitions. GitHub Actions
runs `./mvnw verify`.

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

Source of truth:

- Code and tests override every document.
- Milestone order and the next milestone: `PROJECT_ROADMAP.md`, section
  "Current state (verified 2026-10-12)". Section 0 of that file is the
  2026-10-02 audit. It still mentions `POST /tasks` and a placeholder
  `TaskWorker`. Those are gone.
- Lifecycle rules: `docs/adr/0004-task-lifecycle-and-execution-model.md`.
- `.plans/` is gitignored. The M1 plan there is finished historical
  material. A fresh clone will not contain it. Do not implement from it.

Current milestone:

M0 is complete, including GitHub Actions CI.

M1 is complete, including former roadmap M2 (the worker runtime) and the
later idle-backoff and `workerPool` health work. Do not rebuild the task
API, the executor, the pool, idle backoff, or the health indicator.

Next milestone is M3, leases and heartbeats. It is not started. There is
no `attempt` column, no `lease_expires_at`, and no heartbeat.

M3 includes: set a lease and increment `attempt` on claim; extend the
lease for the owning worker and attempt; abort the handler in-process
when an extension fails; put `attempt` on `completeTask` and `failTask`;
replace `tasks_running_has_owner_check` with one check that also requires
the lease; add `attempt` and `leaseExpiresAt` to `TaskResponse`. The next
migration is V5. Do not edit V1–V4.

M3 does not include: the reaper, any engine path from `RUNNING` back to
`PENDING`, crash or zombie tests, retries, idempotency, workflows, a
wall-clock handler timeout, or changing cancel so a `RUNNING` task
succeeds. Cancel of `RUNNING` stays `409`. Requiring
`lease_expires_at > now()` on complete or fail is M4. At-least-once
recovery is M4 (ADR 0002). The attempt fence is introduced in M3 and
proved against a later claim in M4 (ADR 0003).

Acceptance criteria and the explicit out-of-scope list are in the M3
"Start here" block in `PROJECT_ROADMAP.md`.

Numbering was not changed in the 2026-10-12 reconciliation. From the
first committed roadmap, M2 was the worker runtime (later merged into
M1), M3 was leases and heartbeats, and M4 was crash recovery and fencing.
No committed roadmap defines M2 as leases. README phase headings are not
milestone ids.

Later, after M3 and M4: retries (M5), idempotency (M6), workflow runs
(M7), then the rest of `PROJECT_ROADMAP.md`. Do not start those inside M3.

Design decisions are recorded in `docs/adr/`. ADRs 0002 and 0003 are
accepted targets. Their current-implementation notes match the code:
finish writes exist, and recovery and the attempt fence do not. ADR
0004's current-implementation section matches the code, including idle
backoff and `workerPool` health.

Packages:

- `api`: `TaskController`, `CreateTaskRequest`, `TaskResponse`,
  `TaskPayloadValidator`, `ApiExceptionHandler`,
  `IllegalTaskTransitionException`
- `task`: `Task`, `TaskStatus`, `TaskService`, `TaskRepository`,
  `TransitionResult`, `CancelResult`, `TaskNotFoundException`
- `worker`: `WorkerPool`, `WorkerProperties`, `IdleBackoff`,
  `WorkerPoolHealthIndicator`, `TaskExecutor`, `HandlerConfiguration`,
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
`PostgresIntegrationTest`: one shared PostgreSQL container, workers
disabled, `DELETE FROM tasks` before each test. `V4MigrationTest` uses
its own database and does not start the Spring context. Do not turn the
shared application pool on inside those tests.

A worker-loop exception that is not a database or transaction failure is
logged and the loop continues with no `error-backoff`. That is current
behavior. It does not block M3, and M3 should not be widened to "fix" it.



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
