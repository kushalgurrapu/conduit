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

- POST /tasks creates a task. The body is the payload. The task type is
  stored as NOOP until the versioned API accepts a type
- POST /tasks/claim is a temporary test endpoint that claims with a random worker id
- TaskRepository uses JdbcClient
- TaskService owns transaction boundaries
- Task claiming uses FOR UPDATE SKIP LOCKED
- The claim update re-checks status PENDING, and sets worker_id, claimed_at,
  and updated_at (claimed_at and updated_at share the same now())
- A null worker id is rejected before the claim write
- Claim returns the updated Task
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

Not implemented yet: the worker loop, the versioned REST API, attempt
counter, leases, heartbeats, crash recovery, fencing, retries, idempotency,
and workflow definitions. TaskWorker is a placeholder that prints a line.
Nothing calls runOnce when the application starts. The temporary POST /tasks
and POST /tasks/claim endpoints are still the HTTP API. GitHub Actions runs
`./mvnw verify`.

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

Current milestone:

M0 (foundation hardening) is complete, including GitHub Actions CI.
See PROJECT_ROADMAP.md.

M1 is the current milestone: task lifecycle, the REST API, and the worker
runtime. Roadmap M2 is merged into M1. Those decisions are recorded in
ADR 0004 and in the roadmap deviations table. M1.1, M1.2, and M1.3 are
implemented: the task record, TaskStatus, Flyway V4, guarded complete,
fail, and cancel, and TaskExecutor (claim, handler, one finish write).
The worker pool and the versioned API are not.

After M1:

- leases
- crash recovery
- fencing
- retries
- idempotency
- workflow state machine
- scheduling/fairness
- observability

Design decisions are recorded in docs/adr/. ADRs 0002 (at-least-once) and
0003 (fencing via attempt) describe planned behavior, not current code.
ADR 0004's current-implementation section matches the code through the
executor. The pool and the versioned API in that ADR are not built.



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
