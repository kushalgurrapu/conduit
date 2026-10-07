# Durable Workflow Execution Engine

A backend-focused workflow execution engine built to demonstrate
practical software engineering and distributed-systems concepts.

The long-term goal is to build a small but realistic platform that can
execute workflows reliably in the presence of concurrency, failures,
retries, scheduling, and multiple workers.

## Project Goals

This project is intentionally focused on backend engineering rather than
simply building a CRUD application.

The main concepts we will progressively implement are:

-   REST API design
-   relational database design
-   transactions
-   concurrent task claiming
-   worker pools
-   row locking
-   `FOR UPDATE SKIP LOCKED`
-   leases and heartbeats
-   crash recovery
-   fencing
-   retries and exponential backoff
-   idempotency
-   workflow state machines
-   scheduling
-   fairness
-   rate limiting
-   backpressure
-   observability
-   testing concurrent and failure-prone systems
-   containerization and deployment

The project should remain incremental. New infrastructure should only be
introduced when it solves a real problem in the current milestone.

## Technology Stack

### Current

-   Java 25
-   Spring Boot
-   Maven
-   PostgreSQL
-   Spring JDBC / `JdbcClient`
-   Flyway
-   Docker
-   Docker Compose (local PostgreSQL)
-   REST/HTTP
-   JUnit 5
-   Testcontainers (PostgreSQL 16)
-   GitHub Actions (`.github/workflows/ci.yml` runs `./mvnw verify`)

### Planned

-   Redis, if/when justified
-   React + TypeScript frontend
-   Docker Compose for the full stack (application, workers, observability)
-   OpenTelemetry
-   Prometheus/Grafana

Technologies such as Kafka, Kubernetes, Terraform, and cloud
infrastructure are intentionally deferred until there is a concrete
reason to introduce them.

## Current Architecture

The current request flow is:

``` text
HTTP Request
    |
    v
TaskController
    |
    v
TaskService
    |
    v
TaskRepository
    |
    v
PostgreSQL
```

### Controller

`TaskController` exposes the HTTP API. Claim, complete, and fail are not
HTTP operations. A worker calls the service for those.

``` text
POST /api/v1/tasks
GET  /api/v1/tasks/{id}
POST /api/v1/tasks/{id}/cancel
```

`POST /api/v1/tasks` creates a task and returns `201` with
`Location: /api/v1/tasks/{id}` and a `TaskResponse`. The body is
`taskType` plus a JSON `payload`. `taskType` must match
`^[A-Z][A-Z0-9_]{0,63}$`. A type that matches and has no handler is still
accepted. The worker fails that task later. JSON null and a NUL byte in
a key or a text value are `400`.

`GET /api/v1/tasks/{id}` returns the task, including `workerId` and
`claimedAt`, or `404`.

`POST /api/v1/tasks/{id}/cancel` returns `200` when the task is `PENDING`
or already `CANCELLED`, `409` when it is `RUNNING` or another terminal
status, and `404` when the id is missing.

Errors use RFC 9457 `ProblemDetail` (`application/problem+json`). A
database failure is a generic `500`. The response does not include the
exception text.

``` text
curl -sS -D - -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d "{\"taskType\":\"ECHO\",\"payload\":{\"message\":\"hello\"}}"

curl -sS http://localhost:8080/api/v1/tasks/{id}

curl -sS -X POST http://localhost:8080/api/v1/tasks/{id}/cancel
```

With the worker pool running, `ECHO` may already be `COMPLETED` by the
time `GET` runs. Cancel applies only while the task is still `PENDING`.

### Service

`TaskService` contains application-level logic and owns the transaction
boundary for claiming a task and for complete, fail, and cancel.

The important distinction is:

``` text
Controller
    Handles HTTP

Service
    Handles application logic and transactions

Repository
    Handles database access
```

### Repository

`TaskRepository` uses Spring's `JdbcClient` to communicate with
PostgreSQL.

It currently handles:

-   inserting tasks
-   atomically claiming pending tasks
-   completing or failing a running task for its owning worker
-   cancelling a pending task

## Task Model

Tasks currently have the following fields:

``` text
id          uuid
task_type   text, format-checked (for example NOOP)
status      TaskStatus in Java, text in the database
payload     jsonb, read back as text
result      jsonb, null until a task completes
error       text, null until a task fails
created_at  timestamptz
updated_at  timestamptz, set again on every write
claimed_at  timestamptz, null while the task is pending
finished_at timestamptz, set only when the task is terminal
worker_id   uuid, null while the task is pending
```

`TaskStatus` is `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, or
`CANCELLED`. `isTerminal()` is true for the last three. The enum does
not decide which transition is legal. Each write is one `UPDATE`, and
its `WHERE` clause is the rule:

``` text
claim    PENDING  -> RUNNING     for one worker
complete RUNNING -> COMPLETED    only that worker
fail     RUNNING -> FAILED       only that worker
cancel   PENDING -> CANCELLED
```

A task begins as `PENDING`. Claim moves it to `RUNNING` and records the
worker. Complete and fail then set `finished_at` and `updated_at`. Cancel
does the same, and only while the task is still pending. If the `UPDATE`
changes zero rows, the service reads the row once to name the outcome
(already applied, already cancelled, or rejected). That read does not
write again. The HTTP API does not expose complete, fail, or cancel yet.

Calling complete again with a different result does not overwrite the
first result. The second call is "already applied" when the same worker
already reached that status.

There is no engine path from `RUNNING` back to `PENDING`. An operator can
requeue by hand, and only by pinning the worker they observed. That
statement is not an API. It is safe for the engine's row. The handler may
already have run, and it may run again:

``` sql
UPDATE tasks
SET status = 'PENDING', worker_id = NULL, claimed_at = NULL,
    updated_at = now()
WHERE id = :id
  AND status = 'RUNNING'
  AND worker_id = :observedWorkerId;
```

`TaskExecutor.runOnce` is one claim, one handler call, and one finish
write. `WorkerPool` calls it from a fixed number of platform threads
(`engine.worker.concurrency`, default 4) when `engine.worker.enabled`
is true. Each loop has its own worker id for as long as it lives, and
the thread is named `host-pid-loopN`. An empty queue waits
`engine.worker.poll-interval` and then tries again. A loop does not
pick up a second task while one is still running.

On shutdown the pool stops claiming and waits
`engine.worker.shutdown-timeout` for work it already claimed. That wait
must be shorter than `spring.lifecycle.timeout-per-shutdown-phase`
(30 seconds unless changed). A task already claimed is still executed.
If a handler is still going when the wait ends, it is interrupted and
left `RUNNING` when it honors the interrupt. The timeout is not a limit
on how long a handler may run while the process is up. Hikari's
`maximum-pool-size` must be at least the worker concurrency plus
headroom for the API. A handler does not hold a connection. Nothing
checks that size at startup.

A caller of `runOnce` does three things:

1.  Claim one pending task, in a transaction.
2.  Run the handler for that `task_type` with no transaction and no
    database connection.
3.  Write the outcome once: complete, or fail. If that write throws, the
    task stays `RUNNING`. There is no second attempt.

Handlers are always registered: `NOOP` completes and stores JSON null,
`ECHO` stores the payload as the result, `FAIL` fails (a textual
`reason` field in the payload is the error, otherwise the error is
`failed`), and `SLEEP` sleeps for the payload's `millis` (at most 60
seconds) and then completes. An unknown type, a thrown exception, or a
null return is one fail write. An interrupt while the handler is still
running leaves the task `RUNNING` with the same worker. If the handler
already returned, that outcome is still written once. Startup fails if
two handlers use the same type, or if a type does not match the
`task_type` check.

The HTTP API is the controller section above. Further states may be
introduced later if they are justified by the workflow requirements.

## Database

PostgreSQL runs locally through Docker Compose (`docker-compose.yml`:
image `postgres:16-alpine`, database `durable_workflow`, port 5432, data
kept in the named volume `pgdata`).

Flyway migrations in `src/main/resources/db/migration` build the schema:

-   `V1` creates the `tasks` table and enables PostgreSQL's `pgcrypto`
    extension for UUID generation.
-   `V2` fixes the default on `claimed_at`.
-   `V3` converts `created_at` and `claimed_at` to `timestamptz`, adds
    `updated_at`, adds the `CHECK` constraint on `status`, and adds the
    partial index `idx_tasks_pending` on `created_at` for pending rows.
-   `V4` adds `task_type`, `result`, `error`, and `finished_at`. Existing
    rows become `task_type = 'NOOP'`, then that default is dropped. Pending
    rows that still have `claimed_at` or `worker_id` (possible after V1)
    have those cleared. Terminal rows get `finished_at`. Named checks then
    require: `task_type` matches `^[A-Z][A-Z0-9_]{0,63}$`; `finished_at` is
    set exactly when the status is terminal; `RUNNING` has an owner;
    `PENDING` has none. A `RUNNING` row with no owner is not repaired, so
    the migration fails. If a local Compose database was edited into that
    shape, `docker compose down -v` drops it. A `NOOP` row completes with
    a JSON null result when a worker claims it. With
    `engine.worker.enabled=true` (the default), the pool does that while
    the application is running. Tests set the flag to false so they can
    claim rows themselves.

The datasource is read from `DB_URL`, `DB_USERNAME`, and `DB_PASSWORD`.
The defaults match the Compose database, so the application runs without
setting them. Integration tests do not use these settings; they start a
temporary PostgreSQL container through Testcontainers.

The database is intentionally the source of truth for task state.

### Running locally

``` text
docker compose up -d
./mvnw spring-boot:run
./mvnw verify
```

Use `.\mvnw.cmd` on Windows. `verify` needs Docker running (for
Testcontainers) but does not need the Compose database.

`spring-boot:run` starts the worker pool. `POST /api/v1/tasks` with
`taskType` `ECHO`, `FAIL`, `SLEEP`, or `NOOP` is claimed by that pool.
A database error backs off (`engine.worker.error-backoff`) and the loop
keeps running. A crash after claim leaves the task `RUNNING`; the pool
does not pick that row up again.

## Concurrent Task Claiming

The first major distributed-systems problem implemented by the project
is safe concurrent task claiming.

A naive implementation could allow two workers to observe the same
`PENDING` task before either updates it.

The project instead uses PostgreSQL row locking:

``` sql
SELECT id
FROM tasks
WHERE status = 'PENDING'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED
```

The claim operation then changes the selected task to:

``` text
status     = RUNNING
claimed_at = current timestamp
updated_at = current timestamp (same now() as claimed_at)
worker_id  = claiming worker
```

The update also requires the row to still be `PENDING`. A null
`workerId` is rejected in Java before that write.

The repository uses PostgreSQL's `RETURNING` capability to return the
updated task.

The service method is transactional so that the database operation is
executed within a transaction boundary.

### Why `SKIP LOCKED`?

If Worker A has locked a pending task, Worker B should not wait for
Worker A to finish before looking for work.

Instead:

``` text
Worker A                    Worker B

lock Task 1
                            Task 1 is locked
                            skip Task 1
                            claim Task 2
```

This allows multiple workers to make progress concurrently.

### How this is tested

-   `ConcurrentTaskClaimTest` has many threads claim from a set of
    pending tasks at once and asserts that no task is claimed twice and
    every task is claimed. A claim that waited on a lock would also pass
    this test, so on its own it does not prove `SKIP LOCKED`.
-   `SkipLockedClaimTest` holds a row lock on the oldest pending task in
    a separate, uncommitted JDBC transaction and asserts that
    `claimTask` returns the next task (or `null` if every pending task is
    locked) instead of waiting. Removing `SKIP LOCKED` makes it fail
    because of a short `statement_timeout`.
-   `DurableWorkflowEngineApplicationTests` checks that the application
    context starts and the migrations apply to a fresh database.
-   `V4MigrationTest` migrates a second database only as far as V3, inserts
    a pending row that still has `claimed_at`, a running row with an owner,
    and a completed row with no `finished_at`, then migrates to V4 and
    checks the backfill and the constraint names.
-   `ClaimUpdatedAtTest` backdates `updated_at`, claims the task, and
    checks that `updated_at` moved to the same instant as `claimed_at`.
    It also checks that a null worker id is rejected and the row stays
    `PENDING`.
-   `GuardedTransitionTest` tries complete, fail, and cancel from every
    status. An applied write moves `updated_at` and `finished_at`. A
    rejected write leaves the row unchanged. A second complete with a
    different result is already applied. The documented requeue, pinned
    to the observed worker, lets a new worker finish and rejects the old
    one.
-   `TransitionLockHoldTest` holds an uncommitted claim. Cancel waits
    (`pg_blocking_pids`), then is rejected if that claim commits and
    applies if it rolls back. A second claim returns nothing while the
    first claim is still open, and the committed worker is still the
    first one.
-   `TaskExecutorTest` calls `runOnce` directly. Echo completes with the
    payload as the result, a failed outcome and a thrown exception and
    an unknown type each fail once, an empty queue returns false, the
    handler runs with no transaction and no borrowed connection, an
    interrupt during `SLEEP` leaves the task `RUNNING`, and a returned
    outcome is still written once if the thread is interrupted afterward.
-   `WorkerPoolTest` builds its own pool and stops it afterwards. The
    shared application pool stays off. Four loops drain fifty tasks with
    one handler call each. In-flight handlers stay within `concurrency`.
    Database errors do not kill a loop. Shutdown finishes a task already
    claimed, including one claimed but not yet executed, and does not
    claim anything new. A handler still blocked at
    `shutdown-timeout` is left `RUNNING` with its worker id. A row
    claimed by hand and never finished stays `RUNNING` while a later
    task completes.

All of the Spring tests extend `PostgresIntegrationTest`, which uses one
shared Testcontainers PostgreSQL container, deletes every task before
each test, and leaves workers disabled. The container turns `fsync` off
and sets short lock timeouts so the suite does not hang. That container
is not evidence that a crash is durable. `V4MigrationTest` uses the same
server and its own database, so it does not start the Spring context.

## Verified So Far

The current implementation has successfully demonstrated:

-   Spring Boot starts successfully
-   PostgreSQL runs through Docker
-   Flyway creates the database schema
-   tasks can be created, read, and cancelled through `/api/v1/tasks`
-   the application can communicate with PostgreSQL
-   a pending task can be claimed
-   a claimed task changes from `PENDING` to `RUNNING`
-   `claimed_at` and `updated_at` are set to the same timestamp
-   `worker_id` is populated
-   a null worker id does not claim, complete, or fail the task
-   complete and fail apply only for the owning worker, and they set
    `finished_at` and `updated_at` together
-   a second complete by that worker does not change the stored result
-   cancel applies only to a pending task, and cancelling twice does not
    write the row again
-   a cancel that loses the race to a committed claim does not apply
-   V4 applies to a fresh database and to a V3 database with a V1-shaped
    pending row
-   the updated task is returned to the caller
-   concurrent workers never claim the same task twice
    (`ConcurrentTaskClaimTest`)
-   a task locked by another transaction is skipped, not waited on
    (`SkipLockedClaimTest`)
-   all Flyway migrations apply to a fresh database
-   integration tests run against Testcontainers, with no local database
    required
-   a bounded worker pool claims, runs, and finishes tasks, and shutdown
    does not claim new work (`WorkerPoolTest`)

Example successful state transition:

``` text
Before:

status     = PENDING
claimed_at = NULL
worker_id  = NULL


After:

status     = RUNNING
claimed_at = <timestamp>
updated_at = <same timestamp>
worker_id  = <worker UUID>
```

## Current Milestone

**M0 --- Foundation hardening** is complete. See `PROJECT_ROADMAP.md`.

The basic task-claiming mechanism is complete, and its concurrency
behavior has been verified by tests. M0 made that base reproducible and
trustworthy before new behavior is added.

Done in M0:

-   package renamed to `com.kushal.workflow`
-   `V3__task_hardening.sql` (`timestamptz`, `updated_at`, status
    `CHECK`, partial index for pending tasks) and `Instant` in Java
-   one shared Testcontainers PostgreSQL instance for all integration
    tests
-   a deterministic test that fails if `SKIP LOCKED` is removed
-   `docker-compose.yml` and environment-based datasource settings
-   `.gitattributes` line-ending normalization
-   Maven project name and description
-   this documentation and the first architecture decision records
-   a GitHub Actions workflow that runs `./mvnw verify`

**M1 --- task lifecycle, API, and worker runtime** is the current
milestone. Roadmap M2 is merged into M1. The decisions are recorded in
ADR 0004 and in the roadmap's deviations table.

M1.1 is in place: `TaskStatus`, the wider `Task` record, and `V4`.
M1.2 is in place: complete, fail, and cancel are guarded updates, and
the service classifies a zero-row write instead of retrying it.
M1.3 is in place: `TaskExecutor.runOnce` claims a task, runs its
handler with no connection held, and writes the outcome once.
M1.4 is in place: `WorkerPool` runs that cycle on a fixed set of
platform threads and stops claiming before it interrupts a handler
that is still going. The HTTP API still cannot finish or cancel a task.

Leases and failure recovery stay in later milestones.

## Planned Development Order

The project should progress roughly in this order:

### Phase 1 --- Task claiming

-   [x] Spring Boot project
-   [x] PostgreSQL
-   [x] Flyway
-   [x] Task schema
-   [x] Create-task API
-   [x] Repository
-   [x] Service layer
-   [x] Transactional task claiming
-   [x] `FOR UPDATE SKIP LOCKED`
-   [x] Verify a task can be claimed
-   [x] Concurrent worker test
-   [x] Deterministic `SKIP LOCKED` test
-   [x] GitHub Actions workflow that runs `./mvnw verify`

### Phase 2 --- Workers

-   [ ] Worker abstraction
-   [ ] Worker loop
-   [ ] Multiple workers
-   [ ] Graceful shutdown
-   [ ] Concurrent integration tests

### Phase 3 --- Leases and failure recovery

-   [ ] Task lease expiration
-   [ ] Heartbeats
-   [ ] Worker crash simulation
-   [ ] Recovery of abandoned tasks
-   [ ] Prevent stale workers from modifying tasks
-   [ ] Fencing

### Phase 4 --- Retries and idempotency

-   [ ] Retry policy
-   [ ] Exponential backoff
-   [ ] Maximum attempts
-   [ ] Idempotency keys
-   [ ] Duplicate side-effect protection

Important principle:

> Do not claim exactly-once execution unless the implementation and
> external side effects actually justify that claim. The system should
> initially be designed around at-least-once execution with idempotent
> effects where appropriate.

### Phase 5 --- Workflow execution

-   [ ] Workflow definition
-   [ ] Workflow state machine
-   [ ] Sequential steps
-   [ ] Conditional steps
-   [ ] HTTP actions
-   [ ] Delay/wait steps
-   [ ] Run history

### Phase 6 --- Scheduling and reliability

-   [ ] Scheduled workflows
-   [ ] Fairness
-   [ ] Rate limiting
-   [ ] Backpressure
-   [ ] Priority
-   [ ] Concurrency limits

### Phase 7 --- Observability

-   [ ] Structured logging
-   [ ] Metrics
-   [ ] Tracing
-   [ ] OpenTelemetry
-   [ ] Prometheus/Grafana

### Phase 8 --- Frontend and deployment

-   [ ] React + TypeScript UI
-   [ ] Workflow/run dashboard
-   [ ] Docker Compose for the full stack (a PostgreSQL-only Compose file already exists)
-   [ ] CI/CD
-   [ ] Production deployment

## Development Philosophy

This project is primarily a learning and portfolio project, so
correctness and understanding are more important than maximizing the
number of technologies used.

### Prefer

-   small incremental changes
-   tests before major architectural changes
-   simple designs before distributed infrastructure
-   measurable concurrency behavior
-   explicit failure handling
-   clear separation of responsibilities
-   understanding why a technology is needed

### Avoid

-   unnecessary microservices
-   adding Kafka just for resume value
-   adding Kubernetes before deployment requires it
-   premature abstractions
-   claiming guarantees that have not been tested
-   replacing working simple designs with complex infrastructure

The project should initially remain close to a modular monolith with
separate worker processes where useful.

## AI-Assisted Development

AI tools are being used heavily for implementation, debugging, tests,
and refactoring.

However, the developer should personally understand the core concepts
that are important for interviews and system design discussions.

In particular, the developer should understand:

-   database transactions
-   row locking
-   `FOR UPDATE`
-   `SKIP LOCKED`
-   race conditions
-   leases
-   heartbeats
-   fencing
-   retries
-   idempotency
-   state machines
-   concurrency
-   failure recovery

AI-generated code should be reviewed and tested rather than accepted
blindly.

## Architecture Decision Records

Key design decisions are recorded in `docs/adr/`:

-   [0001 --- PostgreSQL as the task store and queue](docs/adr/0001-postgres-as-queue.md)
-   [0002 --- At-least-once execution](docs/adr/0002-at-least-once.md)
-   [0003 --- Fencing via the attempt counter](docs/adr/0003-fencing-via-attempt.md)
-   [0004 --- Task lifecycle and execution model](docs/adr/0004-task-lifecycle-and-execution-model.md)

ADRs 0002 and 0003 describe planned behavior, not what the code does
today. ADR 0004's "Current implementation versus planned" section matches
the code: the lifecycle writes, `TaskExecutor`, the worker pool, and the
`/api/v1/tasks` API are in place.

## Repository Structure

Current layout (abbreviated):

``` text
durable-workflow-engine/
├── README.md
├── PROJECT_CONTEXT.md
├── PROJECT_ROADMAP.md
├── docker-compose.yml
├── pom.xml
├── .github/workflows/ci.yml
├── docs/
│   └── adr/
└── src/
    ├── main/
    │   ├── java/com/kushal/workflow/
    │   │   ├── api/
    │   │   ├── task/
    │   │   └── worker/
    │   └── resources/
    │       └── db/migration/
    └── test/
        └── java/com/kushal/workflow/
            ├── api/
            ├── support/
            ├── task/
            └── worker/
```

The exact structure should evolve with the application rather than being
created all at once.

## Current Status

**Milestone: M0 complete. M1.1 through M1.5 are in place.**

The system can currently create a task, store it in PostgreSQL, safely
claim a pending task using PostgreSQL row locking, and complete, fail, or
cancel that task through the service. `TaskExecutor.runOnce` claims one
pending task, runs `NOOP`, `ECHO`, `FAIL`, or `SLEEP` with no connection
held, and writes the outcome once. `WorkerPool` does that on a fixed
number of platform threads and, on shutdown, stops claiming, finishes
work already claimed, and interrupts a handler that is still running
when `engine.worker.shutdown-timeout` ends. The `WHERE` clause of each
update is the lifecycle rule. Concurrent claiming, the cancel-versus-claim
race, the executor, and the pool are covered by integration tests that
run against Testcontainers. GitHub Actions runs `./mvnw verify`.

Not built yet: attempts, leases, heartbeats, recovery of abandoned
tasks, fencing, retries, and idempotency. A crash after claim, an
interrupt during the handler, or a finish write that throws leaves the
task `RUNNING`. ADR 0004 records the rest of the M1 model. The finish
guards, the executor, the pool, and `/api/v1/tasks` in that ADR match
the code. Docs and a curl demo that waits until a task finishes are M1.6.
