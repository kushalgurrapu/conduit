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

`TaskController` exposes the HTTP API.

Current endpoints:

``` text
POST /tasks
POST /tasks/claim
```

`POST /tasks` creates a new task.

`POST /tasks/claim` is currently a temporary testing endpoint used to
exercise the task-claiming logic. It will eventually be replaced by the
worker itself calling the service layer.

### Service

`TaskService` contains application-level logic and owns the transaction
boundary for task claiming.

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

## Task Model

Tasks currently have the following fields:

``` text
id          uuid
status      text, limited by a CHECK constraint (see below)
payload     jsonb
created_at  timestamptz
claimed_at  timestamptz, NULL until the task is claimed
updated_at  timestamptz
worker_id   uuid, NULL until the task is claimed
```

The database only accepts these statuses: `PENDING`, `RUNNING`,
`COMPLETED`, `FAILED`, and `CANCELLED`. The application currently only
produces `PENDING` and `RUNNING`.

A task begins in:

``` text
PENDING
```

When a worker successfully claims it:

``` text
RUNNING
```

There is no complete or fail operation yet, so a claimed task stays
`RUNNING`. The intended future lifecycle is:

``` text
PENDING
   |
   v
RUNNING
   |
   +------> COMPLETED
   |
   +------> FAILED
```

The schema also allows `CANCELLED`, but nothing sets it yet. The planned
M1 lifecycle (complete, fail, cancel, and a worker loop) is recorded in
[ADR 0004](docs/adr/0004-task-lifecycle-and-execution-model.md). It is not
built. Further states may be introduced later if they are justified by
the workflow requirements.

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
worker_id  = claiming worker
```

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

All of them extend `PostgresIntegrationTest`, which provides one shared
Testcontainers PostgreSQL container.

## Verified So Far

The current implementation has successfully demonstrated:

-   Spring Boot starts successfully
-   PostgreSQL runs through Docker
-   Flyway creates the database schema
-   tasks can be created through `POST /tasks`
-   the application can communicate with PostgreSQL
-   a pending task can be claimed
-   a claimed task changes from `PENDING` to `RUNNING`
-   `claimed_at` is populated
-   `worker_id` is populated
-   the updated task is returned to the caller
-   concurrent workers never claim the same task twice
    (`ConcurrentTaskClaimTest`)
-   a task locked by another transaction is skipped, not waited on
    (`SkipLockedClaimTest`)
-   all Flyway migrations apply to a fresh database
-   integration tests run against Testcontainers, with no local database
    required

Example successful state transition:

``` text
Before:

status     = PENDING
claimed_at = NULL
worker_id  = NULL


After:

status     = RUNNING
claimed_at = <timestamp>
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
ADR 0004 and in the roadmap's deviations table. None of that behavior is
implemented yet: there is still no complete, fail, or cancel operation,
and `TaskWorker` still only prints a line.

The intended shape, once M1 is built, is three short steps: claim in a
transaction, execute the handler with no transaction, then one complete
or fail. Leases and failure recovery stay in later milestones.

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

ADRs 0002, 0003, and 0004 describe planned behavior, not what the code
does today. Each one says so explicitly.

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
    │   │   └── task/
    │   └── resources/
    │       └── db/migration/
    └── test/
        └── java/com/kushal/workflow/
            ├── support/
            └── task/
```

The exact structure should evolve with the application rather than being
created all at once.

## Current Status

**Milestone: M0 complete. M1 (lifecycle, API, and worker runtime) is next, and not built yet.**

The system can currently create a task, store it in PostgreSQL, safely
claim a pending task using PostgreSQL row locking, and return the
resulting task. Concurrent claiming is covered by integration tests that
run against Testcontainers. GitHub Actions runs `./mvnw verify`.

Not built yet: completing, failing, or cancelling a task, a worker loop,
attempts, leases, heartbeats, recovery of abandoned tasks, fencing,
retries, and idempotency. A worker that dies after claiming a task leaves
it in `RUNNING` forever. ADR 0004 records how M1 will behave. It does not
describe the code today.
