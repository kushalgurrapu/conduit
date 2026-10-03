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
-   REST/HTTP

### Planned

-   JUnit
-   Testcontainers
-   Redis, if/when justified
-   React + TypeScript frontend
-   Docker Compose
-   OpenTelemetry
-   Prometheus/Grafana
-   GitHub Actions

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
id
status
payload
created_at
claimed_at
worker_id
```

A task begins in:

``` text
PENDING
```

When a worker successfully claims it:

``` text
RUNNING
```

The intended future lifecycle is:

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

Additional states may be introduced later if they are justified by the
workflow requirements.

## Database

PostgreSQL is currently running through Docker.

The initial Flyway migration creates the `tasks` table and enables
PostgreSQL's `pgcrypto` extension for UUID generation.

The database is intentionally the source of truth for task state.

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

The basic task-claiming mechanism is complete.

The next milestone is to **prove the concurrency behavior
experimentally**.

The next work should:

1.  Create multiple pending tasks.
2.  Simulate multiple workers.
3.  Have workers attempt to claim tasks concurrently.
4.  Verify that workers do not claim the same task.
5.  Verify that locked tasks are skipped.
6.  Turn the successful claim operation into an actual worker loop.

After that, the project will move toward:

``` text
Worker
  |
  +--> claim task
  |
  +--> execute task
  |
  +--> complete/fail task
```

Then we will introduce leases and failure recovery.

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
-   [ ] Concurrent worker test

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
-   [ ] Docker Compose
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

## Repository Structure

The project is expected to evolve toward something similar to:

``` text
durable-workflow-engine/
├── README.md
├── PROJECT_CONTEXT.md
├── docs/
├── src/
│   ├── main/
│   │   ├── java/
│   │   └── resources/
│   └── test/
├── pom.xml
└── docker-compose.yml
```

The exact structure should evolve with the application rather than being
created all at once.

## Current Status

**Milestone: Basic task creation and transactional task claiming**

The system can currently create a task, store it in PostgreSQL, safely
claim a pending task using PostgreSQL row locking, and return the
resulting task.

The immediate goal is to demonstrate that this behavior remains correct
when multiple workers operate concurrently.
