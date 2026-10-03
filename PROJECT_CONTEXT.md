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

Implemented: PENDING → RUNNING (claim)

Intended, not implemented: RUNNING → COMPLETED / FAILED

There is no complete or fail operation yet, so a claimed task stays RUNNING.
The database CHECK constraint also allows CANCELLED, but nothing sets it.

Current implementation:

- POST /tasks creates a task
- POST /tasks/claim is a temporary test endpoint that claims with a random worker id
- TaskRepository uses JdbcClient
- TaskService owns transaction boundaries
- Task claiming uses FOR UPDATE SKIP LOCKED
- Claim records worker_id and claimed_at
- Claim returns the updated Task
- Schema is managed by Flyway V1-V3: timestamptz columns, updated_at,
  a CHECK constraint on status, and a partial index on pending tasks
- Datasource is configured with DB_URL, DB_USERNAME and DB_PASSWORD;
  defaults match the PostgreSQL in docker-compose.yml

Not implemented yet: attempt counter, leases, heartbeats, worker loop,
crash recovery, fencing, retries, idempotency, workflow definitions, CI.
TaskWorker is a placeholder.

Verified:

- PostgreSQL task creation works
- Flyway migrations apply to a fresh database
- A task can be successfully claimed
- Claimed task changes from PENDING → RUNNING
- claimed_at and worker_id are populated
- Concurrent workers do not claim the same task (ConcurrentTaskClaimTest)
- A task locked by another transaction is skipped, not waited on (SkipLockedClaimTest)
- All integration tests share one Testcontainers PostgreSQL instance and
  do not need a local database

Current milestone:

M0 (foundation hardening) is in progress. See PROJECT_ROADMAP.md.

Remaining in M0: a GitHub Actions workflow that runs the build and tests.

Next milestone: M1, the task lifecycle (complete/fail, validated API, error handling).

After that:

- worker loop
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
