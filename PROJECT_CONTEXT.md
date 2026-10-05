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
- Schema is managed by Flyway V1-V4: timestamptz columns, task_type, result,
  error, finished_at, updated_at, a CHECK constraint on status, named checks
  for task_type format, finished_at, and owner fields, and a partial index
  on pending tasks
- Datasource is configured with DB_URL, DB_USERNAME and DB_PASSWORD;
  defaults match the PostgreSQL in docker-compose.yml

Not implemented yet: complete, fail, and cancel; attempt counter, leases,
heartbeats, worker loop, crash recovery, fencing, retries, idempotency,
and workflow definitions. TaskWorker is a placeholder that prints a line.
GitHub Actions runs `./mvnw verify`.

Verified:

- PostgreSQL task creation works
- Flyway migrations apply to a fresh database
- A task can be successfully claimed
- Claimed task changes from PENDING → RUNNING
- claimed_at, updated_at, and worker_id are populated on claim
- Concurrent workers do not claim the same task (ConcurrentTaskClaimTest)
- A task locked by another transaction is skipped, not waited on (SkipLockedClaimTest)
- All integration tests share one Testcontainers PostgreSQL instance and
  do not need a local database

Current milestone:

M0 (foundation hardening) is complete, including GitHub Actions CI.
See PROJECT_ROADMAP.md.

M1 is the current milestone: task lifecycle, the REST API, and the worker
runtime. Roadmap M2 is merged into M1. Those decisions are recorded in
ADR 0004 and in the roadmap deviations table. M1.1 is implemented: the
task record, TaskStatus, and Flyway V4. Complete, fail, cancel, and the
worker runtime are not.

After M1:

- leases
- crash recovery
- fencing
- retries
- idempotency
- workflow state machine
- scheduling/fairness
- observability

Design decisions are recorded in docs/adr/. ADRs 0002 (at-least-once),
0003 (fencing via attempt), and 0004 (lifecycle and execution) describe
planned behavior, not current code.



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
