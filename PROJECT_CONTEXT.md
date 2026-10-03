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
- Docker
- Maven

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

Current task lifecycle:

PENDING → RUNNING → COMPLETED / FAILED

Current implementation:

- POST /tasks creates a task
- TaskRepository uses JdbcClient
- TaskService owns transaction boundaries
- Task claiming uses FOR UPDATE SKIP LOCKED
- Claim records worker_id and claimed_at
- Claim returns the updated Task

Verified:

- PostgreSQL task creation works
- Flyway migration works
- A task can be successfully claimed
- Claimed task changes from PENDING → RUNNING
- claimed_at and worker_id are populated

Current next milestone:

Test concurrent workers and verify that two workers cannot claim

the same task.

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