# ADR 0001: PostgreSQL as the task store and queue

## Status

Accepted

## Context

The engine needs to store tasks durably and let several workers take tasks
without two of them taking the same one.

A common design is a database for state plus a separate broker (Redis,
Kafka, RabbitMQ, SQS) for the queue. That splits one task's truth across
two systems. The broker's "message delivered" and the database's "task
claimed" can then disagree after a crash, and the engine has to reason
about that gap before it has even solved its own problems: lifecycle,
leases, retries, fencing.

The current scope is small. There is one `tasks` table, one claim
operation, and no measured load.

## Decision

PostgreSQL is currently all of the following:

- **Durable task store.** Tasks live in the `tasks` table, with the schema
  managed by Flyway.
- **Queue.** A pending task is a row with `status = 'PENDING'`, ordered by
  `created_at`. The partial index `idx_tasks_pending` covers exactly those
  rows.
- **Coordination and locking.** Claiming is one statement that runs inside
  the `@Transactional` method `TaskService.claimTask`:

  ```sql
  WITH next_task AS (
      SELECT id FROM tasks
      WHERE status = 'PENDING'
      ORDER BY created_at
      LIMIT 1
      FOR UPDATE SKIP LOCKED
  )
  UPDATE tasks t
  SET status = 'RUNNING', claimed_at = NOW(), worker_id = :workerId
  FROM next_task
  WHERE t.id = next_task.id
  RETURNING ...
  ```

  `FOR UPDATE` locks the selected row until the transaction ends.
  `SKIP LOCKED` makes other claimers ignore rows that are already locked,
  so concurrent claimers get different rows (or none) and never wait on
  each other.
- **Source of truth.** Task status, owner, and timestamps are whatever the
  database says. Timestamps come from the database clock (`NOW()`).

The tests for this are `ConcurrentTaskClaimTest` (no task is claimed
twice) and `SkipLockedClaimTest` (a locked row is skipped, not waited on,
and the test fails if `SKIP LOCKED` is removed). They cover the claim
query on PostgreSQL 16. They say nothing about throughput.

We start here because it is sufficient for the current scope, keeps the
core to a single piece of infrastructure, and makes claiming and locking
explicit SQL that can be read, tested, and reasoned about directly.

### Alternatives deferred

- **Redis** (lists, streams, or locks) as the queue: lower latency, but
  task state would live in two places and Redis durability would need its
  own analysis.
- **Kafka**: a replayable, partitioned log. Per-task claiming, leases and
  retries are not what a log gives you directly, and it is a lot of
  infrastructure for this stage.
- **RabbitMQ / SQS**: a managed or dedicated queue with its own
  acknowledgement and redelivery semantics, which would overlap with the
  lease and retry design this project wants to build itself.

These are deferred, not rejected. Each has strengths PostgreSQL does not,
and this ADR does not claim PostgreSQL is the better queue in general.

## Consequences

- Task state changes are ordinary transactions. Later, a task completion
  and the next workflow step can be recorded in one commit.
- There is one system to run, back up, and debug. Integration tests run
  against a real PostgreSQL (Testcontainers), not a fake.
- Workers pull work. Nothing pushes to them, so an idle worker has to poll.
- Every claim is a write to the shared `tasks` table, so claim rate is
  bounded by that database. No benchmark has been run, and no performance
  or scalability claim is made here.
- Finished rows stay in the table until something removes them. Retention
  is not handled.
- A transaction must never stay open while user logic runs. Today the
  claim transaction covers only the claim statement, and this has to stay
  true once a worker loop exists.

## Future implications

- Leases, heartbeats, and the recovery of abandoned tasks (later
  milestones) will reuse the same pattern: guarded `UPDATE`s and
  `SKIP LOCKED` scans on `tasks`.
- Fencing (ADR 0003) depends on the guard being part of the same
  statement as the write, which this design makes possible.
- The decision should be revisited if measurements show the database is
  the bottleneck, or if a need appears that the table cannot meet, such as
  fan-out to many consumers or event replay. A broker could then be added
  alongside PostgreSQL, with the consistency gap above handled
  deliberately.
- The `LISTEN/NOTIFY` feature could reduce polling later. It is not used
  now.
