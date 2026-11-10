# ADR 0002: At-least-once execution, not exactly-once

## Status

Accepted

The decision is accepted. Leases and heartbeats exist (M3). The
mechanisms that would deliver at-least-once execution (recovery,
retries, idempotency) are **not implemented yet**. See "Current
implementation versus planned".

## Context

Workers can crash, hang, or lose their network connection at any point
while running a task. Consider this failure:

1. Worker A claims a task.
2. A calls an external service and the side effect happens (for example,
   a payment is charged or an email is sent).
3. A crashes before it records the task as completed.
4. The engine cannot tell whether step 2 happened. Another worker may
   later claim the task and run it again.

When the engine recovers such a task, it has two choices:

- **Do not run it again** (at-most-once). A task can be lost or stuck. That
  is not acceptable for a durable workflow engine.
- **Run it again** (at-least-once). The side effect may happen twice.

Exactly-once would need the engine's database commit and the external
side effect to succeed or fail together. For an arbitrary external service
that is not possible, because the engine cannot see whether the call took
effect.

## Decision

The engine targets **at-least-once execution**. A task whose worker
disappears will eventually be run again.

Where duplicate effects matter, the design relies on **idempotency**: an
idempotency key, or an equivalent, is passed to the external system or
checked in a dedupe record, so a repeated execution does not repeat the
effect. This works only where a key or equivalent exists and the other side
honors it.

The engine will **not claim general exactly-once execution**. At best it can
say "effectively once" for effects that are idempotent.

## Current implementation versus planned

**Implemented today:** create, claim (`PENDING` to `RUNNING`), complete,
fail, and cancel. A worker loop claims a task, runs the handler with no
connection held, and writes the outcome once. `POST /api/v1/tasks` creates
a task; `GET` reads it until the status is terminal.

Since M3, a claim is a lease. The claim statement sets
`lease_expires_at` from the database clock plus `engine.lease.duration`
(default 30s) and increments `attempt`. While a handler runs,
`LeaseHeartbeat` extends that lease every
`engine.lease.heartbeat-interval` (default 10s). A rejected extension
sets `TaskContext.leaseLost()`, which asks the handler to stop. The
handler may ignore it. ADR 0003 describes the `attempt` guard on those
writes.

**Not implemented:** recovery of abandoned tasks (a reaper or any other
engine path from `RUNNING` back to `PENDING`), retries, and idempotency
keys. Those are M4, M5, and M6.

A lease that has expired is only a timestamp in the past. It is not lost
ownership, and nothing reads it to requeue the row. A crash after claim,
an interrupt during the handler, or a finish write that throws leaves
the task `RUNNING` with a lease that soon expires. A garbage-collection
pause or a slow heartbeat can also let the lease expire while the
handler is alive. In every case the row stays `RUNNING`. The code is not
at-least-once. A stuck task is lost work until an operator requeues it
by hand (ADR 0004) or until recovery exists (M4). At-least-once is still
the target of that later work.

"The handler ran at most once" is this process's control flow. It is not
a durability guarantee. A manual requeue, or a future reaper, runs the
handler again.

## Consequences

- Task handlers must tolerate being run more than once.
- Duplicates can also happen without a crash. A slow worker whose lease
  has expired may still be running while another worker takes over. ADR
  0003 protects the engine's own state from that worker. It does not undo
  effects the worker already caused outside the engine, which is why
  idempotency is needed as well.
- Tests and documentation must treat "ran twice" as allowed, and
  "completion recorded twice with conflicting results" as a bug.
- Documentation must not use the words "exactly-once" for the engine as a
  whole.

### Alternatives not chosen

- **At-most-once**: simple, but loses work.
- **Exactly-once through two-phase commit or a transactional outbox**:
  needs cooperation from every external system. That is out of scope.

## Future implications

- Leases and heartbeats (M3) give the engine a timestamp after which a
  worker may be gone. Nothing acts on that timestamp yet.
- Recovery (M4) will put tasks with an expired lease back into the queue,
  and retries will bound how many times that happens.
- Idempotency support will give handlers a stable key per task and attempt
  to pass downstream.
- Any later step type that calls outside services (for example HTTP) should
  send such a key.
