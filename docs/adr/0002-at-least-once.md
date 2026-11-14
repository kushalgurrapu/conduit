# ADR 0002: At-least-once execution, not exactly-once

## Status

Accepted

The decision is accepted. Leases and heartbeats exist (M3), and
recovery of abandoned tasks exists (M4, ADR 0005). The engine is
at-least-once only in the scope stated under "Current implementation
versus planned". Retries (M5) and idempotency (M6) are **not
implemented yet**.

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

Since M4, `LeaseReaper` returns a `RUNNING` row whose lease has
expired to `PENDING`, keeps `attempt`, and records the attempt as
`LEASE_EXPIRED` in `task_attempts`. A worker claims it again at
`attempt + 1`, and the old attempt's writes are rejected (ADR 0003,
ADR 0005). An expired lease makes the row eligible for requeue. It does
not end ownership: until a reap commits, the old worker can still
extend, complete, or fail.

**At-least-once, as far as M4 goes.** A task is requeued and run again
when its attempt's lease stops being extended.

- **Tested causes** (`RecoveryIntegrationTest`): a stopped heartbeat
  with a handler that never returns (a crash or a pause), a handler
  interrupted at shutdown, and a finish write that did not commit.
- **Argued from the same mechanism, not tested:** the process is killed
  after the claim commits. No test kills a JVM.
- This holds when **all** of these are true: at least one process runs a
  reaper; at least one worker loop claims; and the handler does not take
  its worker down on every attempt. An ordinary handler `Error` is a
  `Failed` finish, but a `VirtualMachineError` other than
  `StackOverflowError` still kills the loop, and if it repeats on every
  attempt it keeps killing loops until M5 adds `max_attempts`.

Nothing here bounds how long recovery takes, and nothing stops a
zombie. ADR 0005 lists what M4 does not provide.

**Not implemented:** retries (M5) and idempotency keys (M6). `FAILED`
is terminal.

"The handler ran at most once" is this process's control flow. It is not
a durability guarantee. A reap, or the operator's expire-the-lease
procedure (ADR 0005), runs the handler again.

## Consequences

- Task handlers must tolerate being run more than once.
- Duplicates can also happen without a crash. A slow worker whose lease
  has expired may still be running when a reaper requeues its task and
  another worker claims it. Takeover needs that requeue: an expired lease
  alone does not let another worker claim the row. ADR 0003 protects the
  engine's own state from the slow worker. It does not undo
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
  worker may be gone. Since M4 the reaper acts on it.
- Recovery (M4) puts tasks with an expired lease back into the queue.
  Retries (M5) will bound how many times that happens.
- Idempotency support will give handlers a stable key per task and attempt
  to pass downstream.
- Any later step type that calls outside services (for example HTTP) should
  send such a key.
