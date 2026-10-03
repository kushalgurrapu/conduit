# ADR 0002: At-least-once execution, not exactly-once

## Status

Accepted

The decision is accepted. The mechanisms that deliver it (leases, recovery,
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

**Implemented today:** task creation and claiming (`PENDING` to `RUNNING`).

**Not implemented:** completing or failing a task, leases, heartbeats,
recovery of abandoned tasks, retries, and idempotency keys.

This means the current behavior after a worker crash is that the task stays
`RUNNING` forever. The code is not yet at-least-once. A lost task is the
at-most-once outcome. At-least-once is the target that the planned lease,
recovery, and retry work is meant to reach.

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

- Leases and heartbeats will give the engine a way to notice that a worker
  is gone.
- Recovery will put abandoned tasks back into the queue, and retries will
  bound how many times that happens.
- Idempotency support will give handlers a stable key per task and attempt
  to pass downstream.
- Any later step type that calls outside services (for example HTTP) should
  send such a key.
