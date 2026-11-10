-- Attempt history. Do not edit V1–V5. Apply with the process stopped.
-- One row per claim. outcome and ended_at are null while open. Claim
-- opens the row; an applied complete or fail, or the lease reaper,
-- closes it in the same transaction as the tasks update.
-- attempt can be 0 for RUNNING rows claimed before V5 counted attempts.
-- Terminal and PENDING rows are not backfilled: their history is unknown.
-- A tasks row set to a negative attempt by hand fails this migration.

alter table tasks
    add constraint tasks_attempt_nonnegative_check check (attempt >= 0);

create table task_attempts (
    task_id    uuid        not null,
    attempt    int         not null,
    worker_id  uuid        not null,
    started_at timestamptz not null,
    ended_at   timestamptz,
    outcome    text,
    error      text,
    constraint task_attempts_pkey primary key (task_id, attempt),
    constraint task_attempts_task_id_fkey foreign key (task_id)
        references tasks (id) on delete cascade,
    constraint task_attempts_attempt_check check (attempt >= 0),
    constraint task_attempts_outcome_check
        check (outcome in ('COMPLETED', 'FAILED', 'LEASE_EXPIRED')),
    constraint task_attempts_ended_iff_outcome_check
        check ((outcome is null) = (ended_at is null))
);

insert into task_attempts (task_id, attempt, worker_id, started_at)
select id, attempt, worker_id, claimed_at
from tasks
where status = 'RUNNING';
