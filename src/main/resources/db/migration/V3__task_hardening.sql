alter table tasks
    alter column created_at type timestamptz using created_at at time zone 'UTC';

alter table tasks
    alter column claimed_at type timestamptz using claimed_at at time zone 'UTC';

alter table tasks
    add column updated_at timestamptz not null default now();

alter table tasks
    add constraint tasks_status_check
        check (status in ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'));

create index idx_tasks_pending on tasks (created_at) where status = 'PENDING';
