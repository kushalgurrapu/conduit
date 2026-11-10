-- Lease and attempt. Do not edit V1–V4.
--
-- attempt stays default 0 so an insert does not have to name it. Claim is
-- the only statement that increments it. last_heartbeat_at stays null until
-- a later heartbeat. Existing RUNNING rows already have claimed_at (V4).
-- Copy that into lease_expires_at before the check is replaced. A past
-- claimed_at leaves an expired lease. The row stays RUNNING.

alter table tasks
    add column lease_expires_at timestamptz,
    add column attempt int not null default 0,
    add column last_heartbeat_at timestamptz;

update tasks
   set lease_expires_at = claimed_at
 where status = 'RUNNING'
   and lease_expires_at is null;

alter table tasks drop constraint tasks_running_has_owner_check;

alter table tasks
    add constraint tasks_running_has_owner_check
        check (status <> 'RUNNING'
            or (worker_id is not null
                and claimed_at is not null
                and lease_expires_at is not null));

create index idx_tasks_running_lease
    on tasks (lease_expires_at)
    where status = 'RUNNING';
