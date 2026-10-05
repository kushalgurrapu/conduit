-- Columns the lifecycle writes will use.
--
-- Existing rows become task_type NOOP, then the default is dropped so a new
-- insert must name a type. Those NOOP rows will complete as no-ops once
-- workers start.
--
-- PENDING rows can still carry claimed_at from V1 (that column defaulted to
-- now()). Those owner fields are cleared. Terminal rows receive finished_at.
-- A RUNNING row with no worker_id or claimed_at is left as it is, so
-- tasks_running_has_owner_check fails the migration instead of inventing an
-- owner. There is no result/error exclusivity check and no timestamp-ordering
-- check: now() can step backwards, and the V3 UTC conversion skews older rows.

alter table tasks
    add column task_type text not null default 'NOOP',
    add column result jsonb,
    add column error text,
    add column finished_at timestamptz;

alter table tasks
    alter column task_type drop default;

update tasks
   set worker_id = null,
       claimed_at = null
 where status = 'PENDING'
   and (worker_id is not null or claimed_at is not null);

update tasks
   set finished_at = greatest(created_at, claimed_at, updated_at)
 where status in ('COMPLETED', 'FAILED', 'CANCELLED')
   and finished_at is null;

alter table tasks
    add constraint tasks_task_type_format_check
        check (task_type ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    add constraint tasks_finished_at_iff_terminal_check
        check ((status in ('COMPLETED', 'FAILED', 'CANCELLED')) = (finished_at is not null)),
    add constraint tasks_running_has_owner_check
        check (status <> 'RUNNING' or (worker_id is not null and claimed_at is not null)),
    add constraint tasks_pending_has_no_owner_check
        check (status <> 'PENDING' or (worker_id is null and claimed_at is null));
