create extension if not exists pgcrypto;

create table tasks (
    id uuid primary key default gen_random_uuid(),
    status text not null,
    payload jsonb not null,
    created_at timestamp not null default now(),
    claimed_at timestamp default now(),
    worker_id uuid
);