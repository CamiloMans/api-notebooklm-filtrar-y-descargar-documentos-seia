-- Table for encrypted NotebookLM credentials stored per MyMA user.
-- Run this in Supabase SQL editor before enabling the feature in production.

create table if not exists public.notebook_user_credentials (
    user_id uuid primary key references auth.users(id) on delete cascade,
    payload_enc text not null,
    cookie_names text[] not null default '{}'::text[],
    validated_at timestamptz not null,
    last_checked_at timestamptz not null,
    last_used_at timestamptz,
    status text not null default 'valid',
    last_error text not null default '',
    failure_count integer not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.notebook_user_credentials_events (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null references auth.users(id) on delete cascade,
    event_type text not null,
    source text not null default '',
    ok boolean,
    status_before text not null default '',
    status_after text not null default '',
    checked_at timestamptz not null default now(),
    duration_ms integer,
    cookie_count integer,
    failure_count integer,
    last_error text not null default '',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

alter table if exists public.notebook_user_credentials
    add column if not exists payload_enc text not null default '';
alter table if exists public.notebook_user_credentials
    add column if not exists cookie_names text[] not null default '{}'::text[];
alter table if exists public.notebook_user_credentials
    add column if not exists validated_at timestamptz not null default now();
alter table if exists public.notebook_user_credentials
    add column if not exists last_checked_at timestamptz not null default now();
alter table if exists public.notebook_user_credentials
    add column if not exists last_used_at timestamptz;
alter table if exists public.notebook_user_credentials
    add column if not exists status text not null default 'valid';
alter table if exists public.notebook_user_credentials
    add column if not exists last_error text not null default '';
alter table if exists public.notebook_user_credentials
    add column if not exists failure_count integer not null default 0;
alter table if exists public.notebook_user_credentials
    add column if not exists created_at timestamptz not null default now();
alter table if exists public.notebook_user_credentials
    add column if not exists updated_at timestamptz not null default now();

alter table if exists public.notebook_user_credentials_events
    add column if not exists event_type text not null default '';
alter table if exists public.notebook_user_credentials_events
    add column if not exists source text not null default '';
alter table if exists public.notebook_user_credentials_events
    add column if not exists ok boolean;
alter table if exists public.notebook_user_credentials_events
    add column if not exists status_before text not null default '';
alter table if exists public.notebook_user_credentials_events
    add column if not exists status_after text not null default '';
alter table if exists public.notebook_user_credentials_events
    add column if not exists checked_at timestamptz not null default now();
alter table if exists public.notebook_user_credentials_events
    add column if not exists duration_ms integer;
alter table if exists public.notebook_user_credentials_events
    add column if not exists cookie_count integer;
alter table if exists public.notebook_user_credentials_events
    add column if not exists failure_count integer;
alter table if exists public.notebook_user_credentials_events
    add column if not exists last_error text not null default '';
alter table if exists public.notebook_user_credentials_events
    add column if not exists metadata jsonb not null default '{}'::jsonb;
alter table if exists public.notebook_user_credentials_events
    add column if not exists created_at timestamptz not null default now();

create index if not exists idx_notebook_user_credentials_status
    on public.notebook_user_credentials(status);

create index if not exists idx_notebook_user_credentials_events_user_checked_at
    on public.notebook_user_credentials_events(user_id, checked_at desc);

create index if not exists idx_notebook_user_credentials_events_event_type
    on public.notebook_user_credentials_events(event_type);

create index if not exists idx_notebook_user_credentials_events_status_after
    on public.notebook_user_credentials_events(status_after);

create or replace function public.set_notebook_user_credentials_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_notebook_user_credentials_updated_at
    on public.notebook_user_credentials;

create trigger trg_notebook_user_credentials_updated_at
before update on public.notebook_user_credentials
for each row
execute function public.set_notebook_user_credentials_updated_at();

alter table public.notebook_user_credentials enable row level security;
alter table public.notebook_user_credentials_events enable row level security;

drop policy if exists "Users can read own notebook credentials"
    on public.notebook_user_credentials;
create policy "Users can read own notebook credentials"
on public.notebook_user_credentials
for select
using (auth.uid() = user_id);

drop policy if exists "Users can insert own notebook credentials"
    on public.notebook_user_credentials;
create policy "Users can insert own notebook credentials"
on public.notebook_user_credentials
for insert
with check (auth.uid() = user_id);

drop policy if exists "Users can update own notebook credentials"
    on public.notebook_user_credentials;
create policy "Users can update own notebook credentials"
on public.notebook_user_credentials
for update
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "Users can delete own notebook credentials"
    on public.notebook_user_credentials;
create policy "Users can delete own notebook credentials"
on public.notebook_user_credentials
for delete
using (auth.uid() = user_id);

drop policy if exists "Users can read own notebook credential events"
    on public.notebook_user_credentials_events;
create policy "Users can read own notebook credential events"
on public.notebook_user_credentials_events
for select
using (auth.uid() = user_id);

notify pgrst, 'reload schema';
