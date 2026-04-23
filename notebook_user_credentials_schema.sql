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

create index if not exists idx_notebook_user_credentials_status
    on public.notebook_user_credentials(status);

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

notify pgrst, 'reload schema';
