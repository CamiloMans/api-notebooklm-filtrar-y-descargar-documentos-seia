-- Tables for the CP6B frontend-filter flow.
-- Run this in the Supabase SQL editor before using the new endpoints.

create extension if not exists pgcrypto;

create table if not exists public.adenda_document_runs (
    id uuid primary key default gen_random_uuid(),
    tipo text not null default '',
    id_documento text not null,
    documento_seia text not null,
    output_dir text not null,
    status text not null default 'listed',
    metadata jsonb not null default '{}'::jsonb,
    docs_report_stats jsonb not null default '{}'::jsonb,
    trace_stats jsonb not null default '{}'::jsonb,
    exclude_keywords jsonb not null default '[]'::jsonb,
    progress_stage text not null default '',
    progress_current integer not null default 0,
    progress_total integer not null default 0,
    progress_percent integer not null default 0,
    progress_message text not null default '',
    error_message text not null default '',
    listado_excel_path text,
    trace_excel_path text,
    notebooklm_id text,
    nombre_notebooklm text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.adenda_document_files (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references public.adenda_document_runs(id) on delete cascade,
    seleccionar boolean not null default true,
    selected boolean not null default true,
    categoria text not null default '',
    texto_link text not null default '',
    url_origen text not null default '',
    nombre_archivo text not null,
    nombre_archivo_final text not null default '',
    extension text not null default '',
    formato text not null default '',
    ruta_relativa text not null,
    tamano_bytes bigint not null default 0,
    nivel_descarga_descompresion integer not null default 0,
    origen text not null default '',
    upload_status text not null default 'pending',
    upload_error text not null default '',
    created_at timestamptz not null default now(),
    unique (run_id, ruta_relativa)
);

alter table if exists public.adenda_document_runs
    add column if not exists tipo text not null default '';
alter table if exists public.adenda_document_runs
    add column if not exists exclude_keywords jsonb not null default '[]'::jsonb;
alter table if exists public.adenda_document_runs
    add column if not exists progress_stage text not null default '';
alter table if exists public.adenda_document_runs
    add column if not exists progress_current integer not null default 0;
alter table if exists public.adenda_document_runs
    add column if not exists progress_total integer not null default 0;
alter table if exists public.adenda_document_runs
    add column if not exists progress_percent integer not null default 0;
alter table if exists public.adenda_document_runs
    add column if not exists progress_message text not null default '';
alter table if exists public.adenda_document_runs
    add column if not exists error_message text not null default '';
alter table if exists public.adenda_document_runs
    alter column id_adenda drop not null;

alter table if exists public.adenda_document_files
    add column if not exists seleccionar boolean not null default true;
alter table if exists public.adenda_document_files
    add column if not exists selected boolean not null default true;
alter table if exists public.adenda_document_files
    add column if not exists categoria text not null default '';
alter table if exists public.adenda_document_files
    add column if not exists texto_link text not null default '';
alter table if exists public.adenda_document_files
    add column if not exists url_origen text not null default '';
alter table if exists public.adenda_document_files
    add column if not exists nombre_archivo_final text not null default '';
alter table if exists public.adenda_document_files
    add column if not exists formato text not null default '';
alter table if exists public.adenda_document_files
    add column if not exists upload_error text not null default '';

drop index if exists idx_adenda_document_runs_id_adenda;

create index if not exists idx_adenda_document_runs_tipo
    on public.adenda_document_runs(tipo);

create index if not exists idx_adenda_document_runs_id_documento
    on public.adenda_document_runs(id_documento);

create index if not exists idx_adenda_document_files_run_id
    on public.adenda_document_files(run_id);

create index if not exists idx_adenda_document_files_upload_status
    on public.adenda_document_files(upload_status);

create or replace function public.set_adenda_document_runs_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_adenda_document_runs_updated_at
    on public.adenda_document_runs;

create trigger trg_adenda_document_runs_updated_at
before update on public.adenda_document_runs
for each row
execute function public.set_adenda_document_runs_updated_at();

notify pgrst, 'reload schema';
