#!/usr/bin/env python3
"""Worker para procesar corridas SEIA fuera del web server."""

from __future__ import annotations

import signal
import time
import traceback
from typing import Any, Dict, Optional

from api_app import (
    NOTEBOOK_CREDENTIALS_USER_ID_METADATA_KEY,
    getenv_positive_int,
    get_supabase_client,
    load_run_documents,
    load_stored_notebook_auth_for_worker,
    mark_run_failed,
    process_cp6b_listing_background,
    process_notebook_upload_background,
    retryable_document_ids,
    run_metadata_dict,
    supabase_data,
)


WORKER_POLL_INTERVAL_SEC = getenv_positive_int("WORKER_POLL_INTERVAL_SEC", 5)
WORKER_IDLE_LOG_EVERY_SEC = getenv_positive_int("WORKER_IDLE_LOG_EVERY_SEC", 60)
RUNS_TABLE = "adenda_document_runs"

_STOP = False


def _handle_stop(_signum: int, _frame: object) -> None:
    global _STOP
    _STOP = True
    print("[worker] Stop solicitado; terminando luego del job actual.", flush=True)


def _first_row(response: Any) -> Optional[Dict[str, Any]]:
    rows = supabase_data(response)
    return rows[0] if rows else None


def _select_next_cp6b(client: Any) -> Optional[Dict[str, Any]]:
    response = (
        client.table(RUNS_TABLE)
        .select("*")
        .eq("status", "queued")
        .eq("progress_stage", "queued")
        .order("created_at")
        .limit(1)
        .execute()
    )
    return _first_row(response)


def _select_next_upload(client: Any) -> Optional[Dict[str, Any]]:
    response = (
        client.table(RUNS_TABLE)
        .select("*")
        .eq("status", "uploading")
        .eq("progress_stage", "upload_queued")
        .order("updated_at")
        .limit(1)
        .execute()
    )
    return _first_row(response)


def _claim_run(
    client: Any,
    run: Dict[str, Any],
    *,
    expected_stage: str,
    claimed_stage: str,
    claimed_status: str,
    message: str,
) -> bool:
    run_id = str(run.get("id") or "").strip()
    response = (
        client.table(RUNS_TABLE)
        .update({
            "status": claimed_status,
            "progress_stage": claimed_stage,
            "progress_message": message,
            "error_message": "",
        })
        .eq("id", run_id)
        .eq("progress_stage", expected_stage)
        .execute()
    )
    return bool(supabase_data(response))


def _process_cp6b(client: Any, run: Dict[str, Any]) -> None:
    run_id = str(run.get("id") or "").strip()
    if not _claim_run(
        client,
        run,
        expected_stage="queued",
        claimed_stage="worker_claimed",
        claimed_status="running",
        message="Worker tomo la corrida CP6B.",
    ):
        return

    exclude_keywords = run.get("exclude_keywords")
    if not isinstance(exclude_keywords, list):
        exclude_keywords = []

    print(f"[worker] CP6B start run_id={run_id}", flush=True)
    process_cp6b_listing_background(
        run_id,
        str(run.get("tipo") or ""),
        str(run.get("documento_seia") or ""),
        str(run.get("output_dir") or ""),
        [str(keyword) for keyword in exclude_keywords if str(keyword).strip()],
    )
    print(f"[worker] CP6B end run_id={run_id}", flush=True)


def _notebook_auth_for_run(client: Any, run: Dict[str, Any]) -> Dict[str, Any]:
    metadata = run_metadata_dict(run)
    user_id = str(metadata.get(NOTEBOOK_CREDENTIALS_USER_ID_METADATA_KEY) or "").strip()
    return load_stored_notebook_auth_for_worker(client, user_id)


def _process_upload(client: Any, run: Dict[str, Any]) -> None:
    run_id = str(run.get("id") or "").strip()
    if not _claim_run(
        client,
        run,
        expected_stage="upload_queued",
        claimed_stage="worker_claimed_upload",
        claimed_status="uploading",
        message="Worker tomo la carga a NotebookLM.",
    ):
        return

    print(f"[worker] upload start run_id={run_id}", flush=True)
    try:
        rows = load_run_documents(client, run_id)
        selected_ids = retryable_document_ids(rows)
        if not selected_ids:
            client.table(RUNS_TABLE).update({
                "status": "success",
                "progress_stage": "completed",
                "progress_percent": 100,
                "progress_message": "No hay documentos pendientes para cargar.",
                "error_message": "",
            }).eq("id", run_id).execute()
            print(f"[worker] upload skip run_id={run_id}: no pending docs", flush=True)
            return

        notebook_auth = _notebook_auth_for_run(client, run)
        notebook_id = str(run.get("notebooklm_id") or "").strip()
        if notebook_id:
            nombre_notebook = None
            existing_notebook_id = notebook_id
        else:
            nombre_notebook = str(run.get("nombre_notebooklm") or "").strip()
            existing_notebook_id = None
            if not nombre_notebook:
                raise RuntimeError("La corrida no tiene nombre_notebooklm para crear notebook.")

        process_notebook_upload_background(
            run_id,
            selected_ids,
            nombre_notebook,
            existing_notebook_id,
            notebook_auth,
        )
        print(f"[worker] upload end run_id={run_id}", flush=True)
    except Exception as exc:  # noqa: BLE001
        mark_run_failed(client, run_id, str(exc))
        raise


def _tick(client: Any) -> bool:
    cp6b_run = _select_next_cp6b(client)
    if cp6b_run:
        _process_cp6b(client, cp6b_run)
        return True

    upload_run = _select_next_upload(client)
    if upload_run:
        _process_upload(client, upload_run)
        return True

    return False


def main() -> int:
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    client = get_supabase_client()
    print(
        "[worker] Started "
        f"poll_interval={WORKER_POLL_INTERVAL_SEC}s",
        flush=True,
    )
    last_idle_log = 0.0

    while not _STOP:
        try:
            did_work = _tick(client)
        except Exception as exc:  # noqa: BLE001
            print(f"[worker] Job error: {exc}", flush=True)
            traceback.print_exc()
            did_work = False

        if did_work:
            continue

        now = time.monotonic()
        if now - last_idle_log >= WORKER_IDLE_LOG_EVERY_SEC:
            last_idle_log = now
            print("[worker] idle", flush=True)
        time.sleep(WORKER_POLL_INTERVAL_SEC)

    print("[worker] Stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
