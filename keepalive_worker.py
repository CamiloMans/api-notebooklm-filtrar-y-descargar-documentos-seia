from __future__ import annotations

import asyncio
import random
from datetime import timedelta
from typing import Callable, Dict, List

from notebooklm.auth import fetch_tokens
from supabase import Client

from notebook_auth_store import (
    decrypt_payload,
    list_keepalive_candidates,
    parse_timestamp,
    record_keepalive_result,
    utcnow,
)


def _is_recently_active(row: Dict[str, object], *, active_days: int) -> bool:
    reference = parse_timestamp(row.get("last_used_at")) or parse_timestamp(row.get("validated_at"))
    if reference is None:
        return False
    return reference >= utcnow() - timedelta(days=max(1, active_days))


async def _process_single_user(
    row: Dict[str, object],
    *,
    get_client: Callable[[], Client],
    timeout_sec: int,
) -> None:
    user_id = str(row.get("user_id") or "").strip()
    if not user_id:
        return

    try:
        payload = decrypt_payload(str(row.get("payload_enc") or ""))
        cookies = payload.get("cookies") if isinstance(payload, dict) else None
        if not isinstance(cookies, dict) or not cookies:
            raise ValueError("Las credenciales guardadas no incluyen cookies validas.")

        await asyncio.sleep(random.uniform(0.1, 0.8))
        await asyncio.wait_for(fetch_tokens(dict(cookies)), timeout=max(1, timeout_sec))
        record_keepalive_result(get_client(), user_id, ok=True)
    except asyncio.TimeoutError:
        record_keepalive_result(
            get_client(),
            user_id,
            ok=False,
            last_error="Keepalive NotebookLM agotado por timeout.",
            expired=False,
        )
    except ValueError as exc:
        record_keepalive_result(
            get_client(),
            user_id,
            ok=False,
            last_error=str(exc).strip() or "Sesion NotebookLM expirada o invalida.",
            expired=True,
        )
    except Exception as exc:  # noqa: BLE001
        record_keepalive_result(
            get_client(),
            user_id,
            ok=False,
            last_error=str(exc).strip() or exc.__class__.__name__,
            expired=False,
        )


async def _run_iteration(
    *,
    get_client: Callable[[], Client],
    active_days: int,
    max_concurrency: int,
    timeout_sec: int,
) -> None:
    candidates = [
        row
        for row in list_keepalive_candidates(get_client())
        if _is_recently_active(row, active_days=active_days)
    ]
    if not candidates:
        return

    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async def _runner(row: Dict[str, object]) -> None:
        async with semaphore:
            await _process_single_user(row, get_client=get_client, timeout_sec=timeout_sec)

    await asyncio.gather(*[_runner(row) for row in candidates], return_exceptions=True)


async def run_keepalive_loop(
    *,
    get_client: Callable[[], Client],
    interval_sec: int,
    active_days: int,
    max_concurrency: int,
    timeout_sec: int,
) -> None:
    while True:
        try:
            await _run_iteration(
                get_client=get_client,
                active_days=active_days,
                max_concurrency=max_concurrency,
                timeout_sec=timeout_sec,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[keepalive] Error inesperado en iteracion: {exc}")

        await asyncio.sleep(max(30, interval_sec))
