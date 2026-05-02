from __future__ import annotations

import asyncio
import random
import time
from datetime import timedelta
from typing import Callable, Dict, List, Optional

from notebooklm import AuthTokens, NotebookLMClient
from notebooklm.auth import fetch_tokens
from supabase import Client

from notebook_auth_store import (
    decrypt_payload,
    list_keepalive_candidates,
    parse_timestamp,
    record_keepalive_result,
    update_cookies,
    utcnow,
)


def _extract_rotated_cookies_from_client(
    client: NotebookLMClient,
    baseline: Dict[str, str],
) -> Dict[str, str]:
    core = getattr(client, "_core", None)
    http = getattr(core, "_http_client", None) if core is not None else None
    jar_holder = getattr(http, "cookies", None) if http is not None else None
    raw_jar = getattr(jar_holder, "jar", None) if jar_holder is not None else None
    if raw_jar is None:
        return {}
    rotated: Dict[str, str] = {}
    try:
        for cookie in raw_jar:
            name = getattr(cookie, "name", None)
            value = getattr(cookie, "value", None)
            if not name or value is None:
                continue
            if baseline.get(name) != value:
                rotated[name] = value
    except Exception:  # noqa: BLE001
        return {}
    return rotated


async def _exercise_session_and_capture_rotation(
    cookies: Dict[str, str],
    csrf_token: str,
    session_id: str,
    *,
    timeout: float,
) -> Dict[str, str]:
    """Hace una llamada autenticada minima para forzar rotacion del 1PSIDTS y captura cookies nuevas."""
    auth = AuthTokens(cookies=dict(cookies), csrf_token=csrf_token, session_id=session_id)
    async with NotebookLMClient(auth, timeout=timeout) as client:
        try:
            await client.notebooks.list()
        except Exception:  # noqa: BLE001
            return {}
        return _extract_rotated_cookies_from_client(client, cookies)


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

    started_at = time.perf_counter()
    cookie_count = None

    def _elapsed_ms() -> int:
        return int((time.perf_counter() - started_at) * 1000)

    try:
        payload = decrypt_payload(str(row.get("payload_enc") or ""))
        cookies = payload.get("cookies") if isinstance(payload, dict) else None
        if not isinstance(cookies, dict) or not cookies:
            raise ValueError("Las credenciales guardadas no incluyen cookies validas.")
        cookie_count = len(cookies)

        await asyncio.sleep(random.uniform(0.1, 0.8))
        csrf_token, session_id = await asyncio.wait_for(
            fetch_tokens(dict(cookies)), timeout=max(1, timeout_sec)
        )

        rotated: Dict[str, str] = {}
        try:
            rotated = await asyncio.wait_for(
                _exercise_session_and_capture_rotation(
                    cookies,
                    csrf_token,
                    session_id,
                    timeout=max(1, timeout_sec),
                ),
                timeout=max(1, timeout_sec),
            )
        except asyncio.TimeoutError:
            rotated = {}
        except Exception as exc:  # noqa: BLE001
            print(f"[keepalive] No se pudo ejercitar sesion para rotacion ({user_id}): {exc}")

        if rotated:
            try:
                update_cookies(get_client(), user_id, rotated, event_source="keepalive_rotation")
            except Exception as exc:  # noqa: BLE001
                print(f"[keepalive] update_cookies fallo para {user_id}: {exc}")

        record_keepalive_result(
            get_client(),
            user_id,
            ok=True,
            duration_ms=_elapsed_ms(),
            cookie_count=cookie_count,
        )
    except asyncio.TimeoutError:
        record_keepalive_result(
            get_client(),
            user_id,
            ok=False,
            last_error="Keepalive NotebookLM agotado por timeout.",
            expired=False,
            duration_ms=_elapsed_ms(),
            cookie_count=cookie_count,
        )
    except ValueError as exc:
        record_keepalive_result(
            get_client(),
            user_id,
            ok=False,
            last_error=str(exc).strip() or "Sesion NotebookLM expirada o invalida.",
            expired=True,
            duration_ms=_elapsed_ms(),
            cookie_count=cookie_count,
        )
    except Exception as exc:  # noqa: BLE001
        record_keepalive_result(
            get_client(),
            user_id,
            ok=False,
            last_error=str(exc).strip() or exc.__class__.__name__,
            expired=False,
            duration_ms=_elapsed_ms(),
            cookie_count=cookie_count,
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
