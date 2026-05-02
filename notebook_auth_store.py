from __future__ import annotations

import base64
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from supabase import Client

NOTEBOOK_CREDENTIALS_TABLE = "notebook_user_credentials"
NOTEBOOK_CREDENTIALS_EVENTS_TABLE = "notebook_user_credentials_events"
NOTEBOOK_AUTH_ENCRYPTION_KEY_ENV = "NOTEBOOK_AUTH_ENCRYPTION_KEY"
_AAD = b"notebook-user-credentials:v1"
_PREFIX = "v1."


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}".encode("ascii"))


def _response_rows(response: Any) -> List[Dict[str, Any]]:
    data = getattr(response, "data", None) or []
    return [dict(row) for row in data]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: Optional[datetime] = None) -> str:
    return (value or utcnow()).astimezone(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def compute_days_until_soft_expiry(
    validated_at: Any,
    *,
    soft_expiry_days: int,
    now: Optional[datetime] = None,
) -> Optional[int]:
    validated_at_dt = parse_timestamp(validated_at)
    if validated_at_dt is None:
        return None
    remaining = validated_at_dt + timedelta(days=max(1, soft_expiry_days)) - (now or utcnow())
    return math.ceil(remaining.total_seconds() / 86400)


def _load_encryption_key() -> bytes:
    raw_value = os.getenv(NOTEBOOK_AUTH_ENCRYPTION_KEY_ENV, "").strip()
    if not raw_value:
        raise RuntimeError(f"{NOTEBOOK_AUTH_ENCRYPTION_KEY_ENV} no configurado.")

    if len(raw_value) == 64:
        try:
            decoded_hex = bytes.fromhex(raw_value)
        except ValueError:
            decoded_hex = b""
        else:
            if len(decoded_hex) in (16, 24, 32):
                return decoded_hex

    try:
        decoded = _urlsafe_b64decode(raw_value)
    except Exception as exc:  # noqa: BLE001
        decoded = b""
        decode_error = exc
    else:
        decode_error = None
        if len(decoded) in (16, 24, 32):
            return decoded

    if len(raw_value.encode("utf-8")) in (16, 24, 32):
        return raw_value.encode("utf-8")

    raise RuntimeError(
        f"{NOTEBOOK_AUTH_ENCRYPTION_KEY_ENV} debe ser base64url, hex o texto plano de 16/24/32 bytes."
    ) from decode_error


def encrypt_payload(payload_dict: Dict[str, Any]) -> str:
    plaintext = json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    nonce = os.urandom(12)
    ciphertext = AESGCM(_load_encryption_key()).encrypt(nonce, plaintext, _AAD)
    return f"{_PREFIX}{_urlsafe_b64encode(nonce + ciphertext)}"


def decrypt_payload(payload_enc: str) -> Dict[str, Any]:
    raw_value = (payload_enc or "").strip()
    if not raw_value.startswith(_PREFIX):
        raise ValueError("payload_enc tiene un formato desconocido.")
    blob = _urlsafe_b64decode(raw_value[len(_PREFIX) :])
    if len(blob) <= 12:
        raise ValueError("payload_enc no contiene nonce/ciphertext validos.")
    nonce = blob[:12]
    ciphertext = blob[12:]
    plaintext = AESGCM(_load_encryption_key()).decrypt(nonce, ciphertext, _AAD)
    payload = json.loads(plaintext.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("payload_enc no contiene un objeto JSON valido.")
    return payload


def _normalized_cookie_names(payload_dict: Dict[str, Any]) -> List[str]:
    raw_cookie_names = payload_dict.get("cookie_names")
    if isinstance(raw_cookie_names, list):
        normalized = [str(name).strip() for name in raw_cookie_names if str(name).strip()]
        if normalized:
            return normalized

    raw_cookies = payload_dict.get("cookies")
    if isinstance(raw_cookies, dict):
        return sorted(str(name).strip() for name in raw_cookies.keys() if str(name).strip())

    return []


def load_credentials(client: Client, user_id: str) -> Optional[Dict[str, Any]]:
    response = client.table(NOTEBOOK_CREDENTIALS_TABLE).select("*").eq("user_id", user_id).limit(1).execute()
    rows = _response_rows(response)
    return rows[0] if rows else None


def list_keepalive_candidates(client: Client) -> List[Dict[str, Any]]:
    response = client.table(NOTEBOOK_CREDENTIALS_TABLE).select("*").eq("status", "valid").execute()
    return _response_rows(response)


def record_credentials_event(
    client: Client,
    user_id: str,
    *,
    event_type: str,
    source: str,
    ok: Optional[bool] = None,
    status_before: str = "",
    status_after: str = "",
    last_error: str = "",
    duration_ms: Optional[int] = None,
    cookie_count: Optional[int] = None,
    failure_count: Optional[int] = None,
    checked_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Registra un evento no sensible de credenciales NotebookLM.

    El log es observabilidad: si la tabla aun no existe, no debe romper el flujo principal.
    """
    normalized_user_id = (user_id or "").strip()
    normalized_event_type = (event_type or "").strip()
    if not normalized_user_id or not normalized_event_type:
        return None

    row: Dict[str, Any] = {
        "user_id": normalized_user_id,
        "event_type": normalized_event_type,
        "source": (source or "").strip(),
        "ok": ok,
        "status_before": (status_before or "").strip(),
        "status_after": (status_after or "").strip(),
        "checked_at": iso_utc(checked_at),
        "last_error": (last_error or "").strip()[:1200],
        "metadata": metadata or {},
    }
    if duration_ms is not None:
        row["duration_ms"] = max(0, int(duration_ms))
    if cookie_count is not None:
        row["cookie_count"] = max(0, int(cookie_count))
    if failure_count is not None:
        row["failure_count"] = max(0, int(failure_count))

    try:
        response = client.table(NOTEBOOK_CREDENTIALS_EVENTS_TABLE).insert(row).execute()
        rows = _response_rows(response)
        return rows[0] if rows else row
    except Exception as exc:  # noqa: BLE001
        print(f"[notebook-auth] No se pudo registrar evento {normalized_event_type}: {exc}")
        return None


def store_credentials(
    client: Client,
    user_id: str,
    payload_dict: Dict[str, Any],
    *,
    status: str = "valid",
    validated_at: Optional[datetime] = None,
    last_checked_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    previous_row = load_credentials(client, user_id)
    cookie_names = _normalized_cookie_names(payload_dict)
    validated_at_iso = iso_utc(validated_at)
    last_checked_at_iso = iso_utc(last_checked_at or validated_at)
    row = {
        "user_id": user_id,
        "payload_enc": encrypt_payload(payload_dict),
        "cookie_names": cookie_names,
        "validated_at": validated_at_iso,
        "last_checked_at": last_checked_at_iso,
        "last_used_at": None,
        "status": status,
        "last_error": "",
        "failure_count": 0,
    }
    client.table(NOTEBOOK_CREDENTIALS_TABLE).upsert(row, on_conflict="user_id").execute()
    stored_row = load_credentials(client, user_id) or row
    record_credentials_event(
        client,
        user_id,
        event_type="store",
        source="credentials_store",
        ok=status == "valid",
        status_before=str((previous_row or {}).get("status") or "missing"),
        status_after=str(stored_row.get("status") or status),
        cookie_count=len(cookie_names),
        failure_count=0,
        checked_at=parse_timestamp(last_checked_at_iso),
    )
    return stored_row


def delete_credentials(client: Client, user_id: str) -> bool:
    client.table(NOTEBOOK_CREDENTIALS_TABLE).delete().eq("user_id", user_id).execute()
    return True


def mark_credentials_status(
    client: Client,
    user_id: str,
    *,
    status: str,
    last_error: str = "",
    increment_failure: bool = False,
    reset_failure: bool = False,
    last_checked_at: Optional[datetime] = None,
    last_used_at: Optional[datetime] = None,
    event_type: str = "",
    event_source: str = "",
    event_ok: Optional[bool] = None,
    event_duration_ms: Optional[int] = None,
    event_cookie_count: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    current_row = load_credentials(client, user_id)
    if not current_row:
        return None

    current_failure_count = int(current_row.get("failure_count") or 0)
    if reset_failure:
        failure_count = 0
    elif increment_failure:
        failure_count = current_failure_count + 1
    else:
        failure_count = current_failure_count

    updates: Dict[str, Any] = {
        "status": status,
        "last_error": (last_error or "").strip(),
        "failure_count": failure_count,
        "last_checked_at": iso_utc(last_checked_at),
    }
    if last_used_at is not None:
        updates["last_used_at"] = iso_utc(last_used_at)

    client.table(NOTEBOOK_CREDENTIALS_TABLE).update(updates).eq("user_id", user_id).execute()
    updated_row = load_credentials(client, user_id)

    if event_type:
        cookie_names = (updated_row or current_row).get("cookie_names") or []
        cookie_count = (
            event_cookie_count
            if event_cookie_count is not None
            else len(cookie_names)
            if isinstance(cookie_names, list)
            else None
        )
        record_credentials_event(
            client,
            user_id,
            event_type=event_type,
            source=event_source or event_type,
            ok=event_ok,
            status_before=str(current_row.get("status") or "unknown"),
            status_after=str((updated_row or {}).get("status") or status),
            last_error=last_error,
            duration_ms=event_duration_ms,
            cookie_count=cookie_count,
            failure_count=failure_count,
            checked_at=parse_timestamp(updates["last_checked_at"]),
        )

    return updated_row


def touch_last_used(client: Client, user_id: str) -> Optional[Dict[str, Any]]:
    now = utcnow()
    return mark_credentials_status(
        client,
        user_id,
        status="valid",
        last_error="",
        reset_failure=True,
        last_checked_at=now,
        last_used_at=now,
        event_type="operation_success",
        event_source="notebook_operation",
        event_ok=True,
    )


def record_keepalive_result(
    client: Client,
    user_id: str,
    *,
    ok: bool,
    last_error: str = "",
    expired: bool = False,
    duration_ms: Optional[int] = None,
    cookie_count: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    now = utcnow()
    if ok:
        return mark_credentials_status(
            client,
            user_id,
            status="valid",
            last_error="",
            reset_failure=True,
            last_checked_at=now,
            event_type="keepalive",
            event_source="keepalive",
            event_ok=True,
            event_duration_ms=duration_ms,
            event_cookie_count=cookie_count,
        )

    return mark_credentials_status(
        client,
        user_id,
        status="expired" if expired else "valid",
        last_error=last_error,
        increment_failure=True,
        last_checked_at=now,
        event_type="keepalive",
        event_source="keepalive",
        event_ok=False,
        event_duration_ms=duration_ms,
        event_cookie_count=cookie_count,
    )
