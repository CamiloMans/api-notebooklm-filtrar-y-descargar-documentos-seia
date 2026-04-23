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


def store_credentials(
    client: Client,
    user_id: str,
    payload_dict: Dict[str, Any],
    *,
    status: str = "valid",
    validated_at: Optional[datetime] = None,
    last_checked_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    validated_at_iso = iso_utc(validated_at)
    last_checked_at_iso = iso_utc(last_checked_at or validated_at)
    row = {
        "user_id": user_id,
        "payload_enc": encrypt_payload(payload_dict),
        "cookie_names": _normalized_cookie_names(payload_dict),
        "validated_at": validated_at_iso,
        "last_checked_at": last_checked_at_iso,
        "last_used_at": None,
        "status": status,
        "last_error": "",
        "failure_count": 0,
    }
    client.table(NOTEBOOK_CREDENTIALS_TABLE).upsert(row, on_conflict="user_id").execute()
    return load_credentials(client, user_id) or row


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
    return load_credentials(client, user_id)


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
    )


def record_keepalive_result(
    client: Client,
    user_id: str,
    *,
    ok: bool,
    last_error: str = "",
    expired: bool = False,
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
        )

    return mark_credentials_status(
        client,
        user_id,
        status="expired" if expired else "valid",
        last_error=last_error,
        increment_failure=True,
        last_checked_at=now,
    )
