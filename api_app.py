#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""API MVP para procesar documento SEIA y persistir notebook en Supabase."""

import asyncio
import base64
import hashlib
import json
import math
import os
import re
import time
import threading
import unicodedata
import uuid
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.parse import urlparse

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from supabase import Client, create_client

from notebooklm import (
    AuthError,
    AuthTokens,
    NotebookLMClient,
    RPCError,
    SharePermission,
    ShareViewLevel,
)
from notebooklm.auth import MINIMUM_REQUIRED_COOKIES, _is_allowed_auth_domain, fetch_tokens

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from download_documento_seia import (
    DEFAULT_OUTPUT,
    NOTEBOOK_SOURCES_PER_NOTEBOOK,
    NOTEBOOK_UPLOAD_LIMIT,
    NotebookAPIError,
    NotebookCredentialsExpired,
    _long_path,
    _path_exists,
    build_adenda_notebook_title,
    build_notebook_upload_filename,
    build_tipo_notebook_title,
    compact_component,
    extract_id_documento,
    force_https,
    list_notebook_sources,
    notify_notebook_api,
    prepare_notebook_client_seed,
    run_seia_notebook_pipeline,
    upload_documents_batch_and_single,
)
from keepalive_worker import run_keepalive_loop
from notebook_auth_store import (
    compute_days_until_soft_expiry,
    decrypt_payload,
    delete_credentials,
    load_credentials,
    mark_credentials_status,
    parse_timestamp,
    store_credentials,
    touch_last_used,
    update_cookies,
)

if load_dotenv:
    load_dotenv()


API_BASE_DIR = Path(__file__).resolve().parent
API_BEARER_TOKEN = os.getenv("API_BEARER_TOKEN", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
NOTEBOOK_API_BASE_URL = os.getenv("NOTEBOOK_API_BASE_URL", "http://127.0.0.1:8001").strip()
API_OUTPUT_ROOT = Path(os.getenv("API_OUTPUT_ROOT", str(DEFAULT_OUTPUT / "api_runs")))
if not API_OUTPUT_ROOT.is_absolute():
    API_OUTPUT_ROOT = (API_BASE_DIR / API_OUTPUT_ROOT).resolve()
CORS_ORIGINS_RAW = os.getenv(
    "API_CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000",
).strip()


def getenv_positive_int(name: str, default: int) -> int:
    """Lee enteros positivos desde entorno usando un fallback seguro."""
    raw_value = os.getenv(name, str(default)).strip()
    try:
        parsed_value = int(raw_value)
    except ValueError:
        return default
    return max(1, parsed_value)


MAX_CONCURRENT_JOBS = getenv_positive_int("MAX_CONCURRENT_JOBS", 3)
_SUPABASE_CLIENT: Optional[Client] = None
_JOB_SEMAPHORE = threading.BoundedSemaphore(MAX_CONCURRENT_JOBS)
RETRYABLE_UPLOAD_STATUSES = ("failed", "not_uploaded", "selected", "uploading", "pending")
NOTEBOOK_AUTH_HEADER = "X-NotebookLM-Auth"
NOTEBOOK_USER_JWT_HEADER = "X-Myma-User-JWT"
ZIP_EXPORT_PART_SIZE_BYTES = getenv_positive_int("ZIP_EXPORT_PART_SIZE_BYTES", 8 * 1024 * 1024)
ZIP_ENTRY_FILENAME_MAX_CHARS = max(32, getenv_positive_int("ZIP_ENTRY_FILENAME_MAX_CHARS", 140))
ZIP_ENTRY_PART_LIMITS = (18, 32, 42, 42)
NOTEBOOK_AUTH_SOFT_EXPIRY_DAYS = getenv_positive_int("NOTEBOOK_AUTH_SOFT_EXPIRY_DAYS", 14)
NOTEBOOK_KEEPALIVE_ENABLED = os.getenv("NOTEBOOK_KEEPALIVE_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
NOTEBOOK_KEEPALIVE_INTERVAL_SEC = getenv_positive_int("NOTEBOOK_KEEPALIVE_INTERVAL_SEC", 1800)
NOTEBOOK_KEEPALIVE_ACTIVE_DAYS = getenv_positive_int("NOTEBOOK_KEEPALIVE_ACTIVE_DAYS", 7)
NOTEBOOK_KEEPALIVE_MAX_CONCURRENCY = getenv_positive_int("NOTEBOOK_KEEPALIVE_MAX_CONCURRENCY", 2)
NOTEBOOK_KEEPALIVE_TIMEOUT_SEC = getenv_positive_int("NOTEBOOK_KEEPALIVE_TIMEOUT_SEC", 20)


def validate_notebook_source_capacity(
    document_count: int,
    *,
    notebook_id: Optional[str] = None,
    notebook_auth: Optional[Dict[str, Any]] = None,
    auth_seed: Optional[Dict[str, Any]] = None,
) -> None:
    """Evita cargas que exceden el limite de fuentes de NotebookLM por notebook."""
    if NOTEBOOK_SOURCES_PER_NOTEBOOK <= 0 or document_count <= 0:
        return

    current_sources = 0
    normalized_notebook_id = (notebook_id or "").strip()
    if normalized_notebook_id:
        try:
            current_sources = len(
                list_notebook_sources(
                    normalized_notebook_id,
                    api_base_url=NOTEBOOK_API_BASE_URL,
                    notebook_auth=notebook_auth,
                    auth_seed=auth_seed,
                )
            )
            _touch_stored_notebook_credentials(notebook_auth)
        except Exception as e:
            _mark_stored_notebook_credentials_failure(notebook_auth, e)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=(
                    "No se pudo validar cuantas fuentes tiene el notebook destino. "
                    f"Valida las cookies de NotebookLM e intenta nuevamente. Detalle: {e}"
                ),
            ) from e

    available_slots = max(0, NOTEBOOK_SOURCES_PER_NOTEBOOK - current_sources)
    if document_count <= available_slots:
        return

    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=(
            "La seleccion excede la capacidad configurada de NotebookLM: "
            f"{NOTEBOOK_SOURCES_PER_NOTEBOOK} fuente(s) por notebook. "
            f"El notebook destino ya tiene {current_sources} fuente(s), "
            f"quedan {available_slots} cupo(s), y seleccionaste {document_count}. "
            "Selecciona menos documentos, crea otro notebook para el siguiente lote, "
            "o ajusta NOTEBOOK_SOURCES_PER_NOTEBOOK si tu cuenta tiene un limite mayor."
        ),
    )


def parse_cors_origins(raw_value: str) -> List[str]:
    """Parsea origenes CORS desde variable de entorno separada por comas."""
    origins = []
    for item in (raw_value or "").split(","):
        value = item.strip()
        if value.endswith("/"):
            value = value.rstrip("/")
        if value:
            origins.append(value)
    return origins


def decode_notebook_auth_header_value(raw_value: str) -> Dict[str, Any]:
    """Decodifica el payload compacto de auth NotebookLM enviado por header."""
    normalized_value = (raw_value or "").strip()
    if not normalized_value:
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} no puede venir vacio.")

    padding = "=" * (-len(normalized_value) % 4)
    try:
        decoded_bytes = base64.urlsafe_b64decode(f"{normalized_value}{padding}".encode("ascii"))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} no tiene un base64url valido.") from exc

    try:
        payload = json.loads(decoded_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} no contiene un JSON valido.") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} debe contener un objeto JSON.")
    if payload.get("version") != 1:
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} debe incluir version=1.")

    raw_cookies = payload.get("cookies")
    if not isinstance(raw_cookies, dict):
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} debe incluir un objeto 'cookies'.")

    cookies: Dict[str, str] = {}
    for raw_name, raw_value in raw_cookies.items():
        if raw_value is None:
            continue
        name = str(raw_name).strip()
        value = str(raw_value)
        if name and value:
            cookies[name] = value

    if "SID" not in cookies:
        raise ValueError(f"{NOTEBOOK_AUTH_HEADER} no incluye la cookie SID.")

    return {
        "version": 1,
        "cookies": cookies,
        "cookie_names": [
            str(name).strip()
            for name in payload.get("cookie_names", [])
            if str(name).strip()
        ] or sorted(cookies.keys()),
        "cookie_domains": [
            str(domain).strip()
            for domain in payload.get("cookie_domains", [])
            if str(domain).strip()
        ],
    }


class CreateNotebookRequest(BaseModel):
    documento_seia: str = Field(..., description="URL de documento SEIA")
    id_adenda: int = Field(..., gt=0, description="ID de la tabla adendas")


class CreateCP6BRequest(BaseModel):
    documento_seia: str = Field(..., description="URL de documento SEIA")
    tipo: str = Field(..., min_length=1, description="Prefijo semantico que se antepone al nombre final de los archivos.")
    exclude_keywords: List[str] = Field(
        default_factory=list,
        description="Palabras a excluir del listado CP6B. Si viene vacio, no se aplica filtro por palabras.",
    )


class CreateNotebookResponse(BaseModel):
    status: str
    id_adenda: int
    id_documento: str
    notebooklm_id: str
    nombre_notebooklm: str
    documents_found: int
    documents_uploaded_ok: int
    documents_uploaded_failed: int
    output_dir: str
    elapsed_seconds: float


class CP6BDocumentResponse(BaseModel):
    document_id: str
    seleccionar: bool
    selected: bool
    categoria: str
    texto_link: str
    url_origen: str
    nombre_archivo: str
    nombre_archivo_final: str
    nombre_archivo_notebook: str
    nombre_para_notebook: str
    formato: str
    ruta_relativa: str
    tamano_bytes: int
    nivel_descarga_descompresion: int
    origen: str
    upload_status: str


class CreateCP6BResponse(BaseModel):
    status: str
    run_id: str
    tipo: str
    id_documento: str
    documents_found: int
    documents: List[CP6BDocumentResponse]


class CP6BStatusResponse(CreateCP6BResponse):
    progress_stage: str
    progress_current: int
    progress_total: int
    progress_percent: int
    progress_message: str
    error_message: str
    notebooklm_id: str = ""
    nombre_notebooklm: str = ""
    retry_attempts: int = 0
    retry_documents_count: int = 0
    retry_document_ids: List[str] = []


class UploadSelectionRequest(BaseModel):
    run_id: str = Field(..., description="ID de corrida devuelto por /adenda/descarga-documentos-seia")
    selected_document_ids: List[str] = Field(..., description="IDs de documentos seleccionados por el frontend")
    nombre_notebook: Optional[str] = Field(
        default=None,
        description="Nombre del notebook a crear. Debe enviarse cuando se quiere crear un notebook nuevo.",
    )
    notebook_id: Optional[str] = Field(
        default=None,
        description="ID de un notebook existente. Debe enviarse cuando se quiere reutilizar un notebook ya creado.",
    )


class UploadSelectionResponse(BaseModel):
    status: str
    run_id: str
    tipo: str
    id_documento: str
    notebooklm_id: str = ""
    nombre_notebooklm: str
    documents_uploaded_ok: int
    documents_uploaded_failed: int
    retry_attempts: int = 0
    retry_documents_count: int = 0
    retry_document_ids: List[str] = []
    selected_documents: List[CP6BDocumentResponse]


class RetryUploadRequest(BaseModel):
    run_id: str = Field(..., description="ID de corrida previamente cargada al notebook.")


class RetryUploadResponse(BaseModel):
    status: str
    run_id: str
    notebooklm_id: str
    documents_uploaded_ok: int
    documents_uploaded_failed: int
    retry_attempts: int = 0
    retry_documents_count: int = 0
    retry_document_ids: List[str] = []
    selected_documents: List[CP6BDocumentResponse]


class DownloadSelectedDocumentsZipRequest(BaseModel):
    selected_document_ids: List[str] = Field(
        ...,
        description="IDs de documentos visibles/seleccionados por el frontend para exportar en zip.",
    )


class SelectedDocumentsZipExportResponse(BaseModel):
    export_id: str
    filename: str
    size_bytes: int
    part_size_bytes: int
    parts: int


class ShareSetPublicRequest(BaseModel):
    public: bool = Field(..., description="True habilita sharing publico; False lo deshabilita.")


class ShareSetViewLevelRequest(BaseModel):
    view_level: Literal["full_notebook", "chat_only"] = Field(
        ..., description="Scope para viewers del notebook compartido."
    )


class ShareAddUserRequest(BaseModel):
    email: str = Field(..., min_length=3, description="Email del usuario a invitar.")
    permission: Literal["viewer", "editor"] = Field(default="viewer")
    notify: bool = Field(default=True)
    welcome_message: str = Field(default="")


class ShareUpdateUserRequest(BaseModel):
    permission: Literal["viewer", "editor"] = Field(..., description="Nuevo nivel de permiso.")


class ValidateCookiesRequest(BaseModel):
    cookies_text: str = Field(
        ...,
        min_length=1,
        description="Cookies Netscape o storage JSON de Playwright para autenticar NotebookLM.",
    )


class NotebookCredentialsStoreRequest(BaseModel):
    cookies_text: str = Field(
        ...,
        min_length=1,
        description="Cookies Netscape o storage JSON de Playwright para guardar en la cuenta MyMA.",
    )


class NotebookCredentialsStatusResponse(BaseModel):
    has_credentials: bool
    valid: bool
    status: str
    validated_at: Optional[str] = None
    last_checked_at: Optional[str] = None
    last_used_at: Optional[str] = None
    cookie_names: List[str] = Field(default_factory=list)
    days_until_soft_expiry: Optional[int] = None
    last_error: str = ""
    failure_count: int = 0
    keepalive_enabled: bool


class NotebookCredentialsDeleteResponse(BaseModel):
    deleted: bool


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    keepalive_task: Optional[asyncio.Task[Any]] = None
    if NOTEBOOK_KEEPALIVE_ENABLED:
        keepalive_task = asyncio.create_task(
            run_keepalive_loop(
                get_client=get_supabase_client,
                interval_sec=NOTEBOOK_KEEPALIVE_INTERVAL_SEC,
                active_days=NOTEBOOK_KEEPALIVE_ACTIVE_DAYS,
                max_concurrency=NOTEBOOK_KEEPALIVE_MAX_CONCURRENCY,
                timeout_sec=NOTEBOOK_KEEPALIVE_TIMEOUT_SEC,
            )
        )
        app.state.notebook_keepalive_task = keepalive_task

    try:
        yield
    finally:
        if keepalive_task is not None:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass


app = FastAPI(
    title="SEIA Notebook API",
    version="1.0.0",
    description="Crea notebook desde documento SEIA y persiste notebooklm_id en Supabase.",
    lifespan=app_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=parse_cors_origins(CORS_ORIGINS_RAW),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_supabase_client() -> Client:
    """Retorna cliente Supabase singleton."""
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("SUPABASE_URL/SUPABASE_KEY no configurados.")
        _SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _SUPABASE_CLIENT


def _resolve_user_id_from_jwt(
    raw_jwt: Optional[str],
    *,
    raise_if_missing: bool,
) -> Optional[str]:
    normalized_jwt = (raw_jwt or "").strip()
    if not normalized_jwt:
        if raise_if_missing:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Falta header {NOTEBOOK_USER_JWT_HEADER} valido.",
            )
        return None

    try:
        user_response = get_supabase_client().auth.get_user(normalized_jwt)
        user = getattr(user_response, "user", None)
        user_id = str(getattr(user, "id", "") or "").strip()
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No fue posible validar la sesion del usuario.",
        ) from exc

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No fue posible identificar al usuario autenticado.",
        )
    return user_id


def get_current_user_id(
    x_myma_user_jwt: Optional[str] = Header(default=None, alias=NOTEBOOK_USER_JWT_HEADER),
) -> str:
    return _resolve_user_id_from_jwt(x_myma_user_jwt, raise_if_missing=True) or ""


def _row_timestamp_to_iso(value: Any) -> Optional[str]:
    parsed = parse_timestamp(value)
    return parsed.isoformat() if parsed is not None else None


def _notebook_credentials_status_payload(
    row: Optional[Dict[str, Any]],
) -> NotebookCredentialsStatusResponse:
    if not row:
        return NotebookCredentialsStatusResponse(
            has_credentials=False,
            valid=False,
            status="missing",
            validated_at=None,
            last_checked_at=None,
            last_used_at=None,
            cookie_names=[],
            days_until_soft_expiry=None,
            last_error="",
            failure_count=0,
            keepalive_enabled=NOTEBOOK_KEEPALIVE_ENABLED,
        )

    normalized_status = str(row.get("status") or "").strip() or "unknown"
    return NotebookCredentialsStatusResponse(
        has_credentials=True,
        valid=normalized_status == "valid",
        status=normalized_status,
        validated_at=_row_timestamp_to_iso(row.get("validated_at")),
        last_checked_at=_row_timestamp_to_iso(row.get("last_checked_at")),
        last_used_at=_row_timestamp_to_iso(row.get("last_used_at")),
        cookie_names=[str(name).strip() for name in (row.get("cookie_names") or []) if str(name).strip()],
        days_until_soft_expiry=compute_days_until_soft_expiry(
            row.get("validated_at"),
            soft_expiry_days=NOTEBOOK_AUTH_SOFT_EXPIRY_DAYS,
        ),
        last_error=str(row.get("last_error") or "").strip(),
        failure_count=to_int(row.get("failure_count")),
        keepalive_enabled=NOTEBOOK_KEEPALIVE_ENABLED,
    )


def _revalidate_stored_notebook_credentials(user_id: str) -> NotebookCredentialsStatusResponse:
    client = get_supabase_client()
    started_at = time.perf_counter()
    cookie_count = None

    def _elapsed_ms() -> int:
        return int((time.perf_counter() - started_at) * 1000)

    try:
        row = load_credentials(client, user_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No se pudo consultar el estado de las credenciales NotebookLM: {exc}",
        ) from exc

    if not row:
        return _notebook_credentials_status_payload(None)

    try:
        payload = decrypt_payload(str(row.get("payload_enc") or ""))
        cookies = payload.get("cookies") if isinstance(payload, dict) else None
        if not isinstance(cookies, dict) or not cookies:
            raise ValueError("Las credenciales guardadas no incluyen cookies validas.")
        cookie_count = len(cookies)

        asyncio.run(
            asyncio.wait_for(
                fetch_tokens(dict(cookies)),
                timeout=max(1, NOTEBOOK_KEEPALIVE_TIMEOUT_SEC),
            )
        )
    except Exception as exc:  # noqa: BLE001
        message = str(exc).strip() or exc.__class__.__name__
        is_expired = isinstance(exc, ValueError) or _is_notebook_auth_failure(exc)
        updated_row = mark_credentials_status(
            client,
            user_id,
            status="expired" if is_expired else "valid",
            last_error=message,
            increment_failure=True,
            event_type="revalidate",
            event_source="manual_revalidate",
            event_ok=False,
            event_duration_ms=_elapsed_ms(),
            event_cookie_count=cookie_count,
        )
        return _notebook_credentials_status_payload(updated_row or row)

    updated_row = mark_credentials_status(
        client,
        user_id,
        status="valid",
        last_error="",
        reset_failure=True,
        event_type="revalidate",
        event_source="manual_revalidate",
        event_ok=True,
        event_duration_ms=_elapsed_ms(),
        event_cookie_count=cookie_count,
    )
    return _notebook_credentials_status_payload(updated_row or row)


def _is_stored_notebook_credentials_payload(notebook_auth: Optional[Dict[str, Any]]) -> bool:
    return bool(
        isinstance(notebook_auth, dict)
        and str(notebook_auth.get("_credentials_source") or "").strip() == "stored"
        and str(notebook_auth.get("_credentials_user_id") or "").strip()
    )


def _stored_notebook_credentials_user_id(notebook_auth: Optional[Dict[str, Any]]) -> str:
    if not _is_stored_notebook_credentials_payload(notebook_auth):
        return ""
    return str(notebook_auth.get("_credentials_user_id") or "").strip()


def _touch_stored_notebook_credentials(notebook_auth: Optional[Dict[str, Any]]) -> None:
    user_id = _stored_notebook_credentials_user_id(notebook_auth)
    if not user_id:
        return
    try:
        touch_last_used(get_supabase_client(), user_id)
    except Exception as exc:  # noqa: BLE001
        print(f"[notebook-auth] No se pudo actualizar last_used_at para {user_id}: {exc}")


def _is_notebook_auth_failure(exc: Exception) -> bool:
    if isinstance(exc, AuthError):
        return True

    message = str(exc).strip().lower()
    if not message:
        return False

    auth_markers = (
        "accounts.google.com",
        "auth notebooklm",
        "cookie sid",
        "cookies notebooklm",
        "expirada",
        "expired",
        "falta header x-notebooklm-auth",
        "failed to fetch tokens",
        "invalid notebooklm credentials",
        "login",
        "no fue posible validar la sesion",
        "no incluye la cookie sid",
        "sesion notebooklm",
        "unauthorized",
    )
    return any(marker in message for marker in auth_markers)


def _mark_stored_notebook_credentials_failure(
    notebook_auth: Optional[Dict[str, Any]],
    exc: Exception,
) -> None:
    user_id = _stored_notebook_credentials_user_id(notebook_auth)
    if not user_id:
        return

    message = str(exc).strip() or exc.__class__.__name__
    next_status = "expired" if _is_notebook_auth_failure(exc) else "valid"
    try:
        mark_credentials_status(
            get_supabase_client(),
            user_id,
            status=next_status,
            last_error=message,
            increment_failure=True,
            event_type="operation_failure",
            event_source="notebook_operation",
            event_ok=False,
        )
    except Exception as mark_exc:  # noqa: BLE001
        print(f"[notebook-auth] No se pudo actualizar estado de credenciales para {user_id}: {mark_exc}")


def require_bearer_token(authorization: Optional[str] = Header(default=None)) -> None:
    """Valida Authorization bearer token."""
    if not API_BEARER_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API_BEARER_TOKEN no configurado en entorno.",
        )

    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token.strip() != API_BEARER_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_notebook_auth_payload(
    x_notebooklm_auth: Optional[str] = Header(default=None, alias=NOTEBOOK_AUTH_HEADER),
    x_myma_user_jwt: Optional[str] = Header(default=None, alias=NOTEBOOK_USER_JWT_HEADER),
) -> Optional[Dict[str, Any]]:
    """Resuelve auth NotebookLM priorizando credenciales guardadas por usuario."""
    has_explicit_auth = bool(x_notebooklm_auth and x_notebooklm_auth.strip())
    user_id = None
    if x_myma_user_jwt and x_myma_user_jwt.strip():
        try:
            user_id = _resolve_user_id_from_jwt(x_myma_user_jwt, raise_if_missing=False)
        except HTTPException:
            if not has_explicit_auth:
                raise

    if user_id:
        try:
            credentials_row = load_credentials(get_supabase_client(), user_id)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"No se pudieron cargar las credenciales NotebookLM del usuario: {exc}",
            ) from exc

        if credentials_row:
            status_value = str(credentials_row.get("status") or "").strip()
            if status_value == "valid":
                try:
                    payload = decrypt_payload(str(credentials_row.get("payload_enc") or ""))
                except ValueError as exc:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"No se pudieron decodificar las credenciales NotebookLM guardadas: {exc}",
                    ) from exc
                except Exception as exc:  # noqa: BLE001
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"No se pudo abrir el almacenamiento cifrado de NotebookLM: {exc}",
                    ) from exc

                if not isinstance(payload, dict):
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Las credenciales NotebookLM guardadas tienen un formato invalido.",
                    )

                payload = dict(payload)
                payload["_credentials_source"] = "stored"
                payload["_credentials_user_id"] = user_id
                return payload

    if has_explicit_auth:
        try:
            return decode_notebook_auth_header_value(x_notebooklm_auth or "")
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc

    return None


def _parse_share_permission(value: str) -> SharePermission:
    return {"viewer": SharePermission.VIEWER, "editor": SharePermission.EDITOR}[value]


def _parse_share_view_level(value: str) -> ShareViewLevel:
    return {
        "full_notebook": ShareViewLevel.FULL_NOTEBOOK,
        "chat_only": ShareViewLevel.CHAT_ONLY,
    }[value]


def _validate_email(email: str) -> str:
    cleaned = (email or "").strip()
    if "@" not in cleaned:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="email debe tener formato valido (ej: usuario@dominio.com).",
        )
    local, _, domain = cleaned.partition("@")
    if not local or "." not in domain:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="email debe tener formato valido (ej: usuario@dominio.com).",
        )
    return cleaned


def _share_status_payload(share_status: Any) -> Dict[str, Any]:
    users = [
        {
            "email": user.email,
            "permission": user.permission.name.lower(),
            "display_name": user.display_name,
            "avatar_url": user.avatar_url,
        }
        for user in share_status.shared_users
    ]
    return {
        "notebook_id": share_status.notebook_id,
        "is_public": share_status.is_public,
        "access": share_status.access.name.lower(),
        "view_level": share_status.view_level.name.lower(),
        "share_url": share_status.share_url,
        "shared_users": users,
    }


def _map_notebook_share_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, HTTPException):
        return exc
    message = str(exc).strip() or exc.__class__.__name__
    if isinstance(exc, AuthError):
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Sesion NotebookLM invalida o expirada: {message}",
        )
    if isinstance(exc, RPCError):
        return HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Error RPC NotebookLM: {message}",
        )
    if isinstance(exc, ValueError):
        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message,
        )
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=message,
    )


def run_notebook_share_operation(
    notebook_auth: Optional[Dict[str, Any]],
    operation: Callable[[NotebookLMClient], Any],
    *,
    timeout: float = 60.0,
) -> Any:
    """Crea NotebookLMClient por request con auth del header y ejecuta operacion async."""
    if not notebook_auth:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Falta header {NOTEBOOK_AUTH_HEADER} valido con cookies NotebookLM.",
        )

    cookies = notebook_auth.get("cookies") or {}
    if not cookies:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"{NOTEBOOK_AUTH_HEADER} no contiene cookies validas.",
        )

    async def _run() -> Any:
        csrf_token, session_id = await fetch_tokens(dict(cookies))
        tokens = AuthTokens(
            cookies=dict(cookies),
            csrf_token=str(csrf_token),
            session_id=str(session_id),
        )
        async with NotebookLMClient(tokens, timeout=timeout) as client:
            return await operation(client)

    try:
        result = asyncio.run(_run())
        _touch_stored_notebook_credentials(notebook_auth)
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        _mark_stored_notebook_credentials_failure(notebook_auth, exc)
        raise _map_notebook_share_exception(exc) from exc


def _parse_netscape_cookies_text(raw_text: str) -> Dict[str, Any]:
    """Convierte cookies Netscape a un storage state compatible con Playwright."""
    cookies: List[Dict[str, Any]] = []

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        http_only = False
        if line.startswith("#HttpOnly_"):
            http_only = True
            line = line[len("#HttpOnly_") :]
        elif line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) != 7:
            continue

        domain, include_subdomains, cookie_path, secure_flag, expires, name, value = parts
        normalized_domain = domain.strip()
        if not normalized_domain or not str(name).strip():
            continue

        try:
            expires_value = int(str(expires).strip())
        except (TypeError, ValueError):
            expires_value = -1

        cookies.append(
            {
                "name": str(name).strip(),
                "value": str(value),
                "domain": normalized_domain,
                "path": cookie_path.strip() or "/",
                "expires": expires_value,
                "httpOnly": http_only,
                "secure": secure_flag.strip().upper() == "TRUE",
                "sameSite": "Lax",
                "includeSubdomains": include_subdomains.strip().upper() == "TRUE",
            }
        )

    return {"cookies": cookies, "origins": []}


def _normalize_storage_state_from_text(raw_text: str) -> tuple[Dict[str, Any], str]:
    """Detecta el formato pegado y lo normaliza a storage state."""
    cleaned_text = (raw_text or "").strip()
    if not cleaned_text:
        raise ValueError("Debes pegar cookies antes de validarlas.")

    try:
        parsed_json = json.loads(cleaned_text)
    except json.JSONDecodeError:
        storage_state = _parse_netscape_cookies_text(cleaned_text)
        if not storage_state["cookies"]:
            raise ValueError(
                "No se pudo interpretar el texto pegado como cookies Netscape ni como storage JSON."
            ) from None
        return storage_state, "netscape_text"

    if isinstance(parsed_json, dict) and isinstance(parsed_json.get("cookies"), list):
        return {
            "cookies": list(parsed_json.get("cookies") or []),
            "origins": list(parsed_json.get("origins") or []),
        }, "playwright_storage_json"

    raise ValueError(
        "El JSON pegado no tiene formato valido. Se esperaba un storage state con la clave 'cookies'."
    )


def _select_auth_cookies_from_storage(
    storage_state: Dict[str, Any],
) -> tuple[Dict[str, str], Dict[str, str], List[str]]:
    """Selecciona cookies utiles para NotebookLM respetando dominios admitidos."""
    cookies: Dict[str, str] = {}
    cookie_domains: Dict[str, str] = {}

    for cookie in storage_state.get("cookies", []):
        if not isinstance(cookie, dict):
            continue

        domain = str(cookie.get("domain", "")).strip()
        name = str(cookie.get("name", "")).strip()
        value = str(cookie.get("value", ""))
        if not _is_allowed_auth_domain(domain) or not name or not value:
            continue

        is_base_domain = domain == ".google.com"
        if name not in cookies or is_base_domain:
            cookies[name] = value
            cookie_domains[name] = domain

    missing_required = sorted(MINIMUM_REQUIRED_COOKIES - set(cookies.keys()))
    return cookies, cookie_domains, missing_required


def _allowed_cookie_domains_from_storage(storage_state: Dict[str, Any]) -> List[str]:
    """Retorna dominios Google validos presentes en las cookies pegadas."""
    domains = {
        str(cookie.get("domain", "")).strip()
        for cookie in storage_state.get("cookies", [])
        if isinstance(cookie, dict)
        and _is_allowed_auth_domain(str(cookie.get("domain", "")).strip())
    }
    return sorted(domain for domain in domains if domain)


def _build_compact_auth_payload_from_storage(
    storage_state: Dict[str, Any],
) -> tuple[Dict[str, Any], List[str]]:
    """Genera el payload compacto que la UI guarda y reenvia por header."""
    cookies, cookie_domains_by_name, missing_required = _select_auth_cookies_from_storage(
        storage_state
    )
    payload = {
        "version": 1,
        "cookies": cookies,
        "cookie_names": sorted(cookies.keys()),
        "cookie_domains": sorted(set(cookie_domains_by_name.values())),
    }
    return payload, missing_required


def validate_documento_seia(documento_seia: str) -> str:
    """Valida URL de documento SEIA y retorna versión normalizada."""
    raw = (documento_seia or "").strip()
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="documento_seia es requerido",
        )

    if "://" not in raw:
        raw = f"https://{raw}"
    normalized = force_https(raw)
    parsed = urlparse(normalized)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()

    is_seia_document = "seia.sea.gob.cl" in host and "/documentos/documento.php" in path
    is_infofirma_document = "infofirma.sea.gob.cl" in host and "/documentossea/mostrardocumento" in path
    if not (is_seia_document or is_infofirma_document):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "documento_seia debe apuntar a un documento valido de "
                "seia.sea.gob.cl o infofirma.sea.gob.cl"
            ),
        )

    id_documento = extract_id_documento(normalized)
    if not id_documento or id_documento == "desconocido":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="documento_seia debe incluir query param idDocumento",
        )

    return normalized


def validate_tipo(tipo: str) -> str:
    """Valida y normaliza el tipo usado como prefijo semantico."""
    value = (tipo or "").strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="tipo es requerido",
        )
    return value


def normalize_optional_text(value: Optional[str]) -> Optional[str]:
    """Normaliza texto opcional devolviendo None cuando queda vacio."""
    normalized = (value or "").strip()
    return normalized or None


def safe_id_documento_token(id_documento: str) -> str:
    """Normaliza el id_documento para usarlo en nombres de carpetas locales."""
    return compact_component(str(id_documento or "desconocido"), maxlen=60)


def adenda_exists(client: Client, id_adenda: int) -> bool:
    """Verifica existencia de adenda por id."""
    response = client.table("adendas").select("id").eq("id", id_adenda).limit(1).execute()
    data = getattr(response, "data", None) or []
    return len(data) > 0


def persist_adenda_notebook(client: Client, id_adenda: int, notebook_id: str, notebook_name: str) -> None:
    """Actualiza notebooklm_id y nombre_notebooklm en Supabase."""
    payload = {
        "notebooklm_id": notebook_id,
        "nombre_notebooklm": notebook_name,
    }
    response = client.table("adendas").update(payload).eq("id", id_adenda).execute()
    data = getattr(response, "data", None) or []
    if not data:
        raise RuntimeError("Update de adendas no devolvio filas afectadas.")


def supabase_data(response) -> List[Dict[str, Any]]:
    """Extrae data de una respuesta Supabase."""
    data = getattr(response, "data", None) or []
    return list(data)


def json_safe(value: Any) -> Any:
    """Convierte estructuras de Python a valores seguros para columnas JSON."""
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


def run_metadata_dict(run: Dict[str, Any]) -> Dict[str, Any]:
    """Retorna metadata de corrida como dict mutable y segura."""
    metadata = run.get("metadata")
    return dict(metadata) if isinstance(metadata, dict) else {}


def to_int(value: Any, default: int = 0) -> int:
    """Convierte valores numericos de forma tolerante para respuestas API."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def with_run_context(row: Dict[str, Any], tipo: str = "") -> Dict[str, Any]:
    """Inyecta contexto de corrida en una fila de documento sin mutar el original."""
    enriched = dict(row)
    if tipo and not enriched.get("tipo"):
        enriched["tipo"] = tipo
    return enriched


def public_document_from_row(row: Dict[str, Any], tipo: str = "") -> CP6BDocumentResponse:
    """Construye el contrato publico del documento sin exponer ruta absoluta."""
    row = with_run_context(row, tipo=tipo)
    notebook_name = str(
        row.get("nombre_para_notebook")
        or row.get("nombre_archivo_notebook")
        or build_notebook_upload_filename(row)
    )
    return CP6BDocumentResponse(
        document_id=str(row["id"]),
        seleccionar=bool(row.get("seleccionar", row.get("selected", True))),
        selected=bool(row.get("selected", row.get("seleccionar", True))),
        categoria=str(row.get("categoria") or ""),
        texto_link=str(row.get("texto_link") or ""),
        url_origen=str(row.get("url_origen") or ""),
        nombre_archivo=str(row.get("nombre_archivo") or ""),
        nombre_archivo_final=str(row.get("nombre_archivo_final") or row.get("nombre_archivo") or ""),
        nombre_archivo_notebook=notebook_name,
        nombre_para_notebook=notebook_name,
        formato=str(row.get("formato") or ""),
        ruta_relativa=str(row.get("ruta_relativa") or ""),
        tamano_bytes=to_int(row.get("tamano_bytes")),
        nivel_descarga_descompresion=to_int(row.get("nivel_descarga_descompresion")),
        origen=str(row.get("origen") or ""),
        upload_status=str(row.get("upload_status") or "pending"),
    )


def create_cp6b_run_record(
    client: Client,
    run_id: str,
    tipo: str,
    documento_seia: str,
    id_documento: str,
    output_dir: str,
    exclude_keywords: List[str],
) -> None:
    """Crea corrida inicial para procesamiento asincrono."""
    run_payload = {
        "id": run_id,
        "tipo": tipo,
        "id_documento": id_documento,
        "documento_seia": documento_seia,
        "output_dir": output_dir,
        "status": "queued",
        "metadata": {},
        "docs_report_stats": {},
        "trace_stats": {},
        "exclude_keywords": json_safe(exclude_keywords),
        "progress_stage": "queued",
        "progress_current": 0,
        "progress_total": 0,
        "progress_percent": 0,
        "progress_message": "Corrida creada y en cola.",
        "error_message": "",
    }
    client.table("adenda_document_runs").insert(run_payload).execute()


def persist_cp6b_run_result(
    client: Client,
    run_id: str,
    result: Dict[str, Any],
) -> List[CP6BDocumentResponse]:
    """Persiste resultado final CP6B y documentos enriquecidos."""
    documents = []
    file_payloads = []
    run_tipo = str(result.get("tipo") or "")
    for item in result.get("docs_report") or []:
        document_id = str(uuid.uuid4())
        file_row = {
            "id": document_id,
            "run_id": run_id,
            "nombre_archivo": item.get("nombre_archivo") or "",
            "nombre_archivo_final": item.get("nombre_archivo_final") or item.get("nombre_archivo") or "",
            "extension": item.get("extension") or "",
            "formato": item.get("formato") or "",
            "ruta_relativa": item.get("ruta_relativa") or "",
            "tamano_bytes": to_int(item.get("tamano_bytes")),
            "nivel_descarga_descompresion": to_int(item.get("nivel_descarga_descompresion")),
            "origen": item.get("origen") or "",
            "categoria": item.get("categoria") or "",
            "texto_link": item.get("texto_link") or "",
            "url_origen": item.get("url_origen") or "",
            "seleccionar": bool(item.get("seleccionar", True)),
            "selected": bool(item.get("selected", True)),
            "upload_status": "pending",
            "upload_error": "",
        }
        file_payloads.append(file_row)
        documents.append(public_document_from_row(file_row, tipo=run_tipo))

    if file_payloads:
        client.table("adenda_document_files").delete().eq("run_id", run_id).execute()
        client.table("adenda_document_files").insert(file_payloads).execute()

    client.table("adenda_document_runs").update({
        "status": "listed",
        "metadata": json_safe(result.get("metadata") or {}),
        "docs_report_stats": json_safe(result.get("docs_report_stats") or {}),
        "trace_stats": json_safe(result.get("trace_stats") or {}),
        "listado_excel_path": result.get("excel_path"),
        "trace_excel_path": result.get("trace_excel_path"),
        "progress_stage": "listed",
        "progress_current": len(file_payloads),
        "progress_total": max(1, len(file_payloads)),
        "progress_percent": 100 if file_payloads else 0,
        "progress_message": "Listado CP6B generado.",
        "error_message": "",
    }).eq("id", run_id).execute()
    return documents


def update_run_progress(client: Client, run_id: str, payload: Dict[str, Any]) -> None:
    """Actualiza progreso incremental de la corrida."""
    current = to_int(payload.get("current"))
    total = to_int(payload.get("total"))
    percent = 0
    if total > 0:
        percent = max(0, min(100, int((current / total) * 100)))
    stage = str(payload.get("stage") or "running")
    if stage in {"upload_queued", "creating_notebook", "uploading"}:
        run_status = "uploading"
    elif stage in {"listed", "completed"}:
        run_status = "listed"
    else:
        run_status = "running"
    update_payload = {
        "status": run_status,
        "progress_stage": str(payload.get("stage") or "running"),
        "progress_current": current,
        "progress_total": total,
        "progress_percent": percent,
        "progress_message": str(payload.get("message") or ""),
    }
    client.table("adenda_document_runs").update(update_payload).eq("id", run_id).execute()


def mark_run_failed(client: Client, run_id: str, error_message: str) -> None:
    """Marca corrida fallida en Supabase."""
    client.table("adenda_document_runs").update({
        "status": "failed",
        "progress_stage": "failed",
        "progress_percent": 0,
        "progress_message": "La corrida fallo.",
        "error_message": error_message[:2000],
    }).eq("id", run_id).execute()


def queue_notebook_upload_selection(
    client: Client,
    run_id: str,
    selected_ids: List[str],
    notebook_name: str,
    existing_notebook_id: Optional[str],
) -> None:
    """Marca la seleccion para carga y deja la corrida lista para background upload."""
    run = load_run(client, run_id)
    metadata = run_metadata_dict(run)
    metadata["retry_upload_attempts"] = 0
    client.table("adenda_document_files").update({
        "seleccionar": False,
        "selected": False,
        "upload_status": "pending",
        "upload_error": "",
    }).eq("run_id", run_id).execute()
    client.table("adenda_document_files").update({
        "seleccionar": True,
        "selected": True,
        "upload_status": "selected",
        "upload_error": "",
    }).in_("id", selected_ids).execute()

    progress_total = max(1, len(selected_ids))
    run_update_payload = {
        "status": "uploading",
        "nombre_notebooklm": notebook_name,
        "metadata": json_safe(metadata),
        "progress_stage": "upload_queued",
        "progress_current": 0,
        "progress_total": progress_total,
        "progress_percent": 0,
        "progress_message": "Carga al notebook en cola.",
        "error_message": "",
    }
    if existing_notebook_id:
        run_update_payload["notebooklm_id"] = existing_notebook_id
    client.table("adenda_document_runs").update(run_update_payload).eq("id", run_id).execute()


def update_document_upload_state(
    client: Client,
    document_id: str,
    upload_status: str,
    upload_error: str = "",
) -> None:
    """Actualiza estado individual de carga para un documento."""
    client.table("adenda_document_files").update({
        "upload_status": upload_status,
        "upload_error": upload_error[:2000],
    }).eq("id", document_id).execute()


def retryable_document_ids(rows: List[Dict[str, Any]]) -> List[str]:
    """Lista document_id que pueden reintentarse por no quedar cargados correctamente."""
    ids = []
    for row in rows:
        is_selected = bool(row.get("selected", row.get("seleccionar", False)))
        if not is_selected:
            continue
        status_value = str(row.get("upload_status") or "").strip().lower()
        if status_value in RETRYABLE_UPLOAD_STATUSES:
            ids.append(str(row.get("id") or ""))
    return [doc_id for doc_id in ids if doc_id]


def get_run_retry_attempts(run: Dict[str, Any]) -> int:
    """Retorna cantidad de reintentos manuales de carga registrados para la corrida."""
    metadata = run_metadata_dict(run)
    return max(0, to_int(metadata.get("retry_upload_attempts")))


def set_run_retry_attempts(
    client: Client,
    run_id: str,
    run: Dict[str, Any],
    retry_attempts: int,
) -> int:
    """Persiste contador de reintentos manuales en metadata sin requerir migracion adicional."""
    metadata = run_metadata_dict(run)
    normalized_attempts = max(0, retry_attempts)
    metadata["retry_upload_attempts"] = normalized_attempts
    client.table("adenda_document_runs").update({
        "metadata": json_safe(metadata),
    }).eq("id", run_id).execute()
    run["metadata"] = metadata
    return normalized_attempts


def mark_remaining_documents_for_retry(
    client: Client,
    run_id: str,
    selected_ids: List[str],
    attempted_document_ids: List[str],
    reason: str,
) -> List[str]:
    """Marca documentos seleccionados no intentados como pendientes de reintento."""
    attempted_set = {str(doc_id).strip() for doc_id in attempted_document_ids if str(doc_id).strip()}
    remaining_ids = [
        str(doc_id).strip()
        for doc_id in selected_ids
        if str(doc_id).strip() and str(doc_id).strip() not in attempted_set
    ]
    if not remaining_ids:
        return []
    client.table("adenda_document_files").update({
        "upload_status": "not_uploaded",
        "upload_error": reason[:2000],
    }).in_("id", remaining_ids).execute()
    return remaining_ids


def load_run(client: Client, run_id: str) -> Dict[str, Any]:
    """Carga una corrida CP6B por id."""
    response = (
        client.table("adenda_document_runs")
        .select("*")
        .eq("id", run_id)
        .limit(1)
        .execute()
    )
    rows = supabase_data(response)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No existe corrida CP6B con run_id={run_id}",
        )
    return rows[0]


def load_run_documents(client: Client, run_id: str) -> List[Dict[str, Any]]:
    """Carga todos los documentos de una corrida."""
    response = (
        client.table("adenda_document_files")
        .select("*")
        .eq("run_id", run_id)
        .execute()
    )
    rows = supabase_data(response)
    rows.sort(key=lambda row: (str(row.get("ruta_relativa") or "").lower(), str(row.get("id") or "")))
    return rows


def load_selected_documents(
    client: Client,
    run_id: str,
    selected_document_ids: List[str],
) -> List[Dict[str, Any]]:
    """Carga y valida que todos los documentos pertenezcan a la corrida."""
    response = (
        client.table("adenda_document_files")
        .select("*")
        .eq("run_id", run_id)
        .in_("id", selected_document_ids)
        .execute()
    )
    rows = supabase_data(response)
    rows_by_id = {str(row["id"]): row for row in rows}
    missing = [doc_id for doc_id in selected_document_ids if doc_id not in rows_by_id]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Hay documentos seleccionados que no pertenecen a esta corrida.",
        )
    return [rows_by_id[doc_id] for doc_id in selected_document_ids]


def load_retryable_selected_documents(client: Client, run_id: str) -> List[Dict[str, Any]]:
    """Retorna documentos seleccionados que siguen pendientes/fallidos tras la carga."""
    rows = load_run_documents(client, run_id)
    retryable_ids = set(retryable_document_ids(rows))
    if not retryable_ids:
        return []
    return [row for row in rows if str(row.get("id") or "") in retryable_ids]


def resolve_document_path(output_dir: str, ruta_relativa: str) -> str:
    """Reconstruye ruta absoluta desde la corrida sin confiar en input del frontend."""
    raw_output_dir = str(output_dir or "").strip()
    if not raw_output_dir:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="La corrida no tiene output_dir configurado.",
        )

    base_dir = Path(raw_output_dir)
    if not base_dir.is_absolute():
        base_dir = (API_BASE_DIR / base_dir).resolve()
    else:
        base_dir = base_dir.resolve()

    candidate = (base_dir / ruta_relativa).resolve()
    try:
        candidate.relative_to(base_dir)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Ruta relativa invalida en documento persistido: {ruta_relativa}",
        ) from exc
    return str(candidate)


def normalize_zip_name_part(value: str) -> str:
    """Normaliza una parte del nombre sin agregar hash."""
    normalized = str(value or "").strip()
    for char in '<>:"/\\|?*':
        normalized = normalized.replace(char, "_")
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = normalized.replace(".", "-")
    normalized = re.sub(r"[ _]+", "_", normalized).strip(" ._-")
    return normalized


def truncate_zip_name_part(value: str, max_chars: int) -> str:
    """Recorta una parte del nombre manteniendo cortes limpios."""
    normalized = normalize_zip_name_part(value)
    if len(normalized) <= max_chars:
        return normalized
    return normalized[:max_chars].rstrip(" ._")


def split_zip_filename_extension(file_name: str) -> tuple[str, str]:
    """Separa stem/extension; solo la extension conserva punto."""
    path = PurePosixPath(str(file_name or "").strip())
    extension = path.suffix if 1 < len(path.suffix) <= 20 else ""
    stem = path.stem if extension else path.name
    return stem, extension.lower()


def compact_zip_entry_filename(file_name: str) -> str:
    """Mantiene nombres internos del ZIP compatibles con el extractor nativo de Windows."""
    safe_name = str(file_name or "").strip(" .")
    if not safe_name:
        return "documento"

    raw_stem, extension = split_zip_filename_extension(safe_name)
    stem = normalize_zip_name_part(raw_stem) or "documento"
    safe_name = f"{stem}{extension}"
    if len(safe_name) <= ZIP_ENTRY_FILENAME_MAX_CHARS:
        return safe_name

    suffix = extension
    stem_budget = max(1, ZIP_ENTRY_FILENAME_MAX_CHARS - len(suffix))
    truncated_stem = stem[:stem_budget].rstrip(" ._-") or "documento"
    return f"{truncated_stem}{suffix}"


def zip_entry_filename_with_suffix(file_name: str, suffix_number: int) -> str:
    """Agrega sufijo de duplicado sin exceder el limite de nombre ZIP."""
    raw_stem, extension = split_zip_filename_extension(file_name)
    stem = normalize_zip_name_part(raw_stem) or "documento"
    suffix = f"_{suffix_number}{extension}"
    stem_budget = max(1, ZIP_ENTRY_FILENAME_MAX_CHARS - len(suffix))
    return f"{(stem[:stem_budget].rstrip(' ._-') or 'documento')}{suffix}"


def build_zip_entry_filename_from_row(
    row: Dict[str, Any],
    *,
    tipo: str,
    fallback_name: str,
    index: int,
) -> str:
    """Arma nombre ZIP desde partes cortas para evitar rutas largas en Windows."""
    ruta_relativa = str(row.get("ruta_relativa") or "")
    original_name = str(row.get("nombre_archivo") or "")
    final_name = str(row.get("nombre_archivo_final") or "")
    current_path = PurePosixPath(ruta_relativa.replace("\\", "/") or final_name or original_name)
    extension = (
        PurePosixPath(final_name).suffix
        or PurePosixPath(original_name).suffix
        or current_path.suffix
        or PurePosixPath(fallback_name).suffix
    )
    if not extension:
        formato = str(row.get("formato") or "").strip().lstrip(".")
        if formato:
            extension = f".{formato.lower()}"
    if not extension:
        extension = ".pdf"

    stem_source = (
        PurePosixPath(original_name).stem
        or PurePosixPath(final_name).stem
        or current_path.stem
        or PurePosixPath(fallback_name).stem
        or f"documento_{index}"
    )
    raw_parts = [
        tipo,
        str(row.get("categoria") or ""),
        str(row.get("texto_link") or ""),
        stem_source,
    ]
    parts = [
        truncate_zip_name_part(part, ZIP_ENTRY_PART_LIMITS[min(part_index, len(ZIP_ENTRY_PART_LIMITS) - 1)])
        for part_index, part in enumerate(raw_parts)
    ]
    parts = [part for part in parts if part]
    if not parts:
        return sanitize_zip_entry_filename(fallback_name, f"documento_{index}{extension}")

    file_name = f"{'_'.join(parts)}{extension.lower()}"
    return compact_zip_entry_filename(file_name)


def sanitize_zip_entry_filename(file_name: str, fallback: str) -> str:
    """Normaliza nombres de archivo para que se puedan extraer bien desde Windows."""
    raw_name = str(file_name or "").strip()
    candidate = PurePosixPath(raw_name.replace("\\", "/")).name.strip()
    if not candidate:
        candidate = str(fallback or "").strip()

    safe_name = "".join(
        ch if ch not in '<>:"/\\|?*' and ord(ch) >= 32 else "_"
        for ch in candidate
    ).strip(" .")
    return compact_zip_entry_filename(safe_name or "documento")


def selected_documents_zip_export_id(run_id: str, selected_document_ids: List[str]) -> str:
    """Genera un identificador estable para una seleccion de documentos."""
    normalized_ids = [
        str(document_id).strip()
        for document_id in selected_document_ids
        if str(document_id).strip()
    ]
    payload = {
        "run_id": str(run_id or "").strip(),
        "selected_document_ids": normalized_ids,
    }
    encoded_payload = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded_payload).hexdigest()[:16]


def validate_zip_export_id(export_id: str) -> str:
    """Valida que el export_id no pueda escapar del directorio de exportaciones."""
    normalized_export_id = str(export_id or "").strip().lower()
    if (
        len(normalized_export_id) != 16
        or any(ch not in "0123456789abcdef" for ch in normalized_export_id)
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="export_id invalido.",
        )
    return normalized_export_id


def selected_documents_export_dir(run: Dict[str, Any]) -> Path:
    """Directorio donde se guardan los ZIP exportados desde la tabla."""
    output_dir = str(run.get("output_dir") or "").strip()
    if not output_dir:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="La corrida no tiene output_dir configurado para empaquetar documentos.",
        )

    base_dir = Path(resolve_document_path(output_dir, ".")).resolve()
    export_dir = base_dir / "_table_exports"
    os.makedirs(_long_path(export_dir), exist_ok=True)
    return export_dir


def selected_documents_zip_path(run: Dict[str, Any], export_id: Optional[str] = None) -> Path:
    """Retorna la ruta fisica del ZIP de documentos seleccionados."""
    run_id = str(run.get("id") or "").strip()
    normalized_export_id = validate_zip_export_id(export_id) if export_id else ""
    export_suffix = f"_{normalized_export_id}" if normalized_export_id else ""
    return (
        selected_documents_export_dir(run)
        / f"documentos_para_notebook_{run_id[:8] or 'corrida'}{export_suffix}.zip"
    )


def build_retry_documents_zip(run: Dict[str, Any], rows: List[Dict[str, Any]]) -> Path:
    """Empaqueta documentos pendientes/fallidos en un zip descargable para carga manual."""
    output_dir = str(run.get("output_dir") or "").strip()
    if not output_dir:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="La corrida no tiene output_dir configurado para empaquetar documentos.",
        )

    base_dir = Path(resolve_document_path(output_dir, ".")).resolve()
    export_dir = base_dir / "_manual_retry_exports"
    os.makedirs(_long_path(export_dir), exist_ok=True)

    run_id = str(run.get("id") or "").strip()
    retry_attempts = get_run_retry_attempts(run)
    zip_path = export_dir / (
        f"documentos_fallidos_{run_id[:8] or 'corrida'}_retry_{retry_attempts}.zip"
    )

    seen_arc_names: set[str] = set()
    included_count = 0
    missing_entries: List[str] = []

    # Los PDFs/ZIP ya vienen comprimidos, asi que usar ZIP_STORED acelera mucho la exportacion.
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zip_file:
        for index, row in enumerate(rows, start=1):
            ruta_relativa = str(row.get("ruta_relativa") or "")
            fallback_name = str(
                row.get("nombre_archivo_final") or row.get("nombre_archivo") or f"documento_{index}"
            ).strip() or f"documento_{index}"
            source_path = Path(resolve_document_path(output_dir, ruta_relativa))
            if not _path_exists(source_path):
                missing_entries.append(f"- {fallback_name}: no se encontro el archivo fisico.")
                continue

            relative_parts = [
                part.strip()
                for part in PurePosixPath(ruta_relativa.replace("\\", "/")).parts
                if part not in {"", ".", ".."}
            ]
            if not relative_parts:
                relative_parts = [fallback_name]

            base_arc_name = PurePosixPath("documentos_fallidos", *relative_parts).as_posix()
            arc_name = base_arc_name
            suffix = 2
            while arc_name in seen_arc_names:
                arc_path = PurePosixPath(base_arc_name)
                stem = arc_path.stem or "documento"
                extension = arc_path.suffix
                parent = arc_path.parent
                arc_name = parent.joinpath(f"{stem}_{suffix}{extension}").as_posix()
                suffix += 1

            seen_arc_names.add(arc_name)
            zip_file.write(_long_path(source_path), arc_name)
            included_count += 1

        readme_lines = [
            "Documentos pendientes para carga manual en NotebookLM.",
            "",
            f"Corrida: {run_id or 'desconocida'}",
            f"Reintentos manuales registrados: {retry_attempts}",
            f"Documentos incluidos: {included_count}",
        ]
        if missing_entries:
            readme_lines.extend([
                "",
                "Archivos no incluidos porque no se encontraron en disco:",
                *missing_entries,
            ])
        zip_file.writestr("LEEME.txt", "\n".join(readme_lines))

    if included_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No se encontraron archivos fisicos para empaquetar en el zip de fallidos.",
        )

    return zip_path


def build_selected_documents_zip(
    run: Dict[str, Any],
    rows: List[Dict[str, Any]],
    export_id: Optional[str] = None,
) -> Path:
    """Empaqueta documentos visibles usando nombre_para_notebook como nombre dentro del zip."""
    output_dir = str(run.get("output_dir") or "").strip()
    run_id = str(run.get("id") or "").strip()
    run_tipo = str(run.get("tipo") or "").strip()
    zip_path = selected_documents_zip_path(run, export_id)

    seen_arc_names: set[str] = set()
    seen_source_paths: set[str] = set()
    included_count = 0
    missing_entries: List[str] = []
    duplicate_entries: List[str] = []

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zip_file:
        for index, row in enumerate(rows, start=1):
            ruta_relativa = str(row.get("ruta_relativa") or "")
            fallback_name = str(
                row.get("nombre_archivo_final") or row.get("nombre_archivo") or f"documento_{index}"
            ).strip() or f"documento_{index}"
            source_path = Path(resolve_document_path(output_dir, ruta_relativa))
            if not _path_exists(source_path):
                missing_entries.append(f"- {fallback_name}: no se encontro el archivo fisico.")
                continue

            source_key = os.path.normcase(str(source_path.resolve()))
            if source_key in seen_source_paths:
                duplicate_entries.append(f"- {fallback_name}: archivo duplicado omitido.")
                continue
            seen_source_paths.add(source_key)

            archive_file_name = build_zip_entry_filename_from_row(
                row,
                tipo=run_tipo,
                fallback_name=fallback_name,
                index=index,
            )

            base_arc_name = PurePosixPath(
                "documentos_para_notebook",
                archive_file_name,
            ).as_posix()
            arc_name = base_arc_name
            suffix = 2
            while arc_name in seen_arc_names:
                arc_path = PurePosixPath(base_arc_name)
                dup_stem = arc_path.stem or "documento"
                dup_extension = arc_path.suffix
                parent = arc_path.parent
                arc_name = parent.joinpath(
                    zip_entry_filename_with_suffix(
                        f"{dup_stem}{dup_extension}",
                        suffix,
                    )
                ).as_posix()
                suffix += 1

            seen_arc_names.add(arc_name)
            zip_file.write(_long_path(source_path), arc_name)
            included_count += 1

        readme_lines = [
            "Documentos exportados desde la tabla CP6B para NotebookLM.",
            "",
            f"Corrida: {run_id or 'desconocida'}",
            f"Documentos incluidos: {included_count}",
            "Nombres dentro del ZIP: nombre_para_notebook.",
        ]
        if missing_entries:
            readme_lines.extend(
                [
                    "",
                    "Archivos no incluidos porque no se encontraron en disco:",
                    *missing_entries,
                ]
            )
        if duplicate_entries:
            readme_lines.extend(
                [
                    "",
                    "Archivos duplicados omitidos:",
                    *duplicate_entries,
                ]
            )
        zip_file.writestr("LEEME.txt", "\n".join(readme_lines))

    if included_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No se encontraron archivos fisicos para empaquetar en el zip de la tabla.",
        )

    return zip_path


def process_cp6b_listing_background(
    run_id: str,
    tipo: str,
    normalized_url: str,
    output_dir: str,
    exclude_keywords: List[str],
) -> None:
    """Ejecuta la descarga CP6B en background y persiste estado/documentos."""
    client = get_supabase_client()

    def _progress(payload: Dict[str, Any]) -> None:
        update_run_progress(client, run_id, payload)

    try:
        with _JOB_SEMAPHORE:
            update_run_progress(client, run_id, {
                "stage": "starting",
                "current": 0,
                "total": 1,
                "message": "Iniciando procesamiento CP6B.",
            })
            result = run_seia_notebook_pipeline(
                documento_seia=normalized_url,
                tipo=tipo,
                output_dir=output_dir,
                output_base_dir=API_OUTPUT_ROOT,
                skip_size_estimation=True,
                no_extract=False,
                keep_existing=False,
                enable_download=True,
                upload_limit=NOTEBOOK_UPLOAD_LIMIT,
                notebook_title=None,
                notebook_api_base_url=NOTEBOOK_API_BASE_URL,
                require_notebook=False,
                stop_after_cp6b=True,
                exclude_keywords=exclude_keywords,
                progress_callback=_progress,
            )
        persist_cp6b_run_result(client=client, run_id=run_id, result=result)
    except Exception as e:
        mark_run_failed(client, run_id, str(e))


def process_notebook_upload_background(
    run_id: str,
    selected_ids: List[str],
    nombre_notebook: Optional[str],
    existing_notebook_id: Optional[str],
    notebook_auth: Optional[Dict[str, Any]] = None,
) -> None:
    """Crea/reutiliza notebook y sube la seleccion en background actualizando progreso."""
    client = get_supabase_client()
    attempted_document_ids: List[str] = []
    try:
        auth_seed = None
        if notebook_auth is not None:
            try:
                auth_seed = prepare_notebook_client_seed(notebook_auth)
            except ValueError as preflight_exc:
                raise NotebookCredentialsExpired(
                    f"Pre-flight NotebookLM fallo: {preflight_exc}"
                ) from preflight_exc

        run = load_run(client, run_id)
        tipo = str(run.get("tipo") or "").strip()
        id_documento = str(run.get("id_documento") or "")
        if not tipo or not id_documento:
            raise RuntimeError("La corrida CP6B no tiene tipo/id_documento validos.")

        all_selected_rows = load_selected_documents(client, run_id, selected_ids)
        already_uploaded_ids: List[str] = [
            str(row["id"]) for row in all_selected_rows
            if str(row.get("upload_status") or "").strip().lower() == "uploaded"
        ]
        selected_rows = [
            row for row in all_selected_rows
            if str(row.get("upload_status") or "").strip().lower() != "uploaded"
        ]
        if already_uploaded_ids:
            attempted_document_ids.extend(already_uploaded_ids)
            print(
                f"[notebook-upload] Run {run_id}: {len(already_uploaded_ids)} doc(s) ya "
                f"con upload_status=uploaded; se omiten."
            )
        total_docs = max(1, len(selected_rows))
        create_new_notebook = bool(nombre_notebook)
        notebook_name = normalize_optional_text(nombre_notebook) or str(
            run.get("nombre_notebooklm") or existing_notebook_id or ""
        )
        if create_new_notebook and not notebook_name:
            notebook_name = build_tipo_notebook_title(tipo, id_documento)

        docs_for_upload = []
        for row in selected_rows:
            enriched_row = with_run_context(row, tipo=tipo)
            ruta_relativa = str(row.get("ruta_relativa") or "")
            docs_for_upload.append({
                "document_id": str(row["id"]),
                "nombre_archivo": str(
                    row.get("nombre_archivo_final") or row.get("nombre_archivo") or ""
                ),
                "nombre_archivo_notebook": build_notebook_upload_filename(enriched_row),
                "extension": str(row.get("extension") or ""),
                "ruta_relativa": ruta_relativa,
                "ruta_absoluta": resolve_document_path(
                    str(run.get("output_dir") or ""),
                    ruta_relativa,
                ),
                "tamano_bytes": to_int(row.get("tamano_bytes")),
                "nivel_descarga_descompresion": to_int(
                    row.get("nivel_descarga_descompresion")
                ),
                "origen": str(row.get("origen") or ""),
            })

        if create_new_notebook:
            update_run_progress(client, run_id, {
                "stage": "creating_notebook",
                "current": 0,
                "total": total_docs,
                "message": "Creando notebook en NotebookLM.",
            })
            notebook_id, notebook_title_used, notebook_error = notify_notebook_api(
                notebook_title=notebook_name,
                api_base_url=NOTEBOOK_API_BASE_URL,
                raise_on_error=True,
                notebook_auth=notebook_auth,
                auth_seed=auth_seed,
            )
            if not notebook_id:
                raise NotebookAPIError(notebook_error or "No se obtuvo notebook_id.")
        else:
            notebook_id = str(existing_notebook_id or "").strip()
            if not notebook_id:
                raise RuntimeError("No se recibio notebook_id para reutilizar el notebook.")
            notebook_title_used = notebook_name or notebook_id

        client.table("adenda_document_runs").update({
            "notebooklm_id": notebook_id,
            "nombre_notebooklm": notebook_title_used,
            "status": "uploading",
            "progress_stage": "uploading",
            "progress_current": 0,
            "progress_total": total_docs,
            "progress_percent": 0,
            "progress_message": "Iniciando carga de documentos al notebook.",
            "error_message": "",
        }).eq("id", run_id).execute()

        def _progress(payload: Dict[str, Any]) -> None:
            update_run_progress(client, run_id, payload)

        def _item_progress(event: str, item: Dict[str, Any], index: int, total: int) -> None:
            document_id = str(item.get("document_id") or "").strip()
            if not document_id:
                return
            if event == "completed":
                attempted_document_ids.append(document_id)
            if event == "starting":
                update_document_upload_state(client, document_id, "uploading")
            elif event == "completed":
                update_document_upload_state(
                    client,
                    document_id,
                    "uploaded" if item.get("uploaded") else "failed",
                    "" if item.get("uploaded") else str(item.get("error") or ""),
                )

        rotation_user_id = (
            (notebook_auth or {}).get("_credentials_user_id")
            if isinstance(notebook_auth, dict) else None
        )

        def _persist_rotated_cookies(new_cookies: Dict[str, str]) -> None:
            if not rotation_user_id or not new_cookies:
                return
            try:
                update_cookies(
                    client,
                    str(rotation_user_id),
                    new_cookies,
                    event_source="upload_rotation",
                )
            except Exception as cb_exc:  # noqa: BLE001
                print(
                    f"[notebook-upload] update_cookies fallo para {rotation_user_id}: "
                    f"{cb_exc}"
                )

        upload_stats = upload_documents_batch_and_single(
            notebook_id=notebook_id,
            docs_report=docs_for_upload,
            limit=NOTEBOOK_UPLOAD_LIMIT,
            api_base_url=NOTEBOOK_API_BASE_URL,
            progress_callback=_progress,
            item_callback=_item_progress,
            notebook_auth=notebook_auth,
            auth_seed=auth_seed,
            cookie_rotation_callback=(
                _persist_rotated_cookies if rotation_user_id else None
            ),
        )

        not_uploaded_ids = mark_remaining_documents_for_retry(
            client=client,
            run_id=run_id,
            selected_ids=selected_ids,
            attempted_document_ids=[item.get("document_id") for item in upload_stats.get("items", [])],
            reason="Documento no fue intentado en esta corrida de carga; queda pendiente para reintento.",
        )

        retryable_count = to_int(upload_stats.get("uploaded_failed")) + len(not_uploaded_ids)
        final_status = "success" if retryable_count == 0 else "partial_success"
        final_total = max(1, len(selected_rows))
        final_ok = to_int(upload_stats.get("uploaded_ok"))
        final_failed = retryable_count
        final_percent = 100 if final_total > 0 else 0
        final_message = (
            f"Carga finalizada: {final_ok} ok, {final_failed} con error."
            if final_failed
            else f"Carga finalizada: {final_ok} documentos subidos."
        )
        client.table("adenda_document_runs").update({
            "status": final_status,
            "notebooklm_id": notebook_id,
            "nombre_notebooklm": notebook_title_used,
            "progress_stage": final_status,
            "progress_current": final_total,
            "progress_total": final_total,
            "progress_percent": final_percent,
            "progress_message": final_message,
            "error_message": "" if final_failed == 0 else str(run.get("error_message") or ""),
        }).eq("id", run_id).execute()
        _touch_stored_notebook_credentials(notebook_auth)
    except NotebookCredentialsExpired as e:
        message = str(e).strip() or "Credenciales NotebookLM caducas."
        user_id = (
            (notebook_auth or {}).get("_credentials_user_id")
            if isinstance(notebook_auth, dict) else None
        )
        if user_id:
            try:
                mark_credentials_status(
                    client,
                    str(user_id),
                    status="expired",
                    last_error=message,
                    increment_failure=True,
                    event_type="batch_expired",
                    event_source="upload_background",
                    event_ok=False,
                )
            except Exception as mark_exc:  # noqa: BLE001
                print(
                    f"[notebook-upload] No se pudo marcar status=expired para "
                    f"{user_id}: {mark_exc}"
                )
        mark_remaining_documents_for_retry(
            client=client,
            run_id=run_id,
            selected_ids=selected_ids,
            attempted_document_ids=attempted_document_ids,
            reason=f"Re-autenticacion NotebookLM requerida: {message}",
        )
        client.table("adenda_document_runs").update({
            "status": "auth_required",
            "progress_stage": "auth_required",
            "progress_message": (
                "Re-autenticacion NotebookLM requerida. "
                "Pega cookies frescas y reintenta."
            ),
            "error_message": message[:2000],
        }).eq("id", run_id).execute()
        return
    except Exception as e:
        _mark_stored_notebook_credentials_failure(notebook_auth, e)
        mark_remaining_documents_for_retry(
            client=client,
            run_id=run_id,
            selected_ids=selected_ids,
            attempted_document_ids=attempted_document_ids,
            reason=f"Carga interrumpida antes de completar el documento: {e}",
        )
        mark_run_failed(client, run_id, str(e))


@app.get("/health")
def health() -> dict:
    """Healthcheck simple."""
    return {"status": "ok"}


@app.post(
    "/auth/validate-cookies",
    dependencies=[Depends(require_bearer_token)],
)
def validate_cookies(body: ValidateCookiesRequest) -> Dict[str, Any]:
    """Valida cookies pegadas por el usuario y devuelve auth compacta reutilizable."""
    try:
        storage_state, format_detected = _normalize_storage_state_from_text(body.cookies_text)
        auth_payload, missing_required = _build_compact_auth_payload_from_storage(storage_state)
        cookie_domains = _allowed_cookie_domains_from_storage(storage_state)

        if missing_required:
            return {
                "ok": False,
                "message": (
                    "Faltan cookies obligatorias para autenticar en NotebookLM: "
                    + ", ".join(missing_required)
                ),
                "format_detected": format_detected,
                "cookie_domains": cookie_domains,
                "selected_cookie_names": auth_payload["cookie_names"],
                "missing_required_cookies": missing_required,
                "token_fetch_ok": False,
                "auth_payload": None,
            }

        asyncio.run(fetch_tokens(dict(auth_payload["cookies"])))
        return {
            "ok": True,
            "message": "Cookies validas para NotebookLM.",
            "format_detected": format_detected,
            "cookie_domains": cookie_domains,
            "selected_cookie_names": auth_payload["cookie_names"],
            "missing_required_cookies": [],
            "token_fetch_ok": True,
            "auth_payload": auth_payload,
        }
    except Exception as exc:  # noqa: BLE001
        message = str(exc).strip() or "No se pudieron validar las cookies."
        return {
            "ok": False,
            "message": message,
            "format_detected": "unknown",
            "cookie_domains": [],
            "selected_cookie_names": [],
            "missing_required_cookies": [],
            "token_fetch_ok": False,
            "auth_payload": None,
        }


@app.post(
    "/api/v1/adenda/notebook/credentials",
    response_model=NotebookCredentialsStatusResponse,
    dependencies=[Depends(require_bearer_token)],
)
def store_notebook_credentials(
    body: NotebookCredentialsStoreRequest,
    user_id: str = Depends(get_current_user_id),
) -> NotebookCredentialsStatusResponse:
    """Valida y guarda cookies NotebookLM cifradas por usuario."""
    try:
        storage_state, _format_detected = _normalize_storage_state_from_text(body.cookies_text)
        auth_payload, missing_required = _build_compact_auth_payload_from_storage(storage_state)
        if missing_required:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Faltan cookies obligatorias para autenticar en NotebookLM: "
                    + ", ".join(missing_required)
                ),
            )

        asyncio.run(fetch_tokens(dict(auth_payload["cookies"])))
        row = store_credentials(get_supabase_client(), user_id, auth_payload)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc).strip() or "No se pudieron validar/guardar las credenciales NotebookLM.",
        ) from exc

    return _notebook_credentials_status_payload(row)


@app.get(
    "/api/v1/adenda/notebook/credentials/status",
    response_model=NotebookCredentialsStatusResponse,
    dependencies=[Depends(require_bearer_token)],
)
def get_notebook_credentials_status(
    user_id: str = Depends(get_current_user_id),
) -> NotebookCredentialsStatusResponse:
    """Retorna el estado actual de las credenciales NotebookLM guardadas del usuario."""
    try:
        row = load_credentials(get_supabase_client(), user_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No se pudo consultar el estado de las credenciales NotebookLM: {exc}",
        ) from exc

    return _notebook_credentials_status_payload(row)


@app.post(
    "/api/v1/adenda/notebook/credentials/revalidate",
    response_model=NotebookCredentialsStatusResponse,
    dependencies=[Depends(require_bearer_token)],
)
def revalidate_notebook_credentials(
    user_id: str = Depends(get_current_user_id),
) -> NotebookCredentialsStatusResponse:
    """Revalida ahora las cookies NotebookLM guardadas del usuario y actualiza su estado."""
    return _revalidate_stored_notebook_credentials(user_id)


@app.delete(
    "/api/v1/adenda/notebook/credentials",
    response_model=NotebookCredentialsDeleteResponse,
    dependencies=[Depends(require_bearer_token)],
)
def remove_notebook_credentials(
    user_id: str = Depends(get_current_user_id),
) -> NotebookCredentialsDeleteResponse:
    """Elimina las credenciales NotebookLM guardadas del usuario autenticado."""
    try:
        delete_credentials(get_supabase_client(), user_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No se pudieron eliminar las credenciales NotebookLM: {exc}",
        ) from exc
    return NotebookCredentialsDeleteResponse(deleted=True)


@app.get(
    "/notebooks",
    dependencies=[Depends(require_bearer_token)],
)
def list_notebooks(
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Lista notebooks de la cuenta autenticada via X-NotebookLM-Auth."""

    async def _op(client: NotebookLMClient):
        return await client.notebooks.list()

    notebooks = run_notebook_share_operation(notebook_auth, _op, timeout=30.0)
    payload = [{"id": notebook.id, "title": notebook.title} for notebook in notebooks]
    return {"ok": True, "items": payload}


@app.post(
    "/api/v1/adenda/descarga-documentos-seia",
    response_model=CreateCP6BResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_bearer_token)],
)
def create_adenda_cp6b_listing(
    payload: CreateCP6BRequest,
    background_tasks: BackgroundTasks,
) -> CreateCP6BResponse:
    """Encola la generacion del listado CP6B y responde inmediatamente con run_id."""
    normalized_url = validate_documento_seia(payload.documento_seia)
    tipo = validate_tipo(payload.tipo)
    id_documento = extract_id_documento(normalized_url)
    run_id = str(uuid.uuid4())
    tipo_slug = "".join(ch if ch.isalnum() else "_" for ch in tipo).strip("_") or "tipo"
    safe_doc_id = safe_id_documento_token(id_documento)
    output_dir = API_OUTPUT_ROOT / f"{tipo_slug}_doc_{safe_doc_id}_cp6b_{run_id[:8]}"
    exclude_keywords = [
        str(keyword).strip()
        for keyword in (payload.exclude_keywords or [])
        if str(keyword).strip()
    ]

    try:
        supabase = get_supabase_client()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e

    try:
        create_cp6b_run_record(
            client=supabase,
            run_id=run_id,
            tipo=tipo,
            documento_seia=normalized_url,
            id_documento=id_documento,
            output_dir=str(output_dir),
            exclude_keywords=exclude_keywords,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No se pudo crear la corrida CP6B: {e}",
        ) from e

    background_tasks.add_task(
        process_cp6b_listing_background,
        run_id,
        tipo,
        normalized_url,
        str(output_dir),
        exclude_keywords,
    )

    return CreateCP6BResponse(
        status="queued",
        run_id=run_id,
        tipo=tipo,
        id_documento=id_documento,
        documents_found=0,
        documents=[],
    )


@app.post(
    "/api/v1/adenda/crear-y-cargar-notebook-filtrado",
    response_model=UploadSelectionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_bearer_token)],
)
def create_adenda_notebook_from_selection(
    payload: UploadSelectionRequest,
    background_tasks: BackgroundTasks,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> UploadSelectionResponse:
    """Crea notebook y sube solo documentos seleccionados desde una corrida CP6B."""
    selected_ids = [
        doc_id.strip()
        for doc_id in dict.fromkeys(payload.selected_document_ids)
        if doc_id and doc_id.strip()
    ]
    nombre_notebook = normalize_optional_text(payload.nombre_notebook)
    existing_notebook_id = normalize_optional_text(payload.notebook_id)

    if not selected_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="selected_document_ids debe incluir al menos un documento.",
        )
    if bool(nombre_notebook) == bool(existing_notebook_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Debes enviar exactamente uno: nombre_notebook para crear un notebook nuevo, "
                "o notebook_id para reutilizar uno existente."
            ),
        )

    try:
        supabase = get_supabase_client()
        run = load_run(supabase, payload.run_id)
        selected_rows = load_selected_documents(supabase, payload.run_id, selected_ids)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error leyendo seleccion desde Supabase: {e}",
        ) from e

    tipo = str(run.get("tipo") or "").strip()
    id_documento = str(run.get("id_documento") or "")
    if not tipo or not id_documento:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="La corrida CP6B no tiene tipo/id_documento validos.",
        )
    create_new_notebook = bool(nombre_notebook)
    notebook_name = nombre_notebook or str(run.get("nombre_notebooklm") or existing_notebook_id or "")
    if create_new_notebook and not notebook_name:
        notebook_name = build_tipo_notebook_title(tipo, id_documento)

    validate_notebook_source_capacity(
        len(selected_rows),
        notebook_id=existing_notebook_id,
        notebook_auth=notebook_auth,
    )

    selected_documents = []
    for row in selected_rows:
        enriched_row = with_run_context(row, tipo=tipo)
        selected_documents.append(public_document_from_row(enriched_row, tipo=tipo))

    try:
        queue_notebook_upload_selection(
            client=supabase,
            run_id=payload.run_id,
            selected_ids=selected_ids,
            notebook_name=notebook_name,
            existing_notebook_id=existing_notebook_id,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No se pudo encolar la carga al notebook: {e}",
        ) from e

    background_tasks.add_task(
        process_notebook_upload_background,
        payload.run_id,
        selected_ids,
        nombre_notebook,
        existing_notebook_id,
        notebook_auth,
    )

    notebook_id = str(existing_notebook_id or "")
    notebook_title_used = notebook_name

    return UploadSelectionResponse(
        status="upload_queued",
        run_id=payload.run_id,
        tipo=tipo,
        id_documento=id_documento,
        notebooklm_id=notebook_id,
        nombre_notebooklm=notebook_title_used,
        documents_uploaded_ok=0,
        documents_uploaded_failed=0,
        retry_attempts=0,
        retry_documents_count=0,
        retry_document_ids=[],
        selected_documents=selected_documents,
    )


@app.get(
    "/api/v1/adenda/descarga-documentos-seia/{run_id}",
    response_model=CP6BStatusResponse,
    dependencies=[Depends(require_bearer_token)],
)
def get_adenda_cp6b_status(run_id: str) -> CP6BStatusResponse:
    """Retorna estado y documentos de una corrida CP6B."""
    try:
        supabase = get_supabase_client()
        run = load_run(supabase, run_id)
        run_tipo = str(run.get("tipo") or "")
        run_rows = load_run_documents(supabase, run_id)
        documents = [public_document_from_row(row, tipo=run_tipo) for row in run_rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error leyendo estado CP6B: {e}",
        ) from e

    return CP6BStatusResponse(
        status=str(run.get("status") or "queued"),
        run_id=run_id,
        tipo=str(run.get("tipo") or ""),
        id_documento=str(run.get("id_documento") or ""),
        documents_found=len(documents),
        documents=documents,
        progress_stage=str(run.get("progress_stage") or ""),
        progress_current=to_int(run.get("progress_current")),
        progress_total=to_int(run.get("progress_total")),
        progress_percent=to_int(run.get("progress_percent")),
        progress_message=str(run.get("progress_message") or ""),
        error_message=str(run.get("error_message") or ""),
        notebooklm_id=str(run.get("notebooklm_id") or ""),
        nombre_notebooklm=str(run.get("nombre_notebooklm") or ""),
        retry_attempts=get_run_retry_attempts(run),
        retry_documents_count=len(retryable_document_ids(run_rows)),
        retry_document_ids=retryable_document_ids(run_rows),
    )


@app.get(
    "/api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-fallidos.zip",
    dependencies=[Depends(require_bearer_token)],
)
def download_retryable_documents_zip(run_id: str) -> FileResponse:
    """Descarga un zip con los documentos seleccionados que siguen pendientes o fallidos."""
    try:
        supabase = get_supabase_client()
        run = load_run(supabase, run_id)
        retryable_rows = load_retryable_selected_documents(supabase, run_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error preparando descarga de documentos fallidos: {e}",
        ) from e

    if not retryable_rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay documentos pendientes/fallidos para descargar en esta corrida.",
        )

    zip_path = build_retry_documents_zip(run, retryable_rows)
    return FileResponse(
        path=zip_path,
        media_type="application/zip",
        filename=zip_path.name,
    )


@app.post(
    "/api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-seleccionados.zip",
    dependencies=[Depends(require_bearer_token)],
)
def download_selected_documents_zip(
    run_id: str,
    payload: DownloadSelectedDocumentsZipRequest,
) -> FileResponse:
    """Descarga un zip con documentos visibles usando nombre_para_notebook."""
    selected_document_ids = [
        str(document_id).strip()
        for document_id in payload.selected_document_ids
        if str(document_id).strip()
    ]
    if not selected_document_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Debes enviar al menos un document_id para descargar el zip.",
        )

    try:
        supabase = get_supabase_client()
        run = load_run(supabase, run_id)
        selected_rows = load_selected_documents(supabase, run_id, selected_document_ids)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error preparando descarga de documentos visibles: {e}",
        ) from e

    zip_path = build_selected_documents_zip(run, selected_rows)
    return FileResponse(
        path=zip_path,
        media_type="application/zip",
        filename=zip_path.name,
    )


@app.post(
    "/api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-seleccionados/export",
    response_model=SelectedDocumentsZipExportResponse,
    dependencies=[Depends(require_bearer_token)],
)
def create_selected_documents_zip_export(
    run_id: str,
    payload: DownloadSelectedDocumentsZipRequest,
) -> SelectedDocumentsZipExportResponse:
    """Prepara un ZIP y devuelve metadata para descargarlo por partes pequenas."""
    selected_document_ids = [
        str(document_id).strip()
        for document_id in payload.selected_document_ids
        if str(document_id).strip()
    ]
    if not selected_document_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Debes enviar al menos un document_id para preparar el zip.",
        )

    try:
        supabase = get_supabase_client()
        run = load_run(supabase, run_id)
        selected_rows = load_selected_documents(supabase, run_id, selected_document_ids)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error preparando exportacion de documentos visibles: {e}",
        ) from e

    export_id = selected_documents_zip_export_id(run_id, selected_document_ids)
    zip_path = build_selected_documents_zip(run, selected_rows, export_id=export_id)
    size_bytes = zip_path.stat().st_size
    parts = max(1, (size_bytes + ZIP_EXPORT_PART_SIZE_BYTES - 1) // ZIP_EXPORT_PART_SIZE_BYTES)
    return SelectedDocumentsZipExportResponse(
        export_id=export_id,
        filename=zip_path.name,
        size_bytes=size_bytes,
        part_size_bytes=ZIP_EXPORT_PART_SIZE_BYTES,
        parts=parts,
    )


@app.get(
    "/api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-seleccionados/export/{export_id}/part/{part_index}",
    dependencies=[Depends(require_bearer_token)],
)
def download_selected_documents_zip_export_part(
    run_id: str,
    export_id: str,
    part_index: int,
) -> Response:
    """Descarga una parte pequena de un ZIP preparado previamente."""
    normalized_export_id = validate_zip_export_id(export_id)
    if part_index < 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="part_index debe ser mayor o igual a 0.",
        )

    try:
        supabase = get_supabase_client()
        run = load_run(supabase, run_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error leyendo corrida para descargar parte del zip: {e}",
        ) from e

    zip_path = selected_documents_zip_path(run, normalized_export_id)
    if not _path_exists(zip_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el ZIP preparado. Vuelve a iniciar la descarga.",
        )

    size_bytes = zip_path.stat().st_size
    if size_bytes <= 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="El ZIP preparado esta vacio. Vuelve a iniciar la descarga.",
        )

    start = part_index * ZIP_EXPORT_PART_SIZE_BYTES
    if start >= size_bytes:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="La parte solicitada esta fuera del tamano del ZIP.",
        )
    end = min(start + ZIP_EXPORT_PART_SIZE_BYTES, size_bytes) - 1
    chunk_size = end - start + 1
    parts = max(1, (size_bytes + ZIP_EXPORT_PART_SIZE_BYTES - 1) // ZIP_EXPORT_PART_SIZE_BYTES)

    with open(_long_path(zip_path), "rb") as zip_file:
        zip_file.seek(start)
        content = zip_file.read(chunk_size)

    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(len(content)),
            "Content-Range": f"bytes {start}-{end}/{size_bytes}",
            "X-Zip-Export-Id": normalized_export_id,
            "X-Zip-Part-Index": str(part_index),
            "X-Zip-Part-Count": str(parts),
            "X-Zip-Filename": zip_path.name,
        },
    )


@app.post(
    "/api/v1/adenda/reintentar-carga-notebook",
    response_model=RetryUploadResponse,
    dependencies=[Depends(require_bearer_token)],
)
def retry_failed_notebook_upload(
    payload: RetryUploadRequest,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> RetryUploadResponse:
    """Reintenta subir al notebook documentos fallidos o no alcanzados ya seleccionados."""
    auth_seed = None
    try:
        supabase = get_supabase_client()
        run = load_run(supabase, payload.run_id)
        if notebook_auth is not None:
            auth_seed = prepare_notebook_client_seed(notebook_auth)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error leyendo corrida para reintento: {e}",
        ) from e

    notebook_id = str(run.get("notebooklm_id") or "")
    if not notebook_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="La corrida no tiene notebooklm_id; no se puede reintentar la carga.",
        )

    response = (
        supabase.table("adenda_document_files")
        .select("*")
        .eq("run_id", payload.run_id)
        .eq("selected", True)
        .in_("upload_status", list(RETRYABLE_UPLOAD_STATUSES))
        .execute()
    )
    failed_rows = supabase_data(response)
    if not failed_rows:
        return RetryUploadResponse(
            status="no_failed_documents",
            run_id=payload.run_id,
            notebooklm_id=notebook_id,
            documents_uploaded_ok=0,
            documents_uploaded_failed=0,
            retry_attempts=get_run_retry_attempts(run),
            retry_documents_count=0,
            retry_document_ids=[],
            selected_documents=[],
        )

    validate_notebook_source_capacity(
        len(failed_rows),
        notebook_id=notebook_id,
        notebook_auth=notebook_auth,
        auth_seed=auth_seed,
    )

    retry_attempts = set_run_retry_attempts(
        supabase,
        payload.run_id,
        run,
        get_run_retry_attempts(run) + 1,
    )

    docs_for_upload = []
    selected_documents = []
    run_tipo = str(run.get("tipo") or "")
    for row in failed_rows:
        enriched_row = with_run_context(row, tipo=run_tipo)
        ruta_relativa = str(row.get("ruta_relativa") or "")
        docs_for_upload.append({
            "document_id": str(row["id"]),
            "nombre_archivo": str(row.get("nombre_archivo_final") or row.get("nombre_archivo") or ""),
            "nombre_archivo_notebook": build_notebook_upload_filename(enriched_row),
            "extension": str(row.get("extension") or ""),
            "ruta_relativa": ruta_relativa,
            "ruta_absoluta": resolve_document_path(str(run.get("output_dir") or ""), ruta_relativa),
            "tamano_bytes": to_int(row.get("tamano_bytes")),
            "nivel_descarga_descompresion": to_int(row.get("nivel_descarga_descompresion")),
            "origen": str(row.get("origen") or ""),
        })
        selected_documents.append(public_document_from_row(enriched_row, tipo=run_tipo))

    try:
        for row in failed_rows:
            update_document_upload_state(
                supabase,
                str(row["id"]),
                "selected",
                "",
            )
        upload_stats = upload_documents_batch_and_single(
            notebook_id=notebook_id,
            docs_report=docs_for_upload,
            limit=None,
            api_base_url=NOTEBOOK_API_BASE_URL,
            notebook_auth=notebook_auth,
            auth_seed=auth_seed,
        )
        not_uploaded_ids = mark_remaining_documents_for_retry(
            client=supabase,
            run_id=payload.run_id,
            selected_ids=[str(row["id"]) for row in failed_rows],
            attempted_document_ids=[item.get("document_id") for item in upload_stats.get("items", [])],
            reason="Documento no fue intentado durante el reintento; queda pendiente para nueva carga.",
        )
        for item in upload_stats.get("items", []):
            document_id = item.get("document_id")
            if not document_id:
                continue
            supabase.table("adenda_document_files").update({
                "upload_status": "uploaded" if item.get("uploaded") else "failed",
                "upload_error": "" if item.get("uploaded") else str(item.get("error") or "")[:2000],
            }).eq("id", document_id).execute()
        _touch_stored_notebook_credentials(notebook_auth)
    except Exception as e:
        _mark_stored_notebook_credentials_failure(notebook_auth, e)
        mark_remaining_documents_for_retry(
            client=supabase,
            run_id=payload.run_id,
            selected_ids=[str(row["id"]) for row in failed_rows],
            attempted_document_ids=[],
            reason=f"Reintento interrumpido antes de completar la carga: {e}",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reintentando carga de notebook: {e}",
        ) from e

    remaining_rows = load_run_documents(supabase, payload.run_id)
    retry_ids = retryable_document_ids([row for row in remaining_rows if bool(row.get("selected"))])
    retry_status = "success" if not retry_ids else "partial_success"
    return RetryUploadResponse(
        status=retry_status,
        run_id=payload.run_id,
        notebooklm_id=notebook_id,
        documents_uploaded_ok=upload_stats["uploaded_ok"],
        documents_uploaded_failed=len(retry_ids),
        retry_attempts=retry_attempts,
        retry_documents_count=len(retry_ids),
        retry_document_ids=retry_ids,
        selected_documents=selected_documents,
    )


@app.post(
    "/api/v1/adendas/notebooklm",
    response_model=CreateNotebookResponse,
    dependencies=[Depends(require_bearer_token)],
)
def create_adenda_notebook(
    payload: CreateNotebookRequest,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> CreateNotebookResponse:
    """Endpoint principal para crear notebook y persistirlo en adendas."""
    normalized_url = validate_documento_seia(payload.documento_seia)
    id_documento = extract_id_documento(normalized_url)
    notebook_name = build_adenda_notebook_title(payload.id_adenda, id_documento)
    safe_doc_id = safe_id_documento_token(id_documento)
    output_dir = API_OUTPUT_ROOT / f"adenda_{payload.id_adenda}_doc_{safe_doc_id}_{notebook_name.split('_')[-1]}"

    try:
        supabase = get_supabase_client()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e

    try:
        if not adenda_exists(supabase, payload.id_adenda):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe adenda con id={payload.id_adenda}",
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error validando adenda en Supabase: {e}",
        ) from e

    def on_notebook_created(notebook_id: str, notebook_title: str) -> None:
        try:
            persist_adenda_notebook(
                client=supabase,
                id_adenda=payload.id_adenda,
                notebook_id=notebook_id,
                notebook_name=notebook_title,
            )
        except Exception as e:
            raise RuntimeError(f"No se pudo guardar notebook en Supabase: {e}") from e

    try:
        with _JOB_SEMAPHORE:
            result = run_seia_notebook_pipeline(
                documento_seia=normalized_url,
                id_adenda=payload.id_adenda,
                output_dir=output_dir,
                output_base_dir=API_OUTPUT_ROOT,
                skip_size_estimation=True,
                no_extract=False,
                keep_existing=False,
                enable_download=True,
                upload_limit=NOTEBOOK_UPLOAD_LIMIT,
                notebook_title=notebook_name,
                notebook_api_base_url=NOTEBOOK_API_BASE_URL,
                require_notebook=True,
                on_notebook_created=on_notebook_created,
                notebook_auth=notebook_auth,
            )
        _touch_stored_notebook_credentials(notebook_auth)
    except NotebookAPIError as e:
        _mark_stored_notebook_credentials_failure(notebook_auth, e)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e)) from e
    except HTTPException:
        raise
    except RuntimeError as e:
        _mark_stored_notebook_credentials_failure(notebook_auth, e)
        message = str(e)
        if "No se encontraron documentos descargables" in message:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=message,
            ) from e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=message,
        ) from e
    except Exception as e:
        _mark_stored_notebook_credentials_failure(notebook_auth, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno procesando solicitud: {e}",
        ) from e

    return CreateNotebookResponse(
        status=result["status"],
        id_adenda=payload.id_adenda,
        id_documento=result["id_documento"],
        notebooklm_id=result["notebooklm_id"],
        nombre_notebooklm=result["nombre_notebooklm"],
        documents_found=result["documents_found"],
        documents_uploaded_ok=result["documents_uploaded_ok"],
        documents_uploaded_failed=result["documents_uploaded_failed"],
        output_dir=result["output_dir"],
        elapsed_seconds=result["elapsed_seconds"],
    )


@app.get(
    "/notebooks/{notebook_id}/share",
    dependencies=[Depends(require_bearer_token)],
)
def get_share_status(
    notebook_id: str,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Retorna configuracion de sharing y colaboradores del notebook."""
    async def _op(client: NotebookLMClient):
        return await client.sharing.get_status(notebook_id)

    share_status = run_notebook_share_operation(notebook_auth, _op)
    return {"ok": True, "item": _share_status_payload(share_status)}


@app.post(
    "/notebooks/{notebook_id}/share/public",
    dependencies=[Depends(require_bearer_token)],
)
def set_public_share(
    notebook_id: str,
    body: ShareSetPublicRequest,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Habilita o deshabilita sharing publico del notebook."""
    async def _op(client: NotebookLMClient):
        return await client.sharing.set_public(notebook_id, body.public)

    share_status = run_notebook_share_operation(notebook_auth, _op)
    return {"ok": True, "item": _share_status_payload(share_status)}


@app.post(
    "/notebooks/{notebook_id}/share/view-level",
    dependencies=[Depends(require_bearer_token)],
)
def set_share_view_level(
    notebook_id: str,
    body: ShareSetViewLevelRequest,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Define scope de viewers (full_notebook o chat_only)."""
    view_level = _parse_share_view_level(body.view_level)

    async def _op(client: NotebookLMClient):
        return await client.sharing.set_view_level(notebook_id, view_level)

    share_status = run_notebook_share_operation(notebook_auth, _op)
    return {"ok": True, "item": _share_status_payload(share_status)}


@app.post(
    "/notebooks/{notebook_id}/share/users",
    dependencies=[Depends(require_bearer_token)],
)
def add_share_user(
    notebook_id: str,
    body: ShareAddUserRequest,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Comparte notebook con un usuario como viewer/editor."""
    email = _validate_email(body.email)
    permission = _parse_share_permission(body.permission)

    async def _op(client: NotebookLMClient):
        return await client.sharing.add_user(
            notebook_id,
            email,
            permission=permission,
            notify=body.notify,
            welcome_message=body.welcome_message,
        )

    share_status = run_notebook_share_operation(notebook_auth, _op)
    return {"ok": True, "item": _share_status_payload(share_status)}


@app.patch(
    "/notebooks/{notebook_id}/share/users/{email}",
    dependencies=[Depends(require_bearer_token)],
)
def update_share_user(
    notebook_id: str,
    email: str,
    body: ShareUpdateUserRequest,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Actualiza permiso de un usuario ya compartido."""
    valid_email = _validate_email(email)
    permission = _parse_share_permission(body.permission)

    async def _op(client: NotebookLMClient):
        return await client.sharing.update_user(
            notebook_id,
            valid_email,
            permission=permission,
        )

    share_status = run_notebook_share_operation(notebook_auth, _op)
    return {"ok": True, "item": _share_status_payload(share_status)}


@app.delete(
    "/notebooks/{notebook_id}/share/users/{email}",
    dependencies=[Depends(require_bearer_token)],
)
def remove_share_user(
    notebook_id: str,
    email: str,
    notebook_auth: Optional[Dict[str, Any]] = Depends(get_notebook_auth_payload),
) -> Dict[str, Any]:
    """Revoca acceso compartido de un usuario."""
    valid_email = _validate_email(email)

    async def _op(client: NotebookLMClient):
        return await client.sharing.remove_user(notebook_id, valid_email)

    share_status = run_notebook_share_operation(notebook_auth, _op)
    return {"ok": True, "item": _share_status_payload(share_status)}
