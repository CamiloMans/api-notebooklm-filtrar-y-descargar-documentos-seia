#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-myma-496119}"
TARGET="${DOCUMENTOS_SEIA_ENV_FILE:-/run/myma-secrets/documentos-seia.env}"
TARGET_DIR="$(dirname "$TARGET")"
TMP_FILE="$(mktemp)"
export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-/run/gcloud-documentos-seia}"

umask 077
trap 'rm -f "$TMP_FILE"' EXIT
install -d -m 700 "$TARGET_DIR"
install -d -m 700 "$CLOUDSDK_CONFIG"

secret() {
  gcloud secrets versions access latest --project "$PROJECT_ID" --secret "$1"
}

{
  printf 'SUPABASE_URL=%s\n' "$(secret documentos-seia-supabase-url)"
  printf 'SUPABASE_KEY=%s\n' "$(secret documentos-seia-supabase-key)"
  printf 'API_BEARER_TOKEN=%s\n' "$(secret documentos-seia-api-bearer-token)"
  printf 'NOTEBOOK_AUTH_ENCRYPTION_KEY=%s\n' "$(secret documentos-seia-auth-encryption-key)"
  printf '%s\n' \
    'API_CORS_ORIGINS=https://aplicaciones-myma.onrender.com,https://app.myma.cl,https://notebooklm-test.myma.cl' \
    'RUN_JOBS_IN_WEB=false' \
    'MAX_CONCURRENT_JOBS=1' \
    'SEIA_DOWNLOAD_MAX_WORKERS=1' \
    'SEIA_DOWNLOAD_RETRY_ATTEMPTS=20' \
    'SEIA_DOWNLOAD_READ_TIMEOUT_SEC=600' \
    'SEIA_DOWNLOAD_RETRY_BASE_SEC=8' \
    'SEIA_DOWNLOAD_FAILED_RETRY_PASSES=1' \
    'SEIA_DOWNLOAD_ESTIMATE_SIZES_FOR_PROGRESS=1' \
    'SEIA_DOWNLOAD_PROGRESS_MIN_INTERVAL_SEC=5' \
    'WORKER_POLL_INTERVAL_SEC=5' \
    'WORKER_IDLE_LOG_EVERY_SEC=60' \
    'NOTEBOOK_UPLOAD_MAX_WORKERS=1' \
    'NOTEBOOK_UPLOAD_RETRY_ATTEMPTS=5' \
    'NOTEBOOK_UPLOAD_RETRY_BASE_SEC=5' \
    'NOTEBOOK_UPLOAD_WAIT_TIMEOUT_SEC=600' \
    'NOTEBOOK_UPLOAD_SUBMIT_JITTER_SEC=0.6' \
    'NOTEBOOK_AUTH_RESUME_WAIT_SEC=600' \
    'NOTEBOOK_AUTH_RESUME_POLL_SEC=5' \
    'NOTEBOOK_AUTH_SOFT_EXPIRY_DAYS=14' \
    'NOTEBOOK_KEEPALIVE_ENABLED=true' \
    'NOTEBOOK_KEEPALIVE_INTERVAL_SEC=1800' \
    'NOTEBOOK_KEEPALIVE_ACTIVE_DAYS=7' \
    'NOTEBOOK_KEEPALIVE_MAX_CONCURRENCY=2' \
    'NOTEBOOK_KEEPALIVE_TIMEOUT_SEC=20' \
    'NOTEBOOK_PDF_OCR_ENABLED=false' \
    'ENVIRONMENT=production' \
    'LOG_LEVEL=INFO'
} > "$TMP_FILE"

chmod 600 "$TMP_FILE"
mv -f "$TMP_FILE" "$TARGET"
trap - EXIT
