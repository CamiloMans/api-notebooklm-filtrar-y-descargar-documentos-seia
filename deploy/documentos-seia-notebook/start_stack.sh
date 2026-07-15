#!/usr/bin/env bash
set -euo pipefail

ROOT="${DOCUMENTOS_SEIA_ROOT:-/opt/documentos-seia-notebook}"
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-myma-496119}"
export DOCUMENTOS_SEIA_ENV_FILE="${DOCUMENTOS_SEIA_ENV_FILE:-/run/myma-secrets/documentos-seia.env}"
export IMAGE_TAG="${IMAGE_TAG:?IMAGE_TAG is required}"

"$ROOT/render_runtime_env.sh"
docker compose --project-directory "$ROOT" -f "$ROOT/docker-compose.yml" \
  --profile worker up -d --no-build
