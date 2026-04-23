FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    p7zip-full \
    unrar-free \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY \
    api_app.py \
    download_documento_seia.py \
    keepalive_worker.py \
    notebook_auth_store.py \
    notebook_user_credentials_schema.sql \
    supabase_cp6b_schema.sql \
    ./

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    API_OUTPUT_ROOT=/data/downloads

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

CMD ["uvicorn", "api_app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
