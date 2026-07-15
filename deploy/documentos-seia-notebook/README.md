# documentos-seia-notebook deploy assets

Este directorio contiene los artefactos de despliegue para la VM Linux existente.

## Estructura esperada en la VM

```text
/opt/documentos-seia-notebook/
├── docker-compose.yml
├── .env
└── repos/
    └── documentos-seia/
```

Persistencia:

```text
/var/lib/documentos-seia-notebook/downloads/
```

El stack corre dos contenedores sobre el mismo volumen:

- `documentos-seia-api`: responde HTTP, consulta estado y sirve descargas.
- `documentos-seia-worker`: procesa descargas/descompresion/cargas NotebookLM.

Esto evita que el proceso web ejecute trabajos pesados, pero conserva acceso a
los mismos archivos descargados.

## Preparacion de la VM

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin nginx python3-certbot-nginx
sudo mkdir -p /opt/documentos-seia-notebook/repos/documentos-seia
sudo mkdir -p /var/lib/documentos-seia-notebook/downloads
sudo chown -R "$USER":"$USER" /opt/documentos-seia-notebook /var/lib/documentos-seia-notebook
```

## Archivos a copiar

- Copiar `docker-compose.yml` a `/opt/documentos-seia-notebook/docker-compose.yml`
- Copiar `.env.example` como `/opt/documentos-seia-notebook/.env` y completar secretos reales
- Copiar el repo actual a `/opt/documentos-seia-notebook/repos/documentos-seia/`

## Nginx del host

1. Reemplazar `__SERVER_NAME__` en `nginx/documentos-seia-notebook.conf`
2. Copiar a `/etc/nginx/sites-available/documentos-seia-notebook.conf`
3. Habilitar el sitio:

```bash
sudo ln -s /etc/nginx/sites-available/documentos-seia-notebook.conf /etc/nginx/sites-enabled/documentos-seia-notebook.conf
sudo nginx -t
sudo systemctl reload nginx
```

4. Emitir TLS:

```bash
sudo certbot --nginx -d your-subdomain.example.com
```

## Deploy del stack

```bash
cd /opt/documentos-seia-notebook
docker compose build
docker compose up -d
docker compose ps
```

## Smoke checks

```bash
curl http://127.0.0.1:8020/health
docker compose ps
docker compose logs -f documentos-seia-worker
```

## Descargas SEIA

`SEIA_DOWNLOAD_MAX_WORKERS=2` permite descargar archivos de una misma corrida
en paralelo acotado. En VM chica partir con `2`; subir a `3` solo si no aparecen
429/503 de SEIA, cortes de descarga, CPU alta o presion de disco. El progreso se
reporta por bytes cuando `SEIA_DOWNLOAD_ESTIMATE_SIZES_FOR_PROGRESS=1`, pero se
persiste con throttle via `SEIA_DOWNLOAD_PROGRESS_MIN_INTERVAL_SEC=5` para no
saturar Supabase con actualizaciones por chunk.

## Piloto keepalive NotebookLM

`NOTEBOOK_KEEPALIVE_ENABLED=true` queda habilitado en `.env.example` para
piloto controlado. Monitorear `notebook_user_credentials_events` por eventos
`keepalive` y `keepalive_rotation`; para rollback, volver a
`NOTEBOOK_KEEPALIVE_ENABLED=false` y reiniciar el stack.

## Nota para Render

No separar en dos servicios Render usando disco local sin cambiar almacenamiento:
el disco de un Web Service y el de un Background Worker no es compartido. Para
Render hay dos caminos seguros:

- Mantener un solo servicio y bajar carga (`MAX_CONCURRENT_JOBS=1`,
  `NOTEBOOK_UPLOAD_MAX_WORKERS=1`).
- Separar web/worker solo si los documentos se guardan en almacenamiento
  compartido externo (por ejemplo Supabase Storage o S3), no en `/data/downloads`.
