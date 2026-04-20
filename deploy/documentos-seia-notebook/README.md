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
```
