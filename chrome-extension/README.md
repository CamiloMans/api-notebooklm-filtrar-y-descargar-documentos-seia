# Myma NotebookLM Cookie Sync (Chrome Extension)

Extension MV3 que lee las cookies de Google del browser actual y las envia al
backend Myma para mantener viva la sesion NotebookLM sin que el usuario tenga
que re-pegar cookies manualmente.

Esto resuelve la limitacion de las cookies pegadas a mano:
`__Secure-1PSIDTS` rota cada ~10-15 min en uso real, y solo el browser captura
esa rotacion. La extension la sincroniza al backend para mantener la sesion
servidor en sintonia con el browser.

## Que hace

- Lee `chrome.cookies.getAll` sobre dominios `.google.com` / `accounts.google.com`
  / `notebooklm.google.com`.
- Convierte el resultado al formato Playwright storage state que ya acepta
  `POST /api/v1/adenda/notebook/credentials` (campo `cookies_text` con un JSON
  `{"cookies": [...]}`).
- Envia con headers `Authorization: Bearer <API_BEARER_TOKEN>` y
  `X-Myma-User-JWT: <Supabase access_token>`.
- Repite cada N minutos via `chrome.alarms` (default 10 min, configurable
  1-60).

El backend valida con `fetch_tokens` y persiste cifrado via
`store_credentials`, asi que cada sync rota tambien el `__Secure-1PSIDTS`
fresco.

## Instalacion (sideload, modo desarrollador)

1. Clona o copia este directorio (`chrome-extension/`) localmente.
2. En Chrome abrir `chrome://extensions`.
3. Activar **Modo de desarrollador** (toggle arriba a la derecha).
4. Click en **Cargar descomprimida** y seleccionar la carpeta
   `chrome-extension/`.
5. La extension aparecera con un boton M en la barra de Chrome.

## Configuracion inicial

1. Asegurarse de tener sesion abierta en `accounts.google.com` con la cuenta
   Google que usa NotebookLM.
2. Abrir el popup de la extension (click en el icono).
3. Llenar:
   - **Backend URL** — ej. `https://api.tu-dominio` (sin `/` final).
   - **API Bearer Token** — el `API_BEARER_TOKEN` del backend (mismo que usa
     el resto de la API Myma).
   - **Supabase refresh_token** — el `refresh_token` (no el access_token) del
     usuario duenno de las credenciales. La extension lo intercambia por un
     `access_token` fresco contra `POST /api/v1/adenda/auth/refresh` antes de
     cada sync, asi no hay que repegar nada cuando el access_token expira a la
     hora. Para sacarlo: en `apps/web` logueado, DevTools → Application →
     Local Storage → key `sb-<ref>-auth-token` → field `refresh_token`. O en
     consola: `(await window.supabase.auth.getSession()).data.session.refresh_token`.
   - **Intervalo (min)** — default 10. Bajar a 5 si quieres mas frecuencia.
4. Click **Guardar**.
5. Click **Permitir host backend** y aceptar el prompt de Chrome (para que la
   extension pueda hacer `fetch` hacia el backend).
6. Click **Sync ahora** para verificar.
   - Si todo OK aparece "Sync OK." y el campo "Ultimo sync" se actualiza.
   - Si falla, el error sale rojo (HTTP code + body).

A partir de ahi la sync corre sola cada N minutos mientras Chrome este abierto
(o en background si Chrome ejecuta service workers).

## Verificacion en backend

Cada sync exitoso deja un registro en la tabla
`notebook_user_credentials_events` con `event_type=store` y luego eventos
`cookie_rotation` cuando el upload posterior detecta cookies rotadas.

```sql
select event_type, source, ok, status_after, checked_at, last_error
from public.notebook_user_credentials_events
where user_id = '<UUID>'
order by checked_at desc
limit 20;
```

Si la sesion se mantiene viva, `__Secure-1PSIDTS` debe cambiar entre sync
sucesivos en `payload_enc.cookies` de `notebook_user_credentials`.

## Permisos

- `cookies` — leer cookies de Google.
- `alarms` — disparar sync periodico.
- `storage` — guardar config (URL backend, tokens) en `chrome.storage.local`.
- `host_permissions: https://*.google.com/*` — alcance de cookies.
- `optional_host_permissions: https://*/*, http://*/*` — la extension pide
  permiso de host para el backend en runtime al hacer click en
  **Permitir host backend**, asi se acota al dominio que el usuario configure.

La extension **no** envia cookies a ningun lugar fuera del backend que el
usuario configura.

## Limites

- El sync depende de que Chrome este corriendo. Si el browser esta cerrado,
  el sync se reanuda al abrir Chrome.
- La extension no provee UI para multiples cuentas — sincroniza la cuenta
  actualmente logueada en `accounts.google.com`. Si el usuario cambia de
  cuenta Google en su browser, se sincroniza la nueva (y el backend pasara a
  validar contra esa).
- No persiste el bearer token / JWT cifrados — quedan en
  `chrome.storage.local` (espacio aislado por extension). Para entornos
  sensibles usar perfil Chrome dedicado.
