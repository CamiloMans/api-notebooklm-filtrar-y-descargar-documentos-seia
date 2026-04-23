# API Adenda Documentos SEIA

## Resumen

El flujo nuevo se divide en tres etapas:

1. `POST /api/v1/adenda/descarga-documentos-seia`
2. `GET /api/v1/adenda/descarga-documentos-seia/{run_id}`
3. `POST /api/v1/adenda/crear-y-cargar-notebook-filtrado`

Adicionalmente existe:

- `POST /api/v1/adenda/reintentar-carga-notebook`
- `POST /api/v1/adendas/notebooklm` como flujo legacy "todo en uno"

## 1. Encolar descarga CP6B

`POST /api/v1/adenda/descarga-documentos-seia`

Request:

```json
{
  "documento_seia": "https://infofirma.sea.gob.cl/DocumentosSEA/MostrarDocumento?docId=08/28/664d007e319ec36d30bb209a4779bae8e390",
  "tipo": "ifa",
  "exclude_keywords": ["plano", "foto", "lamina"]
}
```

Notas:

- `documento_seia` acepta URLs de `seia.sea.gob.cl` y `infofirma.sea.gob.cl`.
- `tipo` es obligatorio y se antepone al nombre final de cada archivo usando guion bajo.
- `exclude_keywords` es opcional.
- Si `exclude_keywords` viene vacio, no se aplica filtro por palabras en CP6B.
- La respuesta es asincrona y devuelve `202 Accepted`.
- `nombre_archivo_final` sigue siendo el nombre fisico local.
- `nombre_archivo_notebook` y `nombre_para_notebook` representan el nombre descriptivo usado al subir a NotebookLM.
- El mismo `run_id` se reutiliza luego para seguir el avance de la carga al notebook.

Response:

```json
{
  "status": "queued",
  "run_id": "uuid-de-corrida",
  "tipo": "ifa",
  "id_documento": "2141618022",
  "documents_found": 0,
  "documents": []
}
```

## 2. Consultar estado de corrida

`GET /api/v1/adenda/descarga-documentos-seia/{run_id}`

Response:

```json
{
  "status": "listed",
  "run_id": "uuid-de-corrida",
  "tipo": "ifa",
  "id_documento": "2141618022",
  "documents_found": 12,
  "progress_stage": "listed",
  "progress_current": 12,
  "progress_total": 12,
  "progress_percent": 100,
  "progress_message": "Listado CP6B generado.",
  "error_message": "",
  "notebooklm_id": "",
  "nombre_notebooklm": "",
  "retry_attempts": 0,
  "retry_documents_count": 0,
  "retry_document_ids": [],
  "documents": [
    {
      "document_id": "uuid-documento",
      "seleccionar": true,
      "selected": true,
      "categoria": "Linea de Base",
      "texto_link": "Capitulo 3",
      "url_origen": "https://seia.sea.gob.cl/archivos/...",
      "nombre_archivo": "Cap_3_Linea_de_Base.pdf",
      "nombre_archivo_final": "ifa_Linea_de_Base_Capitulo_3_Cap_3_Linea_de_Base.pdf",
      "nombre_archivo_notebook": "ifa_Linea_de_Base_Capitulo_3_Cap_3_Linea_de_Base.pdf",
      "nombre_para_notebook": "ifa_Linea_de_Base_Capitulo_3_Cap_3_Linea_de_Base.pdf",
      "formato": "pdf",
      "ruta_relativa": "Linea_de_Base/ifa_Linea_de_Base_Capitulo_3_Cap_3_Linea_de_Base.pdf",
      "tamano_bytes": 123456,
      "nivel_descarga_descompresion": 0,
      "origen": "descarga",
      "upload_status": "pending"
    }
  ]
}
```

Estados esperados de corrida:

- `queued`
- `running`
- `listed`
- `upload_queued`
- `creating_notebook`
- `uploading`
- `success`
- `partial_success`
- `failed`

## 3. Crear y cargar notebook filtrado

`POST /api/v1/adenda/crear-y-cargar-notebook-filtrado`

Request:

```json
{
  "run_id": "uuid-de-corrida",
  "nombre_notebook": "EIA - Proyecto X",
  "selected_document_ids": [
    "uuid-documento-1",
    "uuid-documento-2"
  ]
}
```

O para reutilizar un notebook existente:

```json
{
  "run_id": "uuid-de-corrida",
  "notebook_id": "notebook-existente-id",
  "selected_document_ids": [
    "uuid-documento-1",
    "uuid-documento-2"
  ]
}
```

Notas:

- Debes enviar exactamente uno entre `nombre_notebook` y `notebook_id`.
- `nombre_notebook`: crea un notebook nuevo con ese nombre.
- `notebook_id`: reutiliza un notebook ya existente y sube ahi los documentos seleccionados.
- La respuesta ahora es asincrona y devuelve `202 Accepted`.
- Para ver el avance de la carga debes seguir consultando `GET /api/v1/adenda/descarga-documentos-seia/{run_id}`.
- Durante la carga, `progress_stage` avanza por `upload_queued`, `creating_notebook` y `uploading`.
- Cuando termina la carga, el `GET` tambien devuelve `retry_attempts`, `retry_document_ids` y `retry_documents_count` con los documentos que fallaron o no alcanzaron a intentarse.
- La API de NotebookLM actual expone una subida por archivo, asi que este proyecto acelera la carga usando varias subidas individuales en paralelo.
- Por defecto se valida un maximo de `300` fuentes por notebook (`NOTEBOOK_SOURCES_PER_NOTEBOOK=300`) para cuentas NotebookLM Pro.
- Si la cuenta usa Standard, Plus o Ultra, ajusta `NOTEBOOK_SOURCES_PER_NOTEBOOK` y, si corresponde, `NOTEBOOK_UPLOAD_LIMIT` en `.env`.
- Puedes ajustar el paralelismo con `NOTEBOOK_UPLOAD_MAX_WORKERS` en `.env`. El valor recomendado en la VM `e2-micro` es `1`.

Response:

```json
{
  "status": "upload_queued",
  "run_id": "uuid-de-corrida",
  "tipo": "ifa",
  "id_documento": "2141618022",
  "notebooklm_id": "",
  "nombre_notebooklm": "EIA - Proyecto X",
  "documents_uploaded_ok": 0,
  "documents_uploaded_failed": 0,
  "retry_documents_count": 0,
  "retry_document_ids": [],
  "selected_documents": []
}
```

## 4. Reintentar carga fallida

`POST /api/v1/adenda/reintentar-carga-notebook`

Request:

```json
{
  "run_id": "uuid-de-corrida"
}
```

Comportamiento:

- Usa el `notebooklm_id` ya guardado en la corrida.
- Reintenta documentos con `selected=true` y `upload_status` en `failed`, `not_uploaded`, `selected`, `uploading` o `pending`.
- El response incrementa `retry_attempts` y devuelve `retry_document_ids` si aun quedan documentos pendientes de reintento.

## 5. Descargar zip de documentos fallidos

`GET /api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-fallidos.zip`

Comportamiento:

- Empaqueta en un `.zip` los documentos seleccionados que sigan con estado retryable.
- Incluye un archivo `LEEME.txt` con contexto de la corrida y cantidad de reintentos manuales.
- Esta pensado para el caso en que ya se hizo al menos un reintento y todavia quedan archivos para subir manualmente.

## 6. Descargar zip de documentos visibles de la tabla

`POST /api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-seleccionados.zip`

Request:

```json
{
  "selected_document_ids": [
    "uuid-documento-1",
    "uuid-documento-2"
  ]
}
```

Comportamiento:

- Empaqueta los documentos que sigan visibles en la tabla del frontend.
- Valida que cada `document_id` pertenezca al `run_id`.
- Usa `nombre_para_notebook` como nombre del archivo dentro del ZIP.
- Incluye un `LEEME.txt` con el contexto de la corrida.

## 7. Descargar zip de documentos visibles por partes

Para descargas grandes desde `app.myma.cl`, usar el flujo por partes para evitar limites del proxy.

`POST /api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-seleccionados/export`

Request:

```json
{
  "selected_document_ids": [
    "uuid-documento-1",
    "uuid-documento-2"
  ]
}
```

Response:

```json
{
  "export_id": "hash-seleccion",
  "filename": "documentos_para_notebook_corrida_hash.zip",
  "size_bytes": 144419790,
  "part_size_bytes": 8388608,
  "parts": 18
}
```

Luego descargar cada parte:

`GET /api/v1/adenda/descarga-documentos-seia/{run_id}/documentos-seleccionados/export/{export_id}/part/{part_index}`

Comportamiento:

- `part_index` empieza en `0`.
- Cada respuesta incluye `Content-Range`, `X-Zip-Part-Count`, `X-Zip-Part-Index` y `X-Zip-Filename`.
- El frontend debe concatenar las partes en orden para reconstruir el mismo `.zip`.

## 8. Endpoint legacy

`POST /api/v1/adendas/notebooklm`

- Mantiene el comportamiento anterior.
- No usa background.
- Sigue creando y cargando en una sola llamada.
