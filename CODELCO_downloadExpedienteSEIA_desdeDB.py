# Extracción de documentos de expedientes SEIA
# Versión 1: Usando endpoint AJAX directo

# ============================================================================
# IMPORTS
# ============================================================================
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import json
import sqlite3
import unicodedata
import os
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import csv
import zipfile
import tempfile
import shutil

try:
    import rarfile
    RARFILE_DISPONIBLE = True
except ImportError:
    RARFILE_DISPONIBLE = False
    rarfile = None

try:
    import py7zr
    PY7ZR_DISPONIBLE = True
except ImportError:
    PY7ZR_DISPONIBLE = False
    py7zr = None

# Extensiones de archivos comprimidos que se descargan temporalmente para listar contenido (recursivo)
EXTENSIONES_ARCHIVO_COMPRIMIDO = (".zip", ".rar", ".7z", ".kmz")

# ============================================================================
# CONFIGURACIÓN - TODAS LAS VARIABLES E INSUMOS
# ============================================================================

# --- Ruta base del script (donde está este archivo) ---
SCRIPT_DIR = Path(__file__).parent.resolve()

# --- Configuración de SEIA ---
BASE_SEIA = "https://seia.sea.gob.cl"
AJAX_ENDPOINT = "/expediente/xhr_documentos.php"

# --- Configuración de Base de Datos ---
DB_PATH = SCRIPT_DIR / "seia_cache.sqlite"  # Base de datos SQLite en la misma carpeta del script
DB_ESTADO_FILTRO = "aprobado"  # Estado de proyectos a filtrar
DB_PATRONES_TITULAR = ("codelco", "corporacion nacional del cobre")  # Patrones a buscar en titular

# --- Configuración de archivos de entrada (opcional, si no se usa DB) ---
# NOTA: Actualmente se usa DB como fuente principal. Excel/CSV son opcionales.
EXCEL_PATH = None  # Ruta al Excel con expedientes (opcional)
CSV_PATH_FALLBACK = None  # Ruta al CSV con expedientes (opcional)
COLUMN_NAME = "id_expediente"  # Nombre de la columna con los IDs de expedientes

# --- Configuración de procesamiento ---
LIMIT_EXPEDIENTES = None  # None = procesar todos, número = límite de expedientes
MAX_WORKERS = 2  # Número de threads. Mantener en 1 para reducir riesgo de bloqueo por scraping.

# --- Throttling / anti-bloqueo (scraping respetuoso) ---
REQUEST_DELAY_SEC = 1.2  # Segundos de espera entre cada petición HTTP al servidor SEIA
REQUEST_DELAY_BETWEEN_EXPEDIENTES_SEC = 3.0  # Segundos entre terminar un expediente y empezar el siguiente (solo efectivo con MAX_WORKERS=1)
MAX_REQUEST_RETRIES = 3  # Reintentos si el servidor responde 429 (rate limit) o 503 (sobrecarga)
RETRY_BACKOFF_BASE_SEC = 5  # Segundos base para backoff exponencial en reintentos

# --- Filtro SIG (Sistemas de Información Geográfica) ---
# Si es True, solo se conservan documentos con extensiones SIG relevantes (el resto se descarta del output).
# Si es False, se listan todos los documentos como antes.
FILTRAR_SOLO_SIG = True
EXTENSIONES_SIG = (
    # Vectoriales
    ".shp", ".shx", ".dbf", ".prj", ".cpg",  # Shapefile y sus compañeros
    ".gdb",                                     # ESRI File Geodatabase
    ".gpkg",                                    # GeoPackage (formato moderno)
    ".kml", ".kmz",                             # Google Earth / KML
    ".geojson", ".json",                        # GeoJSON
    ".gml",                                     # Geography Markup Language
    ".gpx",                                     # GPS Exchange Format
    ".tab", ".mif", ".mid",                     # MapInfo
    ".e00",                                     # ARC/INFO interchange
    ".dxf", ".dwg",                             # AutoCAD (frecuente en proyectos SEIA)
    # Raster / imágenes georreferenciadas
    ".tif", ".tiff",                            # GeoTIFF
    ".img",                                     # ERDAS Imagine
    ".ecw",                                     # Enhanced Compressed Wavelet
    ".jp2",                                     # JPEG2000 (georreferenciado)
    ".sid",                                     # MrSID
    ".nc",                                      # NetCDF
    ".adf",                                     # ArcGrid (ESRI raster)
    # LiDAR
    ".las", ".laz",
    # Comprimidos (pueden contener datos SIG)
    ".zip", ".rar", ".7z",
)

# --- Configuración de descargas ---
DOWNLOAD_FILES = False  # True = descargar archivos, False = solo extraer enlaces
MAX_DOWNLOAD_SIZE_MB = None  # Límite máximo por archivo en MB. None = sin límite. Ej: 200 para limitar a 200 MB.

# --- Configuración de archivos de salida ---
# Las carpetas se crearán en la misma ruta del script
OUTPUT_CSV_PREFIX = "CODELCO_SEIA-Expediente_SIG" if FILTRAR_SOLO_SIG else "CODELCO_SEIA-Expediente_completo"

# Rutas de salida (se actualizarán automáticamente cuando se ejecute el script)
# Valores por defecto (se sobrescriben al crear carpetas con timestamp)
DOWNLOAD_BASE_SEIA = SCRIPT_DIR / "downloads"
DEBUG_DIR = SCRIPT_DIR / "debug"

# NOTA: Las siguientes rutas se generan automáticamente en SCRIPT_DIR:
# - downloads/          (carpeta de descargas)
# - debug/              (carpeta de debug)
# - outputs_TIMESTAMP/  (carpeta de salida con timestamp)

# ============================================================================
# FIN DE CONFIGURACIÓN
# ============================================================================

def strip_accents(s: str) -> str:
    s = s or ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(ch)
    )

def norm(s: str) -> str:
    s = strip_accents(s)
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _extraer_extension_documento(doc: dict) -> str:
    """Extrae la extensión de un documento a partir de su nombre o URL."""
    nombre = doc.get("nombre_documento")
    if nombre and isinstance(nombre, str):
        parte = nombre.replace("\\", "/").strip().split("/")[-1]
        if parte:
            suf = Path(parte).suffix.lower()
            if suf:
                return suf
    url = doc.get("url_documento")
    if url and isinstance(url, str) and not url.startswith("javascript:"):
        try:
            return (Path(urlparse(url).path or "").suffix or "").lower()
        except Exception:
            pass
    return ""


def filtrar_documentos_sig(documentos: list) -> tuple:
    """
    Filtra documentos conservando solo aquellos con extensiones SIG relevantes.
    Returns:
        tuple: (documentos_filtrados, total_original, total_filtrado)
    """
    total_original = len(documentos)
    filtrados = []
    for doc in documentos:
        ext = _extraer_extension_documento(doc)
        if ext and ext in EXTENSIONES_SIG:
            filtrados.append(doc)
    return filtrados, total_original, len(filtrados)


def _safe_request(session, method, url, stream=False, timeout=30, **kwargs):
    """
    Realiza una petición HTTP con throttle (delay) y reintentos ante 429/503.
    Reduce riesgo de bloqueo por scraping.
    """
    last_exception = None
    for intent in range(MAX_REQUEST_RETRIES):
        time.sleep(REQUEST_DELAY_SEC)
        try:
            if method == "get":
                r = session.get(url, stream=stream, timeout=timeout, **kwargs)
            else:
                r = session.head(url, timeout=timeout, **kwargs)
            if r.status_code in (429, 503):
                if intent < MAX_REQUEST_RETRIES - 1:
                    retry_after = r.headers.get("Retry-After")
                    wait = int(retry_after) if retry_after and str(retry_after).isdigit() else (RETRY_BACKOFF_BASE_SEC * (2 ** intent))
                    print(f"      ⏳ Servidor ocupado ({r.status_code}). Reintento en {wait}s...")
                    time.sleep(wait)
                    continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            last_exception = e
            if intent < MAX_REQUEST_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE_SEC * (2 ** intent)
                print(f"      ⏳ Error de red. Reintento en {wait}s: {e}")
                time.sleep(wait)
            else:
                raise
    raise last_exception


def get_expediente_ids_from_db(
    db_path: Path,
    estado: str = None,
    patrones: tuple = None,
) -> list[str]:
    """
    Obtiene IDs de expedientes desde la base de datos SQLite.
    
    Args:
        db_path: Ruta al archivo SQLite
        estado: Estado a filtrar (default: usa DB_ESTADO_FILTRO de configuración)
        patrones: Tupla con patrones a buscar en titular (default: usa DB_PATRONES_TITULAR de configuración)
    
    Returns:
        Lista de IDs de expedientes únicos ordenados
    """
    if estado is None:
        estado = DB_ESTADO_FILTRO
    if patrones is None:
        patrones = DB_PATRONES_TITULAR
    
    # Convertir Path a string para sqlite3
    db_path_str = str(db_path) if isinstance(db_path, Path) else db_path
    con = sqlite3.connect(db_path_str)
    cur = con.cursor()
    cur.execute("""
        SELECT id_expediente, COALESCE(titular,'')
        FROM projects
        WHERE lower(estado) = ?
    """, (estado.lower(),))
    rows = cur.fetchall()
    con.close()

    p1 = patrones[0]
    p2 = patrones[1]

    ids_union = set()
    ids_p1 = set()
    ids_p2 = set()

    for id_exp, titular in rows:
        if id_exp is None:
            continue
        t = norm(titular)

        hit1 = p1 in t
        hit2 = p2 in t

        if hit1:
            ids_p1.add(str(id_exp))
            ids_union.add(str(id_exp))
        if hit2:
            ids_p2.add(str(id_exp))
            ids_union.add(str(id_exp))

    print("Conteos de proyectos APROBADOS (IDs únicos) desde DB:")
    print(f'- Titular contiene "{p1}": {len(ids_p1)}')
    print(f'- Titular contiene "corporación nacional del cobre" (con o sin tilde): {len(ids_p2)}')
    print(f"- Total (unión, sin duplicar): {len(ids_union)}")

    return sorted(ids_union)


def clean_text(text):
    """Limpia texto de espacios y caracteres especiales"""
    if not text:
        return ""
    return " ".join(str(text).split())

def extract_cell_text(cell):
    """
    Extrae texto de una celda HTML de forma robusta.
    Captura texto incluso cuando hay elementos HTML anidados.
    IMPORTANTE: Usar esta función en lugar de clean_text(cell.get_text()) 
    para asegurar que se capture todo el contenido de las celdas.
    """
    if cell is None:
        return ""
    
    # Método 1: Obtener texto con separador para preservar espacios entre elementos
    # Esto captura texto incluso cuando hay elementos HTML anidados (span, div, etc.)
    text = cell.get_text(separator=' ', strip=True)
    
    # Método 2: Si no hay texto, intentar obtener de atributos como title, alt, etc.
    if not text or text.strip() == "":
        text = cell.get('title', '') or cell.get('alt', '') or cell.get('data-title', '')
    
    # Método 3: Si aún no hay texto, intentar obtener de elementos hijos directamente
    if not text or text.strip() == "":
        # Buscar en elementos span, div, etc. dentro de la celda
        for elem in cell.find_all(['span', 'div', 'p', 'td', 'th']):
            elem_text = elem.get_text(separator=' ', strip=True)
            if elem_text:
                text = elem_text
                break
    
    # Limpiar el texto final
    return clean_text(text)

def normalize_header(header_text):
    """
    Normaliza un texto de header para comparación robusta.
    Elimina saltos de línea, normaliza espacios y convierte a minúsculas.
    
    Args:
        header_text: Texto del header a normalizar
    
    Returns:
        str: Header normalizado
    """
    if not header_text:
        return ""
    # Reemplazar saltos de línea y múltiples espacios por un solo espacio
    normalized = re.sub(r'\s+', ' ', str(header_text).replace('\n', ' ').replace('\r', ' '))
    return normalized.strip()

def find_header_index(headers, search_terms, start_idx=0):
    """
    Encuentra el índice de un header basándose en términos de búsqueda.
    
    Args:
        headers: Lista de headers normalizados
        search_terms: Lista de términos a buscar (ej: ["n°", "numero"])
        start_idx: Índice desde donde empezar a buscar (default: 0)
    
    Returns:
        int: Índice del header encontrado, o -1 si no se encuentra
    """
    for idx in range(start_idx, len(headers)):
        header_lower = headers[idx].lower()
        for term in search_terms:
            if term.lower() in header_lower:
                return idx
    return -1

def get_cell_value_by_header(headers, cells, search_terms, extract_func=None, default=None):
    """
    Obtiene el valor de una celda basándose en términos de búsqueda del header.
    
    Args:
        headers: Lista de headers normalizados
        cells: Lista de celdas HTML
        search_terms: Lista de términos a buscar en headers
        extract_func: Función para extraer texto de la celda (default: extract_cell_text)
        default: Valor por defecto si no se encuentra (default: None)
    
    Returns:
        str: Valor de la celda encontrada o default
    """
    if extract_func is None:
        extract_func = extract_cell_text
    
    idx = find_header_index(headers, search_terms)
    if idx >= 0 and idx < len(cells):
        return extract_func(cells[idx])
    return default

# Configuración de debug
def save_debug_info(id_expediente, debug_data, debug_dir=None):
    """
    Guarda información de debug en un archivo JSON.
    
    Args:
        id_expediente: ID del expediente
        debug_data: Diccionario con información de debug
        debug_dir: Directorio donde guardar los archivos de debug (si es None, usa DEBUG_DIR)
    """
    if debug_dir is None:
        debug_dir = DEBUG_DIR
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_file = debug_dir / f"debug_{id_expediente}.json"
        
        with open(debug_file, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, ensure_ascii=False, indent=2)
        
        print(f"  💾 Debug guardado: {debug_file}")
    except Exception as e:
        print(f"  ⚠️  Error guardando debug: {e}")

def create_debug_data(id_expediente, headers_raw, headers_normalized, first_col_empty, 
                      estructura, total_documentos, errores=None):
    """
    Crea un diccionario con información de debug estructurada.
    
    Args:
        id_expediente: ID del expediente
        headers_raw: Lista de headers sin normalizar
        headers_normalized: Lista de headers normalizados
        first_col_empty: Si la primera columna está vacía
        estructura: Tipo de estructura detectada ("moderna" o "antigua")
        total_documentos: Número total de documentos extraídos
        errores: Lista de errores encontrados (opcional)
    
    Returns:
        dict: Diccionario con información de debug
    """
    return {
        "id_expediente": id_expediente,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "estructura_detectada": estructura,
        "headers": {
            "raw": headers_raw,
            "normalized": headers_normalized,
            "total": len(headers_raw),
            "first_column_empty": first_col_empty
        },
        "extraccion": {
            "total_documentos": total_documentos,
            "errores": errores or []
        }
    }

def normalize_url(url):
    """
    Normaliza una URL para usar como clave de deduplicación.
    Elimina espacios, parámetros de query, fragmentos, y normaliza la estructura.
    
    Ejemplo:
    - "https://seia.sea.gob.cl/archivos/2024/06/28/856_Anexo_10_Paleontologia.rar " 
      → "https://seia.sea.gob.cl/archivos/2024/06/28/856_anexo_10_paleontologia.rar"
    """
    if not url or url.startswith("javascript:"):
        return None
    
    # Eliminar espacios al inicio y final, y espacios dentro de la URL
    url = url.strip()
    
    # Eliminar fragmentos (#)
    if '#' in url:
        url = url.split('#')[0]
    
    # Eliminar parámetros de query comunes que no afectan el archivo
    # Para archivos directos (/archivos/...), los query params no importan
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(url)
    
    # Normalizar path (eliminar espacios, dobles slashes, etc.)
    path = parsed.path.strip().replace(' ', '')
    # Normalizar dobles slashes pero mantener http://
    if path.startswith('//'):
        path = '/' + path.lstrip('/')
    
    # Para URLs de archivos directos, ignorar query params completamente
    # Para URLs de páginas intermedias, mantener solo parámetros importantes
    if '/archivos/' in path.lower():
        # Es un archivo directo, eliminar todos los query params
        query = ''
    else:
        # Es una página intermedia, mantener query params pero normalizar
        query = parsed.query.strip()
        # Eliminar espacios en query params
        if query:
            query = '&'.join(sorted(query.split('&')))  # Ordenar parámetros
    
    # Reconstruir URL normalizada
    normalized = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        '',  # params
        query,
        ''   # fragment
    ))
    
    return normalized

def is_better_documento_padre(current, candidate):
    """
    Determina si candidate es un mejor documento_padre que current.
    Prioriza nombres más informativos sobre genéricos como "Documento sin nombre".
    
    Reglas de priorización:
    1. Nombres específicos (ej: "Adenda", "Declaración de impacto ambiental") > genéricos ("Documento sin nombre")
    2. Entre genéricos, mantener el más largo
    3. Entre específicos, mantener el más largo (más informativo)
    """
    if not candidate or candidate.strip() == "":
        return False
    if not current or current.strip() == "":
        return True
    
    current_lower = current.lower().strip()
    candidate_lower = candidate.lower().strip()
    
    # Si son iguales, no cambiar
    if current_lower == candidate_lower:
        return False
    
    # Nombres genéricos a evitar (en orden de menos a más genérico)
    generic_names = [
        "documento sin nombre",
        "sin nombre", 
        "anexo sin nombre",
        "documento",
        "anexo"
    ]
    
    # Determinar si son genéricos
    current_is_generic = any(gen in current_lower for gen in generic_names)
    candidate_is_generic = any(gen in candidate_lower for gen in generic_names)
    
    # Si current es genérico y candidate no, candidate es mejor
    if current_is_generic and not candidate_is_generic:
        return True
    
    # Si candidate es genérico y current no, mantener current
    if candidate_is_generic and not current_is_generic:
        return False
    
    # Si ambos son genéricos o ambos no, mantener el más largo (más informativo)
    # Pero también considerar: si candidate tiene más palabras, es probablemente más descriptivo
    if len(candidate) > len(current):
        return True
    
    # Si tienen similar longitud, preferir el que tiene más palabras (más descriptivo)
    candidate_words = len(candidate.split())
    current_words = len(current.split())
    if candidate_words > current_words:
        return True
    
    return False

def merge_document_metadata(existing_doc, new_doc):
    """
    Fusiona metadatos de un documento nuevo con uno existente.
    Prioriza información más completa y evita sobrescribir con valores vacíos.
    """
    merged = existing_doc.copy()
    
    # Fusionar página_origen: mantener todas las fuentes
    if new_doc.get("pagina_origen") and new_doc["pagina_origen"] != existing_doc.get("pagina_origen"):
        if existing_doc.get("pagina_origen"):
            # Si ya hay una, crear lista o concatenar
            if isinstance(existing_doc["pagina_origen"], list):
                if new_doc["pagina_origen"] not in existing_doc["pagina_origen"]:
                    merged["pagina_origen"] = existing_doc["pagina_origen"] + [new_doc["pagina_origen"]]
            else:
                merged["pagina_origen"] = [existing_doc["pagina_origen"], new_doc["pagina_origen"]]
        else:
            merged["pagina_origen"] = new_doc["pagina_origen"]
    
    # Fusionar documento_padre: usar el mejor (más informativo)
    if is_better_documento_padre(existing_doc.get("documento_padre"), new_doc.get("documento_padre")):
        merged["documento_padre"] = new_doc.get("documento_padre")
        # Si cambió el documento_padre, también actualizar otros campos relacionados
        if new_doc.get("nombre_documento") and not existing_doc.get("nombre_documento"):
            merged["nombre_documento"] = new_doc.get("nombre_documento")
    
    # Fusionar otros campos: solo si el existente está vacío y el nuevo tiene valor
    fields_to_merge = [
        "nombre_documento", "categoria", "fecha", "etapa_proyecto", 
        "observaciones", "numero_resolucion", "folio", "remitido_por", 
        "destinado_a", "tipo_enlace"
    ]
    
    for field in fields_to_merge:
        existing_val = existing_doc.get(field)
        new_val = new_doc.get(field)
        
        # Si el existente está vacío/None y el nuevo tiene valor, usar el nuevo
        if (not existing_val or (isinstance(existing_val, str) and existing_val.strip() == "")) and new_val:
            merged[field] = new_val
    
    # Fusionar otros_datos
    if "otros_datos" not in merged:
        merged["otros_datos"] = {}
    if "otros_datos" in new_doc:
        merged["otros_datos"].update(new_doc["otros_datos"])
    
    return merged

def deduplicate_documents(documentos):
    """
    Deduplica documentos basándose en URL normalizada.
    Fusiona metadatos y prioriza documento_padre más informativo.
    """
    if not documentos:
        return documentos
    
    # Diccionario para almacenar documentos únicos por URL normalizada
    unique_docs = {}
    duplicates_count = 0
    
    for doc in documentos:
        url = doc.get("url_documento")
        
        # Si no tiene URL, agregarlo directamente (no se puede deduplicar)
        if not url or url.startswith("javascript:"):
            # Para documentos sin URL, usar nombre_documento + id_expediente como clave
            key = f"{doc.get('id_expediente')}_{doc.get('nombre_documento', '')}_{doc.get('etapa_proyecto', '')}"
            if key not in unique_docs:
                unique_docs[key] = doc
            else:
                # Fusionar si es duplicado
                unique_docs[key] = merge_document_metadata(unique_docs[key], doc)
                duplicates_count += 1
            continue
        
        # Normalizar URL para usar como clave
        normalized_url = normalize_url(url)
        
        if normalized_url:
            if normalized_url in unique_docs:
                # Documento duplicado: fusionar metadatos
                unique_docs[normalized_url] = merge_document_metadata(unique_docs[normalized_url], doc)
                duplicates_count += 1
            else:
                # Nuevo documento único
                unique_docs[normalized_url] = doc
        else:
            # URL no válida, agregar directamente
            key = f"no_url_{len(unique_docs)}"
            unique_docs[key] = doc
    
    if duplicates_count > 0:
        print(f"  🔄 Deduplicados {duplicates_count} documentos duplicados")
    
    # Convertir de diccionario a lista
    return list(unique_docs.values())

def safe_filename(name, maxlen=200):
    """
    Convierte un nombre a un nombre de archivo seguro.
    Elimina caracteres no permitidos y limita la longitud.
    """
    if not name:
        return "sin_nombre"
    # Reemplazar caracteres problemáticos
    safe = str(name)
    # Eliminar caracteres no permitidos en nombres de archivo
    forbidden = '<>:"/\\|?*'
    for char in forbidden:
        safe = safe.replace(char, '_')
    # Limitar longitud
    if len(safe) > maxlen:
        safe = safe[:maxlen]
    # Eliminar espacios al inicio/final
    safe = safe.strip()
    # Si queda vacío, usar nombre por defecto
    if not safe:
        safe = "sin_nombre"
    return safe

def get_download_path(doc, id_expediente, download_base=DOWNLOAD_BASE_SEIA):
    """
    Genera la ruta de descarga para un documento.

    Estructura:
        {base}/{id_expediente}_{nombre_proyecto}/{documento_padre}/{nombre_archivo}

    - La carpeta raíz combina id_expediente + nombre del proyecto.
    - Dentro, cada documento se agrupa por su documento_padre (si existe)
      o por su propio nombre como carpeta contenedora.
    """
    nombre_proyecto = doc.get("nombre_proyecto") or ""
    if nombre_proyecto:
        carpeta_expediente = safe_filename(f"{id_expediente}_{nombre_proyecto}", maxlen=150)
    else:
        carpeta_expediente = safe_filename(str(id_expediente))

    base_path = download_base / carpeta_expediente

    # Subcarpeta: documento_padre si es anexo, o el nombre del propio documento
    documento_padre = doc.get("documento_padre")
    if documento_padre:
        subcarpeta = safe_filename(documento_padre, maxlen=120)
    else:
        nombre_doc = doc.get("nombre_documento", "documento")
        # Quitar extensión para crear carpeta (evitar ".pdf" como nombre de carpeta)
        nombre_sin_ext = Path(nombre_doc).stem if nombre_doc else "documento"
        subcarpeta = safe_filename(nombre_sin_ext, maxlen=120)

    filename = safe_filename(doc.get("nombre_documento", "documento"))

    return base_path / subcarpeta / filename

def _describir_tipo_archivo(extension: str) -> str:
    """Retorna descripción legible del tipo de archivo según su extensión."""
    ext = (extension or "").lower().strip(".")
    tipos_comprimido = {
        "zip": "archivo comprimido ZIP", "rar": "archivo comprimido RAR",
        "7z": "archivo comprimido 7Z", "kmz": "archivo KMZ (comprimido)",
        "gz": "archivo comprimido GZ", "tar": "archivo TAR", "bz2": "archivo comprimido BZ2",
    }
    return tipos_comprimido.get(ext, f"archivo .{ext}" if ext else "archivo")


def download_document_file(session, doc, id_expediente, download_base=DOWNLOAD_BASE_SEIA):
    """
    Descarga un archivo de documento a la ruta especificada por get_download_path.
    Si MAX_DOWNLOAD_SIZE_MB no es None, verifica que el archivo no supere ese límite.

    Returns:
        tuple: (ruta_descargada | None, error_string | None)
    """
    url = doc.get("url_documento")
    if not url:
        return (None, None)

    max_bytes = MAX_DOWNLOAD_SIZE_MB * 1024 * 1024 if MAX_DOWNLOAD_SIZE_MB else None

    try:
        download_path = get_download_path(doc, id_expediente, download_base)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        if download_path.exists() and download_path.stat().st_size > 0:
            return (download_path, None)

        nombre_doc = doc.get("nombre_documento", "documento")
        extension = ""
        if nombre_doc:
            suf = Path(nombre_doc.replace("\\", "/").split("/")[-1]).suffix
            if suf:
                extension = suf
        if not extension:
            try:
                extension = Path(urlparse(url).path or "").suffix or ""
            except Exception:
                pass

        # Verificar tamaño con HEAD antes de descargar (solo si hay límite)
        if max_bytes:
            size_bytes = estimate_file_size(session, url, timeout=15)
            if size_bytes is not None and size_bytes > max_bytes:
                size_mb = size_bytes / (1024 * 1024)
                tipo = _describir_tipo_archivo(extension)
                error_msg = f"Descarga omitida: {tipo} de {size_mb:,.1f} MB supera límite de {MAX_DOWNLOAD_SIZE_MB} MB"
                print(f"      ⛔ {error_msg}")
                return (None, error_msg)

        response = _safe_request(session, "get", url, stream=True, timeout=30)
        response.raise_for_status()

        # Verificar Content-Length del GET (solo si hay límite)
        if max_bytes:
            cl = response.headers.get("Content-Length")
            if cl and int(cl) > max_bytes:
                size_mb = int(cl) / (1024 * 1024)
                tipo = _describir_tipo_archivo(extension)
                error_msg = f"Descarga omitida: {tipo} de {size_mb:,.1f} MB supera límite de {MAX_DOWNLOAD_SIZE_MB} MB"
                print(f"      ⛔ {error_msg}")
                response.close()
                return (None, error_msg)

        if not download_path.suffix:
            content_type = response.headers.get("Content-Type", "")
            ext_map = {
                "application/pdf": ".pdf",
                "application/msword": ".doc",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                "application/vnd.ms-excel": ".xls",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                "application/zip": ".zip",
                "application/x-rar-compressed": ".rar",
            }
            ext = ext_map.get(content_type, ".bin")
            download_path = download_path.with_suffix(ext)

        downloaded = 0
        with open(download_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    downloaded += len(chunk)
                    if max_bytes and downloaded > max_bytes:
                        break
                    f.write(chunk)

        if max_bytes and downloaded > max_bytes:
            try:
                download_path.unlink()
            except Exception:
                pass
            size_mb = downloaded / (1024 * 1024)
            tipo = _describir_tipo_archivo(extension)
            error_msg = f"Descarga abortada: {tipo} superó {MAX_DOWNLOAD_SIZE_MB} MB durante descarga ({size_mb:,.1f} MB recibidos)"
            print(f"      ⛔ {error_msg}")
            return (None, error_msg)

        return (download_path, None)

    except Exception as e:
        error_msg = f"Error de descarga: {e}"
        print(f"      ⚠️  Error descargando {doc.get('nombre_documento', 'documento')}: {e}")
        return (None, error_msg)


def _listar_contenido_archivo_recursivo(archivo_path: Path, ruta_prefijo: str = "") -> list:
    """
    Lista el contenido de un archivo comprimido (ZIP, RAR, 7Z, KMZ) de forma recursiva.
    Si dentro hay más archivos comprimidos, los lista también.
    Retorna lista de tuplas (ruta_interna, tamaño_bytes o None).
    """
    resultado = []
    ext = archivo_path.suffix.lower()
    if ext not in EXTENSIONES_ARCHIVO_COMPRIMIDO:
        return resultado

    try:
        if ext in (".zip", ".kmz"):
            with zipfile.ZipFile(archivo_path, "r") as z:
                for info in z.infolist():
                    nombre = info.filename.rstrip("/")
                    if not nombre:
                        continue
                    tamaño = info.file_size if not info.is_dir() else 0
                    ruta_completa = f"{ruta_prefijo}{nombre}" if ruta_prefijo else nombre
                    if info.is_dir():
                        continue
                    # Si es otro archivo comprimido, extraer a temp y listar recursivamente
                    suf = Path(nombre).suffix.lower()
                    if suf in EXTENSIONES_ARCHIVO_COMPRIMIDO:
                        with tempfile.NamedTemporaryFile(suffix=suf, delete=False) as tmp:
                            tmp_path = Path(tmp.name)
                            try:
                                with z.open(info) as member_file:
                                    tmp_path.write_bytes(member_file.read())
                                sub = _listar_contenido_archivo_recursivo(tmp_path, ruta_completa + "/")
                                resultado.extend(sub)
                            finally:
                                if tmp_path.exists():
                                    tmp_path.unlink(missing_ok=True)
                    else:
                        resultado.append((ruta_completa, tamaño))
        elif ext == ".rar" and RARFILE_DISPONIBLE:
            with rarfile.RarFile(archivo_path, "r") as rf:
                for info in rf.infolist():
                    if info.is_dir():
                        continue
                    nombre = info.filename.replace("\\", "/")
                    tamaño = info.file_size
                    ruta_completa = f"{ruta_prefijo}{nombre}" if ruta_prefijo else nombre
                    suf = Path(nombre).suffix.lower()
                    if suf in EXTENSIONES_ARCHIVO_COMPRIMIDO:
                        tmp_dir = tempfile.mkdtemp()
                        try:
                            rf.extract(info.filename, tmp_dir)
                            extraido = Path(tmp_dir) / nombre.replace("/", os.sep)
                            if extraido.exists():
                                sub = _listar_contenido_archivo_recursivo(extraido, ruta_completa + "/")
                                resultado.extend(sub)
                        finally:
                            shutil.rmtree(tmp_dir, ignore_errors=True)
                    else:
                        resultado.append((ruta_completa, tamaño))
        elif ext == ".7z" and PY7ZR_DISPONIBLE:
            with py7zr.SevenZipFile(archivo_path, mode="r") as z7:
                for nombre in z7.getnames():
                    if nombre.endswith("/"):
                        continue
                    nombre = nombre.rstrip("/")
                    ruta_completa = f"{ruta_prefijo}{nombre}" if ruta_prefijo else nombre
                    suf = Path(nombre).suffix.lower()
                    if suf in EXTENSIONES_ARCHIVO_COMPRIMIDO:
                        archivos = z7.read(targets=[nombre])
                        with tempfile.NamedTemporaryFile(suffix=suf, delete=False) as tmp:
                            tmp_path = Path(tmp.name)
                            try:
                                for _nom, stream in archivos.items():
                                    tmp_path.write_bytes(stream.read())
                                    break
                                sub = _listar_contenido_archivo_recursivo(tmp_path, ruta_completa + "/")
                                resultado.extend(sub)
                            finally:
                                if tmp_path.exists():
                                    tmp_path.unlink(missing_ok=True)
                    else:
                        resultado.append((ruta_completa, None))
    except Exception as e:
        print(f"      ⚠️  Error listando contenido de {archivo_path.name}: {e}")
    return resultado


def _descargar_archivo_temporal(session, url: str, timeout: int = 120):
    """Descarga un archivo desde URL a un archivo temporal. Retorna Path o None."""
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        resp = _safe_request(session, "get", url, stream=True, timeout=timeout)
        resp.raise_for_status()
        suf = Path(urlparse(url).path).suffix.lower() or ".bin"
        if suf not in EXTENSIONES_ARCHIVO_COMPRIMIDO:
            suf = ".zip"
        with tempfile.NamedTemporaryFile(suffix=suf, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    tmp.write(chunk)
        return tmp_path
    except Exception as e:
        print(f"      ⚠️  Error descargando temporalmente {url[:60]}...: {e}")
        return None


def expandir_documentos_contenido_archivos(documentos: list, session, id_expediente: str, nombre_proyecto=None) -> list:
    """
    Para cada documento cuya URL sea un archivo comprimido (ZIP, RAR, 7Z, KMZ), lo descarga
    temporalmente, lista su contenido (recursivo si hay más comprimidos dentro) y agrega
    cada elemento como un documento virtual al resultado. Se ejecuta siempre, independiente
    de DOWNLOAD_FILES, para tener el inventario completo en el output.
    """
    resultado = list(documentos)
    for doc in documentos:
        url = doc.get("url_documento")
        if not url or not isinstance(url, str) or url.startswith("javascript:"):
            continue
        path_parsed = urlparse(url)
        suf = Path(path_parsed.path or "").suffix.lower()
        if suf not in EXTENSIONES_ARCHIVO_COMPRIMIDO:
            continue
        nombre_archivo = doc.get("nombre_documento") or Path(path_parsed.path or "").name or "archivo"
        print(f"    📦 Listando contenido de archivo comprimido: {nombre_archivo[:70]}...")
        tmp_path = _descargar_archivo_temporal(session, url)
        if tmp_path is None:
            continue
        try:
            contenidos = _listar_contenido_archivo_recursivo(tmp_path, "")
            for ruta_interna, tamaño in contenidos:
                virtual = {
                    "id_expediente": id_expediente,
                    "nombre_proyecto": nombre_proyecto,
                    "documento_padre": nombre_archivo,
                    "nombre_documento": ruta_interna,
                    "url_documento": None,
                    "tipo_enlace": "contenido_archivo",
                    "pagina_origen": None,
                    "categoria": None,
                    "fecha": None,
                    "etapa_proyecto": None,
                    "observaciones": None,
                    "numero_resolucion": None,
                    "folio": None,
                    "remitido_por": None,
                    "destinado_a": None,
                    "otros_datos": {"url_archivo_contenedor": url, "tamano_bytes": tamaño, "es_contenido_archivo": True},
                }
                resultado.append(virtual)
            if contenidos:
                print(f"      ✅ {len(contenidos)} elementos dentro (recursivo)")
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
    return resultado


def extract_documents_from_intermediate_page(url, session, parent_doc_name=None):
    """
    Extrae documentos/anexos de una página intermedia.
    Retorna lista de documentos encontrados en esa página.
    """
    documentos_intermedios = []
    
    try:
        # Hacer petición a la página intermedia (con throttle y reintentos)
        response = _safe_request(session, "get", url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")
        
        # Buscar enlaces a documentos (archivos directos)
        all_links = soup.find_all("a", href=True)
        
        for link in all_links:
            href = link.get("href", "")
            link_text = clean_text(link.get_text())
            
            # IGNORAR enlaces JavaScript (no se pueden procesar)
            if href.startswith("javascript:"):
                continue
            
            # Construir URL absoluta
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = BASE_SEIA + href
            else:
                full_url = urljoin(BASE_SEIA, href)
            
            # Filtrar solo enlaces a documentos
            is_document = (
                any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar", ".txt"]) or
                any(keyword in href.lower() for keyword in ["documento", "descargar", "archivo", "anexo", "download"])
            )
            
            if is_document:
                doc_data = {
                    "id_expediente": None,  # Se asignará después
                    "tipo_enlace": "directo" if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]) else "pagina_intermedia",
                    "url_documento": full_url,
                    "nombre_documento": link_text or f"Anexo de {parent_doc_name}" if parent_doc_name else "Anexo sin nombre",
                    "categoria": None,
                    "fecha": None,
                    "etapa_proyecto": None,
                    "observaciones": None,
                    "numero_resolucion": None,
                    "folio": None,
                    "remitido_por": None,
                    "destinado_a": None,
                    "pagina_origen": url,  # URL de la página intermedia de donde viene
                    "documento_padre": parent_doc_name,  # Nombre del documento padre
                    "otros_datos": {}
                }
                documentos_intermedios.append(doc_data)
        
        # También buscar en tablas (puede haber tablas de anexos)
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue
            
            # Buscar headers
            headers = []
            header_row = table.find("thead")
            if header_row:
                header_cells = header_row.find_all(["th", "td"])
                headers = [clean_text(cell.get_text()) for cell in header_cells]
            else:
                if rows:
                    first_row = rows[0]
                    header_cells = first_row.find_all(["th", "td"])
                    headers = [clean_text(cell.get_text()) for cell in header_cells]
                    rows = rows[1:]
            
            # Procesar filas de la tabla
            for row in rows:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                
                # Buscar enlaces en las celdas
                for cell in cells:
                    links = cell.find_all("a", href=True)
                    for link in links:
                        href = link.get("href", "")
                        link_text = clean_text(link.get_text())
                        
                        if href.startswith("http"):
                            full_url = href
                        elif href.startswith("/"):
                            full_url = BASE_SEIA + href
                        else:
                            full_url = urljoin(BASE_SEIA, href)
                        
                        # Verificar si es un documento
                        is_document = (
                            any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]) or
                            any(keyword in href.lower() for keyword in ["documento", "descargar", "archivo", "anexo"])
                        )
                        
                        if is_document:
                            # Verificar si ya está en la lista
                            if not any(doc["url_documento"] == full_url for doc in documentos_intermedios):
                                doc_data = {
                                    "id_expediente": None,
                                    "tipo_enlace": "directo" if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]) else "pagina_intermedia",
                                    "url_documento": full_url,
                                    "nombre_documento": link_text or f"Anexo de {parent_doc_name}" if parent_doc_name else "Anexo sin nombre",
                                    "categoria": None,
                                    "fecha": None,
                                    "etapa_proyecto": None,
                                    "observaciones": None,
                                    "numero_resolucion": None,
                                    "folio": None,
                                    "remitido_por": None,
                                    "destinado_a": None,
                                    "pagina_origen": url,
                                    "documento_padre": parent_doc_name,
                                    "otros_datos": {}
                                }
                                documentos_intermedios.append(doc_data)
        
    except Exception as e:
        print(f"    ⚠️  Error extrayendo anexos de página intermedia {url}: {e}")
        return []
    
    return documentos_intermedios

def extract_nombre_proyecto(id_expediente, session):
    """
    Extrae el nombre del proyecto desde el título de la página del expediente.
    
    Args:
        id_expediente: ID del expediente
        session: Sesión de requests
    
    Returns:
        str: Nombre del proyecto o None si no se puede obtener
    """
    try:
        url_expediente = f"{BASE_SEIA}/expediente/expedientesEvaluacion.php?id_expediente={id_expediente}"
        response = _safe_request(session, "get", url_expediente, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")
        title_tag = soup.find("title")
        
        if title_tag:
            title_text = title_tag.get_text().strip()
            # Extraer el nombre del proyecto del título
            # Formato: "Ficha del Proyecto: Nombre del Proyecto"
            if "Ficha del Proyecto:" in title_text:
                nombre = title_text.split("Ficha del Proyecto:")[-1].strip()
                return nombre
            else:
                return title_text
        return None
    except Exception as e:
        print(f"      ⚠️  Error obteniendo nombre del proyecto: {e}")
        return None

def extract_documents_from_expediente(id_expediente, session=None, download_files=False):
    """
    Extrae documentos de un expediente usando el endpoint AJAX xhr_documentos.php.
    Retorna tupla (lista de documentos, nombre_proyecto).
    
    SOPORTA DOS ESTRUCTURAS DE SEIA:
    
    1. ESTRUCTURA MODERNA (Expedientes Nuevos):
       - Solo tiene tabla de documentos e-SEIA
       - Headers: N°, Folio, Documento, Remitido Por, Destinado A, Fecha, Acciones
       - Endpoint: xhr_documentos.php (equivalente a xhr_expediente2.php)
       - Características: Estructura simple y directa
       
    2. ESTRUCTURA ANTIGUA (Expedientes Antiguos):
       - Tiene AMBAS tablas (son COMPLEMENTARIAS, no duplicadas):
         a) Tabla de ETAPAS: Etapa del Proyecto, Observaciones, Fecha, N° Resolución, Archivo Digital
         b) Tabla E-SEIA: N°, Folio, Documento, Remitido Por, Destinado A, Fecha, Acciones
       - Endpoint: xhr_documentos.php (ambos endpoints son equivalentes)
       - IMPORTANTE: Se procesan AMBAS tablas porque aportan información diferente:
         * Tabla Etapas: Información organizacional por etapa del proyecto
         * Tabla e-SEIA: Documentos descargables con metadatos completos
    
    La función detecta automáticamente la estructura y procesa las tablas correspondientes.
    En estructura antigua, ambas tablas se procesan para obtener información completa.
    
    Args:
        id_expediente: ID del expediente
        session: Sesión de requests (opcional)
        download_files: Si es True, descarga los archivos. Si es False, solo extrae los enlaces (por defecto False)
    
    Returns:
        tuple: (documentos, nombre_proyecto) donde documentos es lista de dicts y nombre_proyecto es str o None
    """
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9",
            "Referer": f"{BASE_SEIA}/expediente/expedientesEvaluacion.php?id_expediente={id_expediente}"
        })
    
    # Obtener nombre del proyecto
    nombre_proyecto = extract_nombre_proyecto(id_expediente, session)
    
    url = f"{BASE_SEIA}{AJAX_ENDPOINT}?id_expediente={id_expediente}"
    documentos = []
    
    # Variables para debug
    debug_headers_raw = []
    debug_headers_normalized = []
    debug_first_col_empty = False
    debug_estructura = None
    
    try:
        # Hacer petición al endpoint AJAX (con throttle y reintentos)
        response = _safe_request(session, "get", url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")
        
        # DETECTAR ESTRUCTURA: Moderna o Antigua
        # CRITERIO PRINCIPAL: 
        # - Estructura ANTIGUA: tiene secciones/encabezados "Documentos Publicados" y "Documentos e-seia"
        # - Estructura MODERNA: NO tiene esas secciones, solo tiene tabla con id="tbldocumentos"
        
        # Buscar texto que indique secciones de estructura antigua
        texto_html = soup.get_text().lower()
        tiene_seccion_publicados = any(phrase in texto_html for phrase in [
            "documentos publicados", 
            "documentos publicados:", 
            "documentos publicados\n"
        ])
        tiene_seccion_eseia = any(phrase in texto_html for phrase in [
            "documentos e-seia", 
            "documentos e-seia:", 
            "documentos e-seia\n"
        ])
        
        # Buscar también en elementos HTML (h2, h3, h4, divs con clases específicas)
        elementos_publicados = soup.find_all(string=re.compile(r"Documentos\s+Publicados", re.I))
        elementos_eseia = soup.find_all(string=re.compile(r"Documentos\s+e-seia", re.I))
        
        tiene_secciones_antiguas = (
            tiene_seccion_publicados or 
            tiene_seccion_eseia or 
            len(elementos_publicados) > 0 or 
            len(elementos_eseia) > 0
        )
        
        # Verificar tabla moderna específica
        tabla_moderna_tbldocumentos = soup.find("table", id="tbldocumentos")
        
        estructura = None
        if tabla_moderna_tbldocumentos and not tiene_secciones_antiguas:
            # Si tiene id="tbldocumentos" Y NO tiene secciones antiguas, es moderna
            estructura = "moderna"
            debug_estructura = "moderna"
            print(f"  🆕 Estructura MODERNA detectada:")
            print(f"     • Criterio: Tabla 'tbldocumentos' encontrada sin secciones antiguas")
            print(f"     • Tabla ID: {tabla_moderna_tbldocumentos.get('id', 'N/A')}")
            print(f"     • Endpoint: xhr_documentos.php")
        elif tiene_secciones_antiguas:
            # Si tiene secciones "Documentos Publicados" o "Documentos e-seia", es antigua
            estructura = "antigua"
            debug_estructura = "antigua"
            print(f"  📜 Estructura ANTIGUA detectada:")
            print(f"     • Criterio: Secciones 'Documentos Publicados' o 'Documentos e-seia' encontradas")
            print(f"     • Secciones detectadas: {len(elementos_publicados)} publicados, {len(elementos_eseia)} e-seia")
            print(f"     • Endpoint: xhr_documentos.php")
            print(f"     • IMPORTANTE: Se procesarán AMBAS tablas (Etapas + e-SEIA) porque son complementarias")
        elif tabla_moderna_tbldocumentos:
            # Tiene tbldocumentos pero también secciones antiguas (caso raro)
            estructura = "moderna"
            debug_estructura = "moderna"
            print(f"  🆕 Estructura MODERNA detectada (tabla tbldocumentos)")
            print(f"     • Endpoint: xhr_documentos.php")
        else:
            # Fallback: verificar por thead/tbody
            tablas_con_thead = soup.find_all("table")
            tiene_thead = any(t.find("thead") for t in tablas_con_thead)
            if tiene_thead and soup.find("table", class_=re.compile(r"tabla-dinamica|dataTable", re.I)):
                estructura = "moderna"
                debug_estructura = "moderna"
                print(f"  🆕 Estructura MODERNA detectada (por clases CSS/thead)")
                print(f"     • Endpoint: xhr_documentos.php")
            else:
                estructura = "antigua"
                debug_estructura = "antigua"
                print(f"  📜 Estructura ANTIGUA detectada (por defecto)")
                print(f"     • Endpoint: xhr_documentos.php")
        
        # Debug: buscar tabla por ID específico primero
        tabla_publicados = soup.find("table", id=re.compile(r"documentos_publicados|tbldocumentos", re.I))
        if tabla_publicados:
            print(f"  🔍 Tabla 'documentos_publicados' o 'tbldocumentos' encontrada por ID")
        
        # Buscar todas las tablas
        all_tables = soup.find_all("table")
        print(f"  📋 Total de tablas encontradas: {len(all_tables)}")
        
        for table in all_tables:
            # Extraer filas de la tabla
            rows = table.find_all("tr")
            if not rows:
                continue
            
            # Intentar identificar headers (compatible con ambas estructuras)
            headers_raw = []
            headers = []
            header_row = table.find("thead")
            if header_row:
                # Estructura moderna: usa thead
                header_cells = header_row.find_all(["th", "td"])
                # Usar extract_cell_text para mejor captura de headers (puede tener saltos de línea)
                headers_raw = [extract_cell_text(cell) for cell in header_cells]
                # Normalizar headers
                headers = [normalize_header(h) for h in headers_raw]
                # En estructura moderna, las filas de datos están en tbody
                tbody = table.find("tbody")
                if tbody:
                    rows = tbody.find_all("tr")
                else:
                    # Si no hay tbody, usar todas las filas excepto las del thead
                    rows = [r for r in rows if r.parent.name != "thead"]
            else:
                # Estructura antigua: headers en primera fila o sin thead
                if rows:
                    first_row = rows[0]
                    header_cells = first_row.find_all(["th", "td"])
                    # Usar extract_cell_text para mejor captura de headers (puede tener saltos de línea)
                    headers_raw = [extract_cell_text(cell) for cell in header_cells]
                    # Normalizar headers
                    headers = [normalize_header(h) for h in headers_raw]
                    # Verificar si la primera fila es realmente header (contiene palabras como "Etapa", "Fecha", etc.)
                    headers_text = " ".join(headers).lower()
                    is_likely_header = any(keyword in headers_text for keyword in ["etapa", "fecha", "resolución", "archivo", "observaciones", "documento", "categoría"])
                    if is_likely_header:
                        rows = rows[1:]  # Saltar header
            
            # Debug: detectar primera columna vacía
            first_col_empty = len(headers) > 0 and headers[0] == ""
            if first_col_empty:
                print(f"  ⚠️  ADVERTENCIA: Primera columna vacía detectada en expediente {id_expediente}")
                print(f"     Total de headers: {len(headers)}")
                print(f"     Headers raw: {headers_raw}")
                print(f"     Headers normalizados: {headers}")
            
            # Verificar si es la tabla de "Etapa del Proyecto" (formato antiguo)
            # IMPORTANTE: Esta verificación debe ser MÁS ESPECÍFICA y ANTES que documentos e-seia
            headers_lower = [h.lower() for h in headers]
            headers_joined = " ".join(headers_lower)
            
            # Tabla de ETAPAS: debe tener "etapa del proyecto" Y "archivo digital" Y ("resolución" o "n°")
            # Y NO debe tener "remitido por" (que es característico de documentos e-seia)
            is_etapas_table = (
                "etapa" in headers_joined and 
                "proyecto" in headers_joined and
                ("resolución" in headers_joined or "n°" in headers_joined) and
                ("archivo" in headers_joined or "digital" in headers_joined) and
                "remitido" not in headers_joined  # Excluir si tiene "remitido por"
            )
            
            # Verificar si es la tabla "Documentos e-seia" (formato antiguo)
            # Tiene columnas: N°, Folio, Documento, Remitido Por, Destinado A, Fecha, Acciones
            # IMPORTANTE: Solo si NO es tabla de etapas
            is_documentos_table = (
                not is_etapas_table and  # Excluir si ya es tabla de etapas
                (
                    ("documento" in headers_joined or "documentos" in headers_joined) and
                    ("remitido" in headers_joined or "remitido por" in headers_joined) and
                    ("fecha" in headers_joined or "generación" in headers_joined)
                ) or table.get("id", "").lower() in ["tbldocumentos"]
            )
            
            # Debug: mostrar qué tipo de tabla se detectó
            if is_etapas_table:
                print(f"  ✅ Tabla de ETAPAS detectada - Headers: {headers}")
            elif is_documentos_table:
                print(f"  ✅ Tabla de DOCUMENTOS E-SEIA detectada - Headers: {headers}")
            
            # Guardar información de debug si es tabla de documentos e-seia (después de definir is_documentos_table)
            if is_documentos_table:
                debug_headers_raw = headers_raw.copy()
                debug_headers_normalized = headers.copy()
                debug_first_col_empty = first_col_empty
            
            # Procesar filas de datos
            for row in rows:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                
                doc_data = {
                    "id_expediente": id_expediente,
                    "tipo_enlace": None,  # "directo" o "pagina_intermedia"
                    "url_documento": None,
                    "nombre_documento": None,
                    "categoria": None,
                    "fecha": None,
                    "etapa_proyecto": None,
                    "observaciones": None,
                    "numero_resolucion": None,
                    "folio": None,
                    "remitido_por": None,
                    "destinado_a": None,
                    "otros_datos": {}
                }
                
                # IMPORTANTE: Procesar ETAPAS ANTES que documentos e-seia
                # Si es tabla de etapas, procesar de forma especial: UNA fila = UN documento completo
                if is_etapas_table:
                    # Verificar que esta fila NO sea el header (comparar con headers)
                    row_text = " ".join([clean_text(cell.get_text()) for cell in cells]).lower()
                    headers_text = " ".join(headers).lower()
                    # Si el texto de la fila es muy similar a los headers, es el header
                    if row_text == headers_text or all(h in row_text for h in headers if len(h) > 3):
                        continue  # Saltar esta fila, es el header
                    
                    # Primero, extraer todos los datos de la fila
                    for idx, cell in enumerate(cells):
                        # Usar extract_cell_text en lugar de clean_text(cell.get_text()) para mejor extracción
                        cell_text = extract_cell_text(cell)
                        header_name = headers[idx] if idx < len(headers) else f"col_{idx}"
                        header_lower = header_name.lower()
                        
                        # Mapear cada columna a su campo correspondiente
                        if "etapa" in header_lower and "proyecto" in header_lower:
                            doc_data["etapa_proyecto"] = cell_text
                        elif "observaciones" in header_lower:
                            doc_data["observaciones"] = cell_text
                        elif "fecha" in header_lower:
                            doc_data["fecha"] = cell_text
                        elif "resolución" in header_lower or ("n°" in header_lower and "resolución" in header_lower):
                            doc_data["numero_resolucion"] = cell_text
                        elif "archivo" in header_lower or "digital" in header_lower:
                            # Esta columna tiene el enlace al documento (puede ser un botón de descarga)
                            href = None
                            
                            # Estrategia 1: Buscar enlaces <a href>
                            links = cell.find_all("a", href=True)
                            if links:
                                href = links[0].get("href", "")
                            
                            # Estrategia 2: Buscar en onclick (botones, imágenes con JavaScript)
                            if not href:
                                onclick_elems = cell.find_all(attrs={"onclick": True})
                                for elem in onclick_elems:
                                    onclick = elem.get("onclick", "")
                                    # Buscar URLs en onclick (pueden estar entre comillas)
                                    url_match = re.search(r"['\"]([^'\"]*(?:documento|descargar|archivo)[^'\"]*)['\"]", onclick, re.IGNORECASE)
                                    if url_match:
                                        href = url_match.group(1)
                                        break
                                    # También buscar URLs que empiecen con / o http
                                    url_match = re.search(r"(https?://[^\s'\"<>]+|/[^\s'\"<>]+)", onclick)
                                    if url_match:
                                        href = url_match.group(1)
                                        break
                            
                            # Estrategia 3: Buscar en data-href, data-url, etc.
                            if not href:
                                for attr in ["data-href", "data-url", "href"]:
                                    elem = cell.find(attrs={attr: True})
                                    if elem:
                                        href = elem.get(attr, "")
                                        if href:
                                            break
                            
                            # Construir URL absoluta si encontramos href
                            if href:
                                if href.startswith("http"):
                                    full_url = href
                                elif href.startswith("/"):
                                    full_url = BASE_SEIA + href
                                else:
                                    full_url = urljoin(BASE_SEIA, href)
                                
                                doc_data["url_documento"] = full_url
                                
                                # Detectar tipo de enlace
                                if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]):
                                    doc_data["tipo_enlace"] = "directo"
                                elif any(keyword in href.lower() for keyword in ["documento.php", "descargar", "ver", "mostrar"]):
                                    doc_data["tipo_enlace"] = "pagina_intermedia"
                                else:
                                    doc_data["tipo_enlace"] = "pagina_intermedia"
                    
                    # Construir nombre del documento basado en etapa y resolución
                    if not doc_data["nombre_documento"]:
                        nombre_parts = []
                        if doc_data["etapa_proyecto"]:
                            nombre_parts.append(doc_data["etapa_proyecto"])
                        if doc_data["numero_resolucion"]:
                            nombre_parts.append(f"Resolución {doc_data['numero_resolucion']}")
                        if doc_data["fecha"]:
                            nombre_parts.append(doc_data["fecha"])
                        doc_data["nombre_documento"] = " - ".join(nombre_parts) if nombre_parts else "Documento de etapa"
                    
                    # IMPORTANTE: Agregar el documento completo SIEMPRE si tiene etapa_proyecto
                    # Incluso si no tiene URL (como "Admisión" que no tiene archivo digital)
                    if doc_data["etapa_proyecto"]:
                        documentos.append(doc_data)
                        # Debug
                        print(f"      ➕ Etapa agregada: {doc_data['etapa_proyecto']} - URL: {'Sí' if doc_data['url_documento'] else 'No'}")
                    continue  # Saltar el procesamiento general para esta fila
                
                # Si es tabla de "Documentos e-seia", procesar de forma especial: UNA fila = UN documento completo
                if is_documentos_table:
                    # Verificar que esta fila NO sea el header
                    row_text = " ".join([clean_text(cell.get_text()) for cell in cells]).lower()
                    headers_text = " ".join(headers).lower()
                    if row_text == headers_text or all(h in row_text for h in headers if len(h) > 3):
                        continue  # Saltar esta fila, es el header
                    
                    # Extraer todos los datos de la fila usando búsqueda robusta por headers
                    # Número (N°)
                    numero_value = get_cell_value_by_header(headers, cells, ["n°", "numero"], extract_func=extract_cell_text)
                    if numero_value:
                        doc_data["otros_datos"]["numero"] = numero_value
                    
                    # Folio
                    folio_value = get_cell_value_by_header(headers, cells, ["folio"], extract_func=extract_cell_text)
                    if folio_value:
                        doc_data["folio"] = folio_value
                    
                    # Documento (nombre y enlace)
                    doc_idx = find_header_index(headers, ["documento"])
                    if doc_idx >= 0 and doc_idx < len(cells):
                        cell = cells[doc_idx]
                        cell_text = extract_cell_text(cell)
                        # Esta columna tiene el nombre y posiblemente el enlace
                        # Buscar enlaces en la celda
                        links = cell.find_all("a", href=True)
                        if links:
                            # Buscar el primer enlace que NO sea JavaScript
                            for link in links:
                                href = link.get("href", "")
                                link_text = clean_text(link.get_text())
                                
                                # IGNORAR enlaces JavaScript
                                if href.startswith("javascript:"):
                                    # Si es JavaScript, solo usar el texto como nombre
                                    if link_text and not doc_data["nombre_documento"]:
                                        doc_data["nombre_documento"] = link_text
                                    continue
                                
                                # Construir URL absoluta (solo si no es JavaScript)
                                if href.startswith("http"):
                                    full_url = href
                                elif href.startswith("/"):
                                    full_url = BASE_SEIA + href
                                else:
                                    full_url = urljoin(BASE_SEIA, href)
                                
                                doc_data["url_documento"] = full_url
                                doc_data["nombre_documento"] = link_text or cell_text
                                
                                # Detectar tipo de enlace
                                if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]):
                                    doc_data["tipo_enlace"] = "directo"
                                elif any(keyword in href.lower() for keyword in ["documento.php", "descargar", "ver", "mostrar", "elementosfisicos"]):
                                    doc_data["tipo_enlace"] = "pagina_intermedia"
                                else:
                                    doc_data["tipo_enlace"] = "pagina_intermedia"
                                break  # Usar el primer enlace válido
                        else:
                            # Si no hay enlace, usar el texto como nombre
                            if cell_text and not doc_data["nombre_documento"]:
                                doc_data["nombre_documento"] = cell_text
                    
                    # Remitido Por
                    remitido_idx = find_header_index(headers, ["remitido", "remitido por"])
                    if remitido_idx >= 0 and remitido_idx < len(cells):
                        cell = cells[remitido_idx]
                        doc_data["remitido_por"] = extract_cell_text(cell)
                    
                    # Destinado A
                    destinado_idx = find_header_index(headers, ["destinado", "destinado a"])
                    if destinado_idx >= 0 and destinado_idx < len(cells):
                        cell = cells[destinado_idx]
                        cell_text = extract_cell_text(cell)
                        # Mejorar extracción de "Destinado A"
                        # Puede tener enlaces JavaScript con "X destinatarios"
                        links_in_cell = cell.find_all("a", href=True)
                        if links_in_cell:
                            # Si hay enlaces, verificar si son JavaScript
                            for link in links_in_cell:
                                href = link.get("href", "")
                                link_text = clean_text(link.get_text())
                                
                                # Si es JavaScript (verDestinatarios), extraer ID del documento
                                if href.startswith("javascript:") and "verDestinatarios" in href:
                                    # Extraer ID del documento del JavaScript
                                    id_match = re.search(r"id_documento=(\d+)", href)
                                    if id_match:
                                        doc_id = id_match.group(1)
                                        # Guardar el texto del enlace (ej: "2 destinatarios") pero también el ID
                                        doc_data["destinado_a"] = link_text
                                        doc_data["otros_datos"]["id_documento_destinatarios"] = doc_id
                                    else:
                                        doc_data["destinado_a"] = link_text
                                elif href.startswith("http") or href.startswith("/"):
                                    # Es un enlace HTTP válido, usar el texto del enlace
                                    doc_data["destinado_a"] = link_text or cell_text
                                else:
                                    # Otro tipo de enlace, usar el texto
                                    doc_data["destinado_a"] = link_text or cell_text
                        else:
                            # No hay enlaces, usar el texto de la celda
                            doc_data["destinado_a"] = cell_text
                    
                    # Fecha
                    fecha_value = get_cell_value_by_header(headers, cells, ["fecha", "generación", "of. partes"], extract_func=extract_cell_text)
                    if fecha_value:
                        doc_data["fecha"] = fecha_value
                    
                    # Acciones
                    acciones_idx = find_header_index(headers, ["acciones"])
                    if acciones_idx >= 0 and acciones_idx < len(cells):
                        cell = cells[acciones_idx]
                        # Esta columna tiene botones de acción (ver/descargar)
                        # Buscar enlaces de descarga (iconos con flecha hacia abajo)
                        # Si no encontramos URL en la columna Documento, buscar aquí
                        if not doc_data["url_documento"]:
                            # Buscar enlaces en botones
                            links = cell.find_all("a", href=True)
                            if links:
                                # Buscar el enlace de descarga (puede tener title="Descargar" o similar)
                                for link in links:
                                    href = link.get("href", "")
                                    title = link.get("title", "").lower()
                                    # Buscar iconos de descarga o enlaces que no sean JavaScript
                                    if href and not href.startswith("javascript:"):
                                        # Es un enlace HTTP válido
                                        if href.startswith("http"):
                                            doc_data["url_documento"] = href
                                        elif href.startswith("/"):
                                            doc_data["url_documento"] = BASE_SEIA + href
                                        else:
                                            doc_data["url_documento"] = urljoin(BASE_SEIA, href)
                                        doc_data["tipo_enlace"] = "directo" if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]) else "pagina_intermedia"
                                        break
                            
                            # Si no encontramos enlace HTTP, buscar en onclick
                            if not doc_data["url_documento"]:
                                onclick_elems = cell.find_all(attrs={"onclick": True})
                                for elem in onclick_elems:
                                    onclick = elem.get("onclick", "")
                                    # Ignorar JavaScript de ventanas (verDestinatarios)
                                    if "verDestinatarios" in onclick:
                                        continue
                                    # Buscar URLs en onclick
                                    url_match = re.search(r"['\"]([^'\"]*(?:documento|descargar|archivo)[^'\"]*)['\"]", onclick, re.IGNORECASE)
                                    if url_match:
                                        href = url_match.group(1)
                                        if href.startswith("http"):
                                            doc_data["url_documento"] = href
                                        elif href.startswith("/"):
                                            doc_data["url_documento"] = BASE_SEIA + href
                                        else:
                                            doc_data["url_documento"] = urljoin(BASE_SEIA, href)
                                        doc_data["tipo_enlace"] = "pagina_intermedia"
                                        break
                    
                    # Agregar el documento completo
                    # IMPORTANTE: Solo agregar si tiene URL válida (no JavaScript)
                    # NO agregar documentos basados solo en enlaces JavaScript de "Destinado A"
                    url_doc = doc_data.get("url_documento", "")
                    url_valida = bool(url_doc) and not url_doc.startswith("javascript:")
                    
                    # Solo agregar si tiene URL válida (no JavaScript)
                    # Esto evita crear documentos falsos basados en enlaces JavaScript
                    # Todos los documentos reales en la tabla e-seia deberían tener una URL válida
                    if url_valida:
                        documentos.append(doc_data)
                    continue  # Saltar el procesamiento general para esta fila
                
                # Procesamiento general para otras tablas (NO etapas)
                for idx, cell in enumerate(cells):
                    # Usar extract_cell_text en lugar de clean_text(cell.get_text()) para mejor extracción
                    cell_text = extract_cell_text(cell)
                    header_name = headers[idx] if idx < len(headers) else f"col_{idx}"
                    header_lower = header_name.lower()
                    
                    # Buscar enlaces en la celda
                    links = cell.find_all("a", href=True)
                    
                    if links:
                        for link in links:
                            href = link.get("href", "")
                            link_text = clean_text(link.get_text())
                            
                            # Construir URL absoluta
                            if href.startswith("http"):
                                full_url = href
                            elif href.startswith("/"):
                                full_url = BASE_SEIA + href
                            else:
                                full_url = urljoin(BASE_SEIA, href)
                            
                            # Detectar tipo de enlace
                            # Enlaces directos a archivos
                            if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar"]):
                                if not doc_data["url_documento"]:
                                    doc_data["tipo_enlace"] = "directo"
                                    doc_data["url_documento"] = full_url
                                    doc_data["nombre_documento"] = link_text or cell_text or f"documento_{idx}"
                            
                            # Enlaces a páginas intermedias (documento.php, descargar, etc.)
                            elif any(keyword in href.lower() for keyword in ["documento.php", "descargar", "ver", "mostrar"]):
                                if not doc_data["url_documento"]:
                                    doc_data["tipo_enlace"] = "pagina_intermedia"
                                    doc_data["url_documento"] = full_url
                                    doc_data["nombre_documento"] = link_text or cell_text or f"documento_{idx}"
                            
                            # Otros enlaces (pueden ser documentos también)
                            else:
                                if not doc_data["url_documento"]:
                                    doc_data["tipo_enlace"] = "pagina_intermedia"
                                    doc_data["url_documento"] = full_url
                                    doc_data["nombre_documento"] = link_text or cell_text or f"documento_{idx}"
                    
                    # Guardar datos adicionales según el header
                    if cell_text:
                        if "nombre" in header_lower or "documento" in header_lower:
                            if not doc_data["nombre_documento"]:
                                doc_data["nombre_documento"] = cell_text
                        elif "categoria" in header_lower or "tipo" in header_lower:
                            doc_data["categoria"] = cell_text
                        elif "fecha" in header_lower and not doc_data["fecha"]:
                            doc_data["fecha"] = cell_text
                        else:
                            doc_data["otros_datos"][header_name] = cell_text
                
                # Solo agregar si tiene URL o nombre
                if doc_data["url_documento"] or doc_data["nombre_documento"]:
                    documentos.append(doc_data)
        
        # También buscar enlaces fuera de tablas (en divs, párrafos, etc.)
        # Buscar enlaces a documentos en todo el contenido
        all_links = soup.find_all("a", href=True)
        for link in all_links:
            href = link.get("href", "")
            link_text = clean_text(link.get_text())
            
            # Filtrar solo enlaces relevantes
            if any(keyword in href.lower() for keyword in ["documento", "descargar", ".pdf", ".doc", ".xls"]):
                if href.startswith("http"):
                    full_url = href
                elif href.startswith("/"):
                    full_url = BASE_SEIA + href
                else:
                    full_url = urljoin(BASE_SEIA, href)
                
                # Verificar si ya está en documentos
                if not any(doc["url_documento"] == full_url for doc in documentos):
                    doc_data = {
                        "id_expediente": id_expediente,
                        "tipo_enlace": "directo" if any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx"]) else "pagina_intermedia",
                        "url_documento": full_url,
                        "nombre_documento": link_text or "Documento sin nombre",
                        "categoria": None,
                        "fecha": None,
                        "etapa_proyecto": None,
                        "observaciones": None,
                        "numero_resolucion": None,
                        "folio": None,
                        "remitido_por": None,
                        "destinado_a": None,
                        "otros_datos": {}
                    }
                    documentos.append(doc_data)
        
        # Agregar campos de rastreo a todos los documentos encontrados
        for doc in documentos:
            if "pagina_origen" not in doc:
                doc["pagina_origen"] = None
            if "documento_padre" not in doc:
                doc["documento_padre"] = None
            if doc["id_expediente"] is None:
                doc["id_expediente"] = id_expediente
        
        # Debug: mostrar qué tablas se encontraron
        etapas_count = sum(1 for d in documentos if d.get("etapa_proyecto"))
        documentos_eseia_count = sum(1 for d in documentos if not d.get("etapa_proyecto") and not d.get("documento_padre"))
        print(f"  📊 Documentos extraídos: {etapas_count} etapas, {documentos_eseia_count} documentos e-seia")
        
        # VALIDACIÓN: Verificar que ambas tablas se encontraron en estructura antigua
        if estructura == "antigua":
            # Verificar que se encontraron ambas tablas
            tablas_etapas_encontradas = any(
                t for t in all_tables 
                if t.find('thead') and any(
                    "etapa" in h.lower() and "proyecto" in h.lower() and "archivo" in h.lower()
                    for h in [extract_cell_text(cell) for cell in t.find('thead').find('tr').find_all(['th', 'td'])]
                )
            )
            tablas_eseia_encontradas = any(
                t for t in all_tables
                if t.find('thead') and any(
                    "documento" in h.lower() and "remitido" in h.lower()
                    for h in [extract_cell_text(cell) for cell in t.find('thead').find('tr').find_all(['th', 'td'])]
                )
            )
            
            if not tablas_etapas_encontradas:
                print(f"  ⚠️  Advertencia: No se encontró tabla de ETAPAS en expediente antiguo {id_expediente}")
                print(f"     • Esto puede indicar un problema en la detección o que el expediente no tiene tabla de etapas")
            if not tablas_eseia_encontradas:
                print(f"  ⚠️  Advertencia: No se encontró tabla E-SEIA en expediente antiguo {id_expediente}")
                print(f"     • Esto puede indicar un problema en la detección o que el expediente no tiene tabla e-seia")
            
            if tablas_etapas_encontradas and tablas_eseia_encontradas:
                print(f"  ✅ Estructura ANTIGUA: Ambas tablas encontradas y procesadas correctamente")
                print(f"     • Tabla Etapas: {etapas_count} documentos (información organizacional)")
                print(f"     • Tabla e-SEIA: {documentos_eseia_count} documentos (documentos descargables)")
            elif tablas_etapas_encontradas:
                print(f"  ⚠️  Estructura ANTIGUA: Solo tabla Etapas encontrada ({etapas_count} documentos)")
            elif tablas_eseia_encontradas:
                print(f"  ⚠️  Estructura ANTIGUA: Solo tabla e-SEIA encontrada ({documentos_eseia_count} documentos)")
        elif estructura == "moderna":
            print(f"  ✅ Estructura MODERNA: Tabla e-SEIA procesada correctamente")
            print(f"     • Documentos e-SEIA: {documentos_eseia_count} documentos")
        
        # Procesar páginas intermedias para extraer anexos
        # FILTRAR: Solo URLs HTTP válidas (no JavaScript)
        paginas_intermedias = [
            d for d in documentos 
            if d.get("tipo_enlace") == "pagina_intermedia" 
            and d.get("url_documento")
            and not d.get("url_documento", "").startswith("javascript:")
        ]
        print(f"  📋 Buscando anexos en {len(paginas_intermedias)} páginas intermedias...")
        documentos_intermedios = []
        for doc in paginas_intermedias:
            print(f"    🔍 Extrayendo anexos de: {doc['nombre_documento']}")
            try:
                anexos = extract_documents_from_intermediate_page(
                    doc["url_documento"], 
                    session, 
                    parent_doc_name=doc["nombre_documento"]
                )
            except Exception as e:
                print(f"      ⚠️  Error extrayendo anexos: {e}")
                anexos = []
            # Asignar id_expediente a los anexos
            for anexo in anexos:
                anexo["id_expediente"] = id_expediente
            documentos_intermedios.extend(anexos)
            print(f"      ✅ Encontrados {len(anexos)} anexos")
        
        # Agregar los anexos a la lista de documentos
        documentos.extend(documentos_intermedios)
        
        print(f"  📦 Total documentos (incluyendo anexos): {len(documentos)}")
        
        # DEDUPLICACIÓN: Eliminar documentos duplicados basándose en URL normalizada
        documentos_antes = len(documentos)
        documentos = deduplicate_documents(documentos)
        documentos_despues = len(documentos)
        
        if documentos_antes != documentos_despues:
            print(f"  🔄 Deduplicación: {documentos_antes} → {documentos_despues} documentos únicos")
        
        # Expandir archivos comprimidos: descargar temporalmente RAR/ZIP/7Z/KMZ, listar contenido
        # (recursivo) y agregar cada elemento al output. Se omite cuando FILTRAR_SOLO_SIG está activo
        # (solo interesa listar archivos, no descomprimir).
        if not FILTRAR_SOLO_SIG:
            documentos = expandir_documentos_contenido_archivos(documentos, session, id_expediente, nombre_proyecto)
            print(f"  📦 Total documentos (incl. contenido de comprimidos): {len(documentos)}")
        else:
            print(f"  🗺️  Modo SIG: expansión de comprimidos omitida (solo listado)")
        
        # Guardar información de debug (se guardará en la carpeta de salida si está disponible)
        debug_data = create_debug_data(
            id_expediente=id_expediente,
            headers_raw=debug_headers_raw,
            headers_normalized=debug_headers_normalized,
            first_col_empty=debug_first_col_empty,
            estructura=debug_estructura or estructura,
            total_documentos=len(documentos)
        )
        # Guardar debug en la carpeta de salida (se definirá más adelante)
        save_debug_info(id_expediente, debug_data)
        
    except Exception as e:
        print(f"  ERROR extrayendo documentos de expediente {id_expediente}: {e}")
        # Guardar información de debug incluso si hay error
        debug_data = create_debug_data(
            id_expediente=id_expediente,
            headers_raw=debug_headers_raw,
            headers_normalized=debug_headers_normalized,
            first_col_empty=debug_first_col_empty,
            estructura=debug_estructura or "desconocida",
            total_documentos=len(documentos),
            errores=[str(e)]
        )
        save_debug_info(id_expediente, debug_data)
        return [], None
    
    return documentos, nombre_proyecto

def show_folder_structure(documents, id_expediente, download_base=DOWNLOAD_BASE_SEIA):
    """
    Muestra la estructura de carpetas que se generaría para los documentos.
    """
    print(f"\n📁 Estructura de carpetas para expediente {id_expediente}:")
    print(f"   Base: {download_base}")
    print(f"\n   {id_expediente}/")
    
    # Agrupar documentos por tipo
    etapas = [d for d in documents if d.get("etapa_proyecto")]
    documentos_eseia = [d for d in documents if not d.get("etapa_proyecto") and not d.get("documento_padre")]
    anexos = [d for d in documents if d.get("documento_padre")]
    
    if etapas:
        print(f"   ├── 01_Etapas_Proyecto/")
        for i, doc in enumerate(etapas[:5], 1):  # Mostrar máximo 5
            filename = safe_filename(doc.get("nombre_documento", "documento"))
            print(f"   │   ├── {filename}")
        if len(etapas) > 5:
            print(f"   │   └── ... ({len(etapas) - 5} más)")
    
    if documentos_eseia:
        print(f"   ├── 02_Documentos_eSEIA/")
        for i, doc in enumerate(documentos_eseia[:5], 1):
            filename = safe_filename(doc.get("nombre_documento", "documento"))
            print(f"   │   ├── {filename}")
        if len(documentos_eseia) > 5:
            print(f"   │   └── ... ({len(documentos_eseia) - 5} más)")
    
    if anexos:
        print(f"   └── 03_Anexos/")
        # Agrupar anexos por documento padre
        anexos_por_padre = {}
        for anexo in anexos:
            padre = anexo.get("documento_padre", "sin_padre")
            if padre not in anexos_por_padre:
                anexos_por_padre[padre] = []
            anexos_por_padre[padre].append(anexo)
        
        padres_list = list(anexos_por_padre.items())[:3]  # Mostrar máximo 3 padres
        for i, (padre, anexos_list) in enumerate(padres_list):
            padre_folder = safe_filename(padre, maxlen=50)
            is_last = i == len(padres_list) - 1 and len(anexos_por_padre) <= 3
            prefix = "   │   └──" if is_last and len(anexos_por_padre) <= 3 else "   │   ├──"
            print(f"{prefix} {padre_folder}/")
            for j, anexo in enumerate(anexos_list[:3], 1):  # Máximo 3 anexos por padre
                filename = safe_filename(anexo.get("nombre_documento", "anexo"))
                is_last_anexo = j == min(3, len(anexos_list)) and (i == len(padres_list) - 1 or len(anexos_por_padre) > 3)
                anexo_prefix = "   │   │   └──" if is_last_anexo else "   │   │   ├──"
                print(f"{anexo_prefix} {filename}")
            if len(anexos_list) > 3:
                print(f"   │   │   └── ... ({len(anexos_list) - 3} más)")
        
        if len(anexos_por_padre) > 3:
            print(f"   │   └── ... ({len(anexos_por_padre) - 3} documentos padre más)")


def validar_integridad_documento(doc: dict) -> list:
    """
    Valida la integridad de un documento y retorna lista de advertencias.
    
    Args:
        doc: Diccionario con datos del documento
    
    Returns:
        list: Lista de strings con advertencias (vacía si todo está bien)
    """
    advertencias = []
    origen = doc.get("origen")
    
    # Si origen no está definido, intentar inferirlo
    if not origen:
        if doc.get("etapa_proyecto"):
            origen = "Etapa del Proyecto"
        elif doc.get("documento_padre"):
            origen = "Anexo"
        else:
            origen = "Documento e-seia"
    
    # Validar según origen
    if origen == "Anexo":
        if not doc.get("documento_padre"):
            advertencias.append("Anexo sin documento_padre")
    
    elif origen == "Etapa del Proyecto":
        if not doc.get("etapa_proyecto"):
            advertencias.append("Documento de Etapa sin etapa_proyecto")
        if not doc.get("numero_resolucion") and not doc.get("observaciones"):
            advertencias.append("Documento de Etapa sin numero_resolucion ni observaciones")
    
    elif origen == "Documento e-seia":
        if not doc.get("folio") and not doc.get("remitido_por"):
            advertencias.append("Documento e-SEIA sin folio ni remitido_por")
    
    # Validar URL
    url = doc.get("url_documento")
    if url and not url.startswith(("http://", "https://", "javascript:")):
        advertencias.append(f"URL con formato sospechoso: {url[:50]}")
    
    # Validar fecha (formato básico)
    fecha = doc.get("fecha")
    if fecha:
        fecha_str = str(fecha).strip()
        # Verificar que tenga formato razonable (contiene números y separadores)
        if fecha_str and not any(c.isdigit() for c in fecha_str):
            advertencias.append(f"Fecha con formato sospechoso: {fecha_str}")
    
    return advertencias


def validar_lote_documentos(documentos: list) -> dict:
    """
    Valida un lote de documentos y retorna estadísticas.
    
    Args:
        documentos: Lista de documentos a validar
    
    Returns:
        dict: Estadísticas de validación
    """
    total = len(documentos)
    documentos_con_advertencias = 0
    total_advertencias = 0
    advertencias_por_tipo = {}
    
    for doc in documentos:
        advertencias = validar_integridad_documento(doc)
        if advertencias:
            documentos_con_advertencias += 1
            total_advertencias += len(advertencias)
            
            for adv in advertencias:
                tipo = adv.split(":")[0] if ":" in adv else adv
                advertencias_por_tipo[tipo] = advertencias_por_tipo.get(tipo, 0) + 1
    
    return {
        "total_documentos": total,
        "documentos_con_advertencias": documentos_con_advertencias,
        "total_advertencias": total_advertencias,
        "advertencias_por_tipo": advertencias_por_tipo,
        "porcentaje_validos": ((total - documentos_con_advertencias) / total * 100) if total > 0 else 0
    }


def normalize_document_record(doc: dict, download_base=DOWNLOAD_BASE_SEIA) -> dict:
    """
    Normaliza un registro de documento a un esquema plano para Excel.
    
    Args:
        doc: Diccionario con los datos del documento
        download_base: Ruta base de descarga para calcular ruta relativa
    """
    origen = None
    if doc.get("etapa_proyecto"):
        origen = "Etapa del Proyecto"
    elif doc.get("documento_padre"):
        origen = "Anexo"
    else:
        origen = "Documento e-seia"
    
    # Calcular ruta relativa (relativa a download_base)
    ruta_relativa = None
    id_expediente = doc.get("id_expediente")
    if id_expediente:
        try:
            full_path = get_download_path(doc, id_expediente, download_base)
            # Convertir download_base a Path para calcular ruta relativa
            base_path = Path(download_base) if not isinstance(download_base, Path) else download_base
            
            # Calcular ruta relativa
            try:
                ruta_relativa = str(full_path.relative_to(base_path))
            except ValueError:
                # Si no es subdirectorio, usar la ruta completa relativa al expediente
                ruta_relativa = str(Path(id_expediente) / full_path.name)
        except Exception as e:
            # Si hay error, dejar None
            ruta_relativa = None
    
    # Indicar si tiene enlace
    tiene_enlace = "Sí" if doc.get("url_documento") else "No"
    
    # Construir link_expediente (URL madre del proyecto)
    link_expediente = None
    if id_expediente:
        link_expediente = f"{BASE_SEIA}/expediente/expedientesEvaluacion.php?id_expediente={id_expediente}"
    
    # Normalizar fecha (limpiar formato si es necesario)
    fecha = doc.get("fecha")
    if fecha:
        # Limpiar espacios extra y normalizar
        fecha = clean_text(str(fecha))
        # Intentar parsear y normalizar formato (mantener formato original si no se puede parsear)
        try:
            # Si tiene formato DD/MM/YYYY HH:MM:SS, mantenerlo pero limpiar
            if isinstance(fecha, str) and "/" in fecha:
                # Limpiar espacios extra alrededor de la fecha
                fecha = fecha.strip()
        except Exception:
            # Si hay error, mantener el valor original limpio
            pass
    
    # Extraer datos de otros_datos
    otros_datos = doc.get("otros_datos", {})
    numero = otros_datos.get("numero")
    id_documento_destinatarios = otros_datos.get("id_documento_destinatarios")
    url_archivo_contenedor = otros_datos.get("url_archivo_contenedor")
    es_contenido_archivo = "Sí" if otros_datos.get("es_contenido_archivo") else "No"
    
    # Extensión del documento: desde nombre_documento o, si no hay, desde url_documento
    extension_documento = ""
    nombre_doc = doc.get("nombre_documento")
    if nombre_doc and isinstance(nombre_doc, str):
        # Si es ruta con carpetas (ej. contenido de archivo), tomar la última parte
        parte = nombre_doc.replace("\\", "/").strip().split("/")[-1]
        if parte:
            suf = Path(parte).suffix
            if suf:
                extension_documento = suf.lower()
    if not extension_documento and doc.get("url_documento"):
        try:
            path = urlparse(doc["url_documento"]).path or ""
            if path:
                extension_documento = (Path(path).suffix or "").lower()
        except Exception:
            pass
    
    return {
        "id_expediente": doc.get("id_expediente"),
        "nombre_proyecto": doc.get("nombre_proyecto"),
        "link_expediente": link_expediente,
        "origen": origen,
        "nombre_documento": doc.get("nombre_documento"),
        "extension_documento": extension_documento or None,
        "tipo_enlace": doc.get("tipo_enlace"),
        "url_documento": doc.get("url_documento"),
        "tiene_enlace": tiene_enlace,
        "ruta_relativa": ruta_relativa,
        "categoria": doc.get("categoria"),
        "fecha": fecha,  # Fecha normalizada
        "etapa_proyecto": doc.get("etapa_proyecto"),
        "observaciones": doc.get("observaciones"),
        "numero_resolucion": doc.get("numero_resolucion"),
        "numero": numero,  # N° de la columna N°
        "folio": doc.get("folio"),
        "remitido_por": doc.get("remitido_por"),
        "destinado_a": doc.get("destinado_a"),
        "id_documento_destinatarios": id_documento_destinatarios,  # ID extraído de JavaScript
        "documento_padre": doc.get("documento_padre"),
        "pagina_origen": doc.get("pagina_origen"),
        "es_contenido_archivo": es_contenido_archivo,
        "url_archivo_contenedor": url_archivo_contenedor,
        "error": doc.get("error"),
    }


def documents_to_dataframe(documents: list, download_base=DOWNLOAD_BASE_SEIA) -> pd.DataFrame:
    """
    Convierte la lista de documentos (incluyendo anexos) en un DataFrame ordenado.
    
    Args:
        documents: Lista de diccionarios con datos de documentos
        download_base: Ruta base de descarga para calcular rutas relativas
    
    Returns:
        pd.DataFrame: DataFrame con columnas ordenadas de forma consistente
    """
    registros = [normalize_document_record(d, download_base=download_base) for d in documents]
    df = pd.DataFrame(registros)
    
    # Definir orden de columnas (consistente para todos los outputs)
    column_order = [
        "id_expediente",
        "nombre_proyecto",
        "link_expediente",
        "origen",
        "nombre_documento",
        "extension_documento",
        "tipo_enlace",
        "url_documento",
        "tiene_enlace",
        "ruta_relativa",
        "categoria",
        "fecha",
        # Campos específicos de Etapas
        "etapa_proyecto",
        "observaciones",
        "numero_resolucion",
        # Campos específicos de e-SEIA
        "numero",  # N° de la columna N°
        "folio",
        "remitido_por",
        "destinado_a",
        "id_documento_destinatarios",  # ID extraído de JavaScript
        # Campos de anexos
        "documento_padre",
        "pagina_origen",
        # Contenido de archivos comprimidos (RAR/ZIP/7Z/KMZ listados recursivamente)
        "es_contenido_archivo",
        "url_archivo_contenedor",
        # Errores
        "error",
    ]
    
    # Reordenar columnas (mantener solo las que existen)
    existing_columns = [col for col in column_order if col in df.columns]
    other_columns = [col for col in df.columns if col not in column_order]
    df = df[existing_columns + other_columns]
    
    # Orden sugerido: id_expediente, origen, fecha, nombre_documento
    if "id_expediente" in df.columns:
        df.sort_values(by=["id_expediente", "origen", "fecha", "nombre_documento"], inplace=True, na_position="last")
    return df


def estimate_file_size(session, url, timeout=10):
    """
    Estima el tamaño de un archivo usando HEAD request (sin descargar).
    
    Args:
        session: Sesión de requests
        url: URL del archivo
        timeout: Timeout en segundos
    
    Returns:
        Tamaño en bytes (int) o None si no se puede determinar
    """
    if not url or url.startswith("javascript:"):
        return None
    
    try:
        response = _safe_request(session, "head", url, timeout=timeout, allow_redirects=True)
        if response.status_code == 200:
            content_length = response.headers.get("Content-Length")
            if content_length:
                return int(content_length)
    except Exception:
        pass
    
    return None


def estimate_expediente_size_and_time(documents, session, avg_download_speed_mbps=5.0, show_progress=True):
    """
    Estima el tamaño total y tiempo de descarga para un expediente.
    Incluye tiempo de procesamiento estimado basado en delays entre requests.
    
    Args:
        documents: Lista de documentos del expediente
        session: Sesión de requests
        avg_download_speed_mbps: Velocidad promedio de descarga en Mbps (default: 5 Mbps)
        show_progress: Si es True, muestra progreso durante la estimación
    
    Returns:
        dict con: total_size_bytes, total_size_mb, total_size_gb, estimated_time_seconds, 
                 estimated_time_str, processing_time_seconds, total_time_seconds, total_time_str,
                 urls_checked, urls_failed, total_documents
    """
    import time as time_module
    
    total_size = 0
    urls_checked = 0
    urls_failed = 0
    start_time = time_module.time()
    
    # Filtrar solo documentos con URL válida
    docs_con_url = [d for d in documents if d.get("url_documento") and not d.get("url_documento", "").startswith("javascript:")]
    
    if show_progress:
        print(f"  📊 Estimando tamaño de {len(docs_con_url)} documentos...")
    
    # Tiempo estimado de procesamiento (delay entre requests + tiempo de HEAD request)
    avg_request_time = 0.5  # segundos promedio por request (incluye delay)
    estimated_processing_time = len(docs_con_url) * avg_request_time
    
    for i, doc in enumerate(docs_con_url, 1):
        url = doc.get("url_documento")
        if not url:
            continue
        
        try:
            # Hacer HEAD request para obtener tamaño
            size = estimate_file_size(session, url)
            
            if size is not None:
                total_size += size
                urls_checked += 1
            else:
                urls_failed += 1
        except Exception:
            urls_failed += 1
        
        # Mostrar progreso cada 10 documentos
        if show_progress and i % 10 == 0:
            elapsed = time_module.time() - start_time
            avg_time_per_doc = elapsed / i if i > 0 else 0
            remaining_docs = len(docs_con_url) - i
            estimated_remaining = remaining_docs * avg_time_per_doc
            print(f"     Procesados: {i}/{len(docs_con_url)} | Tamaño acumulado: {total_size / (1024*1024):.2f} MB | Tiempo restante estimado: {estimated_remaining:.1f}s")
    
    # Calcular tiempo de descarga estimado
    # Convertir Mbps a bytes/segundo: 5 Mbps = 5 * 1024 * 1024 / 8 bytes/seg = 655360 bytes/seg
    bytes_per_second = avg_download_speed_mbps * 1024 * 1024 / 8
    estimated_download_time = total_size / bytes_per_second if bytes_per_second > 0 else 0
    
    # Tiempo total = tiempo de procesamiento (estimación) + tiempo de descarga
    total_time_seconds = estimated_processing_time + estimated_download_time
    
    # Formatear tiempo de descarga
    hours = int(estimated_download_time // 3600)
    minutes = int((estimated_download_time % 3600) // 60)
    seconds = int(estimated_download_time % 60)
    
    if hours > 0:
        estimated_time_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        estimated_time_str = f"{minutes}m {seconds}s"
    else:
        estimated_time_str = f"{seconds}s"
    
    # Formatear tiempo total
    total_hours = int(total_time_seconds // 3600)
    total_minutes = int((total_time_seconds % 3600) // 60)
    total_seconds = int(total_time_seconds % 60)
    
    if total_hours > 0:
        total_time_str = f"{total_hours}h {total_minutes}m {total_seconds}s"
    elif total_minutes > 0:
        total_time_str = f"{total_minutes}m {total_seconds}s"
    else:
        total_time_str = f"{total_seconds}s"
    
    return {
        "total_size_bytes": total_size,
        "total_size_mb": total_size / (1024 * 1024),
        "total_size_gb": total_size / (1024 * 1024 * 1024),
        "estimated_download_time_seconds": estimated_download_time,
        "estimated_download_time_str": estimated_time_str,
        "processing_time_seconds": estimated_processing_time,
        "total_time_seconds": total_time_seconds,
        "total_time_str": total_time_str,
        "urls_checked": urls_checked,
        "urls_failed": urls_failed,
        "total_documents": len(docs_con_url)
    }


print("✅ Función 'extract_documents_from_expediente' definida")
print("   Uso: documentos = extract_documents_from_expediente('157')")
print("✅ Función 'show_folder_structure' definida")
print("   Uso: show_folder_structure(documentos, '157')")
print("✅ Función 'documents_to_dataframe' definida")
print("   Uso: df = documents_to_dataframe(documentos)")
print("✅ Función 'estimate_file_size' definida")
print("   Uso: size = estimate_file_size(session, url)")
print("✅ Función 'estimate_expediente_size_and_time' definida")
print("   Uso: estimacion = estimate_expediente_size_and_time(documentos, session)")

# ============================================================================
# LECTURA DE EXPEDIENTES A PROCESAR
# ============================================================================

# Obtener IDs de expedientes desde la base de datos
expediente_ids = []
if DB_PATH.exists():
    expediente_ids = get_expediente_ids_from_db(DB_PATH)
    print(f"📋 Procesando {len(expediente_ids)} expedientes desde SQLite: {DB_PATH}")
else:
    print(f"⚠️  No se encontró la base de datos: {DB_PATH}")
    # Intentar leer desde Excel/CSV como fallback
    if EXCEL_PATH and Path(EXCEL_PATH).exists():
        try:
            df_expedientes = pd.read_excel(EXCEL_PATH, dtype=str)
            if COLUMN_NAME not in df_expedientes.columns:
                raise ValueError(f"La columna '{COLUMN_NAME}' no existe en el Excel. Columnas disponibles: {list(df_expedientes.columns)}")
            expediente_ids = df_expedientes[COLUMN_NAME].dropna().astype(str).tolist()
            print(f"✅ Leídos {len(expediente_ids)} expedientes del Excel: {EXCEL_PATH}")
        except Exception as e:
            print(f"⚠️  Error leyendo Excel: {e}")
            # Intentar CSV como fallback
            if CSV_PATH_FALLBACK and Path(CSV_PATH_FALLBACK).exists():
                df_expedientes = pd.read_csv(CSV_PATH_FALLBACK, sep=";", dtype=str)
                expediente_ids = df_expedientes[COLUMN_NAME].dropna().astype(str).tolist()
                print(f"✅ Leídos {len(expediente_ids)} expedientes del CSV: {CSV_PATH_FALLBACK}")
            else:
                raise FileNotFoundError(f"No se encontró ni la DB ni archivos de entrada. Configura DB_PATH, EXCEL_PATH o CSV_PATH_FALLBACK.")
    elif CSV_PATH_FALLBACK and Path(CSV_PATH_FALLBACK).exists():
        df_expedientes = pd.read_csv(CSV_PATH_FALLBACK, sep=";", dtype=str)
        expediente_ids = df_expedientes[COLUMN_NAME].dropna().astype(str).tolist()
        print(f"✅ Leídos {len(expediente_ids)} expedientes del CSV: {CSV_PATH_FALLBACK}")
    else:
        raise FileNotFoundError(f"No se encontró ninguna fuente de datos. Configura DB_PATH, EXCEL_PATH o CSV_PATH_FALLBACK.")

# Limitar número de expedientes si se especificó un límite
if LIMIT_EXPEDIENTES is not None and LIMIT_EXPEDIENTES > 0:
    total_expedientes = len(expediente_ids)
    expediente_ids = expediente_ids[:LIMIT_EXPEDIENTES]
    print(f"🔢 Modo PRUEBA: Procesando solo los primeros {len(expediente_ids)} de {total_expedientes} expedientes")
    print(f"   (Para procesar todos, cambia LIMIT_EXPEDIENTES = None)")
else:
    print(f"📋 Procesando todos los {len(expediente_ids)} expedientes")

if FILTRAR_SOLO_SIG:
    print(f"\n🗺️  MODO FILTRO SIG ACTIVO")
    print(f"   Solo se conservarán documentos con extensiones: {', '.join(sorted(set(EXTENSIONES_SIG)))}")
    print(f"   Expansión de archivos comprimidos: DESACTIVADA")
    print(f"   Para listar todos los documentos, cambia: FILTRAR_SOLO_SIG = False")

# Crear sesión reutilizable
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9"
})

# ============================================================================
# PROCESAR TODOS LOS EXPEDIENTES CON THREADPOOLEXECUTOR
# ============================================================================

# Carpeta fija (sin timestamp): mismo nombre que el script para reutilizar datos ya descargados
script_name = Path(__file__).stem
output_folder_name = script_name  # ej. CODELCO_downloadExpedienteSEIA_desdeDB
output_dir = SCRIPT_DIR / output_folder_name
output_dir.mkdir(parents=True, exist_ok=True)

# Crear subcarpetas dentro de la carpeta de salida
debug_output_dir = output_dir / "debug"
debug_output_dir.mkdir(parents=True, exist_ok=True)

# Crear subcarpeta para descargas dentro de la carpeta de salida
downloads_output_dir = output_dir / "downloads"
downloads_output_dir.mkdir(parents=True, exist_ok=True)

# Variables globales para usar las carpetas dentro de la carpeta de salida
DEBUG_DIR = debug_output_dir
DOWNLOAD_BASE_SEIA = downloads_output_dir

print(f"📁 Carpeta de salida creada: {output_dir}")
print(f"   📂 Debug: {debug_output_dir}")
print(f"   📂 Downloads: {downloads_output_dir}")

# Lock para escritura thread-safe en CSV
csv_lock = Lock()

# Función para procesar un expediente
def procesar_expediente(expediente_id, idx, total):
    """Procesa un expediente y retorna los datos."""
    # Pausa entre expedientes para no saturar el servidor (solo efectivo con MAX_WORKERS=1)
    if idx > 1 and REQUEST_DELAY_BETWEEN_EXPEDIENTES_SEC > 0:
        time.sleep(REQUEST_DELAY_BETWEEN_EXPEDIENTES_SEC)
    # Crear sesión propia para cada thread
    thread_session = requests.Session()
    thread_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    
    try:
        print(f"\n[{idx}/{total}] 📋 Procesando expediente: {expediente_id}")
        
        # Extraer documentos y nombre del proyecto
        expediente_docs, nombre_proyecto = extract_documents_from_expediente(
            expediente_id, thread_session, download_files=DOWNLOAD_FILES
        )
        
        if nombre_proyecto:
            print(f"   ✅ Nombre proyecto: {nombre_proyecto[:80]}...")
        else:
            print(f"   ⚠️  No se pudo obtener nombre del proyecto")
            nombre_proyecto = None
        
        print(f"   📄 Documentos encontrados: {len(expediente_docs)}")
        
        # Agregar nombre_proyecto a cada documento
        for doc in expediente_docs:
            doc["nombre_proyecto"] = nombre_proyecto
        
        # Filtro SIG: conservar solo documentos con extensiones geoespaciales
        if FILTRAR_SOLO_SIG and expediente_docs:
            expediente_docs, total_orig, total_sig = filtrar_documentos_sig(expediente_docs)
            descartados = total_orig - total_sig
            print(f"   🗺️  Filtro SIG: {total_sig} documentos SIG de {total_orig} totales ({descartados} descartados)")
            if not expediente_docs:
                print(f"   ℹ️  Sin documentos SIG en este expediente")
        
        # VALIDACIÓN DE INTEGRIDAD: Validar documentos antes de procesar
        if expediente_docs:
            # Calcular origen para cada documento (sin normalizar completamente)
            docs_para_validar = []
            for doc in expediente_docs:
                doc_copy = doc.copy()
                # Calcular origen igual que en normalize_document_record
                if doc_copy.get("etapa_proyecto"):
                    doc_copy["origen"] = "Etapa del Proyecto"
                elif doc_copy.get("documento_padre"):
                    doc_copy["origen"] = "Anexo"
                else:
                    doc_copy["origen"] = "Documento e-seia"
                docs_para_validar.append(doc_copy)
            
            stats_validacion = validar_lote_documentos(docs_para_validar)
            
            if stats_validacion["documentos_con_advertencias"] > 0:
                print(f"   ⚠️  Validación: {stats_validacion['documentos_con_advertencias']}/{stats_validacion['total_documentos']} documentos con advertencias")
                print(f"      Porcentaje válidos: {stats_validacion['porcentaje_validos']:.1f}%")
                if stats_validacion['advertencias_por_tipo']:
                    tipos_adv = list(stats_validacion['advertencias_por_tipo'].items())[:3]
                    print(f"      Tipos de advertencias: {', '.join([f'{k} ({v})' for k, v in tipos_adv])}")
            else:
                print(f"   ✅ Validación: Todos los documentos pasaron la validación de integridad")
        
        # Escribir documentos al CSV incremental
        if expediente_docs:
            df_docs = documents_to_dataframe(expediente_docs, download_base=DOWNLOAD_BASE_SEIA)
            
            # Agregar columna nombre_proyecto si no existe
            if "nombre_proyecto" not in df_docs.columns:
                df_docs["nombre_proyecto"] = nombre_proyecto
            
            # Reordenar columnas para poner nombre_proyecto después de id_expediente
            cols = list(df_docs.columns)
            if "nombre_proyecto" in cols and "id_expediente" in cols:
                cols.remove("nombre_proyecto")
                idx_pos = cols.index("id_expediente")
                cols.insert(idx_pos + 1, "nombre_proyecto")
                df_docs = df_docs[cols]
            
            # Escribir al CSV (append mode)
            with csv_lock:
                file_exists = output_csv.exists()
                df_docs.to_csv(
                    output_csv,
                    mode='a',
                    header=not file_exists,
                    index=False,
                    encoding='utf-8-sig',
                    sep=';'
                )
                print(f"   ✅ Documentos escritos en CSV: {len(df_docs)} filas")
        
        return {
            'expediente_id': expediente_id,
            'nombre_proyecto': nombre_proyecto,
            'documentos': expediente_docs,
            'error': None
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"   ❌ Error procesando expediente {expediente_id}: {error_msg}")
        return {
            'expediente_id': expediente_id,
            'nombre_proyecto': None,
            'documentos': [],
            'error': error_msg
        }
    finally:
        thread_session.close()

# Nombre de archivo fijo (sin timestamp) para retomar
output_csv = output_dir / f"{OUTPUT_CSV_PREFIX}.csv"

# Retomar: si el CSV ya existe, procesar solo los pendientes.
# Con N workers pueden estar escribiendo N expedientes a la vez; si se cancela, los últimos N
# expedientes (por orden de aparición al final del CSV) se consideran posiblemente incompletos y se reprocesan.
expediente_ids_ya_procesados = set()
if output_csv.exists():
    try:
        df_existente = pd.read_csv(output_csv, sep=";", encoding="utf-8-sig", low_memory=False)
        if "id_expediente" not in df_existente.columns or len(df_existente) == 0:
            expediente_ids_ya_procesados = set()
        else:
            # Últimos N expedientes (N = MAX_WORKERS) pueden estar incompletos si se cortó con varios workers
            n_posiblemente_incompletos = max(1, MAX_WORKERS)
            ids_orden = df_existente["id_expediente"].dropna().astype(str)
            # Recorrer de atrás hacia adelante y reunir los últimos N expedientes únicos
            ultimos_n_ids = []
            vistos = set()
            for eid in reversed(ids_orden.tolist()):
                if eid not in vistos:
                    vistos.add(eid)
                    ultimos_n_ids.append(eid)
                    if len(ultimos_n_ids) >= n_posiblemente_incompletos:
                        break
            expedientes_a_reprocesar = set(ultimos_n_ids)
            expediente_ids_ya_procesados = set(ids_orden.unique()) - expedientes_a_reprocesar
            # Quitar del CSV las filas de esos expedientes y guardar (sobrescribir)
            df_sin_ultimos = df_existente[~df_existente["id_expediente"].astype(str).isin(expedientes_a_reprocesar)]
            df_sin_ultimos.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
            print(f"   🔄 Últimos {len(expedientes_a_reprocesar)} expediente(s) en CSV (posiblemente incompletos) se reprocesarán: {', '.join(sorted(expedientes_a_reprocesar))}")
        expediente_ids = [eid for eid in expediente_ids if str(eid) not in expediente_ids_ya_procesados]
        if expediente_ids_ya_procesados:
            print(f"   🔄 Retomando: {len(expediente_ids_ya_procesados)} ya en CSV, {len(expediente_ids)} pendientes")
    except Exception as e:
        print(f"   ⚠️  No se pudo leer CSV existente para retomar: {e}")

print(f"📁 Archivos de salida (rutas fijas):")
print(f"   📄 CSV documentos: {output_csv.name}")

# Procesar expedientes con ThreadPoolExecutor (solo los pendientes si se retoma)
all_expedientes_docs = {}
resultados = []
max_workers = MAX_WORKERS

if not expediente_ids:
    print(f"\n✅ No hay expedientes pendientes. El CSV ya está al día.")
else:
    print(f"\n📋 PROCESANDO {len(expediente_ids)} EXPEDIENTES (con ThreadPoolExecutor)")
    print(f"   ⏱️  Throttle: {REQUEST_DELAY_SEC}s entre peticiones, {REQUEST_DELAY_BETWEEN_EXPEDIENTES_SEC}s entre expedientes")
    print(f"   🔄 Reintentos: {MAX_REQUEST_RETRIES} (429/503), backoff base {RETRY_BACKOFF_BASE_SEC}s")
    print("="*60)

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Enviar todas las tareas
    future_to_expediente = {
        executor.submit(procesar_expediente, exp_id, idx, len(expediente_ids)): exp_id
        for idx, exp_id in enumerate(expediente_ids, 1)
    }
    
    # Procesar resultados conforme se completan
    for future in as_completed(future_to_expediente):
        expediente_id = future_to_expediente[future]
        try:
            resultado = future.result()
            resultados.append(resultado)
            all_expedientes_docs[expediente_id] = resultado['documentos']
        except Exception as e:
            print(f"❌ Error inesperado con expediente {expediente_id}: {e}")
            all_expedientes_docs[expediente_id] = []

# Resumen final
print("\n" + "="*60)
print("📊 RESUMEN FINAL")
print("="*60)

total_docs = 0
expedientes_exitosos = 0
expedientes_con_error = 0

for resultado in resultados:
    if resultado['error'] is None and resultado['documentos']:
        total_docs += len(resultado['documentos'])
        expedientes_exitosos += 1
        print(f"✅ Expediente {resultado['expediente_id']}: {len(resultado['documentos'])} documentos")
    else:
        expedientes_con_error += 1
        print(f"⚠️  Expediente {resultado['expediente_id']}: {'Error: ' + resultado['error'] if resultado['error'] else 'Sin documentos'}")

print(f"\n📈 Resumen general:")
print(f"   Expedientes procesados: {len(expediente_ids)}")
print(f"   Expedientes exitosos: {expedientes_exitosos}")
print(f"   Expedientes con error: {expedientes_con_error}")
print(f"   Total documentos extraídos: {total_docs}")

# ============================================================================
# DESCARGAR ARCHIVOS (solo si DOWNLOAD_FILES = True)
# ============================================================================
if DOWNLOAD_FILES:
    print("\n" + "="*60)
    print("📥 DESCARGANDO ARCHIVOS")
    print(f"   ⚙️  Límite por archivo: {f'{MAX_DOWNLOAD_SIZE_MB} MB' if MAX_DOWNLOAD_SIZE_MB else 'sin límite'}")
    print("="*60)
    print(f"📁 Base de descarga: {DOWNLOAD_BASE_SEIA}")
    
    total_descargados = 0
    total_errores = 0
    total_omitidos_tamaño = 0
    errores_descarga = {}  # {(id_expediente, url_documento): error_msg}
    
    for expediente_id, expediente_docs in all_expedientes_docs.items():
        if not expediente_docs:
            continue
            
        print(f"\n{'='*60}")
        print(f"📥 Descargando archivos del expediente {expediente_id}")
        print(f"{'='*60}")
        
        docs_con_url = [d for d in expediente_docs if d.get("url_documento")]
        print(f"\n📦 Total de documentos con URL para descargar: {len(docs_con_url)}")
        
        descargados = 0
        errores = 0
        omitidos = 0
        
        for i, doc in enumerate(docs_con_url, 1):
            nombre = doc.get("nombre_documento", "documento")
            print(f"\n[{i}/{len(docs_con_url)}] {nombre[:60]}...")
            
            ruta, error_msg = download_document_file(session, doc, expediente_id, DOWNLOAD_BASE_SEIA)
            
            if error_msg:
                doc["error"] = error_msg
                errores_descarga[(str(expediente_id), doc.get("url_documento", ""))] = error_msg
                if "supera límite" in error_msg or "superó" in error_msg:
                    omitidos += 1
                else:
                    errores += 1
            elif ruta and ruta.exists() and ruta.stat().st_size > 0:
                descargados += 1
                print(f"      ✅ Guardado: {ruta}")
            else:
                errores += 1
                error_msg = "Descarga fallida: archivo vacío o no encontrado"
                doc["error"] = error_msg
                errores_descarga[(str(expediente_id), doc.get("url_documento", ""))] = error_msg
            
            time.sleep(0.3)
        
        total_descargados += descargados
        total_errores += errores
        total_omitidos_tamaño += omitidos
        
        print(f"\n   Resumen expediente {expediente_id}:")
        print(f"   ✅ Archivos guardados: {descargados}")
        if omitidos:
            print(f"   ⛔ Omitidos por tamaño (>{MAX_DOWNLOAD_SIZE_MB} MB): {omitidos}")
        print(f"   ❌ Errores: {errores}")
        print(f"   📁 Ubicación: {DOWNLOAD_BASE_SEIA / str(expediente_id)}")
    
    print(f"\n📊 Resumen general de descargas:")
    print(f"   ✅ Total archivos guardados: {total_descargados}")
    if total_omitidos_tamaño:
        print(f"   ⛔ Total omitidos por tamaño (>{MAX_DOWNLOAD_SIZE_MB} MB): {total_omitidos_tamaño}")
    print(f"   ❌ Total errores: {total_errores}")
    print(f"   📁 Ubicación base: {DOWNLOAD_BASE_SEIA}")
    
    # Actualizar CSV con errores de descarga (si hubo alguno)
    if errores_descarga and output_csv.exists():
        print(f"\n📝 Actualizando CSV con {len(errores_descarga)} error(es) de descarga...")
        try:
            df_update = pd.read_csv(output_csv, sep=";", encoding="utf-8-sig", low_memory=False)
            if "error" not in df_update.columns:
                df_update["error"] = None
            for (eid, url), err in errores_descarga.items():
                mask = (df_update["id_expediente"].astype(str) == eid) & (df_update["url_documento"].astype(str) == url)
                df_update.loc[mask, "error"] = err
            df_update.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
            print(f"   ✅ CSV actualizado con columna 'error'")
        except Exception as e:
            print(f"   ⚠️  No se pudo actualizar CSV con errores: {e}")
else:
    print(f"\n💡 Para activar descargas, cambia: DOWNLOAD_FILES = True")
    print(f"   Los archivos se descargarán en: {DOWNLOAD_BASE_SEIA}")
    print(f"   ⚙️  Límite por archivo: {f'{MAX_DOWNLOAD_SIZE_MB} MB' if MAX_DOWNLOAD_SIZE_MB else 'sin límite'}")

# ============================================================================
# GENERAR EXCEL DEL CSV CONSOLIDADO (después de descargas para incluir errores)
# ============================================================================
output_excel = None
if output_csv.exists():
    print(f"\n📊 Generando archivo Excel del consolidado...")
    try:
        df_consolidado = pd.read_csv(output_csv, sep=';', encoding='utf-8-sig', low_memory=False)
        output_excel = output_dir / f"{OUTPUT_CSV_PREFIX}.xlsx"
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            df_consolidado.to_excel(writer, sheet_name='Documentos', index=False)
            doc_data = {
                "Tipo de Documento": [
                    "Etapa del Proyecto",
                    "Documento e-seia",
                    "Anexo"
                ],
                "Columnas Relevantes": [
                    "etapa_proyecto, observaciones, numero_resolucion, fecha, url_documento",
                    "folio, remitido_por, destinado_a, fecha, url_documento",
                    "documento_padre, pagina_origen, url_documento"
                ],
                "Columnas NULL (No aplican)": [
                    "folio, remitido_por, destinado_a",
                    "etapa_proyecto, observaciones, numero_resolucion",
                    "etapa_proyecto, observaciones, numero_resolucion, folio, remitido_por, destinado_a"
                ],
                "Descripción": [
                    "Documentos organizados por etapa del proyecto. Información sobre resoluciones y observaciones.",
                    "Documentos del sistema e-SEIA con metadatos completos (folio, remitente, destinatario).",
                    "Anexos asociados a documentos principales. Requieren documento_padre para identificar origen."
                ]
            }
            df_doc = pd.DataFrame(doc_data)
            df_doc.to_excel(writer, sheet_name='Guía de Columnas', index=False)
            columnas_info = {
                "Columna": [
                    "id_expediente", "nombre_proyecto", "link_expediente", "origen",
                    "nombre_documento", "extension_documento", "tipo_enlace", "url_documento", "tiene_enlace",
                    "ruta_relativa", "categoria", "fecha",
                    "etapa_proyecto", "observaciones", "numero_resolucion",
                    "numero", "folio", "remitido_por", "destinado_a", "id_documento_destinatarios",
                    "documento_padre", "pagina_origen",
                    "es_contenido_archivo", "url_archivo_contenedor",
                    "error"
                ],
                "Descripción": [
                    "ID único del expediente en SEIA",
                    "Nombre del proyecto asociado al expediente",
                    "URL completa del expediente en SEIA",
                    "Tipo de origen: 'Etapa del Proyecto', 'Documento e-seia' o 'Anexo'",
                    "Nombre del documento",
                    "Extensión del archivo (ej. .pdf, .zip, .xlsx) extraída de nombre_documento o url_documento",
                    "Tipo de enlace: 'directo', 'pagina_intermedia', etc.",
                    "URL del documento (puede ser directa o página intermedia)",
                    "Indica si el documento tiene enlace descargable (Sí/No)",
                    "Ruta relativa donde se descargaría el archivo",
                    "Categoría del documento",
                    "Fecha del documento (formato: DD/MM/YYYY HH:MM:SS)",
                    "Etapa del proyecto (solo para documentos de Etapa)",
                    "Observaciones (solo para documentos de Etapa)",
                    "Número de resolución (solo para documentos de Etapa)",
                    "Número secuencial de la columna N° (solo para documentos e-SEIA)",
                    "Folio del documento (solo para documentos e-SEIA)",
                    "Quién remitió el documento (solo para documentos e-SEIA)",
                    "A quién está destinado el documento (solo para documentos e-SEIA)",
                    "ID del documento extraído de enlaces JavaScript de destinatarios (solo para documentos e-SEIA)",
                    "Documento padre del anexo (solo para anexos)",
                    "Página de origen del documento",
                    "Sí si el registro es un archivo listado dentro de un RAR/ZIP/7Z/KMZ (listado recursivo)",
                    "URL del archivo comprimido contenedor (solo si es_contenido_archivo=Sí)",
                    "Error de descarga o procesamiento (ej. archivo supera límite de tamaño, error de red, etc.)"
                ],
                "Aplica a": [
                    "Todos", "Todos", "Todos", "Todos",
                    "Todos", "Todos", "Todos", "Todos", "Todos",
                    "Todos", "Todos", "Todos",
                    "Etapa del Proyecto", "Etapa del Proyecto", "Etapa del Proyecto",
                    "Documento e-seia", "Documento e-seia", "Documento e-seia", "Documento e-seia", "Documento e-seia",
                    "Anexo", "Todos",
                    "Contenido de archivos comprimidos", "Contenido de archivos comprimidos",
                    "Todos (cuando hay error)"
                ]
            }
            df_columnas = pd.DataFrame(columnas_info)
            df_columnas.to_excel(writer, sheet_name='Descripción Columnas', index=False)
        print(f"   ✅ Excel generado: {output_excel}")
        print(f"   📊 Total de filas: {len(df_consolidado):,}")
        print(f"   📚 Hojas incluidas: Documentos, Guía de Columnas, Descripción Columnas")
    except Exception as e:
        print(f"   ⚠️  Error generando Excel: {e}")
else:
    print(f"\n📊 No hay CSV de documentos para generar Excel.")

print(f"\n📁 Archivos generados:")
print(f"   📄 CSV documentos: {output_csv}")
if output_excel and output_excel.exists():
    print(f"   📊 Excel consolidado: {output_excel}")

if not DOWNLOAD_FILES:
    print(f"\n💡 Para activar descargas, cambia: DOWNLOAD_FILES = True")