#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 DESCARGA CENTRALIZADA — CONTRATACIÓN PÚBLICA DE EUSKADI  v4
═══════════════════════════════════════════════════════════════════════════════
 Arquitectura limpia API-first con fallback a XLSX históricos.

 FUENTE CENTRAL: KontratazioA — Plataforma de Contratación Pública en Euskadi
   → 800+ poderes adjudicadores (GV, Diputaciones, Ayuntamientos, OOAA)
   → Registro de Contratos (REVASCON) + Perfil de Contratante

 MÓDULO A — API REST KontratazioA (JSON paginado, 10 items/pág fijo)
   A1. Contracts        — Muestra 1000 registros (bulk = B1 XLSX)
   A2. Contracting Notices — Muestra 1000 registros (bulk = B1 XLSX)
   A3. Contracting Authorities — 800+ poderes adjudicadores (completo)
   A4. Companies         — Empresas en Registro de Licitadores (completo)

 MÓDULO B — XLSX/CSV Históricos (Open Data Euskadi)
   B1. Contratos Sector Público completo (2011-2026)  → XLSX anual
   B2. REVASCON agregado anual (2013-2018)            → CSV/XLSX
   B3. Contratos últimos 90 días (ventana móvil)      → XLSX

 MÓDULO C — Portales municipales independientes (datos NO centralizados)
   C1. Bilbao — contratos adjudicados (2005-2026)     → CSV
   C2. Vitoria-Gasteiz — contratos menores            → CSV

 Notas:
   · Los módulos B1/B2/B3 son exports del mismo REVASCON → redundantes con A1
     pero se mantienen como backup y para series históricas pre-API.
   · C1/C2 son portales propios que publican datos que PUEDEN no estar en
     KontratazioA (especialmente contratos menores municipales).
═══════════════════════════════════════════════════════════════════════════════
"""

import requests
import time
import json
import logging
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

BASE_DIR = Path("datos_euskadi_contratacion_v4")
DIRS = {
    # Módulo A: API REST
    "api_contracts":     BASE_DIR / "A1_api_contratos",
    "api_notices":       BASE_DIR / "A2_api_anuncios",
    "api_authorities":   BASE_DIR / "A3_api_poderes",
    "api_companies":     BASE_DIR / "A4_api_empresas",
    # Módulo B: XLSX históricos
    "xlsx_anual":        BASE_DIR / "B1_xlsx_sector_publico_anual",
    "revascon_hist":     BASE_DIR / "B2_revascon_historico",
    "ultimos_90d":       BASE_DIR / "B3_ultimos_90_dias",
    # Módulo C: Portales municipales
    "bilbao":            BASE_DIR / "C1_bilbao",
    "vitoria":           BASE_DIR / "C2_vitoria_gasteiz",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (investigacion-academica) contratacion-euskadi/4.0",
    "Accept": "application/json, */*",
}
TIMEOUT  = 120
DELAY    = 1.5   # segundos entre peticiones
RETRIES  = 3

YEAR_NOW = datetime.now().year
YEAR_MIN_GV     = 2011    # Primer año XLSX disponible
YEAR_MIN_BILBAO = 2005    # Bilbao publica desde 2005

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("descarga_euskadi_v4.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

stats = {"ok": 0, "fail": 0, "skip": 0, "bytes": 0}


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def setup_dirs():
    for d in DIRS.values():
        d.mkdir(parents=True, exist_ok=True)


def is_real_data(content: bytes, ext: str) -> bool:
    """Descarta respuestas de error disfrazadas (HTML 404 en vez de datos)."""
    if len(content) < 200:
        return False
    head = content[:500].lower()
    if b"<html" in head and (b"404" in head or b"error" in head):
        return False
    if ext == ".xlsx" and content[:4] != b"PK\x03\x04":
        return False
    if ext == ".xls" and content[:8] != b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return False
    if ext == ".json" and not content.strip()[:1] in (b"{", b"["):
        return False
    return True


def download(url: str, dest: Path, label: str = "",
             skip_retry_on_404: bool = True) -> bool:
    """Descarga un fichero con reintentos. 404 no se reintenta."""
    if dest.exists() and dest.stat().st_size > 100:
        log.info("  SKIP  %s", dest.name)
        stats["skip"] += 1
        return True

    tag = label or dest.name
    for attempt in range(1, RETRIES + 1):
        try:
            log.info("  GET [%d/%d] %s", attempt, RETRIES, tag)
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

            # 404 = definitivo, no reintentar
            if r.status_code == 404 and skip_retry_on_404:
                log.warning("  404  %s — saltando", tag)
                stats["fail"] += 1
                return False

            if r.status_code == 200 and is_real_data(r.content, dest.suffix):
                dest.write_bytes(r.content)
                size = len(r.content)
                stats["ok"] += 1
                stats["bytes"] += size
                log.info("  OK   %s  (%.1f KB)", dest.name, size / 1024)
                return True
            else:
                log.warning("  WARN status=%s size=%d  %s",
                            r.status_code, len(r.content), tag)
        except Exception as e:
            log.warning("  ERR  intento %d: %s", attempt, e)

        time.sleep(DELAY * attempt)

    stats["fail"] += 1
    log.error("  FAIL  %s", tag)
    return False


# ═══════════════════════════════════════════════════════════════
# MÓDULO A — API REST KONTRATAZIOA
# ═══════════════════════════════════════════════════════════════
#
# La API de contrataciones públicas de Euskadi expone 4 endpoints:
#   · Contracts            (C)   → contratos registrados en REVASCON
#   · Contracting Notices  (CN)  → anuncios del Perfil de Contratante
#   · Contracting Authorities (CA) → poderes adjudicadores
#   · Companies            (CO)  → empresas licitadoras
#
# El base URL exacto se autodescubre probando candidatos (la
# documentación oficial no publica un base URL estable).
# ═══════════════════════════════════════════════════════════════

# Candidatos de base URL para la API REST (probados en orden)
API_BASE_CANDIDATES = [
    # Patrón del enlace Swagger UI en la web
    "https://opendata.euskadi.eus/api-procurements",
    # Patrón alternativo (namespace del servlet)
    "https://opendata.euskadi.eus/webopd00-apicontract",
    # Patrón api.euskadi.eus (usado por otras APIs como meteo)
    "https://api.euskadi.eus/procurements",
    # Patrón con /api/ explícito
    "https://opendata.euskadi.eus/api/procurements",
]

# Sufijos de endpoint por tipo de recurso
API_ENDPOINTS = {
    "contracts":    ["/contracts", "/api/contracts",
                     "?api=procurements&_type=contracts",
                     "/es?api=procurements"],
    "notices":      ["/contracting-notices", "/api/contracting-notices",
                     "?api=procurements&_type=contracting-notices",
                     "/notices"],
    "authorities":  ["/contracting-authorities", "/api/contracting-authorities",
                     "?api=procurements&_type=contracting-authorities",
                     "/authorities"],
    "companies":    ["/companies", "/api/companies",
                     "?api=procurements&_type=companies"],
}


def _probe_api() -> dict:
    """
    Autodescubrimiento de endpoints de la API.
    Prueba combinaciones de base_url + endpoint hasta encontrar
    las que devuelven JSON válido.

    Devuelve dict con las URLs funcionales, ej:
        {"contracts": "https://...?currentPage=1",
         "notices": "https://...", ...}
    """
    log.info("  Probando endpoints de la API REST...")
    working = {}

    for resource, suffixes in API_ENDPOINTS.items():
        if resource in working:
            continue
        for base in API_BASE_CANDIDATES:
            if resource in working:
                break
            for suffix in suffixes:
                # Construir URL de prueba — probar currentPage (KontratazioA)
                # y _page (genérico) como param de paginación
                sep = "&" if "?" in suffix else "?"
                test_url = f"{base}{suffix}{sep}currentPage=1"
                try:
                    r = requests.get(test_url, headers=HEADERS, timeout=30)
                    if r.status_code == 200:
                        ct = r.headers.get("Content-Type", "")
                        # Aceptar si es JSON
                        if "json" in ct or "javascript" in ct:
                            data = r.json()
                            if isinstance(data, (dict, list)):
                                # Extraer el base_url funcional (sin paginación)
                                api_url = f"{base}{suffix}"
                                working[resource] = api_url
                                log.info("    ✓ %s → %s", resource, api_url)
                                break
                        # Aceptar si parece JSON aunque CT sea text
                        elif r.text.strip().startswith(("{", "[")):
                            data = r.json()
                            if isinstance(data, (dict, list)):
                                api_url = f"{base}{suffix}"
                                working[resource] = api_url
                                log.info("    ✓ %s → %s", resource, api_url)
                                break
                except Exception:
                    pass

    return working


def _paginate_api(api_url: str, resource_name: str, dest_dir: Path,
                  prefix: str, max_pages: int = 5000, delay: float = DELAY):
    """
    Descarga paginada de la API REST de KontratazioA.

    La API tiene página fija de 10 items (ignora _pageSize).
    Paginación: ?currentPage=N (1-based).
    Estructura respuesta: {totalItems, totalPages, currentPage,
                           itemsOfPage, items: [...]}
    """
    sep = "&" if "?" in api_url else "?"

    # ── Página 1: descubrir totalItems y totalPages ─────────
    first_url = f"{api_url}{sep}currentPage=1"
    try:
        r = requests.get(first_url, headers=HEADERS, timeout=TIMEOUT)
        data = r.json()
    except Exception as e:
        log.error("  ERR %s: no se pudo leer página 1: %s", resource_name, e)
        stats["fail"] += 1
        return

    total_items = data.get("totalItems", 0)
    total_pages = data.get("totalPages", 1)
    page_size = data.get("itemsOfPage", 10)

    log.info("  %s: %d items en %d páginas (fijo %d items/pág)",
             resource_name, total_items, total_pages, page_size)

    # Limitar páginas máximas
    pages_to_download = min(total_pages, max_pages)
    if total_pages > max_pages:
        log.warning("  ⚠ Limitado a %d/%d páginas (%.0f%% de %d items)",
                    max_pages, total_pages,
                    100 * max_pages / total_pages, total_items)

    # ── Guardar página 1 ────────────────────────────────────
    dest = dest_dir / f"{prefix}_p{1:05d}.json"
    if not (dest.exists() and dest.stat().st_size > 100):
        dest.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        stats["ok"] += 1
        stats["bytes"] += dest.stat().st_size
    else:
        stats["skip"] += 1

    # ── Páginas 2..N ────────────────────────────────────────
    errors_consec = 0
    for page in range(2, pages_to_download + 1):
        dest = dest_dir / f"{prefix}_p{page:05d}.json"

        if dest.exists() and dest.stat().st_size > 100:
            stats["skip"] += 1
            continue

        url = f"{api_url}{sep}currentPage={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200:
                log.warning("  %s: status %d en page %d", resource_name, r.status_code, page)
                errors_consec += 1
                if errors_consec >= 5:
                    log.error("  %s: 5 errores consecutivos — abortando.", resource_name)
                    break
                time.sleep(delay * 2)
                continue

            page_data = r.json()
            items = page_data.get("items", [])
            if not items:
                log.info("  %s: página %d vacía — fin.", resource_name, page)
                break

            dest.write_text(
                json.dumps(page_data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            size = dest.stat().st_size
            stats["ok"] += 1
            stats["bytes"] += size
            errors_consec = 0

            # Progreso cada 50 páginas o en la última
            if page % 50 == 0 or page == pages_to_download:
                pct = 100 * page / pages_to_download
                log.info("  %s: p%d/%d (%.0f%%) — %d items descargados",
                         resource_name, page, pages_to_download, pct,
                         page * page_size)

        except Exception as e:
            log.warning("  ERR %s page %d: %s", resource_name, page, e)
            stats["fail"] += 1
            errors_consec += 1
            if errors_consec >= 5:
                log.error("  %s: 5 errores consecutivos — abortando.", resource_name)
                break

        time.sleep(delay)



def dl_A_api(api_urls: dict):
    """
    Descarga los 4 endpoints de la API REST de KontratazioA.

    Estrategia según tamaño:
      · authorities (~800 items, ~80 pág)   → descarga completa
      · companies   (~miles, ~cientos pág)  → descarga completa
      · contracts   (655K+ items, 65K+ pág) → MUESTRA (XLSX = fuente bulk)
      · notices     (grande)                → MUESTRA (XLSX = fuente bulk)

    La API tiene página fija de 10 items (no configurable, usa currentPage=N).
    Los XLSX de B1 contienen los mismos datos de contracts en formato
    tabular, descargables en 2 minutos vs ~27h por API.
    """

    # ── Endpoints pequeños: descarga completa ───────────────
    small_endpoints = [
        ("authorities",  "A3_Poderes",   "api_authorities", "poderes"),
        ("companies",    "A4_Empresas",  "api_companies",   "empresas"),
    ]
    for resource, name, dir_key, prefix in small_endpoints:
        log.info("=" * 60)
        log.info("A. API REST — %s (descarga completa)", name)
        log.info("=" * 60)
        if resource not in api_urls:
            log.warning("  ⚠ Endpoint %s no descubierto — saltando.", resource)
            continue
        _paginate_api(
            api_url=api_urls[resource],
            resource_name=name,
            dest_dir=DIRS[dir_key],
            prefix=prefix,
            max_pages=5000,   # sin límite práctico para datasets pequeños
            delay=0.5,        # más rápido para pocos registros
        )

    # ── Endpoints grandes: muestra (bulk data = XLSX B1) ────
    # Contratos: 655K+ items × 10/pág = 65K+ peticiones (~27h)
    # La misma data está en B1_xlsx como XLSX descargable en 2 min
    API_SAMPLE_PAGES = 100  # 100 págs × 10 items = 1000 registros de muestra

    large_endpoints = [
        ("contracts",  "A1_Contratos",  "api_contracts", "contratos"),
        ("notices",    "A2_Anuncios",   "api_notices",   "anuncios"),
    ]
    for resource, name, dir_key, prefix in large_endpoints:
        log.info("=" * 60)
        log.info("A. API REST — %s (muestra %d págs)", name, API_SAMPLE_PAGES)
        log.info("=" * 60)
        if resource not in api_urls:
            log.warning("  ⚠ Endpoint %s no descubierto — saltando.", resource)
            continue
        log.info("  ℹ La API tiene página fija de 10 items (no configurable).")
        log.info("    Descarga bulk inviable (~27h). Usando XLSX (B1) como")
        log.info("    fuente principal. API = muestra de %d registros.", API_SAMPLE_PAGES * 10)
        _paginate_api(
            api_url=api_urls[resource],
            resource_name=name,
            dest_dir=DIRS[dir_key],
            prefix=prefix,
            max_pages=API_SAMPLE_PAGES,
            delay=0.3,        # delay corto para la muestra
        )


# ═══════════════════════════════════════════════════════════════
# MÓDULO B — XLSX/CSV HISTÓRICOS (OPEN DATA EUSKADI)
# ═══════════════════════════════════════════════════════════════
#
# Exports periódicos de los mismos datos de REVASCON/KontratazioA
# en formato XLSX. Útiles como:
#   · Backup del módulo A
#   · Series históricas pre-API (antes de 2020/2024)
#   · Formato tabular listo para análisis (vs JSON)
# ═══════════════════════════════════════════════════════════════

def dl_B1_xlsx_anual():
    """
    B1. Contratos Administrativos del Sector Público Vasco (XLSX anuales)
    Fuente: Open Data Euskadi → export anual de KontratazioA.
    Incluye contratos de GV + OOAA + poderes adheridos.
    Desde 2019 incluye contratos menores → ficheros de 15-27 MB.
    """
    log.info("=" * 60)
    log.info("B1. XLSX — CONTRATOS SECTOR PÚBLICO (anual, 2011-%d)", YEAR_NOW)
    log.info("=" * 60)
    d = DIRS["xlsx_anual"]
    base = "https://opendata.euskadi.eus/contenidos/ds_contrataciones"

    for year in range(YEAR_MIN_GV, YEAR_NOW + 1):
        url = f"{base}/contrataciones_admin_{year}/opendata/contratos.xlsx"
        dest = d / f"contratos_{year}.xlsx"
        download(url, dest, f"XLSX-{year}")
        time.sleep(DELAY)

    # ── JSON fallback: 2011-2013 XLSX están vacíos (solo cabeceras)
    #    pero los JSON de Open Data SÍ contienen los datos completos.
    log.info("  B1-fix: descargando JSON 2011-2013 (XLSX vacíos)…")
    for year in (2011, 2012, 2013):
        dest_json = d / f"contratos_{year}.json"
        if dest_json.exists() and dest_json.stat().st_size > 500:
            log.info("  SKIP  %s (%.0f KB)", dest_json.name,
                     dest_json.stat().st_size / 1024)
            stats["skip"] += 1
            continue
        url_json = f"{base}/contrataciones_admin_{year}/opendata/contratos.json"
        download(url_json, dest_json, f"JSON-{year}")
        time.sleep(DELAY)


def dl_B2_revascon_historico():
    """
    B2. REVASCON — Registro de Contratos Sector Público (agregado anual)
    Formato más rico que B1 para el período 2013-2018.
    Desde 2019 el modelo cambió a publicación por poder adjudicador,
    cubierto por la API (módulo A).
    """
    log.info("=" * 60)
    log.info("B2. REVASCON HISTÓRICO (agregado anual, 2013-2018)")
    log.info("=" * 60)
    d = DIRS["revascon_hist"]
    base = "https://opendata.euskadi.eus/contenidos/ds_contrataciones"

    # URLs conocidas (CSV donde exista, XLSX como fallback)
    sources = {
        2013: {
            "csv": f"{base}/registro_contratos_2013/es_contracc/adjuntos/revascon-2013.csv",
        },
        2014: {
            "csv": f"{base}/registro_contratos_2014/es_contracc/adjuntos/revascon-2014.csv",
            "xlsx": (f"{base}/contratos_euskadi_2014/es_contracc/adjuntos/"
                     "Registro_de_contratos_del_Sector_Publico_de_Euskadi_del_2014.xlsx"),
        },
    }
    # 2015-2018: solo XLSX disponible
    for y in range(2015, 2019):
        sources[y] = {
            "xlsx": (f"{base}/contratos_euskadi_{y}/es_contracc/adjuntos/"
                     f"Registro_de_contratos_del_Sector_Publico_de_Euskadi_del_{y}.xlsx"),
        }

    for year, urls in sorted(sources.items()):
        # Intentar CSV primero
        if "csv" in urls:
            dest_csv = d / f"revascon_{year}.csv"
            if download(urls["csv"], dest_csv, f"REVASCON-{year}-CSV"):
                time.sleep(DELAY)
                continue

        # Fallback a XLSX
        if "xlsx" in urls:
            dest_xlsx = d / f"revascon_{year}.xlsx"
            download(urls["xlsx"], dest_xlsx, f"REVASCON-{year}-XLSX")

        time.sleep(DELAY)


def dl_B3_ultimos_90d():
    """
    B3. Snapshot de contratos de los últimos 90 días.
    Ventana móvil con datos recientes de toda la CAE.
    """
    log.info("=" * 60)
    log.info("B3. CONTRATOS ÚLTIMOS 90 DÍAS (snapshot)")
    log.info("=" * 60)
    d = DIRS["ultimos_90d"]
    base = ("https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
            "contrataciones_ultimos_dias/opendata/contratos")
    hoy = datetime.now().strftime("%Y%m%d")
    download(f"{base}.xlsx", d / f"ultimos_90d_{hoy}.xlsx", "90-días")


# ═══════════════════════════════════════════════════════════════
# MÓDULO C — PORTALES MUNICIPALES INDEPENDIENTES
# ═══════════════════════════════════════════════════════════════
#
# Bilbao y Vitoria tienen portales open data propios con contratos
# que PUEDEN incluir datos no centralizados en KontratazioA,
# especialmente contratos menores municipales.
# ═══════════════════════════════════════════════════════════════

def dl_C1_bilbao():
    """
    C1. Bilbao — Contratos adjudicados (2005-presente).
    Portal propio: bilbao.eus/opendata. CSV con todos los tipos.
    """
    log.info("=" * 60)
    log.info("C1. BILBAO — CONTRATOS ADJUDICADOS (CSV, 2005-%d)", YEAR_NOW)
    log.info("=" * 60)
    d = DIRS["bilbao"]
    base = "https://www.bilbao.eus/opendata/datos/licitaciones"

    # Descarga por año (serie completa)
    for year in range(YEAR_MIN_BILBAO, YEAR_NOW + 1):
        url = f"{base}?formato=csv&anio={year}&idioma=es"
        dest = d / f"bilbao_{year}.csv"
        download(url, dest, f"Bilbao-{year}")
        time.sleep(DELAY)

    # Descarga por tipo de contrato (histórico completo)
    for tipo in ("obras", "servicios", "suministros"):
        url = f"{base}?formato=csv&tipoContrato={tipo}&idioma=es"
        dest = d / f"bilbao_tipo_{tipo}.csv"
        download(url, dest, f"Bilbao-tipo-{tipo}")
        time.sleep(DELAY)

    # Licitaciones abiertas (snapshot)
    hoy = datetime.now().strftime("%Y%m%d")
    url = f"{base}?formato=csv&abiertas=true&idioma=es"
    dest = d / f"bilbao_abiertas_{hoy}.csv"
    download(url, dest, "Bilbao-abiertas")


def dl_C2_vitoria():
    """
    C2. Vitoria-Gasteiz — Contratos menores formalizados.
    Fuente específica para menores del Ayuntamiento.
    """
    log.info("=" * 60)
    log.info("C2. VITORIA-GASTEIZ — CONTRATOS MENORES (CSV)")
    log.info("=" * 60)
    d = DIRS["vitoria"]
    base = ("https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
            "contratos_menores_formalizados/opendata/contratos_menores")

    download(f"{base}.csv", d / "vitoria_menores.csv", "Vitoria-menores-CSV")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    log.info("╔═══════════════════════════════════════════════════════════╗")
    log.info("║  CONTRATACIÓN PÚBLICA DE EUSKADI — DESCARGA CENTRAL v4  ║")
    log.info("║  API-first · XLSX fallback · Portales municipales       ║")
    log.info("║  Fecha: %s                                  ║",
             datetime.now().strftime("%Y-%m-%d"))
    log.info("╚═══════════════════════════════════════════════════════════╝")

    setup_dirs()

    # ── FASE 0: Autodescubrimiento de la API ────────────────────
    log.info("=" * 60)
    log.info("FASE 0: DESCUBRIMIENTO API REST KONTRATAZIOA")
    log.info("=" * 60)
    api_urls = _probe_api()

    if api_urls:
        log.info("  Endpoints descubiertos: %d/4", len(api_urls))
        for k, v in api_urls.items():
            log.info("    · %s → %s", k, v)
    else:
        log.warning("  ⚠ Ningún endpoint API descubierto.")
        log.warning("    Se usarán exclusivamente los XLSX históricos.")

    # ── MÓDULO A: API REST (fuente principal) ───────────────────
    if api_urls:
        dl_A_api(api_urls)

    # ── MÓDULO B: XLSX históricos (backup + pre-API) ────────────
    dl_B1_xlsx_anual()
    dl_B2_revascon_historico()
    dl_B3_ultimos_90d()

    # ── MÓDULO C: Portales municipales ──────────────────────────
    dl_C1_bilbao()
    dl_C2_vitoria()

    # ── RESUMEN ─────────────────────────────────────────────────
    elapsed = time.time() - t0
    log.info("═" * 60)
    log.info("RESUMEN v4")
    log.info("─" * 60)
    log.info("  Descargados:  %d ficheros", stats["ok"])
    log.info("  Existentes:   %d (skip)", stats["skip"])
    log.info("  Fallidos:     %d", stats["fail"])
    log.info("  Volumen:      %.1f MB", stats["bytes"] / 1024 / 1024)
    log.info("  Tiempo:       %.0f s", elapsed)
    if api_urls:
        log.info("  API endpoints: %s", ", ".join(api_urls.keys()))
    else:
        log.info("  API endpoints: ninguno (solo XLSX/CSV)")
    log.info("═" * 60)

    log.info("\nEstructura:")
    log.info("  A1_api_contratos/          ← JSON muestra 1K registros (bulk = B1)")
    log.info("  A2_api_anuncios/           ← JSON muestra 1K anuncios (bulk = B1)")
    log.info("  A3_api_poderes/            ← JSON completo — 800+ poderes adjudicadores")
    log.info("  A4_api_empresas/           ← JSON completo — empresas licitadoras")
    log.info("  B1_xlsx_sector_publico/    ← XLSX anuales (2011-%d) — FUENTE PRINCIPAL", YEAR_NOW)
    log.info("  B2_revascon_historico/     ← CSV/XLSX 2013-2018 — serie histórica")
    log.info("  B3_ultimos_90_dias/        ← XLSX snapshot reciente")
    log.info("  C1_bilbao/                 ← CSV contratos municipales (2005-%d)", YEAR_NOW)
    log.info("  C2_vitoria_gasteiz/        ← CSV contratos menores municipales")


if __name__ == "__main__":
    main()



#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 CONSOLIDACIÓN — CONTRATACIÓN PÚBLICA DE EUSKADI  v4
═══════════════════════════════════════════════════════════════════════════════
 Convierte la salida del scraper v4 (JSON + XLSX + CSV) en Parquets limpios.

 ENTRADA:  datos_euskadi_contratacion_v4/
 SALIDA:   euskadi_parquet/

 Estrategia:
   · B1 XLSX anuales (2011-2026)  → contratos_master.parquet   ← FUENTE PRINCIPAL
   · A3 JSON poderes (completo)   → poderes_adjudicadores.parquet
   · A4 JSON empresas (completo)  → empresas_licitadoras.parquet
   · B2 REVASCON (2013-2018)      → revascon_historico.parquet  (pre-API)
   · C1 Bilbao CSVs               → bilbao_contratos.parquet
   · A1/A2 muestras API           → IGNORAR (redundante con B1)
   · B3 últimos 90d / C2 Vitoria  → IGNORAR si 404

 Salida final:
   euskadi_parquet/
   ├── contratos_master.parquet        ← 655K+ contratos (B1)
   ├── poderes_adjudicadores.parquet   ← 919 poderes (A3)
   ├── empresas_licitadoras.parquet    ← 9042 empresas (A4)
   ├── revascon_historico.parquet      ← Serie 2013-2018 (B2)
   ├── bilbao_contratos.parquet        ← Contratos municipales (C1)
   ├── stats.json                      ← Estadísticas consolidación
   └── README.md                       ← Documentación
═══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import sys
import warnings
from pathlib import Path
from datetime import datetime

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

INPUT_DIR  = Path("datos_euskadi_contratacion_v4")
OUTPUT_DIR = Path("euskadi_parquet")

# Subdirectorios de entrada (del scraper v4)
PATHS = {
    "api_contracts":   INPUT_DIR / "A1_api_contratos",
    "api_notices":     INPUT_DIR / "A2_api_anuncios",
    "api_authorities": INPUT_DIR / "A3_api_poderes",
    "api_companies":   INPUT_DIR / "A4_api_empresas",
    "xlsx_anual":      INPUT_DIR / "B1_xlsx_sector_publico_anual",
    "revascon_hist":   INPUT_DIR / "B2_revascon_historico",
    "ultimos_90d":     INPUT_DIR / "B3_ultimos_90_dias",
    "bilbao":          INPUT_DIR / "C1_bilbao",
    "vitoria":         INPUT_DIR / "C2_vitoria_gasteiz",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("consolidar_euskadi_v4.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

stats = {}


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def safe_str_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas object a string para evitar tipos mixtos en Parquet."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).replace({"nan": None, "None": None, "": None})
    return df


def load_json_pages(directory: Path) -> pd.DataFrame:
    """
    Carga todos los JSON paginados de la API y extrae los items.
    Estructura esperada: {totalItems, totalPages, items: [...]}
    """
    all_items = []
    json_files = sorted(directory.glob("*.json"))

    if not json_files:
        log.warning("  Sin ficheros JSON en %s", directory)
        return pd.DataFrame()

    for f in json_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            items = data.get("items", [])
            if isinstance(items, list):
                all_items.extend(items)
        except Exception as e:
            log.warning("  Error leyendo %s: %s", f.name, e)

    if not all_items:
        return pd.DataFrame()

    df = pd.json_normalize(all_items, sep="_")
    log.info("  %d registros de %d páginas JSON", len(df), len(json_files))
    return df


def load_xlsx_files(directory: Path, pattern: str = "*.xlsx") -> pd.DataFrame:
    """Carga y concatena todos los XLSX de un directorio."""
    frames = []
    xlsx_files = sorted(directory.glob(pattern))

    if not xlsx_files:
        log.warning("  Sin ficheros XLSX en %s", directory)
        return pd.DataFrame()

    for f in xlsx_files:
        try:
            # Intentar leer con openpyxl (xlsx)
            df = pd.read_excel(f, engine="openpyxl")
            if len(df) > 0:
                # Añadir columna de origen (año del fichero)
                year_str = f.stem.split("_")[-1]
                try:
                    df["_archivo_origen"] = f.name
                    df["_year"] = int(year_str)
                except ValueError:
                    df["_archivo_origen"] = f.name

                frames.append(df)
                log.info("  %s: %d filas × %d cols", f.name, len(df), len(df.columns))
            else:
                log.info("  %s: vacío — saltando", f.name)
        except Exception as e:
            # Fallback: intentar con xlrd (xls)
            try:
                df = pd.read_excel(f, engine="xlrd")
                if len(df) > 0:
                    df["_archivo_origen"] = f.name
                    frames.append(df)
                    log.info("  %s: %d filas × %d cols (xlrd)", f.name, len(df), len(df.columns))
            except Exception as e2:
                log.warning("  Error leyendo %s: %s / %s", f.name, e, e2)

    if not frames:
        return pd.DataFrame()

    # Concatenar con unión de columnas (pueden variar entre años)
    df = pd.concat(frames, ignore_index=True, sort=False)
    log.info("  TOTAL: %d filas × %d cols", len(df), len(df.columns))
    return df


def load_csv_files(directory: Path, pattern: str = "*.csv",
                   encoding: str = "utf-8") -> pd.DataFrame:
    """Carga y concatena todos los CSV de un directorio."""
    frames = []
    csv_files = sorted(directory.glob(pattern))

    if not csv_files:
        log.warning("  Sin ficheros CSV en %s", directory)
        return pd.DataFrame()

    for f in csv_files:
        if f.stat().st_size < 100:
            log.info("  %s: demasiado pequeño — saltando", f.name)
            continue
        try:
            # Detectar separador
            head = f.read_bytes()[:2000].decode(encoding, errors="replace")
            sep = ";" if head.count(";") > head.count(",") else ","

            df = pd.read_csv(f, sep=sep, encoding=encoding, low_memory=False,
                             on_bad_lines="skip")
            if len(df) > 0:
                df["_archivo_origen"] = f.name
                frames.append(df)
                log.info("  %s: %d filas × %d cols (sep='%s')",
                         f.name, len(df), len(df.columns), sep)
        except UnicodeDecodeError:
            # Reintentar con latin-1
            try:
                df = pd.read_csv(f, sep=sep, encoding="latin-1", low_memory=False,
                                 on_bad_lines="skip")
                if len(df) > 0:
                    df["_archivo_origen"] = f.name
                    frames.append(df)
                    log.info("  %s: %d filas × %d cols (latin-1)",
                             f.name, len(df), len(df.columns))
            except Exception as e2:
                log.warning("  Error leyendo %s: %s", f.name, e2)
        except Exception as e:
            log.warning("  Error leyendo %s: %s", f.name, e)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True, sort=False)
    log.info("  TOTAL: %d filas × %d cols", len(df), len(df.columns))
    return df


def save_parquet(df: pd.DataFrame, dest: Path, label: str) -> dict:
    """Guarda DataFrame como Parquet y devuelve estadísticas."""
    if df.empty:
        log.warning("  %s: DataFrame vacío — no se genera Parquet", label)
        return {"registros": 0, "columnas": 0, "tamaño_mb": 0}

    df = safe_str_columns(df)

    # Eliminar columnas completamente vacías
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df = df.drop(columns=empty_cols)
        log.info("  Eliminadas %d columnas vacías", len(empty_cols))

    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False, engine="pyarrow")

    size_mb = dest.stat().st_size / (1024 * 1024)
    log.info("  ✓ %s: %d filas × %d cols → %.1f MB",
             label, len(df), len(df.columns), size_mb)

    return {
        "registros": len(df),
        "columnas": len(df.columns),
        "tamaño_mb": round(size_mb, 2),
        "lista_columnas": df.columns.tolist(),
    }


# ═══════════════════════════════════════════════════════════════
# CONSOLIDADORES POR MÓDULO
# ═══════════════════════════════════════════════════════════════

def consolidar_B1_contratos_master() -> dict:
    """
    B1 → contratos_master.parquet
    FUENTE PRINCIPAL: XLSX anuales de contratos del sector público.
    655K+ contratos, 2011-2026.

    Los XLSX de 2011-2013 están vacíos (solo cabeceras). Los datos de esos
    años están en JSON (Open Data Euskadi). Este consolidador carga ambos.
    """
    log.info("=" * 60)
    log.info("B1. CONTRATOS MASTER (XLSX + JSON anuales → Parquet)")
    log.info("=" * 60)

    src = PATHS["xlsx_anual"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    frames = []

    # ── XLSX (2014-2026, los que tienen datos) ───────────────
    df_xlsx = load_xlsx_files(src, "contratos_*.xlsx")
    if not df_xlsx.empty:
        frames.append(df_xlsx)

    # ── JSON fallback (2011-2013, XLSX vacíos) ───────────────
    json_files = sorted(src.glob("contratos_*.json"))
    for f in json_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))

            # El JSON de Open Data puede tener varias estructuras:
            # 1. Lista directa de contratos: [{"campo": "valor"}, ...]
            # 2. Objeto con key "items" o "contracts": {"items": [...]}
            # 3. Estructura anidada del CMS de Euskadi

            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Buscar la lista de items en las keys del dict
                for key in ("items", "contracts", "contratos", "data",
                            "opendata", "anuncios"):
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break
                # Si no encuentra una lista, puede ser un dict de dicts
                if items is None:
                    # Estructura tipo {id1: {campos...}, id2: {campos...}}
                    first_val = next(iter(data.values()), None)
                    if isinstance(first_val, dict):
                        items = list(data.values())

            if items and len(items) > 0:
                df_json = pd.json_normalize(items, sep="_")
                year_str = f.stem.split("_")[-1]
                df_json["_archivo_origen"] = f.name
                try:
                    df_json["_year"] = int(year_str)
                except ValueError:
                    pass
                frames.append(df_json)
                log.info("  %s: %d filas × %d cols (JSON)",
                         f.name, len(df_json), len(df_json.columns))
            else:
                log.warning("  %s: no se encontraron items en el JSON", f.name)

        except Exception as e:
            log.warning("  Error leyendo %s: %s", f.name, e)

    if not frames:
        return {"registros": 0, "error": "sin datos"}

    df = pd.concat(frames, ignore_index=True, sort=False)
    log.info("  TOTAL combinado: %d filas × %d cols", len(df), len(df.columns))

    # ── Limpieza básica ──────────────────────────────────────
    # Normalizar nombres de columnas (minúsculas, sin espacios extra)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Eliminar filas completamente vacías
    df = df.dropna(how="all")

    # Eliminar duplicados exactos si los hay
    n_antes = len(df)
    df = df.drop_duplicates()
    n_dupes = n_antes - len(df)
    if n_dupes:
        log.info("  Eliminados %d duplicados exactos", n_dupes)

    # ── Tipado de columnas comunes ───────────────────────────
    # Intentar convertir columnas de importe a numérico
    for col in df.columns:
        if any(kw in col for kw in ("importe", "valor", "precio", "presupuesto",
                                     "iva", "canon", "monto")):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Intentar parsear fechas
    for col in df.columns:
        if any(kw in col for kw in ("fecha", "date", "data")):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            except Exception:
                pass

    # Añadir metadatos de fuente
    df["_fuente"] = "B1_xlsx_sector_publico"

    dest = OUTPUT_DIR / "contratos_master.parquet"
    info = save_parquet(df, dest, "contratos_master")
    info["duplicados_eliminados"] = n_dupes
    info["rango_años"] = f"2011-{datetime.now().year}"
    return info


def consolidar_A3_poderes() -> dict:
    """
    A3 → poderes_adjudicadores.parquet
    919 poderes adjudicadores del registro público.
    """
    log.info("=" * 60)
    log.info("A3. PODERES ADJUDICADORES (JSON API → Parquet)")
    log.info("=" * 60)

    src = PATHS["api_authorities"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    df = load_json_pages(src)
    if df.empty:
        return {"registros": 0, "error": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Convertir columnas con listas/dicts a string (no son hashables)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False)
                                    if isinstance(x, (list, dict)) else x)

    # El campo 'id' de la API es el índice dentro de la página (1-10),
    # NO un identificador único. Usamos dedup por contenido completo.
    content_cols = [c for c in df.columns
                    if c not in ("id", "_fuente", "_archivo_origen")
                    and not c.startswith("_")]
    n_antes = len(df)
    df = df.drop_duplicates(subset=content_cols if content_cols else None)
    if len(df) < n_antes:
        log.info("  Deduplicados %d → %d (contenido completo)", n_antes, len(df))

    df["_fuente"] = "A3_api_poderes"
    dest = OUTPUT_DIR / "poderes_adjudicadores.parquet"
    return save_parquet(df, dest, "poderes_adjudicadores")


def consolidar_A4_empresas() -> dict:
    """
    A4 → empresas_licitadoras.parquet
    9042 empresas del Registro de Licitadores.
    """
    log.info("=" * 60)
    log.info("A4. EMPRESAS LICITADORAS (JSON API → Parquet)")
    log.info("=" * 60)

    src = PATHS["api_companies"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    df = load_json_pages(src)
    if df.empty:
        return {"registros": 0, "error": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Convertir columnas con listas/dicts a string (no son hashables)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False)
                                    if isinstance(x, (list, dict)) else x)

    # Deduplicamos por contenido completo para no perder registros.
    content_cols = [c for c in df.columns
                    if c not in ("id", "_fuente", "_archivo_origen")
                    and not c.startswith("_")]
    n_antes = len(df)
    df = df.drop_duplicates(subset=content_cols if content_cols else None)
    if len(df) < n_antes:
        log.info("  Deduplicados %d → %d (contenido completo)", n_antes, len(df))

    df["_fuente"] = "A4_api_empresas"
    dest = OUTPUT_DIR / "empresas_licitadoras.parquet"
    return save_parquet(df, dest, "empresas_licitadoras")


def consolidar_B2_revascon() -> dict:
    """
    B2 → revascon_historico.parquet
    Registro de contratos 2013-2018 (serie pre-API).
    """
    log.info("=" * 60)
    log.info("B2. REVASCON HISTÓRICO (CSV/XLSX → Parquet)")
    log.info("=" * 60)

    src = PATHS["revascon_hist"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    frames = []

    # Cargar CSVs
    df_csv = load_csv_files(src, "revascon_*.csv")
    if not df_csv.empty:
        frames.append(df_csv)

    # Cargar XLSXs
    df_xlsx = load_xlsx_files(src, "revascon_*.xlsx")
    if not df_xlsx.empty:
        frames.append(df_xlsx)

    if not frames:
        return {"registros": 0, "error": "sin datos"}

    df = pd.concat(frames, ignore_index=True, sort=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Eliminar duplicados
    n_antes = len(df)
    df = df.drop_duplicates()
    n_dupes = n_antes - len(df)
    if n_dupes:
        log.info("  Eliminados %d duplicados", n_dupes)

    df["_fuente"] = "B2_revascon_historico"
    dest = OUTPUT_DIR / "revascon_historico.parquet"
    info = save_parquet(df, dest, "revascon_historico")
    info["duplicados_eliminados"] = n_dupes
    return info


def consolidar_C1_bilbao() -> dict:
    """
    C1 → bilbao_contratos.parquet
    Contratos municipales de Bilbao (2005-presente).
    """
    log.info("=" * 60)
    log.info("C1. BILBAO CONTRATOS MUNICIPALES (CSV → Parquet)")
    log.info("=" * 60)

    src = PATHS["bilbao"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    df = load_csv_files(src, "bilbao_*.csv")
    if df.empty:
        return {"registros": 0, "error": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Bilbao descarga por año Y por tipo → posibles duplicados
    n_antes = len(df)
    # Excluir columna de origen para comparar
    compare_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.drop_duplicates(subset=compare_cols)
    n_dupes = n_antes - len(df)
    if n_dupes:
        log.info("  Eliminados %d duplicados (solapamiento año/tipo)", n_dupes)

    # Tipado
    for col in df.columns:
        if any(kw in col for kw in ("importe", "valor", "precio", "presupuesto")):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        if any(kw in col for kw in ("fecha", "date")):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            except Exception:
                pass

    df["_fuente"] = "C1_bilbao"
    dest = OUTPUT_DIR / "bilbao_contratos.parquet"
    info = save_parquet(df, dest, "bilbao_contratos")
    info["duplicados_eliminados"] = n_dupes
    return info


def consolidar_B3_ultimos_90d() -> dict:
    """
    B3 → últimos 90 días (si existe, puede dar 404).
    """
    log.info("=" * 60)
    log.info("B3. ÚLTIMOS 90 DÍAS (si disponible)")
    log.info("=" * 60)

    src = PATHS["ultimos_90d"]
    if not src.exists():
        log.info("  No disponible (404 en descarga)")
        return {"registros": 0, "nota": "no disponible (404)"}

    df = load_xlsx_files(src, "ultimos_*.xlsx")
    if df.empty:
        log.info("  Sin datos")
        return {"registros": 0, "nota": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["_fuente"] = "B3_ultimos_90d"

    dest = OUTPUT_DIR / "ultimos_90d.parquet"
    return save_parquet(df, dest, "ultimos_90d")


# ═══════════════════════════════════════════════════════════════
# GENERACIÓN DE DOCUMENTACIÓN
# ═══════════════════════════════════════════════════════════════

def generar_readme(all_stats: dict):
    """Genera README.md con la documentación del dataset."""
    total_regs = sum(v.get("registros", 0) for v in all_stats.values())
    total_mb = sum(v.get("tamaño_mb", 0) for v in all_stats.values())

    readme = f"""# Contratación Pública de Euskadi — Dataset Consolidado

## Resumen

| Métrica | Valor |
|---------|-------|
| **Fecha consolidación** | {datetime.now().strftime('%Y-%m-%d %H:%M')} |
| **Total registros** | {total_regs:,} |
| **Tamaño Parquet** | {total_mb:.1f} MB |
| **Archivos generados** | {len([v for v in all_stats.values() if v.get('registros', 0) > 0])} |

## Archivos

| Archivo | Registros | Tamaño | Fuente | Descripción |
|---------|-----------|--------|--------|-------------|
"""

    file_docs = {
        "contratos_master": {
            "fuente": "B1 (XLSX Open Data)",
            "desc": "Contratos del Sector Público Vasco 2011-2026 (FUENTE PRINCIPAL)",
        },
        "poderes_adjudicadores": {
            "fuente": "A3 (API KontratazioA)",
            "desc": "800+ poderes adjudicadores (GV, Diputaciones, Aytos, OOAA)",
        },
        "empresas_licitadoras": {
            "fuente": "A4 (API KontratazioA)",
            "desc": "Empresas del Registro de Licitadores de Euskadi",
        },
        "revascon_historico": {
            "fuente": "B2 (Open Data)",
            "desc": "REVASCON agregado 2013-2018 (serie pre-API)",
        },
        "bilbao_contratos": {
            "fuente": "C1 (Portal Bilbao)",
            "desc": "Contratos municipales Bilbao 2005-2026",
        },
        "ultimos_90d": {
            "fuente": "B3 (Open Data)",
            "desc": "Snapshot contratos últimos 90 días (ventana móvil)",
        },
    }

    for key, info in all_stats.items():
        regs = info.get("registros", 0)
        if regs == 0:
            continue
        mb = info.get("tamaño_mb", 0)
        doc = file_docs.get(key, {"fuente": "?", "desc": "?"})
        readme += f"| `{key}.parquet` | {regs:,} | {mb:.1f} MB | {doc['fuente']} | {doc['desc']} |\n"

    readme += f"""
## Notas sobre redundancia

- **contratos_master** (B1) es la fuente principal de contratos y subsume los
  datos que la API expone en A1/A2 (solo muestras de 1K registros). Las
  muestras API **no se incluyen** en la consolidación.
- **revascon_historico** (B2) contiene datos 2013-2018 con formato más rico
  que B1 para ese período. Hay solapamiento con contratos_master.
- **bilbao_contratos** (C1) puede incluir contratos menores municipales que
  no están en KontratazioA/REVASCON.

## Fuentes

- **KontratazioA API**: `https://api.euskadi.eus/procurements/`
- **Open Data Euskadi**: `https://opendata.euskadi.eus/`
- **Portal Bilbao**: `https://www.bilbao.eus/opendata/`

## Esquema de columnas

"""

    for key, info in all_stats.items():
        cols = info.get("lista_columnas", [])
        if cols:
            readme += f"### {key}.parquet\n\n"
            readme += f"Columnas ({len(cols)}): "
            readme += ", ".join(f"`{c}`" for c in cols[:30])
            if len(cols) > 30:
                readme += f" ... (+{len(cols)-30} más)"
            readme += "\n\n"

    (OUTPUT_DIR / "README.md").write_text(readme, encoding="utf-8")
    log.info("  ✓ README.md generado")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    import time as _time
    t0 = _time.time()

    log.info("╔═══════════════════════════════════════════════════════════╗")
    log.info("║  CONSOLIDACIÓN EUSKADI v4 → PARQUET                     ║")
    log.info("║  Fecha: %s                                  ║",
             datetime.now().strftime("%Y-%m-%d"))
    log.info("╚═══════════════════════════════════════════════════════════╝")

    # Verificar dependencias
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        log.error("Falta pyarrow. Instala con: pip install pyarrow")
        sys.exit(1)

    try:
        import openpyxl  # noqa: F401
    except ImportError:
        log.error("Falta openpyxl. Instala con: pip install openpyxl")
        sys.exit(1)

    # Verificar que exista el directorio de entrada
    if not INPUT_DIR.exists():
        log.error("Directorio de entrada no encontrado: %s", INPUT_DIR)
        log.error("Ejecuta primero el scraper: python descarga_euskadi_v4.py")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Consolidar cada módulo ──────────────────────────────
    all_stats = {}

    all_stats["contratos_master"]       = consolidar_B1_contratos_master()
    all_stats["poderes_adjudicadores"]  = consolidar_A3_poderes()
    all_stats["empresas_licitadoras"]   = consolidar_A4_empresas()
    all_stats["revascon_historico"]      = consolidar_B2_revascon()
    all_stats["bilbao_contratos"]       = consolidar_C1_bilbao()
    all_stats["ultimos_90d"]            = consolidar_B3_ultimos_90d()

    # ── Generar documentación ───────────────────────────────
    log.info("=" * 60)
    log.info("DOCUMENTACIÓN")
    log.info("=" * 60)

    # Stats JSON
    stats_out = {
        "fecha": datetime.now().isoformat(),
        "input_dir": str(INPUT_DIR),
        "output_dir": str(OUTPUT_DIR),
        "datasets": all_stats,
    }
    (OUTPUT_DIR / "stats.json").write_text(
        json.dumps(stats_out, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    log.info("  ✓ stats.json generado")

    generar_readme(all_stats)

    # ── Resumen final ───────────────────────────────────────
    elapsed = _time.time() - t0
    total_regs = sum(v.get("registros", 0) for v in all_stats.values())
    total_mb = sum(v.get("tamaño_mb", 0) for v in all_stats.values())
    n_files = len([v for v in all_stats.values() if v.get("registros", 0) > 0])

    log.info("═" * 60)
    log.info("RESUMEN CONSOLIDACIÓN")
    log.info("─" * 60)
    log.info("  Archivos Parquet:  %d", n_files)
    log.info("  Total registros:   %s", f"{total_regs:,}")
    log.info("  Tamaño Parquet:    %.1f MB", total_mb)
    log.info("  Tiempo:            %.0f s", elapsed)
    log.info("─" * 60)

    for key, info in all_stats.items():
        regs = info.get("registros", 0)
        mb = info.get("tamaño_mb", 0)
        if regs > 0:
            log.info("  ✓ %-30s %8s regs  %6.1f MB",
                     f"{key}.parquet", f"{regs:,}", mb)
        else:
            nota = info.get("nota", info.get("error", "sin datos"))
            log.info("  ✗ %-30s %s", key, nota)

    log.info("═" * 60)
    log.info("\nSalida: %s/", OUTPUT_DIR)
    log.info("  Uso:")
    log.info('    df = pd.read_parquet("euskadi_parquet/contratos_master.parquet")')
    log.info("    df.info()")


if __name__ == "__main__":
    main()


