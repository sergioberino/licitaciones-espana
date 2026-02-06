"""
═══════════════════════════════════════════════════════════════════════════════
  MÓDULO TED — v6.0
  
  Parte 1: Descargador de datos TED (Contract Award Notices) para España
  Parte 2: Cross-validation TED ↔ PLACSP/PSCP
  
  Fuentes de datos:
    - CSV bulk: data.europa.eu (2006-2023, formato legacy)
    - TED Search API v3: api.ted.europa.eu (2024+, eForms)
    - TED SPARQL: data.ted.europa.eu (alternativa)
  
  Cambios v6.0 respecto a v5.5:
    - API endpoint correcto: POST https://api.ted.europa.eu/v3/notices/search
    - Body params: "query" (no "q"), "page"/"limit" (no "pageNum"/"pageSize")
    - scope="ALL" (string, no int)
    - Query syntax eForms sin corchetes: notice-type IN (...) 
    - Fields eForms descubiertos: winner-identifier, tender-value, etc.
    - Parser adaptado a estructura real de respuesta API (multi-lot)
    - Eliminado endpoint legacy v3.0 (404 permanente)
    - Eliminada descarga CSV consolidado (404)
  
  Uso:
    1. Ejecutar download_ted_spain() para obtener ted_es_can.parquet
    2. Integrar cross_validate_ted() en el pipeline principal
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import time
import json
import math
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════

class TEDConfig:
    """Configuración del módulo TED."""
    
    # ── Directorios ──
    DATA_DIR = Path("data/ted")
    OUTPUT_DIR = Path("output")
    
    # ── Filtro geográfico ──
    COUNTRY_CODE = "ES"  # España
    
    # ── CSV bulk ──
    # URLs del dataset CSV en data.europa.eu
    # Formato nuevo: "TED%202020/TED%20-%20Contract%20award%20notices%20{year}.csv"
    CSV_BASE_URL = "https://data.europa.eu/euodp/repository/ec/dg-grow/mapps"
    CSV_YEARS_AVAILABLE = range(2006, 2024)  # CSV llega hasta ~2023
    
    # ── TED Search API v3 (2024+, eForms) ──
    # Endpoint correcto (verificado feb 2026):
    #   POST https://api.ted.europa.eu/v3/notices/search
    #   Body JSON: { "query": "...", "fields": [...], "page": 1, "limit": 100, "scope": "ALL" }
    TED_API_SEARCH = "https://api.ted.europa.eu/v3/notices/search"
    TED_API_PAGE_SIZE = 100  # Máximo por página
    TED_API_RATE_LIMIT = 1.0  # Segundos entre requests (0.5 causa 429)
    
    # ── Campos eForms para la API ──
    # Descubiertos via error-mining del endpoint (feb 2026)
    # Solo can-standard/can-social devuelven winner/tender data
    API_FIELDS = [
        "publication-number",
        "notice-type",
        # ── Importe ──
        "tender-value",
        "tender-value-cur",
        "tender-value-highest",
        "tender-value-lowest",
        "result-value-lot",
        "result-value-cur-lot",
        "result-value-notice",
        "result-value-cur-notice",
        "estimated-value-lot",
        "estimated-value-cur-lot",
        "estimated-value-proc",
        "estimated-value-cur-proc",
        "total-value",
        "total-value-cur",
        # ── Ganador ──
        "winner-name",
        "winner-identifier",
        "winner-country",
        "winner-decision-date",
        "winner-city",
        "winner-size",                  # PYME / grande
        "winner-listed",                # ¿Cotizada?
        "winner-owner-nationality",     # Nacionalidad propietario
        "winner-selection-status",      # Estado selección
        # ── Comprador ──
        "buyer-name",
        "buyer-identifier",
        "buyer-country",
        "buyer-city",
        "buyer-legal-type",             # Tipo jurídico (para umbrales UE)
        "buyer-contracting-entity",     # ¿Sectorial?
        "buyer-profile",                # URL perfil contratante
        # ── Clasificación ──
        "classification-cpv",
        # ── Ofertas ──
        "received-submissions-type-val",
        "received-submissions-type-code",
        # ── IDs / Linking ──
        "contract-identifier",
        "tender-identifier",
        "procedure-identifier",         # ID procedimiento TED
        "internal-identifier-proc",     # Nº expediente interno (!)
        "internal-identifier-lot",      # ID lote interno
        "identifier-lot",               # ID lote
        "result-lot-identifier",        # ID lote resultado
        "tender-lot-identifier",        # ID lote oferta
        "modification-previous-notice-identifier",  # Notice previa (modificados)
        # ── Procedimiento ──
        "direct-award-justification-proc",  # Justificación negociado s/p
        "direct-award-justification-text-proc",
        "non-award-justification",      # Justificación no-adjudicación
        "sme-part",                     # ¿Participación PYME?
        # ── Contrato ──
        "duration-period-value-lot",    # Duración contrato
        "subcontracting-value",         # Valor subcontratación
        "subcontracting-value-cur",
        # ── Criterios adjudicación ──
        "award-criterion-type-lot",     # Precio vs calidad
        "award-criterion-number-weight-lot",  # Peso criterio (%)
        # ── Framework ──
        "framework-estimated-value",
        "framework-maximum-value-lot",
        # ── Empresa ──
        "business-country",
        "business-identifier",
    ]
    
    # ── Campos a extraer del CSV bulk ──
    CSV_COLUMNS_KEEP = [
        'ID_NOTICE_CAN', 'YEAR', 'ISO_COUNTRY_CODE',
        'CAE_NAME', 'CAE_NATIONALID', 'CAE_TYPE', 'CAE_TOWN',
        'TAL_LOCATION_NUTS', 'TYPE_OF_CONTRACT', 'CPV',
        'TOP_TYPE',
        'VALUE_EURO_FIN_1',
        'AWARD_VALUE_EURO_FIN_1',
        'WIN_NAME', 'WIN_NATIONALID', 'WIN_COUNTRY_CODE',
        'NUMBER_OFFERS', 'NUMBER_AWARDS',
        'DT_DISPATCH', 'DT_AWARD',
        'B_FRA_AGREEMENT', 'CANCELLED',
        'ID_AWARD', 'ID_LOT_AWARDED',
        'ADDITIONAL_CPV', 'LOTS_NUMBER',
    ]
    
    # ── Umbrales UE para España (2024, sin IVA) ──
    EU_THRESHOLD_WORKS = 5_382_000
    EU_THRESHOLD_SUPPLIES_CENTRAL = 140_000
    EU_THRESHOLD_SUPPLIES_SUB = 215_000
    EU_THRESHOLD_SERVICES_CENTRAL = 140_000
    EU_THRESHOLD_SERVICES_SUB = 215_000
    EU_THRESHOLD_UTILITIES = 431_000
    EU_THRESHOLD_MIN = 140_000
    
    # ── Cross-validation ──
    MATCH_TOLERANCE_PCT = 0.10  # ±10% del importe
    MATCH_TOLERANCE_ABS = 5_000  # O ±5000€
    MATCH_YEAR_WINDOW = 1  # ±1 año para matching temporal


# ═══════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════

log = logging.getLogger('ted_module')


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 1: DESCARGA DE DATOS TED
# ═══════════════════════════════════════════════════════════════════════════

def download_ted_spain(
    years=None,
    force_redownload=False,
    output_path=None,
):
    """
    Descarga y combina datos TED de España.
    
    Estrategia dual:
      - 2006-2023: CSV bulk (legacy format, columnas tipo WIN_NAME, CAE_NATIONALID)
      - 2024+: TED Search API v3 (eForms, campos tipo winner-identifier)
    
    Returns:
        pd.DataFrame con todos los CAN de España
    """
    if not HAS_REQUESTS:
        log.error("Necesitas: pip install requests")
        return None
    
    TEDConfig.DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if output_path is None:
        output_path = TEDConfig.DATA_DIR / "ted_es_can.parquet"
    
    if output_path.exists() and not force_redownload:
        log.info(f"Cargando cache: {output_path}")
        return pd.read_parquet(output_path)
    
    if years is None:
        years = list(range(2010, datetime.now().year + 1))
    
    all_dfs = []
    
    # ── CSV bulk para años disponibles, API para el resto ──
    csv_years = [y for y in years if y in TEDConfig.CSV_YEARS_AVAILABLE]
    api_years = [y for y in years if y not in TEDConfig.CSV_YEARS_AVAILABLE]
    
    # Años que fallan en CSV se reintentan por API
    csv_failed_years = []
    
    if csv_years:
        log.info(f"📥 Descargando CSV bulk para {csv_years[0]}-{csv_years[-1]}...")
        for year in csv_years:
            df_year = _download_csv_year(year, force_redownload)
            if df_year is not None and len(df_year) > 0:
                all_dfs.append(df_year)
            else:
                csv_failed_years.append(year)
    
    # Años que no tienen CSV + años que fallaron en CSV → API
    api_years = sorted(set(api_years + csv_failed_years))
    
    if api_years:
        log.info(f"🌐 Consultando TED API para {api_years}...")
        for year in api_years:
            df_year = _download_api_year(year, force_redownload)
            if df_year is not None and len(df_year) > 0:
                all_dfs.append(df_year)
    
    if not all_dfs:
        log.error("No se obtuvieron datos de ninguna fuente")
        return None
    
    # ── Pre-normalizar columnas CSV antes del concat ──
    # CSV usa MAYÚSCULAS (YEAR, WIN_NATIONALID), API usa minúsculas (year, win_nationalid)
    # Renombramos CSV para que se fusionen correctamente
    csv_to_api = {
        'ID_NOTICE_CAN': 'ted_notice_id',
        'YEAR': 'year',
        'ISO_COUNTRY_CODE': 'iso_country',
        'CAE_NAME': 'cae_name',
        'CAE_NATIONALID': 'cae_nationalid',
        'CAE_TYPE': 'cae_type',
        'CAE_TOWN': 'cae_town',
        'TAL_LOCATION_NUTS': 'nuts',
        'TYPE_OF_CONTRACT': 'type_of_contract',
        'CPV': 'cpv',
        'ADDITIONAL_CPV': 'cpv_additional',
        'TOP_TYPE': 'top_type',
        'VALUE_EURO_FIN_1': 'value_euro',
        'AWARD_VALUE_EURO_FIN_1': 'award_value_euro',
        'WIN_NAME': 'win_name',
        'WIN_NATIONALID': 'win_nationalid',
        'WIN_COUNTRY_CODE': 'win_country',
        'NUMBER_OFFERS': 'number_offers',
        'NUMBER_AWARDS': 'number_awards',
        'DT_DISPATCH': 'dt_dispatch',
        'DT_AWARD': 'dt_award',
        'B_FRA_AGREEMENT': 'is_framework',
        'CANCELLED': 'cancelled',
        'ID_AWARD': 'ted_award_id',
        'ID_LOT_AWARDED': 'lot_id',
        'LOTS_NUMBER': 'lots_number',
    }
    for i, df_part in enumerate(all_dfs):
        rename = {k: v for k, v in csv_to_api.items() if k in df_part.columns}
        if rename:
            all_dfs[i] = df_part.rename(columns=rename)
            if 'source' not in all_dfs[i].columns:
                all_dfs[i]['source'] = 'csv_bulk'
    
    # ── Combinar y normalizar ──
    df = pd.concat(all_dfs, ignore_index=True)
    log.info(f"Total registros brutos: {len(df):,}")
    
    df = _normalize_ted_data(df)
    
    # Guardar
    df.to_parquet(output_path, index=False)
    log.info(f"✅ Guardado: {output_path} ({len(df):,} registros)")
    
    _print_ted_summary(df)
    
    return df


def _download_csv_year(year, force=False):
    """Descarga CSV de CAN para un año y filtra por España."""
    cache_path = TEDConfig.DATA_DIR / f"ted_can_{year}_ES.parquet"
    
    if cache_path.exists() and not force:
        log.info(f"  {year}: usando cache {cache_path}")
        return pd.read_parquet(cache_path)
    
    # URLs en orden de prioridad — espacios codificados como %20
    urls = [
        f"{TEDConfig.CSV_BASE_URL}/TED%202020/TED%20-%20Contract%20award%20notices%20{year}.csv",
        f"{TEDConfig.CSV_BASE_URL}/TED_CAN_{year}.csv",
    ]
    
    df = None
    for url in urls:
        try:
            log.info(f"  {year}: probando {url.split('/')[-1]}...")
            chunks = []
            for chunk in pd.read_csv(
                url, 
                encoding='utf-8',
                low_memory=False,
                chunksize=50_000,
                dtype=str,
                on_bad_lines='skip',
            ):
                if 'ISO_COUNTRY_CODE' not in chunk.columns:
                    log.warning(f"  {year}: columna ISO_COUNTRY_CODE no encontrada")
                    break
                es_mask = chunk['ISO_COUNTRY_CODE'] == TEDConfig.COUNTRY_CODE
                if es_mask.any():
                    keep_cols = [c for c in TEDConfig.CSV_COLUMNS_KEEP if c in chunk.columns]
                    chunks.append(chunk.loc[es_mask, keep_cols])
            
            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                log.info(f"  {year}: {len(df):,} registros España de CSV bulk")
            break
        except Exception as e:
            log.debug(f"  {year}: {url.split('/')[-1]} → {e}")
            continue
    
    if df is None:
        log.warning(f"  {year}: no se pudo descargar CSV")
    elif len(df) > 0:
        df.to_parquet(cache_path, index=False)
    
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  TED SEARCH API v3 — eForms (2024+)
# ═══════════════════════════════════════════════════════════════════════════

def _download_api_year(year, force=False):
    """
    Descarga CAN de España para un año vía TED Search API v3.
    
    La API tiene un límite de ~15,000 resultados por query (150 páginas × 100).
    Para años con más resultados, se divide en trimestres automáticamente.
    """
    cache_path = TEDConfig.DATA_DIR / f"ted_can_{year}_ES_api.parquet"
    
    if cache_path.exists() and not force:
        log.info(f"  {year}: usando cache {cache_path}")
        return pd.read_parquet(cache_path)
    
    # Definir periodos: año completo primero, si falla por límite → trimestres
    periods = [
        (f"{year}0101", f"{year}1231", f"{year}"),
    ]
    
    all_records = []
    needs_split = False
    
    for date_from, date_to, period_label in periods:
        records, hit_limit = _download_api_period(year, date_from, date_to, period_label)
        all_records.extend(records)
        
        if hit_limit:
            needs_split = True
            break
    
    # Si la query anual excede el límite, dividir en trimestres
    if needs_split:
        log.info(f"  {year}: límite paginación alcanzado, dividiendo en trimestres...")
        all_records = []
        quarters = [
            (f"{year}0101", f"{year}0331", f"{year}-Q1"),
            (f"{year}0401", f"{year}0630", f"{year}-Q2"),
            (f"{year}0701", f"{year}0930", f"{year}-Q3"),
            (f"{year}1001", f"{year}1231", f"{year}-Q4"),
        ]
        for date_from, date_to, period_label in quarters:
            records, _ = _download_api_period(year, date_from, date_to, period_label)
            all_records.extend(records)
    
    if not all_records:
        log.warning(f"  {year}: sin resultados de API")
        return None
    
    df = pd.DataFrame(all_records)
    
    # Deduplicar por ted_notice_id + lot_index (trimestres pueden solapar)
    if 'ted_notice_id' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['ted_notice_id', 'lot_index'], keep='first')
        dupes = before - len(df)
        if dupes > 0:
            log.info(f"  {year}: eliminados {dupes:,} duplicados")
    
    log.info(f"  {year}: {len(df):,} registros de API")
    
    if len(df) > 0:
        df.to_parquet(cache_path, index=False)
    
    return df


def _download_api_period(year, date_from, date_to, period_label):
    """
    Descarga un periodo específico de la API.
    Returns: (records_list, hit_pagination_limit)
    """
    query = (
        f"notice-type IN (can-standard, can-social, can-modif, can-desg) "
        f"AND buyer-country=ESP "
        f"AND publication-date>={date_from} "
        f"AND publication-date<={date_to}"
    )
    
    records = []
    page = 1
    total_count = None
    total_pages = None
    consecutive_errors = 0
    max_errors = 3
    hit_limit = False
    
    while True:
        try:
            body = {
                "query": query,
                "fields": TEDConfig.API_FIELDS,
                "page": page,
                "limit": TEDConfig.TED_API_PAGE_SIZE,
                "scope": "ALL",
                "checkQuerySyntax": False,
                "paginationMode": "PAGE_NUMBER",
            }
            
            log.debug(f"  POST {period_label} page={page}")
            resp = requests.post(
                TEDConfig.TED_API_SEARCH,
                json=body,
                timeout=60,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )
            
            if resp.status_code == 429:
                log.warning(f"  Rate limited, esperando 15s...")
                time.sleep(15)
                continue
            
            if resp.status_code == 400:
                # Probable límite de paginación
                if page > 100:
                    log.warning(f"  {period_label}: HTTP 400 en página {page} "
                               f"(límite paginación, {len(records):,} registros obtenidos)")
                    hit_limit = True
                    break
                consecutive_errors += 1
                log.warning(f"  {period_label} page {page}: HTTP 400")
                if consecutive_errors >= max_errors:
                    hit_limit = (page > 50)  # Probable límite si pasamos de 50
                    break
                time.sleep(2)
                continue
            
            if resp.status_code in (404, 405, 500, 502, 503):
                consecutive_errors += 1
                log.warning(f"  {period_label} page {page}: HTTP {resp.status_code}")
                if consecutive_errors >= max_errors:
                    break
                time.sleep(2)
                continue
            
            resp.raise_for_status()
            data = resp.json()
            consecutive_errors = 0
            
        except requests.exceptions.RequestException as e:
            consecutive_errors += 1
            log.warning(f"  {period_label} page {page}: {e}")
            if consecutive_errors >= max_errors:
                break
            time.sleep(2)
            continue
        
        # Parsear respuesta
        notices = data.get("notices", data.get("results", []))
        
        if total_count is None:
            total_count = data.get("total", data.get("totalNoticeCount", None))
            if total_count is not None:
                total_pages = math.ceil(total_count / TEDConfig.TED_API_PAGE_SIZE)
                log.info(f"  {period_label}: {total_count:,} resultados, ~{total_pages} páginas")
            else:
                log.info(f"  {period_label}: paginando ({len(notices)} resultados primera página)")
                total_pages = None
            
            if not notices:
                break
        
        if not notices:
            break
        
        for notice in notices:
            parsed = _parse_api_notice(notice)
            records.extend(parsed)
        
        # Paginación
        if total_pages is not None and page >= total_pages:
            break
        
        if len(notices) < TEDConfig.TED_API_PAGE_SIZE:
            break
        
        page += 1
        time.sleep(TEDConfig.TED_API_RATE_LIMIT)
        
        if page % 20 == 0:
            log.info(f"    {period_label} pág {page}: {len(records):,} registros...")
    
    return records, hit_limit


def _parse_api_notice(notice):
    """
    Parsea un resultado de la TED Search API v3 (eForms) a lista de dicts.
    
    La API devuelve listas para multi-lot notices:
      - winner-name: {'spa': ['EMPRESA A', 'EMPRESA B']}
      - winner-identifier: ['A12345678', 'B87654321']
      - tender-value: ['100000', '200000']
    
    Genera un registro por lot/winner. Para notices con un solo winner,
    genera un único registro.
    
    Returns:
        Lista de dicts (uno por lot/award)
    """
    try:
        pub_number = notice.get("publication-number", "")
        notice_type = notice.get("notice-type", "")
        
        # ── Comprador ──
        buyer_name = _extract_multilang_name(notice.get("buyer-name", {}))
        
        buyer_ids = _as_list(notice.get("buyer-identifier", []))
        # NIF español: 9 chars tipo A12345678 o P0400000F
        buyer_nif = _find_spanish_nif(buyer_ids)
        
        buyer_country = _first_of_list(notice.get("buyer-country", []), "ES")
        buyer_city = _extract_multilang_name(notice.get("buyer-city", {})) \
            if isinstance(notice.get("buyer-city"), dict) else str(notice.get("buyer-city", ""))
        
        # ── CPV ──
        cpv_raw = _as_list(notice.get("classification-cpv", []))
        cpv = str(cpv_raw[0]) if cpv_raw else ""
        
        # ── Ganadores ──
        winner_names = _extract_multilang_list(notice.get("winner-name", {}))
        winner_ids = _as_list(notice.get("winner-identifier", []))
        winner_countries = _as_list(notice.get("winner-country", []))
        winner_dates = _as_list(notice.get("winner-decision-date", []))
        
        # ── Importes (prioridad: tender-value > result-value > estimated) ──
        tender_values = _as_list(notice.get("tender-value", []))
        result_values = _as_list(
            notice.get("result-value-lot", notice.get("result-value-notice", []))
        )
        estimated_values = _as_list(notice.get("estimated-value-lot", []))
        
        tender_cur = _first_of_list(notice.get("tender-value-cur", []), "EUR")
        
        # ── Ofertas recibidas ──
        offers_raw = _as_list(notice.get("received-submissions-type-val", []))
        
        # ── Año de publicación (del publication-number: XXXXXX-YYYY) ──
        pub_year = pub_number.split("-")[-1] if "-" in pub_number else ""
        
        # ── Campos extra (scalar o primer valor) ──
        procedure_id = _first_of_list(notice.get("procedure-identifier", []))
        internal_id_proc = _first_of_list(notice.get("internal-identifier-proc", []))
        internal_id_lot_list = _as_list(notice.get("internal-identifier-lot", []))
        
        total_value = _first_of_list(notice.get("total-value", []))
        total_value_cur = _first_of_list(notice.get("total-value-cur", []), "EUR")
        estimated_value_proc = _first_of_list(notice.get("estimated-value-proc", []))
        
        winner_sizes = _as_list(notice.get("winner-size", []))
        
        buyer_legal_type = _first_of_list(notice.get("buyer-legal-type", []))
        buyer_contracting_entity = _first_of_list(notice.get("buyer-contracting-entity", []))
        buyer_profile = _first_of_list(notice.get("buyer-profile", []))
        
        direct_award_just = _first_of_list(notice.get("direct-award-justification-proc", []))
        direct_award_text = _extract_multilang_name(notice.get("direct-award-justification-text-proc", {}))
        non_award_just = _first_of_list(notice.get("non-award-justification", []))
        sme_part = _first_of_list(notice.get("sme-part", []))
        
        duration_lot_list = _as_list(notice.get("duration-period-value-lot", []))
        subcontracting_value = _first_of_list(notice.get("subcontracting-value", []))
        
        award_criterion_type_list = _as_list(notice.get("award-criterion-type-lot", []))
        award_criterion_weight_list = _as_list(notice.get("award-criterion-number-weight-lot", []))
        
        modification_prev = _first_of_list(notice.get("modification-previous-notice-identifier", []))
        
        lot_ids = _as_list(notice.get("identifier-lot", []))
        result_lot_ids = _as_list(notice.get("result-lot-identifier", []))
        
        framework_est_value = _first_of_list(notice.get("framework-estimated-value", []))
        framework_max_lot = _first_of_list(notice.get("framework-maximum-value-lot", []))
        
        # ── Determinar número de registros ──
        # Para CAN multi-lot: un registro por winner/value
        # Para non-award notices: un solo registro
        if notice_type in ("cn-standard", "cn-social", "pin-buyer", "pin-standard"):
            n_records = 1
        else:
            n_records = max(len(winner_ids), len(winner_names), len(tender_values), 1)
        
        records = []
        for i in range(n_records):
            value = _safe_index(tender_values, i) \
                 or _safe_index(result_values, i) \
                 or _safe_index(estimated_values, i) \
                 or _safe_index(tender_values, 0) \
                 or ""
            
            record = {
                "ted_notice_id": pub_number,
                "year": pub_year,
                "iso_country": buyer_country,
                "notice_type": notice_type,
                # Comprador
                "cae_name": buyer_name,
                "cae_nationalid": buyer_nif,
                "cae_town": buyer_city,
                "buyer_legal_type": buyer_legal_type,
                "buyer_contracting_entity": buyer_contracting_entity,
                "buyer_profile": buyer_profile,
                # Ganador
                "win_name": _safe_index(winner_names, i) or _safe_index(winner_names, -1) or "",
                "win_nationalid": _safe_index(winner_ids, i) or _safe_index(winner_ids, -1) or "",
                "win_country": _safe_index(winner_countries, i) or "",
                "win_size": _safe_index(winner_sizes, i) or _safe_index(winner_sizes, 0) or "",
                # Importe
                "value_euro": value,
                "currency": tender_cur,
                "total_value": total_value,
                "total_value_cur": total_value_cur,
                "estimated_value_proc": estimated_value_proc,
                # Clasificación
                "cpv": cpv,
                # Ofertas
                "number_offers": _safe_index(offers_raw, i) or _safe_index(offers_raw, 0) or "",
                # Fechas
                "dt_award": _safe_index(winner_dates, i) or _safe_index(winner_dates, 0) or "",
                "dt_dispatch": "",
                # IDs / Linking
                "procedure_id": procedure_id,
                "internal_id_proc": internal_id_proc,
                "internal_id_lot": _safe_index(internal_id_lot_list, i) or "",
                "lot_id": _safe_index(lot_ids, i) or _safe_index(result_lot_ids, i) or "",
                "modification_prev_notice": modification_prev,
                # Procedimiento
                "direct_award_justification": direct_award_just,
                "direct_award_justification_text": direct_award_text,
                "non_award_justification": non_award_just,
                "sme_participation": sme_part,
                # Contrato
                "duration_lot": _safe_index(duration_lot_list, i) or "",
                "subcontracting_value": subcontracting_value,
                # Criterios
                "award_criterion_type": _safe_index(award_criterion_type_list, i) or "",
                "award_criterion_weight": _safe_index(award_criterion_weight_list, i) or "",
                # Framework
                "framework_est_value": framework_est_value,
                "framework_max_lot": framework_max_lot,
                # Meta
                "source": "api_v3",
                "lot_index": i if n_records > 1 else 0,
            }
            records.append(record)
        
        return records
    
    except Exception as e:
        log.debug(f"  Error parsing notice: {e}")
        return []


# ── Helpers para parseo eForms ──

def _as_list(val):
    """Asegura que val sea una lista."""
    if isinstance(val, list):
        return val
    if val is None or val == "":
        return []
    return [val]


def _first_of_list(val, default=""):
    """Primer elemento de lista o default."""
    lst = _as_list(val)
    return str(lst[0]) if lst else default


def _safe_index(lst, i, default=None):
    """Acceso seguro a lista por índice."""
    if not lst:
        return default
    try:
        return lst[i]
    except (IndexError, TypeError):
        return default


def _extract_multilang_name(name_dict):
    """Extrae nombre de dict multiidioma {'spa': ['Nombre'], 'eng': ['Name']}."""
    if not isinstance(name_dict, dict):
        return str(name_dict) if name_dict else ""
    
    for lang in ('spa', 'SPA', 'eng', 'ENG'):
        names = name_dict.get(lang, [])
        if names:
            return names[0] if isinstance(names, list) else str(names)
    
    # Fallback: primer valor disponible
    for names in name_dict.values():
        if names:
            return names[0] if isinstance(names, list) else str(names)
    return ""


def _extract_multilang_list(name_dict):
    """Extrae lista de nombres de dict multiidioma."""
    if not isinstance(name_dict, dict):
        if isinstance(name_dict, list):
            return name_dict
        return [str(name_dict)] if name_dict else []
    
    for lang in ('spa', 'SPA', 'eng', 'ENG'):
        names = name_dict.get(lang, [])
        if names:
            return names if isinstance(names, list) else [str(names)]
    
    for names in name_dict.values():
        if names:
            return names if isinstance(names, list) else [str(names)]
    return []


def _find_spanish_nif(id_list):
    """
    Encuentra NIF/CIF español en lista de identificadores.
    NIF: letra + 8 dígitos, o 8 dígitos + letra (9 chars total).
    Si hay múltiples IDs, prioriza el que parece NIF.
    """
    if not id_list:
        return ""
    
    for bid in id_list:
        bid_str = str(bid).strip()
        if len(bid_str) == 9 and (bid_str[0].isalpha() or bid_str[-1].isalpha()):
            return bid_str
    
    # Fallback: último ID (suele ser el NIF en datos TED)
    return str(id_list[-1]).strip()


# ═══════════════════════════════════════════════════════════════════════════
#  NORMALIZACIÓN
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_ted_data(df):
    """Normaliza campos del DataFrame TED a formato uniforme."""
    
    # ── Mapeo de columnas CSV bulk → nombres normalizados ──
    col_map = {
        'ID_NOTICE_CAN': 'ted_notice_id',
        'YEAR': 'year',
        'ISO_COUNTRY_CODE': 'iso_country',
        'CAE_NAME': 'cae_name',
        'CAE_NATIONALID': 'cae_nationalid',
        'CAE_TYPE': 'cae_type',
        'CAE_TOWN': 'cae_town',
        'TAL_LOCATION_NUTS': 'nuts',
        'TYPE_OF_CONTRACT': 'type_of_contract',
        'CPV': 'cpv',
        'ADDITIONAL_CPV': 'cpv_additional',
        'TOP_TYPE': 'top_type',
        'VALUE_EURO_FIN_1': 'value_euro',
        'AWARD_VALUE_EURO_FIN_1': 'award_value_euro',
        'WIN_NAME': 'win_name',
        'WIN_NATIONALID': 'win_nationalid',
        'WIN_COUNTRY_CODE': 'win_country',
        'NUMBER_OFFERS': 'number_offers',
        'NUMBER_AWARDS': 'number_awards',
        'DT_DISPATCH': 'dt_dispatch',
        'DT_AWARD': 'dt_award',
        'B_FRA_AGREEMENT': 'is_framework',
        'CANCELLED': 'cancelled',
        'ID_AWARD': 'ted_award_id',
        'ID_LOT_AWARDED': 'lot_id',
        'LOTS_NUMBER': 'lots_number',
    }
    
    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    
    # ── Aplanar columnas que puedan contener listas (API v3 devuelve listas) ──
    # Primero deduplicar columnas (CSV + API pueden crear duplicados)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Con 500K+ filas, solo checar columnas object
    for col in df.columns:
        if df[col].dtype != object:
            continue
        # Check a larger sample for lists/dicts
        sample = df[col].dropna()
        if len(sample) == 0:
            continue
        # Check first, middle and last chunks
        check_idx = list(sample.index[:20]) + list(sample.index[-20:])
        has_complex = False
        for idx in check_idx:
            val = sample.loc[idx]
            if isinstance(val, (list, dict)):
                has_complex = True
                break
        
        if has_complex:
            def _flatten(x):
                if isinstance(x, list):
                    return x[0] if len(x) > 0 else None
                if isinstance(x, dict):
                    # Multilang dict: {'spa': ['val']}
                    for v in x.values():
                        if isinstance(v, list) and v:
                            return v[0]
                        if v:
                            return v
                    return str(x)
                return x
            df[col] = df[col].apply(_flatten)
    
    # ── Tipos numéricos ──
    numeric_cols = [
        'value_euro', 'award_value_euro', 'number_offers', 'year',
        'total_value', 'estimated_value_proc', 'subcontracting_value',
        'duration_lot', 'framework_est_value', 'framework_max_lot',
    ]
    for col in numeric_cols:
        if col in df.columns:
            # Force to string first, clean, then convert
            df[col] = df[col].apply(lambda x: 
                str(x[0]) if isinstance(x, list) and x else
                (str(x) if x is not None and not isinstance(x, float) else x)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # ── Mejor estimación del importe ──
    # Prioridad: award_value > value_euro (tender-value) > total_value > estimated
    if 'award_value_euro' in df.columns and 'value_euro' in df.columns:
        df['importe_ted'] = df['award_value_euro'].fillna(df['value_euro'])
    elif 'value_euro' in df.columns:
        df['importe_ted'] = pd.to_numeric(df['value_euro'], errors='coerce')
    elif 'award_value_euro' in df.columns:
        df['importe_ted'] = df['award_value_euro']
    else:
        df['importe_ted'] = np.nan
    
    # Fallback a total_value o estimated para registros sin importe
    if 'total_value' in df.columns:
        mask = df['importe_ted'].isna() & df['total_value'].notna()
        df.loc[mask, 'importe_ted'] = df.loc[mask, 'total_value']
    if 'estimated_value_proc' in df.columns:
        mask = df['importe_ted'].isna() & df['estimated_value_proc'].notna()
        df.loc[mask, 'importe_ted'] = df.loc[mask, 'estimated_value_proc']
    
    # ── Limpiar NIF del ganador ──
    nif_col = 'win_nationalid' if 'win_nationalid' in df.columns else None
    if nif_col:
        df['win_nif_clean'] = df[nif_col].fillna('').astype(str).str.strip().str.upper()
        # Quitar prefijo país (ES, ES-, ESA, etc.) solo si va seguido del NIF
        df['win_nif_clean'] = df['win_nif_clean'].str.replace(r'^ES[-\s]*(?=[A-Z0-9])', '', regex=True)
        # Quitar strings vacías, "NONE", "NAN", etc.
        df.loc[df['win_nif_clean'].isin(['', 'NONE', 'NAN', 'N/A']), 'win_nif_clean'] = ''
        df.loc[df['win_nif_clean'].str.len() < 5, 'win_nif_clean'] = ''
    else:
        df['win_nif_clean'] = ''
    
    n_nif = (df['win_nif_clean'] != '').sum()
    log.info(f"  NIFs ganador limpios: {n_nif:,} ({n_nif/len(df)*100:.1f}%)")
    
    # ── Limpiar NIF del órgano ──
    if 'cae_nationalid' in df.columns:
        df['cae_nif_clean'] = df['cae_nationalid'].fillna('').astype(str).str.strip().str.upper()
        df['cae_nif_clean'] = df['cae_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)
    
    # ── Fechas ──
    for col in ['dt_dispatch', 'dt_award']:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(r'\+\d{2}:\d{2}$', '', regex=True)
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
    
    # ── Año ──
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
    # Fallback: extraer de dt_award si year es NaN
    if 'year' in df.columns and 'dt_award' in df.columns:
        mask = df['year'].isna() & df['dt_award'].notna()
        if hasattr(df['dt_award'], 'dt'):
            df.loc[mask, 'year'] = df.loc[mask, 'dt_award'].dt.year
    if ('year' not in df.columns or df['year'].isna().all()) and 'dt_dispatch' in df.columns:
        df['year'] = df['dt_dispatch'].dt.year
    # Último fallback: extraer año del ted_notice_id (formato XXXXXX-YYYY)
    if 'year' in df.columns and 'ted_notice_id' in df.columns:
        mask = df['year'].isna()
        if mask.any():
            extracted = df.loc[mask, 'ted_notice_id'].astype(str).str.extract(r'-(\d{4})$')
            if len(extracted.columns) > 0:
                df.loc[mask, 'year'] = pd.to_numeric(extracted[0], errors='coerce')
    
    # ── Eliminar cancelados ──
    if 'cancelled' in df.columns:
        n_cancelled = (df['cancelled'].astype(str) == '1').sum()
        df = df[df['cancelled'].astype(str) != '1'].copy()
        if n_cancelled > 0:
            log.info(f"  Eliminados {n_cancelled:,} notices cancelados")
    
    # ── CPV limpio (primeros 2 dígitos) ──
    if 'cpv' in df.columns:
        df['cpv_2'] = df['cpv'].fillna('').astype(str).str[:2]
    
    # ── Tipo contrato legible ──
    type_map = {'W': 'obras', 'U': 'suministros', 'S': 'servicios'}
    if 'type_of_contract' in df.columns:
        df['tipo_contrato'] = df['type_of_contract'].map(type_map).fillna('otros')
    
    log.info(f"  Datos TED normalizados: {len(df):,} registros")
    
    return df


def _print_ted_summary(df):
    """Imprime resumen del dataset TED descargado."""
    print("\n" + "=" * 70)
    print("  RESUMEN DATOS TED — ESPAÑA")
    print("=" * 70)
    print(f"  Total registros (CAN): {len(df):,}")
    
    if 'year' in df.columns:
        year_counts = df['year'].dropna().value_counts().sort_index()
        if len(year_counts) > 0:
            print(f"  Rango temporal: {int(year_counts.index.min())}-{int(year_counts.index.max())}")
            print(f"\n  Registros por año:")
            for yr, cnt in year_counts.items():
                tag = " (API)" if yr >= 2024 else " (CSV)"
                print(f"    {int(yr)}: {cnt:>8,}{tag}")
    
    if 'source' in df.columns:
        print(f"\n  Por fuente:")
        for src, cnt in df['source'].value_counts().items():
            print(f"    {src}: {cnt:>8,}")
    
    if 'importe_ted' in df.columns:
        valid_imp = df['importe_ted'].dropna()
        if len(valid_imp) > 0:
            print(f"\n  Importes: media={valid_imp.mean():,.0f}€  mediana={valid_imp.median():,.0f}€")
        print(f"  Con importe: {len(valid_imp):,} ({len(valid_imp)/len(df)*100:.1f}%)")
    
    if 'win_nif_clean' in df.columns:
        n_with_nif = (df['win_nif_clean'].str.len() > 4).sum()
        print(f"  Con NIF ganador: {n_with_nif:,} ({n_with_nif/len(df)*100:.1f}%)")
    
    if 'number_offers' in df.columns:
        n_with_offers = df['number_offers'].notna().sum()
        print(f"  Con nº ofertas: {n_with_offers:,} ({n_with_offers/len(df)*100:.1f}%)")
    
    # ── Campos extra (solo API v3) ──
    api_rows = df[df.get('source', pd.Series()) == 'api_v3'] if 'source' in df.columns else pd.DataFrame()
    if len(api_rows) > 0:
        extras = {
            'internal_id_proc': 'Nº expediente interno',
            'win_size': 'Tamaño ganador (PYME)',
            'direct_award_justification': 'Justificación adj. directa',
            'sme_participation': 'Participación PYME',
            'duration_lot': 'Duración contrato',
            'subcontracting_value': 'Valor subcontratación',
            'award_criterion_type': 'Tipo criterio adjudicación',
            'buyer_legal_type': 'Tipo jurídico comprador',
            'modification_prev_notice': 'Notice previa (modificado)',
        }
        non_empty_extras = []
        for col, desc in extras.items():
            if col in api_rows.columns:
                filled = api_rows[col].notna() & (api_rows[col].astype(str).str.strip() != '')
                n = filled.sum()
                if n > 0:
                    non_empty_extras.append((desc, n, n/len(api_rows)*100))
        
        if non_empty_extras:
            print(f"\n  Campos extra (API v3, {len(api_rows):,} registros):")
            for desc, n, pct in sorted(non_empty_extras, key=lambda x: -x[1]):
                print(f"    {desc:<35}: {n:>6,} ({pct:.0f}%)")
    
    if 'tipo_contrato' in df.columns:
        print(f"\n  Por tipo de contrato:")
        for tipo, cnt in df['tipo_contrato'].value_counts().items():
            print(f"    {tipo:<15}: {cnt:>8,}")
    
    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════════════
#  ALTERNATIVA: Descarga via SPARQL
# ═══════════════════════════════════════════════════════════════════════════

SPARQL_QUERY_TEMPLATE = """
PREFIX epo: <http://data.europa.eu/a4g/ontology#>
PREFIX org: <http://www.w3.org/ns/org#>
PREFIX cccev: <http://data.europa.eu/m8g/>

SELECT ?notice ?publishedDate ?buyerName ?buyerCountry 
       ?winnerName ?winnerId ?cpv ?procedureType ?value
WHERE {{
  ?notice a epo:Notice ;
          epo:hasPublicationDate ?publishedDate ;
          epo:refersToProcedure ?procedure .
  
  ?procedure epo:hasBuyer ?buyer ;
             epo:hasProcedureType ?procedureType .
  
  ?buyer org:identifier/org:notation ?buyerCountry .
  FILTER(?buyerCountry = "ES")
  
  OPTIONAL {{ ?buyer epo:hasLegalName ?buyerName }}
  OPTIONAL {{ ?procedure epo:isSubjectTo/epo:hasMainClassification ?cpv }}
  OPTIONAL {{
    ?procedure epo:hasLotAwardOutcome/epo:hasAwardedValue/epo:hasAmountValue ?value
  }}
  OPTIONAL {{
    ?procedure epo:hasLotAwardOutcome/epo:hasContractor ?winner .
    ?winner epo:hasLegalName ?winnerName .
    OPTIONAL {{ ?winner org:identifier/org:notation ?winnerId }}
  }}
  
  FILTER(YEAR(?publishedDate) = {year})
}}
LIMIT 10000
OFFSET {offset}
"""


def download_ted_spain_sparql(years=None):
    """Descarga datos TED vía SPARQL endpoint."""
    SPARQL_ENDPOINT = "https://data.ted.europa.eu/sparql"
    
    if years is None:
        years = list(range(2020, datetime.now().year + 1))
    
    all_records = []
    
    for year in years:
        log.info(f"  SPARQL {year}...")
        offset = 0
        
        while True:
            query = SPARQL_QUERY_TEMPLATE.format(year=year, offset=offset)
            
            try:
                resp = requests.get(
                    SPARQL_ENDPOINT,
                    params={"query": query, "format": "json"},
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                
                bindings = data.get("results", {}).get("bindings", [])
                if not bindings:
                    break
                
                for b in bindings:
                    record = {
                        "ted_notice_id": b.get("notice", {}).get("value", ""),
                        "year": str(year),
                        "iso_country": "ES",
                        "cae_name": b.get("buyerName", {}).get("value", ""),
                        "cpv": b.get("cpv", {}).get("value", ""),
                        "value_euro": b.get("value", {}).get("value", ""),
                        "win_name": b.get("winnerName", {}).get("value", ""),
                        "win_nationalid": b.get("winnerId", {}).get("value", ""),
                        "source": "sparql",
                    }
                    all_records.append(record)
                
                if len(bindings) < 10000:
                    break
                offset += 10000
                
            except Exception as e:
                log.warning(f"  SPARQL {year}: {e}")
                break
    
    if not all_records:
        return None
    
    return pd.DataFrame(all_records)


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 2: CROSS-VALIDATION TED ↔ PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def cross_validate_ted(df_pipeline, df_ted, src, R=None):
    """
    Cruza datos del pipeline (PLACSP/PSCP) contra TED para:
    
    1. Marcar contratos validados por TED (_ted_validated)
    2. Detectar contratos sobre umbral UE ausentes en TED (MISSING_TED)
    3. Enriquecer campos desde TED (n_ofertas, CPV)
    
    Args:
        df_pipeline: DataFrame del pipeline (con _nif, _imp_adj, _organ, etc.)
        df_ted: DataFrame de TED (output de download_ted_spain)
        src: 'CAT' o 'NAC'
        R: Reporter del pipeline (opcional)
    
    Returns:
        df_pipeline: Con columnas nuevas (_ted_validated, _ted_missing, etc.)
        df_missing: DataFrame con contratos que deberían estar en TED pero no están
    """
    if R:
        R.section(f"3A · CROSS-VALIDATION TED [{src}]")
    else:
        print(f"\n{'='*60}\n  CROSS-VALIDATION TED [{src}]\n{'='*60}")
    
    _log = R.log if R else print
    
    if df_ted is None or len(df_ted) == 0:
        _log("  ⚠️ Sin datos TED — saltando cross-validation")
        df_pipeline['_ted_validated'] = False
        df_pipeline['_ted_missing'] = False
        return df_pipeline, pd.DataFrame()
    
    # ── 1. Preparar lookup de TED ──
    ted_valid = df_ted[
        (df_ted['importe_ted'].notna()) & 
        (df_ted['importe_ted'] > 0)
    ].copy()
    
    # Construir índice primario: (nif_limpio, año) → lista de importes
    ted_lookup = defaultdict(list)
    # Índice secundario: internal_id_proc (nº expediente) → lista
    ted_lookup_exp = defaultdict(list)
    
    for _, row in ted_valid.iterrows():
        nif = row.get('win_nif_clean', '')
        imp = row['importe_ted']
        yr = row.get('year', np.nan)
        
        entry = {
            'importe': imp,
            'ted_id': row.get('ted_notice_id', ''),
            'n_ofertas': row.get('number_offers', np.nan),
            'cpv_ted': row.get('cpv', ''),
            'cae_ted': row.get('cae_name', ''),
            'win_size': row.get('win_size', ''),
            'direct_award': row.get('direct_award_justification', ''),
            'sme_part': row.get('sme_participation', ''),
            'buyer_legal_type': row.get('buyer_legal_type', ''),
            'duration_lot': row.get('duration_lot', np.nan),
            'award_criterion_type': row.get('award_criterion_type', ''),
            'internal_id': row.get('internal_id_proc', ''),
            'consumed': False,
        }
        
        if nif and len(nif) >= 5 and pd.notna(yr):
            yr = int(yr)
            ted_lookup[(nif, yr)].append(entry)
        
        # Índice por nº expediente (matching directo sin NIF+importe)
        exp_id = str(row.get('internal_id_proc', '')).strip()
        if exp_id and len(exp_id) >= 4:
            ted_lookup_exp[exp_id.upper()].append(entry)
    
    _log(f"  TED lookup: {len(ted_lookup):,} claves (nif, año)")
    _log(f"  TED lookup expediente: {len(ted_lookup_exp):,} claves")
    _log(f"  TED registros con importe: {len(ted_valid):,}")
    
    # ── 2. Match pipeline → TED ──
    pipeline_valid = df_pipeline[
        df_pipeline['_nif'].notna() & 
        df_pipeline['_imp_adj'].notna() &
        (df_pipeline['_imp_adj'] > 0)
    ]
    
    matched_idx = []
    match_data = {}
    
    for idx, row in pipeline_valid.iterrows():
        nif = str(row['_nif']).strip().upper()
        imp = row['_imp_adj']
        yr = row.get('_año', np.nan)
        
        if pd.isna(yr):
            fecha = row.get('_fecha_adj', pd.NaT)
            if pd.notna(fecha):
                yr = fecha.year
            else:
                continue
        
        yr = int(yr)
        tol = max(imp * TEDConfig.MATCH_TOLERANCE_PCT, TEDConfig.MATCH_TOLERANCE_ABS)
        
        best_match = None
        best_diff = float('inf')
        best_key = None
        best_match_idx = None
        best_lookup = None  # Track which lookup dict
        
        # ── Estrategia 1: Match por NIF + importe + año ──
        for yr_offset in range(TEDConfig.MATCH_YEAR_WINDOW + 1):
            for yr_try in [yr + yr_offset, yr - yr_offset] if yr_offset > 0 else [yr]:
                key = (nif, yr_try)
                entries = ted_lookup.get(key, [])
                
                for i, entry in enumerate(entries):
                    if entry['consumed']:
                        continue
                    diff = abs(entry['importe'] - imp)
                    if diff <= tol and diff < best_diff:
                        best_match = entry
                        best_diff = diff
                        best_key = key
                        best_match_idx = i
                        best_lookup = ted_lookup
        
        # ── Estrategia 2: Match por nº expediente (si disponible) ──
        if best_match is None and '_expediente' in row.index:
            exp_id = str(row.get('_expediente', '')).strip().upper()
            if exp_id and len(exp_id) >= 4:
                entries = ted_lookup_exp.get(exp_id, [])
                for i, entry in enumerate(entries):
                    if entry['consumed']:
                        continue
                    diff = abs(entry['importe'] - imp)
                    if diff <= tol and diff < best_diff:
                        best_match = entry
                        best_diff = diff
                        best_key = exp_id
                        best_match_idx = i
                        best_lookup = ted_lookup_exp
        
        if best_match is not None:
            best_lookup[best_key][best_match_idx]['consumed'] = True
            matched_idx.append(idx)
            match_data[idx] = {
                'ted_id': best_match['ted_id'],
                'ted_importe': best_match['importe'],
                'ted_n_ofertas': best_match['n_ofertas'],
                'ted_cpv': best_match['cpv_ted'],
                'ted_cae': best_match['cae_ted'],
                'match_diff_euros': best_diff,
                # Campos nuevos v6.0
                'ted_win_size': best_match.get('win_size', ''),
                'ted_direct_award': best_match.get('direct_award', ''),
                'ted_sme_part': best_match.get('sme_part', ''),
                'ted_buyer_legal_type': best_match.get('buyer_legal_type', ''),
                'ted_duration': best_match.get('duration_lot', np.nan),
                'ted_award_criterion': best_match.get('award_criterion_type', ''),
                'ted_internal_id': best_match.get('internal_id', ''),
            }
    
    # ── 3. Aplicar resultados ──
    df_pipeline['_ted_validated'] = False
    df_pipeline.loc[matched_idx, '_ted_validated'] = True
    
    # Columnas de enriquecimiento
    enrich_cols = {
        '_ted_n_ofertas': np.nan,
        '_ted_cpv': '',
        '_ted_id': '',
        '_ted_win_size': '',
        '_ted_direct_award': '',
        '_ted_sme_part': '',
        '_ted_buyer_legal_type': '',
        '_ted_duration': np.nan,
        '_ted_award_criterion': '',
        '_ted_internal_id': '',
    }
    for col, default in enrich_cols.items():
        df_pipeline[col] = default
    
    for idx, data in match_data.items():
        df_pipeline.loc[idx, '_ted_n_ofertas'] = pd.to_numeric(
            data['ted_n_ofertas'], errors='coerce'
        )
        df_pipeline.loc[idx, '_ted_cpv'] = str(data['ted_cpv'])
        df_pipeline.loc[idx, '_ted_id'] = str(data['ted_id'])
        df_pipeline.loc[idx, '_ted_win_size'] = str(data.get('ted_win_size', ''))
        df_pipeline.loc[idx, '_ted_direct_award'] = str(data.get('ted_direct_award', ''))
        df_pipeline.loc[idx, '_ted_sme_part'] = str(data.get('ted_sme_part', ''))
        df_pipeline.loc[idx, '_ted_buyer_legal_type'] = str(data.get('ted_buyer_legal_type', ''))
        df_pipeline.loc[idx, '_ted_duration'] = pd.to_numeric(
            data.get('ted_duration', np.nan), errors='coerce'
        )
        df_pipeline.loc[idx, '_ted_award_criterion'] = str(data.get('ted_award_criterion', ''))
        df_pipeline.loc[idx, '_ted_internal_id'] = str(data.get('ted_internal_id', ''))
    
    n_matched = len(matched_idx)
    _log(f"  ✅ Contratos validados por TED: {n_matched:,}")
    
    # ── 4. Detectar MISSING IN TED ──
    above_threshold = df_pipeline[
        (df_pipeline['_imp_adj'] >= TEDConfig.EU_THRESHOLD_MIN) &
        (~df_pipeline['_es_menor']) &
        (~df_pipeline['_ted_validated']) &
        (df_pipeline['_nif'].notna()) &
        (df_pipeline['_imp_adj'].notna())
    ].copy()
    
    df_pipeline['_ted_missing'] = False
    
    if len(above_threshold) > 0:
        missing_mask = (above_threshold['_imp_adj'] >= TEDConfig.EU_THRESHOLD_MIN)
        
        if '_es_emergencia' in above_threshold.columns:
            missing_mask = missing_mask & (~above_threshold['_es_emergencia'])
        
        missing_idx = above_threshold.loc[missing_mask].index
        df_pipeline.loc[missing_idx, '_ted_missing'] = True
        
        n_missing = len(missing_idx)
        total_above = len(above_threshold)
        _log(f"  ⚠️ Contratos ≥{TEDConfig.EU_THRESHOLD_MIN:,}€ no-menores sin match TED: "
             f"{n_missing:,} de {total_above:,} ({n_missing/max(total_above,1)*100:.1f}%)")
        
        df_missing = df_pipeline.loc[missing_idx].copy()
        
        if len(df_missing) > 0:
            df_missing['umbral_ue_aplicable'] = TEDConfig.EU_THRESHOLD_MIN
            df_missing['exceso_sobre_umbral'] = df_missing['_imp_adj'] - TEDConfig.EU_THRESHOLD_MIN
            df_missing = df_missing.sort_values('_imp_adj', ascending=False)
            
            if R:
                R.subsection(f"TOP 30 MISSING IN TED [{src}]")
                R.log(f"  {'#':<3} {'Importe':>13} {'Órgano':<35} {'NIF':<12} {'Adj.':<25}")
                R.log(f"  {'-'*95}")
                for i, (_, r) in enumerate(df_missing.head(30).iterrows()):
                    R.log(f"  {i+1:<3} {r['_imp_adj']:>13,.0f}€ "
                          f"{str(r['_organ'])[:34]:<35} "
                          f"{str(r['_nif'])[:11]:<12} "
                          f"{str(r.get('_adj',''))[:24]:<25}")
    else:
        df_missing = pd.DataFrame()
        _log(f"  Sin contratos sobre umbral UE para verificar")
    
    # ── 5. Estadísticas de enriquecimiento ──
    if n_matched > 0:
        ted_ofertas = df_pipeline.loc[matched_idx, '_ted_n_ofertas'].dropna()
        if len(ted_ofertas) > 0:
            _log(f"\n  📊 Enriquecimiento desde TED:")
            _log(f"     Nº ofertas disponible para {len(ted_ofertas):,} contratos")
            _log(f"     Media ofertas (TED): {ted_ofertas.mean():.1f}")
            
            both_mask = (
                df_pipeline['_ted_n_ofertas'].notna() & 
                df_pipeline['_ofertas'].notna()
            )
            if both_mask.sum() > 0:
                pip_of = df_pipeline.loc[both_mask, '_ofertas']
                ted_of = df_pipeline.loc[both_mask, '_ted_n_ofertas']
                corr = pip_of.corr(ted_of)
                diff = (pip_of - ted_of).abs().mean()
                _log(f"     Correlación ofertas pipeline↔TED: {corr:.3f}")
                _log(f"     Diferencia media: {diff:.1f} ofertas")
    
    # ── 6. Resumen ──
    total = len(df_pipeline)
    n_validated = df_pipeline['_ted_validated'].sum()
    n_missing_flag = df_pipeline['_ted_missing'].sum()
    _log(f"\n  Resumen [{src}]:")
    _log(f"    Total contratos: {total:,}")
    _log(f"    TED validated: {n_validated:,} ({n_validated/total*100:.1f}%)")
    _log(f"    Missing in TED: {n_missing_flag:,} ({n_missing_flag/total*100:.1f}%)")
    _log(f"    Sin verificar (bajo umbral/sin NIF): "
         f"{total - n_validated - n_missing_flag:,}")
    
    return df_pipeline, df_missing


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 3: INTEGRACIÓN EN SCORING
# ═══════════════════════════════════════════════════════════════════════════

def integrate_ted_in_scoring(df_scored, entity_col, score_col='score_compuesto'):
    """
    Integra los resultados de cross-validation TED en el scoring.
    """
    if '_ted_missing' not in df_scored.columns:
        return df_scored
    
    if 'pct_missing_ted' not in df_scored.columns:
        return df_scored
    
    if 'pct_ted_validated' in df_scored.columns:
        high_quality = df_scored['pct_ted_validated'] > 50
        if high_quality.any():
            df_scored.loc[high_quality, '_ted_quality_flag'] = True
    
    return df_scored


def add_ted_indicators_to_organ_scoring(grp, ted_col_missing='_ted_missing',
                                         ted_col_validated='_ted_validated'):
    """Calcular indicadores TED para un órgano."""
    indicators = {}
    
    if ted_col_missing in grp.columns:
        n_above_threshold = (grp['_imp_adj'] >= TEDConfig.EU_THRESHOLD_MIN).sum()
        n_missing = grp[ted_col_missing].sum()
        
        if n_above_threshold >= 3:
            indicators['pct_missing_ted'] = n_missing / n_above_threshold * 100
            indicators['n_missing_ted'] = int(n_missing)
            indicators['n_above_eu_threshold'] = int(n_above_threshold)
    
    if ted_col_validated in grp.columns:
        n_validated = grp[ted_col_validated].sum()
        indicators['pct_ted_validated'] = n_validated / len(grp) * 100
        indicators['n_ted_validated'] = int(n_validated)
    
    return indicators


def add_ted_indicators_to_adj_scoring(grp, ted_col_missing='_ted_missing',
                                       ted_col_validated='_ted_validated'):
    """Calcular indicadores TED para un adjudicatario."""
    return add_ted_indicators_to_organ_scoring(grp, ted_col_missing, ted_col_validated)


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 4: SCRIPT DE EJECUCIÓN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """
    Uso:
        python ted_module.py download          # Solo descargar datos
        python ted_module.py validate FILE     # Validar contra pipeline
        python ted_module.py full              # Todo
    """
    import argparse
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    
    parser = argparse.ArgumentParser(description='Módulo TED para pipeline v6.0')
    parser.add_argument('command', choices=['download', 'validate', 'full'],
                       help='Comando a ejecutar')
    parser.add_argument('--years', type=str, default='2010-2025',
                       help='Rango de años (ej: 2015-2024)')
    parser.add_argument('--pipeline-file', type=str, default=None,
                       help='Archivo parquet/csv del pipeline para validar')
    parser.add_argument('--force', action='store_true',
                       help='Re-descargar aunque exista cache')
    parser.add_argument('--method', choices=['csv+api', 'sparql'], default='csv+api',
                       help='Método de descarga')
    
    args = parser.parse_args()
    
    if '-' in args.years:
        y_start, y_end = args.years.split('-')
        years = list(range(int(y_start), int(y_end) + 1))
    else:
        years = [int(y) for y in args.years.split(',')]
    
    if args.command in ('download', 'full'):
        print("\n" + "=" * 70)
        print("  DESCARGA DATOS TED — ESPAÑA")
        print("=" * 70)
        
        if args.method == 'sparql':
            df_ted = download_ted_spain_sparql(years=years)
            if df_ted is not None:
                df_ted = _normalize_ted_data(df_ted)
                output_path = TEDConfig.DATA_DIR / "ted_es_can_sparql.parquet"
                df_ted.to_parquet(output_path, index=False)
                _print_ted_summary(df_ted)
        else:
            df_ted = download_ted_spain(years=years, force_redownload=args.force)
    
    if args.command in ('validate', 'full'):
        if args.pipeline_file:
            print("\n" + "=" * 70)
            print("  CROSS-VALIDATION TED ↔ PIPELINE")
            print("=" * 70)
            
            ted_path = TEDConfig.DATA_DIR / "ted_es_can.parquet"
            if not ted_path.exists():
                print("❌ Primero ejecuta 'download' para obtener datos TED")
                return
            
            df_ted = pd.read_parquet(ted_path)
            
            if args.pipeline_file.endswith('.parquet'):
                df_pipeline = pd.read_parquet(args.pipeline_file)
            else:
                df_pipeline = pd.read_csv(args.pipeline_file)
            
            df_result, df_missing = cross_validate_ted(df_pipeline, df_ted, 'NAC')
            
            output_missing = TEDConfig.OUTPUT_DIR / "v6_0_missing_in_ted.csv"
            if len(df_missing) > 0:
                cols_export = [
                    '_organ', '_nif', '_adj', '_imp_adj', '_fecha_adj',
                    '_cpv', '_es_menor', 'umbral_ue_aplicable',
                    'exceso_sobre_umbral'
                ]
                cols_export = [c for c in cols_export if c in df_missing.columns]
                df_missing[cols_export].to_csv(output_missing, index=False)
                print(f"\n✅ Missing in TED: {output_missing} ({len(df_missing):,} registros)")
            
            print(f"\n📊 Pipeline enriquecido: {df_result['_ted_validated'].sum():,} validados, "
                  f"{df_result['_ted_missing'].sum():,} missing")


if __name__ == '__main__':
    main()