#!/usr/bin/env python3
"""
=============================================================================
DESCARGA COMPLETA DE DATOS ABIERTOS - COMUNITAT VALENCIANA
=============================================================================
Portal: https://dadesobertes.gva.es (CKAN API)
Versión: 4.0 FINAL - Todos los IDs verificados y corregidos
Fecha: Enero 2026

Categorías incluidas (14):
- Contratación pública (2014-2025 + DANA)
- Subvenciones y ayudas (2022-2025 + DANA)
- Presupuestos (2024-2025)
- Convenios (2018-2022)
- Lobbies - Registro REGIA (único en España)
- Empleo (ERE/ERTE 2000-2025, DANA, paro, contratos)
- Siniestralidad laboral (2015-2024)
- Patrimonio (inmuebles GVA, BIC, BRL)
- Entidades (locales, asociaciones)
- Territorio (centros docentes)
- Turismo (hoteles, casas rurales, VUT, campings, albergues, etc.)
- Sanidad (mapa sanitario, mortalidad, centros)
- Transporte (autobús interurbano)
- Paro registrado (estadísticas LABORA)
=============================================================================
"""

import requests
import os
import time
import argparse
from pathlib import Path
from datetime import datetime

# Configuración (P2: output under tmp/; LICITACIONES_TMP_DIR or repo tmp)
BASE_URL = "https://dadesobertes.gva.es"
API_URL = f"{BASE_URL}/api/3/action/package_show"
_repo_root = Path(__file__).resolve().parent.parent
_tmp_base = Path(os.environ.get("LICITACIONES_TMP_DIR", _repo_root / "tmp"))
OUTPUT_DIR = _tmp_base / "valencia_datos"

# ============================================================================
# CATÁLOGO COMPLETO DE DATASETS - IDs VERIFICADOS Y CORREGIDOS
# ============================================================================

DATASETS = {
    # ------------------------------------------------------------------------
    # CONTRATACIÓN PÚBLICA
    # Registro Oficial de Contratos de la Generalitat (REGCON)
    # ------------------------------------------------------------------------
    "contratacion": [
        "eco-gvo-contratos-2014",
        "eco-gvo-contratos-2015",
        "eco-gvo-contratos-2016",
        "eco-gvo-contratos-2017",
        "eco-gvo-contratos-2018",
        "eco-gvo-contratos-2019",
        "eco-gvo-contratos-2020",
        "eco-gvo-contratos-2021",
        "eco-gvo-contratos-2022",
        "eco-gvo-contratos-2023",
        "eco-gvo-contratos-2024",
        "eco-gvo-contratos-2025",
        "eco-contratos-dana",                    # Contratos relacionados con DANA
    ],
    
    # ------------------------------------------------------------------------
    # SUBVENCIONES Y AYUDAS
    # Ayudas concedidas por la Generalitat (mensual)
    # ------------------------------------------------------------------------
    "subvenciones": [
        "eco-gvo-subv-2022",
        "eco-gvo-subv-2023",
        "eco-gvo-subv-2024",
        "eco-gvo-subv-2025",
        "eco-ayudas-dana",                       # Subvenciones DANA (no familias)
        "eco-pmp-subvenciones",                  # Periodo medio pago subvenciones
    ],
    
    # ------------------------------------------------------------------------
    # PRESUPUESTOS
    # Ejecución presupuestaria (solo disponible 2024-2025)
    # ------------------------------------------------------------------------
    "presupuestos": [
        "sec-nefis-visor-2024",
        "sec-nefis-visor-2025",
    ],
    
    # ------------------------------------------------------------------------
    # CONVENIOS
    # Convenios suscritos por la GVA (2018-2022, 2023+ no disponibles)
    # ------------------------------------------------------------------------
    "convenios": [
        "gob-convenios-2018",
        "gob-convenios-2019",
        "gob-convenios-2020",
        "gob-convenios-2021",
        "gob-convenios-2022",
    ],
    
    # ------------------------------------------------------------------------
    # LOBBIES - REGISTRO REGIA
    # Registro de Grupos de Interés (único en España a nivel autonómico)
    # ------------------------------------------------------------------------
    "lobbies": [
        "sec-regia-actividades",                 # Actividades de influencia
        "sec-regia-grupos",                      # Grupos de interés registrados
    ],
    
    # ------------------------------------------------------------------------
    # EMPLEO Y MERCADO LABORAL
    # ERE/ERTE, DANA, contratos
    # ------------------------------------------------------------------------
    "empleo": [
        # ERE/ERTE histórico (2000-2025)
        "tra-eres-ertes-v2-2025",
        "tra-eres-ertes-v2-2024",
        # ERTE DANA
        "emp-erte-dana-cv",                      # ERTE DANA detallado
        "emp-erte-dana-pob",                     # ERTE DANA por poblaciones
        "emp-erte-dana-agr",                     # ERTE DANA agregado
        # Estadísticas de contratación por año (LABORA)
        "tra-ocu-contratos-2024",
        "tra-ocu-contratos-2023",
        "tra-ocu-contratos-2022",
    ],
    
    # ------------------------------------------------------------------------
    # PARO REGISTRADO
    # Estadísticas de demandantes (LABORA)
    # ------------------------------------------------------------------------
    "paro": [
        "datos-de-paro-en-la-comunitat-valenciana",
        "datos-del-paro-en-la-comunidad-valenciana-2024",
        "tra-reg-paro-2024",                     # Demandantes activos parados
        "tra-reg-paro-2023",
        "tra-reg-paro-2022",
        "tra-reg-paro-2021",
        "tra-reg-paro-2020",
        "tra-reg-paro-2019",
        "tra-reg-paro-2018",
        "tra-reg-paro-2017",
    ],
    
    # ------------------------------------------------------------------------
    # SINIESTRALIDAD LABORAL
    # Accidentes de trabajo (2015-2024)
    # ------------------------------------------------------------------------
    "siniestralidad": [
        "tra-accidentes-2024",
        "tra-accidentes-2023",
        "tra-accidentes-2022",
        "tra-accidentes-2021",
        "tra-accidentes-2020",
        "tra-accidentes-2019",
        "tra-accidentes-2018",
        "tra-accidentes-2017",
        "tra-accidentes-2016",
        "tra-accidentes-2015",
    ],
    
    # ------------------------------------------------------------------------
    # PATRIMONIO Y BIENES
    # Inmuebles de la Generalitat, BIC, BRL
    # ------------------------------------------------------------------------
    "patrimonio": [
        "hac-bie-inm",                           # Bienes inmuebles GVA
        "hac-bie-inmuebles",                     # BIC y BRL inmuebles
        "hac-bie-inmateriales",                  # Bienes inmateriales culturales
    ],
    
    # ------------------------------------------------------------------------
    # ENTIDADES Y ASOCIACIONES
    # ------------------------------------------------------------------------
    "entidades": [
        "sec-mapets",                            # Entidades locales CV
        "soc-asociaciones",                      # Asociaciones CV
    ],
    
    # ------------------------------------------------------------------------
    # TERRITORIO Y EDUCACIÓN
    # ------------------------------------------------------------------------
    "territorio": [
        "edu-centros",                           # Centros docentes CV
    ],
    
    # ------------------------------------------------------------------------
    # TURISMO - IDs CORREGIDOS Y COMPLETOS
    # Registro de empresas turísticas
    # ------------------------------------------------------------------------
    "turismo": [
        # Datos actualizados diariamente (tur-gestur-*)
        "tur-gestur-vt",                         # Viviendas uso turístico
        "tur-gestur-cr",                         # Casas rurales
        "tur-gestur-ca",                         # Campings (actualizado)
        "tur-gestur-ap",                         # Áreas de pernocta
        "tur-gestur-afp",                        # Acampada finca particular
        "tur-gestur-agv",                        # Agencias de viajes (actualizado)
        "tur-gestur-alb",                        # Albergues turísticos (CORREGIDO)
        # Datos históricos/semanales
        "dades-turisme-hotels-comunitat-valenciana",          # Hoteles (incluye hostales)
        "dades-turisme-campings-comunitat-valenciana",        # Campings histórico
        "dades-turisme-allotjament-rural-comunitat-valenciana", # Alojamiento rural
        "dades-turisme-agencies-viatges-comunitat-valenciana",  # Agencias histórico
        "dades-turisme-habitatges-comunitat-valenciana-2025",   # VUT 2025
        "dades-turisme-actiu-comunitat-valenciana",           # Turismo activo
    ],
    
    # ------------------------------------------------------------------------
    # SANIDAD
    # Mapa sanitario, mortalidad, centros
    # ------------------------------------------------------------------------
    "sanidad": [
        "sanidad-sip",                           # Mapa sanitario (CSV con tramos INE)
        "san-reg-centros-2020",                  # Registro de centros sanitarios
        "sal-tm-cv",                             # TAE mortalidad CV
        "sal-tmb-cv",                            # Tasa bruta mortalidad
    ],
    
    # ------------------------------------------------------------------------
    # TRANSPORTE
    # Autobús interurbano, rutas
    # ------------------------------------------------------------------------
    "transporte": [
        "tra-hyr-atmv-horaris-i-rutes",          # Itinerarios y horarios autobús
    ],
}


def get_dataset_info(dataset_id):
    """Obtiene información del dataset vía API CKAN"""
    try:
        response = requests.get(API_URL, params={"id": dataset_id}, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("result", {})
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status == 404:
            print(f"  ⚠️ No encontrado (404): {dataset_id}")
        elif status == 403:
            print(f"  🔒 Acceso denegado (403): {dataset_id}")
        else:
            print(f"  ⚠️ Error HTTP {status}: {dataset_id}")
    except requests.exceptions.Timeout:
        print(f"  ⏱️ Timeout: {dataset_id}")
    except Exception as e:
        print(f"  ⚠️ Error: {e}")
    return None


def download_file(url, filepath):
    """Descarga un archivo con manejo de errores"""
    try:
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        return True, size_mb
    except requests.exceptions.Timeout:
        return False, "Timeout (5 min)"
    except Exception as e:
        return False, str(e)[:50]


def sanitize_filename(name):
    """Limpia nombre de archivo para Windows/Linux"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    while '  ' in name:
        name = name.replace('  ', ' ')
    return name[:200].strip()


def process_dataset(dataset_id, category_dir):
    """Procesa un dataset y descarga sus recursos CSV"""
    print(f"\n📦 {dataset_id}")
    
    info = get_dataset_info(dataset_id)
    if not info:
        return 0, 0
    
    resources = info.get("resources", [])
    
    # Filtrar recursos CSV
    csv_resources = [r for r in resources if r.get("format", "").upper() in ["CSV", "TEXT/CSV"]]
    
    if not csv_resources:
        print(f"  ⚠️ No hay recursos CSV (puede tener JSON/XML)")
        return 0, 0
    
    downloaded = 0
    total_size = 0
    
    for resource in csv_resources:
        url = resource.get("url")
        name = resource.get("name", "data")
        
        if not url:
            continue
        
        filename = sanitize_filename(name)
        if not filename.lower().endswith('.csv'):
            filename += '.csv'
        
        filepath = category_dir / filename
        
        # Verificar si ya existe
        if filepath.exists():
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  ⏭️ Ya existe: {filename}")
            total_size += size_mb
            downloaded += 1
            continue
        
        # Descargar
        print(f"  ⬇️ {filename}...", end=" ", flush=True)
        success, result = download_file(url, filepath)
        
        if success:
            print(f"✅ ({result:.1f} MB)")
            downloaded += 1
            total_size += result
        else:
            print(f"❌ {result}")
            if filepath.exists():
                filepath.unlink()
        
        time.sleep(0.3)
    
    return downloaded, total_size


def main():
    parser = argparse.ArgumentParser(description="Valencia: descarga datos CKAN (Dades Obertes GVA)")
    parser.add_argument("--categories", type=str, default=None, help="Comma-separated categories (e.g. contratacion) for small run")
    args = parser.parse_args()
    
    categories_filter = [c.strip() for c in args.categories.split(",")] if args.categories else None
    if categories_filter:
        datasets_to_use = {k: v for k, v in DATASETS.items() if k in categories_filter}
    else:
        datasets_to_use = DATASETS
    
    start_time = datetime.now()
    
    print("=" * 70)
    print("DESCARGA COMPLETA DE DATOS ABIERTOS - COMUNITAT VALENCIANA")
    print("=" * 70)
    print(f"Portal: {BASE_URL}")
    print(f"Destino: {OUTPUT_DIR.absolute()}")
    print(f"Categorías: {len(datasets_to_use)}")
    print(f"Datasets totales: {sum(len(v) for v in datasets_to_use.values())}")
    print(f"Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    total_files = 0
    total_size = 0
    stats = {}
    errors = []
    
    for category, datasets in datasets_to_use.items():
        print(f"\n{'=' * 70}")
        print(f"📁 CATEGORÍA: {category.upper()}")
        print("=" * 70)
        
        category_dir = OUTPUT_DIR / category
        category_dir.mkdir(parents=True, exist_ok=True)
        
        cat_files = 0
        cat_size = 0
        
        for dataset_id in datasets:
            files, size = process_dataset(dataset_id, category_dir)
            if files == 0:
                errors.append(dataset_id)
            cat_files += files
            cat_size += size
        
        stats[category] = {"files": cat_files, "size": cat_size}
        total_files += cat_files
        total_size += cat_size
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Resumen final
    print("\n" + "=" * 70)
    print("✅ DESCARGA COMPLETADA")
    print("=" * 70)
    print(f"   Archivos descargados: {total_files}")
    print(f"   Tamaño total: {total_size:.1f} MB ({total_size/1024:.2f} GB)")
    print(f"   Duración: {duration}")
    print(f"   Ubicación: {OUTPUT_DIR.absolute()}")
    
    # Mostrar estructura
    print("\n📂 ESTRUCTURA FINAL:")
    for category, data in sorted(stats.items()):
        if data["files"] > 0:
            print(f"  📁 {category}/")
            print(f"      {data['files']} archivos, {data['size']:.1f} MB")
    
    # Mostrar errores
    if errors:
        print(f"\n⚠️ DATASETS SIN CSV ({len(errors)}):")
        for e in errors:
            print(f"   - {e}")
    
    # Guardar log
    log_file = OUTPUT_DIR / "descarga_log.txt"
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"DESCARGA DATOS ABIERTOS - COMUNITAT VALENCIANA\n")
        f.write(f"{'=' * 50}\n")
        f.write(f"Fecha: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duración: {duration}\n")
        f.write(f"Total archivos: {total_files}\n")
        f.write(f"Total tamaño: {total_size:.1f} MB ({total_size/1024:.2f} GB)\n\n")
        
        f.write("DETALLE POR CATEGORÍA:\n")
        f.write("-" * 50 + "\n")
        for category, data in sorted(stats.items()):
            f.write(f"{category}: {data['files']} archivos, {data['size']:.1f} MB\n")
        
        if errors:
            f.write(f"\nDATASETS SIN CSV:\n")
            f.write("-" * 50 + "\n")
            for e in errors:
                f.write(f"  - {e}\n")
    
    print(f"\n📝 Log guardado: {log_file}")
    
    # Instrucciones siguientes
    print("\n" + "=" * 70)
    print("📌 SIGUIENTE PASO: Convertir a Parquet")
    print("=" * 70)
    print("   python ccaa_valencia_parquet.py")
    print("   (Reducirá ~5 GB a ~500-800 MB)")


if __name__ == "__main__":
    main()