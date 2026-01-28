#!/usr/bin/env python3
"""
================================================================================
CATALUÑA CONTRATACIÓN PÚBLICA Y SECTOR PÚBLICO - DESCARGA COMPLETA v2.0
================================================================================
TODOS los datasets de:
- Portal Transparencia Catalunya (Socrata)
- Open Data Barcelona (CKAN)
- Datos adicionales Gencat

Incluye: Contratación, Subvenciones, Convenios, Presupuestos, Sector Público,
         Retribuciones, Entes locales, etc.
================================================================================
"""

import os
import time
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys
import logging

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

OUTPUT_DIR = "catalunya_datos_completos"
FORCE_DOWNLOAD = False

# =============================================================================
# PORTAL TRANSPARENCIA CATALUNYA (Socrata)
# URL base: https://analisi.transparenciacatalunya.cat
# =============================================================================

SOCRATA_BASE = "https://analisi.transparenciacatalunya.cat"

# TODOS los datasets organizados por categoría
SOCRATA_DATASETS = {
    # =========================================================================
    # CONTRATACIÓN PÚBLICA
    # =========================================================================
    'hb6v-jcbf': ('01_contratacion/registro_publico_contratos', 'Inscripciones Registro Público de Contratos'),
    'ybgg-dgi6': ('01_contratacion/publicaciones_pscp', 'Publicaciones Plataforma Servicios Contratación Pública'),
    'a23c-d6vp': ('01_contratacion/licitaciones_adjudicaciones_curso', 'Licitaciones y adjudicaciones en curso'),
    'u9d7-egbx': ('01_contratacion/contratacion_programada', 'Contratación programada Generalitat'),
    '5fq6-4v39': ('01_contratacion/contratacion_emergencia_covid', 'Contratación emergencia COVID-19'),
    'dkrd-id95': ('01_contratacion/resoluciones_tribunal', 'Resoluciones Tribunal Catalán Contratos'),
    'nn7v-4yxe': ('01_contratacion/adjudicaciones_generalitat', 'Adjudicaciones Generalitat Catalunya'),
    '8idu-wkjv': ('01_contratacion/fase_ejecucion', 'Publicaciones fase ejecución'),
    'ydq4-xy5b': ('01_contratacion/contratos_menores_generalitat', 'Contratos Menores Generalitat'),
    'jxvs-kzbu': ('01_contratacion/adjudicaciones_contractuales_quincenal', 'Adjudicaciones contractuales quincenales'),
    
    # =========================================================================
    # SUBVENCIONES Y AYUDAS (RAISC)
    # =========================================================================
    's9xt-n979': ('02_subvenciones/raisc_concesiones', 'Concesiones RAISC (Registro subvenciones Catalunya)'),
    'khxn-nv6a': ('02_subvenciones/raisc_convocatorias', 'Convocatorias RAISC'),
    '3gku-b36y': ('02_subvenciones/convocatorias_subvenciones', 'Convocatorias de subvenciones'),
    
    # =========================================================================
    # CONVENIOS Y ENCARGOS DE GESTIÓN
    # =========================================================================
    'exh2-diuf': ('03_convenios/registro_convenios', 'Registre de Convenis de Col·laboració i Cooperació'),
    
    # =========================================================================
    # PRESUPUESTOS Y FINANZAS
    # =========================================================================
    'w2cu-rmuv': ('04_presupuestos/evolucion_presupuestos', 'Evolución presupuestos Generalitat'),
    'yd9k-7jhw': ('04_presupuestos/presupuestos_aprobados', 'Presupuestos aprobados Generalitat'),
    'ajns-4mi7': ('04_presupuestos/ejecucion_mensual_despeses', 'Ejecución mensual presupuesto - Despeses'),
    '42am-vra2': ('04_presupuestos/ejecucion_mensual_ingressos', 'Ejecución mensual presupuesto - Ingressos'),
    'wwmk-zys7': ('04_presupuestos/ejecucion_consolidado_sector_publico', 'Ejecución consolidado sector público'),
    'nuym-4erw': ('04_presupuestos/despeses_2019', 'Despeses 2019'),
    
    # =========================================================================
    # SECTOR PÚBLICO Y ENTIDADES
    # =========================================================================
    'gr39-ik6u': ('05_sector_publico/registro_sector_publico', 'Registro sector público Generalitat'),
    'abb9-zcju': ('05_sector_publico/codigos_departamentos_ens', 'Códigos departamentos, ens y organismos'),
    '6nei-4b44': ('05_sector_publico/ens_locals_catalunya', 'Dades generals dels ens locals de Catalunya'),
    'vevv-8fvw': ('05_sector_publico/ajuntaments', 'Dades d\'ens locals - Ajuntaments'),
    'twhi-gz6x': ('05_sector_publico/ajuntaments_catalunya', 'Ajuntaments de Catalunya'),
    'nm3n-3vbj': ('05_sector_publico/composicio_plens', 'Composició plens ajuntaments, consells, diputacions'),
    
    # =========================================================================
    # RECURSOS HUMANOS Y RETRIBUCIONES
    # =========================================================================
    'x9au-abcn': ('06_recursos_humanos/altos_cargos_retribuciones', 'Alts càrrecs i retribucions'),
    'b4zx-cfga': ('06_recursos_humanos/retribuciones_funcionarios', 'Retribucions personal funcionari'),
    'abap-7r6z': ('06_recursos_humanos/retribuciones_laboral', 'Retribucions personal laboral'),
    '3b6m-hrxk': ('06_recursos_humanos/taules_retributives_alts_carrecs', 'Taules retributives alts càrrecs'),
    'a2hm-uzyj': ('06_recursos_humanos/convocatorias_personal', 'Convocatòries de Personal'),
    '6p3u-9s8y': ('06_recursos_humanos/enunciats_examens', 'Enunciats exàmens accés funció pública'),
    
    # =========================================================================
    # GEOGRAFÍA Y TERRITORIO
    # =========================================================================
    '9aju-tpwc': ('07_territorio/municipis_catalunya_geo', 'Municipis Catalunya Geo'),
    'x5xm-w9x7': ('07_territorio/municipis_espanya', 'Municipis d\'Espanya'),
}

# =============================================================================
# OPEN DATA BARCELONA (CKAN)
# =============================================================================

BCN_BASE = "https://opendata-ajuntament.barcelona.cat"

BCN_DATASETS = {
    # Contratación
    'perfil-contractant': 'perfil_contratante',
    'contractes-menors': 'contratos_menores',
    'relacio-contractistes': 'contratistas',
    'resums-trimestrals-contractacio': 'resumen_trimestral',
    'modificacions-de-contractes': 'modificaciones_contratos',
    'contractes-menors-autoritzacio-generica': 'contratos_menores_autorizacion',
}

# =============================================================================
# LOGGING Y ESTADÍSTICAS
# =============================================================================

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__).info

stats = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0, 'records': 0}

# Session con headers
session = requests.Session()
session.headers['User-Agent'] = 'BQuantFinance/2.0 (Gerard BQuant - Investigación académica)'

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def download_with_progress(url, path, desc="", timeout=600):
    """Descarga un archivo con indicador de progreso"""
    global stats
    
    if not FORCE_DOWNLOAD and path.exists() and path.stat().st_size > 0:
        log(f"⏭️ Skip: {path.name}")
        stats['skipped'] += 1
        return True
    
    path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        r = session.get(url, timeout=timeout, stream=True)
        r.raise_for_status()
        
        total_size = int(r.headers.get('content-length', 0))
        downloaded = 0
        start_time = time.time()
        
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    if downloaded % (1024 * 1024) < 8192:
                        elapsed = time.time() - start_time
                        speed = downloaded / elapsed / 1024 / 1024 if elapsed > 0 else 0
                        
                        if total_size > 0:
                            pct = downloaded / total_size * 100
                            print(f"\r   ⏳ {downloaded/1024/1024:.1f}MB / {total_size/1024/1024:.1f}MB ({pct:.0f}%) - {speed:.1f}MB/s", end='', flush=True)
                        else:
                            print(f"\r   ⏳ {downloaded/1024/1024:.1f}MB - {speed:.1f}MB/s", end='', flush=True)
        
        print()  # Nueva línea
        
        size = path.stat().st_size
        if size == 0:
            path.unlink()
            return False
        
        stats['downloaded'] += 1
        stats['bytes'] += size
        size_str = f"{size/1024:.1f}KB" if size < 1024*1024 else f"{size/1024/1024:.2f}MB"
        log(f"✅ {desc}: {path.name} ({size_str})")
        return True
        
    except Exception as e:
        print()
        log(f"❌ Error {desc}: {e}")
        stats['failed'] += 1
        return False


def count_csv_records(path):
    """Cuenta registros en CSV"""
    try:
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            for sep in [',', ';', '\t']:
                try:
                    df = pd.read_csv(path, encoding=encoding, sep=sep, on_bad_lines='skip', low_memory=False, nrows=None)
                    if len(df.columns) > 1:
                        return len(df)
                except:
                    continue
        return 0
    except:
        return 0


# =============================================================================
# DESCARGAS SOCRATA
# =============================================================================

def download_socrata_datasets(output_dir):
    """Descarga todos los datasets de Socrata"""
    log("\n" + "="*70)
    log("📥 PORTAL TRANSPARENCIA CATALUNYA (Socrata)")
    log(f"   {len(SOCRATA_DATASETS)} datasets a descargar")
    log("="*70)
    
    socrata_dir = output_dir / "01_transparencia_catalunya"
    socrata_dir.mkdir(exist_ok=True)
    
    for i, (dataset_id, (subpath, descripcion)) in enumerate(SOCRATA_DATASETS.items(), 1):
        log(f"\n[{i}/{len(SOCRATA_DATASETS)}] 📊 {descripcion}")
        
        # Crear subdirectorio
        full_path = socrata_dir / subpath
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Descargar CSV
        url_csv = f"{SOCRATA_BASE}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"
        path_csv = Path(str(full_path) + ".csv")
        
        if download_with_progress(url_csv, path_csv, descripcion):
            n = count_csv_records(path_csv)
            if n > 0:
                log(f"   📝 {n:,} registros")
                stats['records'] += n
        
        # Pequeña pausa para no sobrecargar
        time.sleep(1)


def download_socrata_metadata(output_dir):
    """Descarga metadatos de todos los datasets"""
    log("\n" + "="*70)
    log("📥 METADATOS SOCRATA")
    log("="*70)
    
    meta_dir = output_dir / "01_transparencia_catalunya" / "_metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)
    
    for dataset_id, (subpath, _) in SOCRATA_DATASETS.items():
        filename = subpath.split('/')[-1]
        url = f"{SOCRATA_BASE}/api/views/{dataset_id}.json"
        path = meta_dir / f"{filename}_metadata.json"
        
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(r.json(), f, ensure_ascii=False, indent=2)
        except:
            pass
        
        time.sleep(0.3)
    
    log(f"✅ Metadatos guardados en {meta_dir}")


# =============================================================================
# DESCARGAS BARCELONA
# =============================================================================

def download_barcelona_datasets(output_dir):
    """Descarga datasets de Open Data Barcelona"""
    log("\n" + "="*70)
    log("📥 OPEN DATA BARCELONA (CKAN)")
    log(f"   {len(BCN_DATASETS)} datasets a descargar")
    log("="*70)
    
    bcn_dir = output_dir / "02_barcelona"
    bcn_dir.mkdir(exist_ok=True)
    
    for i, (dataset_slug, local_name) in enumerate(BCN_DATASETS.items(), 1):
        log(f"\n[{i}/{len(BCN_DATASETS)}] 📊 {dataset_slug}")
        
        try:
            api_url = f"{BCN_BASE}/data/api/3/action/package_show?id={dataset_slug}"
            r = session.get(api_url, timeout=30)
            
            if r.status_code == 200:
                data = r.json()
                if data.get('success') and data.get('result'):
                    resources = data['result'].get('resources', [])
                    
                    dataset_dir = bcn_dir / local_name
                    dataset_dir.mkdir(exist_ok=True)
                    
                    for resource in resources:
                        res_url = resource.get('url', '')
                        res_name = resource.get('name', 'unknown')
                        res_format = resource.get('format', '').lower()
                        
                        if res_format in ['csv', 'xlsx', 'xls', 'json']:
                            safe_name = "".join(c if c.isalnum() or c in '._-' else '_' for c in res_name)
                            if not safe_name.endswith(f'.{res_format}'):
                                safe_name = f"{safe_name}.{res_format}"
                            
                            path = dataset_dir / safe_name
                            if download_with_progress(res_url, path, res_name):
                                if res_format == 'csv':
                                    n = count_csv_records(path)
                                    if n > 0:
                                        log(f"   📝 {n:,} registros")
                                        stats['records'] += n
                        
                        time.sleep(0.5)
            else:
                log(f"   ⚠️ HTTP {r.status_code}")
            
        except Exception as e:
            log(f"   ⚠️ Error: {e}")
        
        time.sleep(1)


# =============================================================================
# DATOS ADICIONALES GENCAT
# =============================================================================

def download_gencat_adicional(output_dir):
    """Descarga datos adicionales de Gencat"""
    log("\n" + "="*70)
    log("📥 DATOS ADICIONALES GENCAT")
    log("="*70)
    
    gencat_dir = output_dir / "03_gencat_adicional"
    gencat_dir.mkdir(exist_ok=True)
    
    # Adjudicaciones históricas por año
    adjudicaciones_url = "https://contractacio.gencat.cat/ca/principis/transparencia-bones-practiques/transparencia/adjudicacions-contractuals-plataforma/"
    
    log(f"\n📋 Nota: Datos históricos adicionales disponibles en:")
    log(f"   {adjudicaciones_url}")
    
    # Guardar info
    with open(gencat_dir / "FUENTES_ADICIONALES.txt", 'w', encoding='utf-8') as f:
        f.write("""FUENTES ADICIONALES DE DATOS DE CONTRATACIÓN

1. Adjudicaciones Contractuales Históricas:
   https://contractacio.gencat.cat/ca/principis/transparencia-bones-practiques/transparencia/adjudicacions-contractuals-plataforma/
   
2. Plataforma de Serveis de Contractació Pública:
   https://contractaciopublica.gencat.cat/
   
3. Registre Públic de Contractes:
   https://contractacio.gencat.cat/ca/serveis/registre-public-contractes/

4. Junta Consultiva de Contractació Administrativa:
   https://contractacio.gencat.cat/ca/jcca/

5. Tribunal Català de Contractes del Sector Públic:
   https://tribunalcontractes.gencat.cat/
""")
    
    log(f"✅ Info guardada en {gencat_dir}/FUENTES_ADICIONALES.txt")


# =============================================================================
# MAIN
# =============================================================================

def main():
    start = time.time()
    
    print("\n" + "="*70)
    print("🚀 CATALUÑA - DESCARGA COMPLETA DE DATOS PÚBLICOS v2.0")
    print("="*70)
    print(f"""
Descargando TODOS los datos disponibles:
- {len(SOCRATA_DATASETS)} datasets del Portal Transparencia Catalunya
- {len(BCN_DATASETS)} datasets de Open Data Barcelona
- Datos adicionales de Gencat

Categorías incluidas:
✓ Contratación pública
✓ Subvenciones y ayudas (RAISC)
✓ Convenios y encargos de gestión
✓ Presupuestos y finanzas
✓ Sector público y entidades
✓ Recursos humanos y retribuciones
✓ Geografía y territorio
""")
    print("="*70)
    
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True)
    log(f"📁 {output_dir.absolute()}")
    
    # === DESCARGAS ===
    download_socrata_datasets(output_dir)
    download_socrata_metadata(output_dir)
    download_barcelona_datasets(output_dir)
    download_gencat_adicional(output_dir)
    
    # === RESUMEN ===
    elapsed = time.time() - start
    
    print("\n" + "="*70)
    log("📊 RESUMEN FINAL")
    print("="*70)
    log(f"✅ Archivos descargados: {stats['downloaded']}")
    log(f"⏭️ Archivos saltados: {stats['skipped']}")
    log(f"❌ Archivos fallidos: {stats['failed']}")
    log(f"📝 Registros totales: {stats['records']:,}")
    log(f"💾 Tamaño total: {stats['bytes']/1024/1024:.2f} MB")
    log(f"⏱️ Tiempo: {elapsed/60:.1f} minutos")
    print("="*70)
    
    # Informe
    with open(output_dir / "INFORME.md", 'w', encoding='utf-8') as f:
        f.write(f"""# CATALUÑA - DATOS PÚBLICOS COMPLETOS
## Informe de descarga v2.0

**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M')}

### Estadísticas
- Archivos descargados: {stats['downloaded']}
- Archivos saltados: {stats['skipped']}
- Archivos fallidos: {stats['failed']}
- Registros totales: {stats['records']:,}
- Tamaño total: {stats['bytes']/1024/1024:.2f} MB
- Tiempo: {elapsed/60:.1f} minutos

### Estructura de directorios
```
{OUTPUT_DIR}/
├── 01_transparencia_catalunya/
│   ├── 01_contratacion/
│   │   ├── registro_publico_contratos.csv  ⭐ PRINCIPAL
│   │   ├── publicaciones_pscp.csv
│   │   ├── licitaciones_adjudicaciones_curso.csv
│   │   ├── contratacion_programada.csv
│   │   ├── contratacion_emergencia_covid.csv
│   │   ├── resoluciones_tribunal.csv
│   │   ├── adjudicaciones_generalitat.csv
│   │   ├── fase_ejecucion.csv
│   │   ├── contratos_menores_generalitat.csv
│   │   └── adjudicaciones_contractuales_quincenal.csv
│   ├── 02_subvenciones/
│   │   ├── raisc_concesiones.csv
│   │   ├── raisc_convocatorias.csv
│   │   └── convocatorias_subvenciones.csv
│   ├── 03_convenios/
│   │   └── registro_convenios.csv
│   ├── 04_presupuestos/
│   │   ├── evolucion_presupuestos.csv
│   │   ├── presupuestos_aprobados.csv
│   │   ├── ejecucion_mensual_despeses.csv
│   │   ├── ejecucion_mensual_ingressos.csv
│   │   └── ejecucion_consolidado_sector_publico.csv
│   ├── 05_sector_publico/
│   │   ├── registro_sector_publico.csv
│   │   ├── codigos_departamentos_ens.csv
│   │   ├── ens_locals_catalunya.csv
│   │   ├── ajuntaments.csv
│   │   └── composicio_plens.csv
│   ├── 06_recursos_humanos/
│   │   ├── altos_cargos_retribuciones.csv
│   │   ├── retribuciones_funcionarios.csv
│   │   ├── retribuciones_laboral.csv
│   │   └── convocatorias_personal.csv
│   ├── 07_territorio/
│   │   ├── municipis_catalunya_geo.csv
│   │   └── municipis_espanya.csv
│   └── _metadata/
├── 02_barcelona/
│   ├── perfil_contratante/
│   ├── contratos_menores/
│   ├── contratistas/
│   ├── resumen_trimestral/
│   ├── modificaciones_contratos/
│   └── contratos_menores_autorizacion/
└── 03_gencat_adicional/
```

### Datasets por categoría

#### Contratación Pública ({len([k for k in SOCRATA_DATASETS if '01_contratacion' in SOCRATA_DATASETS[k][0]])} datasets)
- Registro Público de Contratos (hb6v-jcbf) - PRINCIPAL
- Publicaciones PSCP (ybgg-dgi6)
- Licitaciones y adjudicaciones en curso (a23c-d6vp)
- Contratación programada (u9d7-egbx)
- Emergencia COVID-19 (5fq6-4v39)
- Resoluciones Tribunal (dkrd-id95)
- Adjudicaciones Generalitat (nn7v-4yxe)
- Fase ejecución (8idu-wkjv)
- Contratos menores (ydq4-xy5b)
- Adjudicaciones quincenales (jxvs-kzbu)

#### Subvenciones y Ayudas ({len([k for k in SOCRATA_DATASETS if '02_subvenciones' in SOCRATA_DATASETS[k][0]])} datasets)
- RAISC Concesiones (s9xt-n979)
- RAISC Convocatorias (khxn-nv6a)
- Convocatorias subvenciones (3gku-b36y)

#### Convenios ({len([k for k in SOCRATA_DATASETS if '03_convenios' in SOCRATA_DATASETS[k][0]])} datasets)
- Registro Convenios (exh2-diuf)

#### Presupuestos ({len([k for k in SOCRATA_DATASETS if '04_presupuestos' in SOCRATA_DATASETS[k][0]])} datasets)
- Evolución presupuestos (w2cu-rmuv)
- Presupuestos aprobados (yd9k-7jhw)
- Ejecución mensual despeses (ajns-4mi7)
- Ejecución mensual ingressos (42am-vra2)
- Consolidado sector público (wwmk-zys7)

#### Sector Público ({len([k for k in SOCRATA_DATASETS if '05_sector_publico' in SOCRATA_DATASETS[k][0]])} datasets)
- Registro sector público (gr39-ik6u)
- Códigos departamentos (abb9-zcju)
- Ens locals (6nei-4b44)
- Ajuntaments (vevv-8fvw, twhi-gz6x)
- Composició plens (nm3n-3vbj)

#### Recursos Humanos ({len([k for k in SOCRATA_DATASETS if '06_recursos_humanos' in SOCRATA_DATASETS[k][0]])} datasets)
- Alts càrrecs (x9au-abcn)
- Retribucions funcionaris (b4zx-cfga)
- Retribucions laboral (abap-7r6z)
- Taules retributives (3b6m-hrxk)
- Convocatòries personal (a2hm-uzyj)

### Notas
- Los archivos grandes (>100MB) pueden tardar varios minutos
- El Registro Público de Contratos es el dataset principal de contratación
- RAISC contiene todas las subvenciones de Catalunya
- Los datos de Barcelona solo incluyen el Ayuntamiento y Grupo Municipal
""")
    
    log(f"\n📄 Informe: {output_dir}/INFORME.md")
    
    # Crear índice de archivos
    with open(output_dir / "INDICE.txt", 'w', encoding='utf-8') as f:
        f.write("ÍNDICE DE ARCHIVOS DESCARGADOS\n")
        f.write("="*50 + "\n\n")
        
        for root, dirs, files in os.walk(output_dir):
            level = root.replace(str(output_dir), '').count(os.sep)
            indent = ' ' * 2 * level
            f.write(f'{indent}{os.path.basename(root)}/\n')
            subindent = ' ' * 2 * (level + 1)
            for file in sorted(files):
                filepath = Path(root) / file
                size = filepath.stat().st_size
                size_str = f"{size/1024:.1f}KB" if size < 1024*1024 else f"{size/1024/1024:.2f}MB"
                f.write(f'{subindent}{file} ({size_str})\n')
    
    log(f"📋 Índice: {output_dir}/INDICE.txt")


if __name__ == "__main__":
    main()