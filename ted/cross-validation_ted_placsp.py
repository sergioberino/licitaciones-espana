"""
Cross-validation PLACSP ↔ TED
==============================
Cruza el parquet de licitaciones nacionales contra los datos TED
para detectar contratos que deberían estar publicados en TED pero no lo están.

Uso:
    python run_ted_crossvalidation.py

Inputs:
    - nacional/licitaciones_espana.parquet  (PLACSP)
    - data/ted/ted_es_can.parquet           (TED, generado por ted_module.py)

Outputs:
    - data/ted/crossval_matched.parquet     (contratos PLACSP validados por TED)
    - data/ted/crossval_missing.parquet     (contratos sobre umbral UE no encontrados en TED)
    - data/ted/crossval_stats.txt           (resumen estadístico)
"""

import pandas as pd
import numpy as np
import time
from pathlib import Path
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════

PLACSP_PATH = Path("nacional/licitaciones_espana.parquet")
TED_PATH = Path("data/ted/ted_es_can.parquet")
OUTPUT_DIR = Path("data/ted")

# Umbrales UE (2024, sin IVA)
EU_THRESHOLD_MIN = 140_000  # Umbral mínimo (suministros/servicios AGE)

# Tolerancia matching
MATCH_TOLERANCE_PCT = 0.10   # ±10%
MATCH_TOLERANCE_ABS = 5_000  # O ±5000€
MATCH_YEAR_WINDOW = 1        # ±1 año


# ═══════════════════════════════════════════════════════════════════════════
#  1. CARGAR DATOS
# ═══════════════════════════════════════════════════════════════════════════

def load_placsp(path):
    """Carga y filtra PLACSP: solo adjudicaciones con NIF e importe."""
    print(f"\n📥 Cargando PLACSP: {path}")
    df = pd.read_parquet(path)
    print(f"   Total registros: {len(df):,}")
    
    # Solo licitaciones con adjudicación
    mask = (
        (df['tipo_registro'] == 'LICITACION') &
        (df['estado'].isin(['Resuelta', 'Adjudicada'])) &
        (df['nif_adjudicatario'].notna()) &
        (df['importe_adjudicacion'].notna())
    )
    df = df[mask].copy()
    print(f"   Adjudicaciones con NIF + importe: {len(df):,}")
    
    # Limpiar NIF
    df['_nif'] = df['nif_adjudicatario'].astype(str).str.strip().str.upper()
    df['_nif'] = df['_nif'].str.replace(r'^ES[-\s]*', '', regex=True)
    df.loc[df['_nif'].str.len() < 5, '_nif'] = ''
    
    # Importe
    df['_imp_adj'] = pd.to_numeric(df['importe_adjudicacion'], errors='coerce')
    
    # Año
    df['_año'] = pd.to_numeric(df['ano'], errors='coerce')
    
    # Expediente (para matching secundario)
    df['_expediente'] = df['expediente'].fillna('').astype(str).str.strip()
    
    # Fecha adjudicación
    df['_fecha_adj'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce')
    
    # Tipo contrato (para umbrales UE)
    df['_tipo'] = df['tipo_contrato'].astype(str)
    
    # Órgano
    df['_organ'] = df['organo_contratante'].fillna('')
    df['_nif_organ'] = df['nif_organo'].fillna('').astype(str).str.strip().str.upper()
    
    # Contratos menores
    df['_es_menor'] = df['conjunto'] == 'menores'
    
    # Sobre umbral UE
    df['_sobre_umbral_ue'] = df['_imp_adj'] >= EU_THRESHOLD_MIN
    
    # CPV
    df['_cpv'] = df['cpv_principal'].fillna(0).astype(int).astype(str)
    
    n_sobre = df['_sobre_umbral_ue'].sum()
    n_menor = df['_es_menor'].sum()
    print(f"   Sobre umbral UE (≥{EU_THRESHOLD_MIN:,}€): {n_sobre:,}")
    print(f"   Contratos menores: {n_menor:,}")
    print(f"   Rango años: {df['_año'].min():.0f} - {df['_año'].max():.0f}")
    
    return df


def load_ted(path):
    """Carga datos TED normalizados."""
    print(f"\n📥 Cargando TED: {path}")
    df = pd.read_parquet(path)
    print(f"   Total registros: {len(df):,}")
    
    # Asegurar columnas numéricas
    for col in ['importe_ted', 'year', 'number_offers']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Asegurar NIF limpio
    if 'win_nif_clean' not in df.columns:
        df['win_nif_clean'] = df.get('win_nationalid', pd.Series(dtype=str)).fillna('').astype(str).str.strip().str.upper()
        df['win_nif_clean'] = df['win_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)
    
    valid = df['importe_ted'].notna() & (df['importe_ted'] > 0)
    has_nif = df['win_nif_clean'].str.len() >= 5
    print(f"   Con importe: {valid.sum():,}")
    print(f"   Con NIF ganador: {(valid & has_nif).sum():,}")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  2. CROSS-VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

def cross_validate(df_placsp, df_ted):
    """
    Cruza PLACSP contra TED:
    1. Match por NIF + importe ± tolerancia + año ± ventana
    2. Fallback: match por nº expediente
    3. Detecta contratos ≥ umbral UE sin match (Missing in TED)
    """
    print(f"\n{'='*60}")
    print(f"  CROSS-VALIDATION PLACSP ↔ TED")
    print(f"{'='*60}")
    
    t0 = time.time()
    
    # ── Construir índices TED ──
    ted_valid = df_ted[
        (df_ted['importe_ted'].notna()) &
        (df_ted['importe_ted'] > 0)
    ].copy()
    
    # Índice primario: (nif_limpio, año) → entries
    ted_lookup = defaultdict(list)
    # Índice secundario: nº expediente → entries
    ted_lookup_exp = defaultdict(list)
    
    for _, row in ted_valid.iterrows():
        nif = str(row.get('win_nif_clean', '')).strip()
        imp = row['importe_ted']
        yr = row.get('year', np.nan)
        
        entry = {
            'importe': imp,
            'ted_id': str(row.get('ted_notice_id', '')),
            'n_ofertas': row.get('number_offers', np.nan),
            'cpv_ted': str(row.get('cpv', '')),
            'cae_ted': str(row.get('cae_name', '')),
            'win_size': str(row.get('win_size', '')),
            'direct_award': str(row.get('direct_award_justification', '')),
            'sme_part': str(row.get('sme_participation', '')),
            'buyer_legal_type': str(row.get('buyer_legal_type', '')),
            'duration_lot': row.get('duration_lot', np.nan),
            'award_criterion_type': str(row.get('award_criterion_type', '')),
            'internal_id': str(row.get('internal_id_proc', '')),
            'consumed': False,
        }
        
        if nif and len(nif) >= 5 and pd.notna(yr):
            ted_lookup[(nif, int(yr))].append(entry)
        
        exp_id = str(row.get('internal_id_proc', '')).strip()
        if exp_id and len(exp_id) >= 4:
            ted_lookup_exp[exp_id.upper()].append(entry)
    
    print(f"  TED lookup: {len(ted_lookup):,} claves (nif, año)")
    print(f"  TED lookup expediente: {len(ted_lookup_exp):,} claves")
    print(f"  TED registros con importe: {len(ted_valid):,}")
    
    # ── Filtrar PLACSP: solo registros con NIF + importe válido ──
    mask_valid = (
        (df_placsp['_nif'].str.len() >= 5) &
        (df_placsp['_imp_adj'].notna()) &
        (df_placsp['_imp_adj'] > 0) &
        (df_placsp['_año'].notna())
    )
    pipeline_valid = df_placsp[mask_valid]
    print(f"\n  PLACSP candidatos para match: {len(pipeline_valid):,}")
    
    # ── Solo matchear contratos sobre umbral UE ──
    # (bajo umbral no debería estar en TED)
    pipeline_ue = pipeline_valid[pipeline_valid['_sobre_umbral_ue']].copy()
    print(f"  PLACSP sobre umbral UE (≥{EU_THRESHOLD_MIN:,}€): {len(pipeline_ue):,}")
    
    # ── Matching loop ──
    matched_idx = []
    match_data = {}
    n_match_nif = 0
    n_match_exp = 0
    
    total = len(pipeline_ue)
    for count, (idx, row) in enumerate(pipeline_ue.iterrows()):
        if count % 100_000 == 0 and count > 0:
            elapsed = time.time() - t0
            pct = count / total * 100
            print(f"    {count:,}/{total:,} ({pct:.0f}%) - {len(matched_idx):,} matches - {elapsed:.0f}s")
        
        nif = row['_nif']
        imp = row['_imp_adj']
        yr = int(row['_año'])
        
        tol = max(imp * MATCH_TOLERANCE_PCT, MATCH_TOLERANCE_ABS)
        
        best_match = None
        best_diff = float('inf')
        best_key = None
        best_i = None
        best_lookup = None
        
        # Estrategia 1: NIF + importe + año
        for yr_offset in range(MATCH_YEAR_WINDOW + 1):
            for yr_try in ([yr + yr_offset, yr - yr_offset] if yr_offset > 0 else [yr]):
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
                        best_i = i
                        best_lookup = ted_lookup
        
        # Estrategia 2: Nº expediente
        if best_match is None:
            exp_id = row['_expediente'].strip().upper()
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
                        best_i = i
                        best_lookup = ted_lookup_exp
        
        if best_match is not None:
            best_lookup[best_key][best_i]['consumed'] = True
            matched_idx.append(idx)
            if best_lookup is ted_lookup:
                n_match_nif += 1
            else:
                n_match_exp += 1
            
            match_data[idx] = {
                'ted_id': best_match['ted_id'],
                'ted_importe': best_match['importe'],
                'ted_n_ofertas': best_match['n_ofertas'],
                'ted_cpv': best_match['cpv_ted'],
                'ted_cae': best_match['cae_ted'],
                'ted_win_size': best_match['win_size'],
                'ted_direct_award': best_match['direct_award'],
                'ted_sme_part': best_match['sme_part'],
                'ted_buyer_legal_type': best_match['buyer_legal_type'],
                'ted_duration': best_match['duration_lot'],
                'ted_award_criterion': best_match['award_criterion_type'],
                'ted_internal_id': best_match['internal_id'],
                'match_diff_euros': best_diff,
            }
    
    elapsed = time.time() - t0
    
    # ── Aplicar resultados ──
    df_placsp['_ted_validated'] = False
    df_placsp.loc[matched_idx, '_ted_validated'] = True
    
    # Enriquecimiento
    enrich_cols = [
        '_ted_id', '_ted_n_ofertas', '_ted_cpv', '_ted_win_size',
        '_ted_direct_award', '_ted_sme_part', '_ted_buyer_legal_type',
        '_ted_duration', '_ted_award_criterion', '_ted_internal_id',
    ]
    for col in enrich_cols:
        df_placsp[col] = ''
    df_placsp['_ted_n_ofertas'] = np.nan
    df_placsp['_ted_duration'] = np.nan
    
    for idx, data in match_data.items():
        df_placsp.loc[idx, '_ted_id'] = data['ted_id']
        df_placsp.loc[idx, '_ted_n_ofertas'] = pd.to_numeric(data['ted_n_ofertas'], errors='coerce')
        df_placsp.loc[idx, '_ted_cpv'] = data['ted_cpv']
        df_placsp.loc[idx, '_ted_win_size'] = data['ted_win_size']
        df_placsp.loc[idx, '_ted_direct_award'] = data['ted_direct_award']
        df_placsp.loc[idx, '_ted_sme_part'] = data['ted_sme_part']
        df_placsp.loc[idx, '_ted_buyer_legal_type'] = data['ted_buyer_legal_type']
        df_placsp.loc[idx, '_ted_duration'] = pd.to_numeric(data['ted_duration'], errors='coerce')
        df_placsp.loc[idx, '_ted_award_criterion'] = data['ted_award_criterion']
        df_placsp.loc[idx, '_ted_internal_id'] = data['ted_internal_id']
    
    # ── Missing in TED: contratos ≥ umbral sin match ──
    df_placsp['_ted_missing'] = False
    mask_missing = (
        df_placsp['_sobre_umbral_ue'] &
        ~df_placsp['_ted_validated'] &
        ~df_placsp['_es_menor'] &
        (df_placsp['_año'] >= 2010) &
        (df_placsp['_año'] <= 2025)
    )
    df_placsp.loc[mask_missing, '_ted_missing'] = True
    
    df_missing = df_placsp[df_placsp['_ted_missing']].copy()
    
    # ── Resumen ──
    n_matched = len(matched_idx)
    n_missing = df_placsp['_ted_missing'].sum()
    n_sobre_ue = pipeline_ue[~pipeline_ue['_es_menor']].shape[0]
    
    print(f"\n{'='*60}")
    print(f"  RESULTADOS CROSS-VALIDATION")
    print(f"{'='*60}")
    print(f"  Tiempo: {elapsed:.0f}s")
    print(f"  Contratos PLACSP sobre umbral UE: {len(pipeline_ue):,}")
    print(f"    (excl. menores): {n_sobre_ue:,}")
    print(f"\n  ✅ Validados por TED: {n_matched:,} ({n_matched/len(pipeline_ue)*100:.1f}%)")
    print(f"     Por NIF+importe: {n_match_nif:,}")
    print(f"     Por expediente:  {n_match_exp:,}")
    print(f"\n  ⚠️  Missing in TED: {n_missing:,} ({n_missing/max(n_sobre_ue,1)*100:.1f}% de no-menores sobre umbral)")
    
    # Desglose por año
    if n_matched > 0:
        matched_df = df_placsp[df_placsp['_ted_validated']]
        print(f"\n  Validados por año:")
        year_counts = matched_df['_año'].value_counts().sort_index()
        for yr, cnt in year_counts.items():
            yr_total = len(pipeline_ue[pipeline_ue['_año'] == yr])
            pct = cnt / max(yr_total, 1) * 100
            print(f"    {int(yr)}: {cnt:>6,} / {yr_total:>6,} ({pct:.0f}%)")
    
    if n_missing > 0:
        print(f"\n  Missing por año:")
        miss_year = df_missing['_año'].value_counts().sort_index()
        for yr, cnt in miss_year.items():
            print(f"    {int(yr)}: {cnt:>6,}")
        
        print(f"\n  Top 10 órganos con más missing:")
        top_organs = df_missing['_organ'].value_counts().head(10)
        for organ, cnt in top_organs.items():
            print(f"    {organ[:60]:<60}: {cnt:>4,}")
        
        print(f"\n  Distribución importes missing:")
        imps = df_missing['_imp_adj']
        for thr in [140_000, 215_000, 500_000, 1_000_000, 5_000_000]:
            n = (imps >= thr).sum()
            print(f"    ≥{thr:>10,}€: {n:>6,}")
    
    # Enriquecimiento stats
    if n_matched > 0:
        print(f"\n  Datos enriquecidos desde TED:")
        ted_offers = df_placsp['_ted_n_ofertas'].notna().sum()
        ted_size = (df_placsp['_ted_win_size'] != '').sum()
        ted_da = (df_placsp['_ted_direct_award'] != '').sum()
        print(f"    Nº ofertas:     {ted_offers:,}")
        print(f"    Tamaño ganador: {ted_size:,}")
        print(f"    Adj. directa:   {ted_da:,}")
    
    return df_placsp, df_missing


# ═══════════════════════════════════════════════════════════════════════════
#  3. MAIN
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Cargar
    df_placsp = load_placsp(PLACSP_PATH)
    df_ted = load_ted(TED_PATH)
    
    # Cross-validate
    df_placsp, df_missing = cross_validate(df_placsp, df_ted)
    
    # Guardar
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Solo guardar columnas de enriquecimiento (no todo el parquet de 1GB)
    enrich_cols = [
        'expediente', 'organo_contratante', 'nif_organo', 'nif_adjudicatario',
        'adjudicatario', 'importe_adjudicacion', 'ano', 'estado', 'conjunto',
        'tipo_contrato', 'cpv_principal',
        '_ted_validated', '_ted_missing', '_ted_id', '_ted_n_ofertas',
        '_ted_cpv', '_ted_win_size', '_ted_direct_award', '_ted_sme_part',
        '_ted_buyer_legal_type', '_ted_duration', '_ted_award_criterion',
    ]
    cols_exist = [c for c in enrich_cols if c in df_placsp.columns]
    
    # Matched
    matched = df_placsp[df_placsp['_ted_validated']][cols_exist]
    matched.to_parquet(OUTPUT_DIR / "crossval_matched.parquet", index=False)
    print(f"\n💾 Matched: {OUTPUT_DIR / 'crossval_matched.parquet'} ({len(matched):,} registros)")
    
    # Missing
    if len(df_missing) > 0:
        miss_cols = [c for c in cols_exist if c in df_missing.columns]
        df_missing[miss_cols].to_parquet(OUTPUT_DIR / "crossval_missing.parquet", index=False)
        print(f"💾 Missing: {OUTPUT_DIR / 'crossval_missing.parquet'} ({len(df_missing):,} registros)")
    
    # Stats resumen
    stats_path = OUTPUT_DIR / "crossval_stats.txt"
    with open(stats_path, 'w', encoding='utf-8') as f:
        f.write(f"Cross-validation PLACSP ↔ TED\n")
        f.write(f"{'='*40}\n")
        f.write(f"PLACSP adjudicaciones: {len(df_placsp):,}\n")
        f.write(f"TED registros: {len(df_ted):,}\n")
        f.write(f"Validados: {df_placsp['_ted_validated'].sum():,}\n")
        f.write(f"Missing in TED: {df_placsp['_ted_missing'].sum():,}\n")
    print(f"💾 Stats: {stats_path}")
    
    print(f"\n✅ Cross-validation completado.")
