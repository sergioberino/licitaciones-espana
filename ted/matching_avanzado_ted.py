"""
Matching avanzado: Lotes, Acuerdos Marco y NIF Organo
======================================================
Recupera missing que no se encontraron por NIF adjudicatario + importe
usando 3 estrategias adicionales:

  E3: Match por NIF organo contratante (cae_nationalid)
  E4: Match por importe total agrupado (lotes/acuerdos marco)
  E5: Match fuzzy por nombre organo + importe

Uso:
    python matching_avanzado_ted.py
"""

import pandas as pd
import numpy as np
import re
import time
from pathlib import Path
from collections import defaultdict

# ======================================================================
#  CONFIG
# ======================================================================

SARA_PATH = Path("data/ted/crossval_sara.parquet")
TED_PATH = Path("data/ted/ted_es_can.parquet")
OUTPUT_DIR = Path("data/ted")

MATCH_TOL_PCT = 0.10
MATCH_TOL_ABS = 5_000
MATCH_TOL_PCT_WIDE = 0.15      # Para lotes agrupados
MATCH_TOL_ABS_WIDE = 20_000
YEAR_WINDOW = 1


# ======================================================================
#  HELPERS
# ======================================================================

def normalize_name(name):
    if not name or pd.isna(name):
        return ''
    s = str(name).upper().strip()
    for a, b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U'),
                 ('Ñ','N'),('Ü','U'),('Ç','C')]:
        s = s.replace(a, b)
    s = re.sub(r'[.,;:\-\/\\()\[\]"\'`]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def tol(imp, pct=MATCH_TOL_PCT, abs_val=MATCH_TOL_ABS):
    return max(imp * pct, abs_val)


# ======================================================================
#  CARGAR DATOS
# ======================================================================

print(f"\n{'='*70}")
print(f"  MATCHING AVANZADO TED")
print(f"{'='*70}")

t0 = time.time()

df_sara = pd.read_parquet(SARA_PATH)
df_sara['importe_adjudicacion'] = pd.to_numeric(df_sara['importe_adjudicacion'], errors='coerce')
df_sara['ano'] = pd.to_numeric(df_sara['ano'], errors='coerce')
print(f"  SARA cargados: {len(df_sara):,}")

# Separar ya matched vs missing
already_matched = df_sara['_ted_validated'].fillna(False).astype(bool)
df_matched_prev = df_sara[already_matched].copy()
df_missing = df_sara[~already_matched & df_sara['_ted_missing'].fillna(False).astype(bool)].copy()
print(f"  Ya matched (E1+E2): {len(df_matched_prev):,}")
print(f"  Missing a analizar: {len(df_missing):,}")

# TED
df_ted = pd.read_parquet(TED_PATH)
for col in ['importe_ted', 'year', 'number_offers']:
    if col in df_ted.columns:
        df_ted[col] = pd.to_numeric(df_ted[col], errors='coerce')
if 'win_nif_clean' not in df_ted.columns:
    df_ted['win_nif_clean'] = df_ted.get('win_nationalid', pd.Series(dtype=str)) \
        .fillna('').astype(str).str.strip().str.upper()
    df_ted['win_nif_clean'] = df_ted['win_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)

ted_valid = df_ted[
    (df_ted['importe_ted'].notna()) & (df_ted['importe_ted'] > 0)
].copy()
print(f"  TED con importe: {len(ted_valid):,}")

# Track consumed TED entries globally
ted_valid = ted_valid.reset_index(drop=True)
ted_consumed = set()  # Set of ted_valid indices already matched


# ======================================================================
#  E3: Match por NIF ORGANO + importe
# ======================================================================

print(f"\n{'='*70}")
print(f"  E3: Match por NIF organo contratante + importe")
print(f"{'='*70}")

# Indice TED: (cae_nationalid, year) -> [(ted_idx, importe)]
ted_by_cae = defaultdict(list)
for tidx, row in ted_valid.iterrows():
    cae_nif = str(row.get('cae_nationalid', '')).strip().upper()
    # Limpiar prefijo ES
    cae_nif = re.sub(r'^ES[-\s]*', '', cae_nif)
    yr = row.get('year', np.nan)
    imp = row['importe_ted']
    if cae_nif and len(cae_nif) >= 5 and pd.notna(yr):
        ted_by_cae[(cae_nif, int(yr))].append((tidx, imp))

print(f"  Indice TED por buyer NIF: {len(ted_by_cae):,} claves")

e3_matched = []  # (sara_idx, ted_idx, diff)

for idx, row in df_missing.iterrows():
    nif_org = str(row.get('nif_organo', '')).strip().upper()
    nif_org = re.sub(r'^ES[-\s]*', '', nif_org)
    imp = row['importe_adjudicacion']
    yr = row['ano']
    
    if not nif_org or len(nif_org) < 5 or pd.isna(imp) or pd.isna(yr) or imp <= 0:
        continue
    
    yr = int(yr)
    t = tol(imp)
    best = None
    best_diff = float('inf')
    
    for yr_try in [yr, yr-1, yr+1]:
        for tidx, ted_imp in ted_by_cae.get((nif_org, yr_try), []):
            if tidx in ted_consumed:
                continue
            diff = abs(ted_imp - imp)
            if diff <= t and diff < best_diff:
                best = tidx
                best_diff = diff
    
    if best is not None:
        ted_consumed.add(best)
        e3_matched.append((idx, best, best_diff))

print(f"  E3 matches: {len(e3_matched):,}")

# Actualizar missing
e3_idx = {m[0] for m in e3_matched}
df_missing_after_e3 = df_missing[~df_missing.index.isin(e3_idx)]
print(f"  Missing restante: {len(df_missing_after_e3):,}")


# ======================================================================
#  E4: Match por importe AGRUPADO (lotes / acuerdos marco)
# ======================================================================

print(f"\n{'='*70}")
print(f"  E4: Match por importe agrupado (lotes)")
print(f"{'='*70}")

# Construir indice TED: total-value (importe global del procedimiento)
# Y tambien: (cae_name_norm, year) -> total_value
ted_by_total = defaultdict(list)
for tidx, row in ted_valid.iterrows():
    if tidx in ted_consumed:
        continue
    
    # Usar total_value si existe, si no importe_ted
    total_val = pd.to_numeric(row.get('total_value', np.nan), errors='coerce')
    est_val = pd.to_numeric(row.get('estimated_value_proc', np.nan), errors='coerce')
    lot_imp = row['importe_ted']
    yr = row.get('year', np.nan)
    
    cae_nif = str(row.get('cae_nationalid', '')).strip().upper()
    cae_nif = re.sub(r'^ES[-\s]*', '', cae_nif)
    cae_name = normalize_name(row.get('cae_name', ''))[:40]
    
    if pd.notna(yr):
        yr = int(yr)
        # Indice por NIF organo
        if cae_nif and len(cae_nif) >= 5:
            for val in [total_val, est_val, lot_imp]:
                if pd.notna(val) and val > 0:
                    ted_by_total[(cae_nif, yr)].append((tidx, val))
        # Indice por nombre organo
        if cae_name and len(cae_name) > 5:
            for val in [total_val, est_val, lot_imp]:
                if pd.notna(val) and val > 0:
                    ted_by_total[('name:' + cae_name, yr)].append((tidx, val))

print(f"  Indice TED agrupado: {len(ted_by_total):,} claves")

# Agrupar missing por organo + ano
# Si la suma de importes del grupo matchea un total-value en TED -> son lotes
groups = df_missing_after_e3.groupby(['organo_contratante', 'ano']).agg(
    indices=('importe_adjudicacion', lambda x: list(x.index)),
    n=('importe_adjudicacion', 'count'),
    imp_total=('importe_adjudicacion', 'sum'),
    imp_list=('importe_adjudicacion', list),
    nif_org=('nif_organo', 'first'),
).reset_index()

# Solo grupos con >= 2 contratos (candidatos a lotes)
groups = groups[groups['n'] >= 2].copy()
print(f"  Grupos organo+ano con >=2 contratos: {len(groups):,}")
print(f"  Registros en esos grupos: {groups['n'].sum():,}")

e4_matched_groups = []  # [(group_indices, ted_idx)]
e4_matched_idx = set()

for _, grp in groups.iterrows():
    nif_org = str(grp['nif_org']).strip().upper()
    nif_org = re.sub(r'^ES[-\s]*', '', nif_org)
    organ_name = normalize_name(grp['organo_contratante'])[:40]
    yr = int(grp['ano'])
    imp_total = grp['imp_total']
    indices = grp['indices']
    
    if pd.isna(imp_total) or imp_total <= 0:
        continue
    
    t = tol(imp_total, MATCH_TOL_PCT_WIDE, MATCH_TOL_ABS_WIDE)
    best = None
    best_diff = float('inf')
    
    # Buscar por NIF organo
    for yr_try in [yr, yr-1, yr+1]:
        if nif_org and len(nif_org) >= 5:
            for tidx, ted_val in ted_by_total.get((nif_org, yr_try), []):
                if tidx in ted_consumed:
                    continue
                diff = abs(ted_val - imp_total)
                if diff <= t and diff < best_diff:
                    best = tidx
                    best_diff = diff
        
        # Buscar por nombre organo
        if best is None and organ_name and len(organ_name) > 5:
            for tidx, ted_val in ted_by_total.get(('name:' + organ_name, yr_try), []):
                if tidx in ted_consumed:
                    continue
                diff = abs(ted_val - imp_total)
                if diff <= t and diff < best_diff:
                    best = tidx
                    best_diff = diff
    
    if best is not None:
        ted_consumed.add(best)
        e4_matched_groups.append((indices, best, best_diff, imp_total))
        for i in indices:
            e4_matched_idx.add(i)

n_e4_records = len(e4_matched_idx)
print(f"  E4 grupos matched: {len(e4_matched_groups):,}")
print(f"  E4 registros cubiertos: {n_e4_records:,}")

df_missing_after_e4 = df_missing_after_e3[~df_missing_after_e3.index.isin(e4_matched_idx)]
print(f"  Missing restante: {len(df_missing_after_e4):,}")


# ======================================================================
#  E5: Match fuzzy por nombre organo + importe individual
# ======================================================================

print(f"\n{'='*70}")
print(f"  E5: Match por nombre organo + importe individual")
print(f"{'='*70}")

# Indice TED por nombre normalizado + ano
ted_by_name = defaultdict(list)
for tidx, row in ted_valid.iterrows():
    if tidx in ted_consumed:
        continue
    name = normalize_name(row.get('cae_name', ''))[:40]
    yr = row.get('year', np.nan)
    imp = row['importe_ted']
    if name and len(name) > 5 and pd.notna(yr):
        ted_by_name[(name, int(yr))].append((tidx, imp))

print(f"  Indice TED por buyer name: {len(ted_by_name):,} claves")

e5_matched = []

for idx, row in df_missing_after_e4.iterrows():
    organ = normalize_name(row.get('organo_contratante', ''))[:40]
    imp = row['importe_adjudicacion']
    yr = row['ano']
    
    if not organ or len(organ) < 6 or pd.isna(imp) or pd.isna(yr) or imp <= 0:
        continue
    
    yr = int(yr)
    t = tol(imp)
    best = None
    best_diff = float('inf')
    
    for yr_try in [yr, yr-1, yr+1]:
        for tidx, ted_imp in ted_by_name.get((organ, yr_try), []):
            if tidx in ted_consumed:
                continue
            diff = abs(ted_imp - imp)
            if diff <= t and diff < best_diff:
                best = tidx
                best_diff = diff
    
    if best is not None:
        ted_consumed.add(best)
        e5_matched.append((idx, best, best_diff))

print(f"  E5 matches: {len(e5_matched):,}")

e5_idx = {m[0] for m in e5_matched}
df_missing_final = df_missing_after_e4[~df_missing_after_e4.index.isin(e5_idx)]
print(f"  Missing restante final: {len(df_missing_final):,}")


# ======================================================================
#  RESUMEN CONSOLIDADO
# ======================================================================

print(f"\n{'='*70}")
print(f"  RESUMEN MATCHING CONSOLIDADO")
print(f"{'='*70}")

n_e1e2 = len(df_matched_prev)
n_e3 = len(e3_matched)
n_e4 = n_e4_records
n_e5 = len(e5_matched)
n_total_matched = n_e1e2 + n_e3 + n_e4 + n_e5
n_sara = len(df_sara)
n_missing_final = len(df_missing_final)
# Neg sin pub que siguen missing
n_neg_still = 0
if '_es_neg_sin_pub' in df_sara.columns:
    neg_idx = set(df_sara[df_sara['_es_neg_sin_pub'].fillna(False)].index)
    all_matched_idx = (
        set(df_matched_prev.index) | e3_idx | e4_matched_idx | e5_idx
    )
    n_neg_still = len(neg_idx - all_matched_idx)

print(f"\n  Total SARA: {n_sara:,}")
print(f"")
print(f"  E1 NIF adj. + importe:     {n_e1e2:>8,}  ({n_e1e2/n_sara*100:>5.1f}%)")
print(f"  E3 NIF organo + importe:   {n_e3:>8,}  ({n_e3/n_sara*100:>5.1f}%)")
print(f"  E4 Lotes agrupados:        {n_e4:>8,}  ({n_e4/n_sara*100:>5.1f}%)")
print(f"  E5 Nombre organo + imp.:   {n_e5:>8,}  ({n_e5/n_sara*100:>5.1f}%)")
print(f"  {'─'*42}")
print(f"  Total matched:             {n_total_matched:>8,}  ({n_total_matched/n_sara*100:>5.1f}%)")
print(f"  Missing final:             {n_missing_final:>8,}  ({n_missing_final/n_sara*100:>5.1f}%)")
print(f"    (neg. sin pub. en miss.): {n_neg_still:>7,}")

# ── Desglose por ano ──
print(f"\n  {'Ano':>6} {'SARA':>7} {'E1+E2':>7} {'E3':>6} {'E4':>7} {'E5':>6} "
      f"{'Total':>7} {'%':>6} {'Miss':>7}")
print(f"  {'-'*62}")

for yr in sorted(df_sara['ano'].dropna().unique()):
    yr = int(yr)
    if yr < 2010 or yr > 2026:
        continue
    
    yr_sara = df_sara[df_sara['ano'] == yr]
    yr_e1e2 = df_matched_prev[df_matched_prev['ano'] == yr].shape[0]
    yr_e3 = sum(1 for s, _, _ in e3_matched 
                if s in df_missing.index and df_missing.loc[s, 'ano'] == yr)
    yr_e4 = sum(1 for s in e4_matched_idx 
                if s in df_missing_after_e3.index and df_missing_after_e3.loc[s, 'ano'] == yr)
    yr_e5 = sum(1 for s, _, _ in e5_matched 
                if s in df_missing_after_e4.index and df_missing_after_e4.loc[s, 'ano'] == yr)
    yr_total = yr_e1e2 + yr_e3 + yr_e4 + yr_e5
    yr_miss = len(yr_sara) - yr_total
    pct = yr_total / max(len(yr_sara), 1) * 100
    
    print(f"  {yr:>6} {len(yr_sara):>7,} {yr_e1e2:>7,} {yr_e3:>6,} {yr_e4:>7,} {yr_e5:>6,} "
          f"{yr_total:>7,} {pct:>5.1f}% {yr_miss:>7,}")

# ── Desglose por tipo contrato ──
print(f"\n  Matching por tipo contrato:")
for tc in ['Obras', 'Servicios', 'Suministros']:
    tc_col = '_tipo_contrato' if '_tipo_contrato' in df_sara.columns else 'tipo_contrato'
    tc_sara = df_sara[df_sara[tc_col] == tc]
    tc_prev = df_matched_prev[df_matched_prev[tc_col] == tc].shape[0]
    
    tc_e3 = sum(1 for s, _, _ in e3_matched 
                if s in df_missing.index and df_missing.loc[s, tc_col] == tc)
    tc_e4 = sum(1 for s in e4_matched_idx 
                if s in df_missing_after_e3.index and df_missing_after_e3.loc[s, tc_col] == tc)
    tc_e5 = sum(1 for s, _, _ in e5_matched 
                if s in df_missing_after_e4.index and df_missing_after_e4.loc[s, tc_col] == tc)
    
    tc_total = tc_prev + tc_e3 + tc_e4 + tc_e5
    pct = tc_total / max(len(tc_sara), 1) * 100
    tc_miss = len(tc_sara) - tc_total
    print(f"    {tc:<15}: {len(tc_sara):>8,} SARA | {tc_total:>6,} match ({pct:.0f}%) | {tc_miss:>6,} miss")

# ── Missing final: clasificacion ──
print(f"\n  MISSING FINAL - Clasificacion:")

# Alta confianza (organo con presencia TED, >=2016, >=221K)
if '_organ_in_ted' not in df_missing_final.columns:
    organs_matched = set(df_matched_prev['organo_contratante'].dropna().unique())
    e3_organs = set(df_missing.loc[list(e3_idx), 'organo_contratante'].dropna().unique()) if e3_idx else set()
    all_ted_organs = organs_matched | e3_organs
    df_missing_final = df_missing_final.copy()
    df_missing_final['_organ_in_ted'] = df_missing_final['organo_contratante'].isin(all_ted_organs)

hc = df_missing_final[
    df_missing_final['_organ_in_ted'] &
    (df_missing_final['ano'] >= 2016) &
    (df_missing_final['importe_adjudicacion'] >= 221_000)
]
lc = df_missing_final[~df_missing_final.index.isin(hc.index)]

print(f"    Alta confianza (organo TED + >=2016 + >=221K): {len(hc):,}")
print(f"    Baja confianza (resto):                        {len(lc):,}")

if len(hc) > 0:
    print(f"\n  Top 15 organos - MISSING ALTA CONFIANZA FINAL:")
    top_hc = hc.groupby('organo_contratante').agg(
        n=('importe_adjudicacion', 'count'),
        imp=('importe_adjudicacion', 'sum'),
    ).sort_values('n', ascending=False).head(15)
    for organ, r in top_hc.iterrows():
        print(f"    {str(organ)[:55]:<55}: {int(r['n']):>5,} | {r['imp']/1e6:.0f}M")

# ── Guardar ──
print(f"\n  Guardando resultados...")

# Marcar estrategia de match en df_sara
df_sara['_match_strategy'] = ''
df_sara.loc[df_matched_prev.index, '_match_strategy'] = 'E1_nif_adj'
for s_idx, t_idx, _ in e3_matched:
    df_sara.loc[s_idx, '_match_strategy'] = 'E3_nif_org'
    df_sara.loc[s_idx, '_ted_validated'] = True
    df_sara.loc[s_idx, '_ted_missing'] = False
    df_sara.loc[s_idx, '_ted_id'] = str(ted_valid.loc[t_idx, 'ted_notice_id'])
for indices, t_idx, _, _ in e4_matched_groups:
    for s_idx in indices:
        df_sara.loc[s_idx, '_match_strategy'] = 'E4_lotes'
        df_sara.loc[s_idx, '_ted_validated'] = True
        df_sara.loc[s_idx, '_ted_missing'] = False
        df_sara.loc[s_idx, '_ted_id'] = str(ted_valid.loc[t_idx, 'ted_notice_id'])
for s_idx, t_idx, _ in e5_matched:
    df_sara.loc[s_idx, '_match_strategy'] = 'E5_nombre'
    df_sara.loc[s_idx, '_ted_validated'] = True
    df_sara.loc[s_idx, '_ted_missing'] = False
    df_sara.loc[s_idx, '_ted_id'] = str(ted_valid.loc[t_idx, 'ted_notice_id'])

# Guardar SARA actualizado
df_sara.to_parquet(OUTPUT_DIR / "crossval_sara_v2.parquet", index=False)
print(f"  SARA v2: {OUTPUT_DIR / 'crossval_sara_v2.parquet'} ({len(df_sara):,})")

# Missing final
miss_final_cols = [c for c in df_missing_final.columns if not c.startswith('_') or c in 
                   ['_es_sara','_ted_missing','_is_age','_is_sector','_umbral_sara']]
df_missing_final[miss_final_cols].to_parquet(OUTPUT_DIR / "missing_final.parquet", index=False)
print(f"  Missing final: {OUTPUT_DIR / 'missing_final.parquet'} ({len(df_missing_final):,})")

# Missing alta confianza final
if len(hc) > 0:
    hc_cols = [c for c in hc.columns if not c.startswith('_') or c in 
               ['_es_sara','_is_age','_is_sector','_umbral_sara']]
    hc[hc_cols].to_parquet(OUTPUT_DIR / "missing_alta_confianza_v2.parquet", index=False)
    print(f"  Missing HC: {OUTPUT_DIR / 'missing_alta_confianza_v2.parquet'} ({len(hc):,})")

elapsed = time.time() - t0
print(f"\n  Tiempo total: {elapsed:.0f}s")
print(f"  Matching avanzado completado.")