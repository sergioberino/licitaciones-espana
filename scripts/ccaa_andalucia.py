#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
  SCRAPER v4 DEFINITIVO — Licitaciones Junta de Andalucía (850k)
═══════════════════════════════════════════════════════════════════

  BREAKTHROUGH: Different sort orders return different 10k pages!
  
  sort=idExpediente:asc  → IDs 444k-445k (oldest records)
  sort=fechaPublicacion:desc → IDs 913k-919k (newest records)
  ZERO OVERLAP in first 100 → confirmed different windows!

  STRATEGY:
  1. Dimensional chunking: proc → tipo → estado → tram → perfil → prov → fp
  2. For any chunk still >10k: MULTI-SORT PAGINATION
     - 8+ sort combos × 10k each = 80k+ unique records
     - Deduplication by id_expediente
     - Sorts: idExpediente, fechaPublicacion, importeLicitacion,
              valorEstimado (each asc + desc)
  3. Every record reachable!

  The only truly massive chunk: SYBS03/SUM/RES/O (290k)
  Split by provincia (8 chunks, 23k-68k each)
  Split by formaPresentacion (M vs null, roughly 75/25)
  Largest sub-chunk: prov=29/fp=M = ~55k → 6 sorts needed
═══════════════════════════════════════════════════════════════════
"""

import requests, json, csv, sys, time, logging, re
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
    HAS_PANDAS = True
except:
    HAS_PANDAS = False

BASE = "https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico"
ES_URL = f"{BASE}/elastic/sirec_pdc_expedientes/_search?pretty"
DATA_DIR = Path("./ccaa_Andalucia"); DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(DATA_DIR/"scraper.log", encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger(__name__)

DELAY = 0.3; PAGE_SIZE = 100; MAX_FROM = 9900

# ═══════════════════════════════════════════════════════════════
S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*", "Content-Type": "application/json",
    "Origin": "https://www.juntadeandalucia.es",
    "Referer": f"{BASE}/perfiles-licitaciones/buscador-general",
    "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
})

def init():
    S.get(f"{BASE}/perfiles-licitaciones/licitaciones-publicadas", timeout=20)

def es(body, timeout=90):
    for a in range(3):
        try:
            r = S.post(ES_URL, json=body, timeout=timeout)
            if r.ok: return r.json()
            if r.status_code >= 500: time.sleep(2)
            else: return None
        except: time.sleep(3)
    return None

def cnt(must=None, must_not=None):
    q = {"query":{"bool":{}},"size":0,"track_total_hits":True}
    if must: q["query"]["bool"]["must"] = must
    if must_not: q["query"]["bool"]["must_not"] = must_not
    d = es(q, timeout=60)
    if d:
        t = d.get("hits",{}).get("total",{})
        return t.get("value",0) if isinstance(t,dict) else t
    return 0

def mm(f,v): return {"match":{f:v}}
def mn(f,v): return {"match":{f:{"query":v}}} if isinstance(v,str) else {"match":{f:v}}

# ═══════════════════════════════════════════════════════════════
# DIMENSION VALUES
# ═══════════════════════════════════════════════════════════════
PROCS = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
TIPOS = ["SERV","SUM","OBR","PRIV","PATR","ESP","COL","CSER","GEST","CONS","MIX","ACON","CA",
         "OBRA","ADMESP","ARRED","PAT","GESSERVPUB","CONOBRPUB","COLABPUBPR","CONSERV"]
ESTADOS = ["RES","PUB","ADJ","EVA","ANU","DES","PRE","FOR","REN","PEN","CER","REV","ABD","PAA"]
TRAMS = ["O","U","E","S","N"]
PROVS = ["04","11","14","18","21","23","29","41","51","52","98"]
FPS = ["E","P","M","N","S","O"]
YEARS = ["2018","2019","2020","2021","2022","2023","2024","2025","2026"]

SORT_COMBOS = [
    [{"idExpediente": "asc"}],
    [{"idExpediente": "desc"}],
    [{"importeLicitacion": "asc"}],
    [{"importeLicitacion": "desc"}],
    [{"numeroExpediente": "asc"}],
    [{"numeroExpediente": "desc"}],
    [{"titulo": "asc"}],
    [{"titulo": "desc"}],
    [{"fechaLimitePresentacion": "asc"}],
    [{"fechaLimitePresentacion": "desc"}],
    [{"adjudicaciones.importeAdjudicacion": "asc"}],
    [{"adjudicaciones.importeAdjudicacion": "desc"}],
]

_PERFILES = None
def get_perfiles():
    global _PERFILES
    if _PERFILES: return _PERFILES
    log.info("Discovering perfil codes...")
    perfiles = set()
    for sf, so in [("idExpediente","asc"),("idExpediente","desc"),
                    ("fechaPublicacion","asc"),("fechaPublicacion","desc")]:
        for p in range(0, 9900, 100):
            d = es({"query":{"match_all":{}},"size":100,"sort":[{sf:so}],"from":p})
            if not d: break
            if not d.get("hits",{}).get("hits"): break
            for h in d["hits"]["hits"]:
                pc = h["_source"].get("perfilContratante",{})
                if isinstance(pc,dict) and pc.get("codigo"):
                    perfiles.add(pc["codigo"])
            time.sleep(0.1)
        log.info(f"  {sf} {so}: {len(perfiles)} codes")
    _PERFILES = sorted(perfiles)
    log.info(f"  Total: {len(_PERFILES)} perfil codes")
    return _PERFILES


# ═══════════════════════════════════════════════════════════════
# FLATTEN
# ═══════════════════════════════════════════════════════════════
def extract(data):
    if not data: return []
    return [flatten(h.get("_source",{})) for h in data.get("hits",{}).get("hits",[])]

def flatten(s):
    r = {}
    r["id_expediente"]=s.get("idExpediente","")
    r["numero_expediente"]=s.get("numeroExpediente","")
    r["titulo"]=s.get("titulo","")
    tc=s.get("tipoContrato") or {}
    if isinstance(tc,dict): r["tipo_contrato"]=tc.get("descripcion",""); r["tipo_contrato_codigo"]=tc.get("codigo","")
    else: r["tipo_contrato"]=str(tc or ""); r["tipo_contrato_codigo"]=""
    pc=s.get("perfilContratante") or {}
    if isinstance(pc,dict): r["organo_contratacion"]=pc.get("descripcion",""); r["codigo_perfil"]=pc.get("codigo",""); r["codigo_dir3"]=pc.get("codigoDir3","")
    else: r["organo_contratacion"]=""; r["codigo_perfil"]=""; r["codigo_dir3"]=""
    est=s.get("estado") or {}
    if isinstance(est,dict): r["estado"]=est.get("nombre",""); r["estado_codigo"]=est.get("codigo","")
    else: r["estado"]=""; r["estado_codigo"]=""
    r["importe_licitacion"]=s.get("importeLicitacion","")
    r["valor_estimado"]=s.get("valorEstimado","")
    r["fecha_publicacion"]=_dt(s.get("fechaPublicacion"))
    r["fecha_limite_presentacion"]=_dt(s.get("fechaLimitePresentacion"))
    r["codigo_procedimiento"]=s.get("codigoProcedimiento","")
    r["codigo_tramitacion"]=s.get("codigoTipoTramitacion","")
    r["codigo_normativa"]=s.get("codigoNormativa","")
    r["forma_presentacion"]=s.get("formaPresentacion","")
    r["cofinanciado_ue"]=s.get("cofinanciadoUE","")
    r["subasta_electronica"]=s.get("subastaElectronica","")
    r["sistema_racionalizacion"]=s.get("sistemaRacionalizacion","")
    cpvs=s.get("codigosCpv") or []
    r["cpv"]=";".join(str(c) for c in cpvs) if isinstance(cpvs,list) else ""
    provs=s.get("provinciasEjecucion") or []
    r["provincias_ejecucion"]=";".join(str(p) for p in provs) if isinstance(provs,list) else ""
    adjs=s.get("adjudicaciones") or []
    if adjs and isinstance(adjs,list):
        f0=adjs[0] if isinstance(adjs[0],dict) else {}
        nif=f0.get("nifAdjudicatario") or ""
        r["adjudicatario_nif"]=nif.rstrip(";") if isinstance(nif,str) else str(nif)
        r["importe_adjudicacion"]=f0.get("importeAdjudicacion","")
        r["importe_adjudicacion_iva"]=f0.get("importeAdjudicacionConIva","")
        r["num_adjudicaciones"]=len(adjs)
        if len(adjs)>1: r["todos_adjudicatarios_nif"]=";".join((a.get("nifAdjudicatario") or "").rstrip(";") for a in adjs if isinstance(a,dict))
    ans=s.get("anuncios") or []
    if ans and isinstance(ans,list):
        fechas=[a.get("fechaPublicacion") for a in ans if isinstance(a,dict) and a.get("fechaPublicacion")]
        if fechas: r["anuncio_primera_fecha"]=_dt(min(fechas)); r["anuncio_ultima_fecha"]=_dt(max(fechas))
        r["num_anuncios"]=len(ans)
    meds=s.get("mediosPublicacion") or []
    r["medios_publicacion"]=";".join(m.get("codigo","") for m in meds if isinstance(m,dict)) if isinstance(meds,list) else ""
    r["num_lotes"]=len(s.get("lotes") or [])
    r["url_detalle"]=f"{BASE}/perfiles-licitaciones/detalle-licitacion?idExpediente={r['id_expediente']}"
    return r

def _dt(v):
    if not v: return ""
    m=re.match(r'(\d{4}-\d{2}-\d{2})',str(v)); return m.group(1) if m else str(v)[:10]


# ═══════════════════════════════════════════════════════════════
# PAGINATE — single sort (≤10k)
# ═══════════════════════════════════════════════════════════════
def paginate(must=None, must_not=None, sort=None, label=""):
    if sort is None: sort = [{"idExpediente":"asc"}]
    recs=[]; seen=set(); tc=None
    for fr in range(0, MAX_FROM+PAGE_SIZE, PAGE_SIZE):
        q={"query":{"bool":{}},"size":PAGE_SIZE,"sort":sort,"track_total_hits":True,"from":fr}
        if must: q["query"]["bool"]["must"]=must
        if must_not: q["query"]["bool"]["must_not"]=must_not
        d=es(q)
        if not d: break
        if tc is None:
            t=d.get("hits",{}).get("total",{}); tc=t.get("value",0) if isinstance(t,dict) else t
        batch=extract(d)
        if not batch: break
        for r in batch:
            eid=r["id_expediente"]
            if eid not in seen: seen.add(eid); recs.append(r)
        if len(recs)>=tc: break
        time.sleep(DELAY)
    return recs, tc or 0


# ═══════════════════════════════════════════════════════════════
# MULTI-SORT PAGINATE — for chunks >10k
# ═══════════════════════════════════════════════════════════════
def paginate_multisort(must=None, must_not=None, label="", target=None):
    all_recs = []; seen = set()
    if target is None:
        target = cnt(must=must, must_not=must_not)
    
    for si, sort in enumerate(SORT_COMBOS):
        sort_name = list(sort[0].keys())[0] + ":" + list(sort[0].values())[0]
        new_this_sort = 0
        
        for fr in range(0, MAX_FROM+PAGE_SIZE, PAGE_SIZE):
            q={"query":{"bool":{}},"size":PAGE_SIZE,"sort":sort,"track_total_hits":True,"from":fr}
            if must: q["query"]["bool"]["must"]=must
            if must_not: q["query"]["bool"]["must_not"]=must_not
            d=es(q)
            if not d: break
            batch=extract(d)
            if not batch: break
            new_in_batch = 0
            for r in batch:
                eid=r["id_expediente"]
                if eid not in seen:
                    seen.add(eid); all_recs.append(r); new_in_batch += 1; new_this_sort += 1
            if new_in_batch == 0: break
            time.sleep(DELAY)
        
        pct = len(all_recs)/target*100 if target else 0
        log.info(f"    {label} sort {si+1}/{len(SORT_COMBOS)} ({sort_name}): "
                 f"+{new_this_sort:,} → {len(all_recs):,}/{target:,} ({pct:.0f}%)")
        
        if len(all_recs) >= target: break
    
    if len(all_recs) < target:
        log.warning(f"  ⚠️  {label}: {len(all_recs):,}/{target:,} ({len(all_recs)/target*100:.0f}%) PARTIAL")
    
    return all_recs


# ═══════════════════════════════════════════════════════════════
# RECURSIVE DIMENSIONAL CHUNKER
# ═══════════════════════════════════════════════════════════════
DIMS = [
    ("tipoContrato.codigo", TIPOS),
    ("estado.codigo", ESTADOS),
    ("codigoTipoTramitacion", TRAMS),
    ("perfilContratante.codigo", "PERFILES"),
    ("provinciasEjecucion", PROVS),
    ("formaPresentacion", FPS),
    ("numeroExpediente", YEARS),
]

def scrape_recursive(must, must_not, label, all_records, seen_ids, dim_idx=0):
    total = cnt(must=must, must_not=must_not)
    if total == 0: return 0
    
    if total <= MAX_FROM + PAGE_SIZE:
        recs, _ = paginate(must=must, must_not=must_not, label=label)
        new = 0
        for r in recs:
            eid = r["id_expediente"]
            if eid not in seen_ids: seen_ids.add(eid); all_records.append(r); new += 1
        return new
    
    # Try dimensional split
    if dim_idx < len(DIMS):
        field, values = DIMS[dim_idx]
        dim_name = field.split(".")[-1]
        if values == "PERFILES": values = get_perfiles()
        
        log.info(f"  🔀 {label} ({total:,}) → {dim_name} ({len(values)} vals)")
        
        total_new = 0
        for val in values:
            sub_must = list(must) + [mm(field, val)]
            sub_c = cnt(must=sub_must, must_not=must_not)
            if sub_c == 0: continue
            n = scrape_recursive(sub_must, must_not, f"{label}/{val}", all_records, seen_ids, dim_idx+1)
            total_new += n
            time.sleep(0.05)
        
        # Handle uncovered ("null" for this dimension)
        if total_new < total:
            excl_mn = list(must_not)
            for val in values:
                excl_mn.append(mn(field, val) if isinstance(val,str) else {"match":{field:val}})
            if len(excl_mn) < 900:
                null_c = cnt(must=must, must_not=excl_mn)
                if null_c > 0:
                    log.info(f"  📎 {label}/null_{dim_name}: {null_c:,}")
                    n = scrape_recursive(must, excl_mn, f"{label}/null_{dim_name}", all_records, seen_ids, dim_idx+1)
                    total_new += n
        
        return total_new
    
    # All dims exhausted → multi-sort
    log.info(f"  🔄 {label} ({total:,}) → multi-sort")
    recs = paginate_multisort(must=must, must_not=must_not, label=label, target=total)
    new = 0
    for r in recs:
        eid = r["id_expediente"]
        if eid not in seen_ids: seen_ids.add(eid); all_records.append(r); new += 1
    return new


# ═══════════════════════════════════════════════════════════════
# MAIN SCRAPERS
# ═══════════════════════════════════════════════════════════════
def scrape_std():
    init()
    base_mn = [mn("estado.codigo","BRR"), mn("codigoProcedimiento",9)]
    total = cnt(must_not=base_mn)
    print("="*70); print(f"  SCRAPE ESTÁNDAR: {total:,}"); print("="*70)
    all_recs=[]; seen=set(); t0=time.time()
    
    for proc in PROCS:
        if proc == 9: continue
        c = cnt(must=[mm("codigoProcedimiento",proc)], must_not=base_mn)
        if c == 0: continue
        log.info(f"\n{'─'*60}\n  proc={proc}: {c:,}")
        scrape_recursive([mm("codigoProcedimiento",proc)], base_mn, f"p{proc}", all_recs, seen, 0)
        el=time.time()-t0; rate=len(all_recs)/el if el else 0
        eta=(total-len(all_recs))/rate/60 if rate>0 else 0
        log.info(f"  📈 {len(all_recs):,}/{total:,} ({len(all_recs)/total*100:.1f}%) {rate:.0f}/s ETA={eta:.1f}m")
        save_csv(all_recs, "licitaciones_std_progress.csv")
    
    save_csv(all_recs, "licitaciones_std.csv")
    log.info(f"\n  ✅ STD: {len(all_recs):,}/{total:,} in {(time.time()-t0)/60:.1f}m")
    return all_recs

def scrape_menores():
    init()
    base_mn = [mn("estado.codigo","BRR")]
    total = cnt(must=[mm("codigoProcedimiento",9)], must_not=base_mn)
    print("="*70); print(f"  SCRAPE MENORES: {total:,}"); print("="*70)
    all_recs=[]; seen=set(); t0=time.time()
    
    scrape_recursive([mm("codigoProcedimiento",9)], base_mn, "men", all_recs, seen, 0)
    
    save_csv(all_recs, "licitaciones_menores.csv")
    log.info(f"\n  ✅ MENORES: {len(all_recs):,}/{total:,} in {(time.time()-t0)/60:.1f}m")
    return all_recs

def scrape_all():
    std = scrape_std()
    men = scrape_menores()
    seen=set(); dedup=[]
    for r in std+men:
        if r["id_expediente"] not in seen: seen.add(r["id_expediente"]); dedup.append(r)
    save_csv(dedup, "licitaciones_all.csv")
    log.info(f"  📊 ALL: {len(dedup):,}")


# ═══════════════════════════════════════════════════════════════
# CSV
# ═══════════════════════════════════════════════════════════════
CSV_COLS = [
    "id_expediente","numero_expediente","titulo",
    "tipo_contrato","tipo_contrato_codigo",
    "organo_contratacion","codigo_perfil","codigo_dir3",
    "estado","estado_codigo",
    "importe_licitacion","valor_estimado",
    "importe_adjudicacion","importe_adjudicacion_iva",
    "fecha_publicacion","fecha_limite_presentacion",
    "anuncio_primera_fecha","anuncio_ultima_fecha",
    "adjudicatario_nif","todos_adjudicatarios_nif","num_adjudicaciones",
    "codigo_procedimiento","codigo_tramitacion","codigo_normativa",
    "forma_presentacion","cofinanciado_ue","subasta_electronica",
    "sistema_racionalizacion","cpv","provincias_ejecucion",
    "medios_publicacion","num_lotes","num_anuncios","url_detalle",
]

def save_csv(records, filename):
    if not records: return
    path = DATA_DIR / filename
    clean = [{k:v for k,v in r.items() if not k.startswith("_")} for r in records]
    if HAS_PANDAS:
        df=pd.DataFrame(clean)
        cols=[c for c in CSV_COLS if c in df.columns]
        extra=[c for c in df.columns if c not in CSV_COLS]
        df[cols+extra].to_csv(path,index=False,encoding="utf-8-sig")
    else:
        keys=[c for c in CSV_COLS if any(c in r for r in clean)]
        for r in clean:
            for k in r:
                if k not in keys: keys.append(k)
        with open(path,"w",newline="",encoding="utf-8-sig") as f:
            w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore"); w.writeheader(); w.writerows(clean)
    log.info(f"💾 {path} ({len(records):,})")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("""
  python scraper_v4_final.py scrape-std    Standard (~78k)
  python scraper_v4_final.py scrape-men    Menores (~772k)
  python scraper_v4_final.py scrape        All (~850k)
"""); sys.exit(0)
    cmd = args[0].lower()
    if cmd=="scrape-std": scrape_std()
    elif cmd=="scrape-men": scrape_menores()
    elif cmd=="scrape": scrape_all()
    else: print(f"?: {cmd}")



