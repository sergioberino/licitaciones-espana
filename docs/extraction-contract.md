# Extraction contract (PRP Phase 2)

Contract for extraction scripts: arguments, output paths under `tmp/`, and how to produce small datasets for schema validation. Source of truth for Phase 3 ETL.

---

## Schema validation and optimization (P2, schemas layer only)

SQL schemas in `schemas/` were **validated and optimized** against the Parquet shape defined by the extraction scripts (no DB connection; work ends at the schemas layer).

- **Nacional:** Parquet shape comes from `nacional/licitaciones.py` (`parsear_entry` + export). Column set and types are known; mapping to schema columns is in the comparison table below. Schema types are aligned: NUMERIC for amounts, TIMESTAMPTZ for dates, INTEGER for counts, BOOLEAN for flags, TEXT for ids/strings, JSONB for variant fields (`raw_extra`).
- **Catalunya / Valencia:** Parquet columns vary by file (Socrata/CSV-derived). Schemas use a common core (id, title, authority, dates, amounts, source_url) plus `raw_extra` (JSONB) so ETL can map any Parquet column set. Types in the schema are optimized for the common core (NUMERIC, TIMESTAMPTZ, INTEGER, BOOLEAN, TEXT).
- **Optimization for ETL and DB:** Amounts → NUMERIC(18, 2); dates → TIMESTAMPTZ; counts (e.g. num_ofertas) → INTEGER; flags (e.g. es_pyme) → BOOLEAN; IDs and free text → TEXT; variant/extra fields → raw_extra JSONB. Indexes on authority_id, published_at, geography_id, etc. for ETL and query performance.
- **Runtime check:** Repo Parquet files may be placeholders. After running extraction and producing Parquet under `tmp/`, run `python3 scripts/validate_schemas_parquet.py --base-dir tmp` to generate a column report (`schemas/parquet_columns_report.json`) and confirm alignment with the schemas.

---

## Path convention

All script output lands under a single base directory:

- **Base:** `./tmp` (repo-relative) or `$LICITACIONES_TMP_DIR` if set.
- **Nacional:** `tmp/raw/` (ZIP/ATOM downloads), `tmp/output/` (CSV + Parquet).
- **Catalunya:** `tmp/catalunya_datos_completos/` (CSV download), `tmp/catalunya_parquet/` (Parquet).
- **Valencia:** `tmp/valencia_datos/` (CSV download), `tmp/valencia_parquet/` (Parquet).

Run scripts from the **repository root** so relative paths resolve correctly.

---

## 1. Nacional — `nacional/licitaciones.py`

| Item | Description |
|------|--------------|
| **Source** | PLACSP (Plataforma de Contratación del Sector Público). ATOM/XML syndication. |
| **Script path** | `nacional/licitaciones.py` |

### Arguments

| Argument | Type | Default | Effect |
|----------|------|---------|--------|
| `--anos` | string, required | — | Year range, e.g. `2020-2026` or `2024-2024`. |
| `--conjunto` | string | `todos` | Dataset subset: `licitaciones`, `agregacion`, `menores`, `encargos`, `consultas`, or `todos`. |
| `--solo-descargar` | flag | false | Only download ZIPs; do not process to CSV/Parquet. |
| `--solo-procesar` | flag | false | Only process existing ZIPs; do not download. |

### Data retrieved

- **Licitaciones** (sindicacion_643): Licitaciones (excl. menores).
- **Agregación** (sindicacion_1044): Licitaciones por agregación.
- **Menores** (sindicacion_1143): Contratos menores.
- **Encargos** (sindicacion_1383): Encargos a medios propios.
- **Consultas** (sindicacion_1403): Consultas preliminares de mercado.

### Output paths (under tmp base)

- **Downloads:** `tmp/raw/{conjunto}/*.zip` (e.g. `tmp/raw/licitaciones/`).
- **Export:** `tmp/output/licitaciones_completo_{ano_inicio}_{ano_fin}.csv` and `.parquet`.
- Parquet has column `conjunto` to route rows to the correct nacional table in ETL.

### Small-dataset run (schema validation)

```bash
python nacional/licitaciones.py --anos 2024-2024 --conjunto licitaciones
```

- Produces one year, one conjunto → small Parquet at `tmp/output/licitaciones_completo_2024_2024.parquet`.

---

## 2. Catalunya download — `scripts/ccaa_cataluna.py`

| Item | Description |
|------|--------------|
| **Source** | Socrata (Portal Transparència Catalunya) + CKAN (Open Data Barcelona) + Gencat. |
| **Script path** | `scripts/ccaa_cataluna.py` |

### Arguments

| Argument | Type | Default | Effect |
|----------|------|---------|--------|
| (none) | — | — | No CLI; uses env `LICITACIONES_TMP_DIR` or default `tmp`. Output dir: `{tmp_base}/catalunya_datos_completos`. |
| `--limit-datasets` | optional (if added) | — | Comma-separated Socrata dataset IDs to download only a subset (e.g. `hb6v-jcbf`). |

### Data retrieved

- **Contratación:** registro público contratos, publicaciones PSCP, adjudicaciones, contratación programada, COVID, tribunal, fase ejecución, contratos menores, etc.
- **Subvenciones:** RAISC concesiones/convocatorias, convocatorias subvenciones.
- **Convenios:** registro convenios.
- **Presupuestos:** evolución, aprobados, ejecución mensual (gastos/ingresos), consolidado.
- **Sector público / entidades:** registro sector público, códigos departamentos, ens locals, ajuntaments, composició plens.
- **RRHH:** altos cargos, retribuciones (funcionarios/laboral), tablas retributivas, convocatorias personal, enunciados exámenes.
- **Territorio:** municipis Catalunya, municipis España.
- **Barcelona (CKAN):** perfil contratante, contratos menores, contratistas, resumen trimestral, modificaciones.

### Output paths (under tmp base)

- **Download (CSV):** `tmp/catalunya_datos_completos/` (subdirs: `01_transparencia_catalunya/`, `02_barcelona/`, `03_gencat_adicional/`).
- **Parquet:** produced by `ccaa_cataluna_parquet.py` → `tmp/catalunya_parquet/`.

### Small-dataset run (schema validation)

- **Option A:** Run full download once; use one Parquet (e.g. `tmp/catalunya_parquet/contratacion/contratos_registro.parquet`) for validation.
- **Option B:** If `--limit-datasets hb6v-jcbf` is implemented, run with that to download only registro público de contratos → then run Parquet script.

---

## 3. Catalunya CSV → Parquet — `scripts/ccaa_cataluna_parquet.py`

| Item | Description |
|------|--------------|
| **Source** | Converts CSV output of `ccaa_cataluna.py` to Parquet. |
| **Script path** | `scripts/ccaa_cataluna_parquet.py` |

### Arguments

| Argument | Type | Default | Effect |
|----------|------|---------|--------|
| (none) | — | — | Uses env `LICITACIONES_TMP_DIR` or default `tmp`. Input: `{tmp_base}/catalunya_datos_completos`, Output: `{tmp_base}/catalunya_parquet`. |

### Input / output

- **Input:** `tmp/catalunya_datos_completos/` (CSV from Catalunya download).
- **Output:** `tmp/catalunya_parquet/` (subdirs: `contratacion/`, `subvenciones/`, `convenios/`, `presupuestos/`, `entidades/`, `rrhh/`, `territorio/`).
- Mapping of CSV → Parquet files is defined in script (e.g. `01_transparencia_catalunya/01_contratacion/registro_publico_contratos.csv` → `contratacion/contratos_registro.parquet`).

### Small-dataset run

Run after a (full or limited) Catalunya download. At least one Parquet (e.g. `tmp/catalunya_parquet/contratacion/contratos_registro.parquet`) is used for schema validation.

---

## 4. Valencia download — `scripts/ccaa_valencia.py`

| Item | Description |
|------|--------------|
| **Source** | CKAN (Dades Obertes GVA). |
| **Script path** | `scripts/ccaa_valencia.py` |

### Arguments

| Argument | Type | Default | Effect |
|----------|------|---------|--------|
| (none) | — | — | Uses env `LICITACIONES_TMP_DIR` or default `tmp`. Output dir: `{tmp_base}/valencia_datos`. |
| `--categories` | optional (if added) | — | Comma-separated categories (e.g. `contratacion`) to download only those. |

### Data retrieved (categories)

Contratación, subvenciones, presupuestos, convenios, lobbies (REGIA), empleo, paro, siniestralidad, patrimonio, entidades, territorio, turismo, sanidad, transporte.

### Output paths (under tmp base)

- **Download (CSV):** `tmp/valencia_datos/{category}/` (e.g. `tmp/valencia_datos/contratacion/`).
- **Parquet:** produced by `ccaa_valencia_parquet.py` → `tmp/valencia_parquet/{category}/`.

### Small-dataset run (schema validation)

- **Option A:** Run with `--categories contratacion` (if implemented) for one category only.
- **Option B:** Run full download; use e.g. `tmp/valencia_parquet/contratacion/*.parquet` for validation.

---

## 5. Valencia CSV → Parquet — `scripts/ccaa_valencia_parquet.py`

| Item | Description |
|------|--------------|
| **Source** | Converts CSV output of `ccaa_valencia.py` to Parquet. |
| **Script path** | `scripts/ccaa_valencia_parquet.py` |

### Arguments

| Argument | Type | Default | Effect |
|----------|------|---------|--------|
| (none) | — | — | Uses env `LICITACIONES_TMP_DIR` or default `tmp`. Input: `{tmp_base}/valencia_datos`, Output: `{tmp_base}/valencia_parquet`. |

### Input / output

- **Input:** `tmp/valencia_datos/` (one subdir per category, CSVs inside).
- **Output:** `tmp/valencia_parquet/{category}/` (same folder structure; one Parquet per CSV).

### Small-dataset run

Run after Valencia download. Use at least one category’s Parquets (e.g. `tmp/valencia_parquet/contratacion/`) for schema validation.

---

## Exact paths used for small runs (P2)

Running the commands below produces output under `tmp/` (if `tmp/` is writable). The directory `tmp/` is gitignored; run from repo root to populate it for local schema validation.

| Pipeline | Small-run command / note | Parquet(s) for schema validation |
|----------|--------------------------|----------------------------------|
| Nacional | `python3 nacional/licitaciones.py --anos 2024-2024 --conjunto licitaciones` | `tmp/output/licitaciones_completo_2024_2024.parquet` |
| Catalunya | `python3 scripts/ccaa_cataluna.py --limit-datasets hb6v-jcbf` then `python3 scripts/ccaa_cataluna_parquet.py` | `tmp/catalunya_parquet/contratacion/contratos_registro.parquet` (or one per domain). |
| Valencia | `python3 scripts/ccaa_valencia.py --categories contratacion` then `python3 scripts/ccaa_valencia_parquet.py` | `tmp/valencia_parquet/contratacion/*.parquet` (or one category). |

---

## E2E validation (P2 success criteria)

You can **manually run the downloads** into `tmp/` and then run the validation script to complete the end-to-end validation.

### Step 1: Populate `tmp/` (manual)

From repo root, run the small-run commands above (or a subset). Ensure at least one Parquet per pipeline exists under `tmp/`:

- **Nacional:** `tmp/output/licitaciones_completo_*_*.parquet`
- **Catalunya:** e.g. `tmp/catalunya_parquet/contratacion/contratos_registro.parquet`
- **Valencia:** e.g. `tmp/valencia_parquet/contratacion/*.parquet`

### Step 2: Run schema–Parquet validation

From repo root (with `pyarrow` installed, e.g. `pip install pyarrow` or use project venv):

```bash
python3 scripts/validate_schemas_parquet.py --base-dir tmp
```

The script reads Parquet **metadata only** (no row data) from `tmp/` and writes:

- **Report:** `schemas/parquet_columns_report.json` (column names and suggested PG types per pipeline).

### Step 3: Success criteria

- **Report generated:** `schemas/parquet_columns_report.json` exists and contains non-empty `nacional`, `catalunya`, and/or `valencia` arrays (one entry per column: `name`, `dtype`, `pg_type`).
- **Alignment:** Compare the report columns to the [Parquet ↔ schema column mapping](#parquet--schema-column-mapping) tables below (and to `schemas/002_nacional.sql`, `003_catalunya.sql`, `004_valencia.sql`). Confirm that schema columns and types can absorb the Parquet columns (direct mapping or via `raw_extra`). No schema change is required if the existing mapping holds.

When both steps are done and the report aligns with the schemas, **e2e validation is complete** for P2 (schemas layer; no DB connection).

---

## Parquet ↔ schema column mapping

Tables below compare **Parquet column names** (from extraction scripts) with **schema column names** (PostgreSQL). Used for Phase 3 ETL mapping and for deciding later whether to change naming. Schema naming is kept for P2.

### Nacional (PLACSP)

Parquet: `tmp/output/licitaciones_completo_*_*.parquet`. Schema: `schemas/002_nacional.sql` (tables per conjunto). ETL filters by `conjunto` and inserts into the corresponding table.

| Parquet column | Schema column | Notes |
|----------------|---------------|-------|
| id | id | Same |
| expediente | expediente | Same |
| objeto | objeto | Same; can also feed title/description_raw |
| organo_contratante | authority_name | |
| nif_organo | nif_organo | Same; VARCHAR(9) in schema. |
| dir3_organo | dir3_organo | Same |
| ciudad_organo | ciudad_organo | Same |
| tipo_contrato | tipo_contrato | Same |
| subtipo_code | subtipo_code | Same |
| procedimiento | procedimiento | Same |
| estado | estado | Same |
| importe_sin_iva | budget_amount | |
| importe_con_iva | importe_con_iva | Same |
| importe_adjudicacion | importe_adjudicacion | Same |
| adjudicatario | adjudicatario | Same |
| nif_adjudicatario | nif_adjudicatario | Same; VARCHAR(9) in schema. |
| num_ofertas | num_ofertas | Same |
| es_pyme | es_pyme | Same |
| cpv_principal | cpv_id | INTEGER; FK to cpv_codes(code). ETL parses 8-digit string to INTEGER (leading zeros dropped in storage); display with LPAD(code::TEXT, 8, '0'). Codes must exist in cpv_codes before ETL. |
| cpvs | cpvs | INTEGER[] array of cpv_codes.code. ETL splits semicolon-separated string, parses each to INTEGER, inserts array; each element must exist in cpv_codes. |
| ubicacion | ubicacion | Same |
| nuts | nuts | Same |
| fecha_limite | deadline_at | |
| fecha_adjudicacion | fecha_adjudicacion | Same |
| fecha_publicacion | published_at | |
| url | source_url | |
| conjunto | (routing) | ETL uses to choose table (nacional_licitaciones, etc.) |
| ano | (derived) | Can go to raw_extra or omit |
| tipo_contrato_code, procedimiento_code, estado_code, dependencia, id_plataforma, duracion, duracion_unidad, financiacion_ue, urgencia, hora_limite, importe_adj_con_iva, fecha_updated | raw_extra | Variant fields in JSONB |

### Catalunya

Parquet: `tmp/catalunya_parquet/**/*.parquet`. Schema: `schemas/003_catalunya.sql`. Each Parquet file has Socrata/CSV-derived columns; ETL maps common fields and puts the rest in raw_extra.

| Parquet column (example: contratos_registro) | Schema column | Notes |
|---------------------------------------------|---------------|-------|
| (varies by file) | id | Extract or generate |
| (varies) | title, description_raw | From dataset-specific columns |
| (varies) | authority_name, authority_id | From organ/entity columns |
| (varies) | published_at, deadline_at | Parse dates |
| (varies) | budget_amount | Parse numeric |
| (varies) | source_url | If available |
| (rest) | raw_extra | JSONB |

### Valencia

Parquet: `tmp/valencia_parquet/**/*.parquet`. Schema: `schemas/004_valencia.sql` (`valencia_opportunities`). ETL sets `dataset_type` from category folder (e.g. contratacion, subvenciones).

| Parquet column (varies by category/file) | Schema column | Notes |
|------------------------------------------|---------------|-------|
| (varies) | id | Extract or generate |
| (folder name) | dataset_type | contratacion, subvenciones, etc. |
| (varies) | title, description_raw | |
| (varies) | authority_name, published_at, budget_amount, etc. | Map per category |
| (rest) | raw_extra | JSONB |
