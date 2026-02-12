# PostgreSQL schema (licitaciones-espana)

Modular, domain-split schema (SRP). Schemas are **validated against Parquet** produced by the extraction scripts (PRP Phase 2) and are used by **Phase 3 ETL** and the PostgreSQL database. Apply to PostgreSQL before running ETL or API/ingestion.

## Applying the schema

Run the SQL files **in order** (001 → 001b → 002 → 003 → 004 → 005).

### Option A: ETL init-db (recommended)

From repo root or inside the ETL container:

```bash
licitia-etl init-db
```

This applies, in order: **001_dim_cpv.sql**, **001b_dim_cpv_router.sql**, 002_nacional.sql, 003_catalunya.sql, 004_valencia.sql, 005_views.sql. Idempotent (DDL uses IF NOT EXISTS; CPV inserts use ON CONFLICT DO NOTHING).

### Option B: Fresh database (Docker)

If you use a `database` service that mounts a schema directory into `/docker-entrypoint-initdb.d`, point it at `./schemas`. Postgres runs all `.sql` files there **only on first initialization** (empty data dir). Ensure file order: 001_dim_cpv, 001b_dim_cpv_router, 002_… run in order.

- **First-time:** start the database — schema is applied automatically.
- **If the DB volume already exists** and schema was not applied (or was changed): remove the volume and recreate, then start DB again.

### Option C: Manual (existing database)

If the database already exists and you do not want to wipe it, run the files manually in order:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/dbname"
for f in 001_dim_cpv.sql 001b_dim_cpv_router.sql 002_nacional.sql 003_catalunya.sql 004_valencia.sql 005_views.sql; do
  psql "$DATABASE_URL" -f "schemas/$f"
done
```
_(Run from repo root.)_

---

## Files

| File | Contents |
|------|----------|
| `001_dim_cpv.sql` | dim schema, dim.cpv_dim (num_code, code, label) + ~9.45k CPV rows; source: cpv_code.sql, script: scripts/cpv_dim_convert.py |
| `001b_dim_cpv_router.sql` | dim.cpv_router (num_code, embedding vector(1024), label) + HNSW index for ANN; populated by `generate_embedding --target cpv` |
| `002_nacional.sql` | nacional_licitaciones, nacional_agregacion_ccaa, nacional_contratos_menores, nacional_encargos_medios_propios, nacional_consultas_preliminares |
| `003_catalunya.sql` | catalunya_subvenciones, catalunya_contratacion_publica, catalunya_pressupostos, catalunya_convenios, catalunya_rrhh, catalunya_patrimoni |
| `004_valencia.sql` | valencia_opportunities (dataset_type discriminator) |
| `005_views.sql` | v_opportunities_all (union of all domain tables) |

**Parquet ↔ schema mapping:** See [docs/extraction-contract.md](../docs/extraction-contract.md) (section "Parquet ↔ schema column mapping") for column mapping from extraction Parquet to these tables. Phase 3 ETL uses that mapping to load Parquet into the database.

**Nullability:** In domain tables (nacional, catalunya, valencia), all columns are nullable except `id` (primary key), `created_at`, and `updated_at`. This is intentional (e.g. adjudicatario or other data may be unpublished at publication time).

**Schema validation and optimization (P2):** Schemas were validated against the Parquet shape defined by the extraction scripts and optimized for ETL and DB (NUMERIC, TIMESTAMPTZ, INTEGER, BOOLEAN, TEXT, JSONB; indexes). See the extraction contract section "Schema validation and optimization". **E2E validation:** After you manually run downloads into `tmp/`, run `python3 scripts/validate_schemas_parquet.py --base-dir tmp` from repo root; the report is written to `schemas/parquet_columns_report.json`. See [docs/extraction-contract.md](../docs/extraction-contract.md) section "E2E validation (P2 success criteria)" for the full checklist.

## CPV dimension (dim.cpv_dim) and CPV router (dim.cpv_router)

**CPV dimension is in `dim.cpv_dim`**, created and populated by `001_dim_cpv.sql`. Columns: **num_code** (INTEGER, PK, 8-digit without control digit, used for Parquet/L0 and FKs), **code** (VARCHAR, original code with control digit from public source), **label** (TEXT). Domain tables reference `dim.cpv_dim(num_code)` via `cpv_id` and `cpvs` (INTEGER[]). Display with `LPAD(num_code::TEXT, 8, '0')`. To regenerate `001_dim_cpv.sql` from the source dump, run `python3 scripts/cpv_dim_convert.py` (reads `schemas/cpv_code.sql`, strips control digit, writes `schemas/001_dim_cpv.sql`).

**dim.cpv_router** holds one E5 embedding per CPV for approximate nearest-neighbor search at query time (CPV router, adr-hito6). Created by `001b_dim_cpv_router.sql`; populated by `licitia-etl generate_embedding --target cpv`. Columns: num_code (PK, FK to dim.cpv_dim), embedding vector(1024), label (denormalized). Index: HNSW with cosine distance.

---

## Validation (P2 success criteria)

PostgreSQL connection and database creation live **outside this project scope**. There is no DB connection in this repo until Phase 3 and the external database exist. The validated schemas in this folder are **ready to be applied** when that database is available: run the SQL files in order (001_dim_cpv → 001b_dim_cpv_router → 002 → 003 → 004 → 005), e.g. via `licitia-etl init-db` or `psql $DATABASE_URL -f schemas/001_dim_cpv.sql` then `schemas/001b_dim_cpv_router.sql` … `schemas/005_views.sql`. No errors then indicates the schemas are ready for Phase 3 ETL.
