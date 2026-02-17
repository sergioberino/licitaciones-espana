# PostgreSQL schema (licitaciones-espana)

Modular, domain-split schema (SRP). Schemas are **validated against Parquet** produced by the extraction scripts (PRP Phase 2) and are used by **Phase 3 ETL** and the PostgreSQL database. Apply to PostgreSQL before running ETL or API/ingestion.

## Applying the schema

Run the SQL files **in order** (001 → 002 → 003 → 008 → 009).

### Option A: ETL init-db (recommended)

From repo root or inside the ETL container:

```bash
licitia-etl init-db
```

This applies, in order: **001_dim_cpv.sql**, **002_dim_ccaa.sql**, **002b_dim_provincia.sql**, **003_dim_dir3.sql**, **008_scheduler.sql**, **009_scheduler_runs_pid.sql**. Idempotent (DDL uses IF NOT EXISTS; CPV inserts use ON CONFLICT DO NOTHING). The ETL does not create or maintain dim.cpv_router (that is the indexer's responsibility if needed).

### Option B: Fresh database (Docker)

If you use a `database` service that mounts a schema directory into `/docker-entrypoint-initdb.d`, point it at `./schemas`. Postgres runs all `.sql` files there **only on first initialization** (empty data dir). Ensure file order: 001_dim_cpv, 002_…, 008_scheduler, 009_scheduler_runs_pid run in order.

- **First-time:** start the database — schema is applied automatically.
- **If the DB volume already exists** and schema was not applied (or was changed): remove the volume and recreate, then start DB again.

### Option C: Manual (existing database)

If the database already exists and you do not want to wipe it, run the files manually in order:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/dbname"
for f in 001_dim_cpv.sql 002_dim_ccaa.sql 002b_dim_provincia.sql 003_dim_dir3.sql 008_scheduler.sql 009_scheduler_runs_pid.sql; do
  psql "$DATABASE_URL" -f "schemas/$f"
done
```
_(Run from repo root.)_

---

## Files

| File | Contents |
|------|----------|
| `001_dim_cpv.sql` | dim schema, dim.cpv_dim (num_code, code, label) + ~9.45k CPV rows; source: cpv_code.sql, script: scripts/cpv_dim_convert.py |
| `002_dim_ccaa.sql` | dim.dim_ccaa (CCAA) |
| `002b_dim_provincia.sql` | dim.dim_provincia (provincias, FK to dim_ccaa) |
| `003_dim_dir3.sql` | dim.dim_dir3 (unidades AGE); init-db also runs DIR3 ingest from XLSX |
| `008_scheduler.sql` | scheduler schema, scheduler.tasks, scheduler.runs |
| `009_scheduler_runs_pid.sql` | adds process_id to scheduler.runs |

**Parquet ↔ schema mapping:** See [docs/extraction-contract.md](../docs/extraction-contract.md) (section "Parquet ↔ schema column mapping") for column mapping from extraction Parquet to these tables. Phase 3 ETL uses that mapping to load Parquet into the database.

**Nullability:** In domain tables (nacional, catalunya, valencia), all columns are nullable except `id` (primary key), `created_at`, and `updated_at`. This is intentional (e.g. adjudicatario or other data may be unpublished at publication time).

**Schema validation and optimization (P2):** Schemas were validated against the Parquet shape defined by the extraction scripts and optimized for ETL and DB (NUMERIC, TIMESTAMPTZ, INTEGER, BOOLEAN, TEXT, JSONB; indexes). See the extraction contract section "Schema validation and optimization". **E2E validation:** After you manually run downloads into `tmp/`, run `python3 scripts/validate_schemas_parquet.py --base-dir tmp` from repo root; the report is written to `schemas/parquet_columns_report.json`. See [docs/extraction-contract.md](../docs/extraction-contract.md) section "E2E validation (P2 success criteria)" for the full checklist.

## CPV dimension (dim.cpv_dim)

**CPV dimension is in `dim.cpv_dim`**, created and populated by `001_dim_cpv.sql`. Columns: **num_code** (INTEGER, PK, 8-digit without control digit, used for Parquet/L0 and FKs), **code** (VARCHAR, original code with control digit from public source), **label** (TEXT). Domain tables reference `dim.cpv_dim(num_code)` via `cpv_id` and `cpvs` (INTEGER[]). Display with `LPAD(num_code::TEXT, 8, '0')`. To regenerate `001_dim_cpv.sql` from the source dump, run `python3 scripts/cpv_dim_convert.py` (reads `schemas/cpv_code.sql`, strips control digit, writes `schemas/001_dim_cpv.sql`). The ETL does not create or maintain a CPV router table; that is the indexer's responsibility if required.

---

## Validation (P2 success criteria)

PostgreSQL connection and database creation live **outside this project scope**. There is no DB connection in this repo until Phase 3 and the external database exist. The validated schemas in this folder are **ready to be applied** when that database is available: run the SQL files in order (001_dim_cpv → 002 → 003 → 008 → 009), e.g. via `licitia-etl init-db`. No errors then indicates the schemas are ready for Phase 3 ETL.
