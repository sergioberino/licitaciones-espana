# Operational notes

Notes for operators and follow-up tasks.

---

## Audit: Postgres errors during cpv_router ingestion

During `generate_embedding` (bulk ingestion into `dim.cpv_router`), Postgres errors were observed; the exact error could not be captured due to Docker Desktop crash.

**Task:** Reproduce in a stable environment (e.g. VPS or local Postgres), capture the full error message and stack, then address. Possible causes to investigate:

- Connection timeouts or connection pool exhaustion
- Transaction or payload size limits
- Type/constraint mismatches (e.g. vector dimension, encoding)

Until then, failed ingest batches are logged and can be inspected via staging files under `{LICITACIONES_TMP_DIR}/embedding_cpv/` if needed for retry or debugging.
