-- 035_ops_nlp_runs.sql
-- Hito 3.1.b: tracking de ejecuciones del CLI/API "nlp analizar". Análogo a scheduler.runs
-- pero específico de Batch B. Cada `licitia-etl nlp analizar` crea una fila aquí
-- y los logs LLM (ops.llm_bases_reguladoras_logs.run_id) se enlazan por FK para
-- poder agregar coste/contadores por ejecución.

CREATE TABLE IF NOT EXISTS ops.nlp_runs (
  run_id              BIGSERIAL PRIMARY KEY,
  selector_kind       VARCHAR(20)  NOT NULL,
  selector_value      JSONB        NOT NULL,
  limit_value         INTEGER,
  dry_run             BOOLEAN      NOT NULL DEFAULT FALSE,
  force               BOOLEAN      NOT NULL DEFAULT FALSE,
  llm_model           VARCHAR(80),
  started_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  finished_at         TIMESTAMPTZ,
  status              VARCHAR(20)  NOT NULL DEFAULT 'running',
  items_planned       INTEGER      NOT NULL DEFAULT 0,
  items_processed     INTEGER      NOT NULL DEFAULT 0,
  items_valid         INTEGER      NOT NULL DEFAULT 0,
  items_partial       INTEGER      NOT NULL DEFAULT 0,
  items_invalid       INTEGER      NOT NULL DEFAULT 0,
  items_skipped       INTEGER      NOT NULL DEFAULT 0,
  items_cache_hits    INTEGER      NOT NULL DEFAULT 0,
  cost_usd_total      NUMERIC(12,6) NOT NULL DEFAULT 0,
  input_tokens_total  BIGINT       NOT NULL DEFAULT 0,
  output_tokens_total BIGINT       NOT NULL DEFAULT 0,
  pid                 INTEGER,
  error_message       TEXT,
  cancel_requested_at TIMESTAMPTZ,
  CONSTRAINT chk_nlp_runs_selector_kind CHECK (selector_kind IN ('anos','codigo_bdns','todo')),
  CONSTRAINT chk_nlp_runs_status        CHECK (status IN ('running','ok','failed','cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_nlp_runs_started ON ops.nlp_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_nlp_runs_status  ON ops.nlp_runs (status);

COMMENT ON TABLE ops.nlp_runs IS
  'Tracking de ejecuciones del CLI/API "nlp analizar". Análogo a scheduler.runs.';

-- Enlace logs LLM ↔ ejecución que los generó (compat hacia atrás: NULL para los
-- históricos previos a este hito).
-- En greenfield la columna ya existe (la añade canonical 032 sin FK porque
-- ops.nlp_runs aún no existe en ese punto). En DB existente la añadimos aquí.
ALTER TABLE ops.llm_bases_reguladoras_logs
  ADD COLUMN IF NOT EXISTS run_id BIGINT;

-- FK idempotente (ADD CONSTRAINT no soporta IF NOT EXISTS).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname  = 'llm_br_logs_run_id_fkey'
      AND conrelid = 'ops.llm_bases_reguladoras_logs'::regclass
  ) THEN
    ALTER TABLE ops.llm_bases_reguladoras_logs
      ADD CONSTRAINT llm_br_logs_run_id_fkey
      FOREIGN KEY (run_id) REFERENCES ops.nlp_runs(run_id);
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_llm_br_logs_run
  ON ops.llm_bases_reguladoras_logs (run_id);
