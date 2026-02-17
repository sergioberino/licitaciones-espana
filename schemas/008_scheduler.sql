-- Scheduler: configuración y registro de ejecuciones del ETL.
-- El ETL usa el schema scheduler para estado y configuración de tareas (ingest por conjunto/subconjunto).
-- Poblado de tasks: comando "licitia-etl scheduler register" desde CONJUNTOS_REGISTRY y frecuencias por defecto.

CREATE SCHEMA IF NOT EXISTS scheduler;

CREATE TABLE IF NOT EXISTS scheduler.tasks (
  task_id       SERIAL PRIMARY KEY,
  conjunto      TEXT NOT NULL,
  subconjunto   TEXT NOT NULL,
  schedule_expr TEXT,
  enabled       BOOLEAN NOT NULL DEFAULT true,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (conjunto, subconjunto)
);

CREATE TABLE IF NOT EXISTS scheduler.runs (
  run_id         SERIAL PRIMARY KEY,
  task_id        INTEGER NOT NULL REFERENCES scheduler.tasks(task_id) ON DELETE CASCADE,
  started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at    TIMESTAMPTZ,
  status         TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'ok', 'failed')),
  rows_inserted  INTEGER,
  rows_omitted   INTEGER,
  error_message  TEXT,
  CONSTRAINT runs_status_finished CHECK (
    (status = 'running' AND finished_at IS NULL) OR
    (status IN ('ok', 'failed') AND finished_at IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_scheduler_runs_task_id ON scheduler.runs(task_id);
CREATE INDEX IF NOT EXISTS idx_scheduler_runs_started_at ON scheduler.runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_scheduler_runs_status ON scheduler.runs(status) WHERE status = 'running';

COMMENT ON TABLE scheduler.tasks IS 'Tareas de ingest por (conjunto, subconjunto). schedule_expr: expresión cron o etiqueta (Mensual/Trimestral/Anual).';
COMMENT ON TABLE scheduler.runs IS 'Registro de ejecuciones; una por ejecución de ingest. status=running evita solapamiento.';
