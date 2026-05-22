-- 033_scheduler_batches.sql
-- Soporte de chains secuenciales en el scheduler (WP2.2).

CREATE TABLE IF NOT EXISTS scheduler.batches (
  id              SERIAL PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  description     TEXT,
  enabled         BOOLEAN NOT NULL DEFAULT TRUE,
  cron_expression TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheduler.batch_steps (
  id              SERIAL PRIMARY KEY,
  batch_id        INT NOT NULL REFERENCES scheduler.batches(id) ON DELETE CASCADE,
  step_order      INT NOT NULL,
  task_name       TEXT NOT NULL,
  on_failure      VARCHAR(20) NOT NULL DEFAULT 'abort',
  timeout_minutes INT,
  UNIQUE (batch_id, step_order),
  CONSTRAINT chk_on_failure CHECK (on_failure IN ('abort','continue'))
);

CREATE TABLE IF NOT EXISTS scheduler.batch_runs (
  id              BIGSERIAL PRIMARY KEY,
  batch_id        INT NOT NULL REFERENCES scheduler.batches(id),
  status          VARCHAR(20) NOT NULL,
  started_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  current_step    INT,
  error_message   TEXT,
  CONSTRAINT chk_batch_run_status CHECK (status IN ('queued','running','completed','failed','partial'))
);

CREATE TABLE IF NOT EXISTS scheduler.batch_step_runs (
  id              BIGSERIAL PRIMARY KEY,
  batch_run_id    BIGINT NOT NULL REFERENCES scheduler.batch_runs(id) ON DELETE CASCADE,
  step_order      INT NOT NULL,
  -- scheduler.runs.run_id es SERIAL (INTEGER); el FK debe coincidir en tipo.
  task_run_id     INTEGER REFERENCES scheduler.runs(run_id) ON DELETE SET NULL,
  status          VARCHAR(20) NOT NULL,
  started_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_batch_runs_status ON scheduler.batch_runs (status, started_at);
CREATE INDEX IF NOT EXISTS idx_batch_step_runs_parent ON scheduler.batch_step_runs (batch_run_id, step_order);

COMMENT ON TABLE scheduler.batches IS 'Definición de chains (tareas secuenciales).';
COMMENT ON TABLE scheduler.batch_steps IS 'Pasos ordenados; cada uno apunta a un scheduler.tasks.name existente.';
COMMENT ON TABLE scheduler.batch_runs IS 'Ejecuciones de un batch.';
COMMENT ON TABLE scheduler.batch_step_runs IS 'Ejecuciones de cada step; task_run_id linka al run real.';
