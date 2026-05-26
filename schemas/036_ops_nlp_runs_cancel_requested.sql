-- 036_ops_nlp_runs_cancel_requested.sql
-- Hito 3.2.a: timestamp en que la UI/API solicitó cancelación de un run.
-- El endpoint POST /nlp/runs/{id}/cancel envía SIGTERM al subprocess y
-- marca esta columna en la misma transacción, para que la UI pueda
-- mostrar "cancelando…" mientras el pipeline cierra cooperativamente.

ALTER TABLE ops.nlp_runs
  ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN ops.nlp_runs.cancel_requested_at IS
  'Timestamp en que la UI/API solicitó cancelación (SIGTERM enviado al subprocess).';
