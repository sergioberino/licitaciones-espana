-- Añade process_id a scheduler.runs (PID del proceso de ingest) para status y unregister por PID.

ALTER TABLE scheduler.runs ADD COLUMN IF NOT EXISTS process_id INTEGER NULL;
COMMENT ON COLUMN scheduler.runs.process_id IS 'PID del proceso que ejecutó esta run (licitia-etl ingest). NULL si no se registró.';
