CREATE TABLE IF NOT EXISTS scheduler.incidents (
    incident_id   SERIAL PRIMARY KEY,
    run_id        INTEGER REFERENCES scheduler.runs(run_id) ON DELETE SET NULL,
    task_id       INTEGER REFERENCES scheduler.tasks(task_id) ON DELETE SET NULL,
    severity      TEXT NOT NULL DEFAULT 'error',
    category      TEXT NOT NULL,
    summary       TEXT NOT NULL,
    detail        TEXT,
    log_snapshot  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at   TIMESTAMPTZ,
    resolved_by   TEXT,
    resolution    TEXT
);

CREATE INDEX IF NOT EXISTS idx_incidents_unresolved
    ON scheduler.incidents (created_at DESC)
    WHERE resolved_at IS NULL;
