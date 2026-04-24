-- Tabla de logs de generación de resúmenes de subvenciones mediante LLM.
-- Almacena una entrada por subvención (id es a la vez PK y FK).
CREATE TABLE IF NOT EXISTS ops.llm_resumen_subvenciones_logs (
    id                BIGINT        PRIMARY KEY REFERENCES l0.nacional_subvenciones(id),
    input_tokens      INTEGER       NOT NULL,
    completion_tokens INTEGER       NOT NULL,
    model             VARCHAR(25)   NOT NULL,
    processing_time   NUMERIC(10,3) NOT NULL  -- segundos
);

CREATE INDEX IF NOT EXISTS idx_llm_resumen_logs_model
    ON ops.llm_resumen_subvenciones_logs (model);
