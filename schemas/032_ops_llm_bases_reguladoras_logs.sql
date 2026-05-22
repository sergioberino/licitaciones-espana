-- 032_ops_llm_bases_reguladoras_logs.sql
-- Observabilidad LLM para Batch B + tabla de fallos para diagnóstico.

CREATE TABLE IF NOT EXISTS ops.llm_bases_reguladoras_logs (
  id                BIGSERIAL PRIMARY KEY,
  document_key      TEXT          NOT NULL,
  llm_model         VARCHAR(80)   NOT NULL,
  input_tokens      INTEGER,
  output_tokens     INTEGER,
  duration_ms       INTEGER,
  validation_status VARCHAR(20),
  cost_usd          NUMERIC(10,6),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_br_logs_doc     ON ops.llm_bases_reguladoras_logs (document_key);
CREATE INDEX IF NOT EXISTS idx_llm_br_logs_created ON ops.llm_bases_reguladoras_logs (created_at);

CREATE TABLE IF NOT EXISTS ops.nlp_failures (
  id              BIGSERIAL PRIMARY KEY,
  subvencion_id   INT,
  document_source VARCHAR(30),
  document_ref    TEXT,
  llm_model       VARCHAR(80),
  raw_snippet     TEXT,
  error_message   TEXT,
  failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nlp_failures_subv   ON ops.nlp_failures (subvencion_id);
CREATE INDEX IF NOT EXISTS idx_nlp_failures_failed ON ops.nlp_failures (failed_at);

COMMENT ON TABLE ops.llm_bases_reguladoras_logs IS
  'Tokens y duración LLM por análisis Batch B. Mismo patrón que ops.llm_resumen_subvenciones_logs.';
COMMENT ON TABLE ops.nlp_failures IS
  'Convocatorias cuya invocación LLM o validación schema falló (validation_status=invalid).';
