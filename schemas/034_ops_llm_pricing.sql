-- 034_ops_llm_pricing.sql
-- Hito 3.1.a: tarifas históricas LLM por proveedor/modelo con vigencia temporal.
-- Carga manual (no automatizada). Consultada por etl.nlp.pipeline.compute_cost_usd
-- para persistir cost_usd en ops.llm_bases_reguladoras_logs.

CREATE TABLE IF NOT EXISTS ops.llm_pricing (
  id                 SERIAL PRIMARY KEY,
  provider           VARCHAR(20)   NOT NULL,
  model              VARCHAR(80)   NOT NULL,
  input_per_1m_usd   NUMERIC(10,6) NOT NULL,
  output_per_1m_usd  NUMERIC(10,6) NOT NULL,
  valid_from         DATE          NOT NULL,
  valid_to           DATE,
  source_url         TEXT,
  notes              TEXT,
  created_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_pricing_dates CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_llm_pricing_current
  ON ops.llm_pricing (provider, model)
  WHERE valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_llm_pricing_lookup
  ON ops.llm_pricing (provider, model, valid_from);

COMMENT ON TABLE ops.llm_pricing IS
  'Tarifas históricas LLM por proveedor/modelo con vigencia temporal. Cargada manualmente (no automatizada).';
