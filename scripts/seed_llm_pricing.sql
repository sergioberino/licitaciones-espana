-- Hito 3.1.i — Seed inicial de tarifas LLM para Batch B (NLP).
-- Modelo en producción: gpt-5.4-nano (OpenAI), pricing oficial 2026-05-25.
-- Fuente: https://openai.com/api/pricing
-- Cached input ($0.02/1M) NO se aplica en nuestro flujo: cada item analizado es
-- una invocación independiente sin reuso de contexto cacheado.
-- El índice único uq_llm_pricing_current es PARCIAL (WHERE valid_to IS NULL),
-- así que referenciamos las columnas + el predicate en ON CONFLICT.
INSERT INTO ops.llm_pricing
  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from, source_url, notes)
VALUES (
  'openai',
  'gpt-5.4-nano',
  0.200000,
  1.250000,
  '2026-05-25',
  'https://openai.com/api/pricing',
  'GA pricing as of 2026-05-25. Cached input $0.02/1M not applicable in production flow.'
)
ON CONFLICT (provider, model) WHERE valid_to IS NULL DO NOTHING;
