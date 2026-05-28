-- 037_texto_beneficiario_fino_text.sql
-- Referencia canónica (greenfield): mismo cambio que migración incremental 033 del parent.
-- En BBDD existente aplicar services/postgres/migrations/etl/033_*.sql

ALTER TABLE l0.subvenciones_nlp
  ALTER COLUMN texto_beneficiario_fino TYPE TEXT;

ALTER TABLE l0.nacional_subvenciones
  ALTER COLUMN texto_beneficiario_fino TYPE TEXT;
