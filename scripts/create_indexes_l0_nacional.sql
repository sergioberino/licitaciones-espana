-- Índices B-tree org_key (expediente, id_plataforma) sobre tablas nacionales L0 en schema l0.
-- Ejecutar sobre la BBDD donde las tablas ya existen (p. ej. tras ingest nacional).
-- Uso: psql -f create_indexes_l0_nacional.sql ... o ejecutar desde cliente SQL.

CREATE INDEX IF NOT EXISTS idx_nacional_licitaciones_org_key
  ON l0.nacional_licitaciones ("expediente", "id_plataforma");

CREATE INDEX IF NOT EXISTS idx_nacional_agregacion_ccaa_org_key
  ON l0.nacional_agregacion_ccaa ("expediente", "id_plataforma");

CREATE INDEX IF NOT EXISTS idx_nacional_contratos_menores_org_key
  ON l0.nacional_contratos_menores ("expediente", "id_plataforma");

CREATE INDEX IF NOT EXISTS idx_nacional_encargos_medios_propios_org_key
  ON l0.nacional_encargos_medios_propios ("expediente", "id_plataforma");

CREATE INDEX IF NOT EXISTS idx_nacional_consultas_preliminares_org_key
  ON l0.nacional_consultas_preliminares ("expediente", "id_plataforma");
