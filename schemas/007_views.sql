-- Union view for cross-domain queries (CRUD, BA, exports).
-- Single responsibility: expose a common shape so callers can query "all opportunities" without knowing domain tables.
-- domain = table name. Nacional L0 tables live in schema l0 (created by ingest); this view only unions catalunya/valencia (legacy shape).
-- To query nacional data use l0.nacional_licitaciones etc. directly.

CREATE OR REPLACE VIEW v_opportunities_all AS
SELECT id, 'catalunya_subvenciones'::TEXT AS domain, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM catalunya_subvenciones
UNION ALL
SELECT id, 'catalunya_contratacion_publica'::TEXT, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM catalunya_contratacion_publica
UNION ALL
SELECT id, 'catalunya_pressupostos'::TEXT, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM catalunya_pressupostos
UNION ALL
SELECT id, 'catalunya_convenios'::TEXT, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM catalunya_convenios
UNION ALL
SELECT id, 'catalunya_rrhh'::TEXT, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM catalunya_rrhh
UNION ALL
SELECT id, 'catalunya_patrimoni'::TEXT, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM catalunya_patrimoni
UNION ALL
SELECT id, 'valencia_opportunities'::TEXT, title, description_raw, authority_id, authority_name, geography_id, region_name, published_at, deadline_at, budget_amount, source_url, created_at, updated_at FROM valencia_opportunities;
