"""Persistencia atómica del análisis NLP:
  1. UPSERT en l0.subvenciones_nlp (cache dedup por document_key).
  2. UPDATE en l0.nacional_subvenciones (dual-write 25 cols + puente + metadata).

Para 'valid' y 'partial' se ejecutan ambos pasos (las cols NULL del partial se
propagan; matcher las gestiona vía regla 'null en item → skip').
Para 'invalid' no se persiste nada aquí (caller hace log a ops.nlp_failures).
Atomicidad: una sola transacción Postgres.
"""
from __future__ import annotations

import logging
from typing import Optional

from psycopg2.extras import Json

from etl.nlp.project import MatchingFields
from etl.nlp.resolver import ResolvedDocument
from etl.nlp.validator import ValidationResult


logger = logging.getLogger(__name__)


UPSERT_SQL = """
INSERT INTO l0.subvenciones_nlp (
  document_key, document_source, document_heuristic_step, document_ref,
  schema_version, modelo_documental, input_char_count, llm_model,
  validation_status, validation_errors,
  modalidad_lgs, mecanismo_seleccion, puntuacion_minima_acceso,
  regimen_ue_tipo, rgec_articulo,
  tipo_beneficiario_fino, texto_beneficiario_fino, admite_consorcio,
  art13_lgs_verificado,
  intensidad_maxima_pct, importe_maximo_beneficiario,
  regla_calculo_ayuda, modalidad_pago, garantia_pago, regimen_acumulacion,
  domicilio_fiscal_nuts, domicilio_fiscal_ine, domicilio_fiscal_tipo,
  centros_trabajo_nuts, centros_trabajo_ine,
  ejecucion_nuts, ejecucion_ine, ejecucion_municipios_anexo,
  impacto_nuts, geo_patron, geo_condiciones_suficientes,
  nlp_json
) VALUES (
  %(document_key)s, %(document_source)s, %(document_heuristic_step)s, %(document_ref)s,
  %(schema_version)s, %(modelo_documental)s, %(input_char_count)s, %(llm_model)s,
  %(validation_status)s, %(validation_errors)s,
  %(modalidad_lgs)s, %(mecanismo_seleccion)s, %(puntuacion_minima_acceso)s,
  %(regimen_ue_tipo)s, %(rgec_articulo)s,
  %(tipo_beneficiario_fino)s, %(texto_beneficiario_fino)s, %(admite_consorcio)s,
  %(art13_lgs_verificado)s,
  %(intensidad_maxima_pct)s, %(importe_maximo_beneficiario)s,
  %(regla_calculo_ayuda)s, %(modalidad_pago)s, %(garantia_pago)s, %(regimen_acumulacion)s,
  %(domicilio_fiscal_nuts)s, %(domicilio_fiscal_ine)s, %(domicilio_fiscal_tipo)s,
  %(centros_trabajo_nuts)s, %(centros_trabajo_ine)s,
  %(ejecucion_nuts)s, %(ejecucion_ine)s, %(ejecucion_municipios_anexo)s,
  %(impacto_nuts)s, %(geo_patron)s, %(geo_condiciones_suficientes)s,
  %(nlp_json)s
)
ON CONFLICT (document_key) DO UPDATE SET
  document_source        = EXCLUDED.document_source,
  document_heuristic_step= EXCLUDED.document_heuristic_step,
  document_ref           = EXCLUDED.document_ref,
  schema_version         = EXCLUDED.schema_version,
  extracted_at           = NOW(),
  modelo_documental      = EXCLUDED.modelo_documental,
  input_char_count       = EXCLUDED.input_char_count,
  llm_model              = EXCLUDED.llm_model,
  validation_status      = EXCLUDED.validation_status,
  validation_errors      = EXCLUDED.validation_errors,
  modalidad_lgs          = EXCLUDED.modalidad_lgs,
  mecanismo_seleccion    = EXCLUDED.mecanismo_seleccion,
  puntuacion_minima_acceso = EXCLUDED.puntuacion_minima_acceso,
  regimen_ue_tipo        = EXCLUDED.regimen_ue_tipo,
  rgec_articulo          = EXCLUDED.rgec_articulo,
  tipo_beneficiario_fino = EXCLUDED.tipo_beneficiario_fino,
  texto_beneficiario_fino= EXCLUDED.texto_beneficiario_fino,
  admite_consorcio       = EXCLUDED.admite_consorcio,
  art13_lgs_verificado   = EXCLUDED.art13_lgs_verificado,
  intensidad_maxima_pct  = EXCLUDED.intensidad_maxima_pct,
  importe_maximo_beneficiario = EXCLUDED.importe_maximo_beneficiario,
  regla_calculo_ayuda    = EXCLUDED.regla_calculo_ayuda,
  modalidad_pago         = EXCLUDED.modalidad_pago,
  garantia_pago          = EXCLUDED.garantia_pago,
  regimen_acumulacion    = EXCLUDED.regimen_acumulacion,
  domicilio_fiscal_nuts  = EXCLUDED.domicilio_fiscal_nuts,
  domicilio_fiscal_ine   = EXCLUDED.domicilio_fiscal_ine,
  domicilio_fiscal_tipo  = EXCLUDED.domicilio_fiscal_tipo,
  centros_trabajo_nuts   = EXCLUDED.centros_trabajo_nuts,
  centros_trabajo_ine    = EXCLUDED.centros_trabajo_ine,
  ejecucion_nuts         = EXCLUDED.ejecucion_nuts,
  ejecucion_ine          = EXCLUDED.ejecucion_ine,
  ejecucion_municipios_anexo = EXCLUDED.ejecucion_municipios_anexo,
  impacto_nuts           = EXCLUDED.impacto_nuts,
  geo_patron             = EXCLUDED.geo_patron,
  geo_condiciones_suficientes = EXCLUDED.geo_condiciones_suficientes,
  nlp_json               = EXCLUDED.nlp_json
"""


NLP_COLS = [
    "modalidad_lgs", "mecanismo_seleccion", "puntuacion_minima_acceso",
    "regimen_ue_tipo", "rgec_articulo",
    "tipo_beneficiario_fino", "texto_beneficiario_fino", "admite_consorcio",
    "art13_lgs_verificado",
    "intensidad_maxima_pct", "importe_maximo_beneficiario",
    "regla_calculo_ayuda", "modalidad_pago", "garantia_pago", "regimen_acumulacion",
    "domicilio_fiscal_nuts", "domicilio_fiscal_ine", "domicilio_fiscal_tipo",
    "centros_trabajo_nuts", "centros_trabajo_ine",
    "ejecucion_nuts", "ejecucion_ine", "ejecucion_municipios_anexo",
    "impacto_nuts", "geo_patron",
    "geo_condiciones_suficientes",
]

NLP_JSONB_COLS = {"geo_condiciones_suficientes"}


UPDATE_NACIONAL_SQL = """
UPDATE l0.nacional_subvenciones
SET
  nlp_document_key       = %(document_key)s,
  nlp_validation_status  = %(validation_status)s,
  nlp_extracted_at       = %(nlp_extracted_at)s,
  modalidad_lgs              = %(modalidad_lgs)s,
  mecanismo_seleccion        = %(mecanismo_seleccion)s,
  puntuacion_minima_acceso   = %(puntuacion_minima_acceso)s,
  regimen_ue_tipo            = %(regimen_ue_tipo)s,
  rgec_articulo              = %(rgec_articulo)s,
  tipo_beneficiario_fino     = %(tipo_beneficiario_fino)s,
  texto_beneficiario_fino    = %(texto_beneficiario_fino)s,
  admite_consorcio           = %(admite_consorcio)s,
  art13_lgs_verificado       = %(art13_lgs_verificado)s,
  intensidad_maxima_pct      = %(intensidad_maxima_pct)s,
  importe_maximo_beneficiario = %(importe_maximo_beneficiario)s,
  regla_calculo_ayuda        = %(regla_calculo_ayuda)s,
  modalidad_pago             = %(modalidad_pago)s,
  garantia_pago              = %(garantia_pago)s,
  regimen_acumulacion        = %(regimen_acumulacion)s,
  domicilio_fiscal_nuts      = %(domicilio_fiscal_nuts)s,
  domicilio_fiscal_ine       = %(domicilio_fiscal_ine)s,
  domicilio_fiscal_tipo      = %(domicilio_fiscal_tipo)s,
  centros_trabajo_nuts       = %(centros_trabajo_nuts)s,
  centros_trabajo_ine        = %(centros_trabajo_ine)s,
  ejecucion_nuts             = %(ejecucion_nuts)s,
  ejecucion_ine              = %(ejecucion_ine)s,
  ejecucion_municipios_anexo = %(ejecucion_municipios_anexo)s,
  impacto_nuts               = %(impacto_nuts)s,
  geo_patron                 = %(geo_patron)s,
  geo_condiciones_suficientes = %(geo_condiciones_suficientes)s
WHERE id = %(subvencion_id)s
"""


SELECT_CACHE_SQL = """
SELECT
  document_key, validation_status, extracted_at,
  modalidad_lgs, mecanismo_seleccion, puntuacion_minima_acceso,
  regimen_ue_tipo, rgec_articulo,
  tipo_beneficiario_fino, texto_beneficiario_fino, admite_consorcio,
  art13_lgs_verificado,
  intensidad_maxima_pct, importe_maximo_beneficiario,
  regla_calculo_ayuda, modalidad_pago, garantia_pago, regimen_acumulacion,
  domicilio_fiscal_nuts, domicilio_fiscal_ine, domicilio_fiscal_tipo,
  centros_trabajo_nuts, centros_trabajo_ine,
  ejecucion_nuts, ejecucion_ine, ejecucion_municipios_anexo,
  impacto_nuts, geo_patron,
  geo_condiciones_suficientes
FROM l0.subvenciones_nlp
WHERE document_key = %s
  AND validation_status IN ('valid','partial')
"""


def _wrap_jsonb(col_name: str, value):
    if col_name in NLP_JSONB_COLS and value is not None:
        return Json(value)
    return value


def _fields_to_params(fields: Optional[MatchingFields]) -> dict:
    if fields is None:
        return {k: None for k in NLP_COLS}
    return {k: _wrap_jsonb(k, getattr(fields, k, None)) for k in NLP_COLS}


def persist_analysis(
    conn,
    *,
    subvencion_id: int,
    resolved: ResolvedDocument,
    validation: ValidationResult,
    fields: Optional[MatchingFields],
    nlp_json: Optional[dict],
    modelo_documental: Optional[str],
    input_char_count: int,
    llm_model: Optional[str],
    extracted_at,
) -> None:
    if validation.status == "invalid":
        raise ValueError("persist_analysis no debe llamarse con status='invalid'")

    schema_version = "v5.0.2"
    col_values = _fields_to_params(fields)

    params = {
        "document_key": resolved.document_key,
        "document_source": resolved.document_source,
        "document_heuristic_step": resolved.heuristic_step,
        "document_ref": resolved.document_ref,
        "schema_version": schema_version,
        "modelo_documental": modelo_documental,
        "input_char_count": input_char_count,
        "llm_model": llm_model,
        "validation_status": validation.status,
        "validation_errors": Json([
            {"path": e.path, "message": e.message} for e in validation.errors
        ]) if validation.errors else None,
        "nlp_extracted_at": extracted_at,
        "subvencion_id": subvencion_id,
        "nlp_json": Json(nlp_json or {}),
        **col_values,
    }

    with conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL, params)
            cur.execute(UPDATE_NACIONAL_SQL, params)


def propagate_from_cache(conn, *, subvencion_id: int, document_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(SELECT_CACHE_SQL, (document_key,))
        row = cur.fetchone()
        if row is None:
            return False
        col_names = [d.name for d in cur.description]
        cached = dict(zip(col_names, row))

    params = {
        "document_key": cached["document_key"],
        "validation_status": cached["validation_status"],
        "nlp_extracted_at": cached["extracted_at"],
        "subvencion_id": subvencion_id,
        **{k: cached[k] for k in NLP_COLS},
    }
    with conn:
        with conn.cursor() as cur:
            cur.execute(UPDATE_NACIONAL_SQL, params)
    return True
