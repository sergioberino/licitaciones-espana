import json
from pathlib import Path

from etl.nlp.schema import EstadoExtraccion, NlpBasesReguladoras
from etl.nlp.validator import validate, _check_partial_rules


def _minimal_block(**overrides):
    base = {
        "estado_extraccion": "no_encontrado",
        "descripcion_para_ficha": None,
        "confianza": None,
        "evidencias": [],
    }
    base.update(overrides)
    return base


def _minimal_document(**overrides) -> dict:
    doc = {
        "beneficiario_fino": _minimal_block(
            tipo_beneficiario_fino=None,
            texto_beneficiario_fino=None,
            formas_juridicas_excluidas=[],
            antiguedad_minima_meses=None,
            empleo_minimo_trabajadores=None,
            admite_consorcio=None,
            condiciones_consorcio=None,
            minimo_miembros_consorcio=None,
            maximo_miembros_consorcio=None,
            requiere_pyme_en_consorcio=None,
            porcentaje_maximo_por_entidad_o_grupo=None,
        ),
        "criterios_exclusion": _minimal_block(
            art13_lgs_verificado=False,
            exclusiones_especificas=[],
            exclusiones_criticas_para_matching=[],
        ),
        "requisitos_adicionales": _minimal_block(requisitos=[]),
        "criterios_seleccion": _minimal_block(
            modalidad_lgs_refuerzo=None,
            mecanismo_seleccion=None,
            modalidad_lgs_texto=None,
            coherencia_tipo_bdns="no_informado",
            orden_entrada_relevante=None,
            baremo=[],
            puntuacion_minima_acceso=None,
        ),
        "requisitos_geograficos": {
            "estado_extraccion": "no_encontrado",
            "estado_restriccion": "no_determinado",
            "G1": None,
            "G2": None,
            "G3": None,
            "G4": None,
            "condiciones_suficientes": [],
            "descripcion_logica": None,
            "confianza": None,
            "evidencias": [],
        },
        "economia_ayuda": _minimal_block(
            presupuesto_minimo_proyecto=None,
            presupuesto_maximo_proyecto=None,
            importe_maximo_por_beneficiario=None,
            importe_minimo_por_beneficiario=None,
            importe_minimo_solicitud=None,
            intensidad_maxima_pct=None,
            ayuda_minima_pct=None,
            regla_calculo_ayuda=None,
            modalidad_pago=None,
            garantia_pago=None,
            intensidades_detalle=[],
            categorias_gasto_elegible=[],
            gastos_excluidos=[],
            costes_indirectos_texto=None,
            anticipos_texto=None,
        ),
        "compatibilidad_acumulacion": _minimal_block(
            acumulable_con_otras_ayudas=None,
            compatibilidad_canonica=None,
            restricciones_acumulacion=[],
            regimen_acumulacion_detectado=None,
        ),
        "regimen_ue": _minimal_block(
            tipo_canonico=None,
            reglamento_canonico=None,
            articulos_rgec=[],
            texto_declarado=None,
            fuente_preferente="no_identificado",
            coherencia_bdns="no_comprobado",
        ),
        "objeto_actividades_elegibles": _minimal_block(
            ambitos_actividad=[],
            actividades_concretas=[],
            actividades_excluidas=[],
        ),
        "presentacion_solicitudes": _minimal_block(
            canal_presentacion=None,
            firma_electronica_requerida=None,
            requisitos_presentacion=[],
            efecto_incentivador=None,
            efecto_incentivador_texto=None,
        ),
        "documentacion_exigida": _minimal_block(
            documentos_obligatorios=[],
            documentos_condicionales=[],
            documentos_criticos_para_admisibilidad=[],
        ),
        "obligaciones_beneficiario": _minimal_block(
            obligaciones_principales=[],
            plazo_ejecucion=None,
            fecha_inicio_obligatoria=None,
            obligaciones_publicidad=[],
            obligaciones_control=[],
        ),
        "justificacion": _minimal_block(
            plazo_justificacion=None,
            modalidad_justificacion=None,
            documentos_justificacion=[],
            pago_texto=None,
            reintegro_texto=None,
        ),
        "lineas_ayuda": {
            "hay_lineas_diferenciadas": None,
            "lineas": [],
            "descripcion_para_ficha": None,
            "confianza": None,
            "evidencias": [],
        },
        "control_calidad": {
            "alertas": [],
            "texto_revision": None,
            "resumen_extraccion": None,
            "completitud_campos_criticos": "media",
        },
    }
    doc.update(overrides)
    return doc


def test_invalid_json_string():
    r = validate("not json at all")
    assert r.status == "invalid"
    assert len(r.errors) >= 1


def test_invalid_schema_dict():
    r = validate({"foo": "bar"})
    assert r.status == "invalid"
    assert r.model is None
    assert r.errors


def test_valid_fixture():
    fixture = Path(__file__).parent / "fixtures" / "example_v502_cdti.json"
    data = json.loads(fixture.read_text())
    r = validate(data)
    assert r.status in ("valid", "partial")
    assert r.model is not None


def test_partial_geo_explicito_sin_condiciones():
    data = _minimal_document()
    data["requisitos_geograficos"]["estado_extraccion"] = "explicito"
    data["requisitos_geograficos"]["estado_restriccion"] = "restriccion_detectada"
    model = NlpBasesReguladoras.model_validate(data)
    rg = model.requisitos_geograficos.model_copy(update={"condiciones_suficientes": None})
    model = model.model_copy(update={"requisitos_geograficos": rg})
    errors = _check_partial_rules(model)
    assert len(errors) == 1
    assert errors[0].path == "requisitos_geograficos.condiciones_suficientes"
    r = validate(data)
    assert r.status == "valid"
