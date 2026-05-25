"""Proyección determinista NLP v5.0.2 → columnas relacionales para matching.

NO es un parser genérico: cada mapping es una ruta explícita del schema.
Cambios futuros en el schema (v5.0.3) actualizarán solo este fichero.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from etl.nlp.schema import NlpBasesReguladoras


@dataclass
class MatchingFields:
    modalidad_lgs: Optional[str] = None
    mecanismo_seleccion: Optional[str] = None
    puntuacion_minima_acceso: Optional[int] = None
    regimen_ue_tipo: Optional[str] = None
    rgec_articulo: Optional[str] = None
    tipo_beneficiario_fino: Optional[str] = None
    texto_beneficiario_fino: Optional[str] = None
    admite_consorcio: Optional[bool] = None
    art13_lgs_verificado: Optional[bool] = None
    intensidad_maxima_pct: Optional[float] = None
    importe_maximo_beneficiario: Optional[float] = None
    regla_calculo_ayuda: Optional[str] = None
    modalidad_pago: Optional[str] = None
    garantia_pago: Optional[str] = None
    regimen_acumulacion: Optional[str] = None

    domicilio_fiscal_nuts: list[str] = field(default_factory=list)
    domicilio_fiscal_ine: list[str] = field(default_factory=list)
    domicilio_fiscal_tipo: Optional[str] = None
    centros_trabajo_nuts: list[str] = field(default_factory=list)
    centros_trabajo_ine: list[str] = field(default_factory=list)
    ejecucion_nuts: list[str] = field(default_factory=list)
    ejecucion_ine: list[str] = field(default_factory=list)
    ejecucion_municipios_anexo: Optional[bool] = None
    impacto_nuts: list[str] = field(default_factory=list)
    geo_patron: Optional[str] = None
    geo_condiciones_suficientes: Optional[list[list[str]]] = None


def _categoria_to_tipo_beneficiario_fino(categorias: list[str]) -> Optional[str]:
    """Convierte categorias_beneficiario_fino (multi) a un único enum normalizado."""
    enum_oficial = {
        "autonomo", "microempresa", "pequena_empresa", "mediana_empresa",
        "gran_empresa", "no_determinado",
    }
    if not categorias:
        return None
    matches = [c for c in categorias if c in enum_oficial]
    if len(matches) == 1:
        return matches[0]
    return "otro"


def derive_geo_patron(condiciones_suficientes: Optional[list[list[str]]]) -> str:
    """Tabla de derivación del vault."""
    if condiciones_suficientes is None:
        return "sin_datos"
    if condiciones_suficientes == []:
        return "sin_restriccion"
    canon = tuple(tuple(sorted(set(g))) for g in condiciones_suficientes)
    canon_sorted = tuple(sorted(canon))
    if canon_sorted == (("G1",),):
        return "domicilio_fiscal"
    if canon_sorted == (("G2",),):
        return "centros_trabajo"
    if canon_sorted == (("G3",),):
        return "ejecucion"
    if canon_sorted == (("G4",),):
        return "impacto"
    if canon_sorted == (("G1", "G3"),):
        return "domicilio_fiscal_con_ejecucion"
    if canon_sorted == (("G1",), ("G2",)):
        return "domicilio_fiscal_o_centros_trabajo"
    if canon_sorted == (("G1", "G3"), ("G2",)):
        return "domicilio_fiscal_o_centros_trabajo_con_ejecucion"
    return "complejo"


def extract_matching_fields(model: NlpBasesReguladoras) -> MatchingFields:
    f = MatchingFields()

    bf = model.beneficiario_fino
    f.tipo_beneficiario_fino = bf.tipo_beneficiario_fino
    if f.tipo_beneficiario_fino == "otro":
        f.texto_beneficiario_fino = bf.texto_beneficiario_fino
    f.admite_consorcio = bf.admite_consorcio

    cs = model.criterios_seleccion
    f.modalidad_lgs = cs.modalidad_lgs_refuerzo
    f.mecanismo_seleccion = cs.mecanismo_seleccion
    if cs.puntuacion_minima_acceso is not None:
        f.puntuacion_minima_acceso = int(cs.puntuacion_minima_acceso)

    ru = model.regimen_ue
    f.regimen_ue_tipo = ru.tipo_canonico
    if ru.articulos_rgec:
        f.rgec_articulo = ru.articulos_rgec[0].articulo_canonico

    ea = model.economia_ayuda
    f.intensidad_maxima_pct = ea.intensidad_maxima_pct
    f.importe_maximo_beneficiario = ea.importe_maximo_por_beneficiario
    f.regla_calculo_ayuda = ea.regla_calculo_ayuda
    f.modalidad_pago = ea.modalidad_pago
    f.garantia_pago = ea.garantia_pago

    ce = model.criterios_exclusion
    f.art13_lgs_verificado = ce.art13_lgs_verificado

    ca = model.compatibilidad_acumulacion
    f.regimen_acumulacion = ca.regimen_acumulacion_detectado

    rg = model.requisitos_geograficos
    if rg.G1:
        f.domicilio_fiscal_nuts = list(rg.G1.codigos_nuts or [])
        f.domicilio_fiscal_ine = list(rg.G1.codigos_ine or [])
        f.domicilio_fiscal_tipo = rg.G1.tipo_geografia
    if rg.G2:
        f.centros_trabajo_nuts = list(rg.G2.codigos_nuts or [])
        f.centros_trabajo_ine = list(rg.G2.codigos_ine or [])
    if rg.G3:
        f.ejecucion_nuts = list(rg.G3.codigos_nuts or [])
        f.ejecucion_ine = list(rg.G3.codigos_ine or [])
        f.ejecucion_municipios_anexo = rg.G3.municipios_anexo
    if rg.G4:
        f.impacto_nuts = list(rg.G4.codigos_nuts or [])

    cs_geo = rg.condiciones_suficientes
    f.geo_patron = derive_geo_patron(cs_geo)
    if f.geo_patron == "complejo":
        f.geo_condiciones_suficientes = cs_geo

    return f
