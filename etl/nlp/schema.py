"""Modelos Pydantic para output NLP v5.0.2 de bases reguladoras.

Generados a partir del JSON Schema 'bases_reguladoras_analisis_v5_0_2' del vault.
Estos tipos son contrato estable; cualquier cambio del schema requiere bump (v5.0.3)
y mantenimiento del adapter de proyección (etl/nlp/project.py).
"""
from __future__ import annotations

from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = "v5.0.2"


class EstadoExtraccion(str, Enum):
    explicito = "explicito"
    inferido = "inferido"
    no_encontrado = "no_encontrado"
    no_aplica = "no_aplica"
    contradictorio = "contradictorio"
    remite_a_otro_documento = "remite_a_otro_documento"


class DocumentoEvidencia(str, Enum):
    bases_reguladoras = "bases_reguladoras"
    texto_convocatoria = "texto_convocatoria"
    anexo = "anexo"
    extracto_bdns = "extracto_bdns"
    convocatoria_detalle_bdns = "convocatoria_detalle_bdns"
    diario_oficial = "diario_oficial"
    otra_norma = "otra_norma"
    no_identificado = "no_identificado"


class Evidencia(BaseModel):
    model_config = ConfigDict(extra="forbid")

    documento: DocumentoEvidencia
    localizador: Optional[str] = None
    cita_breve: Optional[str] = None


class BaremoItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    criterio: str
    puntos_max: Optional[float] = None
    detalle: Optional[str] = None


class IntensidadDetalle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tipo_beneficiario: Optional[str] = None
    actividad_o_gasto: Optional[str] = None
    intensidad_pct: Optional[float] = None
    condiciones: Optional[str] = None


class ArticuloRgec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    articulo_canonico: str
    texto: Optional[str] = None


class LineaAyuda(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nombre: Optional[str] = None
    descripcion: Optional[str] = None
    beneficiarios_texto: Optional[str] = None
    economia_texto: Optional[str] = None
    notas_matching: Optional[str] = None


class GeoG1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tipo_geografia: Literal[
        "domicilio_fiscal", "sede_social", "residencia", "establecimiento_sucursal"
    ]
    territorios_texto: list[str] = Field(default_factory=list)
    codigos_nuts: list[str] = Field(default_factory=list)
    codigos_ine: list[str] = Field(default_factory=list)
    origen_codigos: Literal[
        "declarados_en_fuente", "inferidos_por_modelo", "no_codificados", "mixto"
    ]
    texto_requisito: Optional[str] = None


class GeoG2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tipo_geografia: Literal["centro_trabajo"]
    territorios_texto: list[str] = Field(default_factory=list)
    codigos_nuts: list[str] = Field(default_factory=list)
    codigos_ine: list[str] = Field(default_factory=list)
    origen_codigos: Literal[
        "declarados_en_fuente", "inferidos_por_modelo", "no_codificados", "mixto"
    ]
    texto_requisito: Optional[str] = None


class GeoG3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tipo_geografia: Literal["ejecucion_inversion"]
    territorios_texto: list[str] = Field(default_factory=list)
    codigos_nuts: list[str] = Field(default_factory=list)
    codigos_ine: list[str] = Field(default_factory=list)
    origen_codigos: Literal[
        "declarados_en_fuente", "inferidos_por_modelo", "no_codificados", "mixto"
    ]
    municipios_anexo: Optional[bool] = None
    texto_requisito: Optional[str] = None


class GeoG4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tipo_geografia: Literal["impacto_territorial"]
    territorios_texto: list[str] = Field(default_factory=list)
    codigos_nuts: list[str] = Field(default_factory=list)
    codigos_ine: list[str] = Field(default_factory=list)
    origen_codigos: Literal[
        "declarados_en_fuente", "inferidos_por_modelo", "no_codificados", "mixto"
    ]
    texto_requisito: Optional[str] = None


class RequisitoAdicional(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tipo_requisito: str
    descripcion: str
    es_filtro_duro: Optional[bool] = None


class BeneficiarioFino(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    tipo_beneficiario_fino: Optional[str] = None
    texto_beneficiario_fino: Optional[str] = None
    formas_juridicas_excluidas: list[str] = Field(default_factory=list)
    antiguedad_minima_meses: Optional[int] = None
    empleo_minimo_trabajadores: Optional[int] = None
    admite_consorcio: Optional[bool] = None
    condiciones_consorcio: Optional[str] = None
    minimo_miembros_consorcio: Optional[int] = None
    maximo_miembros_consorcio: Optional[int] = None
    requiere_pyme_en_consorcio: Optional[bool] = None
    porcentaje_maximo_por_entidad_o_grupo: Optional[float] = None
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class CriteriosExclusion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    art13_lgs_verificado: bool
    exclusiones_especificas: list[str] = Field(default_factory=list)
    exclusiones_criticas_para_matching: list[str] = Field(default_factory=list)
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class RequisitosAdicionales(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    requisitos: list[RequisitoAdicional] = Field(default_factory=list)
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class CriteriosSeleccion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    modalidad_lgs_refuerzo: Optional[str] = None
    mecanismo_seleccion: Optional[str] = None
    modalidad_lgs_texto: Optional[str] = None
    coherencia_tipo_bdns: str
    orden_entrada_relevante: Optional[bool] = None
    baremo: list[BaremoItem] = Field(default_factory=list)
    puntuacion_minima_acceso: Optional[float] = None
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class RequisitosGeograficos(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    estado_restriccion: Literal[
        "sin_restriccion_explicita", "restriccion_detectada", "no_determinado"
    ]
    G1: Optional[GeoG1] = None
    G2: Optional[GeoG2] = None
    G3: Optional[GeoG3] = None
    G4: Optional[GeoG4] = None
    condiciones_suficientes: list[list[str]] = Field(default_factory=list)
    descripcion_logica: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class EconomiaAyuda(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    presupuesto_minimo_proyecto: Optional[float] = None
    presupuesto_maximo_proyecto: Optional[float] = None
    importe_maximo_por_beneficiario: Optional[float] = None
    importe_minimo_por_beneficiario: Optional[float] = None
    importe_minimo_solicitud: Optional[float] = None
    intensidad_maxima_pct: Optional[float] = None
    ayuda_minima_pct: Optional[float] = None
    regla_calculo_ayuda: Optional[str] = None
    modalidad_pago: Optional[str] = None
    garantia_pago: Optional[str] = None
    intensidades_detalle: list[IntensidadDetalle] = Field(default_factory=list)
    categorias_gasto_elegible: list[str] = Field(default_factory=list)
    gastos_excluidos: list[str] = Field(default_factory=list)
    costes_indirectos_texto: Optional[str] = None
    anticipos_texto: Optional[str] = None
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class CompatibilidadAcumulacion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    acumulable_con_otras_ayudas: Optional[bool] = None
    compatibilidad_canonica: Optional[str] = None
    restricciones_acumulacion: list[str] = Field(default_factory=list)
    regimen_acumulacion_detectado: Optional[str] = None
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class RegimenUe(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    tipo_canonico: Optional[str] = None
    reglamento_canonico: Optional[str] = None
    articulos_rgec: list[ArticuloRgec] = Field(default_factory=list)
    texto_declarado: Optional[str] = None
    fuente_preferente: str
    coherencia_bdns: str
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class ObjetoActividadesElegibles(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    ambitos_actividad: list[str] = Field(default_factory=list)
    actividades_concretas: list[str] = Field(default_factory=list)
    actividades_excluidas: list[str] = Field(default_factory=list)
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class PresentacionSolicitudes(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    canal_presentacion: Optional[str] = None
    firma_electronica_requerida: Optional[bool] = None
    requisitos_presentacion: list[str] = Field(default_factory=list)
    efecto_incentivador: Optional[bool] = None
    efecto_incentivador_texto: Optional[str] = None
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class DocumentacionExigida(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    documentos_obligatorios: list[str] = Field(default_factory=list)
    documentos_condicionales: list[str] = Field(default_factory=list)
    documentos_criticos_para_admisibilidad: list[str] = Field(default_factory=list)
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class ObligacionesBeneficiario(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    obligaciones_principales: list[str] = Field(default_factory=list)
    plazo_ejecucion: Optional[str] = None
    fecha_inicio_obligatoria: Optional[str] = None
    obligaciones_publicidad: list[str] = Field(default_factory=list)
    obligaciones_control: list[str] = Field(default_factory=list)
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class Justificacion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    estado_extraccion: EstadoExtraccion
    plazo_justificacion: Optional[str] = None
    modalidad_justificacion: Optional[str] = None
    documentos_justificacion: list[str] = Field(default_factory=list)
    pago_texto: Optional[str] = None
    reintegro_texto: Optional[str] = None
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class LineasAyuda(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hay_lineas_diferenciadas: Optional[bool] = None
    lineas: list[LineaAyuda] = Field(default_factory=list)
    descripcion_para_ficha: Optional[str] = None
    confianza: Optional[float] = None
    evidencias: list[Evidencia] = Field(default_factory=list)


class ControlCalidad(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alertas: list[str] = Field(default_factory=list)
    texto_revision: Optional[str] = None
    resumen_extraccion: Optional[str] = None
    completitud_campos_criticos: Literal["alta", "media", "baja"]


class NlpBasesReguladoras(BaseModel):
    """Documento NLP v5.0.2 completo."""

    model_config = ConfigDict(extra="forbid")

    beneficiario_fino: BeneficiarioFino
    criterios_exclusion: CriteriosExclusion
    requisitos_adicionales: RequisitosAdicionales
    criterios_seleccion: CriteriosSeleccion
    requisitos_geograficos: RequisitosGeograficos
    economia_ayuda: EconomiaAyuda
    compatibilidad_acumulacion: CompatibilidadAcumulacion
    regimen_ue: RegimenUe
    objeto_actividades_elegibles: ObjetoActividadesElegibles
    presentacion_solicitudes: PresentacionSolicitudes
    documentacion_exigida: DocumentacionExigida
    obligaciones_beneficiario: ObligacionesBeneficiario
    justificacion: Justificacion
    lineas_ayuda: LineasAyuda
    control_calidad: ControlCalidad
