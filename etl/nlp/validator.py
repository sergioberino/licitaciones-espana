"""Validación de output LLM contra schema v5.0.2 + reglas de calidad.

Tres estados:
  valid   → schema OK + reglas críticas OK
  partial → schema OK pero reglas críticas marcan incompletitud (ej. geo explicito sin NUTS)
  invalid → schema fail o JSON no parseable
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from pydantic import ValidationError as PydanticValidationError

from etl.nlp.schema import NlpBasesReguladoras, EstadoExtraccion


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationError:
    path: str
    message: str


@dataclass
class ValidationResult:
    status: str  # 'valid' | 'partial' | 'invalid'
    model: Optional[NlpBasesReguladoras] = None
    raw_dict: Optional[dict] = None
    errors: list[ValidationError] = field(default_factory=list)


def _parse_input(raw: Any) -> Optional[dict]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def _check_partial_rules(model: NlpBasesReguladoras) -> list[ValidationError]:
    """Reglas de calidad que degradan a 'partial' sin invalidar."""
    errors: list[ValidationError] = []
    rg = model.requisitos_geograficos
    if rg.estado_extraccion == EstadoExtraccion.explicito:
        cs = rg.condiciones_suficientes
        if cs is None:
            errors.append(ValidationError(
                path="requisitos_geograficos.condiciones_suficientes",
                message="estado_extraccion=explicito pero condiciones_suficientes=null",
            ))
    # Cuando el LLM marca completitud_campos_criticos='baja' (típico en portales,
    # índices o documentos que no son bases reguladoras), degradamos a partial
    # para no contaminar la cache de matching con análisis vacíos persistidos como
    # 'valid'. Ver regla v1 matcher: 'null en item → skip', así que partial es la
    # señalización correcta (no se propagan los nulls como verdad).
    if model.control_calidad.completitud_campos_criticos == "baja":
        errors.append(ValidationError(
            path="control_calidad.completitud_campos_criticos",
            message=(
                "completitud='baja' — análisis incompleto según LLM "
                "(posible portal/listado o documento no normativo)"
            ),
        ))
    return errors


def validate(raw: Any) -> ValidationResult:
    data = _parse_input(raw)
    if data is None:
        return ValidationResult(
            status="invalid",
            errors=[ValidationError(path="<root>", message="raw is not parseable JSON")],
        )
    try:
        model = NlpBasesReguladoras.model_validate(data)
    except PydanticValidationError as e:
        errs = [
            ValidationError(path=".".join(map(str, err["loc"])), message=err["msg"])
            for err in e.errors()
        ]
        return ValidationResult(status="invalid", raw_dict=data, errors=errs)

    partial_errors = _check_partial_rules(model)
    status = "partial" if partial_errors else "valid"
    return ValidationResult(status=status, model=model, raw_dict=data, errors=partial_errors)
