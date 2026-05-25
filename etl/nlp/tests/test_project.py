import json
from pathlib import Path

import pytest

from etl.nlp.project import derive_geo_patron, _categoria_to_tipo_beneficiario_fino, extract_matching_fields
from etl.nlp.schema import NlpBasesReguladoras


@pytest.mark.parametrize("cs, expected", [
    (None,                              "sin_datos"),
    ([],                                "sin_restriccion"),
    ([["G1"]],                          "domicilio_fiscal"),
    ([["G2"]],                          "centros_trabajo"),
    ([["G3"]],                          "ejecucion"),
    ([["G4"]],                          "impacto"),
    ([["G1", "G3"]],                    "domicilio_fiscal_con_ejecucion"),
    ([["G1"], ["G2"]],                  "domicilio_fiscal_o_centros_trabajo"),
    ([["G1", "G3"], ["G2"]],            "domicilio_fiscal_o_centros_trabajo_con_ejecucion"),
    ([["G1", "G2", "G3"]],              "complejo"),
    ([["G1"], ["G2", "G3"]],            "complejo"),
])
def test_derive_geo_patron(cs, expected):
    assert derive_geo_patron(cs) == expected


@pytest.mark.parametrize("cats, expected", [
    ([],                                None),
    (["pequena_empresa"],               "pequena_empresa"),
    (["microempresa", "pequena_empresa", "mediana_empresa", "gran_empresa"], "otro"),
    (["Consorcio de I+D"],              "otro"),
])
def test_categoria_to_tipo_beneficiario_fino(cats, expected):
    assert _categoria_to_tipo_beneficiario_fino(cats) == expected


def test_extract_matching_fields_from_fixture():
    fixture = Path(__file__).parent / "fixtures" / "example_v502_cdti.json"
    model = NlpBasesReguladoras.model_validate(json.loads(fixture.read_text()))
    fields = extract_matching_fields(model)
    assert fields.tipo_beneficiario_fino == "otro"
    assert fields.admite_consorcio is True
    assert fields.modalidad_lgs == "concurrencia_competitiva"
    assert fields.geo_patron == "domicilio_fiscal"
    assert fields.regimen_ue_tipo == "rgec"
