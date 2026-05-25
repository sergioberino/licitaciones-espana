import json
from pathlib import Path

from etl.nlp.schema import NlpBasesReguladoras, SCHEMA_VERSION


def test_schema_version_constant():
    assert SCHEMA_VERSION == "v5.0.2"


def test_load_example_cdti():
    fixture = Path(__file__).parent / "fixtures" / "example_v502_cdti.json"
    data = json.loads(fixture.read_text())
    model = NlpBasesReguladoras.model_validate(data)
    assert model.beneficiario_fino.estado_extraccion is not None
