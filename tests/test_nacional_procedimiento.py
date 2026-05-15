"""PLACSP / CODICE: códigos de procedimiento y etiquetas en L0 nacional."""

import xml.etree.ElementTree as ET

import pytest

from nacional.licitaciones import (
    PROCEDIMIENTOS,
    normalize_procedimiento_code,
    parsear_entry,
    parsear_entry_cpm,
    procedimiento_etiqueta,
)


class TestNormalizeProcedimientoCode:
    def test_none(self):
        assert normalize_procedimiento_code(None) is None

    def test_empty(self):
        assert normalize_procedimiento_code("") is None
        assert normalize_procedimiento_code("   ") is None

    def test_digits(self):
        assert normalize_procedimiento_code("3") == 3
        assert normalize_procedimiento_code("100") == 100

    def test_non_numeric(self):
        assert normalize_procedimiento_code("N3") is None


class TestProcedimientoEtiqueta:
    def test_placsp_three_four(self):
        assert procedimiento_etiqueta(3) == "Negociado sin publicidad"
        assert procedimiento_etiqueta(4) == "Negociado con publicidad"

    def test_one_to_thirteen(self):
        assert procedimiento_etiqueta(1) == "Abierto"
        assert procedimiento_etiqueta(2) == "Restringido"
        assert procedimiento_etiqueta(5) == "Diálogo competitivo"
        assert procedimiento_etiqueta(6) == "Contrato Menor"
        assert procedimiento_etiqueta(7) == "Basado en Acuerdo Marco"
        assert procedimiento_etiqueta(8) == "Concurso de proyectos"
        assert procedimiento_etiqueta(9) == "Abierto simplificado"
        assert procedimiento_etiqueta(10) == "Asociación para la innovación"
        assert procedimiento_etiqueta(13) == "Licitación con negociación"

    def test_extensions_retained(self):
        assert procedimiento_etiqueta(100) == "Normas Internas"
        assert procedimiento_etiqueta(999) == "Otros"

    def test_unknown_numeric_fallback(self):
        assert procedimiento_etiqueta(42) == "42"

    def test_none_label(self):
        assert procedimiento_etiqueta(None) is None


class TestProcedimientosDictCompleteness:
    """Catálogo explícito debe coincidir con TenderingProcessCode-2.08.gc."""

    def test_expected_keys(self):
        assert set(PROCEDIMIENTOS) == {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 100, 999
        }


def _entry_xml_with_procedure(code: str) -> ET.Element:
    xml = f"""<entry xmlns="http://www.w3.org/2005/Atom"
 xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2"
 xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2"
 xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2"
 xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2">
  <id>https://example.test/proc-{code}</id>
  <link href="https://contrataciondelestado.es/wps/poc?uri=deeplink:detalle_licitacion&amp;idEvl=PROC{code}"/>
  <updated>2026-04-27T10:00:00+02:00</updated>
  <cac-place-ext:ContractFolderStatus>
    <cbc:ContractFolderID>TEST/PROC/{code}</cbc:ContractFolderID>
    <cbc-place-ext:ContractFolderStatusCode>PUB</cbc-place-ext:ContractFolderStatusCode>
    <cac-place-ext:LocatedContractingParty>
      <cac:Party><cac:PartyName><cbc:Name>Órgano test</cbc:Name></cac:PartyName></cac:Party>
    </cac-place-ext:LocatedContractingParty>
    <cac:ProcurementProject>
      <cbc:Name>Objeto test</cbc:Name>
      <cbc:TypeCode>2</cbc:TypeCode>
    </cac:ProcurementProject>
    <cac:TenderingProcess>
      <cbc:ProcedureCode>{code}</cbc:ProcedureCode>
    </cac:TenderingProcess>
  </cac-place-ext:ContractFolderStatus>
</entry>"""
    return ET.fromstring(xml)


class TestParsearEntryProcedureIntegration:
    @pytest.mark.parametrize(
        "code,expected_label",
        [
            ("3", "Negociado sin publicidad"),
            ("4", "Negociado con publicidad"),
            ("7", "Basado en Acuerdo Marco"),
            ("8", "Concurso de proyectos"),
            ("9", "Abierto simplificado"),
            ("100", "Normas Internas"),
            ("999", "Otros"),
        ],
    )
    def test_procedure_label(self, code, expected_label):
        entry = _entry_xml_with_procedure(code)
        out = parsear_entry(entry)
        assert out is not None
        assert out["procedimiento_code"] == int(code)
        assert out["procedimiento"] == expected_label


class TestParsearEntryCpmProcedure:
    def test_cpm_procedure_int_and_label(self):
        xml = """<entry xmlns="http://www.w3.org/2005/Atom"
 xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2"
 xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2"
 xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2"
 xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2">
  <id>https://example.test/cpm-3</id>
  <link href="https://example.test/cpm"/>
  <updated>2026-04-27T10:00:00+02:00</updated>
  <cac-place-ext:PreliminaryMarketConsultationStatus>
    <cbc:PreliminaryMarketConsultationID>CPM/3</cbc:PreliminaryMarketConsultationID>
    <cbc-place-ext:PreliminaryMarketConsultationStatusCode>PUB</cbc-place-ext:PreliminaryMarketConsultationStatusCode>
    <cac-place-ext:LocatedContractingParty>
      <cac:Party><cac:PartyName><cbc:Name>O</cbc:Name></cac:PartyName></cac:Party>
    </cac-place-ext:LocatedContractingParty>
    <cac:ProcurementProject><cbc:Name>C</cbc:Name><cbc:TypeCode>2</cbc:TypeCode></cac:ProcurementProject>
    <cac:TenderingProcess><cbc:ProcedureCode>3</cbc:ProcedureCode></cac:TenderingProcess>
  </cac-place-ext:PreliminaryMarketConsultationStatus>
</entry>"""
        out = parsear_entry_cpm(ET.fromstring(xml))
        assert out["procedimiento_code"] == 3
        assert out["procedimiento"] == "Negociado sin publicidad"
