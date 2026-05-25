from datetime import datetime, timezone

import pytest

from etl.nlp.persistence import persist_analysis, propagate_from_cache
from etl.nlp.project import MatchingFields
from etl.nlp.resolver import ResolvedDocument
from etl.nlp.validator import ValidationResult


@pytest.mark.integration
def test_persist_valid_writes_cache_and_dual_write(pg_conn):
    cur = pg_conn.cursor()
    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id = -888888")
    cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key = 'url:testkey0000000000000000000000ab'")
    cur.execute("""
        INSERT INTO l0.nacional_subvenciones (id, descripcion, url_bases_reguladoras)
        VALUES (-888888, 'test', 'https://example.org/bases')
    """)
    pg_conn.commit()

    now = datetime.now(timezone.utc)

    persist_analysis(
        pg_conn,
        subvencion_id=-888888,
        resolved=ResolvedDocument(
            document_key="url:testkey0000000000000000000000ab",
            document_source="url_bases_reguladoras",
            heuristic_step=1,
            document_ref="https://example.org/bases",
        ),
        validation=ValidationResult(status="valid"),
        fields=MatchingFields(
            geo_patron="sin_restriccion",
            modalidad_lgs="concurrencia_competitiva",
            tipo_beneficiario_fino="pequena_empresa",
        ),
        nlp_json={"_test": True},
        modelo_documental=None,
        input_char_count=1234,
        llm_model="test",
        extracted_at=now,
    )

    cur.execute(
        "SELECT geo_patron, validation_status, modalidad_lgs FROM l0.subvenciones_nlp WHERE document_key=%s",
        ("url:testkey0000000000000000000000ab",),
    )
    assert cur.fetchone() == ("sin_restriccion", "valid", "concurrencia_competitiva")

    cur.execute("""
        SELECT nlp_document_key, nlp_validation_status, geo_patron, modalidad_lgs, tipo_beneficiario_fino
        FROM l0.nacional_subvenciones WHERE id=-888888
    """)
    assert cur.fetchone() == (
        "url:testkey0000000000000000000000ab", "valid", "sin_restriccion",
        "concurrencia_competitiva", "pequena_empresa",
    )

    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id = -888888")
    cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key = 'url:testkey0000000000000000000000ab'")
    pg_conn.commit()


@pytest.mark.integration
def test_persist_partial_propagates_with_nulls(pg_conn):
    cur = pg_conn.cursor()
    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id = -888889")
    cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key = 'url:testkey0000000000000000000000cd'")
    cur.execute("""
        INSERT INTO l0.nacional_subvenciones (id, descripcion, url_bases_reguladoras)
        VALUES (-888889, 'test', 'https://example.org/bases2')
    """)
    pg_conn.commit()

    now = datetime.now(timezone.utc)

    persist_analysis(
        pg_conn,
        subvencion_id=-888889,
        resolved=ResolvedDocument(
            document_key="url:testkey0000000000000000000000cd",
            document_source="url_bases_reguladoras",
            heuristic_step=1,
            document_ref="https://example.org/bases2",
        ),
        validation=ValidationResult(status="partial"),
        fields=MatchingFields(geo_patron="domicilio_fiscal", modalidad_pago=None),
        nlp_json={"_test": "partial"},
        modelo_documental=None,
        input_char_count=1234,
        llm_model="test",
        extracted_at=now,
    )

    cur.execute(
        "SELECT validation_status, geo_patron FROM l0.subvenciones_nlp WHERE document_key=%s",
        ("url:testkey0000000000000000000000cd",),
    )
    assert cur.fetchone() == ("partial", "domicilio_fiscal")

    cur.execute("""
        SELECT nlp_document_key, nlp_validation_status, geo_patron, modalidad_pago
        FROM l0.nacional_subvenciones WHERE id=-888889
    """)
    assert cur.fetchone() == (
        "url:testkey0000000000000000000000cd", "partial", "domicilio_fiscal", None,
    )

    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id = -888889")
    cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key = 'url:testkey0000000000000000000000cd'")
    pg_conn.commit()


@pytest.mark.integration
def test_propagate_from_cache_hit(pg_conn):
    cur = pg_conn.cursor()
    dk = "url:testkey0000000000000000000000ef"
    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN (-777771, -777772)")
    cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key=%s", (dk,))

    cur.execute("""
        INSERT INTO l0.subvenciones_nlp
          (document_key, document_source, document_heuristic_step, validation_status,
           geo_patron, modalidad_lgs, tipo_beneficiario_fino, nlp_json)
        VALUES (%s, 'url_bases_reguladoras', 1, 'valid',
                'domicilio_fiscal', 'concurrencia_competitiva', 'pequena_empresa', '{}'::jsonb)
    """, (dk,))
    cur.execute("""
        INSERT INTO l0.nacional_subvenciones (id, descripcion) VALUES (-777771, 'a'), (-777772, 'b')
    """)
    pg_conn.commit()

    assert propagate_from_cache(pg_conn, subvencion_id=-777771, document_key=dk) is True
    assert propagate_from_cache(pg_conn, subvencion_id=-777772, document_key=dk) is True

    cur.execute("""
        SELECT id, nlp_document_key, geo_patron, modalidad_lgs
        FROM l0.nacional_subvenciones
        WHERE id IN (-777771, -777772) ORDER BY id
    """)
    rows = cur.fetchall()
    assert rows == [
        (-777772, dk, "domicilio_fiscal", "concurrencia_competitiva"),
        (-777771, dk, "domicilio_fiscal", "concurrencia_competitiva"),
    ]

    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN (-777771, -777772)")
    cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key=%s", (dk,))
    pg_conn.commit()


@pytest.mark.integration
def test_propagate_from_cache_miss(pg_conn):
    assert propagate_from_cache(
        pg_conn, subvencion_id=-1, document_key="url:doesnotexist0000000000000000ff"
    ) is False
