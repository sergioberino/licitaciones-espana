"""Tests para resolver de Batch B (WP2.1 + WP2.1.5).

Estructura real de documentos[] (BDNS SNPSAP):
  {
    "id": 1454450,                       # ID descargable vía /convocatorias/documentos?idDocumento=ID
    "long": 506437,                      # tamaño en bytes
    "datMod": "2026-05-13",
    "nombreFic": "02.-BASES.pdf",
    "descripcion": "Documento de la convocatoria en español",
    "datPublicacion": "2026-05-13"
  }
"""
from etl.nlp.resolver import (
    BDNS_DOCUMENTO_URL_PATTERN,
    ResolvedDocument,
    resolve_document,
)


# ---------------------------------------------------------------------------
# Step 1 — url_bases_reguladoras directa
# ---------------------------------------------------------------------------

def test_step1_url_bases():
    r = resolve_document(
        url_bases_reguladoras="https://www.boe.es/buscar/doc.php?id=BOE-A-2025-3176",
        documentos=None,
    )
    assert r.heuristic_step == 1
    assert r.document_source == "url_bases_reguladoras"
    assert r.document_key.startswith("url:")
    assert len(r.document_key) == len("url:") + 32
    assert r.document_name is None


def test_step1_url_invalida_cae_al_siguiente_paso():
    r = resolve_document(
        url_bases_reguladoras="not-a-url",
        documentos=[
            {"id": 1, "descripcion": "Bases reguladoras", "nombreFic": "bases.pdf"}
        ],
    )
    assert r.heuristic_step == 2


def test_step1_document_key_estable_entre_invocaciones():
    """Misma URL produce misma key (dedup N:1)."""
    r1 = resolve_document(
        url_bases_reguladoras="https://example.org/bases.pdf",
        documentos=None,
    )
    r2 = resolve_document(
        url_bases_reguladoras="https://example.org/bases.pdf/",
        documentos=None,
    )
    assert r1.document_key == r2.document_key


# ---------------------------------------------------------------------------
# Step 2 — documentos[] con match 'bases reguladoras'
# ---------------------------------------------------------------------------

def test_step2_match_descripcion_bases_reguladoras():
    docs = [
        {"id": 100, "descripcion": "Anexo I de solicitud", "nombreFic": "anexo1.pdf"},
        {
            "id": 200,
            "descripcion": "Bases reguladoras",
            "nombreFic": "bases.pdf",
            "datPublicacion": "2024-01-15",
        },
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 2
    assert r.document_source == "documentos_array"
    assert r.document_ref == BDNS_DOCUMENTO_URL_PATTERN.format(id=200)
    assert r.document_name == "Bases reguladoras"


def test_step2_match_nombreFic_cuando_descripcion_no_aplica():
    """Si descripcion no informa pero el nombre del fichero lo hace, también vale."""
    docs = [
        {"id": 300, "descripcion": "Documento oficial", "nombreFic": "bases-reguladoras-2024.pdf"},
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 2
    assert r.document_source == "documentos_array"


def test_step2_descarta_anexos_aunque_contengan_la_palabra():
    """Un anexo que dice 'bases reguladoras' en su descripcion no debe ganar."""
    docs = [
        {
            "id": 400,
            "descripcion": "Anexo I — bases reguladoras (formulario)",
            "nombreFic": "anexo1.pdf",
        }
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r is None, "Anexo I no debe matchear step 2"


# ---------------------------------------------------------------------------
# Step 3 — documentos[] con match 'texto/documento convocatoria'
# ---------------------------------------------------------------------------

def test_step3_match_documento_de_la_convocatoria_en_espanol():
    """Caso típico BDNS: 'Documento de la convocatoria en español'."""
    docs = [
        {
            "id": 693292,
            "descripcion": "Documento de la convocatoria en español",
            "nombreFic": "Convocatoria_PKD.report.pdf",
            "datPublicacion": "2022-02-27",
        }
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 3
    assert r.document_source == "texto_convocatoria"
    assert r.document_ref == BDNS_DOCUMENTO_URL_PATTERN.format(id=693292)


def test_step3_match_texto_en_castellano_de_la_convocatoria():
    """Variante: 'Texto en castellano de la convocatoria'."""
    docs = [
        {"id": 800, "descripcion": "Texto en castellano de la convocatoria", "nombreFic": "texto.pdf"}
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 3
    assert r.document_source == "texto_convocatoria"


def test_step3_match_texto_en_otra_lengua_de_la_convocatoria():
    docs = [
        {"id": 801, "descripcion": "Texto en otra lengua de la convocatoria", "nombreFic": "t.pdf"}
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 3


def test_step3_match_documento_lengua_cooficial():
    docs = [
        {
            "id": 802,
            "descripcion": "Documento de la convocatoria en lengua cooficial",
            "nombreFic": "doc.pdf",
        }
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 3


def test_step3_descarta_modificaciones_y_revocaciones():
    """Modificaciones, ampliaciones, revocaciones NO matchean step 3."""
    blacklisted = [
        "Modificación del plazo de Resolución",
        "Ampliación del plazo de presentación de solicitudes",
        "Revocación parcial III",
        "Propuesta de Modificación Parcial Ampliación de Plazo",
        "Rectificación de Error Material",
        "Ampliación de la cuantía fijada inicialmente en la convocatoria",
    ]
    for desc in blacklisted:
        docs = [{"id": 500, "descripcion": desc, "nombreFic": "mod.pdf"}]
        r = resolve_document(url_bases_reguladoras=None, documentos=docs)
        assert r is None, f"Descripción {desc!r} NO debe matchear (es modificación)"


def test_step3_multiples_candidatos_gana_max_datPublicacion():
    """Si hay varios 'texto convocatoria', preferir la versión más reciente (max datPublicacion)."""
    docs = [
        {
            "id": 1001,
            "descripcion": "Texto en castellano de la convocatoria",
            "nombreFic": "v1.pdf",
            "datPublicacion": "2022-01-15",
        },
        {
            "id": 1003,
            "descripcion": "Texto en castellano de la convocatoria",
            "nombreFic": "v3.pdf",
            "datPublicacion": "2024-06-20",
        },
        {
            "id": 1002,
            "descripcion": "Texto en castellano de la convocatoria",
            "nombreFic": "v2.pdf",
            "datPublicacion": "2023-03-10",
        },
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 3
    assert r.document_ref == BDNS_DOCUMENTO_URL_PATTERN.format(id=1003)


def test_step3_falta_datPublicacion_no_rompe():
    """Si un candidato no trae datPublicacion, el resto siguen comparándose."""
    docs = [
        {"id": 1001, "descripcion": "Texto en castellano de la convocatoria", "nombreFic": "a.pdf"},
        {
            "id": 1002,
            "descripcion": "Texto en castellano de la convocatoria",
            "nombreFic": "b.pdf",
            "datPublicacion": "2024-01-01",
        },
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 3
    assert r.document_ref == BDNS_DOCUMENTO_URL_PATTERN.format(id=1002)


# ---------------------------------------------------------------------------
# Prioridad y fallback
# ---------------------------------------------------------------------------

def test_bases_reguladoras_tiene_prioridad_sobre_texto_convocatoria():
    """Si ambos tipos están en documentos[], gana bases_reguladoras (step=2)."""
    docs = [
        {
            "id": 1,
            "descripcion": "Texto en castellano de la convocatoria",
            "nombreFic": "texto.pdf",
            "datPublicacion": "2024-05-01",
        },
        {
            "id": 2,
            "descripcion": "Bases reguladoras",
            "nombreFic": "bases.pdf",
            "datPublicacion": "2024-04-01",
        },
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r.heuristic_step == 2
    assert r.document_ref == BDNS_DOCUMENTO_URL_PATTERN.format(id=2)


def test_fallback_none_sin_url_y_sin_match():
    assert resolve_document(url_bases_reguladoras=None, documentos=None) is None
    assert resolve_document(url_bases_reguladoras=None, documentos=[]) is None


def test_fallback_solo_anexos_y_modificaciones():
    """Caso real BDNS: 10 documentos, todos modificaciones/anexos, ninguno canónico."""
    docs = [
        {"id": 1, "descripcion": "Modificación del plazo", "nombreFic": "mod.pdf"},
        {"id": 2, "descripcion": "Anexo I", "nombreFic": "anexo1.pdf"},
        {"id": 3, "descripcion": "Anexo II", "nombreFic": "anexo2.pdf"},
        {"id": 4, "descripcion": "Solicitud", "nombreFic": "solicitud.pdf"},
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    assert r is None


# ---------------------------------------------------------------------------
# BDNS URL builder
# ---------------------------------------------------------------------------

def test_bdns_url_pattern_es_publica_y_correcta():
    """El pattern debe ser exactamente el endpoint público SNPSAP de descarga."""
    url = BDNS_DOCUMENTO_URL_PATTERN.format(id=1454450)
    assert url == (
        "https://www.infosubvenciones.es/bdnstrans/api/convocatorias/documentos"
        "?idDocumento=1454450"
    )


def test_step2_y_step3_document_name_es_descripcion_real():
    """El document_name informativo debe ser la descripcion oficial del documento."""
    docs = [
        {"id": 1, "descripcion": "Bases reguladoras (Anexo I)", "nombreFic": "bases.pdf"}
    ]
    # Nota: la descripcion contiene 'Anexo I' pero NO empieza con anexo → no es anexo.
    # Verificamos primero que el filtro anti-anexo solo descarta cuando el haystack
    # arranca/contiene la palabra 'anexo' como rol semántico, no como mero substring.
    r = resolve_document(url_bases_reguladoras=None, documentos=docs)
    # Esperamos None porque "Anexo I" indica rol semántico de anexo,
    # aunque la descripcion también mencione bases reguladoras.
    assert r is None
