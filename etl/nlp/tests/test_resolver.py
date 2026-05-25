from etl.nlp.resolver import resolve_document, ResolvedDocument


def test_step1_url_bases():
    r = resolve_document(
        url_bases_reguladoras="https://www.boe.es/buscar/doc.php?id=BOE-A-2025-3176",
        documentos=None,
        texto_reguladora=None,
    )
    assert r.heuristic_step == 1
    assert r.document_source == "url_bases_reguladoras"
    assert r.document_key.startswith("url:")
    assert len(r.document_key) == len("url:") + 32


def test_step2_documentos_array_match():
    docs = [
        {"tipo": "Anexo", "url": "https://example.org/anexo.pdf"},
        {"tipo": "Bases reguladoras", "url": "https://example.org/bases.pdf"},
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs, texto_reguladora=None)
    assert r.heuristic_step == 2
    assert r.document_source == "documentos_array"
    assert r.document_ref == "https://example.org/bases.pdf"


def test_step3_texto_reguladora():
    r = resolve_document(url_bases_reguladoras=None, documentos=None, texto_reguladora="contenido")
    assert r.heuristic_step == 3
    assert r.document_source == "texto_reguladora"
    assert r.document_ref is None
    assert r.document_key.startswith("text:")


def test_fallback_none():
    assert resolve_document(url_bases_reguladoras=None, documentos=None, texto_reguladora=None) is None
    assert resolve_document(url_bases_reguladoras=None, documentos=[], texto_reguladora="   ") is None


def test_url_dinamica_no_pdf_es_valida():
    """URLs dinámicas (PHP/HTML BOE) deben aceptarse."""
    r = resolve_document(
        url_bases_reguladoras="https://www.boe.es/buscar/doc.php?id=BOE-A-2025-3176",
        documentos=None,
        texto_reguladora=None,
    )
    assert r.heuristic_step == 1


def test_url_invalida_cae_al_siguiente_paso():
    r = resolve_document(
        url_bases_reguladoras="not-a-url",
        documentos=[{"tipo": "Bases reguladoras", "url": "https://example.org/bases.pdf"}],
        texto_reguladora=None,
    )
    assert r.heuristic_step == 2


def test_document_key_estable_entre_invocaciones():
    """Misma URL produce misma key (dedup N:1)."""
    r1 = resolve_document(
        url_bases_reguladoras="https://example.org/bases.pdf",
        documentos=None,
        texto_reguladora=None,
    )
    r2 = resolve_document(
        url_bases_reguladoras="https://example.org/bases.pdf/",
        documentos=None,
        texto_reguladora=None,
    )
    assert r1.document_key == r2.document_key


def test_step2_resolves_document_name_from_array():
    docs = [
        {"tipo": "Anexo", "url": "https://example.org/anexo.pdf"},
        {"tipo": "Bases reguladoras", "nombre": "Bases reguladoras (Anexo I)",
         "url": "https://example.org/bases.pdf"},
    ]
    r = resolve_document(url_bases_reguladoras=None, documentos=docs, texto_reguladora=None)
    assert r.heuristic_step == 2
    assert r.document_source == "documentos_array"
    assert r.document_name == "Bases reguladoras (Anexo I)"


def test_step1_document_name_is_none():
    r = resolve_document(
        url_bases_reguladoras="https://example.org/bases.pdf",
        documentos=None,
        texto_reguladora=None,
    )
    assert r.document_name is None


def test_step3_document_name_is_none():
    r = resolve_document(url_bases_reguladoras=None, documentos=None, texto_reguladora="contenido")
    assert r.document_name is None
