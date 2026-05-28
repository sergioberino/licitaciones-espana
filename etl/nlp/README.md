# etl/nlp — Batch B (análisis NLP de bases reguladoras)

## Propósito

Pipeline que extrae estructura de bases reguladoras vía LLM (structured output v5.0.2),
persiste en `l0.subvenciones_nlp` (cache deduplicada por `document_key`) y dual-writea
columnas NLP en `l0.nacional_subvenciones` para feed, filtros y matching.

## Heurística de resolución del documento

Estructura real de cada item en `documentos[]` (BDNS SNPSAP):

```json
{
  "id": 1454450,
  "long": 506437,
  "datMod": "2026-05-13",
  "nombreFic": "Convocatoria_PKD.report.pdf",
  "descripcion": "Documento de la convocatoria en español",
  "datPublicacion": "2026-05-13"
}
```

| Step | Fuente | Match (haystack = `descripcion` + `nombreFic`, normalizado) | Output |
|---|---|---|---|
| 1 | `documentos[]` | regex `bases\s+regulad` + no-anexo | `source='documentos_array'` |
| 2 | `url_bases_reguladoras` | URL libre indicada por BDNS para bases reguladoras | `source='url_bases_reguladoras'` |
| 3 | `documentos[]` | regex `(?:texto\|documento)\s+\S+(?:\s+\S+){0,5}\s+convocatoria` + no-anexo | `source='texto_convocatoria'` |
| fallback | — | nada match | `skipped_no_doc=true` |

**Reorder 2026-05-28.** El nuevo orden prioriza bases reguladoras:
primero si vienen como documento BDNS explícito y después vía `url_bases_reguladoras`.
Solo si esas vías no están disponibles se cae a `texto_convocatoria`, porque:

- URLs de boletines y sedes electrónicas (BORM, CAIB…) son frecuentemente
  SPAs con hash routing o portales que devuelven ~100 chars de menú a httpx
  → contenido inservible para el LLM.
- Resolver step 2 puede requerir `browser-service` (Chromium headless) o un
  fallback escalonado por calidad del payload (ver Task 7 del plan headless
  resolver). Coste: +3-15 s por item + memoria del contenedor.
- Aun con ese coste, el use case gana si alcanzamos bases reguladoras antes de
  conformarnos con texto de convocatoria.

## Step 2 — política escalonada vía browser-service

El step 2 usa `url_bases_reguladoras`. Antes de decidir si
invoca al browser-service, `extract_for_url_bases_reguladoras` aplica tres ramas
de gating:

### (a) Directo descargable → httpx

`is_direct_downloadable(url)`: la URL acaba en `.pdf`, `.doc` o `.docx` (en
path o en query, ej. `?f=doc.pdf`). Se descarga con `httpx` directamente, sin
renderizar. Coste: ~300-800 ms.

### (b) Fragment-only routing → browser-service directo

`is_fragment_only_routing(url)`: la URL tiene hash routing SPA con path
significativo (ej. `https://www.borm.es/#/anuncio/6349`) y sin path HTTP real.
`httpx` recibiría solo el shell vacío del SPA, así que se envía directamente al
browser-service sin pasar por httpx. Coste: ~3-15 s.

### (c) URL "limpia" → httpx + anchor estático + quality check + escalado condicional

Para el resto de URLs:

1. `httpx` descarga la página.
2. Si la respuesta es HTML con texto suficiente, el ETL comprueba si parece una
   landing page documental antes de aceptar el body como fuente final. Para ello
   inspecciona una sola vez el HTML ya descargado y prueba anchors fuertes como
   `Bases reguladoras` o `Convocatoria`.
3. Si esa extracción enlazada es útil, el documento enlazado pasa a ser la
   fuente NLP con `extraction_mode='html_anchor_*'` (por ejemplo
   `html_anchor_pdf` o `html_anchor_html`). Si falla o produce texto de baja
   calidad, se vuelve al body HTML original y continúa la política existente.
   Esta rama es barata: no usa `browser-service`, no renderiza y no hace deep
   crawling. Documentos de ciclo de vida como admisiones, denegaciones,
   listados o sorteos se degradan o excluyen como candidatos.
4. `should_escalate_to_browser` evalúa la calidad del texto extraído:
   - **Escala** si: `<500 chars` de texto útil, O `0 normative_keywords`
     (`regulad`, `convocatoria`, `bases`…), O señales de portal/WAF
     (meta refresh, JS redirect, body vacío con scripts).
   - **Acepta** si pasa los umbrales → se usa el texto de httpx directamente.
5. Si escala al browser-service, el outcome que devuelve `/resolve` determina
   qué hace el ETL:
   - `outcome='pdf_url'` → el ETL descarga el PDF candidato con httpx
     (`extraction_mode='url_bases_reguladoras'`).
   - `outcome='text'` → el cuerpo renderizado se usa como documento
     (`extraction_mode='headless_body'`).
   - `outcome='unresolvable'` → el item se marca `skipped_no_doc` sin llamar
     al LLM.

### Modo degradado

Si `BROWSER_SERVICE_URL` no responde (servicio caído, red inaccesible), el
adaptador `browser_client.py` loguea un warning y el extractor hace fallback
last-resort a httpx puro: se usa el texto descargado incluso si es de baja
calidad. Esto evita que un fallo del browser-service bloquee el batch completo.

### Variables de entorno

- `BROWSER_SERVICE_URL` — URL base del microservicio (default:
  `http://browser-service:8000` en Docker). Propagada vía
  `scripts/distribute-env.sh` (`ETL_KEYS`).
- `BROWSER_SERVICE_PORT` — puerto del host expuesto en `docker-compose.yml`
  (default: `8005`). Solo para acceso desde fuera de la red Docker.

### Costes por step

| Step   | Latencia típica | Recurso externo     |
|--------|-----------------|---------------------|
| 1      | ~ms             | httpx (BDNS SNPSAP) |
| 2 (a)  | ~300-800 ms     | httpx directo       |
| 2 (b)  | ~3-15 s         | browser-service     |
| 2 (c)  | ~300 ms + 3-15 s si escala | httpx + anchor estático barato + browser-service condicional |
| 3      | ~ms             | httpx (BDNS SNPSAP) |

## Arquitectura de scraping (diagrama)

```
run_batch (pipeline.py)
  -> resolve_document(url_br, documentos[])
       ├─ step=1 documentos[] (bases regulad) -> httpx(BDNS docs)
       ├─ step=2 url_bases_reguladoras -> extract_for_url_br
       │    ├─ (a) direct .pdf/.doc -> httpx direct
       │    ├─ (b) frag SPA #/anuncio/... -> browser-service
       │    └─ (c) limpia -> httpx GET
       │         ├─ strong static anchor useful -> linked doc
       │         │    extraction_mode='html_anchor_*'
       │         └─ no anchor / linked extraction poor -> quality check
       │              ├─ OK -> httpx HTML body
       │              └─ BAJA -> browser-service POST /resolve
       │                   ├─ pdf_url -> httpx PDF candidato
       │                   ├─ text -> headless_body
       │                   └─ unresolvable -> skipped_no_doc
       └─ step=3 documentos[] (texto conv.) -> httpx(BDNS docs)

Todos los documentos aceptados convergen en:

  ┌─────────────────────────────────────────────────────────────┐
  │  ExtractedText → LLM (OpenAI structured output v5.0.2)     │
  │   → validate → persist (subvenciones_nlp + nacional dual-w)│
  └─────────────────────────────────────────────────────────────┘
```

### Flujo interno del browser-service (`POST /resolve`)

```
  request { url }
      │
      ▼
  PlaywrightCrawler (keep_alive=True)
      │  browser_launch_options: --no-sandbox --disable-setuid-sandbox
      │
      ▼  navigate(url, timeout=30s)
         wait DOMContentLoaded (20s) + networkidle best-effort (8s)
      │
      ▼  Extract <a href> con agregación a11y:
         text = [innerText, aria-label, title, img@alt, svg>title]
               .filter(Boolean).join(' | ')
      │
      ▼  link_picker.pick_best_candidate(candidates, body_text)
         │
         │  Patrones primarios (sobre anchor text):
         │    1. \bregulad           (regulad-ora/-or/-oras)
         │    2. \bconvocatoria      (incluye plural)
         │    3. resoluci[oó]n\b.*\bbases?\b  (DOTALL)
         │
         │  Fallback body-keyword:
         │    body contiene regulad|convocatoria
         │    AND anchor con href descargable (.pdf/.doc[x]/…)
         │    → devuelve ese anchor
      │
      ▼  renderer.decide_outcome(body_text, picked)
           picked encontrado     → outcome='pdf_url', pdf_url=href
           picked=None, body≥5k  → outcome='text', text=body
           nada útil             → outcome='unresolvable' + reason
```

---

**Construcción de URL en step 1/3:**

```
https://www.infosubvenciones.es/bdnstrans/api/convocatorias/documentos?idDocumento={doc.id}
```

Endpoint público SNPSAP (`Content-Type: application/octet-stream`).

**Reglas de selección:**

- **Anti-anexo / anti-instrumental:** descartamos documentos con `anexo`, `solicitud`,
  `formulario`, `extracto`/`extracte` como token autónomo en el haystack. No representan
  ni las bases ni la convocatoria.
- **Modificaciones, ampliaciones, revocaciones, rectificaciones:** NO matchean los
  regex de step 1/3 (sus descripciones no contienen `bases reguladoras` ni el patrón
  `(texto|documento) … convocatoria`). Se descartan implícitamente.
- **Múltiples candidatos válidos:** se selecciona `max(datPublicacion)` — la versión
  vigente más reciente. Justificación: las bases reguladoras no suelen modificarse,
  pero los textos de convocatoria se readaptan por línea/edición, y queremos siempre
  la versión vigente.
- **Prioridad documental:** si no hay `documentos[]` de bases reguladoras, se intenta
  `url_bases_reguladoras` antes que `texto_convocatoria`; el use case prioriza alcanzar
  bases reguladoras aunque el step 2 pueda requerir browser-service.

El resolver produce un `document_key` base con prefijo `url:` porque todos los
steps apuntan a una URL descargable. Antes de consultar o persistir cache, el
pipeline genera una `analysis_key` contextual (`<document_key>:ctx:<hash>`) con
`tipo_documento` y `ConvocatoriaDetalle_BDNS`; esto evita reutilizar un análisis
creado con metadata BDNS de otra convocatoria que comparta el mismo documento.

La columna `descripcion_bases_reguladoras` (TEXT en `l0.nacional_subvenciones`)
NO se usa como fuente NLP — su contenido suele ser solo referencia BOE (p.ej.
*"Orden ICT/1156/2024, BOE núm. 261"*), no el documento mismo.

## Configuración

Vars en `.env` raíz (propagadas vía `scripts/distribute-env.sh` → `services/etl/.env`):

- `NLP_LLM_PROVIDER=openai`
- `NLP_LLM_MODEL=gpt-5.4-nano`
- `OPENAI_API_KEY=` (obligatorio para ejecución real)
- `NLP_LLM_TIMEOUT_S=180`

## Invocación

### CLI

```bash
# Selector obligatorio (uno y solo uno): --anos | --codigo-bdns | --todo
licitia-etl nlp analizar --anos 2026-2026 --limit 5 --dry-run         # plan, coste 0
licitia-etl nlp analizar --anos 2024-2025 --meses 3-6 --limit 50      # cross-product
licitia-etl nlp analizar --codigo-bdns 999999 --force                 # 1 convocatoria
licitia-etl nlp analizar --todo --limit 0                             # todo el catálogo, sin cap
```

### HTTP (panel admin, requiere sesión vía frontend-etl)

```bash
curl -X POST http://localhost:3001/api/etl/nlp/analizar \
  -H 'Content-Type: application/json' \
  -b 'session=<cookie>' \
  -d '{"anos": "2026-2026", "limit": 5, "dry_run": false}'

curl http://localhost:3001/api/etl/nlp/log?lines=200 -b 'session=<cookie>'
curl http://localhost:3001/api/etl/nlp/current-run    -b 'session=<cookie>'
```

## Filtros temporales y selector (WP2.1.1)

A partir de WP2.1.1, `nlp analizar` exige un selector explícito alineado con el
patrón nacional (no más "lo último por id DESC sin filtro fecha"). Esto es un
**breaking change menor**: invocaciones previas como
`licitia-etl nlp analizar --limit 5` (sin selector) ahora fallan con `exit 2` y
mensaje claro.

### Selector obligatorio (uno y solo uno)

| Flag | Semántica SQL contra `l0.nacional_subvenciones.fecha_recepcion` |
|---|---|
| `--anos X-Y` | `fecha_recepcion >= 'X-01-01' AND fecha_recepcion <= 'Y-12-31'`. NULLs excluidas. |
| `--codigo-bdns N` | `id = N` (PK BIGINT). Quita el filtro `nlp_document_key IS NULL` para permitir reanálisis con `--force`. `--limit` se ignora (warning en log). |
| `--todo` | Sin filtro de fecha. Conserva `nlp_document_key IS NULL` y `(url... OR documentos...) IS NOT NULL`. |

### Modificadores

- `--meses N-M`: solo válido con `--anos`. Rango cerrado [1..12]. Cross-product:
  `--anos 2024-2025 --meses 3-6` ⇒ marzo-junio de 2024 **y** marzo-junio de 2025.
- `--limit N`: cap absoluto. Default 100. **`--limit 0` = sin cap** (omite cláusula
  `LIMIT` del SQL). Ignorado con `--codigo-bdns`.
- `--force`: ignora cache, siempre re-llama al LLM.
- `--dry-run`: no llama al LLM ni persiste; muestra plan por item.

### Guardraíles (errores explícitos, exit_code 2)

- Sin `--anos`/`--codigo-bdns`/`--todo`: `[nlp] error: Falta selector...`.
- `--meses` sin `--anos`: error.
- `--anos + --codigo-bdns` o `--anos + --todo` o `--todo + --codigo-bdns`: error mutual exclusion.
- `--anos 2026-2024` (start > end), `--meses 0-13`, `--meses 5-3`: error.
- En la API HTTP las mismas reglas se aplican vía `model_validator` Pydantic ⇒ 422.

### Cache hit (sin cambios)

En todos los modos: si la convocatoria está en cache (`subvenciones_nlp.document_key`
con `valid|partial`) y no se pasa `--force`, se reaprovecha vía `propagate_from_cache`.
`--force` siempre re-llama al LLM.

### Ejemplos completos

```bash
# Muestreo Q1 2026 con plan previo
licitia-etl nlp analizar --anos 2026-2026 --meses 1-3 --limit 20 --dry-run
licitia-etl nlp analizar --anos 2026-2026 --meses 1-3 --limit 20

# Re-análisis puntual de una convocatoria por id BDNS
licitia-etl nlp analizar --codigo-bdns 1454450 --force

# Barrido completo del catálogo en background (vía API)
curl -X POST http://localhost:3001/api/etl/nlp/analizar \
  -H 'Content-Type: application/json' \
  -d '{"todo": true, "limit": 0}'
```

### AntonIA (backend-to-backend, sin cookie)

```bash
curl http://localhost:3001/api/subvenciones/12345/ficha
```

## QA del muestreo (Hito 2)

### Q1. Estado de cada item

```sql
SELECT s.id, n.validation_status, n.document_source, n.document_heuristic_step,
       n.geo_patron, n.tipo_beneficiario_fino, n.modalidad_lgs
FROM l0.nacional_subvenciones s
LEFT JOIN l0.subvenciones_nlp n ON s.nlp_document_key = n.document_key
ORDER BY s.id DESC LIMIT 5;
```

### Q2. Cobertura cols dual-write

```sql
SELECT s.id,
  (s.nlp_document_key IS NOT NULL)::int AS has_nlp,
  s.nlp_validation_status,
  s.geo_patron,
  array_length(s.domicilio_fiscal_nuts, 1) AS dom_nuts_count,
  array_length(s.ejecucion_nuts, 1) AS ejec_nuts_count,
  s.intensidad_maxima_pct, s.regimen_ue_tipo, s.tipo_beneficiario_fino
FROM l0.nacional_subvenciones s
ORDER BY s.id DESC LIMIT 5;
```

### Q3. Fallos

```sql
SELECT subvencion_id, document_source, document_ref, error_message
FROM ops.nlp_failures
ORDER BY created_at DESC LIMIT 5;
```

### Q4. Coste y latencia

```sql
SELECT document_key, llm_model, input_tokens, output_tokens, duration_ms, validation_status
FROM ops.llm_bases_reguladoras_logs
ORDER BY created_at DESC LIMIT 5;
```

## Scheduler

Registrar sin habilitar hasta validar el muestreo:

```bash
licitia-etl scheduler register nlp --subconjunto analizar --frecuencia Diario --no-enable
```
