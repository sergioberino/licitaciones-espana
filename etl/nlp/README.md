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
| 1 | `url_bases_reguladoras` | URL descargable directa | `source='url_bases_reguladoras'` |
| 2 | `documentos[]` | regex `bases\s+regulad` + no-anexo | `source='documentos_array'` |
| 3 | `documentos[]` | regex `(?:texto\|documento)\s+\S+(?:\s+\S+){0,5}\s+convocatoria` + no-anexo | `source='texto_convocatoria'` |
| fallback | — | nada match | `skipped_no_doc=true` |

**Construcción de URL en step 2/3:**

```
https://www.infosubvenciones.es/bdnstrans/api/convocatorias/documentos?idDocumento={doc.id}
```

Endpoint público SNPSAP (`Content-Type: application/octet-stream`).

**Reglas de selección:**

- **Anti-anexo / anti-instrumental:** descartamos documentos con `anexo`, `solicitud`,
  `formulario`, `extracto`/`extracte` como token autónomo en el haystack. No representan
  ni las bases ni la convocatoria.
- **Modificaciones, ampliaciones, revocaciones, rectificaciones:** NO matchean los
  regex de step 2/3 (sus descripciones no contienen `bases reguladoras` ni el patrón
  `(texto|documento) … convocatoria`). Se descartan implícitamente.
- **Múltiples candidatos válidos:** se selecciona `max(datPublicacion)` — la versión
  vigente más reciente. Justificación: las bases reguladoras no suelen modificarse,
  pero los textos de convocatoria se readaptan por línea/edición, y queremos siempre
  la versión vigente.

Todos los steps producen un `document_key` con prefijo `url:` porque siempre
apuntan a una URL descargable. La columna `descripcion_bases_reguladoras`
(TEXT en `l0.nacional_subvenciones`) NO se usa como fuente NLP — su contenido
suele ser solo referencia BOE (p.ej. *"Orden ICT/1156/2024, BOE núm. 261"*),
no el documento mismo.

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
