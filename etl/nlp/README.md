# etl/nlp — Batch B (análisis NLP de bases reguladoras)

## Propósito

Pipeline que extrae estructura de bases reguladoras vía LLM (structured output v5.0.2),
persiste en `l0.subvenciones_nlp` (cache deduplicada por `document_key`) y dual-writea
columnas NLP en `l0.nacional_subvenciones` para feed, filtros y matching.

## Heurística de resolución del documento

| Step | Fuente | Búsqueda | Output |
|---|---|---|---|
| 1 | `url_bases_reguladoras` | URL descargable directa | `source='url_bases_reguladoras'` |
| 2 | `documentos[]` | regex `bases\s+regulad` sobre `tipo/descripcion/nombre/titulo` | `source='documentos_array'` |
| 3 | `documentos[]` | regex `texto\s+(de\s+(la\s+)?)?convocatoria` (segunda pasada) | `source='texto_convocatoria'` |
| fallback | — | nada match | `skipped_no_doc=true` |

Todos los steps producen un `document_key` con prefijo `url:` porque siempre
apuntan a un documento descargable. La columna `descripcion_bases_reguladoras`
(TEXT en `l0.nacional_subvenciones`) NO se usa como fuente NLP — su contenido
suele ser solo referencia BOE (p.ej. *"Orden ICT/1156/2024, BOE núm. 261"*),
no el documento mismo.

## Configuración

Vars en `.env` raíz (propagadas vía `scripts/distribute-env.sh` → `services/etl/.env`):

- `NLP_LLM_PROVIDER=openai`
- `NLP_LLM_MODEL=gpt-4.1-mini`
- `OPENAI_API_KEY=` (obligatorio para ejecución real)
- `NLP_LLM_TIMEOUT_S=180`

## Invocación

### CLI

```bash
licitia-etl nlp analizar --limit 5 --dry-run         # plan, coste 0
licitia-etl nlp analizar --limit 5                   # ejecución real
licitia-etl nlp analizar --limit 5 --force           # ignora cache, re-llama LLM
```

### HTTP (panel admin, requiere sesión vía frontend-etl)

```bash
curl -X POST http://localhost:3001/api/etl/nlp/analizar \
  -H 'Content-Type: application/json' \
  -b 'session=<cookie>' \
  -d '{"limit": 5, "dry_run": false}'

curl http://localhost:3001/api/etl/nlp/log?lines=200 -b 'session=<cookie>'
curl http://localhost:3001/api/etl/nlp/current-run    -b 'session=<cookie>'
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
