# Changelog

Todos los cambios notables del CLI y del microservicio ETL se documentan aquí.

## [1.5.2] — 2026-04-21

### Resumen

- **Ingesta completa de datos de subvenciones:** migración de 6 a 32 campos por convocatoria, guardando TODOS los datos detallados (instrumentos, beneficiarios, sectores, regiones, presupuesto, documentos, anuncios) en lugar de solo datos ligeros.
- **Normalización de schema:** campos JSONB convertidos a estructuras tipadas (FKs, arrays PostgreSQL nativos) para mejorar rendimiento de consultas e integridad referencial.
- **Simplificación de campos:** extracción de valores únicos (primer instrumento, primer objetivo, solo descripción) para reducir complejidad y redundancia.
- **Nueva arquitectura de scraping en 2 fases:** fase 1 obtiene IDs ligeros (`/busqueda`), fase 2 obtiene detalles completos por cada ID (`/convocatorias`).
- **Optimizaciones de rendimiento:** rate limiting adaptativo global thread-safe, multithreading producer-consumer para scraping histórico, carga paralela de parquets, consulta optimizada 250x más rápida.
- **Eliminación de dependencia API externa:** con datos completos en BD, Licitia ya no necesita llamar a API externa para obtener detalles de subvenciones.

### Añadido

- **`nacional/subvenciones.py`:**
  - Nueva función `fetch_convocatoria_detalle()`: obtiene información completa de una convocatoria desde endpoint de detalle con rate limiting adaptativo.
  - Funciones de normalización con cachés en memoria:
    - `_load_instrumentos_map()` y `_extract_instrumento_id()`: mapeo de descripciones JSONB a IDs de `dim.instrumentos_subvenciones`, extrae solo el primer instrumento.
    - `_load_beneficiarios_map()` y `_extract_beneficiarios_ids()`: mapeo de descripciones JSONB a array de IDs de `dim.beneficiarios_subvenciones`.
    - `_extract_nut_codes()`: extracción de códigos NUT (regiones) desde API.
    - `_extract_sector_codes()`: extracción de códigos CNAE (sectores) con normalización de decimales (`'96.4'` → `'9640'`).
    - `_extract_reglamento_descripcion()`: extrae solo campo `descripcion`, descarta campo `orden`.
    - `_extract_objetivos_descripcion()`: extrae solo descripción del primer objetivo, descarta objetivos adicionales.
    - `_normalize_empty_jsonb()`: convierte arrays vacíos `[]` a `None`.
  - Funciones helper `_flatten_organo()` y `_convert_to_json_serializable()` para procesamiento de datos.
  - Rate limiting adaptativo global con `_api_delay_seconds` y `_api_delay_lock` (thread-safe).
  - Pattern producer-consumer en `scrape_historico`: thread fetcher obtiene detalles y pone en Queue, thread saver consume Queue y genera parquets por batches.
  - Campo `direccion` en `SearchParams`: `"asc"` para histórico, `"desc"` para diario.

- **`etl/cli.py`:**
  - Soporte ThreadPool (3 workers) para carga paralela de múltiples parquets de subvenciones.
  - Solo activo para `nacional/subvenciones` con múltiples archivos parquet.

- **`schemas/012_nacional_subvenciones.sql`:**
  - Campos normalizados con estructuras tipadas:
    - `instrumento_id SMALLINT`: FK a `dim.instrumentos_subvenciones(id)`, extrae solo el primer instrumento del array JSONB.
    - `tipos_beneficiarios SMALLINT[]`: array de FKs a `dim.beneficiarios_subvenciones(id)`, mapeo de descripciones a IDs.
    - `sectores VARCHAR(10)[]`: array de códigos CNAE/NACE con normalización de decimales, referencia a `dim.cnae_dim(codigo)`.
    - `regiones VARCHAR(10)[]`: array de códigos NUT, FK a `dim.nuts_spain(geocode)`.
    - `reglamento TEXT`: solo campo `descripcion`, descartado campo `orden`.
    - `objetivos TEXT`: solo descripción del primer objetivo, descartados objetivos adicionales.
  - Campos JSONB optimizados: arrays vacíos `[]` convertidos a `NULL` en `fondos`, `sectores_productos`, `documentos`, `anuncios`.
  - Índices GIN en arrays para búsquedas eficientes con operadores `@>`, `&&`, `ANY()`.
  - 26 campos adicionales: `sede_electronica`, `tipo_convocatoria`, `presupuesto_total`.
  - Campos de bases reguladoras: `descripcion_finalidad`, `descripcion_bases_reguladoras`, `url_bases_reguladoras`.
  - Estado y fechas: `abierto`, `se_publica_diario_oficial`, `fecha_inicio_solicitud`, `fecha_fin_solicitud`, `text_inicio`, `text_fin`.
  - Ayuda de estado: `ayuda_estado`, `url_ayuda_estado`.

### Mejorado

- **Rate limiting adaptativo:** delay inicial 0ms (sin penalización previa), se activa solo tras HTTP 429, incremento exponencial suave ×1.6 (0ms → 25ms → 40ms → 64ms → 102ms). Resultado: ~7 conv/s (~0.15s por convocatoria).

- **Consulta optimizada en `scrape_diario`:** cambio de `MAX(fecha_recepcion)` (escaneo completo) a `ORDER BY id DESC LIMIT 1` (usa índice PK). Mejora de 250x (500ms → 2ms). Los IDs más altos corresponden implícitamente a fechas más recientes.

- **`scrape_diario`:** ahora también obtiene detalles completos de cada convocatoria nueva (antes solo guardaba datos ligeros). Validación mejorada: lanza error descriptivo si la tabla está vacía, indicando ejecutar ingest histórico primero.

- **`scrape_historico`:** generación de múltiples parquets por batches (max 100 registros cada uno) en lugar de un solo parquet grande. Nomenclatura: `_part_subvenciones_000.parquet`, `_part_subvenciones_001.parquet`, etc.

- **`etl/ingest_l0.py`:** actualizado `SUBVENCIONES_PARQUET_COLUMNS` con tipos normalizados (SMALLINT, SMALLINT[], VARCHAR[], TEXT). Lógica de inserción mejorada para manejar arrays PostgreSQL nativos (conversión automática de numpy arrays a listas Python, deserialización de JSON cuando necesario).

### Eliminado

- **`nacional/subvenciones.py`:**
  - Endpoint `/ultimas` (ahora todo usa `/busqueda` con parámetro `direccion`).
  - Clase `LatestParams` (sustituida por `SearchParams` con campo `direccion`).
  - Clase `RateLimiter` (sustituida por rate limiting global adaptativo).
  - Constante `API_ENDPOINT_ULTIMAS`.
  - Campos redundantes: `numero_convocatoria`, `codigo_bdns` (redundantes con `id`), `codigo_invente` (no disponible en API).

### Corregido

- **Bug crítico en `scrape_historico`:** función `fetcher()` usaba variable `numero_conv` indefinida, causando que todas las peticiones usaran el mismo ID. Solo el primer registro se insertaba, los 607 restantes se marcaban como duplicados. Corregido a `conv["id"]`. Tasa de éxito: 0.16% → 100%.

### Migración requerida (BREAKING CHANGE)

**ATENCIÓN:** Estructura de tabla incompatible (6 → 32 campos, JSONB → estructuras tipadas). Datos existentes no son compatibles.

**Simplificaciones aplicadas:**

- Solo se guarda el **primer instrumento** del array (campo `instrumento_id`).
- Solo se guarda la **descripción del primer objetivo** (campo `objetivos TEXT`).
- Campo `reglamento`: solo se guarda `descripcion`, se descarta `orden`.
- Códigos CNAE con decimales se normalizan automáticamente (`'96.4'` → `'9640'`).
- Arrays vacíos `[]` se convierten a `NULL`.

1. Borrar datos antiguos:

   ```sql
   TRUNCATE TABLE l0.nacional_subvenciones;
   ```

2. Aplicar nuevo schema:

   ```bash
   docker compose exec etl licitia-etl init-db
   ```

3. Reingestar datos:
   ```bash
   docker compose exec etl licitia-etl ingest nacional subvenciones --anos 2020-2026
   ```

**Nota:** El proceso de migración puede tardar varias horas dependiendo de los años a ingestar. Ver `ignored/CAMBIOS_SCHEMA_SUBVENCIONES.md` para guía completa de migración.

---

## [1.5.1] — 2026-04-17

### Resumen

- **Observabilidad del scheduler:** registro estructurado de líneas (`scheduler.log`), tabla `scheduler.incidents`, API de consulta de log e incidentes, latido en fichero (`scheduler.heartbeat`).
- **Recuperación operativa:** al arranque del API se detectan reinicios bruscos (ejecuciones «running» sin proceso, PID huérfano), se registran incidentes en español y se intenta el reinicio automático del daemon del scheduler en segundo plano.
- **Diagnóstico en incidentes:** contexto de contenedores vía API Docker sobre socket Unix (sin binario `docker` en la imagen), con soporte de respuestas HTTP chunked; montaje opcional del socket en `docker-compose` del servicio (solo lectura; evaluar riesgo de seguridad).

### Añadido

- **Migración `021_scheduler_incidents.sql`** (y registro en `INIT_MIGRATIONS`): persistencia de incidentes del supervisor.
- **`etl/log_supervisor.py`:** supervisor de log con recorte, escritura de incidentes en BD y latido.
- **API:** endpoints para leer trazas del scheduler y listar / resolver incidentes (según implementación publicada).
- **`etl/docker_incident_context.py`:** captura de listado de contenedores para adjuntar al detalle de incidentes de arranque.

### Mejorado

- **`POST /scheduler/run` (tarea única):** la petición no bloquea hasta el fin de la ingesta; se lanza el proceso en segundo plano (evita cortes por tiempo de espera en proxies o navegador).
- **Subprocesos de ingesta y tareas del bucle del scheduler:** `PYTHONUNBUFFERED=1` y `flush` en progreso por stderr para que la salida aparezca en tiempo casi real en ficheros de log.
- **Bucle del scheduler:** la línea `Completed` del log puede incluir **insertadas** / **omitidas** leídas de `scheduler.runs` tras el subproceso; incidente `heartbeat_gap` solo si el tick fue anormalmente largo **sin** tareas debidas (evita falsos positivos durante ingestas largas).

### Corregido

- **Ingesta nacional / licitaciones:** se evita reutilizar un ZIP en caché de descarga incompleta o obsoleto (se fuerza re-descarga cuando corresponde).

---

## [1.5.0] — 2026-04-17

### Añadido

- **Tablas dimensionales de órganos gestores:** 4 nuevas tablas en el esquema `dim` para entidades administrativas de nivel 3 — comunidades autónomas (`dim.organos_gestores_ccaa`), ministerios (`dim.ministerios_organos`), provincias (`dim.provincias_organos`) y otros órganos (`dim.otros_organos`). Datos estáticos pre-cargados vía migraciones `014`–`017`.
- **Tablas dimensionales de subvenciones:** 3 nuevas tablas — beneficiarios (`dim.beneficiarios_subvenciones`), instrumentos (`dim.instrumentos_subvenciones`) y política de gastos (`dim.politica_gastos`). Migraciones `018`–`020`.

### Corregido

- **`INIT_MIGRATIONS` — nombre de fichero 014 desalineado:** `cli.py` referenciaba `014_dim_organos_gestores_ccaa.sql` pero el fichero real es `014_dim_ccaa_organos.sql`, lo que impedía que la migración se aplicase durante `init-db`.

---

## [1.4.1] — 2026-04-08

### Mejorado

- **`GET /health`**: devuelve HTTP 503 con `"status": "degraded"` cuando la base de datos no es accesible (antes siempre 200 + `"status": "ok"`). Permite que los health probes de Docker detecten correctamente un servicio degradado.

### Corregido

- **`POST /scheduler/unregister` — eliminación de tareas independiente de `enabled`**: el endpoint usaba `get_task_id()` que filtra por `AND enabled = true`, impidiendo borrar tareas visibles en la UI con `enabled = false` (0 deleted). Ahora ejecuta `DELETE FROM scheduler.tasks WHERE conjunto = %s AND subconjunto = %s` directamente.

### Añadido

- **Registro de ejecuciones del scheduler para `cmd_borme` (ingest)**: `cmd_borme` era el único comando CLI que no registraba runs en `scheduler.runs`. Sin run registrada, `last_finished_at` nunca se actualizaba y `next_run_at` se calculaba siempre sobre `created_at`, dejando la tarea permanentemente "Vencida". Ahora sigue el mismo patrón de tracking que `cmd_ingest` y `cmd_subvenciones`.

---

## [1.4.0] — 2026-04-08

### Añadido

- **CNAE (Clasificación Nacional de Actividades Económicas):** nueva tabla de dimensión `dim.cnae_dim` con migración DDL (`013_dim_cnae.sql`) incluida en `INIT_MIGRATIONS`.
- **Ingesta CNAE desde fuente oficial:** módulo `etl/cnae_ingest.py` que obtiene los códigos CNAE-2025 de la API SDMX del ISTAC (datos.canarias.es), filtra códigos numéricos y realiza upsert en `dim.cnae_dim`.
- **CLI `licitia-etl cnae ingest`:** nuevo verbo para ejecutar la ingesta CNAE bajo demanda (patrón similar a BORME).
- **API `POST /cnae/ingest`:** endpoint REST para disparar la ingesta CNAE desde el panel de administración.
- **`dim_status` en `GET /health`:** el endpoint de salud ahora incluye el estado de las tablas de dimensión (`cpv`, `cnae`): `has_rows` y `has_embeddings`, permitiendo al panel detectar si faltan datos o indexaciones.

---

## [1.3.4] — 2026-04-07

### Corregido

- **`GET /scheduler/status` y `licitia-etl scheduler status`**: `get_next_run_at()` ahora acepta **`created_at`** (alineado con `api.py` y `get_tasks_due`). Antes el código desplegado llamaba con `created_at=...` pero la firma en `scheduler.py` no lo incluía → `TypeError: unexpected keyword argument 'created_at'`.
- **`get_tasks_due`** y **CLI** pasan `created_at` de la tarea para coherencia con el cálculo de `next_run_at` para tareas nuevas sin ejecución previa.

---

## [1.3.3] — 2026-04-07

### Mejorado

- **Logs de subvenciones (BDNS) para Docker / panel “Salida”**: `scrape_historico` y `scrape_diario` emiten progreso por línea (sin `tqdm`), prefijo `[subvenciones]`, marcadores `SCRAPING COMPLETADO` / error, y resúmenes por página (nuevos, duplicados, filtrados) en el flujo diario.

### Alineación de versiones

- **`__version__`**, `pyproject.toml` y metadatos OpenAPI (`FastAPI.version`) unificados en **1.3.3** (antes `__init__.py` / API seguían en 1.2.3 pese a tags posteriores).

### Eliminado

- **`tests/test_api.py`**: smoke tests HTTP contra la app FastAPI (sustituibles por pruebas manuales o contract tests en otro entorno si se desean).

---

## [1.3.2] — 2026-04-06

### Sincronización con repositorio padre

- **Merge de `BquantFinance/licitaciones-espana` `main`** en el fork: incorpora los commits pendientes (mapeo `BudgetAmount`, refactor **Galicia** con pipeline reanudable, `tests/test_galicia.py`, fixture `tests/fixtures/entry_budget.xml`, dependencias `python-dateutil` y `beautifulsoup4`).
- **Conflictos resueltos manualmente**: unión de `.gitignore` (artefactos Galicia upstream + rutas Andalucía/Asturias del fork), `requirements.txt` (deps fork + upstream), `galicia/scraper_galicia.py` (cierre correcto de `main()` según upstream), `tests/test_parsear_entry.py` (fixture de presupuesto upstream + tests extendidos del fork: URLs, criterios, requisitos), `README.md` (documentación del microservicio fork; enlace implícito al upstream para datasets por CCAA).

---

## [1.3.1] — 2026-04-06

### Añadido

- **CLI `licitia-etl subvenciones diario`** ([#24](https://github.com/sergioberino/licitaciones-espana/pull/24), cierra [#25](https://github.com/sergioberino/licitaciones-espana/issues/25)): comando explícito para la **actualización diaria** de subvenciones (misma lógica que puede usar el scheduler), invocando `scrape_diario` / `LatestParams` desde `nacional.subvenciones`. Documentación del módulo CLI actualizada (`subvenciones` en la lista de comandos).

### Mejorado

- **Actualización diaria BDNS**: la rama integrada en #24 afinó el flujo de “últimas” convocatorias (paginación, límite de página, filtros, fechas/camelCase→snake_case, queries e índices según subvenciones, etc.) además del punto de entrada CLI anterior.

### Nota (histórica)

La integración completa con `upstream/main` quedó resuelta en **[1.3.2]**.

---

## [1.3.0] — 2026-04-06

### Añadido

- **Ingesta de subvenciones nacionales (BDNS)** ([#21](https://github.com/sergioberino/licitaciones-espana/pull/21)): nueva fuente vía API `infosubvenciones.es` (`nacional/subvenciones.py`), subconjunto `nacional` / `subvenciones`, migración `schemas/012_nacional_subvenciones.sql`, integración en `ingest_l0`, CLI y scheduler (frecuencia diaria para actualización incremental).

### Corregido

- **JSONB y arrays numpy en ingesta L0** ([#23](https://github.com/sergioberino/licitaciones-espana/pull/23)): normalización recursiva de valores (p. ej. arrays numpy) antes de `psycopg2.extras.Json`, evitando fallos al serializar columnas JSONB en ingestas nacionales.

---

## [1.2.3] — 2026-03-27

### Añadido

- **`dim.nuts_spain`**: nueva tabla de dimensión con clasificación NUTS de España (77 regiones, niveles 1/2/3 según Eurostat). Columnas: `geocode` (PK), `etiqueta`. Idempotente (`ON CONFLICT DO UPDATE`).
- **Migración `004_nuts_spain.sql`** añadida a `INIT_MIGRATIONS` — se aplica automáticamente con `licitia-etl init-db` (CLI) y `POST /init-db` (API).

---

## [1.2.2] — 2026-03-27

### Correcciones

- **Fix crítico ingesta JSONB**: `pd.isna(v)` lanzaba `ValueError: The truth value of an array with more than one element is ambiguous` al procesar filas de `nacional/contratos_menores` con valores reales en las columnas JSONB (`docs_adicionales`, `criterios_adjudicacion`, `requisitos_solvencia`). Los tres batches fallaban completamente.
  - Guardado seguro: `v is None or (not isinstance(v, (list, dict)) and pd.isna(v))`.
  - Serialización JSONB correcta: `psycopg2.extras.Json(v)` para valores `list`/`dict`.
- Test de regresión `test_null_guard_safe_for_jsonb_list_and_dict` añadido.

### Pruebas

- 99 pruebas; 0 fallos.

---

## [1.2.1] — 2026-03-25

### Añadido

- **Intervalos de frecuencia personalizados en el scheduler**: las tareas pueden registrarse con cualquiera de los seis intervalos: `Diario`, `Semanal`, `Mensual`, `Trimestral`, `Semestral`, `Anual`.
- **`VALID_SCHEDULE_EXPRS`**: constante pública con los seis valores válidos de frecuencia.
- **`validate_schedule_expr(expr, default)`**: función auxiliar que valida una expresión de frecuencia y lanza `ValueError` si no es válida.
- **`get_next_run_at` — ramas `Diario`, `Semanal`, `Semestral`**: lógica de cálculo de próxima ejecución para las nuevas frecuencias.
  - `Diario`: misma día a las 02:00 si el slot no ha pasado, si no el día siguiente.
  - `Semanal`: lunes de la misma semana a las 02:00 si el slot no ha pasado, si no el lunes siguiente.
  - `Semestral`: 1 de enero o 1 de julio a las 02:00 (el siguiente en llegar).
- **`GET /scheduler/defaults`**: nuevo endpoint que devuelve los intervalos válidos y el intervalo predeterminado por tarea.
- **`POST /scheduler/register` — validación 422**: si `schedule_expr` es inválido (global o por tarea), devuelve HTTP 422.
- **`register_tasks` — `schedule_overrides`**: parámetro opcional `dict[tuple[str,str], str]` para sobrescribir la frecuencia de tareas concretas.
- **CLI `scheduler register --frecuencia`**: opción nueva que aplica una frecuencia global a todas las tareas registradas en esa llamada.
- **`get_all_task_pairs()`**: función pública que devuelve el conjunto de pares `(conjunto, subconjunto)` del registro.

### Pruebas

- 98 pruebas unitarias + de API (de 87 en v1.2.0); 0 fallos.
- Cobertura de ramas de `get_next_run_at` para cada una de las seis frecuencias.
- Tests de frontera en `:00` para `Diario`, `Semanal` y `Semestral`.

---

## [1.2.0] — 2026-03-25

### Añadido

- **Campo `valor_estimado_contrato`**: nueva columna NUMERIC(18,2) que almacena `EstimatedOverallContractAmount` del presupuesto UBL. Antes se mapeaba incorrectamente a `importe_sin_iva`.
- **Referencias documentales**: columnas `doc_legal_nombre`, `doc_legal_url` (PCAP), `doc_tecnico_nombre`, `doc_tecnico_url` (PPT), y `docs_adicionales` (JSONB) para documentos adicionales con nombre, URL y hash.
- **Criterios de adjudicación**: columna `criterios_adjudicacion` (JSONB) con tipo (OBJ/SUBJ), descripción, nota, subtipo y peso de cada criterio.
- **Requisitos de solvencia**: columna `requisitos_solvencia` (JSONB) con solvencia técnica, económica y requisitos específicos, cada grupo con nombre en español.
- **Sanitización de URLs**: función `_sanitizar_url()` que decodifica entidades HTML (`&amp;` → `&`) en todas las URLs extraídas de los atoms PLACSP.
- **Migración 011**: `schemas/011_nacional_new_columns.sql` — ALTER TABLE para añadir las 8 columnas nuevas a tablas `nacional_*` existentes. Registrada en `INIT_MIGRATIONS`.
- **Guía de arquitectura**: `docs/arquitectura-global.md` documenta el sistema de migraciones y cómo añadir cambios DDL futuros.

### Corregido

- **Mapeo de importes en `BudgetAmount`**: `EstimatedOverallContractAmount` se mapeaba a `importe_sin_iva` (incorrecto). Ahora: `EstimatedOverallContractAmount` → `valor_estimado_contrato`, `TaxExclusiveAmount` → `importe_sin_iva`, `TotalAmount` → `importe_con_iva` (sin cambio).
- **Shadowing de `subtipo_code`**: el loop de criterios de adjudicación sobreescribía la variable `subtipo_code` a nivel de proyecto. Renombrado a `crit_subtipo_code`.

### Upstream PR

- Issue y PR en español a `BquantFinance/licitaciones-espana` con la corrección del mapeo de importes (solo WI-1).

### Nota

- Las filas ya ingestadas tendrán `NULL` en las columnas nuevas. Solo nuevas ingestas populan los campos.
- `parsear_entry_cpm()` devuelve `None` para todos los campos nuevos (CPMs no tienen estos datos).

---

## [1.1.2] — 2026-03-25

### Upstream sync (BquantFinance/licitaciones-espana)

- **Andalucía scraper refactor** (`scripts/ccaa_andalucia.py`): improved coverage and stability (upstream PR #5); 11 new tests in `tests/test_ccaa_andalucia.py`
- **Asturias downloader script** (`scripts/ccaa_asturias.py`): new standalone script to fetch contracts from Asturias open-data portal to Parquet; see `scripts/README.md` for usage. Ingest wiring (CONJUNTOS_REGISTRY) deferred to 1.2.0.
- **Calidad analytics module** (`calidad/calidad_licitaciones.py`): 20 quality indicators on contracts dataset; standalone tool, not part of ingest pipeline
- `requirements.txt`: `requests>=2.31.0` added; `numpy<2` pin preserved

### Fork changes

- **Data hygiene**: removed `catalunya/README.md` (doc-only); `.gitignore` updated to cover `asturias_data/`, `ccaa_Andalucia/perfiles_cache.json`, `catalunya/`
- **No new HTTP API routes** added in this release; API surface unchanged from 1.1.1
- **No new CLI subcommands** added; ingest catalog visible via `GET /ingest/conjuntos` (8 conjuntos)

### QA performed (2026-03-25)

**CLI / pytest**

- 55 tests passed, 1 skipped (DB-dependent), 0 failed — full suite including 11 new upstream Andalucía tests
- `python -m etl.cli --help` and `python -m etl.cli ingest --help` — exit 0, no import errors
- `calidad.calidad_licitaciones` imports cleanly

**API (ETL microservice at `http://localhost:8002`)**

| Endpoint                 | HTTP | Notes                                              |
| ------------------------ | ---- | -------------------------------------------------- |
| `GET /health`            | 200  | `db: true`, 4 migrations pending                   |
| `GET /status`            | 200  | `database: connected`                              |
| `GET /db-info`           | 200  | 37 tables, 765 MB                                  |
| `GET /ingest/conjuntos`  | 200  | 8 conjuntos returned                               |
| `GET /migrations`        | 200  | 6 applied, 4 pending                               |
| `GET /scheduler/status`  | 200  | 5 nacional tasks registered, `loop_running: false` |
| `GET /scheduler/running` | 200  | No active runs                                     |

No 500 errors. All endpoints returned expected codes.

### Notes

- API version string updated from 1.1.0 → 1.1.2
- 4 schema migrations on disk pending application (`005_catalunya.sql`, `006_valencia.sql`, `007_views.sql`, `010_borme.sql`) — apply via `POST /init-db` or `licitia-etl init-db`
- `etl/__init__.py` `__version__` remains `1.1.0`; full multi-file semver alignment planned for 1.2.0

---

## [1.1.1] - 2026-03-03

### Añadido

- **Campo `loop_running` en `GET /scheduler/status`.** Indica si el daemon del scheduler está activo (comprobación por PID file y señal al proceso).
- **`GET /scheduler/running`.** Devuelve la lista de ejecuciones con estado `running` (run_id, task_id, conjunto, subconjunto, process_id, started_at).
- **`POST /scheduler/register` con `tasks[]`.** Acepta un cuerpo `{ "tasks": [{ "conjunto": "...", "subconjunto": "..." }, ...] }` para registrar solo los pares indicados (además del registro por `conjuntos[]`).
- **`POST /scheduler/runs/stop`.** Recibe `{ "run_ids": [ ... ] }`; envía SIGTERM a los procesos de las ejecuciones indicadas y las marca como `failed` con mensaje «Detenido por usuario».
- **`POST /scheduler/unregister`.** Recibe `{ "tasks": [{ "conjunto", "subconjunto" }, ...] }`; elimina las tareas programadas correspondientes (y sus runs por CASCADE).

### Corregido

- **`get_next_run_at`.** La comparación para decidir el siguiente slot programado usa `last_finished_at` (en zona Europe/Madrid) en lugar de `now`, de modo que una tarea programada para el mismo día se ejecute correctamente después de una ejecución previa.

---

## [1.0.0] - 2026-02-17 - Release producción (scheduler completo)

### Añadido

- **Scheduler con config en BD:** Schema `scheduler` con tablas `scheduler.tasks` y `scheduler.runs`. Poblado con `licitia-etl scheduler register`; ejecución con `scheduler run` (intervalo `--tick-seconds`, por defecto 60). Frecuencias: Mensual (día 1 a 02:00 Europe/Madrid), Trimestral (ene/abr/jul/oct), Anual (1 enero).
- **scheduler run / stop:** `scheduler run` inicia el daemon; PID file en directorio temporal; `scheduler stop` envía SIGTERM y elimina el PID file. Con `-d` el daemon se ejecuta en segundo plano y la salida va a `scheduler.log`.
- **scheduler status:** Tabla con última ejecución, estado (**scheduled** cuando la última run fue ok, **running** / **failed**), PID, filas y columna **PRÓXIMA EJECUCIÓN** (fecha/hora de la siguiente ejecución programada).
- **Registro de runs desde ingest:** Cada ingest registra un run al inicio (evita solapamiento) y actualiza al finalizar (status, filas, error). `scheduler status` refleja el estado actual.
- **Comandos `health` y `db-info`:** Comprobación de BD y schema scheduler; tamaño y listado de tablas para supervisión.

### Corregido

- **get_tasks_due:** Uso de `reference_now` en `get_next_run_at` para que las tareas nunca ejecutadas se consideren debidas en el mismo tick (evita desfase de milisegundos que dejaba `due_count` en 0).
- **insert_run_start:** Firma correcta `insert_run_start(conn, task_id)` en `cmd_ingest`.

### Nota

- El ETL no crea ni mantiene `dim.cpv_router`; es responsabilidad del indexador si se usa.

---

## [0.2.0] - Ingesta L0 definitiva (todos los conjuntos + PK surrogada + tests)

### Añadido

- **Comando `ingest` (todos los conjuntos):** `licitia-etl ingest <conjunto> <subconjunto> [--anos X-Y] [--solo-descargar|--solo-procesar]`. Conjuntos: **nacional** (5 subconjuntos), **catalunya** (5), **valencia** (14), **andalucia** (1). Requiere `DB_*` y `DB_SCHEMA`; nacional requiere además `--anos` X-Y.
- **PK surrogada en tablas L0:** Todas las tablas L0 usan `l0_id` BIGSERIAL PRIMARY KEY y `natural_id` TEXT UNIQUE NOT NULL (URL o identificador de fuente), para evitar PK de texto largas y facilitar búsquedas por otros índices.
- **Tablas L0 en DB_SCHEMA:** Tablas `<conjunto>_<subconjunto>` con columnas del parquet (o inferidas) más extensiones CPV (`principal_prefix4`, `principal_prefix6`, `secondary_prefix6`) e `ingested_at`. Carga idempotente con `ON CONFLICT (natural_id) DO NOTHING`.
- **Registro de conjuntos (`CONJUNTOS_REGISTRY`):** Configuración por conjunto (subconjuntos, resolución de parquet, scripts, columnas). Catalunya/Valencia/Andalucía infieren esquema desde el parquet.
- **Integración con scripts:** Nacional invoca `nacional.licitaciones`; catalunya/valencia/andalucía ejecutan los scripts en `scripts/` (ccaa_cataluna, ccaa_valencia_parquet, ccaa_andalucia, etc.) cuando el parquet no existe.
- **Guía de usuario:** Sección «Comando ingest» en `docs/quick-guide-deploy.md` con tabla de conjuntos/subconjuntos, variables de entorno y ejemplos mínimos.
- **Rutina de tests (pytest):** `tests/test_ingest_l0.py` con tests unitarios (registro, rutas, derive_cpv_prefixes, inferencia de esquema) y test de idempotencia marcado con `@pytest.mark.integration` (requiere DB). Ejecución: `pytest tests/ -v` o `pytest tests/ -v -m "not integration"` para solo unitarios.
- **Dependencia pytest** en requirements.txt.

### Cambiado

- **ingest_l0:** `ensure_l0_table` y `load_parquet_to_l0` aceptan `column_defs` y `natural_id_col` opcionales para soportar fuentes genéricas (catalunya, valencia, andalucía). Nacional sigue usando el esquema fijo de 40 columnas.
- **CLI:** `cmd_ingest` valida conjunto y subconjunto contra el registro y enruta scripts y parquet según el conjunto.

### Nota

- E2E recomendado: `init-db` y luego `ingest nacional consultas_preliminares --anos 2026-2026`; re-ejecutar y comprobar 0 insertadas y N omitidas. Para catalunya/valencia/andalucía, ejecutar antes los scripts en `scripts/` para generar los parquets.

---

## [0.1.1] - Datos estáticos dim y schemas en Postgres init

### Añadido

- **Schemas en init de Postgres:** En el script de inicialización del microservicio Postgres (`init.d/01-pgvector.sql`) se crean los esquemas `dim`, `l0`, `l1` y `l2` tras la extensión pgvector, de modo que al levantar el contenedor las capas existan antes de que el ETL ejecute init-db.
- **Datos estáticos dim en init-db:** init-db aplica y rellena la capa dim: `dim.dim_ccaa`, `dim.dim_provincia` y `dim.dim_dir3`. Los esquemas 002_dim_ccaa y 002b_dim_provincia crean las tablas en el schema `dim` con tipos válidos en PostgreSQL (INTEGER sin precisión); la FK de provincias referencia `dim.dim_ccaa(num_code)`.
- **Ingesta DIR3 desde Listado Unidades AGE:** Tras aplicar `003_dim_dir3.sql`, init-db descarga el XLSX del Listado de información básica de unidades orgánicas de la AGE (administracionelectronica.gob.es / datos.gob.es), lo parsea con pandas (openpyxl), mapea columnas a `dim.dim_dir3` e inserta (TRUNCATE + INSERT). Logging en español con prefijo `[dir3_ingest]`; User-Agent, timeout y reintentos en la descarga. La URL es configurable con `DIR3_XLSX_URL`.
- **Dependencia openpyxl** en requirements.txt para lectura de XLSX.

### Cambiado

- **Orden de migraciones:** `SCHEMA_FILES` en init-db queda reducido a la capa dim: `001_dim_cpv.sql`, `002_dim_ccaa.sql`, `002b_dim_provincia.sql`, `003_dim_dir3.sql`, más scheduler (008, 009). cpv_router no es responsabilidad del ETL.
- **FK recursiva dim_dir3:** La restricción `fk_dim_dir3_parent` es `DEFERRABLE INITIALLY DEFERRED` para permitir insertar filas en cualquier orden; se comprueba al commit.

### Nota

- Las fuentes oficiales de los catálogos estáticos (CPV, DIR3, códigos de provincias y CCAA) se documentarán en una iteración posterior.

---

## [0.1.0] - Primera iteración publicable

### Añadido

- **Guía rápida de despliegue:** Documento `docs/quick-guide-deploy.md` (en español): requisitos, arranque rápido, tabla de comandos del CLI, variables de entorno y enlace a notas operativas. Sustituye a `how-to-use-deployment.md`.
- **Comando `status`:** Comprueba en un solo comando la conexión a la base de datos. Mensajes en español. Sustituye a comandos previos de comprobación, que pasan a ser subrutinas internas.
- **CLI en español:** Descripciones, ayuda de argumentos y mensajes de éxito/error en español.
- **Ayuda con atribución:** En `licitia-etl --help` se muestra el origen del fork (BquantFinance/licitaciones-espana) y la autoría del CLI (Sergio Berino, Grupo TOP Digital).
- **Descubribilidad:** En la ayuda se listan las integraciones (PostgreSQL), las variables de entorno (.env) y el orden recomendado: `status` → `init-db` → `ingest`.
- **Mensajes de retorno estándar:** Mensajes claros para salud (conexión DB, estado global) y para init-db / ingest; stderr para errores, exit 0/1 documentados.
- **Versión 0.1.0** fijada en el paquete; `--version` muestra la versión e indica CHANGELOG.md para el historial.

### Cambiado

- La lógica de comprobación de BD se usa desde `status` mediante `_comprobar_base_datos()`.
- Comentarios y docstrings en `cli.py` y `config.py` traducidos al español (invariante de microservicio atómico mantenido).

### Extensión futura (no implementado en esta iteración)

- Prefijo de log opcional (p. ej. variable `LICITIA_ETL_LOG_PREFIX`) para logs agregados; queda para una iteración posterior.
