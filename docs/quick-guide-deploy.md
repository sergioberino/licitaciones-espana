# Guía rápida de despliegue (ETL)

El microservicio ETL se ejecuta de forma **independiente** o dentro de un stack orquestado. Toda la configuración es por **variables de entorno**.

## Requisitos

- **Docker** y **Docker Compose**
- Una instancia **PostgreSQL** accesible (local o remota)

## Arranque rápido

Desde la raíz de este servicio (`services/etl/`):

```bash
cp .env.example .env
# Editar .env con los datos de conexión a la base de datos

docker compose build
docker compose up -d
docker compose run --rm etl licitia-etl status
```

El compose solo levanta el servicio **etl**. Postgres debe estar corriendo en otro sitio; en `.env` configurar el host y credenciales de la base de datos.

## Comandos del CLI (licitia-etl)

| Comando | Uso |
|--------|-----|
| **status** | Comprueba conexión a la base de datos. |
| **init-db** | Aplica esquemas SQL y rellena dimensiones (p. ej. dim.cpv_dim). Una vez por entorno o tras cambios de esquema. |
| **ingest** *conjunto* *subconjunto* [*--anos*] | Descarga/genera parquet e ingesta en tablas L0 del schema de trabajo. Ver sección *Comando ingest* más abajo. |
| **scheduler register** [*conjuntos*] | Registra tareas programadas. Sin argumentos: todos los conjuntos. |
| **scheduler run** [*-d*] | Inicia el scheduler (ejecuta tareas según frecuencia). Con *-d* se ejecuta en segundo plano; la salida se escribe en `scheduler.log` (mismo directorio que el PID). |
| **scheduler stop** | Detiene el proceso del scheduler (SIGTERM al PID del archivo). |
| **scheduler status** | Lista tareas con última ejecución, estado (scheduled/running/failed) y **PRÓXIMA EJECUCIÓN**. |
| **health** | Comprueba BD y schema scheduler; código 0/1 para supervisión. |
| **db-info** | Muestra tamaño por schema y listado de tablas. |

Orden recomendado: **status** → **init-db** → **ingest**; para ejecución programada: **scheduler register** → **scheduler run**.

### Comando ingest

Ingesta datos de contratación pública en tablas L0 (una tabla por conjunto y subconjunto). Las tablas usan **PK surrogada** (`l0_id` BIGSERIAL) y **natural_id** UNIQUE (URL o identificador de fuente) para facilitar búsquedas y evitar PK de texto largas.

**Conjuntos y subconjuntos admitidos:**

| Conjunto   | Subconjuntos (ejemplos) | Requiere --anos |
|-----------|--------------------------|-----------------|
| **nacional** | licitaciones, agregacion_ccaa, contratos_menores, encargos_medios_propios, consultas_preliminares | Sí (X-Y) |
| **catalunya** | contratacion_registro, subvenciones_raisc, convenios, presupuestos_aprobados, rrhh_altos_cargos | No |
| **valencia** | contratacion, subvenciones, presupuestos, convenios, empleo, paro, lobbies, … | No |
| **andalucia** | licitaciones | No |
| **euskadi** | contratos_master, poderes_adjudicadores, empresas_licitadoras, revascon_historico, bilbao_contratos, ultimos_90d | No |
| **madrid** | comunidad, ayuntamiento | No |
| **ted** | ted_es_can (puede omitirse) | Sí (X-Y) |

**Configuración:** Se requiere base de datos configurada y schema de trabajo. El directorio temporal para parquets es opcional.

**Listar subconjuntos de un conjunto:** Para ver los argumentos posibles de *subconjunto* para un conjunto dado, use `--subconjuntos` (no requiere BD ni subconjunto):

```bash
licitia-etl ingest valencia --subconjuntos
licitia-etl ingest nacional --subconjuntos
```

**Ejemplos mínimos:**

```bash
# Nacional (obligatorio --anos). Si no existe el parquet, se ejecuta el script nacional.
licitia-etl ingest nacional consultas_preliminares --anos 2026-2026
licitia-etl ingest nacional contratos_menores --anos 2023-2023

# Solo cargar parquet ya generado (sin ejecutar script)
licitia-etl ingest nacional consultas_preliminares --anos 2026-2026 --solo-procesar

# Catalunya / Valencia / Andalucía: ejecutar primero los scripts en scripts/ para generar parquets, luego ingest.
licitia-etl ingest catalunya contratacion_registro
licitia-etl ingest valencia contratacion
licitia-etl ingest andalucia licitaciones

# Euskadi / Madrid / TED: generar parquets con los scripts en Euskadi/, comunidad_madrid/, ted/ y luego ingest.
# Para euskadi: el subconjunto solo determina qué parquet se carga en L0; la descarga/consolidación siempre es completa.
licitia-etl ingest euskadi contratos_master
licitia-etl ingest madrid comunidad
licitia-etl ingest ted ted_es_can   # puede omitir el subconjunto: licitia-etl ingest ted --anos 2024-2024
```

Idempotencia: re-ejecutar el mismo comando no duplica filas (ON CONFLICT natural_id DO NOTHING).

**Tests de ingest:** Con `conjunto=test` se ejecuta la suite de tests (pytest). Sin opciones: solo unitarios (registro, rutas, CPV, inferencia; no tocan BD). Con `--integration`: además test de idempotencia en BD (parquet mínimo). Con `--conjuntos`: E2E del flujo completo (descarga si aplica + lectura parquet + ingest) de un subconjunto por conjunto, **en schema `test`** (no toca DB_SCHEMA). Con `--delete`: elimina el schema `test` (solo `ingest test --delete` para limpiar; o `ingest test --conjuntos --delete` para borrar `test` al finalizar el E2E).

```bash
licitia-etl ingest test
licitia-etl ingest test --integration   # incluye test de idempotencia en BD
licitia-etl ingest test --verbose       # más detalle (-vv, log DEBUG)
licitia-etl ingest test --conjuntos     # E2E: descarga + ingest en schema test (un subconjunto por conjunto)
licitia-etl ingest test --delete        # elimina el schema test (limpieza)
licitia-etl ingest test --conjuntos --delete   # E2E y luego elimina schema test
```

### Ejecutar comandos

En un contenedor nuevo (se elimina al terminar):

```bash
docker compose run --rm etl licitia-etl status
docker compose run --rm etl licitia-etl init-db
docker compose run --rm etl licitia-etl scheduler register
docker compose run --rm etl licitia-etl scheduler run
```

Dentro del contenedor ya en marcha:

```bash
docker compose exec etl licitia-etl status
docker compose exec etl licitia-etl scheduler stop
```

## Variables de entorno

Copiar `.env.example` a `.env` y rellenar los valores de conexión a la base de datos y el schema de trabajo. Las variables opcionales (directorio temporal, tamaño de lote de ingest) tienen valores por defecto.

## Otros comandos útiles

```bash
docker compose exec etl bash          # Shell dentro del contenedor
docker compose logs -f etl            # Logs del servicio
docker compose down                  # Parar y eliminar contenedores
```

