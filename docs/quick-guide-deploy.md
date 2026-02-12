# Guía rápida de despliegue (ETL)

El microservicio ETL se ejecuta de forma **independiente** (configuras `DB_*` y `EMBEDDING_SERVICE_URL` a tus endpoints) o dentro de un stack orquestado. Toda la configuración es por **variables de entorno**.

## Requisitos

- **Docker** y **Docker Compose**
- Una instancia **PostgreSQL** accesible (local o remota)

## Arranque rápido

Desde la raíz de este servicio (`services/etl/`):

```bash
cp .env.example .env
# Editar .env: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD y opcionalmente EMBEDDING_SERVICE_URL

docker compose build
docker compose up -d
docker compose run --rm etl licitia-etl status
```

El compose solo levanta el servicio **etl**. Postgres debe estar corriendo en otro sitio; en `.env` apunta `DB_HOST` a ese host (p. ej. `localhost` o el nombre del servicio en tu red).

## Comandos del CLI (licitia-etl)

| Comando | Uso |
|--------|-----|
| **status** | Comprueba conexión a PostgreSQL y al servicio de embedding. |
| **init-db** | Aplica esquemas SQL y rellena dimensiones (p. ej. dim.cpv_dim). Una vez por entorno o tras cambios de esquema. |
| **generate_embedding --target cpv** | Rellena dim.cpv_router con vectores desde el servicio de embedding. Para ejecuciones programadas tras init-db. |

Orden recomendado: **status** → **init-db** → **generate_embedding --target cpv**.

### Ejecutar comandos

En un contenedor nuevo (se elimina al terminar):

```bash
docker compose run --rm etl licitia-etl status
docker compose run --rm etl licitia-etl init-db
docker compose run --rm etl licitia-etl generate_embedding --target cpv
```

Dentro del contenedor ya en marcha:

```bash
docker compose exec etl licitia-etl status
```

## Variables de entorno

- **DB_HOST**, **DB_PORT**, **DB_NAME**, **DB_USER**, **DB_PASSWORD** — Conexión a PostgreSQL.
- **EMBEDDING_SERVICE_URL** — URL base del API de embedding (p. ej. `http://embedding:8000`).
- **EMBED_BATCH_SIZE** (opcional, default `256`) — Textos por lote al servicio de embedding.
- **INGEST_BATCH_SIZE** (opcional, default `10000`) — Filas por INSERT masivo en dim.cpv_router.
- **EMBED_MAX_WORKERS** (opcional, default `1`) — Paralelismo de llamadas al embedding; `1` = secuencial.

Copiar `.env.example` a `.env` y rellenar al menos las variables `DB_*` y `EMBEDDING_SERVICE_URL`.

## Otros comandos útiles

```bash
docker compose exec etl bash          # Shell dentro del contenedor
docker compose logs -f etl            # Logs del servicio
docker compose down                  # Parar y eliminar contenedores
```

## Notas operativas

Ver [operational-notes.md](operational-notes.md) para tareas de auditoría y problemas conocidos (p. ej. errores en la ingestión de cpv_router durante `generate_embedding`).
