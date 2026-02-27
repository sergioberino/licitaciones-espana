# ETL API Reference

> **Interactive docs:** When the ETL service is running, full Swagger UI is available at `GET /docs` and ReDoc at `GET /redoc`.

Base URL: `http://<host>:8787`

---

## Health & Status

### `GET /health`

Health check — returns service liveness and database connectivity.

**Response:**

```json
{ "status": "ok", "db": true }
```

### `GET /status`

Database status — checks the database connection.

**Response (200):**

```json
{ "status": "ok", "database": "connected" }
```

**Response (503):**

```json
{ "status": "error", "database": "unavailable" }
```

### `GET /db-info`

Database info — schemas with sizes, table listing, and total database size.

**Response (200):**

```json
{
  "schemas": [{ "schema_name": "l0", "size": "1200 MB" }],
  "tables": [{ "schema": "l0", "name": "contratos" }],
  "total_size": "1500 MB"
}
```

---

## Ingest

### `GET /ingest/conjuntos`

List available conjuntos and their subconjuntos (same catalog as the CLI).

**Response:**

```json
{
  "conjuntos": [
    { "id": "nacional", "subconjuntos": ["contratos", "licitaciones"] }
  ]
}
```

### `POST /ingest/run`

Run ingest for a conjunto/subconjunto (non-blocking). Spawns a CLI subprocess.

**Request body:**

```json
{
  "conjunto": "nacional",
  "subconjunto": "contratos",
  "anos": "2023-2024",
  "solo_descargar": false,
  "solo_procesar": false
}
```

| Field            | Type   | Required | Description                          |
|------------------|--------|----------|--------------------------------------|
| `conjunto`       | string | yes      | Conjunto ID from the registry        |
| `subconjunto`    | string | no       | Subconjunto; auto-selected if only 1 |
| `anos`           | string | no       | Year range, e.g. `"2023-2024"`       |
| `solo_descargar` | bool   | no       | Download only, skip processing       |
| `solo_procesar`  | bool   | no       | Process only, skip download           |

**Response (200):**

```json
{ "ok": true, "message": "Ingest de nacional/contratos iniciado en segundo plano (PID: 12345)..." }
```

### `GET /ingest/current-run`

Current running ingest job — returns the active scheduler run record, if any.

**Response:**

```json
{ "running": true, "run": { "conjunto": "nacional", "subconjunto": "contratos", "started_at": "2025-01-15T10:00:00" } }
```

### `GET /ingest/log`

Tail the ingest subprocess log.

**Query params:**

| Param   | Type | Default | Description              |
|---------|------|---------|--------------------------|
| `lines` | int  | 80      | Number of lines to return |

**Response:**

```json
{ "lines": ["line1", "line2"], "exists": true, "total_lines": 150 }
```

---

## Scheduler

### `GET /scheduler/status`

Scheduler tasks status — lists all registered tasks with last run info and computed `next_run_at`.

**Response:**

```json
{
  "tasks": [
    {
      "conjunto": "nacional",
      "subconjunto": "contratos",
      "last_status": "ok",
      "last_finished_at": "2025-01-15T12:00:00",
      "next_run_at": "2025-04-15T00:00:00"
    }
  ]
}
```

### `POST /scheduler/register`

Register scheduler tasks from CONJUNTOS_REGISTRY.

**Request body (optional):**

```json
{ "conjuntos": ["nacional", "galicia"] }
```

If `conjuntos` is empty or omitted, all conjuntos are registered.

**Response:**

```json
{ "ok": true, "message": "Tasks registered" }
```

### `POST /scheduler/run`

Run a single scheduler task (blocking) or start the full scheduler loop in background.

**Request body:**

```json
{
  "conjunto": "nacional",
  "subconjunto": "contratos",
  "detach": false
}
```

| Field         | Type   | Required | Description                                    |
|---------------|--------|----------|------------------------------------------------|
| `conjunto`    | string | no       | Conjunto ID (required if `detach=false`)        |
| `subconjunto` | string | no       | Subconjunto; auto-selected if only 1            |
| `detach`      | bool   | no       | `true` to start background scheduler loop (202) |

**Response (200):** `{ "ok": true, "message": "Task completed" }`

**Response (202):** `{ "ok": true, "message": "Scheduler started in background" }`

### `POST /scheduler/stop`

Stop the scheduler process (sends SIGTERM).

**Response:**

```json
{ "ok": true, "message": "Scheduler stop requested" }
```

### `POST /scheduler/recover`

Recover stale scheduler runs — marks orphaned "running" records as failed.

**Response:**

```json
{ "ok": true, "recovered": 2 }
```

---

## BORME

### `POST /borme/ingest`

BORME: scrape + parse + load into borme schema. Spawns a background process.

**Request body:**

```json
{ "anos": "2024-2024" }
```

**Response:**

```json
{ "ok": true, "message": "BORME ingest started (PID: 12345)", "pid": 12345, "anos": "2024-2024" }
```

### `POST /borme/anomalias`

BORME: anomaly detector vs L0 nacional. Spawns a background process.

**Request body:**

```json
{ "anos": "2024-2024", "anonimizar": false }
```

| Field        | Type   | Required | Description                          |
|--------------|--------|----------|--------------------------------------|
| `anos`       | string | yes      | Year range, e.g. `"2024-2024"`       |
| `anonimizar` | bool   | no       | Anonymize contractor names in output |

**Response:**

```json
{ "ok": true, "message": "BORME anomalías started (PID: 12345)", "pid": 12345, "anos": "2024-2024", "anonimizar": false }
```

### `GET /borme/jobs/{job_id}`

BORME job status — check if a BORME background process is still running.

**Path params:** `job_id` — the PID returned by `/borme/ingest` or `/borme/anomalias`.

**Response:**

```json
{ "job_id": "12345", "alive": true, "status": "running", "log_tail": ["..."] }
```

### `GET /borme/log`

Tail the BORME subprocess log.

**Query params:**

| Param   | Type | Default | Description              |
|---------|------|---------|--------------------------|
| `lines` | int  | 80      | Number of lines to return |

**Response:**

```json
{ "lines": ["line1", "line2"], "exists": true, "total_lines": 50 }
```
