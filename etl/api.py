from fastapi import FastAPI
from fastapi.responses import JSONResponse

from etl.cli import _comprobar_base_datos

app = FastAPI(title="ETL API", version="1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
def status():
    ok, msg = _comprobar_base_datos()
    if ok:
        return {"status": "ok", "database": "connected"}
    return JSONResponse(
        status_code=503,
        content={"status": "error", "database": "unavailable"},
    )
