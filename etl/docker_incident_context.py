"""Contexto para incidentes de arranque: listado de contenedores vía API Docker (socket Unix).

No requiere el binario `docker` en la imagen. Si /var/run/docker.sock no está montado,
devuelve contexto alternativo (hostname, cgroup) para tickets en español.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import Any


DOCKER_SOCK = "/var/run/docker.sock"
# Rutas habituales de la API Engine
_CONTAINER_LIST_PATHS = (
    "/v1.41/containers/json?all=true",
    "/v1.40/containers/json?all=true",
    "/containers/json?all=true",
)


def _http_get_unix(
    socket_path: str, request_path: str, timeout_sec: float = 5.0
) -> tuple[int, str, bytes]:
    """GET sobre socket Unix; devuelve (status_http, cabeceras_texto, cuerpo_raw tras el doble CRLF)."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout_sec)
    try:
        s.connect(socket_path)
        req = (
            f"GET {request_path} HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        s.sendall(req)
        chunks: list[bytes] = []
        while True:
            b = s.recv(65536)
            if not b:
                break
            chunks.append(b)
    finally:
        s.close()
    raw = b"".join(chunks)
    if b"\r\n\r\n" not in raw:
        return 0, "", raw
    head, initial_body = raw.split(b"\r\n\r\n", 1)
    head_text = head.decode("utf-8", errors="replace")
    first_line = head_text.split("\r\n")[0]
    status = 0
    parts = first_line.split()
    if len(parts) >= 2 and parts[1].isdigit():
        status = int(parts[1])
    return status, head_text, initial_body


def _header_value(head_text: str, header_name: str) -> str:
    prefix = header_name.lower() + ":"
    for line in head_text.split("\r\n")[1:]:
        if line.lower().startswith(prefix):
            return line.split(":", 1)[1].strip()
    return ""


def _decode_chunked(data: bytes) -> bytes:
    """Decodifica cuerpo HTTP con Transfer-Encoding: chunked."""
    out = bytearray()
    i = 0
    n = len(data)
    while i < n:
        line_end = data.find(b"\r\n", i)
        if line_end < 0:
            break
        size_part = data[i:line_end].decode("ascii", errors="replace").split(";", 1)[0].strip()
        try:
            chunk_len = int(size_part, 16)
        except ValueError:
            break
        i = line_end + 2
        if chunk_len == 0:
            break
        if i + chunk_len > n:
            out.extend(data[i:])
            break
        out.extend(data[i : i + chunk_len])
        i += chunk_len
        if i + 2 <= n and data[i : i + 2] == b"\r\n":
            i += 2
    return bytes(out)


def _decoded_response_body(head_text: str, body: bytes) -> bytes:
    """Aplica Content-Length y/o chunked según cabeceras."""
    te = _header_value(head_text, "transfer-encoding").lower()
    if "chunked" in te:
        return _decode_chunked(body)
    cl_s = _header_value(head_text, "content-length")
    if cl_s.isdigit():
        cl = int(cl_s)
        if len(body) > cl:
            return body[:cl]
    return body


def _cgroup_hint() -> str:
    try:
        text = Path("/proc/self/cgroup").read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            if "docker" in line.lower() or "containerd" in line.lower():
                return line.strip()[:240]
    except OSError:
        pass
    return ""


def _format_container_rows(containers: list[dict[str, Any]], max_rows: int) -> str:
    lines = ["NOMBRE\tIMAGEN\tESTADO"]
    for c in containers[:max_rows]:
        names = c.get("Names") or []
        name = (names[0] if names else "").lstrip("/")
        image = (c.get("Image") or "")[:80]
        state = c.get("State", "")
        status = (c.get("Status") or "")[:120]
        est = f"{state} — {status}" if state else status
        lines.append(f"{name}\t{image}\t{est}")
    if len(containers) > max_rows:
        lines.append(f"(… {len(containers) - max_rows} contenedores más)")
    return "\n".join(lines)


def collect_container_snapshot(max_rows: int = 35) -> str:
    """Texto multilínea para adjuntar a detalle de ticket / incidente (es-ES)."""
    parts: list[str] = []
    host = os.environ.get("HOSTNAME") or os.uname().nodename
    parts.append(f"Host del contenedor ETL: {host}")

    if not os.path.exists(DOCKER_SOCK):
        parts.append(
            "Docker: no hay socket en /var/run/docker.sock "
            "(monte read-only el socket en compose para listar contenedores)."
        )
        cg = _cgroup_hint()
        if cg:
            parts.append(f"cgroup (referencia): {cg}")
        return "\n".join(parts)

    last_err = ""
    for path in _CONTAINER_LIST_PATHS:
        try:
            status, head, raw_body = _http_get_unix(DOCKER_SOCK, path)
            if status != 200:
                last_err = f"HTTP {status}"
                continue
            body = _decoded_response_body(head, raw_body)
            text = body.decode("utf-8", errors="replace").strip()
            if not text.startswith("["):
                last_err = "respuesta no JSON"
                continue
            data = json.loads(text)
            if not isinstance(data, list):
                last_err = "JSON inesperado"
                continue
            parts.append("Listado de contenedores (equivalente a docker ps -a):")
            parts.append(_format_container_rows(data, max_rows))
            return "\n".join(parts)
        except (OSError, TimeoutError, json.JSONDecodeError) as e:
            last_err = str(e)
            continue

    parts.append(f"No se pudo obtener el listado Docker: {last_err or 'error desconocido'}.")
    cg = _cgroup_hint()
    if cg:
        parts.append(f"cgroup (referencia): {cg}")
    return "\n".join(parts)
