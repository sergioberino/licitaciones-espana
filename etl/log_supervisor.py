"""LogSupervisor — flight recorder pattern for scheduler logging.

Two modes:
  NORMAL: rolling window of max_normal_lines, trims older lines each tick.
  INCIDENT: triggered by anomaly, stops trimming until cooldown or manual ack.

Incidents are persisted to scheduler.incidents (PostgreSQL) when db_url is set.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from zoneinfo import ZoneInfo

SCHEDULER_TZ = ZoneInfo("Europe/Madrid")


class LogSupervisor:
    def __init__(
        self,
        log_path: Path,
        db_url: Optional[str] = None,
        heartbeat_path: Optional[Path] = None,
        max_normal_lines: int = 200,
        incident_cooldown_hours: int = 24,
        snapshot_lines: int = 50,
    ):
        self.log_path = Path(log_path)
        self.db_url = db_url
        self.heartbeat_path = heartbeat_path
        self.max_normal_lines = max_normal_lines
        self.incident_cooldown = timedelta(hours=incident_cooldown_hours)
        self.snapshot_lines = snapshot_lines
        self.last_incident_at: Optional[datetime] = None

    def _ts(self) -> str:
        return datetime.now(SCHEDULER_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

    def log(self, msg: str, level: str = "INFO") -> None:
        line = f"[{self._ts()}] [{level}] {msg}\n"
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line)
        except OSError:
            print(line, file=sys.stderr, end="")

    def incident(
        self,
        category: str,
        summary: str,
        detail: Optional[str] = None,
        exc: Optional[BaseException] = None,
        run_id: Optional[int] = None,
        task_id: Optional[int] = None,
        severity: str = "error",
    ) -> Optional[int]:
        self.last_incident_at = datetime.now(timezone.utc)

        if exc is not None and detail is None:
            import traceback

            detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

        self.log(f"[INCIDENT] {category}: {summary}", level=severity.upper())
        if detail:
            for line in detail.strip().splitlines():
                self.log(f"  {line}", level=severity.upper())

        log_snapshot = self.get_log_snapshot(self.snapshot_lines)
        incident_id = self._persist_incident(
            category, summary, detail, log_snapshot, run_id, task_id, severity
        )
        return incident_id

    def _persist_incident(
        self,
        category: str,
        summary: str,
        detail: Optional[str],
        log_snapshot: str,
        run_id: Optional[int],
        task_id: Optional[int],
        severity: str,
    ) -> Optional[int]:
        if not self.db_url:
            return None
        try:
            import psycopg2

            with psycopg2.connect(self.db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO scheduler.incidents
                           (run_id, task_id, severity, category, summary, detail, log_snapshot)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           RETURNING incident_id""",
                        (run_id, task_id, severity, category, summary, detail, log_snapshot),
                    )
                    row = cur.fetchone()
                    conn.commit()
                    return row[0] if row else None
        except Exception:
            return None

    def get_log_snapshot(self, lines: int = 50) -> str:
        if not self.log_path.exists():
            return ""
        try:
            all_lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            return "\n".join(all_lines[-lines:])
        except OSError:
            return ""

    def maybe_trim(self) -> None:
        if self.last_incident_at is not None:
            elapsed = datetime.now(timezone.utc) - self.last_incident_at
            if elapsed >= self.incident_cooldown:
                self.log("Incident cooldown expired, resuming NORMAL mode", level="INFO")
                self.last_incident_at = None
            else:
                return

        if not self.log_path.exists():
            return
        try:
            all_lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            if len(all_lines) > self.max_normal_lines:
                trimmed = all_lines[-self.max_normal_lines :]
                self.log_path.write_text("\n".join(trimmed) + "\n", encoding="utf-8")
        except OSError:
            pass

    def write_heartbeat(
        self, tick: int, pid: int, tasks_due: int = 0, tasks_running: int = 0
    ) -> None:
        if self.heartbeat_path is None:
            return
        payload = {
            "pid": pid,
            "tick": tick,
            "ts": datetime.now(SCHEDULER_TZ).isoformat(),
            "tasks_due": tasks_due,
            "tasks_running": tasks_running,
        }
        try:
            self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            self.heartbeat_path.write_text(json.dumps(payload), encoding="utf-8")
        except OSError:
            pass

    def read_heartbeat(self) -> Optional[dict]:
        if self.heartbeat_path is None or not self.heartbeat_path.exists():
            return None
        try:
            return json.loads(self.heartbeat_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    @property
    def in_incident_mode(self) -> bool:
        return self.last_incident_at is not None
