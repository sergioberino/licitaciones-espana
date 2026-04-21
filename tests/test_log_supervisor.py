"""Tests for LogSupervisor — flight recorder pattern."""
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch


def test_log_writes_timestamped_line():
    """log() appends a timestamped line to the log file."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(log_path=log_path, db_url=None)
        sup.log("test message")
        content = log_path.read_text()
        assert "test message" in content
        assert "[INFO]" in content


def test_incident_writes_marker_and_enters_incident_mode():
    """incident() writes [INCIDENT] line, sets last_incident_at."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(log_path=log_path, db_url=None)
        sup.incident("task_failure", "Subprocess exited with code 1")
        content = log_path.read_text()
        assert "[INCIDENT]" in content
        assert "task_failure" in content
        assert sup.last_incident_at is not None


def test_maybe_trim_in_normal_mode():
    """In NORMAL mode, trim log to max_normal_lines when exceeded."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(log_path=log_path, db_url=None, max_normal_lines=10)
        for i in range(25):
            sup.log(f"line {i}")
        sup.maybe_trim()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) <= 10


def test_maybe_trim_preserves_in_incident_mode():
    """In INCIDENT mode, do NOT trim the log."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(log_path=log_path, db_url=None, max_normal_lines=10)
        for i in range(25):
            sup.log(f"line {i}")
        sup.incident("exception", "Something failed")
        sup.maybe_trim()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) > 10


def test_incident_mode_resets_after_cooldown():
    """After cooldown expires with no new incidents, mode resets to NORMAL."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(
            log_path=log_path, db_url=None, max_normal_lines=10, incident_cooldown_hours=24
        )
        sup.incident("exception", "Something failed")
        # Simulate cooldown expired
        sup.last_incident_at = datetime.now(timezone.utc) - timedelta(hours=25)
        for i in range(25):
            sup.log(f"post-cooldown {i}")
        sup.maybe_trim()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) <= 10
        assert sup.last_incident_at is None


def test_log_snapshot_captures_recent_lines():
    """get_log_snapshot() returns the last N lines of the log."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(log_path=log_path, db_url=None)
        for i in range(100):
            sup.log(f"line {i}")
        snapshot = sup.get_log_snapshot(50)
        lines = snapshot.strip().splitlines()
        assert len(lines) == 50
        assert "line 99" in lines[-1]


def test_heartbeat_writes_json():
    """write_heartbeat() writes valid JSON to heartbeat file."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        hb_path = Path(tmp) / "scheduler.heartbeat"
        sup = LogSupervisor(log_path=log_path, db_url=None, heartbeat_path=hb_path)
        sup.write_heartbeat(tick=42, pid=12345, tasks_due=1, tasks_running=0)
        data = json.loads(hb_path.read_text())
        assert data["tick"] == 42
        assert data["pid"] == 12345
        assert "ts" in data


def test_incident_persists_to_db_when_url_set(monkeypatch):
    """When db_url is set, incident() inserts into scheduler.incidents."""
    from etl.log_supervisor import LogSupervisor

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "scheduler.log"
        sup = LogSupervisor(log_path=log_path, db_url="postgresql://fake")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            sup.incident("task_failure", "test fail", severity="error")

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "scheduler.incidents" in sql
