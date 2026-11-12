"""Job registry and background lifecycle for timeline consolidation.

State lives in a small SQLite database under the data volume, so a process
restart never loses job state:

* jobs that were ``pending`` or ``running`` when the process died are marked
  ``failed`` with ``error.code = "interrupted"`` during startup recovery;
* ``done`` jobs keep their status and their result files remain available for
  download;
* ``failed`` jobs keep their registry entry so the error stays queryable.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings, load_settings
from .processor import (
    INPUT_NAME,
    OUTPUT_NAME,
    PipelineError,
    WORK_DB_NAME,
    run_pipeline,
)

REGISTRY_NAME = "jobs.sqlite3"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class JobStore:
    """Process-wide registry + bounded worker pool for timeline jobs."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(settings.data_dir / REGISTRY_NAME), check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id        TEXT PRIMARY KEY,
                status        TEXT NOT NULL,
                error         TEXT,
                input_lines   INTEGER,
                unique_events INTEGER,
                duplicate_events INTEGER,
                awb_count     INTEGER,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
        self._pool = ThreadPoolExecutor(
            max_workers=settings.worker_count, thread_name_prefix="timeline"
        )
        self._recover_interrupted()

    # -- recovery -----------------------------------------------------------

    def _recover_interrupted(self) -> None:
        now = _utc_now()
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_id FROM jobs WHERE status NOT IN ('done', 'failed')"
            ).fetchall()
            for row in rows:
                self._fail_job_locked(
                    row["job_id"],
                    "interrupted",
                    "service restarted before the job finished",
                    now,
                )
            self._conn.commit()

    def _fail_job_locked(self, job_id: str, code: str, message: str, now: str) -> None:
        self._conn.execute(
            "UPDATE jobs SET status = 'failed', error = ?, updated_at = ? WHERE job_id = ?",
            (json.dumps({"code": code, "message": message}), now, job_id),
        )
        job_dir = self.settings.data_dir / job_id
        (job_dir / WORK_DB_NAME).unlink(missing_ok=True)
        (job_dir / f"{OUTPUT_NAME}.tmp").unlink(missing_ok=True)

    # -- job creation -------------------------------------------------------

    def create_job(self) -> tuple[str, Path]:
        job_id = uuid.uuid4().hex
        job_dir = self.settings.data_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=False)
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                "INSERT INTO jobs (job_id, status, created_at, updated_at) "
                "VALUES (?, 'pending', ?, ?)",
                (job_id, now, now),
            )
            self._conn.commit()
        return job_id, job_dir / INPUT_NAME

    def mark_running(self, job_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'running', updated_at = ? WHERE job_id = ?",
                (_utc_now(), job_id),
            )
            self._conn.commit()

    def fail(self, job_id: str, code: str, message: str) -> None:
        """Mark a job failed from outside the worker (e.g. upload persistence)."""
        with self._lock:
            self._fail_job_locked(job_id, code, message, _utc_now())
            self._conn.commit()

    def submit(self, job_id: str) -> None:
        self._pool.submit(self._run, job_id)

    # -- worker -------------------------------------------------------------

    def _run(self, job_id: str) -> None:
        self.mark_running(job_id)
        job_dir = self.settings.data_dir / job_id
        try:
            stats = run_pipeline(job_dir, self.settings)
        except PipelineError as exc:
            with self._lock:
                self._fail_job_locked(job_id, exc.code, str(exc), _utc_now())
                self._conn.commit()
        except Exception as exc:  # defensive: never leave a job stuck
            with self._lock:
                self._fail_job_locked(job_id, "internal_error", str(exc), _utc_now())
                self._conn.commit()
        else:
            now = _utc_now()
            with self._lock:
                self._conn.execute(
                    "UPDATE jobs SET status = 'done', error = NULL, "
                    "input_lines = ?, unique_events = ?, duplicate_events = ?, "
                    "awb_count = ?, updated_at = ? WHERE job_id = ?",
                    (
                        stats.input_lines,
                        stats.unique_events,
                        stats.duplicate_events,
                        stats.awb_count,
                        now,
                        job_id,
                    ),
                )
                self._conn.commit()
            # Input bytes are no longer needed once a durable result exists.
            (job_dir / INPUT_NAME).unlink(missing_ok=True)

    # -- queries ------------------------------------------------------------

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        data = dict(row)
        data["error"] = json.loads(data["error"]) if data["error"] else None
        return data

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._row_to_dict(row) if row is not None else None

    def list_jobs(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC, job_id DESC"
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def result_path(self, job_id: str) -> Path | None:
        job = self.get(job_id)
        if job is None or job["status"] != "done":
            return None
        path = self.settings.data_dir / job_id / OUTPUT_NAME
        return path if path.is_file() else None

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True, cancel_futures=False)
        self._conn.close()


_store: JobStore | None = None
_store_lock = threading.Lock()


def get_store() -> JobStore:
    """Lazily build the process-wide store (also works without a lifespan)."""
    global _store
    store = _store
    if store is None:
        with _store_lock:
            store = _store
            if store is None:
                store = JobStore(load_settings())
                _store = store
    return store


def reset_store(settings: Settings | None = None) -> JobStore:
    """Test/startup helper: tear down the current store and build a fresh one."""
    global _store
    with _store_lock:
        if _store is not None:
            _store.shutdown()
        _store = JobStore(settings or load_settings())
        return _store
