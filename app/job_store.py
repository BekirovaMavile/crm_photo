import sqlite3
import threading
from datetime import datetime, timezone

from app.config import DATA_DIR, JOBS_DB_PATH

ACTIVE_STATUSES = ("queued", "running")


class JobStore:
    def __init__(self, db_path=JOBS_DB_PATH):
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._mark_stale_jobs()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    done INTEGER NOT NULL DEFAULT 0,
                    total INTEGER NOT NULL DEFAULT 0,
                    message TEXT NOT NULL DEFAULT '',
                    error TEXT,
                    date_from TEXT NOT NULL,
                    date_to TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _mark_stale_jobs(self):
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE jobs
                   SET status = 'error',
                       error = COALESCE(error, 'Сервер был перезапущен во время выполнения задачи'),
                       message = CASE
                           WHEN message IS NULL OR message = '' THEN 'Задача остановлена после рестарта сервера'
                           ELSE message
                       END,
                       updated_at = ?
                 WHERE status IN ({','.join('?' for _ in ACTIVE_STATUSES)})
                """,
                (now, *ACTIVE_STATUSES),
            )
            conn.commit()

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _row_to_dict(row):
        if row is None:
            return None
        return dict(row)

    def create_job(self, job_id: str, date_from: str, date_to: str):
        now = self._now()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, status, done, total, message, error,
                    date_from, date_to, created_at, updated_at
                ) VALUES (?, 'queued', 0, 0, '', NULL, ?, ?, ?, ?)
                """,
                (job_id, date_from, date_to, now, now),
            )
            conn.commit()

    def get_job(self, job_id: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            return self._row_to_dict(row)

    def get_active_job(self):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                  FROM jobs
                 WHERE status IN ('queued', 'running')
                 ORDER BY created_at ASC
                 LIMIT 1
                """
            ).fetchone()
            return self._row_to_dict(row)

    def mark_running(self, job_id: str, message: str = ""):
        self._update_fields(job_id, status="running", message=message, error=None)

    def mark_done(self, job_id: str, message: str = "Готово"):
        self._update_fields(job_id, status="done", message=message, error=None)

    def mark_error(self, job_id: str, error: str, message: str = ""):
        self._update_fields(job_id, status="error", error=error, message=message)

    def update_progress(self, job_id: str, done: int, total: int, message: str = ""):
        self._update_fields(
            job_id,
            done=max(0, int(done or 0)),
            total=max(0, int(total or 0)),
            message=message,
        )

    def _update_fields(self, job_id: str, **fields):
        fields = {key: value for key, value in fields.items() if value is not None}
        fields["updated_at"] = self._now()

        assignments = ", ".join(f"{name} = ?" for name in fields.keys())
        values = list(fields.values()) + [job_id]

        with self._lock, self._connect() as conn:
            conn.execute(
                f"UPDATE jobs SET {assignments} WHERE job_id = ?",
                values,
            )
            conn.commit()