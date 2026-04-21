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
                    control_status TEXT NOT NULL DEFAULT 'running',
                    date_from TEXT NOT NULL,
                    date_to TEXT NOT NULL,
                    records_streets INTEGER NOT NULL DEFAULT 0,
                    records_spdp INTEGER NOT NULL DEFAULT 0,
                    attachments_streets INTEGER NOT NULL DEFAULT 0,
                    attachments_spdp INTEGER NOT NULL DEFAULT 0,
                    cleanup_status TEXT NOT NULL DEFAULT 'not_started',
                    cleanup_done INTEGER NOT NULL DEFAULT 0,
                    cleanup_total INTEGER NOT NULL DEFAULT 0,
                    cleanup_deleted INTEGER NOT NULL DEFAULT 0,
                    cleanup_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploaded_fields (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    field_id TEXT NOT NULL,
                    UNIQUE(job_id, entity_id, item_id, field_id)
                )
                """
            )
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
            if "control_status" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN control_status TEXT NOT NULL DEFAULT 'running'")
            if "records_streets" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN records_streets INTEGER NOT NULL DEFAULT 0")
            if "records_spdp" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN records_spdp INTEGER NOT NULL DEFAULT 0")
            if "attachments_streets" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN attachments_streets INTEGER NOT NULL DEFAULT 0")
            if "attachments_spdp" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN attachments_spdp INTEGER NOT NULL DEFAULT 0")
            if "cleanup_status" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN cleanup_status TEXT NOT NULL DEFAULT 'not_started'")
            if "cleanup_done" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN cleanup_done INTEGER NOT NULL DEFAULT 0")
            if "cleanup_total" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN cleanup_total INTEGER NOT NULL DEFAULT 0")
            if "cleanup_deleted" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN cleanup_deleted INTEGER NOT NULL DEFAULT 0")
            if "cleanup_error" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN cleanup_error TEXT")
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
                    job_id, status, done, total, message, error, control_status,
                    date_from, date_to, created_at, updated_at
                ) VALUES (?, 'queued', 0, 0, '', NULL, 'running', ?, ?, ?, ?)
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

    def mark_paused(self, job_id: str, message: str = "Пауза"):
        self._update_fields(job_id, control_status="paused", message=message)

    def mark_resumed(self, job_id: str, message: str = "Возобновлено"):
        self._update_fields(job_id, control_status="running", message=message)

    def mark_cancel_requested(self, job_id: str, message: str = "Остановка запрошена"):
        self._update_fields(job_id, control_status="cancelled", message=message)

    def mark_cancelled(self, job_id: str, message: str = "Остановлено пользователем"):
        self._update_fields(job_id, status="cancelled", control_status="cancelled", message=message)

    def set_stats(
        self,
        job_id: str,
        records_streets: int = 0,
        records_spdp: int = 0,
        attachments_streets: int = 0,
        attachments_spdp: int = 0,
    ):
        self._update_fields(
            job_id,
            records_streets=max(0, int(records_streets or 0)),
            records_spdp=max(0, int(records_spdp or 0)),
            attachments_streets=max(0, int(attachments_streets or 0)),
            attachments_spdp=max(0, int(attachments_spdp or 0)),
        )

    def list_history(self, limit: int = 100):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                  FROM jobs
                 WHERE status IN ('done', 'cancelled')
                 ORDER BY created_at DESC
                 LIMIT ?
                """,
                (max(1, int(limit or 100)),),
            ).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def delete_job(self, job_id: str):
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM uploaded_fields WHERE job_id = ?", (job_id,))
            conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
            conn.commit()

    def delete_history(self):
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM uploaded_fields")
            conn.execute("DELETE FROM jobs")
            conn.commit()

    def add_uploaded_field(self, job_id: str, entity_id: str, item_id: str, field_id: str):
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO uploaded_fields (job_id, entity_id, item_id, field_id)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, str(entity_id), str(item_id), str(field_id)),
            )
            conn.commit()

    def list_uploaded_fields(self, job_id: str):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT entity_id, item_id, field_id
                  FROM uploaded_fields
                 WHERE job_id = ?
                 ORDER BY id ASC
                """,
                (job_id,),
            ).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def mark_cleanup_running(self, job_id: str, total: int):
        self._update_fields(
            job_id,
            cleanup_status="running",
            cleanup_total=max(0, int(total or 0)),
            cleanup_done=0,
            cleanup_deleted=0,
            cleanup_error=None,
        )

    def update_cleanup_progress(self, job_id: str, done: int, deleted: int, message: str = ""):
        self._update_fields(
            job_id,
            cleanup_done=max(0, int(done or 0)),
            cleanup_deleted=max(0, int(deleted or 0)),
            message=message,
        )

    def mark_cleanup_done(self, job_id: str, deleted: int):
        self._update_fields(
            job_id,
            cleanup_status="done",
            cleanup_deleted=max(0, int(deleted or 0)),
            message="Фото удалены из CRM",
        )

    def mark_cleanup_error(self, job_id: str, error: str):
        self._update_fields(
            job_id,
            cleanup_status="error",
            cleanup_error=str(error or ""),
        )

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