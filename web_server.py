import re
import secrets
import sys
import threading
import uuid
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request, send_from_directory, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / ".env"
load_dotenv(ENV_PATH)

from app.config import (  # noqa: E402
    HOST,
    PORT,
    WEB_ADMIN_PASSWORD_HASH,
    WEB_ADMIN_USERNAME,
    WEB_SECRET_KEY,
    WEB_SESSION_COOKIE_SECURE,
    WEB_TRUSTED_PROXY_HOPS,
)
from app.job_store import JobStore  # noqa: E402

STATIC_DIR = ROOT / "static"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")
app.secret_key = WEB_SECRET_KEY or secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=WEB_SESSION_COOKIE_SECURE,
)

if WEB_TRUSTED_PROXY_HOPS > 0:
    app.wsgi_app = ProxyFix(
        app.wsgi_app,
        x_for=WEB_TRUSTED_PROXY_HOPS,
        x_proto=WEB_TRUSTED_PROXY_HOPS,
        x_host=WEB_TRUSTED_PROXY_HOPS,
        x_port=WEB_TRUSTED_PROXY_HOPS,
        x_prefix=WEB_TRUSTED_PROXY_HOPS,
    )

job_store = JobStore()
export_lock = threading.Lock()


def _norm_cred(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).replace("\r", "").replace("\ufeff", "").strip()

from app.config import CRM_USERNAME, CRM_PASSWORD

def _auth_ok(username: str, password: str) -> bool:
    return (
        username.strip() == CRM_USERNAME.strip()
        and password.strip() == CRM_PASSWORD.strip()
    )
    
def _is_public_unauthenticated() -> bool:
    path = request.path
    method = request.method
    if path == "/login" and method == "GET":
        return True
    if path == "/api/login" and method == "POST":
        return True
    if path == "/api/logout" and method == "POST":
        return True
    if path in ("/style.css", "/login.js"):
        return True
    return False


@app.before_request
def require_auth():
    if session.get("logged_in"):
        return None
    if _is_public_unauthenticated():
        return None
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Требуется вход"}), 401
    if request.path == "/app.js":
        return "", 401
    return redirect(url_for("login_page"))


@app.route("/login")
def login_page():
    if session.get("logged_in"):
        return redirect(url_for("index"))
    return send_from_directory(app.static_folder, "login.html")


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


def _validate_dates(date_from: str, date_to: str) -> str | None:
    if not DATE_RE.match(date_from) or not DATE_RE.match(date_to):
        return "Даты укажите в формате YYYY-MM-DD"
    if date_from > date_to:
        return "Дата начала не может быть позже даты конца"
    return None


def _run_export_job(job_id: str, date_from: str, date_to: str):
    from app.exporter import Exporter
    from app.logger import setup_logger

    if not export_lock.acquire(blocking=False):
        job_store.mark_error(job_id, "Уже выполняется другой экспорт")
        return

    try:
        job_store.mark_running(job_id, message="Подготовка к экспорту")

        logger = setup_logger()
        exporter = Exporter(logger)

        def progress_cb(info: dict):
            job_store.update_progress(
                job_id=job_id,
                done=info.get("done", 0),
                total=info.get("total", 0),
                message=info.get("message") or "",
            )

        exporter.run(date_from, date_to, progress_callback=progress_cb)
        job_store.mark_done(job_id, message="Готово")
    except Exception as exc:
        job_store.mark_error(job_id, str(exc))
    finally:
        export_lock.release()


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = data.get("username") or ""
    password = data.get("password") or ""
    if _auth_ok(username, password):
        session.clear()
        session["logged_in"] = True
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Неверный логин или пароль"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/export", methods=["POST"])
def start_export():
    data = request.get_json(silent=True) or {}
    date_from = (data.get("date_from") or "").strip()
    date_to = (data.get("date_to") or "").strip()

    err = _validate_dates(date_from, date_to)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    active_job = job_store.get_active_job()
    if active_job:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Экспорт уже выполняется или стоит в очереди",
                    "active_job_id": active_job["job_id"],
                }
            ),
            409,
        )

    job_id = str(uuid.uuid4())
    job_store.create_job(job_id=job_id, date_from=date_from, date_to=date_to)

    t = threading.Thread(
        target=_run_export_job,
        args=(job_id, date_from, date_to),
        daemon=True,
        name=f"export-job-{job_id}",
    )
    t.start()

    return jsonify({"ok": True, "job_id": job_id})


@app.route("/api/jobs/<job_id>")
def job_status(job_id: str):
    job = job_store.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Задача не найдена"}), 404
    return jsonify({"ok": True, "job": job})


@app.route("/api/active-job")
def active_job():
    job = job_store.get_active_job()
    return jsonify({"ok": True, "job": job})


def main():
    missing = []
    if not _norm_cred(WEB_ADMIN_USERNAME):
        missing.append("WEB_ADMIN_USERNAME")
    if not _norm_cred(WEB_ADMIN_PASSWORD_HASH):
        missing.append("WEB_ADMIN_PASSWORD_HASH")

    if missing:
        print(
            "Не заданы обязательные переменные для веб-входа: " + ", ".join(missing),
            file=sys.stderr,
        )
        sys.exit(1)

    if not WEB_SECRET_KEY:
        print(
            "Предупреждение: WEB_SECRET_KEY не задан — сессии сбросятся после рестарта сервера.",
            file=sys.stderr,
        )

    print(f"Файл настроек: {ENV_PATH}", file=sys.stderr)
    print(f"Веб-логин: {WEB_ADMIN_USERNAME}", file=sys.stderr)
    print(f"SESSION_COOKIE_SECURE={WEB_SESSION_COOKIE_SECURE}", file=sys.stderr)
    print(f"TRUSTED_PROXY_HOPS={WEB_TRUSTED_PROXY_HOPS}", file=sys.stderr)

    app.run(host=HOST, port=PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()