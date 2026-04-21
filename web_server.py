import re
import secrets
import sys
import threading
import uuid
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request, send_from_directory, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / ".env"
load_dotenv(ENV_PATH)

from app.config import (  # noqa: E402
    CRM_FIELDS,
    CRM_PASSWORD,
    CRM_USERNAME,
    HOST,
    PORT,
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
job_controls = {}
cleanup_lock = threading.Lock()


def _norm_cred(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).replace("\r", "").replace("\ufeff", "").strip()


def _auth_ok(username: str, password: str) -> bool:
    return (
        _norm_cred(username) == _norm_cred(CRM_USERNAME)
        and _norm_cred(password) == _norm_cred(CRM_PASSWORD)
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
        job_controls.setdefault(job_id, {"paused": False, "cancelled": False})

        logger = setup_logger()
        exporter = Exporter(logger)

        def progress_cb(info: dict):
            job_store.update_progress(
                job_id=job_id,
                done=info.get("done", 0),
                total=info.get("total", 0),
                message=info.get("message") or "",
            )

        def control_cb():
            state = job_controls.get(job_id) or {}
            return {"paused": bool(state.get("paused")), "cancelled": bool(state.get("cancelled"))}

        def stats_cb(info: dict):
            job_store.set_stats(
                job_id=job_id,
                records_streets=info.get("records_streets", 0),
                records_spdp=info.get("records_spdp", 0),
                attachments_streets=info.get("attachments_streets", 0),
                attachments_spdp=info.get("attachments_spdp", 0),
            )

        def uploaded_field_cb(entity_id: str, item_id: str, field_id: str):
            job_store.add_uploaded_field(
                job_id=job_id,
                entity_id=entity_id,
                item_id=item_id,
                field_id=field_id,
            )

        exporter.run(
            date_from,
            date_to,
            progress_callback=progress_cb,
            control_callback=control_cb,
            stats_callback=stats_cb,
            uploaded_field_callback=uploaded_field_cb,
        )
        if (job_controls.get(job_id) or {}).get("cancelled"):
            job_store.mark_cancelled(job_id, message="Остановлено пользователем")
        else:
            job_store.mark_done(job_id, message="Готово")
    except Exception as exc:
        if str(exc) == "Остановлено пользователем":
            job_store.mark_cancelled(job_id, message="Остановлено пользователем")
        else:
            job_store.mark_error(job_id, str(exc))
    finally:
        job_controls.pop(job_id, None)
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
    job_controls[job_id] = {"paused": False, "cancelled": False}

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


@app.route("/api/jobs/<job_id>/pause", methods=["POST"])
def pause_job(job_id: str):
    job = job_store.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Задача не найдена"}), 404
    if job.get("status") not in ("queued", "running"):
        return jsonify({"ok": False, "error": "Пауза доступна только для активной задачи"}), 400
    state = job_controls.setdefault(job_id, {"paused": False, "cancelled": False})
    state["paused"] = True
    job_store.mark_paused(job_id, message="Пауза")
    return jsonify({"ok": True})


@app.route("/api/jobs/<job_id>/resume", methods=["POST"])
def resume_job(job_id: str):
    job = job_store.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Задача не найдена"}), 404
    if job.get("status") not in ("queued", "running"):
        return jsonify({"ok": False, "error": "Возобновление доступно только для активной задачи"}), 400
    state = job_controls.setdefault(job_id, {"paused": False, "cancelled": False})
    state["paused"] = False
    job_store.mark_resumed(job_id, message="Возобновлено")
    return jsonify({"ok": True})


@app.route("/api/jobs/<job_id>/stop", methods=["POST"])
def stop_job(job_id: str):
    job = job_store.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Задача не найдена"}), 404
    if job.get("status") not in ("queued", "running"):
        return jsonify({"ok": False, "error": "Остановка доступна только для активной задачи"}), 400
    state = job_controls.setdefault(job_id, {"paused": False, "cancelled": False})
    state["cancelled"] = True
    state["paused"] = False
    job_store.mark_cancel_requested(job_id, message="Остановка запрошена")
    return jsonify({"ok": True})


def _run_cleanup_job(job_id: str):
    from app.crm_api import CRMClient
    from app.logger import setup_logger

    if not cleanup_lock.acquire(blocking=False):
        job_store.mark_cleanup_error(job_id, "Уже выполняется удаление из CRM")
        return
    try:
        items = job_store.list_uploaded_fields(job_id)
        total = len(items)
        job_store.mark_cleanup_running(job_id, total=total)
        if total == 0:
            job_store.mark_cleanup_done(job_id, deleted=0)
            return
        logger = setup_logger()
        crm = CRMClient(logger)
        crm.login()

        done = 0
        deleted = 0
        for item in items:
            entity_id = item.get("entity_id")
            item_id = item.get("item_id")
            field_id = item.get("field_id")
            try:
                removed = crm.delete_attachments(entity_id=entity_id, item_id=item_id, field_id=field_id)
                if removed:
                    deleted += len(removed)
                done += 1
                job_store.update_cleanup_progress(
                    job_id=job_id,
                    done=done,
                    deleted=deleted,
                    message=f"Удаление из CRM: {done}/{total}",
                )
            except Exception as exc:
                job_store.mark_cleanup_error(job_id, str(exc))
                return

        job_store.mark_cleanup_done(job_id, deleted=deleted)
    finally:
        cleanup_lock.release()


@app.route("/api/jobs/<job_id>/delete-uploaded", methods=["POST"])
def delete_uploaded_from_crm(job_id: str):
    job = job_store.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Задача не найдена"}), 404
    if job.get("status") not in ("done", "cancelled"):
        return jsonify({"ok": False, "error": "Удаление доступно только после завершения или остановки выгрузки"}), 400
    if str(job.get("cleanup_status") or "") == "running":
        return jsonify({"ok": False, "error": "Удаление уже выполняется"}), 409

    t = threading.Thread(
        target=_run_cleanup_job,
        args=(job_id,),
        daemon=True,
        name=f"cleanup-job-{job_id}",
    )
    t.start()
    return jsonify({"ok": True})


@app.route("/api/history")
def history_list():
    items = job_store.list_history(limit=200)
    return jsonify({"ok": True, "items": items})


@app.route("/api/history", methods=["DELETE"])
def history_delete_all():
    job_store.delete_history()
    return jsonify({"ok": True})


@app.route("/api/history/<job_id>", methods=["DELETE"])
def history_delete_one(job_id: str):
    job_store.delete_job(job_id)
    return jsonify({"ok": True})


@app.route("/api/server-counts")
def server_counts():
    from app.crm_api import CRMClient
    from app.logger import setup_logger
    from app.utils import normalize_value

    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()
    err = _validate_dates(date_from, date_to)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    logger = setup_logger()
    crm = CRMClient(logger)
    crm.login()

    entity_map = {
        "79": "streets",
        "110": "spdp",
    }
    counts = {
        "streets": 0,
        "spdp": 0,
    }
    attachments = {
        "streets": 0,
        "spdp": 0,
    }
    debug_logs = []
    diagnostics = {}

    for entity_id, key in entity_map.items():
        fields = CRM_FIELDS.get(entity_id) or {}
        date_field = (fields.get("date") or "").strip()
        photos_field = (fields.get("photos") or "").strip()
        photos_extra_field = (fields.get("photos_extra") or "").strip()
        photo_fields = [x for x in [photos_field, photos_extra_field] if x]
        if not date_field:
            msg = f"Не настроено поле даты для entity_id={entity_id}"
            debug_logs.append(msg)
            diagnostics[key] = {
                "api_total": 0,
                "api_filtered": 0,
                "local_filtered": 0,
                "with_photos": 0,
                "photo_items": 0,
                "used_mode": "no_date_field",
            }
            continue

        filters = {
            date_field: f"{date_from},{date_to}",
        }
        debug_logs.append(
            f"entity={entity_id}: select с filters по полю даты {date_field} ({date_from}..{date_to})"
        )

        select_fields = [date_field]
        for field_id in photo_fields:
            select_fields.append(field_id)
        # Убираем дубли, чтобы CRM не получала повторяющиеся field id
        select_fields = list(dict.fromkeys([str(x).strip() for x in select_fields if str(x).strip()]))

        try:
            all_records = crm.select_records(
                entity_id=entity_id,
                select_fields=select_fields,
                limit=0,
            )
        except Exception as exc:
            counts[key] = 0
            diagnostics[key] = {
                "api_total": 0,
                "api_filtered": 0,
                "local_filtered": 0,
                "with_photos": 0,
                "photo_items": 0,
                "used_mode": "all_records_error",
            }
            debug_logs.append(f"entity={entity_id}: ошибка запроса all_records: {exc}")
            continue

        api_total = len(all_records)
        local_filtered_records = crm.filter_records_by_date(
            records=all_records,
            date_field=date_field,
            date_from=date_from,
            date_to=date_to,
        )
        local_filtered = len(local_filtered_records)

        used_mode = "local"
        try:
            filtered_records = crm.select_records(
                entity_id=entity_id,
                select_fields=select_fields,
                filters=filters,
                limit=0,
            )
            api_filtered = len(filtered_records)
            debug_logs.append(f"entity={entity_id}: CRM вернула {api_filtered} записей (через filters)")
        except Exception as exc:
            used_mode = "local_fallback_after_filters_error"
            debug_logs.append(
                f"entity={entity_id}: filters не сработал ({exc}); fallback на локальный фильтр"
            )
            filtered_records = local_filtered_records
            api_filtered = len(filtered_records)
            debug_logs.append(
                f"entity={entity_id}: всего {api_total}, после локального фильтра {api_filtered}"
            )

        with_photos = 0
        photo_items = 0
        for record in local_filtered_records:
            names_in_record = 0
            for field_id in photo_fields:
                raw_value = normalize_value(record.get(str(field_id)))
                if not raw_value:
                    continue
                parsed = [item.strip() for item in str(raw_value).split(",") if item.strip()]
                names_in_record += len(parsed)
            if names_in_record > 0:
                with_photos += 1
                photo_items += names_in_record

        # Истинный итог по периоду считаем по локальной фильтрации даты,
        # т.к. filters CRM в этом проекте может игнорироваться.
        counts[key] = local_filtered
        attachments[key] = photo_items

        diagnostics[key] = {
            "api_total": api_total,
            "api_filtered": api_filtered,
            "local_filtered": local_filtered,
            "with_photos": with_photos,
            "photo_items": photo_items,
            "used_mode": used_mode,
        }
        debug_logs.append(
            f"entity={entity_id}: api_total={api_total}, api_filtered={api_filtered}, "
            f"local_filtered={local_filtered}, with_photos={with_photos}, photo_items={photo_items}, mode={used_mode}"
        )

    total = counts["streets"] + counts["spdp"]
    total_attachments = attachments["streets"] + attachments["spdp"]
    return jsonify({
        "ok": True,
        "period": {
            "date_from": date_from,
            "date_to": date_to,
        },
        "counts": {
            **counts,
            "total": total,
        },
        "attachments": {
            **attachments,
            "total": total_attachments,
        },
        "diagnostics": diagnostics,
        "debug_logs": debug_logs,
    })


def main():
    if not _norm_cred(CRM_USERNAME) or not _norm_cred(CRM_PASSWORD):
        print(
            "Не заданы CRM_USERNAME или CRM_PASSWORD для входа в веб-интерфейс.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not WEB_SECRET_KEY:
        print(
            "Предупреждение: WEB_SECRET_KEY не задан — сессии сбросятся после рестарта сервера.",
            file=sys.stderr,
        )

    print(f"Файл настроек: {ENV_PATH}", file=sys.stderr)
    print(f"Веб-логин: {CRM_USERNAME}", file=sys.stderr)
    print(f"SESSION_COOKIE_SECURE={WEB_SESSION_COOKIE_SECURE}", file=sys.stderr)
    print(f"TRUSTED_PROXY_HOPS={WEB_TRUSTED_PROXY_HOPS}", file=sys.stderr)

    app.run(host=HOST, port=PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()