import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError, AttributeError):
        return default


CRM_BASE_URL = os.getenv("CRM_BASE_URL", "").rstrip("/")
CRM_API_ENDPOINT = os.getenv("CRM_API_ENDPOINT", "").strip()
if not CRM_API_ENDPOINT:
    CRM_API_ENDPOINT = f"{CRM_BASE_URL}/api/rest.php"

CRM_API_KEY = os.getenv("CRM_API_KEY")
CRM_USERNAME = os.getenv("CRM_USERNAME")
CRM_PASSWORD = os.getenv("CRM_PASSWORD")

CRM_ENTITY_IDS = [
    item.strip()
    for item in os.getenv("CRM_ENTITY_IDS", "").split(",")
    if item.strip()
]

CRM_FIELDS = {
    "79": {
        "photos": os.getenv("CRM_79_FIELD_PHOTOS"),
        "photos_extra": os.getenv("CRM_79_FIELD_PHOTOS_EXTRA"),
        "date": os.getenv("CRM_79_FIELD_DATE"),
        "user": os.getenv("CRM_79_FIELD_USER"),
        "entity_name": os.getenv("CRM_79_FIELD_ENTITY_NAME"),
    },
    "110": {
        "photos": os.getenv("CRM_110_FIELD_PHOTOS"),
        "date": os.getenv("CRM_110_FIELD_DATE"),
        "user": os.getenv("CRM_110_FIELD_USER"),
        "entity_name": os.getenv("CRM_110_FIELD_ENTITY_NAME"),
    },
}

CRM_FILE_BASE_URL = os.getenv("CRM_FILE_BASE_URL", CRM_BASE_URL).rstrip("/")
CRM_FILES_PATH_TEMPLATE = os.getenv(
    "CRM_FILES_PATH_TEMPLATE",
    "/uploads/attachments/{filename}",
)

YANDEX_DISK_TOKEN = os.getenv("YANDEX_DISK_TOKEN")
YANDEX_DISK_BASE_PATH = os.getenv("YANDEX_DISK_BASE_PATH", "/CRM Photos")

WEB_ADMIN_USERNAME = os.getenv("WEB_ADMIN_USERNAME", "").strip()
WEB_ADMIN_PASSWORD_HASH = os.getenv("WEB_ADMIN_PASSWORD_HASH", "").strip()
WEB_SECRET_KEY = os.getenv("WEB_SECRET_KEY", "").strip()
WEB_SESSION_COOKIE_SECURE = _get_bool("WEB_SESSION_COOKIE_SECURE", True)
WEB_TRUSTED_PROXY_HOPS = _get_int("WEB_TRUSTED_PROXY_HOPS", 1)

HOST = os.getenv("HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = _get_int("PORT", 8787)

LOG_DIR = BASE_DIR / "logs"
LOG_FILE_PATH = LOG_DIR / "export.log"
LOG_MAX_BYTES = _get_int("LOG_MAX_BYTES", 5 * 1024 * 1024)
LOG_BACKUP_COUNT = _get_int("LOG_BACKUP_COUNT", 5)

DATA_DIR = BASE_DIR / "data"
JOBS_DB_PATH = Path(os.getenv("JOBS_DB_PATH", str(DATA_DIR / "jobs.sqlite3")))