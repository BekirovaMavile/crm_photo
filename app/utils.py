import json
from datetime import datetime
from urllib.parse import urljoin


def safe_name(value: str) -> str:
    if not value:
        return "Без названия"

    value = str(value)

    forbidden = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for ch in forbidden:
        value = value.replace(ch, "_")

    value = " ".join(value.split())
    return value.strip() or "Без названия"


def format_date_folder(date_str: str) -> str:
    if not date_str:
        return "Без даты"

    date_str = str(date_str).strip()

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return date_str[:10]


def build_disk_path(base_path: str, user_name: str, date_folder: str, entity_name: str) -> str:
    base_path = (base_path or "").rstrip("/")
    user_name = safe_name(user_name)
    date_folder = safe_name(date_folder)
    entity_name = safe_name(entity_name)

    return f"{base_path}/{user_name}/{date_folder}/{entity_name}"


def normalize_value(value):
    if value is None:
        return ""

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, dict):
        # если вдруг поле пришло объектом, попробуем взять понятное значение
        for key in ("name", "title", "value"):
            if key in value and value[key]:
                return str(value[key]).strip()

    return str(value).strip()


def parse_attachment_field(raw_value):
    """
    Приводит поле вложений к списку.
    Поддерживает:
    - list
    - dict
    - JSON string
    - обычную строку
    """
    if not raw_value:
        return []

    if isinstance(raw_value, list):
        return raw_value

    if isinstance(raw_value, dict):
        return [raw_value]

    raw_value = str(raw_value).strip()

    if not raw_value:
        return []

    try:
        parsed = json.loads(raw_value)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except Exception:
        pass

    return [raw_value]


def build_absolute_url(base_url: str, maybe_relative_url: str) -> str:
    if not maybe_relative_url:
        return ""
    return urljoin(base_url.rstrip("/") + "/", str(maybe_relative_url).lstrip("/"))