import base64
import json
from datetime import datetime
import requests

from app.config import (
    CRM_API_ENDPOINT,
    CRM_API_KEY,
    CRM_USERNAME,
    CRM_PASSWORD,
)
from app.utils import normalize_value


class CRMClient:
    def __init__(self, logger):
        self.logger = logger
        self.session = requests.Session()
        self.cookie_header = ""
        self.is_logged_in = False

    def _base_payload(self):
        return {
            "key": CRM_API_KEY,
            "username": CRM_USERNAME,
            "password": CRM_PASSWORD,
        }

    def login(self):
        if self.is_logged_in:
            return

        payload = {
            "key": CRM_API_KEY,
            "username": CRM_USERNAME,
            "password": CRM_PASSWORD,
            "action": "login",
        }

        self.logger.info("CRM API login...")
        response = self.session.post(CRM_API_ENDPOINT, data=payload, timeout=60)
        self.logger.info(f"CRM login response status: {response.status_code}")

        if response.status_code >= 400:
            raise RuntimeError(f"Не удалось выполнить login в CRM API. HTTP {response.status_code}")

        try:
            result = response.json()
        except json.JSONDecodeError:
            self.logger.error(response.text[:2000])
            raise RuntimeError("CRM API login вернул не JSON")

        if result.get("status") != "success":
            raise RuntimeError(f"CRM API login error: {result}")

        self.cookie_header = self._build_cookie_header_from_response(response)
        if self.cookie_header:
            self.session.headers.update({"Cookie": self.cookie_header})

        self.is_logged_in = True
        self.logger.info("CRM API login successful")

    def select_records(self, entity_id: str, select_fields=None, filters=None, limit=None):
        payload = self._base_payload()
        payload.update({
            "action": "select",
            "entity_id": entity_id,
        })

        if select_fields:
            field_ids = [str(field).strip() for field in select_fields if str(field).strip()]
            payload["select_fields"] = ",".join(field_ids)

        if filters:
            payload["filters"] = filters

        if limit is not None:
            payload["limit"] = limit

        self.logger.info(
            f"CRM request: entity_id={entity_id}, "
            f"select_fields={payload.get('select_fields')}, "
            f"limit={limit}"
        )

        response = self.session.post(CRM_API_ENDPOINT, data=payload, timeout=120)
        self.logger.info(f"CRM response status: {response.status_code}")

        if response.status_code >= 400:
            self.logger.error(response.text[:2000])
            response.raise_for_status()

        try:
            result = response.json()
        except json.JSONDecodeError:
            self.logger.error(response.text[:3000])
            raise RuntimeError("CRM API select вернул не JSON")

        if result.get("status") != "success":
            raise RuntimeError(f"Ошибка CRM API: {result}")

        data = result.get("data", [])

        if isinstance(data, dict):
            return list(data.values())

        if isinstance(data, list):
            return data

        return []

    def download_attachment_by_name(self, entity_id: str, item_id: str, field_id: str, filename: str):
        """
        Аналог логики из мобильного приложения:
        action=download_attachment + filename
        Возвращает (output_filename, file_bytes)
        """
        candidates = [
            {"filename": filename},
            {"name": filename},
            {"file": filename},
        ]

        last_error = None

        for extra in candidates:
            payload = self._base_payload()
            payload.update({
                "action": "download_attachment",
                "entity_id": str(entity_id),
                "item_id": str(item_id),
                "field_id": str(field_id),
                **extra,
            })

            response = self.session.post(CRM_API_ENDPOINT, data=payload, timeout=120)

            if response.status_code >= 400:
                last_error = f"HTTP {response.status_code}"
                continue

            try:
                result = response.json()
            except json.JSONDecodeError:
                last_error = f"non-json response: {response.text[:500]}"
                continue

            if result.get("status") != "success":
                last_error = result
                continue

            data = result.get("data")
            if not isinstance(data, dict):
                last_error = f"unexpected data type: {type(data)}"
                continue

            output_name = (
                str(data.get("filename") or data.get("name") or filename).strip()
                or filename
            )
            content = str(data.get("content") or "").strip()

            if not content:
                last_error = "empty content"
                continue

            try:
                if "," in content and "base64" in content[:50]:
                    content = content.split(",", 1)[1]

                file_bytes = base64.b64decode(content)
                return output_name, file_bytes
            except Exception as e:
                last_error = f"base64 decode error: {e}"
                continue

        raise RuntimeError(
            f'Не удалось скачать вложение "{filename}" '
            f"(entity={entity_id}, item={item_id}, field={field_id}). last_error={last_error}"
        )

    def delete_attachments(self, entity_id: str, item_id: str, field_id: str):
        payload = self._base_payload()
        payload.update({
            "action": "delete_attachment",
            "entity_id": str(entity_id),
            "item_id": str(item_id),
            "field_id": str(field_id),
        })
        response = self.session.post(CRM_API_ENDPOINT, data=payload, timeout=120)
        if response.status_code >= 400:
            self.logger.error(response.text[:2000])
            response.raise_for_status()
        try:
            result = response.json()
        except json.JSONDecodeError:
            self.logger.error(response.text[:2000])
            raise RuntimeError("CRM API delete_attachment вернул не JSON")
        if result.get("status") != "success":
            raise RuntimeError(f"Ошибка delete_attachment: {result}")
        data = result.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, str) and data.strip():
            return [x.strip() for x in data.split(",") if x.strip()]
        return []

    def get_field_value(self, record: dict, field_id: str):
        if not field_id:
            return ""
        return normalize_value(record.get(str(field_id)))

    def filter_records_by_date(self, records, date_field: str, date_from: str, date_to: str):
        filtered = []

        for record in records:
            value = self.get_field_value(record, date_field)
            normalized = self.normalize_crm_date(value)

            if not normalized:
                continue

            if date_from <= normalized <= date_to:
                filtered.append(record)

        return filtered

    @staticmethod
    def normalize_crm_date(value: str):
        value = normalize_value(value)
        if not value:
            return ""

        token = value.split(" ")[0].strip()

        # ISO-like format: YYYY-MM-DD
        if len(token) == 10 and token[4] == "-" and token[7] == "-":
            try:
                dt = datetime.strptime(token, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Rukovoditel usually returns DD/MM/YYYY in RU setups.
        if "/" in token:
            for fmt in ("%d/%m/%Y", "%m/%d/%Y"):
                try:
                    dt = datetime.strptime(token, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue

        return ""

    @staticmethod
    def _build_cookie_header_from_response(response):
        set_cookie = response.headers.get("set-cookie")
        if not set_cookie:
            return ""

        ignored = {
            "expires",
            "max-age",
            "path",
            "domain",
            "secure",
            "httponly",
            "samesite",
            "priority",
        }

        import re
        matches = re.finditer(r'(^|,\s*)([A-Za-z0-9_\-]+)=([^;,\s]+)', set_cookie)
        cookies = {}

        for m in matches:
            name = (m.group(2) or "").strip()
            value = (m.group(3) or "").strip()
            if not name or not value:
                continue
            if name.lower() in ignored:
                continue
            cookies[name] = value

        return "; ".join(f"{k}={v}" for k, v in cookies.items())