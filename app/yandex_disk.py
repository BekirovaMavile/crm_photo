import os
import requests

from app.config import YANDEX_DISK_TOKEN


class YandexDiskClient:
    API_BASE = "https://cloud-api.yandex.net/v1/disk/resources"

    def __init__(self, logger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"OAuth {YANDEX_DISK_TOKEN}"
        })

    def create_folder(self, path: str):
        response = self.session.put(
            self.API_BASE,
            params={"path": path},
            timeout=60,
        )

        if response.status_code in (201, 409):
            return True

        response.raise_for_status()
        return True

    def ensure_folder_tree(self, full_path: str):
        parts = [p for p in full_path.strip("/").split("/") if p]
        current = ""

        for part in parts:
            current += f"/{part}"
            self.create_folder(current)

    def file_exists(self, disk_path: str) -> bool:
        response = self.session.get(
            self.API_BASE,
            params={"path": disk_path},
            timeout=60,
        )

        if response.status_code == 200:
            return True

        if response.status_code == 404:
            return False

        response.raise_for_status()
        return False

    def get_upload_url(self, disk_path: str, overwrite: bool = False):
        response = self.session.get(
            f"{self.API_BASE}/upload",
            params={
                "path": disk_path,
                "overwrite": str(overwrite).lower(),
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        return data["href"]

    def upload_file(self, local_file_path: str, disk_path: str, overwrite: bool = False):
        upload_url = self.get_upload_url(disk_path=disk_path, overwrite=overwrite)

        with open(local_file_path, "rb") as f:
            response = requests.put(upload_url, files={"file": f}, timeout=300)

        if response.status_code not in (201, 202):
            response.raise_for_status()

        self.logger.info(f"Загружен файл на Яндекс Диск: {disk_path}")

    def upload_bytes(self, file_bytes: bytes, disk_path: str, overwrite: bool = False):
        upload_url = self.get_upload_url(disk_path=disk_path, overwrite=overwrite)

        response = requests.put(upload_url, data=file_bytes, timeout=300)

        if response.status_code not in (201, 202):
            response.raise_for_status()

        self.logger.info(f"Загружен файл на Яндекс Диск: {disk_path}")

    def upload_if_not_exists(self, local_file_path: str, disk_path: str):
        folder_path = os.path.dirname(disk_path).replace("\\", "/")
        self.ensure_folder_tree(folder_path)

        if self.file_exists(disk_path):
            self.logger.info(f"Файл уже существует, пропускаю: {disk_path}")
            return

        self.upload_file(local_file_path, disk_path, overwrite=False)