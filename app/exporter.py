import io
import zipfile

from app.config import (
    CRM_ENTITY_IDS,
    CRM_FIELDS,
    YANDEX_DISK_BASE_PATH,
)
from app.crm_api import CRMClient
from app.utils import (
    build_disk_path,
    format_date_folder,
    normalize_value,
    safe_name,
)
from app.yandex_disk import YandexDiskClient


class Exporter:
    ENTITY_GROUP_NAMES = {
        "79": "Улицы",
        "110": "СП-ДП",
    }

    def __init__(self, logger):
        self.logger = logger
        self.crm = CRMClient(logger)
        self.disk = YandexDiskClient(logger)

        self.crm.login()

        self._progress_callback = None
        self._control_callback = None
        self._stats_callback = None
        self._uploaded_field_callback = None
        self._done = 0
        self._total = 0
        self._seen_disk_paths = set()
        self._records_stats = {"79": 0, "110": 0}
        self._attachments_stats = {"79": 0, "110": 0}

    def run(
        self,
        date_from: str,
        date_to: str,
        progress_callback=None,
        control_callback=None,
        stats_callback=None,
        uploaded_field_callback=None,
    ):
        self._progress_callback = progress_callback
        self._control_callback = control_callback
        self._stats_callback = stats_callback
        self._done = 0
        self._total = self.count_expected_uploads(date_from=date_from, date_to=date_to)
        self._records_stats = {"79": 0, "110": 0}
        self._attachments_stats = {"79": 0, "110": 0}
        self._seen_disk_paths = set()
        self._uploaded_field_callback = uploaded_field_callback

        self.logger.info("Старт экспорта")
        self.logger.info(f"Период: {date_from} - {date_to}")

        self._emit_progress("Старт экспорта")

        for entity_id in CRM_ENTITY_IDS:
            self.process_entity(entity_id=entity_id, date_from=date_from, date_to=date_to)

        self.logger.info("Экспорт завершён")
        self._emit_progress("Экспорт завершён")

    @staticmethod
    def _photo_field_ids(fields: dict) -> list[str]:
        ids = []
        if fields.get("photos"):
            ids.append(fields.get("photos"))
        if fields.get("photos_extra"):
            ids.append(fields.get("photos_extra"))
        return [str(x).strip() for x in ids if x]

    def count_expected_uploads(self, date_from: str, date_to: str) -> int:
        """
        Оценка числа файлов, которые будут загружены на Диск (по списку имён в CRM).
        Если в ZIP окажется больше файлов, чем в списке, total увеличится при разборе архива.
        """
        total = 0

        for entity_id in CRM_ENTITY_IDS:
            self._check_control()
            fields = CRM_FIELDS.get(entity_id)
            if not fields:
                continue

            select_fields = [
                fields.get("date"),
                fields.get("user"),
                fields.get("entity_name"),
                fields.get("photos"),
            ]
            if fields.get("photos_extra"):
                select_fields.append(fields.get("photos_extra"))

            records = self.crm.select_records(
                entity_id=entity_id,
                select_fields=select_fields,
            )
            filtered_records = self.crm.filter_records_by_date(
                records=records,
                date_field=fields.get("date"),
                date_from=date_from,
                date_to=date_to,
            )

            for record in filtered_records:
                self._check_control()
                for photo_field in self._photo_field_ids(fields):
                    raw_value = normalize_value(record.get(str(photo_field)))
                    file_names = self.parse_photo_names(raw_value)
                    total += len(file_names)

        return total

    def _emit_progress(self, message: str | None = None):
        if not self._progress_callback:
            return

        self._progress_callback({
            "done": self._done,
            "total": self._total,
            "message": message or "",
        })

    def _emit_stats(self):
        if not self._stats_callback:
            return
        self._stats_callback({
            "records_streets": self._records_stats.get("79", 0),
            "records_spdp": self._records_stats.get("110", 0),
            "attachments_streets": self._attachments_stats.get("79", 0),
            "attachments_spdp": self._attachments_stats.get("110", 0),
        })

    def _check_control(self):
        if not self._control_callback:
            return
        while True:
            state = self._control_callback() or {}
            if state.get("cancelled"):
                raise RuntimeError("Остановлено пользователем")
            if state.get("paused"):
                self._emit_progress("Пауза")
                import time
                time.sleep(0.4)
                continue
            return

    def _adjust_total(self, delta: int):
        if not delta:
            return
        self._total += delta
        self._emit_progress()

    def _increment_done(self, n: int = 1, message: str | None = None):
        self._done += n
        self._emit_progress(message)

    def process_entity(self, entity_id: str, date_from: str, date_to: str):
        self._check_control()
        fields = CRM_FIELDS.get(entity_id)

        if not fields:
            self.logger.warning(f"Нет настроек для entity_id={entity_id}")
            return

        self.logger.info(f"Обработка entity_id={entity_id}")

        select_fields = [
            fields.get("date"),
            fields.get("user"),
            fields.get("entity_name"),
            fields.get("photos"),
        ]

        if fields.get("photos_extra"):
            select_fields.append(fields.get("photos_extra"))

        records = self.crm.select_records(
            entity_id=entity_id,
            select_fields=select_fields,
        )
        self.logger.info(f"Получено записей: {len(records)}")

        filtered_records = self.crm.filter_records_by_date(
            records=records,
            date_field=fields.get("date"),
            date_from=date_from,
            date_to=date_to,
        )
        self.logger.info(f"Записей после фильтра по дате: {len(filtered_records)}")
        if entity_id in self._records_stats:
            self._records_stats[entity_id] = len(filtered_records)
            self._emit_stats()

        for record in filtered_records:
            self._check_control()
            self.process_record(entity_id=entity_id, record=record, fields=fields)

    def process_record(self, entity_id: str, record: dict, fields: dict):
        self._check_control()
        item_id = normalize_value(record.get("id"))
        if not item_id:
            self.logger.warning(f"У записи нет id: entity={entity_id}")
            return

        date_value = self.crm.get_field_value(record, fields.get("date"))
        user_value = self.crm.get_field_value(record, fields.get("user"))
        entity_name_value = self.crm.get_field_value(record, fields.get("entity_name"))

        normalized_date = self.crm.normalize_crm_date(date_value)
        date_folder = format_date_folder(normalized_date)

        user_name = normalize_value(user_value) or "Неизвестный пользователь"
        entity_name = normalize_value(entity_name_value) or f"entity_{entity_id}"
        entity_group = self.ENTITY_GROUP_NAMES.get(str(entity_id), f"entity_{entity_id}")

        folder_path = build_disk_path(
            base_path=YANDEX_DISK_BASE_PATH,
            user_name=user_name,
            entity_group=entity_group,
            date_folder=date_folder,
            entity_name=entity_name,
        )

        photo_fields = [fields.get("photos")]
        if fields.get("photos_extra"):
            photo_fields.append(fields.get("photos_extra"))

        for photo_field in photo_fields:
            self._check_control()
            if not photo_field:
                continue

            raw_value = normalize_value(record.get(str(photo_field)))
            file_names = self.parse_photo_names(raw_value)

            if not file_names:
                continue

            try:
                self.download_and_upload_field_attachments(
                    entity_id=entity_id,
                    item_id=item_id,
                    field_id=str(photo_field),
                    folder_path=folder_path,
                    original_file_names=file_names,
                )
                if self._uploaded_field_callback:
                    self._uploaded_field_callback(
                        entity_id=str(entity_id),
                        item_id=str(item_id),
                        field_id=str(photo_field),
                    )
            except Exception as e:
                self.logger.exception(
                    f"Ошибка обработки вложений поля "
                    f"(entity={entity_id}, item={item_id}, field={photo_field}): {e}"
                )

    @staticmethod
    def parse_photo_names(raw_value: str):
        if not raw_value:
            return []

        return [
            item.strip()
            for item in str(raw_value).split(",")
            if item.strip()
        ]

    def download_and_upload_field_attachments(
        self,
        entity_id: str,
        item_id: str,
        field_id: str,
        folder_path: str,
        original_file_names: list[str],
    ):
        """
        CRM может вернуть zip-архив со всеми файлами поля.
        Поэтому скачиваем вложения поля один раз.
        """

        probe_filename = original_file_names[0]
        self._check_control()
        if entity_id in self._attachments_stats:
            self._attachments_stats[entity_id] += len(original_file_names)
            self._emit_stats()

        self.logger.info(
            f"Скачивание вложений поля из CRM: "
            f"entity={entity_id}, item={item_id}, field={field_id}, probe_file={probe_filename}"
        )

        output_name, file_bytes = self.crm.download_attachment_by_name(
            entity_id=entity_id,
            item_id=item_id,
            field_id=field_id,
            filename=probe_filename,
        )

        output_name = safe_name(output_name or f"attachments-{item_id}.zip")

        if self.is_zip_file(output_name, file_bytes):
            self.logger.info(
                f"CRM вернула ZIP-архив: entity={entity_id}, item={item_id}, field={field_id}, file={output_name}"
            )
            self.upload_zip_contents(
                zip_bytes=file_bytes,
                folder_path=folder_path,
                entity_id=entity_id,
                item_id=item_id,
                field_id=field_id,
                listed_names=original_file_names,
            )
        else:
            self.logger.info(
                f"CRM вернула одиночный файл: entity={entity_id}, item={item_id}, field={field_id}, file={output_name}"
            )
            self.upload_single_file(
                file_name=output_name,
                file_bytes=file_bytes,
                folder_path=folder_path,
            )

    @staticmethod
    def is_zip_file(file_name: str, file_bytes: bytes) -> bool:
        if file_name.lower().endswith(".zip"):
            return True
        return file_bytes[:4] == b"PK\x03\x04"

    def upload_zip_contents(
        self,
        zip_bytes: bytes,
        folder_path: str,
        entity_id: str,
        item_id: str,
        field_id: str,
        listed_names: list[str],
    ):
        self._check_control()
        self.disk.ensure_folder_tree(folder_path)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            members = [m for m in zf.infolist() if not m.is_dir()]

            self.logger.info(
                f"В архиве файлов: {len(members)} "
                f"(entity={entity_id}, item={item_id}, field={field_id})"
            )

            if len(members) != len(listed_names):
                self._adjust_total(len(members) - len(listed_names))

            for member in members:
                self._check_control()
                inner_name = member.filename.split("/")[-1].split("\\")[-1].strip()
                if not inner_name:
                    self._increment_done(1, message="Пустое имя в архиве, пропуск")
                    continue

                safe_inner_name = safe_name(inner_name)
                disk_path = f"{folder_path}/{safe_inner_name}"
                disk_key = disk_path.lower()

                if disk_key in self._seen_disk_paths or self.disk.file_exists(disk_path):
                    self.logger.info(f"Файл уже существует, пропускаю: {disk_path}")
                    self._seen_disk_paths.add(disk_key)
                    self._increment_done(1, message=f"Уже есть: {safe_inner_name}")
                    continue

                with zf.open(member) as f:
                    file_bytes = f.read()

                if not file_bytes:
                    self.logger.warning(f"Пустой файл в архиве, пропускаю: {inner_name}")
                    self._increment_done(1, message=f"Пустой файл: {inner_name}")
                    continue

                self.disk.upload_bytes(
                    file_bytes=file_bytes,
                    disk_path=disk_path,
                    overwrite=False,
                )

                self.logger.info(
                    f"Файл загружен из ZIP: entity={entity_id} | item={item_id} | "
                    f"field={field_id} | file={safe_inner_name} | disk_path={disk_path}"
                )
                self._seen_disk_paths.add(disk_key)
                self._increment_done(1, message=f"Загружен: {safe_inner_name}")

    def upload_single_file(self, file_name: str, file_bytes: bytes, folder_path: str):
        self._check_control()
        self.disk.ensure_folder_tree(folder_path)

        safe_file_name = safe_name(file_name)
        disk_path = f"{folder_path}/{safe_file_name}"
        disk_key = disk_path.lower()

        if disk_key in self._seen_disk_paths or self.disk.file_exists(disk_path):
            self.logger.info(f"Файл уже существует, пропускаю: {disk_path}")
            self._seen_disk_paths.add(disk_key)
            self._increment_done(1, message=f"Уже есть: {safe_file_name}")
            return

        self.disk.upload_bytes(
            file_bytes=file_bytes,
            disk_path=disk_path,
            overwrite=False,
        )

        self.logger.info(f"Файл загружен: {disk_path}")
        self._seen_disk_paths.add(disk_key)
        self._increment_done(1, message=f"Загружен: {safe_file_name}")