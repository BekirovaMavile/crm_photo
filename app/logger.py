import logging
from logging.handlers import RotatingFileHandler

from app.config import LOG_BACKUP_COUNT, LOG_DIR, LOG_FILE_PATH, LOG_MAX_BYTES


def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("crm_export")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger