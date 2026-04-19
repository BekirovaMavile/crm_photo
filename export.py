import argparse

from app.exporter import Exporter
from app.logger import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Экспорт фото из CRM на Яндекс Диск")
    parser.add_argument(
        "--from",
        dest="date_from",
        required=True,
        help="Дата начала в формате YYYY-MM-DD",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        required=True,
        help="Дата конца в формате YYYY-MM-DD",
    )

    args = parser.parse_args()

    logger = setup_logger()
    logger.info(f"Запуск экспорта с {args.date_from} по {args.date_to}")

    exporter = Exporter(logger)
    exporter.run(date_from=args.date_from, date_to=args.date_to)

    logger.info("Готово")


if __name__ == "__main__":
    main()