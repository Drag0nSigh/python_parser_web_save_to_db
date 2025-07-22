import asyncio
import logging

from src.constante import DEFAULT_CUTOFF_DATE
from src.db.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from src.db.db import Database
from src.parcers.parser_link import SpimexParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    db = Database(
        host=DB_HOST,
        db_name=DB_NAME,
        db_password=DB_PASS,
        user=DB_USER,
        db_port=DB_PORT,
    )
    try:
        await db.init_db()
    except Exception as e:
        logger.error(f"Ошибка: инициализации БД {e}")

    max_date = await db.get_max_data_bulletin()
    logger.info(f"Максимальная дата из базы {max_date}")

    cutoff_date = DEFAULT_CUTOFF_DATE
    parser = SpimexParser(max_date=max_date, cutoff_date=cutoff_date)
    await parser.run()
    await db.put_data_into_bd(parser.queue_data_for_db, parser)
    await db.close_db()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Ошибка в main: {e}")
    finally:
        logger.info("Программа завершена")
