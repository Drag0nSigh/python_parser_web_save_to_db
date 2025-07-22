import logging
from typing import Optional

import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileDownloader:
    """Класс для асинхронного скачивания файлов."""

    def __init__(self, session: aiohttp.ClientSession):
        """Инициализация загрузчика файлов.

        Args:
            session: Асинхронная HTTP-сессия.
        """
        self.session = session

    async def download_file(self, link: str) -> Optional[bytes]:
        """Скачивание файла по ссылке.

        Args:
            link: URL файла для скачивания.

        Returns:
            Optional[bytes]: Содержимое файла или None, если скачивание не удалось.

        Raises:
            aiohttp.ClientResponseError: Если запрос не удался (кроме 404).
        """
        logger.info(f"Попытка скачать файл по ссылке {link}")
        try:
            async with self.session.get(link) as response:
                response.raise_for_status()
                logger.info(f"Успешно скачан файл: {link}")
                return await response.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.info(f"Файл не найден (404): {link}")
                return None
            logger.error(f"Ошибка при скачивании {link}: {e}")
            raise
        except Exception as e:
            logger.error(f"Неизвестная ошибка при скачивании {link}: {e}")
            return None
