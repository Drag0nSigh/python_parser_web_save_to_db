import asyncio
import logging
import re
import urllib.parse
from asyncio import Event, Queue
from datetime import datetime
from typing import List, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup

from src.constante import (
    BASE_URL,
    CONSUMER_TIMEOUT,
    DATE_FORMAT,
    LINK_CLASS,
    LINK_PATTERN,
    NUM_CONSUMERS_LINK,
    PAGE_PARAM,
    REQUEST_DELAY,
)
from src.parcers.file_downloader import FileDownloader
from src.parcers.parser_file import FileParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpimexParser:
    """Класс для парсинга ссылок на файлы с сайта SPIMEX."""

    def __init__(
        self,
        cutoff_date: datetime,
        max_date: Optional[datetime] = None,
        base_url: str = BASE_URL,
        link_class: str = LINK_CLASS,
        link_pattern: str = LINK_PATTERN,
        page_param: str = PAGE_PARAM,
        date_format: str = DATE_FORMAT,
        num_consumers_link: int = NUM_CONSUMERS_LINK,
        consumer_timeout: int = CONSUMER_TIMEOUT,
        request_delay: int = REQUEST_DELAY,
    ):
        """Инициализация парсера SPIMEX.

        Args:
            cutoff_date (datetime): Дата, до которой парсятся данные (резервная дата для ограничения поиска).
            max_date (Optional[datetime]): Максимальная дата для фильтрации ссылок (из базы данных). Если None,
            используется текущая дата.
            base_url (str): Базовый URL сайта SPIMEX. По умолчанию используется значение из констант.
            link_class (str): CSS-класс ссылок на файлы. По умолчанию используется значение из констант.
            link_pattern (str): Регулярное выражение для извлечения дат из ссылок. По умолчанию используется значение
            из констант.
            page_param (str): Шаблон параметра пагинации для URL. По умолчанию используется значение из констант.
            date_format (str): Формат даты для парсинга ссылок. По умолчанию используется значение из констант.
            num_consumers_link (int): Количество асинхронных потребителей для обработки ссылок. По умолчанию
            используется значение из констант.
            consumer_timeout (int): Таймаут (в секундах) для ожидания данных из очереди ссылок. По умолчанию
            используется значение из констант.
            request_delay (float): Задержка (в секундах) между HTTP-запросами для избежания перегрузки сервера.
            По умолчанию используется значение из констант.

        Notes:
            Создаёт очереди для ссылок и данных, а также событие для отслеживания завершения продюсера.
        """
        self.max_date = max_date
        self.cutoff_date = cutoff_date
        self.session: Optional[aiohttp.ClientSession] = None
        self.queue_link: Queue = Queue()
        self.queue_data_for_db: Queue = Queue()
        self.producer_link_done: Event = Event()
        self.producer_db = 0
        self.base_url = base_url
        self.link_class = link_class
        self.link_pattern = link_pattern
        self.page_param = page_param
        self.date_format = date_format
        self.num_consumers_link = num_consumers_link
        self.consumer_timeout = consumer_timeout
        self.request_delay = request_delay
        logger.info(
            f"Инициализация SpimexParser: max_date={self.max_date.strftime(self.date_format) if max_date else None}, "
            f"cutoff_date={self.cutoff_date.strftime(self.date_format)}"
        )

    async def start_session(self) -> None:
        """Инициализация асинхронной HTTP-сессии."""
        logger.info("Инициализация HTTP-сессии")
        self.session = aiohttp.ClientSession()

    async def close_session(self) -> None:
        """Закрытие асинхронной HTTP-сессии."""
        if self.session and not self.session.closed:
            logger.info("Закрытие HTTP-сессии")
            await self.session.close()

    async def fetch_page(self, page_number: int) -> Optional[str]:
        """Получение HTML-кода страницы.

        Args:
            page_number: Номер страницы для загрузки.

        Returns:
            Optional[str]: HTML-код страницы или None при ошибке.

        Raises:
            aiohttp.ClientResponseError: Если запрос не удался.
        """
        url = self.base_url + self.page_param.format(page_number)
        logger.info(f"Запрос страницы {page_number}: {url}")
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientResponseError as e:
            logger.error(f"Ошибка при загрузке страницы {page_number}: {e}")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при загрузке страницы {page_number}: {e}")
            return None

    async def parse_links(self, html: str) -> Tuple[List[Tuple[str, datetime]], bool]:
        """Парсинг ссылок на файлы и извлечение дат из них.

        Args:
            html: HTML-код страницы.

        Returns:
            Tuple[List[Tuple[str, datetime]], bool]: Список кортежей (ссылка, дата) и флаг, указывающий,
            была ли найдена невалидная дата.

        Raises:
            ValueError: Если дата в ссылке не соответствует формату `date_format`.
        """
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a", class_=self.link_class)
        result = []
        found_invalid_date = False

        for link in links:
            href = link.get("href", "")
            match = re.search(self.link_pattern, href)
            if match:
                date_str = match.group(1)
                try:
                    date_file = datetime.strptime(date_str, self.date_format)
                    if not self._is_valid_date(date_file):
                        logger.info(f"Невалидная дата {date_file.strftime(self.date_format)}, завершение перебора")
                        found_invalid_date = True
                        break
                    parsed_url = urllib.parse.urlparse(href)
                    clean_url = urllib.parse.urlunparse(
                        (
                            parsed_url.scheme,
                            parsed_url.netloc,
                            parsed_url.path,
                            "",
                            "",
                            "",
                        )
                    )
                    full_url = urllib.parse.urljoin(self.base_url, clean_url)
                    result.append((full_url, date_file))
                except ValueError:
                    logger.error(f"Некорректная дата в ссылке: {href}")
                    continue

        logger.info(f"Извлечено {len(result)} ссылок со страницы")
        return result, found_invalid_date

    def _is_valid_date(self, date_file: datetime) -> bool:
        """Проверка, соответствует ли дата ограничениям.

        Args:
            date_file: Дата для проверки.

        Returns:
            bool: True, если дата валидна, иначе False.
        """
        current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # Проверяем, что дата не в будущем
        if date_file > current_date:
            return False
        # Если max_date не задано, проверяем, что дата >= cutoff_date
        if self.max_date is None:
            return date_file >= self.cutoff_date
        # Если max_date задано, проверяем, что date_file в диапазоне [max_date, current_date]
        return self.max_date < datetime.date(date_file) <= datetime.date(current_date)

    async def produce_links(self) -> None:
        """Асинхронно извлекает ссылки на XLS-файлы с пагинированных страниц и добавляет их в очередь.

        Notes:
            Перебирает страницы, пока не закончатся ссылки или не будет найдена невалидная дата.
            Задержка между запросами задаётся параметром `request_delay`.

        Raises:
            Exception: При ошибке во время выполнения запросов или обработки страниц.
        """
        page_number = 1
        should_continue = True

        try:
            while should_continue:

                html = await self.fetch_page(page_number)
                if html is None:
                    logger.info(f"Прекращение обработки: не удалось загрузить страницу {page_number}")
                    break
                links, found_invalid_date = await self.parse_links(html)
                if not links and not found_invalid_date:
                    logger.info(f"Нет ссылок на странице {page_number}, завершение")
                    break

                for link, date_file in links:
                    logger.info(f"Положил ссылку {link}")
                    await self.queue_link.put((link, date_file))

                if found_invalid_date:
                    logger.info(f"Обнаружена невалидная дата на странице {page_number}, завершение")
                    break

                page_number += 1
                await asyncio.sleep(self.request_delay)

        except Exception as e:
            logger.error(f"Ошибка в продюсере: {e}")
        finally:
            logger.info("Продюсер завершил работу")
            self.producer_link_done.set()

    async def consume_links(self, downloader: FileDownloader, parser: FileParser) -> None:
        """Обрабатывает ссылки из очереди: скачивает файлы и парсит их, помещая данные в очередь для базы данных.

        Args:
            downloader (FileDownloader): Экземпляр загрузчика файлов.
            parser (FileParser): Экземпляр парсера файлов.

        Notes:
            Работает до тех пор, пока продюсер не завершит работу и очередь ссылок не станет пустой.
            Использует таймаут `consumer_timeout` для ожидания данных из очереди.

        Raises:
            asyncio.TimeoutError: Если истёк таймаут ожидания ссылки из очереди.
            Exception: При ошибке во время скачивания или парсинга файла.
        """
        self.producer_db += 1
        while not (self.producer_link_done.is_set() and self.queue_link.empty()):
            try:
                link, date_file = await asyncio.wait_for(self.queue_link.get(), timeout=self.consumer_timeout)
                logger.info(f"Достали из очереди {link}")
                file_content = await downloader.download_file(link)
                parsed_data = await parser.parse_file(file_content, date_file)
                self.queue_link.task_done()
                await self.queue_data_for_db.put(parsed_data)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Ошибка в потребителе для ссылки {link}: {e}")
                self.queue_link.task_done()
        self.producer_db -= 1

    async def run(self) -> None:
        """Запуск парсера: запуск продюсера и нескольких потребителей.

        Returns:
            None
        """
        await self.start_session()
        downloader = FileDownloader(self.session)
        parser = FileParser()
        try:
            producer = asyncio.create_task(self.produce_links())
            consumers = [
                asyncio.create_task(self.consume_links(downloader, parser)) for _ in range(self.num_consumers_link)
            ]
            await asyncio.gather(producer, *consumers)
        except Exception as e:
            logger.error(f"Ошибка в методе run: {e}")
        finally:
            await self.close_session()
