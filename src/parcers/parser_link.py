import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import re
from typing import List, Optional, Tuple, Dict, Any
from asyncio import Queue, Event
import urllib.parse
import logging
import io
import xlrd
import mimetypes

from src.db.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from src.db.postgresql import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы
BASE_URL = 'https://spimex.com/markets/oil_products/trades/results/'
LINK_CLASS = 'accordeon-inner__item-title link xls'
LINK_PATTERN = r'/upload/reports/oil_xls/oil_xls_(\d{8})'
PAGE_PARAM = '?page=page-{}'
DATE_FORMAT = '%Y%m%d'
DEFAULT_CUTOFF_DATE = datetime(2025, 7, 1)
NUM_CONSUMERS = 3
CONSUMER_TIMEOUT = 5
REQUEST_DELAY = 0.3
MAX_DATE = datetime(2025, 7, 21)  # Если None, то скачивание от текущей даты до DEFAULT_CUTOFF_DATE
METRIC_TON_UNIT = 'Единица измерения: Метрическая тонна'
ITOGO = 'Итого:'
COLUMN_NAMES = {
    'Код\nИнструмента': {'type': 'str'},
    'Наименование\nИнструмента': {'type': 'str'},
    'Базис\nпоставки': {'type': 'str'},
    'Объем\nДоговоров\nв единицах\nизмерения': {'type': 'int'},
    'Обьем\nДоговоров,\nруб.': {'type': 'int'},
    'Количество\nДоговоров,\nшт.': {'type': 'int'}
}
FIELD_NAMES = {
    'exchange_product_id': 'Код\nИнструмента',
    'exchange_product_name': 'Наименование\nИнструмента',
    'delivery_basis_name': 'Базис\nпоставки',
    'volume': 'Объем\nДоговоров\nв единицах\nизмерения',
    'total': 'Обьем\nДоговоров,\nруб.',
    'count': 'Количество\nДоговоров,\nшт.'
}

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
        logger.info(f'Попытка скачать файл по ссылке {link}')
        try:
            async with self.session.get(link) as response:
                response.raise_for_status()
                logger.info(f'Успешно скачан файл: {link}')
                return await response.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.info(f'Файл не найден (404): {link}')
                return None
            logger.error(f'Ошибка при скачивании {link}: {e}')
            raise
        except Exception as e:
            logger.error(f'Неизвестная ошибка при скачивании {link}: {e}')
            return None

class FileParser:
    """Класс для парсинга файлов."""

    def process_headers(self, row: List[Any]) -> Dict[str, int]:
        """Парсинг строки заголовков и определение индексов столбцов.

        Args:
            row: Строка данных из XLS-файла.

        Returns:
            Dict[str, int]: Словарь с названиями столбцов и их индексами, или пустой словарь, если заголовки не найдены.
        """
        column_indices = {}
        for col_idx, value in enumerate(row):
            if isinstance(value, str) and value.strip() in COLUMN_NAMES:
                column_indices[value.strip()] = col_idx
        logger.info(f'заголовки: {column_indices}')
        if all(col in column_indices for col in COLUMN_NAMES):
            return column_indices
        logger.error(f'Не найдены все необходимые заголовки в строке: {row}')
        return {}

    async def parse_file(self, file_content: Optional[bytes], date: datetime) -> List[Dict[str, Any]]:
        """Парсинг содержимого XLS-файла и извлечение данных из таблицы с единицей измерения 'Метрическая тонна'.

        Args:
            file_content: Содержимое файла в байтах или None, если файл не скачан.
            date: Дата, связанная с файлом.

        Returns:
            List[Dict[str, Any]]: Список словарей с данными для сохранения в базу данных.
        """
        if not self.not_file_content(file_content, date):
            return []

        try:
            if not self.checking_html_file(file_content, date):
                return []

            # Чтение XLS-файла с помощью xlrd
            data = self.xls_to_list_data(file_content, date)
            # Инициализация результата
            result = []
            in_metric_ton_section = False
            skip_next_row = False
            column_indices = {}

            # Проход по строкам для поиска нужной секции
            for index, row in enumerate(data):
                # Проверяем начало секции 'Метрическая тонна'
                if len(row) > 1 and isinstance(row[1], str) and METRIC_TON_UNIT in row[1]:
                    in_metric_ton_section = True
                    continue

                # Пропускаем строку подзаголовков
                if skip_next_row:
                    skip_next_row = False
                    continue

                # Проверяем заголовки
                if not column_indices and in_metric_ton_section:
                    column_indices = self.process_headers(row)
                    if column_indices:
                        skip_next_row = True  # Пропускаем следующую строку (подзаголовки)
                    continue

                # Проверяем конец секции
                if len(row) > 1 and isinstance(row[1], str) and (METRIC_TON_UNIT in row[1] or ITOGO in row[1]):
                    in_metric_ton_section = False
                    continue

                # Обрабатываем строки в нужной секции
                if in_metric_ton_section and row[1] and isinstance(row[1], str) and len(row[1]) > 3:
                    try:
                        # Проверяем наличие всех необходимых столбцов
                        if not all(col in column_indices for col in COLUMN_NAMES):
                            logger.error(f'Недостаточно столбцов для обработки строки {index}: {row}')
                            continue

                        result.extend(self.valid_row_in_dict_for_db(row, column_indices))
                    except (ValueError, IndexError) as e:
                        logger.error(f'Ошибка при обработке строки {index}: {e}')
                        continue

            logger.info(f'Извлечено {len(result)} записей для даты {date.strftime(DATE_FORMAT)}')
            return result

        except Exception as e:
            logger.error(f'Общая ошибка при парсинге файла для даты {date.strftime(DATE_FORMAT)}: {e}')
            return []

    @staticmethod
    def not_file_content(file_content: Optional[bytes], date_file: datetime) -> bool:
        """Проверка наличия содержимого файла.

        Args:
            file_content: Содержимое файла в байтах или None, если файл не скачан.
            date_file: Дата, связанная с файлом.

        Returns:
            bool: True, если файл содержит данные, False, если файл отсутствует.
        """
        if file_content is None:
            logger.info(f'Пропуск парсинга, файл не скачан для даты: {date_file.strftime(DATE_FORMAT)}')
            return False
        return True

    @staticmethod
    def checking_html_file(file_content: Optional[bytes], date_file: datetime) -> bool:
        """Проверка, является ли файл HTML.

        Args:
            file_content: Содержимое файла в байтах.
            date_file: Дата, связанная с файлом.

        Returns:
            bool: True, если файл не является HTML, False, если файл является HTML.
        """
        is_html = file_content.startswith(b'<!DOCTYPE') or file_content.startswith(b'<html')
        if is_html:
            logger.error(f'Файл для даты {date_file.strftime(DATE_FORMAT)} является HTML, а не XLS')
            return False
        return True

    @staticmethod
    def xls_to_list_data(file_content: Optional[bytes], date_file: datetime) -> List[List[str]]:
        """Чтение XLS-файла и преобразование его в список строк.

        Args:
            file_content: Содержимое XLS-файла в байтах.
            date_file: Дата, связанная с файлом.

        Returns:
            List[List[str]]: Список строк данных из XLS-файла или пустой список при ошибке.

        Raises:
            xlrd.XLRDError: Если файл не является корректным XLS-файлом.
        """
        try:
            workbook = xlrd.open_workbook(file_contents=file_content)
            sheet = workbook.sheet_by_index(0)
            data = []
            for row_idx in range(sheet.nrows):
                row = sheet.row_values(row_idx)
                data.append(row)
            logger.info(f'Файл для даты {date_file.strftime(DATE_FORMAT)} успешно прочитан как XLS')
            return data
        except xlrd.XLRDError as xlrd_err:
            logger.error(f'Ошибка при чтении XLS для даты {date_file.strftime(DATE_FORMAT)}: {xlrd_err}')
            return []

    @staticmethod
    def valid_row_in_dict_for_db(row: List[str], column_indices: Dict[str, int]) -> List[Dict[str, Any]]:
        """Преобразование строки данных в список словарей для базы данных.

        Args:
            row: Строка данных из XLS-файла.
            column_indices: Словарь с названиями столбцов и их индексами.

        Returns:
            List[Dict[str, Any]]: Список словарей с данными, если строка валидна и содержит договоры, иначе пустой список.
        """
        result = []
        count_col = FIELD_NAMES['count']
        try:
            count = int(float(row[column_indices[count_col]])) if len(row) > column_indices[count_col] and row[column_indices[count_col]] else 0
        except (ValueError, TypeError):
            count = 0
        if count > 0:
            record = {}
            for field_key, col_name in FIELD_NAMES.items():
                col_idx = column_indices[col_name]
                col_type = COLUMN_NAMES[col_name]['type']
                if col_type == 'int':
                    try:
                        value = int(float(row[col_idx])) if len(row) > col_idx and row[col_idx] else 0
                    except (ValueError, TypeError):
                        value = 0
                else:
                    value = str(row[col_idx]).strip() if len(row) > col_idx and row[col_idx] else ''
                record[field_key] = value
            result.append(record)
        return result

class SpimexParser:
    """Класс для парсинга ссылок на файлы с сайта SPIMEX."""

    def __init__(self, max_date: Optional[datetime] = None, cutoff_date: datetime = DEFAULT_CUTOFF_DATE):
        """Инициализация парсера SPIMEX.

        Args:
            max_date: Максимальная дата для фильтрации ссылок (из БД). Если None, используется текущая дата.
            cutoff_date: Резервная дата для ограничения поиска.
        """
        self.max_date = max_date
        self.cutoff_date = cutoff_date
        self.session: Optional[aiohttp.ClientSession] = None
        self.queue: Queue = Queue()
        self.producer_done: Event = Event()
        logger.info(f'Инициализация SpimexParser: max_date={self.max_date.strftime(DATE_FORMAT) if max_date else None}, cutoff_date={self.cutoff_date.strftime(DATE_FORMAT)}')

    async def start_session(self) -> None:
        """Инициализация асинхронной HTTP-сессии."""
        logger.info('Инициализация HTTP-сессии')
        self.session = aiohttp.ClientSession()

    async def close_session(self) -> None:
        """Закрытие асинхронной HTTP-сессии."""
        if self.session and not self.session.closed:
            logger.info('Закрытие HTTP-сессии')
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
        url = BASE_URL + PAGE_PARAM.format(page_number)
        logger.info(f'Запрос страницы {page_number}: {url}')
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientResponseError as e:
            logger.error(f'Ошибка при загрузке страницы {page_number}: {e}')
            return None
        except Exception as e:
            logger.error(f'Неизвестная ошибка при загрузке страницы {page_number}: {e}')
            return None

    async def parse_links(self, html: str) -> Tuple[List[Tuple[str, datetime]], bool]:
        """Парсинг ссылок на файлы и извлечение дат из них.

        Args:
            html: HTML-код страницы.

        Returns:
            Tuple[List[Tuple[str, datetime]], bool]: Список кортежей (ссылка, дата) и флаг, указывающий, была ли найдена невалидная дата.
        """
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a', class_=LINK_CLASS)
        result = []
        found_invalid_date = False

        for link in links:
            href = link.get('href', '')
            match = re.search(LINK_PATTERN, href)
            if match:
                date_str = match.group(1)
                try:
                    date_file = datetime.strptime(date_str, DATE_FORMAT)
                    if not self._is_valid_date(date_file):
                        logger.info(f'Невалидная дата {date_file.strftime(DATE_FORMAT)}, завершение перебора')
                        found_invalid_date = True
                        break
                    parsed_url = urllib.parse.urlparse(href)
                    clean_url = urllib.parse.urlunparse(
                        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', '')
                    )
                    full_url = urllib.parse.urljoin(BASE_URL, clean_url)
                    result.append((full_url, date_file))
                except ValueError:
                    logger.error(f'Некорректная дата в ссылке: {href}')
                    continue

        logger.info(f'Извлечено {len(result)} ссылок со страницы')
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
        return self.max_date <= date_file <= current_date

    async def produce_links(self) -> None:
        """Асинхронное извлечение ссылок с пагинацией и добавление их в очередь.

        Returns:
            None
        """
        page_number = 1
        should_continue = True

        try:
            while should_continue:
                html = await self.fetch_page(page_number)
                if html is None:
                    logger.info(f'Прекращение обработки: не удалось загрузить страницу {page_number}')
                    break

                links, found_invalid_date = await self.parse_links(html)
                if not links and not found_invalid_date:
                    logger.info(f'Нет ссылок на странице {page_number}, завершение')
                    break

                for link, date_file in links:
                    logger.info(f'Положил ссылку {link}')
                    await self.queue.put((link, date_file))

                if found_invalid_date:
                    logger.info(f'Обнаружена невалидная дата на странице {page_number}, завершение')
                    break

                page_number += 1
                await asyncio.sleep(REQUEST_DELAY)

        except Exception as e:
            logger.error(f'Ошибка в продюсере: {e}')
        finally:
            logger.info('Продюсер завершил работу')
            self.producer_done.set()


    async def consume_links(self, downloader: FileDownloader, parser: FileParser) -> None:
        """Обработка ссылок из очереди: скачивание и парсинг файлов.

        Args:
            downloader: Экземпляр загрузчика файлов.
            parser: Экземпляр парсера файлов.

        Returns:
            None
        """
        while not (self.producer_done.is_set() and self.queue.empty()):
            try:
                link, date_file = await asyncio.wait_for(self.queue.get(), timeout=CONSUMER_TIMEOUT)
                logger.info(f'Достали из очереди {link}')
                file_content = await downloader.download_file(link)
                parsed_data = await parser.parse_file(file_content, date_file)
                self.queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f'Ошибка в потребителе для ссылки {link}: {e}')
                self.queue.task_done()

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
            consumers = [asyncio.create_task(self.consume_links(downloader, parser)) for _ in range(NUM_CONSUMERS)]
            await asyncio.gather(producer, *consumers)
        except Exception as e:
            logger.error(f'Ошибка в методе run: {e}')
        finally:
            await self.close_session()

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
        logger.error(f"Ошибка: {e}")
    cutoff_date = DEFAULT_CUTOFF_DATE
    parser = SpimexParser(max_date=MAX_DATE, cutoff_date=cutoff_date)
    await parser.run()
    await db.close_db()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f'Ошибка в main: {e}')
    finally:
        logger.info('Программа завершена')