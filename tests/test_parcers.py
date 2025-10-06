""" Тесты для парсера"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest

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


@pytest.fixture
def file_downloader(mock_session):
    """Фикстура для создания экземпляра FileDownloader"""
    return FileDownloader(mock_session)


class TestFileDownloader:
    """Тесты для класса FileDownloader"""

    def test_file_downloader_init(self, mock_session):
        """Тест инициализации FileDownloader"""
        downloader = FileDownloader(mock_session)
        assert downloader.session == mock_session

    @pytest.mark.asyncio
    async def test_download_file_success(self, file_downloader, mock_session):
        """Тест успешного скачивания файла"""
        # Подготовка данных
        test_url = "https://example.com/test.xls"
        test_content = b"test file content"

        # Настройка мока ответа
        mock_response = AsyncMock()
        mock_response.raise_for_status = Mock()
        mock_response.read = AsyncMock(return_value=test_content)

        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Выполнение теста
        result = await file_downloader.download_file(test_url)

        # Проверки
        assert result == test_content
        mock_session.get.assert_called_once_with(test_url)
        mock_response.raise_for_status.assert_called_once()
        mock_response.read.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_file_404_error(self, file_downloader, mock_session):
        """Тест обработки ошибки 404 (файл не найден)"""
        # Подготовка данных
        test_url = "https://example.com/notfound.xls"

        # Настройка мока для ошибки 404
        mock_response = AsyncMock()
        error = aiohttp.ClientResponseError(request_info=Mock(), history=(), status=404, message="Not Found")
        mock_response.raise_for_status = Mock(side_effect=error)  # Обычный Mock для синхронного метода
        mock_response.read = AsyncMock()  # AsyncMock для асинхронного метода

        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Выполнение теста
        result = await file_downloader.download_file(test_url)

        # Проверки
        assert result is None
        mock_session.get.assert_called_once_with(test_url)
        mock_response.raise_for_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_file_other_http_error(self, file_downloader, mock_session):
        """Тест обработки других HTTP ошибок (не 404)"""
        # Подготовка данных
        test_url = "https://example.com/error.xls"

        # Настройка мока для ошибки 500
        mock_response = AsyncMock()
        error = aiohttp.ClientResponseError(
            request_info=Mock(), history=(), status=500, message="Internal Server Error"
        )
        mock_response.raise_for_status = Mock(side_effect=error)  # Обычный Mock для синхронного метода
        mock_response.read = AsyncMock()  # AsyncMock для асинхронного метода

        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Выполнение теста - должно подняться исключение
        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await file_downloader.download_file(test_url)

        # Проверки
        assert exc_info.value.status == 500
        mock_session.get.assert_called_once_with(test_url)
        mock_response.raise_for_status.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception,expected_result",
        [
            (Exception("Network error"), None),
            (asyncio.TimeoutError("Request timeout"), None),
            (ValueError("Invalid URL"), None),
        ],
    )
    async def test_download_file_exceptions(self, file_downloader, mock_session, exception, expected_result):
        """Параметризованный тест обработки различных исключений"""
        # Подготовка данных
        test_url = "https://example.com/error.xls"

        # Настройка мока для исключения
        mock_session.get.side_effect = exception

        # Выполнение теста
        result = await file_downloader.download_file(test_url)

        # Проверки
        assert result == expected_result
        mock_session.get.assert_called_once_with(test_url)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url,expected_calls",
        [
            ("https://example.com/test.xls", 1),
            ("", 1),
            ("invalid-url", 1),
        ],
    )
    async def test_download_file_urls(self, file_downloader, mock_session, url, expected_calls):
        """Параметризованный тест с различными URL"""
        # Настройка мока для исключения (чтобы не делать реальные запросы)
        mock_session.get.side_effect = Exception("Test exception")

        # Выполнение теста
        result = await file_downloader.download_file(url)

        # Проверки
        assert result is None
        assert mock_session.get.call_count == expected_calls
        mock_session.get.assert_called_with(url)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code,should_raise",
        [
            (200, False),
            (404, False),  # 404 обрабатывается специально
            (500, True),
            (403, True),
            (401, True),
        ],
    )
    async def test_download_file_http_status_codes(self, file_downloader, mock_session, status_code, should_raise):
        """Параметризованный тест различных HTTP статус кодов"""
        # Подготовка данных
        test_url = "https://example.com/test.xls"
        test_content = b"test content" if status_code == 200 else None

        # Настройка мока ответа
        mock_response = AsyncMock()
        mock_response.read = AsyncMock(return_value=test_content)

        if status_code == 200:
            mock_response.raise_for_status = Mock()  # Обычный Mock для синхронного метода
        else:
            error = aiohttp.ClientResponseError(
                request_info=Mock(), history=(), status=status_code, message=f"HTTP {status_code}"
            )
            mock_response.raise_for_status = Mock(side_effect=error)  # Обычный Mock для синхронного метода

        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Выполнение теста
        if should_raise:
            with pytest.raises(aiohttp.ClientResponseError) as exc_info:
                await file_downloader.download_file(test_url)
            assert exc_info.value.status == status_code
        else:
            result = await file_downloader.download_file(test_url)
            expected_result = test_content if status_code == 200 else None
            assert result == expected_result

        # Проверки
        mock_session.get.assert_called_once_with(test_url)
        mock_response.raise_for_status.assert_called_once()


class TestSpimexParser:
    """Тесты для класса SpimexParser"""

    def test_spimexparser_init(self, spimexparser):
        """Тест инициализации SpimexParser"""
        assert spimexparser.session is None
        assert isinstance(spimexparser.queue_link, asyncio.Queue)
        assert isinstance(spimexparser.queue_data_for_db, asyncio.Queue)
        assert isinstance(spimexparser.producer_link_done, asyncio.Event)
        assert spimexparser.producer_db == 0
        assert spimexparser.base_url == BASE_URL
        assert spimexparser.link_class == LINK_CLASS
        assert spimexparser.link_pattern == LINK_PATTERN
        assert spimexparser.page_param == PAGE_PARAM
        assert spimexparser.date_format == DATE_FORMAT
        assert spimexparser.num_consumers_link == NUM_CONSUMERS_LINK
        assert spimexparser.consumer_timeout == CONSUMER_TIMEOUT
        assert spimexparser.request_delay == REQUEST_DELAY

    @pytest.mark.asyncio
    async def test_spimexparser_start_session(self, spimexparser):
        """Тест инициализации асинхронной HTTP-сессии"""
        await spimexparser.start_session()
        assert spimexparser.session is not None
        assert isinstance(spimexparser.session, aiohttp.ClientSession)

    @pytest.mark.asyncio
    async def test_spimexparser_close_session(self, spimexparser):
        """Тест закрытия асинхронной HTTP-сессии"""
        spimexparser.session = aiohttp.ClientSession()
        await spimexparser.close_session()
        assert spimexparser.session.closed is True

    @pytest.mark.asyncio
    async def test_spimexparser_fetch_page(self, spimexparser):
        """Тест получения HTML-кода страницы"""
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        spimexparser.session = mock_session

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value="<html>test content</html>")
        mock_session.get.return_value.__aenter__.return_value = mock_response

        result = await spimexparser.fetch_page(1)

        assert result == "<html>test content</html>"
        mock_session.get.assert_called_once_with(f"{BASE_URL}?page=page-1")

    @pytest.mark.parametrize(
        "date_file,current_date,cutoff_date,max_date,expected_result",
        [
            (datetime(2025, 1, 2), datetime(2025, 1, 1), datetime(2025, 1, 1), None, False),
            (datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 1), None, True),
            (datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 2), None, False),
            (datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 1), datetime(2026, 1, 1), False),
            (datetime(2025, 1, 2), datetime(2025, 1, 3), datetime(2025, 1, 1), datetime(2025, 1, 1), True),
        ],
    )
    def test_spimexparser_is_valid_date(
        self, spimexparser, date_file, current_date, cutoff_date, expected_result, max_date
    ):
        """Тест проверки валидности даты"""
        spimexparser.cutoff_date = cutoff_date
        spimexparser.max_date = max_date

        with patch("src.parcers.parser_link.datetime") as mock_datetime:
            # Настраиваем мок для now()
            mock_datetime.now.return_value = current_date
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            result = spimexparser._is_valid_date(date_file)
            assert result == expected_result

    @pytest.mark.asyncio
    async def test_parse_links_success(self, spimexparser):
        """Тест успешного парсинга ссылок с валидными датами"""
        # Подготовка тестового HTML
        html = """
        <html>
            <body>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250105162000.xls">Бюллетень 05.01.2025</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250104162000.xls">Бюллетень 04.01.2025</a>
            </body>
        </html>
        """
        
        # Мокируем _is_valid_date, чтобы все даты были валидными
        with patch.object(spimexparser, '_is_valid_date', return_value=True):
            # Выполнение теста
            result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки
        assert len(result) == 2
        assert found_invalid_date is False
        assert result[0][0] == "https://spimex.com/upload/reports/oil_xls/oil_xls_20250105162000.xls"
        assert result[0][1] == datetime(2025, 1, 5)
        assert result[1][0] == "https://spimex.com/upload/reports/oil_xls/oil_xls_20250104162000.xls"
        assert result[1][1] == datetime(2025, 1, 4)

    @pytest.mark.asyncio
    async def test_parse_links_with_invalid_date(self, spimexparser):
        """Тест парсинга ссылок с невалидной датой (прерывание по дате)"""
        # Подготовка тестового HTML
        html = """
        <html>
            <body>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250105162000.xls">Бюллетень 05.01.2025</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20241230162000.xls">Бюллетень 30.12.2024</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20241229162000.xls">Бюллетень 29.12.2024</a>
            </body>
        </html>
        """
        
        # Мокируем _is_valid_date: первая дата валидна, остальные - нет
        with patch.object(spimexparser, '_is_valid_date', side_effect=[True, False, False]):
            # Выполнение теста
            result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки - должна быть обработана только первая ссылка
        assert len(result) == 1
        assert found_invalid_date is True
        assert result[0][1] == datetime(2025, 1, 5)

    @pytest.mark.asyncio
    async def test_parse_links_empty_html(self, spimexparser):
        """Тест парсинга пустого HTML (нет ссылок)"""
        html = """
        <html>
            <body>
                <p>Нет ссылок</p>
            </body>
        </html>
        """
        
        # Выполнение теста
        result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки
        assert len(result) == 0
        assert found_invalid_date is False

    @pytest.mark.asyncio
    async def test_parse_links_no_matching_pattern(self, spimexparser):
        """Тест парсинга HTML с ссылками, не соответствующими паттерну"""
        html = """
        <html>
            <body>
                <a class="accordeon-inner__item-title link xls" href="/some/other/link.xls">Другой файл</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/no_date_here.xls">Без даты</a>
            </body>
        </html>
        """
        
        # Выполнение теста
        result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки - ссылки не соответствуют паттерну, поэтому результат пустой
        assert len(result) == 0
        assert found_invalid_date is False

    @pytest.mark.asyncio
    async def test_parse_links_with_malformed_date(self, spimexparser):
        """Тест парсинга ссылок с некорректной датой (должна быть пропущена)"""
        html = """
        <html>
            <body>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20259999162000.xls">Некорректная дата</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250105162000.xls">Бюллетень 05.01.2025</a>
            </body>
        </html>
        """
        
        # Мокируем _is_valid_date, чтобы вторая дата была валидной
        with patch.object(spimexparser, '_is_valid_date', return_value=True):
            # Выполнение теста
            result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки - некорректная дата должна быть пропущена, валидная обработана
        assert len(result) == 1
        assert found_invalid_date is False
        assert result[0][1] == datetime(2025, 1, 5)

    @pytest.mark.asyncio
    async def test_parse_links_with_query_params(self, spimexparser):
        """Тест парсинга ссылок с параметрами запроса (должны быть удалены)"""
        html = """
        <html>
            <body>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250105162000.xls?version=1&tracking=abc">Бюллетень</a>
            </body>
        </html>
        """
        
        # Мокируем _is_valid_date
        with patch.object(spimexparser, '_is_valid_date', return_value=True):
            # Выполнение теста
            result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки - параметры запроса должны быть удалены
        assert len(result) == 1
        assert "?" not in result[0][0]
        assert result[0][0] == "https://spimex.com/upload/reports/oil_xls/oil_xls_20250105162000.xls"

    @pytest.mark.asyncio
    async def test_parse_links_with_max_date(self, spimexparser):
        """Тест парсинга ссылок с установленным max_date"""
        html = """
        <html>
            <body>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250105162000.xls">Бюллетень 05.01.2025</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250103162000.xls">Бюллетень 03.01.2025</a>
                <a class="accordeon-inner__item-title link xls" href="/upload/reports/oil_xls/oil_xls_20250102162000.xls">Бюллетень 02.01.2025</a>
            </body>
        </html>
        """
        
        # Мокируем _is_valid_date: первая дата валидна, остальные - нет
        with patch.object(spimexparser, '_is_valid_date', side_effect=[True, False, False]):
            # Выполнение теста
            result, found_invalid_date = await spimexparser.parse_links(html)
        
        # Проверки - должна быть обработана только первая ссылка (05.01)
        # 03.01 и 02.01 должны быть отфильтрованы
        assert len(result) == 1
        assert found_invalid_date is True
        assert result[0][1] == datetime(2025, 1, 5)