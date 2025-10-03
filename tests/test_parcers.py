""" Тесты для парсера"""

import asyncio
import pytest
import aiohttp
from unittest.mock import AsyncMock, Mock
from src.parcers.file_downloader import FileDownloader


@pytest.fixture
def mock_session():
    """Фикстура для создания мока aiohttp.ClientSession"""
    return AsyncMock(spec=aiohttp.ClientSession)


@pytest.fixture
def file_downloader(mock_session):
    """Фикстура для создания экземпляра FileDownloader"""
    return FileDownloader(mock_session)


def test_file_downloader_init(mock_session):
    """Тест инициализации FileDownloader"""
    downloader = FileDownloader(mock_session)
    assert downloader.session == mock_session


@pytest.mark.asyncio
async def test_download_file_success(file_downloader, mock_session):
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
async def test_download_file_404_error(file_downloader, mock_session):
    """Тест обработки ошибки 404 (файл не найден)"""
    # Подготовка данных
    test_url = "https://example.com/notfound.xls"
    
    # Настройка мока для ошибки 404
    mock_response = AsyncMock()
    error = aiohttp.ClientResponseError(
        request_info=Mock(),
        history=(),
        status=404,
        message="Not Found"
    )
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
async def test_download_file_other_http_error(file_downloader, mock_session):
    """Тест обработки других HTTP ошибок (не 404)"""
    # Подготовка данных
    test_url = "https://example.com/error.xls"
    
    # Настройка мока для ошибки 500
    mock_response = AsyncMock()
    error = aiohttp.ClientResponseError(
        request_info=Mock(),
        history=(),
        status=500,
        message="Internal Server Error"
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
@pytest.mark.parametrize("exception,expected_result", [
    (Exception("Network error"), None),
    (asyncio.TimeoutError("Request timeout"), None),
    (ValueError("Invalid URL"), None),
])
async def test_download_file_exceptions(file_downloader, mock_session, exception, expected_result):
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
@pytest.mark.parametrize("url,expected_calls", [
    ("https://example.com/test.xls", 1),
    ("", 1),
    ("invalid-url", 1),
])
async def test_download_file_urls(file_downloader, mock_session, url, expected_calls):
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
@pytest.mark.parametrize("status_code,should_raise", [
    (200, False),
    (404, False),  # 404 обрабатывается специально
    (500, True),
    (403, True),
    (401, True),
])
async def test_download_file_http_status_codes(file_downloader, mock_session, status_code, should_raise):
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
            request_info=Mock(),
            history=(),
            status=status_code,
            message=f"HTTP {status_code}"
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