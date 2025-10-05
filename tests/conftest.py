"""Конфигурация pytest для проекта."""

import datetime
from unittest.mock import AsyncMock

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
from src.db.db import Database
from src.parcers.parser_link import SpimexParser


@pytest.fixture
def mock_session():
    """Фикстура для создания мока aiohttp.ClientSession"""
    return AsyncMock(spec=aiohttp.ClientSession)


@pytest.fixture
def database():
    """Фикстура для создания объекта Database для тестов."""
    return Database("localhost", "test_db", "password", "user", 5432)


@pytest.fixture
def spimexparser():
    """Фикстура для создания объекта SpimexParser для тестов."""
    return SpimexParser(
        datetime.datetime(2025, 1, 1),
        None,
        BASE_URL,
        LINK_CLASS,
        LINK_PATTERN,
        PAGE_PARAM,
        DATE_FORMAT,
        NUM_CONSUMERS_LINK,
        CONSUMER_TIMEOUT,
        REQUEST_DELAY,
    )
