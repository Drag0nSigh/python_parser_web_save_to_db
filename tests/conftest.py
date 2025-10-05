"""Конфигурация pytest для проекта."""

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import pytest

# Добавляем корень проекта в PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def mock_session():
    """Фикстура для создания мока aiohttp.ClientSession"""
    return AsyncMock(spec=aiohttp.ClientSession)


@pytest.fixture
def database():
    """Фикстура для создания объекта Database для тестов."""
    from src.db.db import Database

    return Database("localhost", "test_db", "password", "user", 5432)
