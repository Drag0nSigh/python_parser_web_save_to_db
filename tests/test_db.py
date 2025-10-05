"""Тесты базы данных"""

import importlib
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import src.db.config
from src.db.bulletin import Bulletin
from src.db.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER


class TestBulletin:
    """Тесты для модели Bulletin."""

    def test_valid_bulletin_creation(self):
        """Тест создания валидного Bulletin."""
        bulletin = Bulletin(
            exchange_product_id=1,
            exchange_product_name="test",
            oil_id="Rg1",
            delivery_basis_id="Gj0",
            delivery_basis_name="Yuf1",
            delivery_type_id="uy7",
            volume=1,
            total=10,
            count=1,
            date=date(2025, 1, 1),
        )

        assert bulletin.exchange_product_id == 1
        assert bulletin.exchange_product_name == "test"
        assert bulletin.oil_id == "Rg1"
        assert bulletin.delivery_basis_id == "Gj0"
        assert bulletin.delivery_basis_name == "Yuf1"
        assert bulletin.delivery_type_id == "uy7"
        assert bulletin.volume == 1
        assert bulletin.total == 10
        assert bulletin.count == 1
        assert bulletin.date == date(2025, 1, 1)


class TestDBConfig:
    """Тесты для конфигурации базы данных."""

    @patch.dict(
        "os.environ",
        {
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "POSTGRES_DB": "test_db",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
        },
    )
    def test_db_config_with_all_variables(self):
        """Тест загрузки всех переменных окружения для БД."""
        # Перезагружаем модуль для применения новых переменных окружения
        importlib.reload(src.db.config)

        assert src.db.config.DB_USER == "test_user"
        assert src.db.config.DB_PASS == "test_password"
        assert src.db.config.DB_NAME == "test_db"
        assert src.db.config.DB_HOST == "localhost"
        assert src.db.config.DB_PORT == "5432"

    @patch.dict(
        "os.environ",
        {
            "POSTGRES_USER": "user",
            "POSTGRES_PASSWORD": "pass",
            "POSTGRES_DB": "db",
            "DB_HOST": "host",
            "DB_PORT": "port",
        },
    )
    def test_db_config_with_partial_variables(self):
        """Тест загрузки части переменных окружения."""
        # Перезагружаем модуль для применения новых переменных окружения
        importlib.reload(src.db.config)

        assert src.db.config.DB_USER == "user"
        assert src.db.config.DB_PASS == "pass"
        assert src.db.config.DB_NAME == "db"
        assert src.db.config.DB_HOST == "host"
        assert src.db.config.DB_PORT == "port"

    @patch.dict(
        "os.environ", {"POSTGRES_USER": "", "POSTGRES_PASSWORD": "", "POSTGRES_DB": "", "DB_HOST": "", "DB_PORT": ""}
    )
    def test_db_config_with_empty_variables(self):
        """Тест загрузки пустых переменных окружения."""
        # Перезагружаем модуль для применения новых переменных окружения
        importlib.reload(src.db.config)

        assert src.db.config.DB_USER == ""
        assert src.db.config.DB_PASS == ""
        assert src.db.config.DB_NAME == ""
        assert src.db.config.DB_HOST == ""
        assert src.db.config.DB_PORT == ""

    def test_db_config_variable_types(self):
        """Тест типов переменных конфигурации."""
        # Проверяем, что все переменные являются строками или None
        assert isinstance(DB_USER, (str, type(None)))
        assert isinstance(DB_PASS, (str, type(None)))
        assert isinstance(DB_NAME, (str, type(None)))
        assert isinstance(DB_HOST, (str, type(None)))
        assert isinstance(DB_PORT, (str, type(None)))


class TestDatabase:
    """Тесты для класса Database."""

    def test_init(self, database):
        """Тест инициализации базы данных."""
        assert database.host == "localhost"
        assert database.db_name == "test_db"
        assert database.db_password == "password"
        assert database.user == "user"
        assert database.db_port == 5432

    @pytest.mark.asyncio
    async def test_init_db_success(self, database):
        """Тест успешной инициализации базы данных."""

        # Мокаем create_db
        with patch.object(database, "create_db", new_callable=AsyncMock) as mock_create_db:
            # Мокаем create_async_engine
            with patch("src.db.db.create_async_engine") as mock_create_engine:
                # Мокаем async_sessionmaker
                with patch("src.db.db.async_sessionmaker") as mock_sessionmaker:
                    # Мокаем engine
                    mock_engine = MagicMock()
                    mock_create_engine.return_value = mock_engine

                    # Мокаем контекстный менеджер engine.begin()
                    mock_conn = MagicMock()
                    mock_engine.begin.return_value.__aenter__.return_value = mock_conn

                    # Мокаем session_factory
                    mock_session_factory = MagicMock()
                    mock_sessionmaker.return_value = mock_session_factory

                    # Выполняем инициализацию
                    await database.init_db()

                    # Проверяем, что create_db был вызван
                    mock_create_db.assert_called_once()

                    # Проверяем, что create_async_engine был вызван с правильными параметрами
                    expected_url = f"{database.base_url}/{database.db_name}"
                    mock_create_engine.assert_called_once_with(expected_url, pool_pre_ping=True, echo=True)

                    # Проверяем, что run_sync был вызван для создания таблиц
                    mock_conn.run_sync.assert_called_once()

                    # Проверяем, что session_factory был создан
                    mock_sessionmaker.assert_called_once_with(mock_engine, class_=AsyncSession, expire_on_commit=False)

                    # Проверяем, что engine и session_factory установлены
                    assert database.engine == mock_engine
                    assert database.session_factory == mock_session_factory

    @pytest.mark.asyncio
    async def test_init_db_with_exception_during_table_creation(self, database):
        """Тест инициализации БД с исключением при создании таблиц."""

        with patch.object(database, "create_db", new_callable=AsyncMock):
            with patch("src.db.db.create_async_engine") as mock_create_engine:
                with patch("src.db.db.async_sessionmaker") as mock_sessionmaker:
                    # Мокаем engine
                    mock_engine = MagicMock()
                    mock_create_engine.return_value = mock_engine

                    # Мокаем контекстный менеджер engine.begin() с исключением
                    mock_engine.begin.return_value.__aenter__.side_effect = Exception("Table creation error")

                    # Мокаем session_factory
                    mock_session_factory = MagicMock()
                    mock_sessionmaker.return_value = mock_session_factory

                    # Выполняем инициализацию (не должно вызывать исключение)
                    await database.init_db()

                    # Проверяем, что create_async_engine был вызван
                    mock_create_engine.assert_called_once()

                    # Проверяем, что session_factory все равно был создан
                    mock_sessionmaker.assert_called_once()

                    # Проверяем, что engine и session_factory установлены
                    assert database.engine == mock_engine
                    assert database.session_factory == mock_session_factory

    @pytest.mark.asyncio
    async def test_init_db_create_db_exception(self, database):
        """Тест инициализации БД с исключением в create_db."""

        # Мокаем create_db с исключением
        with patch.object(database, "create_db", new_callable=AsyncMock, side_effect=Exception("DB creation error")):
            # Выполняем инициализацию (должно вызвать исключение)
            with pytest.raises(Exception, match="DB creation error"):
                await database.init_db()

            # Проверяем, что engine и session_factory остались None
            assert database.engine is None
            assert database.session_factory is None

    @pytest.mark.asyncio
    async def test_init_db_engine_creation_failure(self, database):
        """Тест инициализации БД с ошибкой создания engine."""

        with patch.object(database, "create_db", new_callable=AsyncMock):
            with patch("src.db.db.create_async_engine", side_effect=Exception("Engine creation error")):
                with patch("src.db.db.async_sessionmaker") as mock_sessionmaker:
                    # Выполняем инициализацию (должно вызвать исключение)
                    with pytest.raises(Exception, match="Engine creation error"):
                        await database.init_db()

                    # Проверяем, что session_factory не был создан
                    mock_sessionmaker.assert_not_called()

                    # Проверяем, что engine остался None
                    assert database.engine is None
                    assert database.session_factory is None
