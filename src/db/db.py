import asyncio
import datetime
import logging
from typing import Optional

import asyncpg
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql import func

from src.db.bulletin import Base, Bulletin
from src.parcers.parser_link import SpimexParser

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, host: str, db_name: str, db_password: str, user: str, db_port: int = 5432):
        """Инициализирует объект для работы с базой данных PostgreSQL.

        Args:
            host (str): Хост базы данных.
            db_name (str): Имя базы данных.
            db_password (str): Пароль для подключения к базе данных.
            user (str): Имя пользователя для подключения к базе данных.
            db_port (int): Порт базы данных. По умолчанию 5432.

        Notes:
            Формирует URL подключения для SQLAlchemy с использованием asyncpg.
            Инициализирует engine и session как None, они создаются при вызове init_db.
        """
        self.host = host
        self.db_name = db_name
        self.db_password = db_password
        self.user = user
        self.db_port = db_port
        self.engine = None
        self.session_factory = None
        self.base_url = f"postgresql+asyncpg://{user}:{db_password}@{host}:{db_port}"

    async def create_db(self):
        """Создает базу данных, если она не существует."""
        try:
            conn = await asyncpg.connect(
                database="postgres",
                user=self.user,
                password=self.db_password,
                host=self.host,
                port=self.db_port,
            )
            result = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", self.db_name)
            if not result:
                logger.info(f"Создаётся БД: {self.db_name}")
                await conn.execute(f"CREATE DATABASE {self.db_name}")
            else:
                logger.info(f"База данных {self.db_name} уже существует")
            await conn.close()
        except Exception as e:
            logger.error(f"При создании БД {self.db_name} произошла ошибка {e}")

    async def init_db(self):
        """Инициализируем базу данных и создаём таблицы"""

        await self.create_db()
        self.engine = create_async_engine(f"{self.base_url}/{self.db_name}", pool_pre_ping=True, echo=True)

        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                logger.info("Таблица успешно создана или существует")
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц {e}")

        self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def close_db(self):
        """Закрывает движок базы данных, если он был инициализирован."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self.session_factory = None
            logger.info("Движок базы данных закрыт")

    async def get_session(self) -> AsyncSession:
        """Возвращает асинхронную сессию SQLAlchemy для работы с базой данных."""
        if self.session_factory is None:
            if self.engine is None:
                self.engine = create_async_engine(f"{self.base_url}/{self.db_name}", pool_pre_ping=True, echo=True)
            self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        return self.session_factory()

    async def get_max_data_bulletin(self) -> Optional[datetime.date]:
        """Получает максимальную дату бюллетеня из таблицы Bulletin.

        Returns:
            Optional[datetime.date]: Максимальная дата из столбца data таблицы Bulletin или None, если таблица пуста
            или произошла ошибка.

        Raises:
            sqlalchemy.exc.DatabaseError: Если произошла ошибка при выполнении запроса к базе данных.
        """
        async with await self.get_session() as session:
            try:
                max_data_bulletin = await session.execute(func.max(Bulletin.date))
                logger.info(f"Максимальная дата биллютеня: {max_data_bulletin}")
                max_data_bulletin = max_data_bulletin.scalar()
                return max_data_bulletin
            except Exception as e:
                logger.error(e)
                return None

    async def put_data_into_bd(self, queue: asyncio.Queue, obj: SpimexParser):
        """Сохраняет данные из очереди в базу данных, используя модель Bulletin.

        Args:
            queue (asyncio.Queue): Очередь, содержащая списки словарей с данными для вставки.
            obj (SpimexParser): Экземпляр парсера SPIMEX для проверки состояния продюсеров.

        Notes:
            Обрабатывает данные из очереди, пока она не пуста и продюсеры активны.
            Использует таймаут 5 секунд для ожидания данных из очереди.
            Выполняет вставку данных через SQLAlchemy ORM.

        Raises:
            asyncio.TimeoutError: Если истёк таймаут ожидания данных из очереди.
            sqlalchemy.exc.DatabaseError: Если произошла ошибка при вставке данных в базу.
        """
        while not (obj.producer_db == 0 and queue.empty()):
            data_bulletin = await asyncio.wait_for(queue.get(), timeout=5)
            if data_bulletin:
                try:
                    async with self.session_factory() as session:
                        await session.execute(insert(Bulletin), data_bulletin)
                        await session.commit()
                except Exception as e:
                    logger.error(f"Ошибка при записи в БД {e}")
