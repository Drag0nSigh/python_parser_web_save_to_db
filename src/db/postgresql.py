from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
import asyncpg
import logging

from .config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from .bulletin import Base

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, host: str, db_name: str, db_password: str,  user: str, db_port: int = 5432):
        self.host = host
        self.db_name = db_name
        self.db_password = db_password
        self.user = user
        self.db_port = db_port
        self.engine = None
        self.session = None
        self.base_url = f'postgresql+asyncpg://{user}:{db_password}@{host}:{db_port}'

    async def create_db(self):
        """Создает базу данных, если она не существует."""
        try:
            conn = await asyncpg.connect(
                database='postgres',
                user=self.user,
                password=self.db_password,
                host=self.host,
                port=self.db_port,

            )
            result = await conn.fetchval('SELECT 1 FROM pg_database WHERE datname=$1', self.db_name)
            if not result:
                logger.info(f'Создаётся БД: {self.db_name}')
                await conn.execute(f'CREATE DATABASE {self.db_name}')
            else:
                logger.info(f'База данных {self.db_name} уже существует')
            await conn.close()
        except Exception as e:
            logger.error(f'При создании БД {self.db_name} произошла ошибка')

    async def init_db(self):
        """Инициализируем базу данных и создаём таблицы"""
        await self.create_db()
        self.engine = create_async_engine(f'{self.base_url}/{self.db_name}', pool_pre_ping=True, echo=True)

        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                logger.info(f'Таблица успешно создана или существует')
        except Exception as e:
            logger.error(f'Ошибка при создании таблиц {e}')

        self.session = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def close_db(self):
        if self.engine:
            await self.engine.dispose()
            logger.info(f'Движок бд закрыт')

    async def get_session(self) -> AsyncSession:
        if self.session is None:
            await self.init_db()
        return self.session()

