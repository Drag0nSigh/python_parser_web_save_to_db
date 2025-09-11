from sqlalchemy.ext.asyncio import AsyncSession

from src.db.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from src.db.db import Database

database = Database(
    host=DB_HOST or "localhost",
    db_name=DB_NAME or "parser",
    db_password=DB_PASS or "postgres",
    user=DB_USER or "postgres",
    db_port=int(DB_PORT or 5432),
)


async def get_session() -> AsyncSession:
    session = await database.get_session()
    try:
        yield session
    finally:
        await session.close()


__all__ = ["database", "get_session"]
