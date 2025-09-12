from datetime import date
from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy import and_, desc, distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_session, get_redis
from src.api.models import BulletinModel, BulletinModelShort
from src.cache.decorators import cached, manual_cache_clear
from src.db.bulletin import Bulletin


async def health_check() -> dict:
    return {"status": "ok"}


async def list_bulletins(session: AsyncSession = Depends(get_session)) -> List[BulletinModel]:
    """Получить список последний 100 бюллетеней."""
    result = await session.execute(select(Bulletin).order_by(Bulletin.date.desc()).limit(100))
    rows = result.scalars().all()
    return [BulletinModel.model_validate(row, from_attributes=True) for row in rows]


async def get_last_trading_dates(
    limit: int = Query(10, ge=1, le=100, description="Количество последних торговых дней"),
    session: AsyncSession = Depends(get_session),
) -> List[str]:
    """Получить список дат последних торговых дней."""
    result = await session.execute(select(distinct(Bulletin.date)).order_by(desc(Bulletin.date)).limit(limit))
    dates = result.scalars().all()
    return [date.isoformat() for date in dates if date]


@cached(key_prefix="dynamics")
async def get_dynamics(
    oil_id: Optional[str] = Query(None, description="ID нефтепродукта"),
    delivery_type_id: Optional[str] = Query(None, description="ID типа поставки"),
    delivery_basis_id: Optional[str] = Query(None, description="ID базиса поставки"),
    start_date: Optional[date] = Query(None, description="Начальная дата (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Конечная дата (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, description="Максимальное количество записей"),
    session: AsyncSession = Depends(get_session),
) -> List[dict]:
    """Получить динамику торгов за заданный период с фильтрацией."""
    query = select(Bulletin)

    # Строим условия фильтрации
    conditions = []

    if oil_id:
        conditions.append(Bulletin.oil_id == oil_id)
    if delivery_type_id:
        conditions.append(Bulletin.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        conditions.append(Bulletin.delivery_basis_id == delivery_basis_id)
    if start_date:
        conditions.append(Bulletin.date >= start_date)
    if end_date:
        conditions.append(Bulletin.date <= end_date)

    # Применяем условия фильтрации
    if conditions:
        query = query.where(and_(*conditions))

    # Сортируем по дате (новые сначала) и ограничиваем количество
    query = query.order_by(desc(Bulletin.date)).limit(limit)

    result = await session.execute(query)
    rows = result.scalars().all()
    return [
        BulletinModelShort.model_validate(row, from_attributes=True).model_dump()
        for row in rows
    ]


@cached(key_prefix="trading_results")
async def get_trading_results(
    oil_id: Optional[str] = Query(None, description="ID нефтепродукта"),
    delivery_type_id: Optional[str] = Query(None, description="ID типа поставки"),
    delivery_basis_id: Optional[str] = Query(None, description="ID базиса поставки"),
    limit: int = Query(100, ge=1, description="Количество последних торгов"),
    session: AsyncSession = Depends(get_session),
) -> List[dict]:
    """Получить список последних торгов с фильтрацией."""
    query = select(Bulletin)

    # Строим условия фильтрации
    conditions = []

    if oil_id:
        conditions.append(Bulletin.oil_id == oil_id)
    if delivery_type_id:
        conditions.append(Bulletin.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        conditions.append(Bulletin.delivery_basis_id == delivery_basis_id)

    # Применяем условия фильтрации
    if conditions:
        query = query.where(and_(*conditions))

    # Сортируем по дате (новые сначала) и ограничиваем количество
    query = query.order_by(desc(Bulletin.date)).limit(limit)

    result = await session.execute(query)
    rows = result.scalars().all()
    return [BulletinModelShort.model_validate(row, from_attributes=True).model_dump() for row in rows]


async def get_cache_info(redis_client=Depends(get_redis)) -> dict:
    """Получить информацию о состоянии кэша"""
    return await redis_client.get_cache_info()


@manual_cache_clear()
async def clear_cache() -> dict:
    """Ручная очистка всего кэша"""
    return {"message": "Кэш очищен вручную"}






__all__ = ["health_check", "list_bulletins", "get_last_trading_dates", "get_dynamics", "get_trading_results"]
