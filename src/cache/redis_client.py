import json
import asyncio
from datetime import datetime, time
from typing import Any, Optional
import redis.asyncio as redis
from src.cache.config import REDIS_URL, CACHE_RESET_TIME


class RedisClient:
    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def connect(self):
        """Подключение к Redis и запуск планировщика"""
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        self._is_running = True
        # Запускаем планировщик в фоновом режиме
        self._scheduler_task = asyncio.create_task(self._scheduler())

    async def disconnect(self):
        """Отключение от Redis и остановка планировщика"""
        self._is_running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        if self.redis:
            await self.redis.close()

    async def _scheduler(self):
        """Планировщик для сброса кэша в определенное время"""
        while self._is_running:
            try:
                now = datetime.now()
                reset_time = time(*map(int, CACHE_RESET_TIME.split(':')))
                
                # Вычисляем время до следующего сброса
                next_reset = datetime.combine(now.date(), reset_time)
                if next_reset <= now:
                    next_reset = datetime.combine(now.date().replace(day=now.day + 1), reset_time)
                
                sleep_seconds = (next_reset - now).total_seconds()
                
                # Ждем до времени сброса
                await asyncio.sleep(sleep_seconds)
                
                # Сбрасываем весь кэш
                if self._is_running:
                    await self.clear_all_cache()
                    print(f"Кэш сброшен в {datetime.now().strftime('%H:%M:%S')}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Ошибка в планировщике кэша: {e}")
                await asyncio.sleep(60)  # Ждем минуту перед повтором

    async def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша"""
        if not self.redis:
            return None
        
        try:
            value = await self.redis.get(key)
            return json.loads(value) if value else None
        except Exception:
            return None

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Сохранить значение в кэш"""
        if not self.redis:
            return False
        
        try:
            # Если TTL не указан, устанавливаем до времени сброса
            if ttl is None:
                ttl = self._get_ttl_until_reset()
            
            await self.redis.set(key, json.dumps(value, default=str), ex=ttl)
            return True
        except Exception:
            return False

    def _get_ttl_until_reset(self) -> int:
        """Вычислить TTL до времени сброса кэша"""
        now = datetime.now()
        reset_time = time(*map(int, CACHE_RESET_TIME.split(':')))
        
        next_reset = datetime.combine(now.date(), reset_time)
        if next_reset <= now:
            next_reset = datetime.combine(now.date().replace(day=now.day + 1), reset_time)
        
        return int((next_reset - now).total_seconds())

    async def delete(self, key: str) -> bool:
        """Удалить значение из кэша"""
        if not self.redis:
            return False
        
        try:
            await self.redis.delete(key)
            return True
        except Exception:
            return False

    async def delete_pattern(self, pattern: str) -> bool:
        """Удалить все ключи по паттерну"""
        if not self.redis:
            return False
        
        try:
            keys = await self.redis.keys(pattern)
            if keys:
                await self.redis.delete(*keys)
            return True
        except Exception:
            return False

    async def clear_all_cache(self) -> bool:
        """Очистить весь кэш"""
        if not self.redis:
            return False
        
        try:
            await self.redis.flushdb()
            return True
        except Exception:
            return False

    async def get_cache_info(self) -> dict:
        """Получить информацию о кэше"""
        if not self.redis:
            return {"status": "disconnected"}
        
        try:
            info = await self.redis.info()
            keys_count = await self.redis.dbsize()
            return {
                "status": "connected",
                "keys_count": keys_count,
                "memory_usage": info.get("used_memory_human", "unknown"),
                "next_reset": CACHE_RESET_TIME
            }
        except Exception:
            return {"status": "error"}


# Глобальный экземпляр Redis клиента
redis_client = RedisClient()
