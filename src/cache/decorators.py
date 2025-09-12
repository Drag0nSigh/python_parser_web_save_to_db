import hashlib
from functools import wraps
from typing import Any, Callable, Optional
from src.cache.redis_client import redis_client


def cache_key(*args, **kwargs) -> str:
    """Генерация ключа кэша на основе аргументов функции"""
    key_data = f"{args}:{sorted(kwargs.items())}"
    return hashlib.md5(key_data.encode()).hexdigest()


def cached(key_prefix: str = ""):
    """
    Декоратор для кэширования результатов функций
    TTL автоматически устанавливается до времени сброса кэша (14:11)
    
    Args:
        key_prefix: Префикс для ключа кэша
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Генерируем ключ кэша
            cache_key_value = f"{key_prefix}:{func.__name__}:{cache_key(*args, **kwargs)}"
            
            # Пытаемся получить данные из кэша
            cached_result = await redis_client.get(cache_key_value)
            if cached_result is not None:
                return cached_result
            
            # Выполняем функцию и кэшируем результат
            result = await func(*args, **kwargs)
            # TTL будет автоматически установлен до времени сброса
            await redis_client.set(cache_key_value, result)
            
            return result
        
        return wrapper
    return decorator


def invalidate_cache(pattern: str):
    """
    Декоратор для инвалидации кэша после выполнения функции
    
    Args:
        pattern: Паттерн ключей для удаления (например, "dynamics:*")
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            await redis_client.delete_pattern(pattern)
            return result
        
        return wrapper
    return decorator


def manual_cache_clear():
    """
    Декоратор для ручной очистки всего кэша
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            await redis_client.clear_all_cache()
            return result
        
        return wrapper
    return decorator
