import os
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_URL = os.environ.get("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

# Настройки кэширования
CACHE_TTL = int(os.environ.get("CACHE_TTL", 300))  # 5 минут по умолчанию
CACHE_RESET_TIME = os.environ.get("CACHE_RESET_TIME", "14:11")  # Время сброса кэша

__all__ = ["REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD", "REDIS_URL", "CACHE_TTL", "CACHE_RESET_TIME"]
