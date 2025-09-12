from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.deps import database, redis_client
from src.api.routes import router as api_router


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    # Инициализация базы данных
    await database.init_db()
    
    # Инициализация Redis с планировщиком
    await redis_client.connect()
    
    try:
        yield
    finally:
        # Закрытие соединений
        await database.close_db()
        await redis_client.disconnect()


app = FastAPI(title="Parser API", lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Parser API is running!"}


app.include_router(api_router)


__all__ = ["app"]
