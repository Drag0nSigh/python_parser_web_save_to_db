from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.deps import database
from src.api.routes import router as api_router


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    await database.init_db()
    try:
        yield
    finally:
        await database.close_db()


app = FastAPI(title="Parser API", lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Parser API is running!"}


app.include_router(api_router)


__all__ = ["app"]
