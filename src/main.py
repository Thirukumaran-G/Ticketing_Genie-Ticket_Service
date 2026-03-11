import uvicorn

from src.api.rest.app import create_app
from src.config.settings import settings
from src.data.clients.postgres_client import engine
from src.data.models.postgres.models import Base
from sqlalchemy import text


async def create_tables() -> None:
    """Create all ticket schema tables if they don't exist."""
    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS ticket"))
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.run_sync(Base.metadata.create_all)


app = create_app()


@app.on_event("startup")
async def startup() -> None:
    await create_tables()   


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host="127.0.0.1",
        port=settings.PORT,
        reload=False,
        log_level=settings.LOG_LEVEL.lower(),
    )
