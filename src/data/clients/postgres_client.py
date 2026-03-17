from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.config.settings import settings

# ── FastAPI engine — persistent pool for HTTP request lifecycle ───────────────
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    connect_args={"server_settings": {"search_path": "ticket"}},
)

AsyncSessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

fresh_read_engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    execution_options={"isolation_level": "AUTOCOMMIT"},
    connect_args={"server_settings": {"search_path": "ticket"}},
)

FreshReadSessionFactory = async_sessionmaker(
    bind=fresh_read_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

# ── Celery engine — NullPool, fresh connection per task ──────────────────────
celery_engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    poolclass=NullPool,
    connect_args={"server_settings": {"search_path": "ticket"}},
)

CelerySessionFactory = async_sessionmaker(
    bind=celery_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — persistent pool, full transaction control."""
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_fresh_read_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — AUTOCOMMIT, always sees latest committed rows.
    Use for read endpoints that must reflect writes from other services/processes.
    Do NOT use for writes or multi-statement transactions."""
    async with FreshReadSessionFactory() as session:
        try:
            yield session
        except Exception:
            raise
        finally:
            await session.close()