from typing import Any, Generic, TypeVar

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Base

ModelT = TypeVar("ModelT", bound=Base)

class BaseRepository(Generic[ModelT]):
    """Generic repository with common async DB operations."""

    model: type[ModelT]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, record_id: int) -> ModelT | None:
        return await self.session.get(self.model, record_id)

    async def create(self, obj: ModelT) -> ModelT:
        self.session.add(obj)
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def update_fields(self, record_id: int, fields: dict[str, Any]) -> None:
        """Update specific fields on a record by ID."""
        pk_col = next(iter(self.model.__table__.primary_key.columns))  # type: ignore[attr-defined]
        stmt = update(self.model).where(pk_col == record_id).values(**fields)
        await self.session.execute(stmt)
