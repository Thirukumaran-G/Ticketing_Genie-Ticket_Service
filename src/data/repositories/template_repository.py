"""
NotificationTemplateRepository — CRUD for notification_template table.
src/data/repositories/template_repository.py
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import NotificationTemplate
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class NotificationTemplateRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_all(self, active_only: bool = True) -> list[NotificationTemplate]:
        q = select(NotificationTemplate)
        if active_only:
            q = q.where(NotificationTemplate.is_active.is_(True))
        q = q.order_by(NotificationTemplate.created_at.asc())
        result = await self._session.execute(q)
        return list(result.scalars().all())

    async def get_by_id(self, template_id: str) -> Optional[NotificationTemplate]:
        result = await self._session.execute(
            select(NotificationTemplate).where(
                NotificationTemplate.id == uuid.UUID(template_id)
            )
        )
        return result.scalar_one_or_none()

    async def get_by_key(self, key: str) -> Optional[NotificationTemplate]:
        result = await self._session.execute(
            select(NotificationTemplate).where(
                NotificationTemplate.key == key
            )
        )
        return result.scalar_one_or_none()

    async def update(
        self,
        template_id: str,
        updated_by:  str,
        subject:     Optional[str] = None,
        body:        Optional[str] = None,
        name:        Optional[str] = None,
        is_active:   Optional[bool] = None,
    ) -> Optional[NotificationTemplate]:
        tpl = await self.get_by_id(template_id)
        if not tpl:
            return None

        if subject   is not None: tpl.subject    = subject
        if body      is not None: tpl.body       = body
        if name      is not None: tpl.name       = name
        if is_active is not None: tpl.is_active  = is_active
        tpl.updated_by = uuid.UUID(updated_by)

        logger.info(
            "notification_template_updated",
            template_id=template_id,
            updated_by=updated_by,
        )
        return tpl