"""
ConversationRepository + AttachmentRepository
src/data/repositories/conversation_repository.py
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Attachment, Conversation


class ConversationRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, conversation: Conversation) -> Conversation:
        self._s.add(conversation)
        await self._s.flush()
        await self._s.refresh(conversation)
        return conversation

    async def get_by_ticket(
        self,
        ticket_id: str,
        include_internal: bool = False,
    ) -> list[Conversation]:
        """
        Return all conversations for a ticket, oldest first.
        - Customers never see internal notes (is_internal=True).
        - Agents see everything (include_internal=True).
        - AI drafts (is_ai_draft=True) are excluded from the thread; they are
          surfaced separately on the ticket row itself.
        """
        q = (
            select(Conversation)
            .where(
                Conversation.ticket_id == uuid.UUID(ticket_id),
                Conversation.is_ai_draft.is_(False),
            )
            .order_by(Conversation.created_at.asc())
        )
        if not include_internal:
            q = q.where(Conversation.is_internal.is_(False))
        r = await self._s.execute(q)
        return list(r.scalars().all())

    async def get_by_id(self, conversation_id: str) -> Optional[Conversation]:
        r = await self._s.execute(
            select(Conversation).where(
                Conversation.id == uuid.UUID(conversation_id)
            )
        )
        return r.scalar_one_or_none()


class AttachmentRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, attachment: Attachment) -> Attachment:
        self._s.add(attachment)
        await self._s.flush()
        await self._s.refresh(attachment)
        return attachment

    async def get_by_ticket(self, ticket_id: str) -> list[Attachment]:
        r = await self._s.execute(
            select(Attachment)
            .where(Attachment.ticket_id == uuid.UUID(ticket_id))
            .order_by(Attachment.created_at.asc())
        )
        return list(r.scalars().all())

    async def get_by_id(self, attachment_id: str) -> Optional[Attachment]:
        r = await self._s.execute(
            select(Attachment).where(
                Attachment.id == uuid.UUID(attachment_id)
            )
        )
        return r.scalar_one_or_none()

    async def get_by_id_and_ticket(
        self,
        attachment_id: str,
        ticket_id: str,
    ) -> Optional[Attachment]:
        """Scoped fetch — prevents cross-ticket attachment enumeration."""
        r = await self._s.execute(
            select(Attachment).where(
                Attachment.id == uuid.UUID(attachment_id),
                Attachment.ticket_id == uuid.UUID(ticket_id),
            )
        )
        return r.scalar_one_or_none()