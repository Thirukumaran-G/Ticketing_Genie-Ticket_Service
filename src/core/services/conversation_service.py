from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Attachment, Conversation
from src.data.repositories.conversation_repository import (
    AttachmentRepository,
    ConversationRepository,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# ── Attachment constraints ─────────────────────────────────────────────────────

ALLOWED_MIME_TYPES = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
    "application/pdf",
}

MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB


def _sanitise_filename(name: str) -> str:
    import re
    from pathlib import Path
    name = Path(name).name
    name = re.sub(r"[^\w.\- ]", "_", name)
    return name[:200]


class ConversationService:
    def __init__(self, session: AsyncSession) -> None:
        self._session   = session
        self._conv_repo = ConversationRepository(session)
        self._att_repo  = AttachmentRepository(session)

    # ── Thread retrieval ───────────────────────────────────────────────────────

    async def get_thread(
        self,
        ticket_id:        str,
        include_internal: bool = False,
    ) -> list[Conversation]:
        return await self._conv_repo.get_by_ticket(
            ticket_id=ticket_id,
            include_internal=include_internal,
        )

    async def get_attachments(self, ticket_id: str) -> list[Attachment]:
        return await self._att_repo.get_by_ticket(ticket_id)

    # ── Customer actions ───────────────────────────────────────────────────────

    async def customer_reply(
        self,
        ticket_id:   str,
        customer_id: str,
        content:     str,
    ) -> Conversation:
        convo = Conversation(
            ticket_id=uuid.UUID(ticket_id),
            author_id=uuid.UUID(customer_id),
            author_type="customer",
            content=content,
            is_internal=False,
            is_ai_draft=False,
        )
        convo = await self._conv_repo.create(convo)
        await self._session.commit()

        logger.info(
            "customer_reply_posted",
            ticket_id=ticket_id,
            conversation_id=str(convo.id),
            customer_id=customer_id,
        )
        return convo

    # ── Agent actions ──────────────────────────────────────────────────────────

    async def agent_reply(
        self,
        ticket_id:   str,
        agent_id:    str,
        content:     str,
        is_internal: bool = False,
    ) -> Conversation:
        convo = Conversation(
            ticket_id=uuid.UUID(ticket_id),
            author_id=uuid.UUID(agent_id),
            author_type="agent",
            content=content,
            is_internal=is_internal,
            is_ai_draft=False,
        )
        convo = await self._conv_repo.create(convo)
        await self._session.commit()

        logger.info(
            "agent_reply_posted",
            ticket_id=ticket_id,
            conversation_id=str(convo.id),
            agent_id=agent_id,
            is_internal=is_internal,
        )
        return convo

    # ── Attachment upload → GCS ────────────────────────────────────────────────

    async def upload_attachment(
        self,
        ticket_id:   str,
        uploader_id: str,
        filename:    str,
        file_bytes:  bytes,
        mime_type:   Optional[str] = None,
    ) -> Attachment:
        normalised_mime = (mime_type or "").lower().strip()

        if normalised_mime not in ALLOWED_MIME_TYPES:
            raise ValueError(
                f"File type '{normalised_mime}' is not allowed. "
                f"Allowed: JPEG, PNG, WEBP, PDF."
            )

        if len(file_bytes) > MAX_FILE_SIZE_BYTES:
            raise ValueError(
                f"File exceeds the 10 MB limit "
                f"({len(file_bytes) / 1024 / 1024:.1f} MB uploaded)."
            )

        safe_name = _sanitise_filename(filename)

        from src.core.services.gcs_service import upload_attachment as gcs_upload
        public_url = gcs_upload(
            file_bytes=file_bytes,
            filename=safe_name,
            folder=f"tickets/{ticket_id}",
            mime_type=normalised_mime,
        )

        attachment = Attachment(
            ticket_id=uuid.UUID(ticket_id),
            file_name=safe_name,
            file_path=public_url,
            file_size=len(file_bytes),
            mime_type=normalised_mime,
            uploaded_by=uuid.UUID(uploader_id),
        )
        attachment = await self._att_repo.create(attachment)
        await self._session.commit()

        logger.info(
            "attachment_uploaded_to_gcs",
            ticket_id=ticket_id,
            attachment_id=str(attachment.id),
            public_url=public_url,
            filename=safe_name,
            uploader_id=uploader_id,
        )
        return attachment

    # ── Attachment download ────────────────────────────────────────────────────

    def get_download_url(self, public_url: str, expiration_minutes: int = 30) -> str:
        from src.core.services.gcs_service import get_signed_url
        return get_signed_url(public_url, expiration_minutes=expiration_minutes)

    # ── Attachment retrieval ───────────────────────────────────────────────────

    async def get_attachment_by_id(
        self,
        attachment_id: str,
        ticket_id:     str,
    ) -> Optional[Attachment]:
        return await self._att_repo.get_by_id_and_ticket(
            attachment_id=attachment_id,
            ticket_id=ticket_id,
        )