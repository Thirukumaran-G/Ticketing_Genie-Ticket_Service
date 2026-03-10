"""
ConversationService — reply + attachment handling for both customer and agent.
src/core/services/conversation_service.py

Responsibilities:
  - Post a reply (customer or agent)
  - Upload an attachment (linked to a conversation or standalone on ticket)
  - Fetch conversation thread + attachments for a ticket
  - Serve an attachment file (returns local path for the route to stream)

Attachment storage:
  Files are saved to  UPLOAD_ROOT / tickets / {ticket_id} / {uuid}_{filename}
  UPLOAD_ROOT defaults to  ./uploads  (override via UPLOAD_ROOT env var).
  The stored file_path is the *relative* path from UPLOAD_ROOT so the app
  remains portable across environments.

Security:
  - Customers can only post to their own tickets (ticket ownership verified by caller).
  - Agents can only post to tickets assigned to them (verified by caller).
  - File extension is validated against an allowlist.
  - Filenames are sanitised before storage.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Attachment, Conversation, Ticket
from src.data.repositories.conversation_repository import (
    AttachmentRepository,
    ConversationRepository,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# ── Storage config ─────────────────────────────────────────────────────────────

UPLOAD_ROOT = Path(os.getenv("UPLOAD_ROOT", "uploads"))

ALLOWED_EXTENSIONS = {
    # images
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    # documents
    ".pdf", ".txt", ".md", ".csv",
    # office
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    # archives
    ".zip", ".tar", ".gz",
    # logs / code
    ".log", ".json", ".xml", ".yaml", ".yml",
}

MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB


def _sanitise_filename(name: str) -> str:
    """Strip path separators and dangerous characters, keep extension."""
    name = Path(name).name  # drop any directory component
    name = re.sub(r"[^\w.\- ]", "_", name)
    return name[:200]


def _ticket_upload_dir(ticket_id: str) -> Path:
    return UPLOAD_ROOT / "tickets" / ticket_id


# ── Service ────────────────────────────────────────────────────────────────────


class ConversationService:

    def __init__(self, session: AsyncSession) -> None:
        self._session  = session
        self._conv_repo = ConversationRepository(session)
        self._att_repo  = AttachmentRepository(session)

    # ── Thread retrieval ───────────────────────────────────────────────────────

    async def get_thread(
        self,
        ticket_id: str,
        include_internal: bool = False,
    ) -> list[Conversation]:
        """Return ordered conversation thread for a ticket."""
        return await self._conv_repo.get_by_ticket(
            ticket_id=ticket_id,
            include_internal=include_internal,
        )

    async def get_attachments(self, ticket_id: str) -> list[Attachment]:
        """Return all attachments for a ticket."""
        return await self._att_repo.get_by_ticket(ticket_id)

    # ── Customer actions ───────────────────────────────────────────────────────

    async def customer_reply(
        self,
        ticket_id: str,
        customer_id: str,
        content: str,
    ) -> Conversation:
        """Customer posts a reply on their ticket."""
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
        ticket_id: str,
        agent_id: str,
        content: str,
        is_internal: bool = False,
    ) -> Conversation:
        """
        Agent posts a reply or internal note.
        is_internal=True  → only visible to agents / team leads.
        is_internal=False → visible to the customer as well.
        """
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

    # ── Attachment upload ──────────────────────────────────────────────────────

    async def upload_attachment(
        self,
        ticket_id: str,
        uploader_id: str,
        filename: str,
        file_bytes: bytes,
        mime_type: Optional[str] = None,
    ) -> Attachment:
        """
        Persist file to disk and record metadata in the DB.

        Steps:
          1. Validate extension and size.
          2. Build a unique storage path.
          3. Write file to disk.
          4. Insert Attachment row.
          5. Commit.
        """
        safe_name = _sanitise_filename(filename)
        ext = Path(safe_name).suffix.lower()

        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(
                f"File type '{ext}' is not allowed. "
                f"Allowed types: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
            )

        if len(file_bytes) > MAX_FILE_SIZE_BYTES:
            raise ValueError(
                f"File exceeds the 25 MB limit "
                f"({len(file_bytes) / 1024 / 1024:.1f} MB uploaded)."
            )

        # Build storage path
        upload_dir = _ticket_upload_dir(ticket_id)
        upload_dir.mkdir(parents=True, exist_ok=True)
        unique_prefix = uuid.uuid4().hex[:8]
        stored_name   = f"{unique_prefix}_{safe_name}"
        abs_path      = upload_dir / stored_name
        # Store relative path so the app is portable
        rel_path      = str(abs_path.relative_to(UPLOAD_ROOT))

        # Write to disk
        abs_path.write_bytes(file_bytes)
        logger.info(
            "attachment_written",
            ticket_id=ticket_id,
            path=str(abs_path),
            size=len(file_bytes),
        )

        attachment = Attachment(
            ticket_id=uuid.UUID(ticket_id),
            file_name=safe_name,
            file_path=rel_path,
            file_size=len(file_bytes),
            mime_type=mime_type,
            uploaded_by=uuid.UUID(uploader_id),
        )
        attachment = await self._att_repo.create(attachment)
        await self._session.commit()

        logger.info(
            "attachment_created",
            ticket_id=ticket_id,
            attachment_id=str(attachment.id),
            filename=safe_name,
            uploader_id=uploader_id,
        )
        return attachment

    # ── Attachment retrieval (for streaming) ───────────────────────────────────

    def resolve_attachment_path(self, relative_path: str) -> Path:
        """Return the absolute filesystem path for an attachment."""
        return UPLOAD_ROOT / relative_path

    async def get_attachment_by_id(
        self,
        attachment_id: str,
        ticket_id: str,
    ) -> Optional[Attachment]:
        """Scoped fetch — returns None if not found or wrong ticket."""
        return await self._att_repo.get_by_id_and_ticket(
            attachment_id=attachment_id,
            ticket_id=ticket_id,
        )