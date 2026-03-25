from __future__ import annotations

from datetime import datetime, timezone

from fastapi import BackgroundTasks, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.services.agent_services import AgentService
from src.core.services.conversation_service import ConversationService
from src.core.services.notification_service import NotificationService
from src.data.repositories.ticket_repository import TicketRepository
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class AgentConversationService:
    """
    Business logic for all agent conversation actions.

    Owns:
      - agent ownership guard
      - reply + ticket status progression (assigned → in_progress)
      - first_response_at stamping
      - customer notification dispatch
      - attachment upload / download
    """

    def __init__(
        self,
        session:          AsyncSession,
        background_tasks: BackgroundTasks,
    ) -> None:
        self._session          = session
        self._background_tasks = background_tasks
        self._agent_svc        = AgentService(session, background_tasks)
        self._conv_svc         = ConversationService(session)
        self._ticket_repo      = TicketRepository(session)

    # ── Ownership guard ────────────────────────────────────────────────────────

    async def assert_agent_owns_ticket(
        self, ticket_id: str, agent_id: str
    ) -> None:
        try:
            await self._agent_svc.get_ticket_by_id(
                agent_id=agent_id, ticket_id=ticket_id
            )
        except ValueError:
            raise HTTPException(status_code=404, detail="Ticket not found.")

    # ── Thread ─────────────────────────────────────────────────────────────────

    async def get_thread(self, ticket_id: str, agent_id: str):
        await self.assert_agent_owns_ticket(ticket_id, agent_id)
        conversations = await self._conv_svc.get_thread(
            ticket_id=ticket_id, include_internal=True
        )
        attachments = await self._conv_svc.get_attachments(ticket_id=ticket_id)
        return conversations, attachments

    # ── Comment / reply ────────────────────────────────────────────────────────

    async def post_comment(
        self,
        ticket_id:   str,
        agent_id:    str,
        content:     str,
        is_internal: bool,
    ):
        if len(content.strip()) < 1:
            raise HTTPException(status_code=422, detail="Content cannot be empty.")

        await self.assert_agent_owns_ticket(ticket_id, agent_id)

        convo = await self._conv_svc.agent_reply(
            ticket_id=ticket_id,
            agent_id=agent_id,
            content=content,
            is_internal=is_internal,
        )

        if not is_internal:
            await self._handle_external_reply(ticket_id, content)

        return convo

    async def _handle_external_reply(self, ticket_id: str, content: str) -> None:
        """
        After an external (customer-visible) agent reply:
          1. Progress ticket status from 'assigned' → 'in_progress'.
          2. Stamp first_response_at if not already set.
          3. Notify the customer.
        """
        ticket = await self._ticket_repo.get_by_id(ticket_id)
        if not ticket:
            return

        updates: dict = {}
        if ticket.status in ("assigned",):
            updates["status"] = "in_progress"
        if not ticket.first_response_at:
            updates["first_response_at"] = datetime.now(timezone.utc)
        if updates:
            await self._ticket_repo.update_fields(ticket_id, updates)

        notif_svc = NotificationService(self._session, self._background_tasks)
        await notif_svc.notify(
            recipient_id=str(ticket.customer_id),
            ticket=ticket,
            notif_type="agent_reply",
            title=f"New reply on ticket {ticket.ticket_number}",
            message=f"[{ticket.ticket_number}] Agent: {content[:200]}",
            is_internal=False,
            email_subject=f"[{ticket.ticket_number}] New reply from your support agent",
            email_body=(
                f"Hi,\n\nYour support agent has posted a reply on ticket "
                f"{ticket.ticket_number}.\n\n"
                f"Reply:\n{content}\n\n"
                f"— Ticketing Genie Support Team"
            ),
        )

    # ── Attachments ────────────────────────────────────────────────────────────

    async def upload_attachment(
        self,
        ticket_id:  str,
        agent_id:   str,
        filename:   str,
        file_bytes: bytes,
        mime_type:  str | None,
    ):
        await self.assert_agent_owns_ticket(ticket_id, agent_id)
        try:
            return await self._conv_svc.upload_attachment(
                ticket_id=ticket_id,
                uploader_id=agent_id,
                filename=filename,
                file_bytes=file_bytes,
                mime_type=mime_type,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

    async def get_attachment_signed_url(
        self,
        ticket_id:     str,
        agent_id:      str,
        attachment_id: str,
    ) -> str:
        await self.assert_agent_owns_ticket(ticket_id, agent_id)

        att = await self._conv_svc.get_attachment_by_id(attachment_id, ticket_id)
        if not att:
            raise HTTPException(status_code=404, detail="Attachment not found.")

        return self._conv_svc.get_download_url(att.file_path)