from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import BackgroundTasks, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import NotFoundException
from src.core.services.conversation_service import ConversationService
from src.core.services.notification_service import NotificationService
from src.core.services.ticket_service import TicketService
from src.data.models.postgres.models import Notification
from src.data.repositories.ticket_repository import NotificationRepository, TicketRepository
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class CustomerConversationService:
    def __init__(
        self,
        session:          AsyncSession,
        background_tasks: BackgroundTasks,
    ) -> None:
        self._session          = session
        self._background_tasks = background_tasks
        self._ticket_svc       = TicketService(session)
        self._conv_svc         = ConversationService(session)
        self._ticket_repo      = TicketRepository(session)

    # ── Ownership guard ────────────────────────────────────────────────────────

    async def assert_customer_owns_ticket(
        self, ticket_id: str, customer_id: str
    ) -> None:
        try:
            await self._ticket_svc.get_my_ticket(
                ticket_id=ticket_id, customer_id=customer_id
            )
        except NotFoundException:
            raise HTTPException(status_code=404, detail="Ticket not found.")

    # ── Thread ─────────────────────────────────────────────────────────────────

    async def get_thread(self, ticket_id: str, customer_id: str):
        await self.assert_customer_owns_ticket(ticket_id, customer_id)
        conversations = await self._conv_svc.get_thread(
            ticket_id=ticket_id, include_internal=False
        )
        attachments = await self._conv_svc.get_attachments(ticket_id=ticket_id)
        return conversations, attachments

    # ── Reply ──────────────────────────────────────────────────────────────────

    async def post_reply(
        self,
        ticket_id:   str,
        customer_id: str,
        message:     str,
    ):
        if len(message.strip()) < 5:
            raise HTTPException(
                status_code=422, detail="Message must be at least 5 characters."
            )

        await self.assert_customer_owns_ticket(ticket_id, customer_id)

        ticket = await self._ticket_repo.get_by_id(ticket_id)

        if ticket and ticket.status == "closed":
            await self._ticket_repo.update_fields(ticket_id, {
                "status":       "reopened",
                "reopen_count": (ticket.reopen_count or 0) + 1,
                "closed_at":    None,
                "closed_by":    None,
            })
            await self._session.commit()

            logger.info(
                "ticket_reopened_by_customer_reply",
                ticket_id=ticket_id,
                ticket_number=ticket.ticket_number,
                reopen_count=(ticket.reopen_count or 0) + 1,
            )

            await self._notify_agent_ticket_reopened(ticket_id)

        convo = await self._conv_svc.customer_reply(
            ticket_id=ticket_id,
            customer_id=customer_id,
            content=message,
        )

        # Re-fetch so assignee reflects any reopen update above
        ticket = await self._ticket_repo.get_by_id(ticket_id)
        if ticket and ticket.assigned_to:
            notif_svc = NotificationService(self._session, self._background_tasks)
            await notif_svc.notify(
                recipient_id=str(ticket.assigned_to),
                ticket=ticket,
                notif_type="customer_reply",
                title=f"Customer replied on {ticket.ticket_number}",
                message=f"[{ticket.ticket_number}] Customer: {message[:200]}",
                is_internal=False,
                email_subject=f"[{ticket.ticket_number}] New customer reply",
                email_body=(
                    f"Hi,\n\nA customer has replied on ticket {ticket.ticket_number}.\n\n"
                    f"Message:\n{message}\n\n"
                    f"— Ticketing Genie"
                ),
            )
            logger.info(
                "agent_notified_customer_reply",
                ticket_id=ticket_id,
                agent_id=str(ticket.assigned_to),
            )

        return convo

    # ── Close ──────────────────────────────────────────────────────────────────

    async def close_ticket(self, ticket_id: str, customer_id: str) -> None:
        await self.assert_customer_owns_ticket(ticket_id, customer_id)

        ticket = await self._ticket_repo.get_by_id(ticket_id)
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found.")

        if ticket.status != "resolved":
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Ticket cannot be closed — current status is '{ticket.status}'. "
                    f"Only resolved tickets may be closed."
                ),
            )

        await self._ticket_repo.update_fields(ticket_id, {
            "status":    "closed",
            "closed_at": datetime.now(timezone.utc),
            "closed_by": uuid.UUID(customer_id),
        })
        await self._session.commit()

        logger.info(
            "ticket_closed_by_customer",
            ticket_id=ticket_id,
            ticket_number=ticket.ticket_number,
            customer_id=customer_id,
        )

    # ── Attachments ────────────────────────────────────────────────────────────

    async def upload_attachment(
        self,
        ticket_id:   str,
        customer_id: str,
        filename:    str,
        file_bytes:  bytes,
        mime_type:   str | None,
    ):
        await self.assert_customer_owns_ticket(ticket_id, customer_id)
        try:
            return await self._conv_svc.upload_attachment(
                ticket_id=ticket_id,
                uploader_id=customer_id,
                filename=filename,
                file_bytes=file_bytes,
                mime_type=mime_type,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

    async def get_attachment_signed_url(
        self,
        ticket_id:     str,
        customer_id:   str,
        attachment_id: str,
    ) -> str:
        await self.assert_customer_owns_ticket(ticket_id, customer_id)

        att = await self._conv_svc.get_attachment_by_id(attachment_id, ticket_id)
        if not att:
            raise HTTPException(status_code=404, detail="Attachment not found.")

        return self._conv_svc.get_download_url(att.file_path)

    # ── Private helpers ────────────────────────────────────────────────────────

    async def _notify_agent_ticket_reopened(self, ticket_id: str) -> None:
        try:
            ticket = await self._ticket_repo.get_by_id(ticket_id)
            if not ticket or not ticket.assigned_to:
                logger.info("reopen_notify_skipped_no_assignee", ticket_id=ticket_id)
                return

            agent_id      = str(ticket.assigned_to)
            notif_title   = f"Ticket {ticket.ticket_number} reopened"
            notif_message = (
                f"Customer replied to closed ticket {ticket.ticket_number}. "
                f"It has been automatically reopened and is assigned back to you."
            )

            notif_repo = NotificationRepository(self._session)
            await notif_repo.create(Notification(
                channel="in_app",
                recipient_id=ticket.assigned_to,
                ticket_id=ticket.id,
                is_internal=False,
                type="ticket_reopened",
                title=notif_title,
                message=notif_message,
            ))
            await self._session.commit()

            from src.core.sse.sse_manager import sse_manager
            await sse_manager.push_notification(
                actor_id=agent_id,
                notif_type="ticket_reopened",
                title=notif_title,
                message=notif_message,
                ticket_number=ticket.ticket_number,
                ticket_id=ticket_id,
            )

            logger.info(
                "agent_notified_ticket_reopened",
                agent_id=agent_id,
                ticket_id=ticket_id,
                ticket_number=ticket.ticket_number,
            )

        except Exception as exc:
            logger.warning(
                "notify_agent_ticket_reopened_failed",
                ticket_id=ticket_id,
                error=str(exc),
            )