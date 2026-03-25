from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import BackgroundTasks, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.constants import TicketStatus
from src.core.exceptions.base import NotFoundException
from src.core.services.audit_service import audit_service
from src.core.services.conversation_service import ConversationService
from src.core.services.notification_service import NotificationService
from src.data.models.postgres.models import Ticket
from src.data.repositories.ticket_repository import TicketRepository
from src.observability.logging.logger import get_logger
from src.schemas.ticket_schema import (
    CustomerTicketDetailResponse,
    CustomerTicketListItem,
    TicketCreateRequest,
    TicketCreateResponse,
)

logger = get_logger(__name__)


class TicketService:

    def __init__(
        self,
        session:          AsyncSession,
        background_tasks: BackgroundTasks | None = None,
    ) -> None:
        self._session          = session
        self._background_tasks = background_tasks
        self._repo             = TicketRepository(session)
        self._conv_svc         = ConversationService(session)

    # ── Create ────────────────────────────────────────────────────────────────

    async def create_ticket(
        self,
        payload:        TicketCreateRequest,
        customer_id:    str,
        company_id:     str | None,
        tier_snapshot:  str | None,
        customer_email: str | None,
    ) -> Ticket:
        try:
            ticket_number = await self._repo.ticket_number()
            ticket = Ticket(
                ticket_number=ticket_number,
                title=payload.title,
                description=payload.description,
                status=TicketStatus.NEW.value,
                source="web",
                environment=payload.environment,
                product_id=payload.product_id,
                customer_id=uuid.UUID(customer_id),
                company_id=uuid.UUID(company_id) if company_id else None,
                tier_snapshot=tier_snapshot,
                customer_priority=payload.customer_severity,
            )
            await self._repo.create(ticket)
            await self._session.commit()
        except Exception as exc:
            logger.error(
                "ticket_create_failed",
                customer_id=customer_id,
                product_id=str(payload.product_id),
                error=str(exc),
            )
            raise

        logger.info(
            "ticket_created",
            ticket_id=str(ticket.id),
            ticket_number=ticket_number,
            customer_id=customer_id,
            product_id=str(payload.product_id),
            company_id=str(company_id) if company_id else None,
            tier=tier_snapshot,
        )

        self._fire_background(ticket, customer_email, customer_id, payload)
        return ticket

    async def create_ticket_with_attachments(
        self,
        title:             str,
        description:       str,
        product_id:        str,
        customer_severity: str,
        customer_id:       str,
        company_id:        str | None,
        tier_snapshot:     str | None,
        customer_email:    str,
        environment:       str | None = None,
        source:            str        = "web",
        file_uploads:      list[tuple[str, bytes, str | None]] | None = None,
    ) -> TicketCreateResponse:
        """
        Customer-facing ticket creation — validates product_id UUID, creates
        the ticket, then uploads any attachments (failures are swallowed so they
        never block ticket creation).

        file_uploads: list of (filename, file_bytes, mime_type) tuples.
        Max-5 enforcement is the router's responsibility.
        """
        try:
            product_uuid = uuid.UUID(product_id)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="Invalid product_id — must be a valid UUID.",
            )

        payload = TicketCreateRequest(
            title=title,
            description=description,
            product_id=product_uuid,
            customer_severity=customer_severity,
            environment=environment,
            source=source,
        )

        ticket = await self.create_ticket(
            payload=payload,
            customer_id=customer_id,
            company_id=company_id,
            tier_snapshot=tier_snapshot,
            customer_email=customer_email,
        )

        for filename, file_bytes, mime_type in (file_uploads or []):
            try:
                await self._conv_svc.upload_attachment(
                    ticket_id=str(ticket.id),
                    uploader_id=customer_id,
                    filename=filename,
                    file_bytes=file_bytes,
                    mime_type=mime_type,
                )
            except Exception:
                pass  # attachment failure must never block ticket creation

        return TicketCreateResponse(
            ticket_id=ticket.id,
            ticket_number=ticket.ticket_number,
            status=ticket.status,
        )

    # ── Customer self-service views ───────────────────────────────────────────

    async def list_my_tickets(
        self,
        customer_id: str,
        status:      str | None = None,
        limit:       int        = 50,
        offset:      int        = 0,
    ) -> list[Ticket]:
        try:
            tickets = await self._repo.get_by_customer(
                customer_id=customer_id,
                status=status,
                limit=limit,
                offset=offset,
            )
        except Exception as exc:
            logger.error("list_my_tickets_failed", customer_id=customer_id, error=str(exc))
            raise

        logger.info(
            "customer_tickets_listed",
            customer_id=customer_id,
            count=len(tickets),
            status_filter=status,
        )
        return tickets

    async def get_my_ticket(
        self,
        ticket_id:   str,
        customer_id: str,
    ) -> Ticket:
        try:
            ticket = await self._repo.get_by_id_and_customer(
                ticket_id=ticket_id,
                customer_id=customer_id,
            )
        except Exception as exc:
            logger.error(
                "get_my_ticket_failed",
                ticket_id=ticket_id,
                customer_id=customer_id,
                error=str(exc),
            )
            raise

        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found.")

        logger.info("customer_ticket_viewed", ticket_id=ticket_id, customer_id=customer_id)
        return ticket

    # ── Agent info ────────────────────────────────────────────────────────────

    async def get_ticket_agent(
        self, ticket_id: str, customer_id: str
    ) -> dict:
        ticket = await self.get_my_ticket(ticket_id=ticket_id, customer_id=customer_id)
        if not ticket.assigned_to:
            return {"assigned": False, "agent_name": None}

        info = await self.get_assigned_agent_name(str(ticket.assigned_to))
        return {
            "assigned":   True,
            "agent_name": info.get("full_name", "Support Agent") if info else "Support Agent",
        }

    async def get_assigned_agent_name(self, agent_id: str) -> dict | None:
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            auth = AuthHttpClient()
            user = await auth.get_user_by_id(agent_id)
            if not user:
                return None
            return {"full_name": user.get("full_name") or "Support Agent"}
        except Exception as exc:
            logger.error(
                "get_assigned_agent_name_failed",
                agent_id=agent_id,
                error=str(exc),
            )
            return None

    # ── Close ─────────────────────────────────────────────────────────────────

    async def close_ticket(self, ticket_id: str, customer_id: str) -> None:
        try:
            await self.get_my_ticket(ticket_id=ticket_id, customer_id=customer_id)
        except NotFoundException:
            raise HTTPException(status_code=404, detail="Ticket not found.")

        ticket = await self._repo.get_by_id(ticket_id)
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

        await self._repo.update_fields(ticket_id, {
            "status":    "closed",
            "closed_at": datetime.now(timezone.utc),
            "closed_by": uuid.UUID(customer_id),
        })
        await self._session.commit()

        if ticket.assigned_to:
            notif_svc = NotificationService(self._session, self._background_tasks)
            await notif_svc.notify(
                recipient_id=str(ticket.assigned_to),
                ticket=ticket,
                notif_type="ticket_closed_by_customer",
                title=f"Ticket {ticket.ticket_number} closed by customer",
                message=f"The customer has closed ticket {ticket.ticket_number}.",
                is_internal=True,
                email_subject=f"[{ticket.ticket_number}] Closed by customer",
                email_body=(
                    f"Hi,\n\nThe customer has closed ticket {ticket.ticket_number}.\n\n"
                    f"No further action is required unless the customer reopens it.\n\n"
                    f"— Ticketing Genie"
                ),
            )
            logger.info(
                "agent_notified_ticket_closed_by_customer",
                ticket_id=ticket_id,
                ticket_number=ticket.ticket_number,
                agent_id=str(ticket.assigned_to),
                customer_id=customer_id,
            )

    # ── Background task dispatch ──────────────────────────────────────────────

    def _fire_background(
        self,
        ticket:         Ticket,
        customer_email: str | None,
        customer_id:    str,
        payload:        TicketCreateRequest,
    ) -> None:
        import asyncio

        try:
            loop = asyncio.get_event_loop()
            loop.create_task(
                audit_service.log(
                    entity_type="ticket",
                    entity_id=ticket.id,
                    action="ticket_created",
                    actor_id=uuid.UUID(customer_id),
                    actor_type="customer",
                    new_value={
                        "ticket_number":     ticket.ticket_number,
                        "title":             payload.title,
                        "source":            "web",
                        "environment":       payload.environment,
                        "product_id":        str(payload.product_id),
                        "company_id":        str(ticket.company_id) if ticket.company_id else None,
                        "tier_snapshot":     ticket.tier_snapshot,
                        "customer_severity": payload.customer_severity,
                    },
                    ticket_id=ticket.id,
                )
            )
        except Exception as exc:
            logger.warning("audit_log_dispatch_failed", ticket_id=str(ticket.id), error=str(exc))

        try:
            from src.core.celery.workers.ai_worker import run_ai_classification
            run_ai_classification.delay(
                ticket_id=str(ticket.id),
                customer_email=customer_email,
            )
        except Exception as exc:
            logger.error(
                "ai_classification_dispatch_failed",
                ticket_id=str(ticket.id),
                error=str(exc),
            )

        logger.info("ticket_background_tasks_fired", ticket_id=str(ticket.id))