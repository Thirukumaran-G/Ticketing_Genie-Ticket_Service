from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from src.constants import TicketStatus
from src.core.exceptions.base import NotFoundException
from src.core.services.audit_service import audit_service
from src.data.models.postgres.models import Ticket
from src.data.repositories.ticket_repository import TicketRepository
from src.observability.logging.logger import get_logger
from src.schemas.ticket_schema import TicketCreateRequest

logger = get_logger(__name__)


class TicketService:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repo    = TicketRepository(session)

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
                product_id=payload.product_id,                        # user-selected product
                customer_id=uuid.UUID(customer_id),
                company_id=uuid.UUID(company_id) if company_id else None,  # from JWT
                tier_snapshot=tier_snapshot,                           # actual tier, may be None
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

    # ── Customer self-service views ───────────────────────────────────────────

    async def list_my_tickets(
        self,
        customer_id: str,
        status:      str | None = None,
        limit:       int = 50,
        offset:      int = 0,
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

    # ── Background task dispatch ──────────────────────────────────────────────

    def _fire_background(
        self,
        ticket:         Ticket,
        customer_email: str | None,
        customer_id:    str,
        payload:        TicketCreateRequest,
    ) -> None:
        import asyncio

        # Audit log
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

        # Ack email
        try:
            from src.core.celery.workers.email_worker import send_acknowledgement_email
            send_acknowledgement_email.delay(
                customer_email=customer_email or "",
                ticket_number=ticket.ticket_number,
                ticket_id=str(ticket.id),
            )
        except Exception as exc:
            logger.error("ack_email_dispatch_failed", ticket_id=str(ticket.id), error=str(exc))

        # AI classification — chains auto_assign + draft inside
        try:
            from src.core.celery.workers.ai_worker import run_ai_classification
            run_ai_classification.delay(
                ticket_id=str(ticket.id),
                customer_email=customer_email,
            )
        except Exception as exc:
            logger.error("ai_classification_dispatch_failed", ticket_id=str(ticket.id), error=str(exc))

        logger.info("ticket_background_tasks_fired", ticket_id=str(ticket.id))