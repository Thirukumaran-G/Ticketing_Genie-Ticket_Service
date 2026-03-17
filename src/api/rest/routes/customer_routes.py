"""
Customer ticket routes.
src/api/rest/routes/customer_ticket_routes.py
"""
from __future__ import annotations

import uuid as _uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_CUSTOMER, require_role
from src.core.services.conversation_service import ConversationService
from src.core.services.notification_service import NotificationService
from src.core.services.ticket_service import TicketService
from src.data.clients.postgres_client import get_db_session
from src.data.repositories.ticket_repository import TicketRepository
from src.schemas.ticket_schema import (
    CustomerTicketDetailResponse,
    CustomerTicketListItem,
    TicketCreateRequest,
    TicketCreateResponse,
)

router = APIRouter(prefix="/customer", tags=["Customer — Tickets"])

_CustomerActor = Depends(require_role(ROLE_CUSTOMER))


def _ticket_svc(session: AsyncSession = Depends(get_db_session)) -> TicketService:
    return TicketService(session)


def _conv_svc(session: AsyncSession = Depends(get_db_session)) -> ConversationService:
    return ConversationService(session)


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("/tickets", response_model=TicketCreateResponse, status_code=201)
async def create_ticket(
    title:             str           = Form(..., min_length=5, max_length=500),
    description:       str           = Form(..., min_length=10),
    product_id:        str           = Form(...),
    customer_severity: str           = Form(..., pattern="^(critical|high|medium|low)$"),
    environment:       Optional[str] = Form(default=None, pattern="^(production|staging|development|other)$"),
    source:            Optional[str] = Form(default="web"),
    files:             List[UploadFile] = File(default=[]),
    actor:             CurrentActor  = _CustomerActor,
    service:           TicketService = Depends(_ticket_svc),
    conv_service:      ConversationService = Depends(_conv_svc),
) -> TicketCreateResponse:
    real_files = [f for f in files if f.filename and f.filename.strip()]
    if len(real_files) > 5:
        raise HTTPException(status_code=422, detail="Maximum 5 attachments per ticket.")

    try:
        product_uuid = _uuid.UUID(product_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid product_id — must be a valid UUID.")

    payload = TicketCreateRequest(
        title=title, description=description, product_id=product_uuid,
        customer_severity=customer_severity, environment=environment,
        source=source or "web",
    )
    tier_snapshot = actor.get_tier_name_for_product(product_id)
    ticket = await service.create_ticket(
        payload=payload, customer_id=actor.actor_id,
        company_id=actor.company_id, tier_snapshot=tier_snapshot,
        customer_email=actor.email,
    )

    for upload in real_files:
        try:
            file_bytes = await upload.read()
            await conv_service.upload_attachment(
                ticket_id=str(ticket.id), uploader_id=actor.actor_id,
                filename=upload.filename or "upload",
                file_bytes=file_bytes, mime_type=upload.content_type,
            )
        except Exception:
            pass

    return TicketCreateResponse(
        ticket_id=ticket.id,
        ticket_number=ticket.ticket_number,
        status=ticket.status,
    )


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("/tickets", response_model=list[CustomerTicketListItem])
async def list_my_tickets(
    status:  str | None  = Query(default=None),
    limit:   int         = Query(default=20, ge=1, le=100),
    offset:  int         = Query(default=0, ge=0),
    actor:   CurrentActor  = _CustomerActor,
    service: TicketService = Depends(_ticket_svc),
) -> list[CustomerTicketListItem]:
    tickets = await service.list_my_tickets(
        customer_id=actor.actor_id, status=status, limit=limit, offset=offset,
    )
    return [CustomerTicketListItem.model_validate(t) for t in tickets]


# ── Detail ────────────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}", response_model=CustomerTicketDetailResponse)
async def get_my_ticket(
    ticket_id: str,
    actor:     CurrentActor  = _CustomerActor,
    service:   TicketService = Depends(_ticket_svc),
) -> CustomerTicketDetailResponse:
    ticket = await service.get_my_ticket(ticket_id=ticket_id, customer_id=actor.actor_id)
    return CustomerTicketDetailResponse.model_validate(ticket)


# ── Agent info ────────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}/agent")
async def get_ticket_agent(
    ticket_id: str,
    actor:     CurrentActor  = _CustomerActor,
    service:   TicketService = Depends(_ticket_svc),
) -> dict:
    ticket = await service.get_my_ticket(ticket_id=ticket_id, customer_id=actor.actor_id)
    if not ticket.assigned_to:
        return {"assigned": False, "agent_name": None}
    info = await service.get_assigned_agent_name(str(ticket.assigned_to))
    return {"assigned": True, "agent_name": info.get("full_name", "Support Agent") if info else "Support Agent"}


# ── Close ticket ───────────────────────────────────────────────────────────────

@router.patch(
    "/tickets/{ticket_id}/close",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Customer — close a resolved ticket",
)
async def customer_close_ticket(
    ticket_id:        str,
    background_tasks: BackgroundTasks,
    actor:            CurrentActor  = _CustomerActor,
    ticket_svc:       TicketService = Depends(_ticket_svc),
    session:          AsyncSession  = Depends(get_db_session),
) -> None:
    from src.core.exceptions.base import NotFoundException
    try:
        await ticket_svc.get_my_ticket(ticket_id=ticket_id, customer_id=actor.actor_id)
    except NotFoundException:
        raise HTTPException(status_code=404, detail="Ticket not found.")

    ticket_repo = TicketRepository(session)
    ticket      = await ticket_repo.get_by_id(ticket_id)

    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found.")
    if ticket.status != "resolved":
        raise HTTPException(
            status_code=422,
            detail=f"Ticket cannot be closed — current status is '{ticket.status}'. Only resolved tickets may be closed.",
        )

    await ticket_repo.update_fields(ticket_id, {
        "status":    "closed",
        "closed_at": datetime.now(timezone.utc),
        "closed_by": _uuid.UUID(actor.actor_id),
    })
    await session.commit()

    # Notify assigned agent that customer closed ticket per agent's preference
    if ticket.assigned_to:
        notif_svc = NotificationService(session, background_tasks)
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


