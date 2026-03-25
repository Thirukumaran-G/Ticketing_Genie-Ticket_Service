from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_CUSTOMER, require_role
from src.data.clients.postgres_client import get_db_session
from src.schemas.ticket_schema import (
    CustomerTicketDetailResponse,
    CustomerTicketListItem,
    TicketCreateResponse,
)
from src.core.services.ticket_service import TicketService

router = APIRouter(prefix="/customer", tags=["Customer — Tickets"])

_CustomerActor = Depends(require_role(ROLE_CUSTOMER))


def _svc(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> TicketService:
    return TicketService(session, background_tasks)


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("/tickets", response_model=TicketCreateResponse, status_code=201)
async def create_ticket(
    title:             str              = Form(..., min_length=5, max_length=500),
    description:       str              = Form(..., min_length=10),
    product_id:        str              = Form(...),
    customer_severity: str              = Form(..., pattern="^(critical|high|medium|low)$"),
    environment:       Optional[str]    = Form(default=None, pattern="^(production|staging|development|other)$"),
    source:            Optional[str]    = Form(default="web"),
    files:             List[UploadFile] = File(default=[]),
    actor:             CurrentActor     = _CustomerActor,
    svc:               TicketService    = Depends(_svc),
) -> TicketCreateResponse:
    real_files = [f for f in files if f.filename and f.filename.strip()]
    if len(real_files) > 5:
        raise HTTPException(status_code=422, detail="Maximum 5 attachments per ticket.")

    file_uploads = [
        (f.filename or "upload", await f.read(), f.content_type)
        for f in real_files
    ]

    return await svc.create_ticket_with_attachments(
        title=title,
        description=description,
        product_id=product_id,
        customer_severity=customer_severity,
        customer_id=actor.actor_id,
        company_id=actor.company_id,
        tier_snapshot=actor.get_tier_name_for_product(product_id),
        customer_email=actor.email,
        environment=environment,
        source=source or "web",
        file_uploads=file_uploads,
    )


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("/tickets", response_model=list[CustomerTicketListItem])
async def list_my_tickets(
    status:  str | None  = Query(default=None),
    limit:   int         = Query(default=20, ge=1, le=100),
    offset:  int         = Query(default=0, ge=0),
    actor:   CurrentActor = _CustomerActor,
    svc:     TicketService = Depends(_svc),
) -> list[CustomerTicketListItem]:
    tickets = await svc.list_my_tickets(
        customer_id=actor.actor_id,
        status=status,
        limit=limit,
        offset=offset,
    )
    return [CustomerTicketListItem.model_validate(t) for t in tickets]


# ── Detail ────────────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}", response_model=CustomerTicketDetailResponse)
async def get_my_ticket(
    ticket_id: str,
    actor:     CurrentActor = _CustomerActor,
    svc:       TicketService = Depends(_svc),
) -> CustomerTicketDetailResponse:
    ticket = await svc.get_my_ticket(ticket_id=ticket_id, customer_id=actor.actor_id)
    return CustomerTicketDetailResponse.model_validate(ticket)


# ── Agent info ────────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}/agent")
async def get_ticket_agent(
    ticket_id: str,
    actor:     CurrentActor = _CustomerActor,
    svc:       TicketService = Depends(_svc),
) -> dict:
    return await svc.get_ticket_agent(ticket_id=ticket_id, customer_id=actor.actor_id)


# ── Close ──────────────────────────────────────────────────────────────────────

@router.patch(
    "/tickets/{ticket_id}/close",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Customer — close a resolved ticket",
)
async def customer_close_ticket(
    ticket_id: str,
    actor:     CurrentActor = _CustomerActor,
    svc:       TicketService = Depends(_svc),
) -> None:
    await svc.close_ticket(ticket_id=ticket_id, customer_id=actor.actor_id)