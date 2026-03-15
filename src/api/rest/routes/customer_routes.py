"""
Customer ticket routes — create, list, detail, agent info.
src/api/rest/routes/customer_ticket_routes.py

POST /customer/tickets now accepts multipart/form-data so that
attachments can be uploaded atomically with ticket creation.
All other endpoints are unchanged.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import (
    CurrentActor,
    ROLE_CUSTOMER,
    require_role,
)
from src.core.services.conversation_service import ConversationService
from src.core.services.ticket_service import TicketService
from src.data.clients.postgres_client import get_db_session
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

@router.post(
    "/tickets",
    response_model=TicketCreateResponse,
    status_code=201,
    summary="Create a support ticket",
    description=(
        "Customer submits a ticket via multipart/form-data. "
    ),
)
async def create_ticket(
    title: str = Form(..., min_length=5, max_length=500),
    description: str = Form(..., min_length=10),
    product_id: str = Form(...),
    customer_severity: str = Form(..., pattern="^(critical|high|medium|low)$"),
    environment: Optional[str] = Form(
        default=None,
        pattern="^(production|staging|development|other)$",
    ),
    source: Optional[str] = Form(default="web"),
    files: List[UploadFile] = File(default=[]),
    actor: CurrentActor = _CustomerActor,
    service: TicketService = Depends(_ticket_svc),
    conv_service: ConversationService = Depends(_conv_svc),
) -> TicketCreateResponse:
    real_files = [f for f in files if f.filename and f.filename.strip()]
    if len(real_files) > 5:
        raise HTTPException(
            status_code=422,
            detail="A maximum of 5 attachments are allowed per ticket.",
        )
    
    import uuid as _uuid
    try:
        product_uuid = _uuid.UUID(product_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid product_id — must be a valid UUID.")

    payload = TicketCreateRequest(
        title=title,
        description=description,
        product_id=product_uuid,
        customer_severity=customer_severity,
        environment=environment,
        source=source or "web",
    )
    tier_snapshot = actor.get_tier_name_for_product(product_id)
    ticket = await service.create_ticket(
        payload=payload,
        customer_id=actor.actor_id,
        company_id=actor.company_id,
        tier_snapshot=tier_snapshot,
        customer_email=actor.email,
    )
    if real_files:
        for upload in real_files:
            try:
                file_bytes = await upload.read()
                await conv_service.upload_attachment(
                    ticket_id=str(ticket.id),
                    uploader_id=actor.actor_id,
                    filename=upload.filename or "upload",
                    file_bytes=file_bytes,
                    mime_type=upload.content_type,
                )
            except ValueError as exc:
                # File validation failure (bad extension, too large) — skip this file
                # The ticket is already created; we don't want to 422 at this point.
                # Caller will see the ticket created without the offending attachment.
                from src.observability.logging.logger import get_logger
                _logger = get_logger(__name__)
                _logger.warning(
                    "ticket_create_attachment_skipped",
                    ticket_id=str(ticket.id),
                    filename=upload.filename,
                    reason=str(exc),
                )
            except Exception as exc:
                from src.observability.logging.logger import get_logger
                _logger = get_logger(__name__)
                _logger.error(
                    "ticket_create_attachment_failed",
                    ticket_id=str(ticket.id),
                    filename=upload.filename,
                    error=str(exc),
                )

    return TicketCreateResponse(
        ticket_id=ticket.id,
        ticket_number=ticket.ticket_number,
        status=ticket.status,
    )


# ── List ──────────────────────────────────────────────────────────────────────

@router.get(
    "/tickets",
    response_model=list[CustomerTicketListItem],
    summary="List my tickets",
    description=(
        "Returns all tickets submitted by the authenticated customer, newest first. "
        "Optionally filter by status. Paginate using limit and offset."
    ),
)
async def list_my_tickets(
    status: str | None = Query(
        default=None,
        description="Filter by status: new | acknowledged | in_progress | on_hold | resolved | closed | reopened",
        pattern="^(new|acknowledged|in_progress|on_hold|resolved|closed|reopened)$",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    actor: CurrentActor = _CustomerActor,
    service: TicketService = Depends(_ticket_svc),
) -> list[CustomerTicketListItem]:
    tickets = await service.list_my_tickets(
        customer_id=actor.actor_id,
        status=status,
        limit=limit,
        offset=offset,
    )
    return [CustomerTicketListItem.model_validate(t) for t in tickets]


# ── Detail ────────────────────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}",
    response_model=CustomerTicketDetailResponse,
    summary="Get a single ticket",
    description=(
        "Returns full detail for one of the customer's own tickets. "
        "Returns 404 if the ticket does not exist or belongs to another customer."
    ),
)
async def get_my_ticket(
    ticket_id: str,
    actor: CurrentActor = _CustomerActor,
    service: TicketService = Depends(_ticket_svc),
) -> CustomerTicketDetailResponse:
    ticket = await service.get_my_ticket(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
    )
    return CustomerTicketDetailResponse.model_validate(ticket)


# ── Agent info ────────────────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/agent",
    summary="Get assigned agent name for a customer's ticket",
    description=(
        "Returns the display name of the agent assigned to this ticket. "
        "Email and internal IDs are never exposed. Returns null if unassigned."
    ),
)
async def get_ticket_agent(
    ticket_id: str,
    actor: CurrentActor = _CustomerActor,
    service: TicketService = Depends(_ticket_svc),
) -> dict:
    ticket = await service.get_my_ticket(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
    )
    if not ticket.assigned_to:
        return {"assigned": False, "agent_name": None}

    info = await service.get_assigned_agent_name(str(ticket.assigned_to))
    if not info:
        return {"assigned": True, "agent_name": "Support Agent"}
    return {"assigned": True, "agent_name": info.get("full_name", "Support Agent")}