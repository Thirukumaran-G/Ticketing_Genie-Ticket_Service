from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import (
    CurrentActor,
    ROLE_CUSTOMER,
    require_role,
)
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


# ── Create ────────────────────────────────────────────────────────────────────

@router.post(
    "/tickets",
    response_model=TicketCreateResponse,
    status_code=201,
    summary="Create a support ticket",
    description=(
        "Customer submits a ticket. Returns immediately with ticket number. "
        "AI classification, SLA assignment, draft generation, and agent auto-assign "
        "run asynchronously in the background."
    ),
)
async def create_ticket(
    payload: TicketCreateRequest,
    actor: CurrentActor = _CustomerActor,
    service: TicketService = Depends(_ticket_svc),
) -> TicketCreateResponse:
    # Derive tier_snapshot from the JWT product_tiers for the selected product.
    # No fallback — if product not in JWT tiers, tier_snapshot is None.
    tier_snapshot = actor.get_tier_name_for_product(str(payload.product_id))

    ticket = await service.create_ticket(
        payload=payload,
        customer_id=actor.actor_id,
        company_id=actor.company_id,        # from JWT company_id claim directly
        tier_snapshot=tier_snapshot,         # actual tier for this product, never "starter"
        customer_email=actor.email,
    )
    return TicketCreateResponse(
        ticket_id=ticket.id,
        ticket_number=ticket.ticket_number,
        status=ticket.status,
    )

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
    limit: int  = Query(default=20, ge=1, le=100),
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