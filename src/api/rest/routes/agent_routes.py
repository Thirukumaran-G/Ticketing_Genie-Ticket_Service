# ticket service
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends  
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_AGENT, require_role
from src.core.services.agent_services import AgentService
from src.core.sse.sse_manager import sse_manager
from src.data.clients.postgres_client import get_db_session
from src.schemas.ticket_schema import TicketQueueItem, StatusUpdateRequest, TicketDetailResponse, UnassignRequest
from fastapi import HTTPException
router = APIRouter(tags=["Agent — Queue"])

_AgentActor = Depends(require_role(ROLE_AGENT))


def _agent_svc(session: AsyncSession = Depends(get_db_session)) -> AgentService:
    return AgentService(session)


@router.get(
    "/agent/queue",
    response_model=list[TicketQueueItem],
    summary="Agent — fetch current ticket queue (initial load)",
)
async def get_agent_queue(
    actor: CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
) -> list[TicketQueueItem]:
    tickets = await service.get_agent_queue(actor.actor_id)
    return [TicketQueueItem.model_validate(t) for t in tickets]

@router.get(
    "/agent/tickets/{ticket_id}",
    response_model=TicketDetailResponse,
    summary="Agent — fetch a single ticket by ID",
)
async def get_agent_ticket(
    ticket_id: str,
    actor: CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
) -> TicketDetailResponse:
    try:
        ticket = await service.get_ticket_by_id(actor.actor_id, ticket_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TicketDetailResponse.model_validate(ticket)


@router.get(
    "/agent/queue/stream",
    summary="Agent — SSE stream for real-time queue updates",
    response_class=StreamingResponse,
)
async def agent_queue_stream(
    actor: CurrentActor = _AgentActor,
) -> StreamingResponse:
    """
    Events:
      event: queue_update  — new ticket assigned or status changed
      : keepalive          — every 30s
    """
    async def _stream():
        async with sse_manager.subscribe(actor.actor_id) as q:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=30)
                    event = payload["event"]
                    data  = json.dumps(payload["data"])
                    yield f"event: {event}\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                except asyncio.CancelledError:
                    break

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@router.get(
    "/agent/tickets/{ticket_id}/customer",
    summary="Agent — fetch customer info for a ticket",
)
async def get_ticket_customer_info(
    ticket_id: str,
    actor: CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
) -> dict:
    try:
        ticket = await service.get_ticket_by_id(actor.actor_id, ticket_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    
    info = await service.get_customer_info(str(ticket.customer_id))
    if not info:
        raise HTTPException(status_code=404, detail="Customer info not found")
    return info


@router.patch(
    "/agent/tickets/{ticket_id}/status",
    summary="Agent — update ticket status",
)
async def update_agent_ticket_status(
    ticket_id: str,
    payload: StatusUpdateRequest,
    actor: CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
):
    try:
        ticket = await service.update_ticket_status(
            agent_id=actor.actor_id,
            ticket_id=ticket_id,
            new_status=payload.status,
            reason=payload.reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return ticket

@router.get(
    "/agent/tickets",
    response_model=list[TicketQueueItem],
    summary="Agent — fetch all assigned tickets",
)
async def get_agent_tickets(
    actor: CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
) -> list[TicketQueueItem]:
    tickets = await service.get_agent_queue(actor.actor_id)
    return [TicketQueueItem.model_validate(t) for t in tickets]

@router.patch(
    "/agent/tickets/{ticket_id}/unassign",
    summary="Agent — self-unassign a ticket with justification",
)
async def unassign_agent_ticket(
    ticket_id: str,
    payload: UnassignRequest,
    actor: CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
):
    """
    Agent unassigns themselves from a ticket.
    Ticket reverts to 'new' status and appears in TL unassigned queue.
    Justification is saved as an internal note on the ticket.
    SSE push fires to team lead immediately.
    """
    try:
        ticket = await service.unassign_ticket(
            agent_id=actor.actor_id,
            ticket_id=ticket_id,
            justification=payload.justification,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TicketDetailResponse.model_validate(ticket)