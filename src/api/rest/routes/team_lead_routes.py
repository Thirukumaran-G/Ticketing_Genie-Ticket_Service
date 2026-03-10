"""
Team Lead routes.
src/api/rest/routes/team_lead_routes.py

Endpoints:
  GET  /teamlead/queue                          — unassigned tickets (initial load)
  GET  /teamlead/queue/stream                   — SSE stream (real-time push)
  GET  /teamlead/tickets                        — all team tickets (filterable by status)
  GET  /teamlead/tickets/{ticket_id}            — single ticket detail
  POST /teamlead/tickets/{ticket_id}/assign     — manually assign to agent
  PATCH /teamlead/tickets/{ticket_id}/status    — update ticket status
  GET  /teamlead/overview                       — team overview + agent workloads
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import (
    CurrentActor,
    ROLE_TEAM_LEAD,
    require_role,
)
from src.core.services.team_lead_service import TeamLeadService
from src.core.sse.sse_manager import sse_manager
from src.data.clients.postgres_client import get_db_session
from src.schemas.team_lead_schema import (
    ManualAssignRequest,
    TeamOverviewResponse,
    TicketStatusUpdateRequest,
    TLTicketDetailResponse,
)
from src.schemas.ticket_schema import TicketQueueItem

router = APIRouter(prefix="/teamlead", tags=["Team Lead"])

_TLActor = Depends(require_role(ROLE_TEAM_LEAD))


def _tl_svc(session: AsyncSession = Depends(get_db_session)) -> TeamLeadService:
    return TeamLeadService(session)


# ── Unassigned queue — initial load ──────────────────────────────────────────

@router.get(
    "/queue",
    response_model=list[TicketQueueItem],
    summary="Unassigned tickets routed to your team (initial load)",
)
async def get_tl_queue(
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> list[TicketQueueItem]:
    tickets = await service.get_unassigned_queue(actor.actor_id)
    return [TicketQueueItem.model_validate(t) for t in tickets]


# ── SSE stream ────────────────────────────────────────────────────────────────

@router.get(
    "/queue/stream",
    summary="SSE stream — real-time push when unassigned ticket arrives",
    response_class=StreamingResponse,
)
async def tl_queue_stream(
    actor: CurrentActor = _TLActor,
) -> StreamingResponse:
    """
    Events:
      event: queue_update   → new unassigned ticket routed to team
                              OR auto-assign threshold not met
      : keepalive           → every 30s
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


# ── All team tickets ──────────────────────────────────────────────────────────

@router.get(
    "/tickets",
    response_model=list[TLTicketDetailResponse],
    summary="All tickets for your team — optionally filter by status",
)
async def get_team_tickets(
    status: Optional[str] = Query(
        None,
        description="Filter by status: new | open | in_progress | resolved | closed",
    ),
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> list[TLTicketDetailResponse]:
    tickets = await service.get_all_team_tickets(actor.actor_id, status=status)
    return [TLTicketDetailResponse.model_validate(t) for t in tickets]


# ── Single ticket detail ──────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}",
    response_model=TLTicketDetailResponse,
    summary="Full detail of a single ticket in your team",
)
async def get_ticket_detail(
    ticket_id: str,
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    ticket = await service.get_ticket(ticket_id, actor.actor_id)
    return TLTicketDetailResponse.model_validate(ticket)


# ── Manual assign ─────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/assign",
    response_model=TLTicketDetailResponse,
    summary="Manually assign a ticket to an agent in your team",
)
async def manual_assign(
    ticket_id: str,
    payload: ManualAssignRequest,
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    """
    TL picks an agent manually from the unassigned queue.
    Fires SSE push to agent immediately.
    Fires in-app notification to agent.
    """
    ticket = await service.manual_assign(ticket_id, payload, actor.actor_id)
    return TLTicketDetailResponse.model_validate(ticket)


# ── Status update ─────────────────────────────────────────────────────────────

@router.patch(
    "/tickets/{ticket_id}/status",
    response_model=TLTicketDetailResponse,
    summary="Update ticket status (TL override)",
)
async def update_ticket_status(
    ticket_id: str,
    payload: TicketStatusUpdateRequest,
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    ticket = await service.update_ticket_status(ticket_id, payload, actor.actor_id)
    return TLTicketDetailResponse.model_validate(ticket)


# ── Team overview ─────────────────────────────────────────────────────────────

@router.get(
    "/overview",
    response_model=TeamOverviewResponse,
    summary="Team overview — agent workloads + unassigned ticket count",
)
async def get_team_overview(
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TeamOverviewResponse:
    """
    Returns:
      - team name, product_id
      - unassigned ticket count
      - each agent's open ticket count + experience + skills
    """
    return await service.get_team_overview(actor.actor_id)