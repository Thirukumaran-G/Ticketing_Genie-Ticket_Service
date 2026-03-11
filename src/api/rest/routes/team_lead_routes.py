"""
Team Lead routes.
src/api/rest/routes/team_lead_routes.py
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
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
    TLTicketThreadResponse,
    TLInternalNoteRequest,
)
from src.schemas.ticket_schema import TicketQueueItem

router = APIRouter(prefix="/teamlead", tags=["Team Lead"])

_TLActor = Depends(require_role(ROLE_TEAM_LEAD))


def _tl_svc(session: AsyncSession = Depends(get_db_session)) -> TeamLeadService:
    return TeamLeadService(session)


# ── Unassigned queue ──────────────────────────────────────────────────────────

@router.get(
    "/queue",
    response_model=list[TicketQueueItem],
    summary="Unassigned tickets across all product teams (initial load)",
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
    summary="All tickets handled by your team members — optionally filter by status",
)
async def get_team_tickets(
    status: Optional[str] = Query(
        None,
        description="Filter by status: new | open | in_progress | on_hold | resolved | closed",
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
    summary="Full detail of a single ticket handled by your team",
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
    summary="Manually assign a ticket to a team member",
)
async def manual_assign(
    ticket_id: str,
    payload: ManualAssignRequest,
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
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
    summary="Team overview — all members' workloads + unassigned count across all products",
)
async def get_team_overview(
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TeamOverviewResponse:
    return await service.get_team_overview(actor.actor_id)


# ── Ticket thread ─────────────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/thread",
    response_model=TLTicketThreadResponse,
    summary="View full conversation thread for a ticket",
)
async def get_tl_ticket_thread(
    ticket_id: str,
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TLTicketThreadResponse:
    try:
        data = await service.get_ticket_thread(ticket_id, actor.actor_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TLTicketThreadResponse(
        conversations=data["conversations"],
        attachments=data["attachments"],
    )


# ── Internal note ─────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/note",
    summary="Add an internal note to a ticket (never visible to customer)",
)
async def add_tl_internal_note(
    ticket_id: str,
    payload: TLInternalNoteRequest,
    actor: CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
):
    try:
        note = await service.add_internal_note(
            ticket_id=ticket_id,
            content=payload.content,
            lead_user_id=actor.actor_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {
        "id":          str(note.id),
        "ticket_id":   str(note.ticket_id),
        "author_id":   str(note.author_id),
        "author_type": note.author_type,
        "content":     note.content,
        "is_internal": note.is_internal,
        "created_at":  note.created_at,
    }