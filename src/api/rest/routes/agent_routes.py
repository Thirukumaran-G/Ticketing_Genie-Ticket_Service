from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_AGENT, require_role
from src.core.services.agent_services import AgentService
from src.core.sse.sse_manager import sse_manager
from src.data.clients.postgres_client import get_db_session
from src.schemas.ticket_schema import (
    TicketQueueItem,
    StatusUpdateRequest,
    TicketDetailResponse,
    UnassignRequest,
    BreachJustificationRequest,
    BreachJustificationResponse,
    EnhanceRequest,
    EnhanceResponse,
    ChildReplyRequest
)
from fastapi.responses import Response
from src.control.agents.enhance_agent import EnhanceAgent
from src.core.services.ticket_group_service import TicketGroupService

def _group_svc_agent(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> TicketGroupService:
    return TicketGroupService(session, background_tasks)

router = APIRouter(tags=["Agent — Queue"])

_AgentActor = Depends(require_role(ROLE_AGENT))
_enhancer   = EnhanceAgent()


def _agent_svc(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> AgentService:
    return AgentService(session, background_tasks)


# ── Queue ─────────────────────────────────────────────────────────────────────

@router.get(
    "/agent/queue",
    response_model=list[TicketQueueItem],
    summary="Agent — fetch current ticket queue",
)
async def get_agent_queue(
    actor:   CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
) -> list[TicketQueueItem]:
    tickets = await service.get_agent_queue(actor.actor_id)
    return [TicketQueueItem.model_validate(t) for t in tickets]


# ── Single ticket ─────────────────────────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}",
    response_model=TicketDetailResponse,
    summary="Agent — fetch a single ticket by ID",
)
async def get_agent_ticket(
    ticket_id: str,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
) -> TicketDetailResponse:
    try:
        ticket = await service.get_ticket_by_id(actor.actor_id, ticket_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TicketDetailResponse.model_validate(ticket)


# ── Customer info ─────────────────────────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}/customer",
    summary="Agent — fetch customer info for a ticket",
)
async def get_ticket_customer_info(
    ticket_id: str,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
) -> dict:
    try:
        ticket = await service.get_ticket_by_id(actor.actor_id, ticket_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    info = await service.get_customer_info(str(ticket.customer_id))
    if not info:
        raise HTTPException(status_code=404, detail="Customer info not found")
    return info


# ── Status update ─────────────────────────────────────────────────────────────

@router.patch(
    "/agent/tickets/{ticket_id}/status",
    response_model=TicketDetailResponse,
    summary="Agent — update ticket status (in_progress / on_hold / resolved)",
)
async def update_agent_ticket_status(
    ticket_id: str,
    payload:   StatusUpdateRequest,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
) -> TicketDetailResponse:
    try:
        ticket = await service.update_ticket_status(
            agent_id=actor.actor_id,
            ticket_id=ticket_id,
            new_status=payload.status,
            reason=payload.reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return TicketDetailResponse.model_validate(ticket)


# ── Set in-progress (called when agent opens composer) ────────────────────────


@router.patch(
    "/agent/tickets/{ticket_id}/in-progress",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Agent — mark ticket in_progress when composer is opened",
)
async def set_in_progress(
    ticket_id: str,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
) -> None:
    await service.set_in_progress(actor.actor_id, ticket_id)


# ── All assigned tickets ──────────────────────────────────────────────────────

@router.get(
    "/agent/tickets",
    response_model=list[TicketQueueItem],
    summary="Agent — fetch all assigned tickets",
)
async def get_agent_tickets(
    actor:   CurrentActor = _AgentActor,
    service: AgentService = Depends(_agent_svc),
) -> list[TicketQueueItem]:
    tickets = await service.get_agent_queue(actor.actor_id)
    return [TicketQueueItem.model_validate(t) for t in tickets]


# ── Self-unassign ─────────────────────────────────────────────────────────────

@router.patch(
    "/agent/tickets/{ticket_id}/unassign",
    summary="Agent — self-unassign a ticket with justification",
)
async def unassign_agent_ticket(
    ticket_id: str,
    payload:   UnassignRequest,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
):
    try:
        ticket = await service.unassign_ticket(
            agent_id=actor.actor_id,
            ticket_id=ticket_id,
            justification=payload.justification,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TicketDetailResponse.model_validate(ticket)


# ── SLA Breach Justification — submit ─────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/breach-justification",
    response_model=BreachJustificationResponse,
    status_code=201,
    summary="Agent — submit SLA breach justification",
)
async def submit_breach_justification(
    ticket_id: str,
    payload:   BreachJustificationRequest,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
) -> BreachJustificationResponse:
    try:
        result = await service.submit_breach_justification(
            agent_id=actor.actor_id,
            ticket_id=ticket_id,
            breach_type=payload.breach_type,
            justification=payload.justification,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    return result


# ── SLA Breach Justification — list ───────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}/breach-justifications",
    response_model=list[BreachJustificationResponse],
    summary="Agent — list breach justifications for this ticket",
)
async def list_breach_justifications_agent(
    ticket_id: str,
    actor:     CurrentActor = _AgentActor,
    service:   AgentService = Depends(_agent_svc),
) -> list[BreachJustificationResponse]:
    try:
        await service.get_ticket_by_id(actor.actor_id, ticket_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return await service.get_breach_justifications(ticket_id)


@router.post(
    "/agent/tickets/{ticket_id}/enhance-reply",
    response_model=EnhanceResponse,
    summary="Agent — enhance a draft reply using AI",
)
async def enhance_reply(
    ticket_id: str,
    payload:   EnhanceRequest,
    actor:     CurrentActor = _AgentActor,
) -> EnhanceResponse:
    """
    Enhance the agent's draft reply text.
    The ticket_id is validated implicitly via the auth dependency —
    the agent must be authenticated. You can add ownership checks here
    if needed.
    """
    if not payload.draft.strip():
        raise HTTPException(status_code=422, detail="Draft text cannot be empty.")
 
    result = await _enhancer.enhance(draft=payload.draft, mode=payload.mode)
    return EnhanceResponse(
        enhanced_text=result.enhanced_text,
        changes_summary=result.changes_summary,
    )


@router.get("/grouped-tickets")
async def get_agent_grouped_tickets(
    actor:   CurrentActor = _AgentActor,  # your agent dependency
    session: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    from sqlalchemy import select
    from src.data.models.postgres.models import Ticket, SimilarTicketGroup
    from src.data.repositories.ticket_group_repository import TicketGroupRepository

    repo = TicketGroupRepository(session)

    # Get all parent tickets assigned to this agent
    r = await session.execute(
        select(Ticket).where(
            Ticket.assigned_to == uuid.UUID(actor.actor_id),
            Ticket.is_parent.is_(True),
        )
    )
    parent_tickets = list(r.scalars().all())

    result = []
    for pt in parent_tickets:
        children = await repo.get_children_of_parent(str(pt.id))
        result.append({
            "ticket_id":       str(pt.id),
            "ticket_number":   pt.ticket_number,
            "title":           pt.title,
            "status":          pt.status,
            "priority":        pt.priority,
            "severity":        pt.severity,
            "child_count":     len(children),
            "created_at":      pt.created_at,
            "sla_resolve_due": pt.sla_resolve_due,
        })
    return result


@router.get("/grouped-tickets/{ticket_id}/children")
async def get_grouped_ticket_children(
    ticket_id: str,
    actor:     CurrentActor       = _AgentActor,
    service:   TicketGroupService = Depends(_group_svc_agent),
) -> list[dict]:
    try:
        return await service.get_grouped_ticket_children_with_conversations(
            parent_ticket_id=ticket_id,
            agent_id=actor.actor_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@router.post("/grouped-tickets/{ticket_id}/reply/{child_ticket_id}")
async def reply_to_child_ticket(
    ticket_id:       str,
    child_ticket_id: str,
    payload:         ChildReplyRequest,
    actor:           CurrentActor       = _AgentActor,
    service:         TicketGroupService = Depends(_group_svc_agent),
) -> dict:
    try:
        return await service.reply_to_child(
            parent_ticket_id=ticket_id,
            child_ticket_id=child_ticket_id,
            agent_id=actor.actor_id,
            message=payload.message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc))