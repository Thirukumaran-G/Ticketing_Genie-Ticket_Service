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
    NotificationTemplateResponse,
    NotificationTemplateUpdateRequest,
    SendApologyRequest,
    SendApologyResponse,
    TeamOverviewResponse,
    TicketStatusUpdateRequest,
    TLTicketDetailResponse,
    TLTicketThreadResponse,
    TLInternalNoteRequest,
    SimilarTicketGroupResponse,
    SimilarTicketMemberResponse,
    ConfirmGroupRequest,
    AddGroupMemberRequest,
    BulkAssignRequest,
    BulkResolveRequest,
    BulkActionResponse,
)
from src.schemas.ticket_schema import (
    TicketQueueItem,
    BreachJustificationResponse,
)

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
    actor:   CurrentActor = _TLActor,
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
                    event   = payload["event"]
                    data    = json.dumps(payload["data"])
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
    summary="All tickets handled by your team members",
)
async def get_team_tickets(
    status:  Optional[str] = Query(None),
    actor:   CurrentActor = _TLActor,
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
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
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
    payload:   ManualAssignRequest,
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    from src.core.reddis.assignment_lock import assignment_lock, AssignmentLockError
    try:
        async with assignment_lock(ticket_id):
            ticket = await service.manual_assign(ticket_id, payload, actor.actor_id)
    except AssignmentLockError:
        raise HTTPException(
            status_code=409,
            detail=(
                "Ticket is currently being assigned by another process. "
                "Please try again in a few seconds."
            ),
        )
    return TLTicketDetailResponse.model_validate(ticket)


# ── Status update ─────────────────────────────────────────────────────────────

@router.patch(
    "/tickets/{ticket_id}/status",
    response_model=TLTicketDetailResponse,
    summary="Update ticket status (TL override)",
)
async def update_ticket_status(
    ticket_id: str,
    payload:   TicketStatusUpdateRequest,
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    ticket = await service.update_ticket_status(ticket_id, payload, actor.actor_id)
    return TLTicketDetailResponse.model_validate(ticket)


# ── Team overview ─────────────────────────────────────────────────────────────

@router.get(
    "/overview",
    response_model=TeamOverviewResponse,
    summary="Team overview — all members workloads + unassigned count",
)
async def get_team_overview(
    actor:   CurrentActor = _TLActor,
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
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
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
    payload:   TLInternalNoteRequest,
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
    session:   AsyncSession = Depends(get_db_session),
):
    try:
        note = await service.add_internal_note(
            ticket_id=ticket_id,
            content=payload.content,
            lead_user_id=actor.actor_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    # Notify assigned agent in_app + SSE
    from src.api.rest.routes.conversation_routes import notify_agent_of_tl_note
    await notify_agent_of_tl_note(
        session=session,
        ticket_id=ticket_id,
        lead_id=actor.actor_id,
        content=payload.content,
    )

    return {
        "id":          str(note.id),
        "ticket_id":   str(note.ticket_id),
        "author_id":   str(note.author_id),
        "author_type": note.author_type,
        "content":     note.content,
        "is_internal": note.is_internal,
        "created_at":  note.created_at,
    }


# ── SLA Breach Justifications — TL view ──────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/breach-justifications",
    response_model=list[BreachJustificationResponse],
    summary="TL — view all breach justifications submitted by agent for this ticket",
)
async def get_breach_justifications_tl(
    ticket_id: str,
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> list[BreachJustificationResponse]:
    try:
        await service.get_ticket(ticket_id, actor.actor_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    from src.core.services.agent_services import AgentService
    agent_svc = AgentService(service._session)
    return await agent_svc.get_breach_justifications(ticket_id)


# ── Notification Templates ────────────────────────────────────────────────────

@router.get(
    "/notification-templates",
    response_model=list[NotificationTemplateResponse],
    summary="TL — list all notification templates",
)
async def list_notification_templates(
    actor:   CurrentActor = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> list[NotificationTemplateResponse]:
    templates = await service.list_templates()
    return [NotificationTemplateResponse.model_validate(t) for t in templates]


@router.get(
    "/notification-templates/{template_id}",
    response_model=NotificationTemplateResponse,
    summary="TL — get a single notification template",
)
async def get_notification_template(
    template_id: str,
    actor:       CurrentActor = _TLActor,
    service:     TeamLeadService = Depends(_tl_svc),
) -> NotificationTemplateResponse:
    try:
        tpl = await service.get_template(template_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return NotificationTemplateResponse.model_validate(tpl)


@router.put(
    "/notification-templates/{template_id}",
    response_model=NotificationTemplateResponse,
    summary="TL — update subject/body of a notification template",
)
async def update_notification_template(
    template_id: str,
    payload:     NotificationTemplateUpdateRequest,
    actor:       CurrentActor = _TLActor,
    service:     TeamLeadService = Depends(_tl_svc),
    session:     AsyncSession = Depends(get_db_session),
) -> NotificationTemplateResponse:
    try:
        tpl = await service.update_template(
            template_id=template_id,
            lead_user_id=actor.actor_id,
            name=payload.name,
            subject=payload.subject,
            body=payload.body,
            is_active=payload.is_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    
    await session.refresh(tpl)
    return NotificationTemplateResponse.model_validate(tpl)


# ── Send Apology ──────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/send-apology",
    response_model=SendApologyResponse,
    summary="TL — send apology/inconvenience message to customer",
    description=(
        "TL selects a template, optionally customises the message, "
        "and sends it to the customer via their notification preference. "
        "An internal note is always saved on the ticket regardless of send outcome."
    ),
)
async def send_apology(
    ticket_id: str,
    payload:   SendApologyRequest,
    actor:     CurrentActor = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> SendApologyResponse:
    try:
        result = await service.send_apology(
            ticket_id=ticket_id,
            lead_user_id=actor.actor_id,
            template_id=str(payload.template_id),
            custom_message=payload.custom_message,
            commit_time=payload.commit_time,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return SendApologyResponse(
        sent=result["sent"],
        channel=result["channel"],
        ticket_id=result["ticket_id"],
        template_key=result["template_key"],
        message=result["message"],
    )

# ── Similar Ticket Groups ─────────────────────────────────────────────────────

def _similar_svc(session: AsyncSession = Depends(get_db_session)):
    from src.core.services.similar_ticket_service import SimilarTicketService
    return SimilarTicketService(session)


@router.get(
    "/ticket-groups",
    response_model=list[SimilarTicketGroupResponse],
    summary="TL — list all similar ticket groups",
)
async def list_ticket_groups(
    confirmed_only: bool = Query(False),
    actor:          CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> list[SimilarTicketGroupResponse]:
    groups = await svc.list_groups(confirmed_only=confirmed_only)
    return [_serialize_group(g) for g in groups]


@router.get(
    "/ticket-groups/{group_id}",
    response_model=SimilarTicketGroupResponse,
    summary="TL — get a single similar ticket group with all members",
)
async def get_ticket_group(
    group_id: str,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> SimilarTicketGroupResponse:
    try:
        group = await svc.get_group(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _serialize_group(group)


@router.post(
    "/ticket-groups/{group_id}/confirm",
    response_model=SimilarTicketGroupResponse,
    summary="TL — confirm a similar ticket group as same root issue",
)
async def confirm_ticket_group(
    group_id: str,
    payload:  ConfirmGroupRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> SimilarTicketGroupResponse:
    try:
        group = await svc.confirm_group(
            group_id=group_id,
            lead_user_id=actor.actor_id,
            name=payload.name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _serialize_group(group)


@router.post(
    "/ticket-groups/{group_id}/members",
    response_model=SimilarTicketGroupResponse,
    summary="TL — manually add a ticket to a group",
)
async def add_ticket_to_group(
    group_id: str,
    payload:  AddGroupMemberRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> SimilarTicketGroupResponse:
    try:
        await svc.add_ticket_to_group(
            group_id=group_id,
            ticket_id=str(payload.ticket_id),
            similarity_score=payload.similarity_score,
        )
        group = await svc.get_group(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return _serialize_group(group)


@router.delete(
    "/ticket-groups/{group_id}/members/{ticket_id}",
    status_code=204,
    response_model=None,                          # ← add this
    summary="TL — remove a ticket from a group",
)
async def remove_ticket_from_group(
    group_id:  str,
    ticket_id: str,
    actor:     CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> None:
    removed = await svc.remove_ticket_from_group(
        group_id=group_id,
        ticket_id=ticket_id,
    )
    if not removed:
        raise HTTPException(status_code=404, detail="Member not found in group.")


@router.post(
    "/ticket-groups/{group_id}/bulk-assign",
    response_model=BulkActionResponse,
    summary="TL — assign all open tickets in group to one agent",
)
async def bulk_assign_group(
    group_id: str,
    payload:  BulkAssignRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> BulkActionResponse:
    try:
        result = await svc.bulk_assign(
            group_id=group_id,
            agent_user_id=str(payload.agent_user_id),
            lead_user_id=actor.actor_id,
            internal_message=payload.internal_message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return BulkActionResponse(
        assigned=result["assigned"],
        skipped=result["skipped"],
        ticket_numbers=result["ticket_numbers"],
    )


@router.post(
    "/ticket-groups/{group_id}/bulk-resolve",
    response_model=BulkActionResponse,
    summary="TL — resolve all open tickets in group and notify each customer",
)
async def bulk_resolve_group(
    group_id: str,
    payload:  BulkResolveRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> BulkActionResponse:
    try:
        result = await svc.bulk_resolve(
            group_id=group_id,
            lead_user_id=actor.actor_id,
            resolution_message=payload.resolution_message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return BulkActionResponse(
        resolved=result["resolved"],
        skipped=result["skipped"],
        ticket_numbers=result["ticket_numbers"],
    )


@router.get(
    "/tickets/{ticket_id}/similar-groups",
    response_model=list[SimilarTicketGroupResponse],
    summary="TL — get all similar groups that contain this ticket",
)
async def get_groups_for_ticket(
    ticket_id: str,
    actor:     CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> list[SimilarTicketGroupResponse]:
    groups = await svc.get_groups_for_ticket(ticket_id)
    return [_serialize_group(g) for g in groups]


# ── Serialization helper ──────────────────────────────────────────────────────

def _serialize_group(group) -> SimilarTicketGroupResponse:
    """Convert ORM group + members into response schema."""
    members = []
    for m in group.members:
        t = m.ticket
        if not t:
            continue
        members.append(
            SimilarTicketMemberResponse(
                ticket_id=t.id,
                ticket_number=t.ticket_number,
                title=t.title,
                status=t.status,
                priority=t.priority,
                severity=t.severity,
                similarity_score=m.similarity_score,
                added_at=m.added_at,
            )
        )
    members.sort(key=lambda x: x.similarity_score, reverse=True)

    return SimilarTicketGroupResponse(
        id=group.id,
        name=group.name,
        confirmed_by_lead=group.confirmed_by_lead,
        confirmed_at=group.confirmed_at,
        confirmed_by=group.confirmed_by,
        member_count=len(members),
        members=members,
        created_at=group.created_at,
        updated_at=group.updated_at,
    )