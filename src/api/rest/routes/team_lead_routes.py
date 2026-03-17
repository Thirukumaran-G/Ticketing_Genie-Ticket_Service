"""
Team Lead routes.
src/api/rest/routes/team_lead_routes.py
"""
from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_TEAM_LEAD, require_role
from src.core.services.team_lead_service import TeamLeadService
from src.core.sse.sse_manager import sse_manager
from src.data.clients.postgres_client import get_db_session
from src.schemas.conversation_schema import AttachmentItem
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
from src.schemas.ticket_schema import TicketQueueItem, BreachJustificationResponse

router = APIRouter(prefix="/teamlead", tags=["Team Lead"])

_TLActor = Depends(require_role(ROLE_TEAM_LEAD))


def _tl_svc(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> TeamLeadService:
    return TeamLeadService(session, background_tasks)


# ── Unassigned queue ──────────────────────────────────────────────────────────

@router.get("/queue", response_model=list[TicketQueueItem])
async def get_tl_queue(
    actor:   CurrentActor    = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> list[TicketQueueItem]:
    tickets = await service.get_unassigned_queue(actor.actor_id)
    return [TicketQueueItem.model_validate(t) for t in tickets]


# ── SSE stream ────────────────────────────────────────────────────────────────

@router.get("/queue/stream", response_class=StreamingResponse)
async def tl_queue_stream(actor: CurrentActor = _TLActor) -> StreamingResponse:
    async def _stream():
        async with sse_manager.subscribe(actor.actor_id) as q:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"event: {payload['event']}\ndata: {json.dumps(payload['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                except asyncio.CancelledError:
                    break

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


# ── All team tickets ──────────────────────────────────────────────────────────

@router.get("/tickets", response_model=list[TLTicketDetailResponse])
async def get_team_tickets(
    status:  Optional[str]   = Query(None),
    actor:   CurrentActor    = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> list[TLTicketDetailResponse]:
    tickets = await service.get_all_team_tickets(actor.actor_id, status=status)
    return [TLTicketDetailResponse.model_validate(t) for t in tickets]


# ── Single ticket ─────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}", response_model=TLTicketDetailResponse)
async def get_ticket_detail(
    ticket_id: str,
    actor:     CurrentActor    = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    ticket = await service.get_ticket(ticket_id, actor.actor_id)
    return TLTicketDetailResponse.model_validate(ticket)


# ── Manual assign ─────────────────────────────────────────────────────────────

@router.post("/tickets/{ticket_id}/assign", response_model=TLTicketDetailResponse)
async def manual_assign(
    ticket_id: str,
    payload:   ManualAssignRequest,
    actor:     CurrentActor    = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    from src.core.reddis.assignment_lock import assignment_lock, AssignmentLockError
    try:
        async with assignment_lock(ticket_id):
            ticket = await service.manual_assign(ticket_id, payload, actor.actor_id)
    except AssignmentLockError:
        raise HTTPException(status_code=409, detail="Ticket is being assigned. Retry in a few seconds.")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return TLTicketDetailResponse.model_validate(ticket)


# ── Status update ─────────────────────────────────────────────────────────────

@router.patch("/tickets/{ticket_id}/status", response_model=TLTicketDetailResponse)
async def update_ticket_status(
    ticket_id: str,
    payload:   TicketStatusUpdateRequest,
    actor:     CurrentActor    = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    try:
        ticket = await service.update_ticket_status(ticket_id, payload, actor.actor_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return TLTicketDetailResponse.model_validate(ticket)


# ── Team overview ─────────────────────────────────────────────────────────────

@router.get("/overview", response_model=TeamOverviewResponse)
async def get_team_overview(
    actor:   CurrentActor    = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> TeamOverviewResponse:
    return await service.get_team_overview(actor.actor_id)


# ── Ticket thread ─────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}/thread", response_model=TLTicketThreadResponse)
async def get_tl_ticket_thread(
    ticket_id: str,
    actor:     CurrentActor    = _TLActor,
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


# ── Attachments ───────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
)
async def tl_upload_attachment(
    ticket_id: str,
    file:      UploadFile       = File(...),
    actor:     CurrentActor     = _TLActor,
    session:   AsyncSession     = Depends(get_db_session),
) -> AttachmentItem:
    from src.core.services.conversation_service import ConversationService
    conv_svc   = ConversationService(session)
    file_bytes = await file.read()
    try:
        att = await conv_svc.upload_attachment(
            ticket_id=ticket_id,
            uploader_id=actor.actor_id,
            filename=file.filename or "upload",
            file_bytes=file_bytes,
            mime_type=file.content_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return AttachmentItem.model_validate(att)


@router.get(
    "/tickets/{ticket_id}/attachments/{attachment_id}",
    response_class=FileResponse,
)
async def tl_get_attachment(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor = _TLActor,
    session:       AsyncSession = Depends(get_db_session),
) -> FileResponse:
    from src.core.services.conversation_service import ConversationService
    conv_svc = ConversationService(session)
    att      = await conv_svc.get_attachment_by_id(attachment_id, ticket_id)
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found.")
    path = conv_svc.resolve_attachment_path(att.file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found on server.")
    return FileResponse(
        path=str(path),
        filename=att.file_name,
        media_type=att.mime_type or "application/octet-stream",
    )


# ── Customer info ─────────────────────────────────────────────────────────────

@router.get("/tickets/{ticket_id}/customer")
async def get_tl_ticket_customer_info(
    ticket_id: str,
    actor:     CurrentActor    = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> dict:
    ticket = await service.get_ticket(ticket_id, actor.actor_id)
    from src.core.services.agent_services import AgentService
    info = await AgentService(service._session).get_customer_info(str(ticket.customer_id))
    if not info:
        raise HTTPException(status_code=404, detail="Customer info not found")
    return info


# ── Internal note ─────────────────────────────────────────────────────────────

@router.post("/tickets/{ticket_id}/note")
async def add_tl_internal_note(
    ticket_id:        str,
    payload:          TLInternalNoteRequest,
    background_tasks: BackgroundTasks,
    actor:            CurrentActor    = _TLActor,
    service:          TeamLeadService = Depends(_tl_svc),
    session:          AsyncSession    = Depends(get_db_session),
):
    try:
        note = await service.add_internal_note(
            ticket_id=ticket_id,
            content=payload.content,
            lead_user_id=actor.actor_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    # Notify assigned agent — no stale conversation_routes import
    try:
        from sqlalchemy import select
        from src.data.models.postgres.models import Ticket
        from src.core.services.notification_service import NotificationService
        import uuid as _uuid

        r      = await session.execute(
            select(Ticket).where(Ticket.id == _uuid.UUID(ticket_id))
        )
        ticket = r.scalar_one_or_none()
        if ticket and ticket.assigned_to:
            notif_svc = NotificationService(session, background_tasks)
            await notif_svc.notify(
                recipient_id=str(ticket.assigned_to),
                ticket=ticket,
                notif_type="internal_note",
                title=f"Internal note added on {ticket.ticket_number}",
                message=(
                    f"Your team lead added an internal note on ticket "
                    f"{ticket.ticket_number}."
                ),
                is_internal=True,
                email_subject=f"[{ticket.ticket_number}] Internal note from team lead",
                email_body=(
                    f"Hi,\n\nYour team lead added an internal note on ticket "
                    f"{ticket.ticket_number}.\n\n"
                    f"Note:\n{payload.content}\n\n"
                    f"— Ticketing Genie"
                ),
            )
    except Exception as exc:
        from src.observability.logging.logger import get_logger
        get_logger(__name__).warning(
            "tl_note_agent_notify_failed",
            ticket_id=ticket_id,
            error=str(exc),
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


# ── SLA Breach Justifications ─────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/breach-justifications",
    response_model=list[BreachJustificationResponse],
)
async def get_breach_justifications_tl(
    ticket_id: str,
    actor:     CurrentActor    = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> list[BreachJustificationResponse]:
    try:
        await service.get_ticket(ticket_id, actor.actor_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    from src.core.services.agent_services import AgentService
    return await AgentService(service._session).get_breach_justifications(ticket_id)


# ── Notification Templates ────────────────────────────────────────────────────

@router.get("/notification-templates", response_model=list[NotificationTemplateResponse])
async def list_notification_templates(
    actor:   CurrentActor    = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
):
    return [
        NotificationTemplateResponse.model_validate(t)
        for t in await service.list_templates()
    ]


@router.get(
    "/notification-templates/{template_id}",
    response_model=NotificationTemplateResponse,
)
async def get_notification_template(
    template_id: str,
    actor:       CurrentActor    = _TLActor,
    service:     TeamLeadService = Depends(_tl_svc),
):
    try:
        return NotificationTemplateResponse.model_validate(
            await service.get_template(template_id)
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.put(
    "/notification-templates/{template_id}",
    response_model=NotificationTemplateResponse,
)
async def update_notification_template(
    template_id: str,
    payload:     NotificationTemplateUpdateRequest,
    actor:       CurrentActor    = _TLActor,
    service:     TeamLeadService = Depends(_tl_svc),
    session:     AsyncSession    = Depends(get_db_session),
):
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

@router.post("/tickets/{ticket_id}/send-apology", response_model=SendApologyResponse)
async def send_apology(
    ticket_id: str,
    payload:   SendApologyRequest,
    actor:     CurrentActor    = _TLActor,
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


@router.get("/ticket-groups", response_model=list[SimilarTicketGroupResponse])
async def list_ticket_groups(
    confirmed_only: bool         = Query(False),
    actor:          CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    return [_serialize_group(g) for g in await svc.list_groups(confirmed_only=confirmed_only)]


@router.get("/ticket-groups/{group_id}", response_model=SimilarTicketGroupResponse)
async def get_ticket_group(
    group_id: str,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    try:
        return _serialize_group(await svc.get_group(group_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/ticket-groups/{group_id}/confirm", response_model=SimilarTicketGroupResponse)
async def confirm_ticket_group(
    group_id: str,
    payload:  ConfirmGroupRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    try:
        return _serialize_group(
            await svc.confirm_group(
                group_id=group_id,
                lead_user_id=actor.actor_id,
                name=payload.name,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/ticket-groups/{group_id}/members", response_model=SimilarTicketGroupResponse)
async def add_ticket_to_group(
    group_id: str,
    payload:  AddGroupMemberRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    try:
        await svc.add_ticket_to_group(
            group_id=group_id,
            ticket_id=str(payload.ticket_id),
            similarity_score=payload.similarity_score,
        )
        return _serialize_group(await svc.get_group(group_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.delete(
    "/ticket-groups/{group_id}/members/{ticket_id}",
    status_code=204,
    response_model=None,
)
async def remove_ticket_from_group(
    group_id:  str,
    ticket_id: str,
    actor:     CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
) -> None:
    if not await svc.remove_ticket_from_group(group_id=group_id, ticket_id=ticket_id):
        raise HTTPException(status_code=404, detail="Member not found in group.")


@router.post("/ticket-groups/{group_id}/bulk-assign", response_model=BulkActionResponse)
async def bulk_assign_group(
    group_id: str,
    payload:  BulkAssignRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    try:
        result = await svc.bulk_assign(
            group_id=group_id,
            agent_user_id=str(payload.agent_user_id),
            lead_user_id=actor.actor_id,
            internal_message=payload.internal_message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return BulkActionResponse(**result)


@router.post("/ticket-groups/{group_id}/bulk-resolve", response_model=BulkActionResponse)
async def bulk_resolve_group(
    group_id: str,
    payload:  BulkResolveRequest,
    actor:    CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    try:
        result = await svc.bulk_resolve(
            group_id=group_id,
            lead_user_id=actor.actor_id,
            resolution_message=payload.resolution_message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return BulkActionResponse(**result)


@router.get(
    "/tickets/{ticket_id}/similar-groups",
    response_model=list[SimilarTicketGroupResponse],
)
async def get_groups_for_ticket(
    ticket_id: str,
    actor:     CurrentActor = _TLActor,
    svc=Depends(_similar_svc),
):
    return [_serialize_group(g) for g in await svc.get_groups_for_ticket(ticket_id)]


def _serialize_group(group) -> SimilarTicketGroupResponse:
    members = sorted(
        [
            SimilarTicketMemberResponse(
                ticket_id=m.ticket.id,
                ticket_number=m.ticket.ticket_number,
                title=m.ticket.title,
                status=m.ticket.status,
                priority=m.ticket.priority,
                severity=m.ticket.severity,
                similarity_score=m.similarity_score,
                added_at=m.added_at,
            )
            for m in group.members
            if m.ticket
        ],
        key=lambda x: x.similarity_score,
        reverse=True,
    )
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