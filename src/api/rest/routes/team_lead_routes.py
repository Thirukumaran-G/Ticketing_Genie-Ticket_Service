# src/api/rest/routers/team_lead_router.py
import asyncio
import os
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
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
    RerouteTicketRequest,
    SendApologyRequest,
    SendApologyResponse,
    TeamOverviewResponse,
    TicketStatusUpdateRequest,
    TLTicketDetailResponse,
    TLTicketThreadResponse,
    TLInternalNoteRequest,
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


# ── Route ticket to another team (Customer Support TL only) ──────────────────

@router.post("/tickets/{ticket_id}/reroute", response_model=TLTicketDetailResponse)
async def reroute_ticket(
    ticket_id: str,
    payload:   RerouteTicketRequest,
    actor:     CurrentActor    = _TLActor,
    service:   TeamLeadService = Depends(_tl_svc),
) -> TLTicketDetailResponse:
    try:
        ticket = await service.reroute_ticket(
            ticket_id=ticket_id,
            target_team_id=str(payload.target_team_id),
            lead_user_id=actor.actor_id,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
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


# ── All teams list (for reroute dropdown) ────────────────────────────────────

@router.get("/teams")
async def get_all_teams(
    product_id: str | None = None,          # <-- add query param
    actor:   CurrentActor    = _TLActor,
    service: TeamLeadService = Depends(_tl_svc),
) -> list[dict]:
    return await service.get_all_teams(product_id=product_id)


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
    "/tickets/{ticket_id}/attachments/{attachment_id}/signed-url",
)
async def tl_get_attachment_signed_url(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor = _TLActor,
    session:       AsyncSession = Depends(get_db_session),
) -> dict:
    from src.core.services.conversation_service import ConversationService
    conv_svc = ConversationService(session)
    att      = await conv_svc.get_attachment_by_id(attachment_id, ticket_id)
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found.")
    signed_url = conv_svc.get_download_url(att.file_path)
    return {"url": signed_url}


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