from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_AGENT, require_role
from src.core.services.agent_services import AgentService
from src.core.services.conversation_service import ConversationService
from src.core.services.notification_service import NotificationService
from src.data.clients.postgres_client import get_db_session
from src.data.repositories.ticket_repository import TicketRepository
from src.schemas.conversation_schema import (
    AgentReplyRequest, AttachmentItem, ConversationItem, TicketThreadResponse,
)

router = APIRouter(tags=["Agent — Conversations"])

_AgentActor = Depends(require_role(ROLE_AGENT))


def _agent_svc(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> AgentService:
    return AgentService(session, background_tasks)


def _conv_svc(session: AsyncSession = Depends(get_db_session)) -> ConversationService:
    return ConversationService(session)


def _ticket_repo(session: AsyncSession = Depends(get_db_session)) -> TicketRepository:
    return TicketRepository(session)


async def _assert_agent_owns_ticket(ticket_id: str, agent_id: str, agent_svc: AgentService) -> None:
    try:
        await agent_svc.get_ticket_by_id(agent_id=agent_id, ticket_id=ticket_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Ticket not found.")


# ── Thread ─────────────────────────────────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
)
async def agent_get_thread(
    ticket_id: str,
    actor:     CurrentActor        = _AgentActor,
    agent_svc: AgentService        = Depends(_agent_svc),
    conv_svc:  ConversationService = Depends(_conv_svc),
) -> TicketThreadResponse:
    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)
    conversations = await conv_svc.get_thread(ticket_id=ticket_id, include_internal=True)
    attachments   = await conv_svc.get_attachments(ticket_id=ticket_id)
    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


# ── Comment / reply ────────────────────────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/comment",
    response_model=ConversationItem,
    status_code=201,
)
async def agent_post_comment(
    ticket_id:        str,
    payload:          AgentReplyRequest,
    background_tasks: BackgroundTasks,
    actor:            CurrentActor        = _AgentActor,
    agent_svc:        AgentService        = Depends(_agent_svc),
    conv_svc:         ConversationService = Depends(_conv_svc),
    ticket_repo:      TicketRepository    = Depends(_ticket_repo),
    session:          AsyncSession        = Depends(get_db_session),
) -> ConversationItem:
    if len(payload.content.strip()) < 1:
        raise HTTPException(status_code=422, detail="Content cannot be empty.")

    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)

    convo = await conv_svc.agent_reply(
        ticket_id=ticket_id,
        agent_id=actor.actor_id,
        content=payload.content,
        is_internal=payload.is_internal,
    )

    if not payload.is_internal:
        ticket = await ticket_repo.get_by_id(ticket_id)
        if ticket:
            updates: dict = {}
            if ticket.status in ("assigned",):
                updates["status"] = "in_progress"
            if not ticket.first_response_at:
                updates["first_response_at"] = datetime.now(timezone.utc)
            if updates:
                await ticket_repo.update_fields(ticket_id, updates)

            notif_svc = NotificationService(session, background_tasks)
            await notif_svc.notify(
                recipient_id=str(ticket.customer_id),
                ticket=ticket,
                notif_type="agent_reply",
                title=f"New reply on ticket {ticket.ticket_number}",
                message=f"[{ticket.ticket_number}] Agent: {payload.content[:200]}",
                is_internal=False,
                email_subject=f"[{ticket.ticket_number}] New reply from your support agent",
                email_body=(
                    f"Hi,\n\nYour support agent has posted a reply on ticket "
                    f"{ticket.ticket_number}.\n\n"
                    f"Reply:\n{payload.content}\n\n"
                    f"— Ticketing Genie Support Team"
                ),
            )

    return ConversationItem.model_validate(convo)


# ── Upload attachment ──────────────────────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
)
async def agent_upload_attachment(
    ticket_id: str,
    file:      UploadFile         = File(...),
    actor:     CurrentActor       = _AgentActor,
    agent_svc: AgentService       = Depends(_agent_svc),
    conv_svc:  ConversationService = Depends(_conv_svc),
) -> AttachmentItem:
    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)
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
    "/agent/tickets/{ticket_id}/attachments/{attachment_id}/signed-url",
    response_model=None,
)
async def agent_get_attachment_signed_url(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor        = _AgentActor,
    agent_svc:     AgentService        = Depends(_agent_svc),
    conv_svc:      ConversationService = Depends(_conv_svc),
) -> dict:
    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)

    att = await conv_svc.get_attachment_by_id(attachment_id, ticket_id)
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found.")

    signed_url = conv_svc.get_download_url(att.file_path)
    return {"url": signed_url}