from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, File, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_AGENT, require_role
from src.data.clients.postgres_client import get_db_session
from src.schemas.conversation_schema import (
    AgentReplyRequest,
    AttachmentItem,
    ConversationItem,
    TicketThreadResponse,
)
from src.core.services.agent_conversation_service import AgentConversationService

router = APIRouter(tags=["Agent — Conversations"])

_AgentActor = Depends(require_role(ROLE_AGENT))


def _svc(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> AgentConversationService:
    return AgentConversationService(session, background_tasks)


# ── Thread ─────────────────────────────────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
)
async def agent_get_thread(
    ticket_id: str,
    actor:     CurrentActor             = _AgentActor,
    svc:       AgentConversationService = Depends(_svc),
) -> TicketThreadResponse:
    conversations, attachments = await svc.get_thread(ticket_id, actor.actor_id)
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
    ticket_id: str,
    payload:   AgentReplyRequest,
    actor:     CurrentActor             = _AgentActor,
    svc:       AgentConversationService = Depends(_svc),
) -> ConversationItem:
    convo = await svc.post_comment(
        ticket_id=ticket_id,
        agent_id=actor.actor_id,
        content=payload.content,
        is_internal=payload.is_internal,
    )
    return ConversationItem.model_validate(convo)


# ── Attachments ────────────────────────────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
)
async def agent_upload_attachment(
    ticket_id: str,
    file:      UploadFile               = File(...),
    actor:     CurrentActor             = _AgentActor,
    svc:       AgentConversationService = Depends(_svc),
) -> AttachmentItem:
    file_bytes = await file.read()
    att = await svc.upload_attachment(
        ticket_id=ticket_id,
        agent_id=actor.actor_id,
        filename=file.filename or "upload",
        file_bytes=file_bytes,
        mime_type=file.content_type,
    )
    return AttachmentItem.model_validate(att)


@router.get(
    "/agent/tickets/{ticket_id}/attachments/{attachment_id}/signed-url",
    response_model=None,
)
async def agent_get_attachment_signed_url(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor             = _AgentActor,
    svc:           AgentConversationService = Depends(_svc),
) -> dict:
    signed_url = await svc.get_attachment_signed_url(
        ticket_id=ticket_id,
        agent_id=actor.actor_id,
        attachment_id=attachment_id,
    )
    return {"url": signed_url}