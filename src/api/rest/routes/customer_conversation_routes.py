from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, File, UploadFile
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_CUSTOMER, require_role
from src.data.clients.postgres_client import get_db_session
from src.schemas.conversation_schema import (
    AttachmentItem,
    ConversationItem,
    CustomerReplyRequest,
    TicketThreadResponse,
)
from src.core.services.customer_conversation_service import CustomerConversationService

router = APIRouter(prefix="/customer", tags=["Customer — Conversations"])

_CustomerActor = Depends(require_role(ROLE_CUSTOMER))


def _svc(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
) -> CustomerConversationService:
    return CustomerConversationService(session, background_tasks)


# ── Thread ─────────────────────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
    summary="Customer — get conversation thread + attachments",
)
async def customer_get_thread(
    ticket_id: str,
    actor:     CurrentActor               = _CustomerActor,
    svc:       CustomerConversationService = Depends(_svc),
) -> TicketThreadResponse:
    conversations, attachments = await svc.get_thread(ticket_id, actor.actor_id)
    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


# ── Reply ──────────────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/reply",
    response_model=ConversationItem,
    status_code=201,
    summary="Customer — post a reply (auto-reopens closed tickets)",
)
async def customer_post_reply(
    ticket_id: str,
    payload:   CustomerReplyRequest,
    actor:     CurrentActor               = _CustomerActor,
    svc:       CustomerConversationService = Depends(_svc),
) -> ConversationItem:
    convo = await svc.post_reply(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
        message=payload.message,
    )
    return ConversationItem.model_validate(convo)


# ── Close ──────────────────────────────────────────────────────────────────────

@router.patch(
    "/tickets/{ticket_id}/close",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Customer — close a resolved ticket",
)
async def customer_close_ticket(
    ticket_id: str,
    actor:     CurrentActor               = _CustomerActor,
    svc:       CustomerConversationService = Depends(_svc),
) -> None:
    await svc.close_ticket(ticket_id, actor.actor_id)


# ── Attachments ────────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
    summary="Customer — upload an attachment",
)
async def customer_upload_attachment(
    ticket_id: str,
    file:      UploadFile                  = File(...),
    actor:     CurrentActor               = _CustomerActor,
    svc:       CustomerConversationService = Depends(_svc),
) -> AttachmentItem:
    file_bytes = await file.read()
    att = await svc.upload_attachment(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
        filename=file.filename or "upload",
        file_bytes=file_bytes,
        mime_type=file.content_type,
    )
    return AttachmentItem.model_validate(att)


@router.get(
    "/tickets/{ticket_id}/attachments/{attachment_id}/signed-url",
    summary="Customer — get signed download URL for an attachment",
)
async def customer_get_attachment_signed_url(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor               = _CustomerActor,
    svc:           CustomerConversationService = Depends(_svc),
) -> dict:
    signed_url = await svc.get_attachment_signed_url(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
        attachment_id=attachment_id,
    )
    return {"url": signed_url}