"""
Customer conversation + attachment routes.
src/api/rest/routes/customer_conversation_routes.py

Endpoints:
  GET  /customer/tickets/{ticket_id}/thread
       → TicketThreadResponse  (conversations + attachment metadata)

  POST /customer/tickets/{ticket_id}/reply
       → ConversationItem  (201)

  POST /customer/tickets/{ticket_id}/attachments
       → AttachmentItem    (201)

  GET  /customer/tickets/{ticket_id}/attachments/{attachment_id}
       → FileResponse (streams the file)

Security:
  - Every endpoint verifies the ticket belongs to the authenticated customer
    (via ticket_service.get_my_ticket which returns 404 for any other owner).
  - Attachment download is similarly scoped to the ticket.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_CUSTOMER, require_role
from src.core.exceptions.base import NotFoundException
from src.core.services.conversation_service import ConversationService
from src.core.services.ticket_service import TicketService
from src.data.clients.postgres_client import get_db_session
from src.schemas.conversation_schema import (
    AttachmentItem,
    ConversationItem,
    CustomerReplyRequest,
    TicketThreadResponse,
)

router = APIRouter(prefix="/customer", tags=["Customer — Conversations"])

_CustomerActor = Depends(require_role(ROLE_CUSTOMER))


def _ticket_svc(session: AsyncSession = Depends(get_db_session)) -> TicketService:
    return TicketService(session)


def _conv_svc(session: AsyncSession = Depends(get_db_session)) -> ConversationService:
    return ConversationService(session)


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _assert_owns_ticket(
    ticket_id: str,
    customer_id: str,
    ticket_svc: TicketService,
) -> None:
    """Raise 404 if the ticket does not belong to this customer."""
    try:
        await ticket_svc.get_my_ticket(ticket_id=ticket_id, customer_id=customer_id)
    except NotFoundException:
        raise HTTPException(status_code=404, detail="Ticket not found.")


# ── Thread ─────────────────────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
    summary="Customer — get conversation thread + attachments",
)
async def customer_get_thread(
    ticket_id: str,
    actor:      CurrentActor      = _CustomerActor,
    ticket_svc: TicketService     = Depends(_ticket_svc),
    conv_svc:   ConversationService = Depends(_conv_svc),
) -> TicketThreadResponse:
    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    conversations = await conv_svc.get_thread(
        ticket_id=ticket_id,
        include_internal=False,   # customers never see internal notes
    )
    attachments = await conv_svc.get_attachments(ticket_id=ticket_id)

    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


# ── Reply ──────────────────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/reply",
    response_model=ConversationItem,
    status_code=201,
    summary="Customer — post a reply",
)
async def customer_post_reply(
    ticket_id: str,
    payload:    CustomerReplyRequest,
    actor:      CurrentActor      = _CustomerActor,
    ticket_svc: TicketService     = Depends(_ticket_svc),
    conv_svc:   ConversationService = Depends(_conv_svc),
) -> ConversationItem:
    if len(payload.message.strip()) < 5:
        raise HTTPException(status_code=422, detail="Message must be at least 5 characters.")

    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    convo = await conv_svc.customer_reply(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
        content=payload.message,
    )
    return ConversationItem.model_validate(convo)


# ── Upload attachment ──────────────────────────────────────────────────────────

@router.post(
    "/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
    summary="Customer — upload an attachment",
    description=(
        "Upload a file (image, PDF, log, etc.) attached to the ticket. "
        "Max 25 MB. Allowed types: images, PDFs, office docs, archives, logs."
    ),
)
async def customer_upload_attachment(
    ticket_id: str,
    file:       UploadFile        = File(...),
    actor:      CurrentActor      = _CustomerActor,
    ticket_svc: TicketService     = Depends(_ticket_svc),
    conv_svc:   ConversationService = Depends(_conv_svc),
) -> AttachmentItem:
    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    file_bytes = await file.read()

    try:
        attachment = await conv_svc.upload_attachment(
            ticket_id=ticket_id,
            uploader_id=actor.actor_id,
            filename=file.filename or "upload",
            file_bytes=file_bytes,
            mime_type=file.content_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return AttachmentItem.model_validate(attachment)


# ── Download attachment ────────────────────────────────────────────────────────

@router.get(
    "/tickets/{ticket_id}/attachments/{attachment_id}",
    summary="Customer — download / view an attachment",
    response_class=FileResponse,
)
async def customer_get_attachment(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor      = _CustomerActor,
    ticket_svc:    TicketService     = Depends(_ticket_svc),
    conv_svc:      ConversationService = Depends(_conv_svc),
) -> FileResponse:
    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    attachment = await conv_svc.get_attachment_by_id(
        attachment_id=attachment_id,
        ticket_id=ticket_id,
    )
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found.")

    abs_path = conv_svc.resolve_attachment_path(attachment.file_path)
    if not abs_path.exists():
        raise HTTPException(status_code=404, detail="File not found on server.")

    return FileResponse(
        path=str(abs_path),
        filename=attachment.file_name,
        media_type=attachment.mime_type or "application/octet-stream",
    )