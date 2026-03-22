from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.api.rest.dependencies import CurrentActor, ROLE_CUSTOMER, require_role
from src.core.exceptions.base import NotFoundException
from src.core.services.conversation_service import ConversationService
from src.core.services.notification_service import NotificationService
from src.core.services.ticket_service import TicketService
from src.data.clients.postgres_client import get_db_session
from src.data.models.postgres.models import Notification, Ticket
from src.data.repositories.ticket_repository import NotificationRepository, TicketRepository
from src.schemas.conversation_schema import (
    AttachmentItem,
    ConversationItem,
    CustomerReplyRequest,
    TicketThreadResponse,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/customer", tags=["Customer — Conversations"])

_CustomerActor = Depends(require_role(ROLE_CUSTOMER))


def _ticket_svc(session: AsyncSession = Depends(get_db_session)) -> TicketService:
    return TicketService(session)


def _conv_svc(session: AsyncSession = Depends(get_db_session)) -> ConversationService:
    return ConversationService(session)


async def _assert_owns_ticket(
    ticket_id:   str,
    customer_id: str,
    ticket_svc:  TicketService,
) -> None:
    try:
        await ticket_svc.get_my_ticket(ticket_id=ticket_id, customer_id=customer_id)
    except NotFoundException:
        raise HTTPException(status_code=404, detail="Ticket not found.")


async def _notify_agent_ticket_reopened(
    session:   AsyncSession,
    ticket_id: str,
) -> None:
    try:
        result = await session.execute(
            select(Ticket).where(Ticket.id == uuid.UUID(ticket_id))
        )
        ticket = result.scalar_one_or_none()
        if not ticket or not ticket.assigned_to:
            logger.info("reopen_notify_skipped_no_assignee", ticket_id=ticket_id)
            return

        agent_id      = str(ticket.assigned_to)
        notif_title   = f"Ticket {ticket.ticket_number} reopened"
        notif_message = (
            f"Customer replied to closed ticket {ticket.ticket_number}. "
            f"It has been automatically reopened and is assigned back to you."
        )

        notif_repo = NotificationRepository(session)
        notif = Notification(
            channel="in_app",
            recipient_id=ticket.assigned_to,
            ticket_id=ticket.id,
            is_internal=False,
            type="ticket_reopened",
            title=notif_title,
            message=notif_message,
        )
        await notif_repo.create(notif)
        await session.commit()

        from src.core.sse.sse_manager import sse_manager
        await sse_manager.push_notification(
            actor_id=agent_id,
            notif_type="ticket_reopened",
            title=notif_title,
            message=notif_message,
            ticket_number=ticket.ticket_number,
            ticket_id=ticket_id,
        )

        logger.info(
            "agent_notified_ticket_reopened",
            agent_id=agent_id,
            ticket_id=ticket_id,
            ticket_number=ticket.ticket_number,
        )

    except Exception as exc:
        logger.warning(
            "notify_agent_ticket_reopened_failed",
            ticket_id=ticket_id,
            error=str(exc),
        )


@router.get(
    "/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
    summary="Customer — get conversation thread + attachments",
)
async def customer_get_thread(
    ticket_id:  str,
    actor:      CurrentActor        = _CustomerActor,
    ticket_svc: TicketService       = Depends(_ticket_svc),
    conv_svc:   ConversationService = Depends(_conv_svc),
) -> TicketThreadResponse:
    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)
    conversations = await conv_svc.get_thread(ticket_id=ticket_id, include_internal=False)
    attachments   = await conv_svc.get_attachments(ticket_id=ticket_id)
    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


@router.post(
    "/tickets/{ticket_id}/reply",
    response_model=ConversationItem,
    status_code=201,
    summary="Customer — post a reply (auto-reopens closed tickets)",
)
async def customer_post_reply(
    ticket_id:        str,
    payload:          CustomerReplyRequest,
    background_tasks: BackgroundTasks,
    actor:            CurrentActor = _CustomerActor,
    session:          AsyncSession = Depends(get_db_session),
) -> ConversationItem:
    if len(payload.message.strip()) < 5:
        raise HTTPException(status_code=422, detail="Message must be at least 5 characters.")

    ticket_svc  = TicketService(session)
    conv_svc    = ConversationService(session)
    ticket_repo = TicketRepository(session)

    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    ticket = await ticket_repo.get_by_id(ticket_id)

    if ticket and ticket.status == "closed":
        await ticket_repo.update_fields(ticket_id, {
            "status":       "reopened",
            "reopen_count": (ticket.reopen_count or 0) + 1,
            "closed_at":    None,
            "closed_by":    None,
        })
        await session.commit()

        logger.info(
            "ticket_reopened_by_customer_reply",
            ticket_id=ticket_id,
            ticket_number=ticket.ticket_number,
            reopen_count=(ticket.reopen_count or 0) + 1,
        )

        await _notify_agent_ticket_reopened(session=session, ticket_id=ticket_id)

    convo = await conv_svc.customer_reply(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
        content=payload.message,
    )

    ticket = await ticket_repo.get_by_id(ticket_id)
    if ticket and ticket.assigned_to:
        notif_svc = NotificationService(session, background_tasks)
        await notif_svc.notify(
            recipient_id=str(ticket.assigned_to),
            ticket=ticket,
            notif_type="customer_reply",
            title=f"Customer replied on {ticket.ticket_number}",
            message=f"[{ticket.ticket_number}] Customer: {payload.message[:200]}",
            is_internal=False,
            email_subject=f"[{ticket.ticket_number}] New customer reply",
            email_body=(
                f"Hi,\n\nA customer has replied on ticket {ticket.ticket_number}.\n\n"
                f"Message:\n{payload.message}\n\n"
                f"— Ticketing Genie"
            ),
        )
        logger.info(
            "agent_notified_customer_reply",
            ticket_id=ticket_id,
            agent_id=str(ticket.assigned_to),
        )

    return ConversationItem.model_validate(convo)


@router.patch(
    "/tickets/{ticket_id}/close",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Customer — close a resolved ticket",
)
async def customer_close_ticket(
    ticket_id: str,
    actor:     CurrentActor = _CustomerActor,
    session:   AsyncSession = Depends(get_db_session),
) -> None:
    ticket_svc  = TicketService(session)
    ticket_repo = TicketRepository(session)

    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    ticket = await ticket_repo.get_by_id(ticket_id)

    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found.")

    if ticket.status != "resolved":
        raise HTTPException(
            status_code=422,
            detail=f"Ticket cannot be closed — current status is '{ticket.status}'. "
                   f"Only resolved tickets may be closed.",
        )

    await ticket_repo.update_fields(ticket_id, {
        "status":    "closed",
        "closed_at": datetime.now(timezone.utc),
        "closed_by": uuid.UUID(actor.actor_id),
    })
    await session.commit()

    logger.info(
        "ticket_closed_by_customer",
        ticket_id=ticket_id,
        ticket_number=ticket.ticket_number,
        customer_id=actor.actor_id,
    )


@router.post(
    "/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
    summary="Customer — upload an attachment",
)
async def customer_upload_attachment(
    ticket_id: str,
    file:      UploadFile   = File(...),
    actor:     CurrentActor = _CustomerActor,
    session:   AsyncSession = Depends(get_db_session),
) -> AttachmentItem:
    ticket_svc = TicketService(session)
    conv_svc   = ConversationService(session)

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


@router.get(
    "/tickets/{ticket_id}/attachments/{attachment_id}/signed-url",
    summary="Customer — get signed download URL for an attachment",
)
async def customer_get_attachment_signed_url(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor = _CustomerActor,
    session:       AsyncSession = Depends(get_db_session),
) -> dict:
    ticket_svc = TicketService(session)
    conv_svc   = ConversationService(session)

    await _assert_owns_ticket(ticket_id, actor.actor_id, ticket_svc)

    attachment = await conv_svc.get_attachment_by_id(
        attachment_id=attachment_id,
        ticket_id=ticket_id,
    )
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found.")

    signed_url = conv_svc.get_download_url(attachment.file_path)
    return {"url": signed_url}