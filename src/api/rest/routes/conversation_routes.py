from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.api.rest.dependencies import (
    CurrentActor,
    ROLE_AGENT,
    ROLE_TEAM_LEAD,
    ROLE_CUSTOMER,
    require_role,
)
from src.core.services.conversation_service import ConversationService
from src.data.clients.postgres_client import get_db_session
from src.data.models.postgres.models import (
    Notification,
    Team,
    Ticket,
)
from src.data.repositories.ticket_repository import (
    NotificationRepository,
    TicketRepository,
)
from src.schemas.conversation_schema import (
    AgentReplyRequest,
    AttachmentItem,
    ConversationItem,
    CustomerReplyRequest,
    TicketThreadResponse,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Conversations"])


def _conv_svc(session: AsyncSession = Depends(get_db_session)) -> ConversationService:
    return ConversationService(session)


# ── Customer thread ───────────────────────────────────────────────────────────

@router.get(
    "/customer/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
    summary="Customer — get public conversation thread",
)
async def customer_get_thread(
    ticket_id: str,
    actor:     CurrentActor = Depends(require_role(ROLE_CUSTOMER)),
    svc:       ConversationService = Depends(_conv_svc),
) -> TicketThreadResponse:
    conversations = await svc.get_thread(
        ticket_id=ticket_id,
        include_internal=False,
    )
    attachments = await svc.get_attachments(ticket_id)
    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


@router.post(
    "/customer/tickets/{ticket_id}/reply",
    response_model=ConversationItem,
    status_code=201,
    summary="Customer — post a reply on their ticket",
)
async def customer_reply(
    ticket_id: str,
    payload:   CustomerReplyRequest,
    actor:     CurrentActor = Depends(require_role(ROLE_CUSTOMER)),
    svc:       ConversationService = Depends(_conv_svc),
) -> ConversationItem:
    convo = await svc.customer_reply(
        ticket_id=ticket_id,
        customer_id=actor.actor_id,
        content=payload.message,
    )
    return ConversationItem.model_validate(convo)


# ── Customer attachment ───────────────────────────────────────────────────────

@router.post(
    "/customer/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
    summary="Customer — upload attachment",
)
async def customer_upload_attachment(
    ticket_id: str,
    file:      UploadFile = File(...),
    actor:     CurrentActor = Depends(require_role(ROLE_CUSTOMER)),
    svc:       ConversationService = Depends(_conv_svc),
) -> AttachmentItem:
    file_bytes = await file.read()
    att = await svc.upload_attachment(
        ticket_id=ticket_id,
        uploader_id=actor.actor_id,
        filename=file.filename or "upload",
        file_bytes=file_bytes,
        mime_type=file.content_type,
    )
    return AttachmentItem.model_validate(att)


@router.get(
    "/customer/tickets/{ticket_id}/attachments/{attachment_id}",
    summary="Customer — download attachment",
)
async def customer_get_attachment(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor = Depends(require_role(ROLE_CUSTOMER)),
    svc:           ConversationService = Depends(_conv_svc),
):
    att = await svc.get_attachment_by_id(attachment_id, ticket_id)
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found.")
    path = svc.resolve_attachment_path(att.file_path)
    return FileResponse(
        path=str(path),
        filename=att.file_name,
        media_type=att.mime_type or "application/octet-stream",
    )


# ── Agent thread ──────────────────────────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
    summary="Agent — get full thread including internal notes",
)
async def agent_get_thread(
    ticket_id: str,
    actor:     CurrentActor = Depends(require_role(ROLE_AGENT)),
    svc:       ConversationService = Depends(_conv_svc),
) -> TicketThreadResponse:
    conversations = await svc.get_thread(
        ticket_id=ticket_id,
        include_internal=True,
    )
    attachments = await svc.get_attachments(ticket_id)
    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


@router.post(
    "/agent/tickets/{ticket_id}/comment",
    response_model=ConversationItem,
    status_code=201,
    summary="Agent — post reply or internal note",
)
async def agent_post_comment(
    ticket_id: str,
    payload:   AgentReplyRequest,
    actor:     CurrentActor = Depends(require_role(ROLE_AGENT)),
    svc:       ConversationService = Depends(_conv_svc),
    session:   AsyncSession = Depends(get_db_session),
) -> ConversationItem:
    convo = await svc.agent_reply(
        ticket_id=ticket_id,
        agent_id=actor.actor_id,
        content=payload.content,
        is_internal=payload.is_internal,
    )

    # ── Stamp first_response_at if this is the first public reply ─────────────
    if not payload.is_internal:
        await _stamp_first_response(
            session=session,
            ticket_id=ticket_id,
            agent_id=actor.actor_id,
        )

    # ── Internal note → notify TL in_app + SSE ────────────────────────────────
    if payload.is_internal:
        await _notify_tl_internal_note(
            session=session,
            ticket_id=ticket_id,
            agent_id=actor.actor_id,
            content=payload.content,
        )

    return ConversationItem.model_validate(convo)


# ── Agent attachment ──────────────────────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
    summary="Agent — upload attachment",
)
async def agent_upload_attachment(
    ticket_id: str,
    file:      UploadFile = File(...),
    actor:     CurrentActor = Depends(require_role(ROLE_AGENT)),
    svc:       ConversationService = Depends(_conv_svc),
) -> AttachmentItem:
    file_bytes = await file.read()
    att = await svc.upload_attachment(
        ticket_id=ticket_id,
        uploader_id=actor.actor_id,
        filename=file.filename or "upload",
        file_bytes=file_bytes,
        mime_type=file.content_type,
    )
    return AttachmentItem.model_validate(att)


@router.get(
    "/agent/tickets/{ticket_id}/attachments/{attachment_id}",
    summary="Agent — download attachment",
)
async def agent_get_attachment(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor = Depends(require_role(ROLE_AGENT)),
    svc:           ConversationService = Depends(_conv_svc),
):
    att = await svc.get_attachment_by_id(attachment_id, ticket_id)
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found.")
    path = svc.resolve_attachment_path(att.file_path)
    return FileResponse(
        path=str(path),
        filename=att.file_name,
        media_type=att.mime_type or "application/octet-stream",
    )


# ── TL thread ─────────────────────────────────────────────────────────────────

@router.get(
    "/teamlead/tickets/{ticket_id}/attachments/{attachment_id}",
    summary="TL — download attachment",
)
async def tl_get_attachment(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor = Depends(require_role(ROLE_TEAM_LEAD)),
    svc:           ConversationService = Depends(_conv_svc),
):
    att = await svc.get_attachment_by_id(attachment_id, ticket_id)
    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found.")
    path = svc.resolve_attachment_path(att.file_path)
    return FileResponse(
        path=str(path),
        filename=att.file_name,
        media_type=att.mime_type or "application/octet-stream",
    )


# ── TL internal note (already in team_lead_routes but notification wired here) ─

async def notify_agent_of_tl_note(
    session:   AsyncSession,
    ticket_id: str,
    lead_id:   str,
    content:   str,
) -> None:
    """
    Called from team_lead_routes.add_tl_internal_note after note is saved.
    Notifies the assigned agent in_app + SSE.
    """
    await _notify_agent_internal_note(
        session=session,
        ticket_id=ticket_id,
        author_id=lead_id,
        author_label="Team Lead",
        content=content,
    )


# ── Internal notification helpers ──────────────────────────────────────────────

async def _stamp_first_response(
    session:   AsyncSession,
    ticket_id: str,
    agent_id:  str,
) -> None:
    """
    Stamp first_response_at on the ticket if not already set.
    This stops the response SLA clock.
    """
    try:
        from datetime import datetime, timezone

        ticket_repo = TicketRepository(session)
        ticket      = await ticket_repo.get_by_id(ticket_id)
        if ticket and ticket.first_response_at is None:
            await ticket_repo.update_fields(ticket_id, {
                "first_response_at": datetime.now(timezone.utc),
            })
            await session.commit()
            logger.info(
                "first_response_stamped",
                ticket_id=ticket_id,
                agent_id=agent_id,
            )
    except Exception as exc:
        logger.warning(
            "stamp_first_response_failed",
            ticket_id=ticket_id,
            error=str(exc),
        )


async def _notify_tl_internal_note(
    session:   AsyncSession,
    ticket_id: str,
    agent_id:  str,
    content:   str,
) -> None:
    """
    Agent posted an internal note → notify TL in_app + SSE.
    Finds the team lead via ticket → team → team_lead_id.
    """
    try:
        ticket_result = await session.execute(
            select(Ticket).where(Ticket.id == uuid.UUID(ticket_id))
        )
        ticket = ticket_result.scalar_one_or_none()
        if not ticket or not ticket.team_id:
            return

        team_result = await session.execute(
            select(Team).where(Team.id == ticket.team_id)
        )
        team = team_result.scalar_one_or_none()
        if not team or not team.team_lead_id:
            return

        tl_id         = str(team.team_lead_id)
        preview       = content[:120] + ("…" if len(content) > 120 else "")
        notif_title   = f"Internal note on {ticket.ticket_number}"
        notif_message = (
            f"Agent posted an internal note on ticket {ticket.ticket_number}.\n\n"
            f"{preview}"
        )

        notif_repo = NotificationRepository(session)
        notif = Notification(
            channel="in_app",
            recipient_id=team.team_lead_id,
            ticket_id=ticket.id,
            is_internal=True,
            type="internal_note",
            title=notif_title,
            message=notif_message,
        )
        await notif_repo.create(notif)
        await session.commit()

        from src.core.sse.sse_manager import sse_manager
        import asyncio
        asyncio.create_task(
            sse_manager.push_notification(
                actor_id=tl_id,
                notif_type="internal_note",
                title=notif_title,
                message=notif_message,
                ticket_number=ticket.ticket_number,
                ticket_id=ticket_id,
            )
        )

        logger.info(
            "tl_notified_internal_note",
            tl_id=tl_id,
            ticket_id=ticket_id,
        )

    except Exception as exc:
        logger.warning(
            "notify_tl_internal_note_failed",
            ticket_id=ticket_id,
            error=str(exc),
        )


async def _notify_agent_internal_note(
    session:      AsyncSession,
    ticket_id:    str,
    author_id:    str,
    author_label: str,
    content:      str,
) -> None:
    """
    TL posted an internal note → notify assigned agent in_app + SSE.
    """
    try:
        ticket_result = await session.execute(
            select(Ticket).where(Ticket.id == uuid.UUID(ticket_id))
        )
        ticket = ticket_result.scalar_one_or_none()
        if not ticket or not ticket.assigned_to:
            return

        agent_id      = str(ticket.assigned_to)
        preview       = content[:120] + ("…" if len(content) > 120 else "")
        notif_title   = f"{author_label} note on {ticket.ticket_number}"
        notif_message = (
            f"{author_label} posted an internal note on your ticket "
            f"{ticket.ticket_number}.\n\n{preview}"
        )

        notif_repo = NotificationRepository(session)
        notif = Notification(
            channel="in_app",
            recipient_id=ticket.assigned_to,
            ticket_id=ticket.id,
            is_internal=True,
            type="internal_note",
            title=notif_title,
            message=notif_message,
        )
        await notif_repo.create(notif)
        await session.commit()

        from src.core.sse.sse_manager import sse_manager
        import asyncio
        asyncio.create_task(
            sse_manager.push_notification(
                actor_id=agent_id,
                notif_type="internal_note",
                title=notif_title,
                message=notif_message,
                ticket_number=ticket.ticket_number,
                ticket_id=ticket_id,
            )
        )

        logger.info(
            "agent_notified_internal_note",
            agent_id=agent_id,
            ticket_id=ticket_id,
            author_label=author_label,
        )

    except Exception as exc:
        logger.warning(
            "notify_agent_internal_note_failed",
            ticket_id=ticket_id,
            error=str(exc),
        )