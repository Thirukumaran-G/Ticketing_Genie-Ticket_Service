"""
Agent conversation + attachment routes.
src/api/rest/routes/agent_conversation_routes.py

Endpoints:
  GET  /agent/tickets/{ticket_id}/thread
       → TicketThreadResponse  (all conversations incl. internal + attachments)

  POST /agent/tickets/{ticket_id}/comment
       → ConversationItem  (201)  [supports is_internal flag]

  POST /agent/tickets/{ticket_id}/attachments
       → AttachmentItem    (201)

  GET  /agent/tickets/{ticket_id}/attachments/{attachment_id}
       → FileResponse

Security:
  - Ticket ownership is verified via agent_service.get_ticket_by_id which checks
    Ticket.assigned_to == agent_id, returning 404 otherwise.
  - Agents see internal notes; customers do not.

Status side-effect:
  When an agent posts a non-internal reply and the ticket status is "new" or
  "acknowledged", the status is automatically advanced to "in_progress" and
  first_response_at is stamped (if not already set).
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, ROLE_AGENT, require_role
from src.core.services.agent_services import AgentService
from src.core.services.conversation_service import ConversationService
from src.data.clients.postgres_client import get_db_session
from src.data.repositories.ticket_repository import TicketRepository
from src.schemas.conversation_schema import (
    AgentReplyRequest,
    AttachmentItem,
    ConversationItem,
    TicketThreadResponse,
)

router = APIRouter(tags=["Agent — Conversations"])

_AgentActor = Depends(require_role(ROLE_AGENT))


def _agent_svc(session: AsyncSession = Depends(get_db_session)) -> AgentService:
    return AgentService(session)


def _conv_svc(session: AsyncSession = Depends(get_db_session)) -> ConversationService:
    return ConversationService(session)


def _ticket_repo(session: AsyncSession = Depends(get_db_session)) -> TicketRepository:
    return TicketRepository(session)


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _assert_agent_owns_ticket(
    ticket_id: str,
    agent_id:  str,
    agent_svc: AgentService,
) -> None:
    """Raise 404 via ValueError if ticket not assigned to agent."""
    try:
        await agent_svc.get_ticket_by_id(agent_id=agent_id, ticket_id=ticket_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Ticket not found.")


# ── Thread ─────────────────────────────────────────────────────────────────────

@router.get(
    "/agent/tickets/{ticket_id}/thread",
    response_model=TicketThreadResponse,
    summary="Agent — get full conversation thread + attachments",
)
async def agent_get_thread(
    ticket_id: str,
    actor:     CurrentActor       = _AgentActor,
    agent_svc: AgentService       = Depends(_agent_svc),
    conv_svc:  ConversationService = Depends(_conv_svc),
) -> TicketThreadResponse:
    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)

    conversations = await conv_svc.get_thread(
        ticket_id=ticket_id,
        include_internal=True,   # agents see internal notes
    )
    attachments = await conv_svc.get_attachments(ticket_id=ticket_id)

    return TicketThreadResponse(
        conversations=[ConversationItem.model_validate(c) for c in conversations],
        attachments=[AttachmentItem.model_validate(a) for a in attachments],
    )


# ── Comment / reply ────────────────────────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/comment",
    response_model=ConversationItem,
    status_code=201,
    summary="Agent — post a comment or internal note",
    description=(
        "Post a reply visible to the customer (is_internal=false) "
        "or an internal note only visible to agents (is_internal=true). "
        "A public reply on a new/acknowledged ticket auto-advances status to "
        "'in_progress' and stamps first_response_at."
    ),
)
async def agent_post_comment(
    ticket_id:   str,
    payload:     AgentReplyRequest,
    actor:       CurrentActor       = _AgentActor,
    agent_svc:   AgentService       = Depends(_agent_svc),
    conv_svc:    ConversationService = Depends(_conv_svc),
    ticket_repo: TicketRepository   = Depends(_ticket_repo),
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

    # Auto-advance status + stamp first response on first public reply
    if not payload.is_internal:
        ticket = await ticket_repo.get_by_id(ticket_id)
        if ticket and ticket.status in ("new", "acknowledged"):
            updates: dict = {"status": "in_progress"}
            if not ticket.first_response_at:
                updates["first_response_at"] = datetime.now(timezone.utc)
            await ticket_repo.update_fields(ticket_id, updates)

    return ConversationItem.model_validate(convo)


# ── Upload attachment ──────────────────────────────────────────────────────────

@router.post(
    "/agent/tickets/{ticket_id}/attachments",
    response_model=AttachmentItem,
    status_code=201,
    summary="Agent — upload an attachment",
)
async def agent_upload_attachment(
    ticket_id: str,
    file:      UploadFile        = File(...),
    actor:     CurrentActor      = _AgentActor,
    agent_svc: AgentService      = Depends(_agent_svc),
    conv_svc:  ConversationService = Depends(_conv_svc),
) -> AttachmentItem:
    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)

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
    "/agent/tickets/{ticket_id}/attachments/{attachment_id}",
    summary="Agent — download / view an attachment",
    response_class=FileResponse,
)
async def agent_get_attachment(
    ticket_id:     str,
    attachment_id: str,
    actor:         CurrentActor      = _AgentActor,
    agent_svc:     AgentService      = Depends(_agent_svc),
    conv_svc:      ConversationService = Depends(_conv_svc),
) -> FileResponse:
    await _assert_agent_owns_ticket(ticket_id, actor.actor_id, agent_svc)

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