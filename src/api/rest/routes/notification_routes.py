"""
Notification routes.
src/api/rest/routes/notification_routes.py
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Literal

from src.api.rest.dependencies import (
    CurrentActor,
    get_current_actor,
    get_current_actor_from_token,
)
from src.core.sse.sse_manager import sse_manager
from src.data.clients.postgres_client import get_db_session
from src.data.repositories.ticket_repository import NotificationRepository
from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository
from src.schemas.ticket_schema import NotificationResponse
from src.schemas.notification_preference_schema import NotificationPreferenceResponse

router = APIRouter(prefix="/notifications", tags=["Notifications"])

_AnyActor = Depends(get_current_actor)


def _notif_repo(
    session: AsyncSession = Depends(get_db_session),
) -> NotificationRepository:
    return NotificationRepository(session)


def _pref_repo(
    session: AsyncSession = Depends(get_db_session),
) -> NotificationPreferenceRepository:
    return NotificationPreferenceRepository()


# ── Fetch notifications ───────────────────────────────────────────────────────

@router.get(
    "",
    response_model=list[NotificationResponse],
    summary="Fetch recent notifications for current user",
)
async def list_notifications(
    unread_only: bool = False,
    actor: CurrentActor = _AnyActor,
    repo: NotificationRepository = Depends(_notif_repo),
) -> list[NotificationResponse]:
    notifs = await repo.get_for_user(actor.actor_id, unread_only=unread_only)
    return [NotificationResponse.model_validate(n) for n in notifs]


# ── Unread count (for sidebar badge) ─────────────────────────────────────────

@router.get(
    "/unread-count",
    summary="Get count of unread notifications for current user",
)
async def get_unread_count(
    actor: CurrentActor = _AnyActor,
    repo:  NotificationRepository = Depends(_notif_repo),
) -> dict:
    """
    Lightweight endpoint for the notification badge in the sidebar.
    Returns { "count": int } — only unread notifications.
    """
    notifs = await repo.get_for_user(actor.actor_id, unread_only=True)
    return {"count": len(notifs)}


# ── SSE stream ────────────────────────────────────────────────────────────────

@router.get(
    "/stream",
    summary="SSE stream — real-time notifications (token passed as query param)",
    response_class=StreamingResponse,
)
async def notification_stream(
    token: str = Query(..., description="JWT — EventSource cannot send Authorization header"),
) -> StreamingResponse:
    """
    EventSource cannot send headers so JWT is passed as ?token=...

    Events:
      event: notification  → new notification for this user
      event: read_receipt  → after PATCH /{id}/read
      event: internal_note → internal note posted on a ticket
      : keepalive          → every 30s
    """
    actor = await get_current_actor_from_token(token)

    async def _stream():
        async with sse_manager.subscribe(actor.actor_id) as q:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=30)
                    event = payload["event"]
                    data  = json.dumps(payload["data"])
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


# ── Mark as read ──────────────────────────────────────────────────────────────

@router.patch(
    "/{notif_id}/read",
    status_code=204,
    response_class=Response,
    response_model=None,
    summary="Mark a notification as read — pushes read_receipt SSE event",
)
async def mark_notification_read(
    notif_id: str,
    actor:    CurrentActor = _AnyActor,
    repo:     NotificationRepository = Depends(_notif_repo),
    session:  AsyncSession = Depends(get_db_session),
) -> None:
    updated = await repo.mark_read(notif_id, actor.actor_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Notification not found.")
    await session.commit()

    # ── Fixed: new sse_manager.push() takes (actor_id, payload: dict) ─────────
    await sse_manager.push(
        actor.actor_id,
        {
            "event": "read_receipt",
            "data":  {"id": notif_id, "is_read": True},
        },
    )


# ── Notification preference ───────────────────────────────────────────────────

class SetPreferenceRequest(BaseModel):
    preferred_contact: Literal["email", "in_app"]


@router.get(
    "/preference",
    response_model=NotificationPreferenceResponse,
    summary="Get current user's notification preference",
)
async def get_preference(
    actor: CurrentActor = _AnyActor,
    repo:  NotificationPreferenceRepository = Depends(_pref_repo),
) -> NotificationPreferenceResponse:
    channel = await repo.get_preferred_contact(actor.actor_id)
    return NotificationPreferenceResponse(
        user_id=actor.actor_id,
        preferred_contact=channel,
    )


@router.put(
    "/preference",
    response_model=NotificationPreferenceResponse,
    summary="Set current user's notification preference",
)
async def set_preference(
    body:    SetPreferenceRequest,
    actor:   CurrentActor = _AnyActor,
    repo:    NotificationPreferenceRepository = Depends(_pref_repo),
    session: AsyncSession = Depends(get_db_session),
) -> NotificationPreferenceResponse:
    await repo.set_preferred_contact(actor.actor_id, body.preferred_contact)
    await session.commit()
    return NotificationPreferenceResponse(
        user_id=actor.actor_id,
        preferred_contact=body.preferred_contact,
    )


@router.patch(
    "/preference/toggle",
    response_model=NotificationPreferenceResponse,
    summary="Toggle notification preference email ↔ in_app",
)
async def toggle_preference(
    actor:   CurrentActor = _AnyActor,
    repo:    NotificationPreferenceRepository = Depends(_pref_repo),
    session: AsyncSession = Depends(get_db_session),
) -> NotificationPreferenceResponse:
    current     = await repo.get_preferred_contact(actor.actor_id)
    new_channel = "email" if current == "in_app" else "in_app"
    await repo.set_preferred_contact(actor.actor_id, new_channel)
    await session.commit()
    return NotificationPreferenceResponse(
        user_id=actor.actor_id,
        preferred_contact=new_channel,
    )