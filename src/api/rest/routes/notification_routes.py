from __future__ import annotations

import asyncio
import json
from typing import Literal

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import (
    CurrentActor,
    get_current_actor,
    get_current_actor_from_token,
)
from src.core.sse.sse_manager import sse_manager
from src.core.services.notification_service import NotificationService
from src.data.clients.postgres_client import get_db_session
from src.schemas.notification_preference_schema import NotificationPreferenceResponse
from src.schemas.ticket_schema import NotificationResponse

router = APIRouter(prefix="/notifications", tags=["Notifications"])

_AnyActor = Depends(get_current_actor)


def _svc(session: AsyncSession = Depends(get_db_session)) -> NotificationService:
    return NotificationService(session)


# ── Fetch ─────────────────────────────────────────────────────────────────────

@router.get(
    "",
    response_model=list[NotificationResponse],
    summary="Fetch recent notifications for current user",
)
async def list_notifications(
    unread_only: bool = False,
    actor: CurrentActor      = _AnyActor,
    svc:   NotificationService = Depends(_svc),
) -> list[NotificationResponse]:
    return await svc.list_notifications(actor.actor_id, unread_only=unread_only)


# ── Unread count ──────────────────────────────────────────────────────────────

@router.get(
    "/unread-count",
    summary="Get count of unread notifications for current user",
)
async def get_unread_count(
    actor: CurrentActor      = _AnyActor,
    svc:   NotificationService = Depends(_svc),
) -> dict:
    return {"count": await svc.get_unread_count(actor.actor_id)}


@router.get(
    "/stream",
    summary="SSE stream — real-time notifications (token passed as query param)",
    response_class=StreamingResponse,
)
async def notification_stream(
    token: str = Query(..., description="JWT — EventSource cannot send Authorization header"),
) -> StreamingResponse:
    actor = await get_current_actor_from_token(token)

    async def _stream():
        async with sse_manager.subscribe(actor.actor_id) as q:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=30)
                    event   = payload["event"]
                    data    = json.dumps(payload["data"])
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
    actor:    CurrentActor      = _AnyActor,
    svc:      NotificationService = Depends(_svc),
) -> None:
    await svc.mark_read(notif_id, actor.actor_id)


# ── Preferences ───────────────────────────────────────────────────────────────

class SetPreferenceRequest(BaseModel):
    preferred_contact: Literal["email", "in_app"]


@router.get(
    "/preference",
    response_model=NotificationPreferenceResponse,
    summary="Get current user's notification preference",
)
async def get_preference(
    actor: CurrentActor      = _AnyActor,
    svc:   NotificationService = Depends(_svc),
) -> NotificationPreferenceResponse:
    return await svc.get_preference(actor.actor_id)


@router.put(
    "/preference",
    response_model=NotificationPreferenceResponse,
    summary="Set current user's notification preference",
)
async def set_preference(
    body:  SetPreferenceRequest,
    actor: CurrentActor      = _AnyActor,
    svc:   NotificationService = Depends(_svc),
) -> NotificationPreferenceResponse:
    return await svc.set_preference(actor.actor_id, body.preferred_contact)


@router.patch(
    "/preference/toggle",
    response_model=NotificationPreferenceResponse,
    summary="Toggle notification preference email ↔ in_app",
)
async def toggle_preference(
    actor: CurrentActor      = _AnyActor,
    svc:   NotificationService = Depends(_svc),
) -> NotificationPreferenceResponse:
    return await svc.toggle_preference(actor.actor_id)