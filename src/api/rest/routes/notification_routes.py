"""
Notification routes — in-app notification stream + read status.
src/api/rest/routes/notification_routes.py

Endpoints:
  GET   /notifications              — fetch recent notifications (initial load)
  GET   /notifications/stream       — SSE stream — pushed on any new notification
  PATCH /notifications/{id}/read    — mark notification as read
"""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.rest.dependencies import CurrentActor, get_current_actor
from src.core.sse.sse_manager import sse_manager
from src.data.clients.postgres_client import get_db_session
from src.data.repositories.ticket_repository import NotificationRepository
from src.schemas.ticket_schema import NotificationResponse

router = APIRouter(prefix="/notifications", tags=["Notifications"])

_AnyActor = Depends(get_current_actor)


def _notif_repo(session: AsyncSession = Depends(get_db_session)) -> NotificationRepository:
    return NotificationRepository(session)


# ── Fetch recent notifications ────────────────────────────────────────────────

@router.get(
    "",
    response_model=list[NotificationResponse],
    summary="Fetch recent notifications for current user (initial load)",
)
async def list_notifications(
    unread_only: bool = False,
    actor: CurrentActor = _AnyActor,
    repo: NotificationRepository = Depends(_notif_repo),
) -> list[NotificationResponse]:
    notifs = await repo.get_for_user(actor.actor_id, unread_only=unread_only)
    return [NotificationResponse.model_validate(n) for n in notifs]


# ── SSE notification stream ───────────────────────────────────────────────────

@router.get(
    "/stream",
    summary="SSE stream — real-time in-app notifications for current user",
    response_class=StreamingResponse,
)
async def notification_stream(
    actor: CurrentActor = _AnyActor,
) -> StreamingResponse:
    """
    SSE stream — client connects on login, keeps alive.
    Events:
      event: notification   → new notification row created for this user
                              data: NotificationResponse JSON
      event: read_receipt   → after PATCH /notifications/{id}/read
                              data: { "id": "<notif_id>", "is_read": true }
      : keepalive           → every 30s
    """
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
    summary="Mark a notification as read — pushes read_receipt SSE event",
    response_class=Response,
    response_model=None
)
async def mark_notification_read(
    notif_id: str,
    actor: CurrentActor = _AnyActor,
    repo: NotificationRepository = Depends(_notif_repo),
    session: AsyncSession = Depends(get_db_session),
) -> None:
    updated = await repo.mark_read(notif_id, actor.actor_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Notification not found.")
    await session.commit()

    # Push read_receipt back to user's SSE stream — badge count update
    await sse_manager.push(
        actor.actor_id,
        event="read_receipt",
        data={"id": notif_id, "is_read": True},
    )