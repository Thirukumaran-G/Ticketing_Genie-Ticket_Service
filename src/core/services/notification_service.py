"""
Central notification service.
src/core/services/notification_service.py
"""
from __future__ import annotations

import uuid as _uuid
from typing import Optional

from fastapi import BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Notification
from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository
from src.data.repositories.ticket_repository import NotificationRepository
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class NotificationService:

    def __init__(
        self,
        session:          AsyncSession,
        background_tasks: Optional[BackgroundTasks] = None,
    ) -> None:
        self._session          = session
        self._background_tasks = background_tasks
        self._pref_repo        = NotificationPreferenceRepository(session)
        self._notif_repo       = NotificationRepository(session)

    async def notify(
        self,
        *,
        recipient_id:   str,
        ticket,
        notif_type:     str,
        title:          str,
        message:        str,
        is_internal:    bool        = False,
        email_subject:  str | None  = None,
        email_body:     str | None  = None,
        fallback_email: str | None  = None,
    ) -> None:
        try:
            channel = await self._pref_repo.get_preferred_contact(recipient_id)
        except Exception as exc:
            logger.warning(
                "notify_preference_lookup_failed",
                recipient_id=recipient_id,
                error=str(exc),
            )
            channel = "in_app"

        logger.info(
            "notify_routing",
            recipient_id=recipient_id,
            channel=channel,
            notif_type=notif_type,
            ticket_id=str(ticket.id),
        )

        if channel == "in_app":
            await self._send_in_app(
                recipient_id=recipient_id,
                ticket=ticket,
                notif_type=notif_type,
                title=title,
                message=message,
                is_internal=is_internal,
            )
        else:
            await self._send_email(
                recipient_id=recipient_id,
                subject=email_subject or title,
                body=email_body or message,
                fallback_email=fallback_email,
            )

    async def _send_in_app(
        self,
        *,
        recipient_id: str,
        ticket,
        notif_type:   str,
        title:        str,
        message:      str,
        is_internal:  bool,
    ) -> None:
        try:
            notif = Notification(
                channel="in_app",
                recipient_id=_uuid.UUID(recipient_id),
                ticket_id=ticket.id,
                is_internal=is_internal,
                type=notif_type,
                title=title,
                message=message,
            )
            await self._notif_repo.create(notif)
            await self._session.flush()

            payload = {
                "event": "notification",
                "data": {
                    "type":          notif_type,
                    "title":         title,
                    "message":       message,
                    "ticket_number": ticket.ticket_number,
                    "ticket_id":     str(ticket.id),
                },
            }

            if self._background_tasks is not None:
                # FastAPI route context — direct push to in-process SSE manager
                from src.core.sse.sse_manager import sse_manager
                delivered = await sse_manager.push(recipient_id, payload)
                logger.info(
                    "notify_in_app_sent",
                    recipient_id=recipient_id,
                    notif_type=notif_type,
                    ticket_id=str(ticket.id),
                    sse_delivered=delivered,
                    context="fastapi",
                )
            else:
                # Celery worker context — use Redis pub/sub to cross process boundary
                from src.core.sse.redis_subscriber import publish_notification
                from src.config.settings import settings
                publish_notification(
                    settings.CELERY_BROKER_URL,
                    recipient_id,
                    payload["data"],
                )
                logger.info(
                    "notify_in_app_sent",
                    recipient_id=recipient_id,
                    notif_type=notif_type,
                    ticket_id=str(ticket.id),
                    sse_delivered="redis",
                    context="celery",
                )

        except Exception as exc:
            logger.warning(
                "notify_in_app_failed",
                recipient_id=recipient_id,
                notif_type=notif_type,
                error=str(exc),
            )

    async def _send_email(
        self,
        *,
        recipient_id:   str,
        subject:        str,
        body:           str,
        fallback_email: str | None,
    ) -> None:
        try:
            email_addr = fallback_email
            if not email_addr:
                from src.handlers.http_clients.auth_client import AuthHttpClient
                email_addr = await AuthHttpClient().get_user_email(recipient_id)

            if not email_addr:
                logger.warning("notify_email_no_address", recipient_id=recipient_id)
                return

            if self._background_tasks is not None:
                self._background_tasks.add_task(
                    _send_email_bg,
                    email_addr=email_addr,
                    subject=subject,
                    body=body,
                )
                logger.info(
                    "notify_email_queued_bg",
                    recipient_id=recipient_id,
                    email=email_addr,
                )
            else:
                from src.core.celery.workers.email_worker import send_notification_email
                send_notification_email.delay(
                    recipient_id=recipient_id,
                    recipient_type="persona",
                    subject=subject,
                    body=body,
                    recipient_email=email_addr,
                )
                logger.info(
                    "notify_email_queued_celery",
                    recipient_id=recipient_id,
                    email=email_addr,
                )
        except Exception as exc:
            logger.warning(
                "notify_email_failed",
                recipient_id=recipient_id,
                error=str(exc),
            )


async def _send_email_bg(email_addr: str, subject: str, body: str) -> None:
    try:
        from src.handlers.http_clients.email_client import EmailClient
        await EmailClient().send_generic(to_email=email_addr, subject=subject, body=body)
        logger.info("bg_email_sent", email=email_addr, subject=subject)
    except Exception as exc:
        logger.error("bg_email_failed", email=email_addr, error=str(exc))