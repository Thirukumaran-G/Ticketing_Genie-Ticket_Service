from __future__ import annotations

from src.core.celery.app import celery_app
from src.core.celery.loop import run_async
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


# ── Universal notification delivery ──────────────────────────────────────────
#
# Single delivery layer for ALL notifications across the entire Celery app.
#
# sla_worker  → notify_recipient.delay()
# ai_worker   → notify_recipient.delay()
#
# Checks recipient's channel preference from the repository.
# Delivers via in_app (SSE + DB row) OR email_worker — never both.
#
# Exception: ticket_raised to customer always delivers both in_app + email
# as a transactional receipt. That is handled directly in ai_worker and
# does not go through this task.
# ─────────────────────────────────────────────────────────────────────────────

@celery_app.task(name="ticket.notification.notify_recipient")
def notify_recipient(
    recipient_id:   str,
    recipient_type: str,    # "agent" | "team_lead" | "customer"
    ticket_id:      str,
    ticket_number:  str,
    notif_type:     str,
    title:          str,
    message:        str,
    email_subject:  str,
    email_body:     str,
    is_internal:    bool,
) -> None:
    """
    Universal notification delivery.

    Fetches the recipient's preferred channel from the repository.
    Delivers in_app (SSE push + DB row) OR queues an email via email_worker.
    Never both.
    """

    async def _run() -> None:
        from src.config.settings import settings
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Notification, Ticket
        from src.data.repositories.ticket_repository import NotificationRepository
        from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository
        from src.core.sse.redis_subscriber import publish_notification
        import uuid as _uuid
        from sqlalchemy import select

        async with CelerySessionFactory() as session:
            notif_repo = NotificationRepository(session)
            pref_repo  = NotificationPreferenceRepository(session)

            # Verify ticket exists
            result = await session.execute(
                select(Ticket).where(Ticket.id == _uuid.UUID(ticket_id))
            )
            ticket = result.scalar_one_or_none()
            if not ticket:
                logger.warning(
                    "notify_recipient_ticket_not_found",
                    ticket_id=ticket_id,
                    recipient_id=recipient_id,
                )
                return

            # Fetch channel preference
            try:
                channel = await pref_repo.get_preferred_contact(recipient_id)
            except Exception as exc:
                logger.warning(
                    "notify_recipient_pref_fetch_failed",
                    recipient_id=recipient_id,
                    recipient_type=recipient_type,
                    error=str(exc),
                )
                channel = "in_app"  # safe default

            if channel == "in_app":
                # ── In-app delivery ───────────────────────────────────────────
                notif = Notification(
                    channel="in_app",
                    recipient_id=_uuid.UUID(recipient_id),
                    ticket_id=_uuid.UUID(ticket_id),
                    is_internal=is_internal,
                    type=notif_type,
                    title=title,
                    message=message,
                )
                await notif_repo.create(notif)
                await session.commit()

                publish_notification(
                    settings.CELERY_BROKER_URL,
                    recipient_id,
                    {
                        "type":          notif_type,
                        "title":         title,
                        "message":       message,
                        "ticket_number": ticket_number,
                        "ticket_id":     ticket_id,
                    },
                )
                logger.info(
                    "notify_recipient_in_app_delivered",
                    recipient_id=recipient_id,
                    recipient_type=recipient_type,
                    notif_type=notif_type,
                    ticket_number=ticket_number,
                )

            else:
                # ── Email delivery ────────────────────────────────────────────
                try:
                    from src.core.celery.utils import fetch_user_email
                    email_addr = await fetch_user_email(_uuid.UUID(recipient_id))
                except Exception as exc:
                    logger.warning(
                        "notify_recipient_email_fetch_failed",
                        recipient_id=recipient_id,
                        error=str(exc),
                    )
                    email_addr = None

                if email_addr:
                    from src.core.celery.workers.email_worker import send_notification_email
                    send_notification_email.delay(
                        recipient_id=recipient_id,
                        recipient_type=recipient_type,
                        subject=email_subject,
                        body=email_body,
                        recipient_email=email_addr,
                    )
                    logger.info(
                        "notify_recipient_email_queued",
                        recipient_id=recipient_id,
                        recipient_type=recipient_type,
                        notif_type=notif_type,
                        ticket_number=ticket_number,
                    )
                else:
                    logger.warning(
                        "notify_recipient_email_skipped_no_address",
                        recipient_id=recipient_id,
                        recipient_type=recipient_type,
                        ticket_number=ticket_number,
                    )

    run_async(_run())