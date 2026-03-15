from __future__ import annotations

import asyncio

from src.core.celery.app import celery_app
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(name="ticket.notification.alert_team_lead")
def alert_team_lead(
    ticket_id:        str,
    ticket_number:    str,
    title:            str,
    priority:         str,
    severity:         str,
    assigned_to:      str,
    assigned_to_name: str,
    team_lead_id:     str,
    reason:           str,
) -> None:
    """
    Notify only the specific team's team lead about a critical ticket assignment.
    Fully preference-aware — in_app OR email, same message content, not both.
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

            # Fetch ticket for relationship
            result = await session.execute(
                select(Ticket).where(Ticket.id == _uuid.UUID(ticket_id))
            )
            ticket = result.scalar_one_or_none()
            if not ticket:
                logger.warning("alert_team_lead_ticket_not_found", ticket_id=ticket_id)
                return

            notif_title   = f"[P0] Critical ticket {ticket_number} assigned"
            notif_message = (
                f"Title: {title}\n"
                f"Priority: {priority} | Severity: {severity}\n"
                f"Assigned to: {assigned_to_name}\n"
                f"{reason}"
            )

            try:
                channel = await pref_repo.get_preferred_contact(team_lead_id)
            except Exception as exc:
                logger.warning(
                    "alert_team_lead_pref_fetch_failed",
                    team_lead_id=team_lead_id,
                    error=str(exc),
                )
                channel = "in_app"  # safe default

            if channel == "in_app":
                notif = Notification(
                    channel="in_app",
                    recipient_id=_uuid.UUID(team_lead_id),
                    ticket_id=_uuid.UUID(ticket_id),
                    is_internal=True,
                    type="critical_ticket_assigned",
                    title=notif_title,
                    message=notif_message,
                )
                await notif_repo.create(notif)
                await session.commit()

                publish_notification(
                    settings.CELERY_BROKER_URL,
                    team_lead_id,
                    {
                        "type":          "critical_ticket_assigned",
                        "title":         notif_title,
                        "message":       notif_message,
                        "ticket_number": ticket_number,
                    },
                )
            else:
                # email
                try:
                    from src.core.celery.utils import fetch_user_email
                    lead_email = await fetch_user_email(_uuid.UUID(team_lead_id))
                except Exception as exc:
                    logger.warning(
                        "alert_team_lead_email_fetch_failed",
                        team_lead_id=team_lead_id,
                        error=str(exc),
                    )
                    lead_email = None

                if lead_email:
                    from src.core.celery.workers.email_worker import send_notification_email
                    send_notification_email.delay(
                        recipient_id=team_lead_id,
                        recipient_type="persona",
                        subject=f"[P0] Critical ticket {ticket_number} assigned — heads up",
                        body=(
                            f"Hi,\n\n"
                            f"A critical ticket has been auto-assigned.\n\n"
                            f"Ticket:      {ticket_number}\n"
                            f"Title:       {title}\n"
                            f"Priority:    {priority}\n"
                            f"Severity:    {severity}\n"
                            f"Assigned to: {assigned_to_name}\n\n"
                            f"{reason}\n\n"
                            f"— Ticketing Genie"
                        ),
                        recipient_email=lead_email,
                    )

            logger.info(
                "critical_ticket_team_lead_notified",
                team_lead_id=team_lead_id,
                ticket_id=ticket_id,
                channel=channel,
            )

    asyncio.run(_run())