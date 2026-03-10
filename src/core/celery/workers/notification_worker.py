"""Notification Celery worker — internal alert helpers.

alert_team_leads: broadcasts a notification + email to all active team leads.
"""

from __future__ import annotations

import asyncio

from src.core.celery.app import celery_app
from src.core.celery.utils import fetch_agent_name, fetch_user_email
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(name="ticket.notification.alert_tls")
def alert_team_leads(
    ticket_id: str,
    ticket_number: str,
    title: str,
    priority: str,
    severity: str,
    assigned_to: str,
    reason: str,
    assigned_to_name: str = "Unknown Agent",
) -> None:
    """Broadcast a TL alert notification + email to all active team leads."""

    async def _notify() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Notification, Team
        from src.data.repositories.ticket_repository import NotificationRepository
        from sqlalchemy import select
        import uuid

        async with CelerySessionFactory() as session:
            notif_repo = NotificationRepository(session)

            # ── Collect distinct active team lead user IDs from Team table ────
            result = await session.execute(
                select(Team.team_lead_id).where(
                    Team.is_active.is_(True),
                    Team.team_lead_id.is_not(None),
                ).distinct()
            )
            tl_user_ids: list[uuid.UUID] = [row for (row,) in result.fetchall()]

            if not tl_user_ids:
                logger.warning("no_team_leads_found", ticket_number=ticket_number)
                return

            for tl_user_id in tl_user_ids:
                tl_name  = await fetch_agent_name(tl_user_id) or "Team Lead"
                tl_email = await fetch_user_email(tl_user_id)

                notif = Notification(
                    channel="in_app",
                    recipient_id=tl_user_id,          # user_id from Team.team_lead_id
                    ticket_id=uuid.UUID(ticket_id) if ticket_id else None,
                    is_internal=True,
                    type="tl_alert",
                    title=f"[{priority.upper()}] {ticket_number} — {reason[:80]}",
                    message=(
                        f"Ticket: {ticket_number}\n"
                        f"Title: {title}\n"
                        f"Priority: {priority} | Severity: {severity}\n"
                        f"Assigned to: {assigned_to_name}\n"
                        f"Reason: {reason}"
                    ),
                )
                await notif_repo.create(notif)

                if tl_email:
                    from src.core.celery.workers.email_worker import send_notification_email
                    send_notification_email.delay(
                        recipient_id=str(tl_user_id),
                        recipient_type="persona",
                        subject=f"[{priority.upper()}] Ticket {ticket_number} needs attention",
                        body=(
                            f"Hi {tl_name},\n\n"
                            f"Ticket {ticket_number} alert.\n\n"
                            f"Title: {title}\n"
                            f"Priority: {priority} | Severity: {severity}\n"
                            f"Assigned to: {assigned_to_name}\n"
                            f"Reason: {reason}\n\n"
                            f"Please monitor SLA compliance.\n\n"
                            f"— Ticketing Genie"
                        ),
                        recipient_email=tl_email,
                    )
                else:
                    logger.warning(
                        "tl_alert_email_skipped",
                        ticket_number=ticket_number,
                        tl_user_id=str(tl_user_id),
                    )

            await session.commit()

    asyncio.run(_notify())