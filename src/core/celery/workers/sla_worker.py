"""
SLA Celery worker — periodic SLA monitoring tasks.
ticket-service: src/core/celery/workers/sla_worker.py

Tasks:
  check_sla_breaches         : warn at 80%, alert + TL on breach (runs every minute)
  escalate_critical_unresponded : escalate if no first response (runs every 5 min)
  auto_close_resolved         : auto-close resolved tickets older than 72 h (runs hourly)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.core.celery.app import celery_app
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


# ── SLA Breach Check ──────────────────────────────────────────────────────────

@celery_app.task(name="ticket.sla.check_breaches")
def check_sla_breaches() -> None:
    """Warn at 80% SLA elapsed; stamp breach and alert TL at 100%."""

    async def _check() -> None:
        from src.core.celery.utils import fetch_customer_tier
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Notification
        from src.data.repositories.ticket_repository import NotificationRepository
        from src.data.repositories.admin_repository import SLARuleRepository
        from src.data.repositories.ticket_repository import NotificationRepository
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            sla_repo    = SLARuleRepository(session)
            notif_repo  = NotificationRepository(session)

            open_tickets = await ticket_repo.get_open_unmonitored(limit=500)
            now = datetime.now(timezone.utc)

            for ticket in open_tickets:
                try:
                    # ── Resolve tier_id via auth-service HTTP ─────────────────
                    # Pass product_id to get the right subscription tier when a
                    # company has multiple product subscriptions.
                    tier_name, tier_id = await fetch_customer_tier(
                        str(ticket.customer_id),
                        str(ticket.product_id) if ticket.product_id else None,
                    )

                    if not tier_id:
                        logger.warning(
                            "sla_check_no_tier_id",
                            ticket_id=str(ticket.id),
                            tier_name=tier_name,
                        )
                        continue

                    sla = await sla_repo.get_by_tier_and_priority(
                        tier_id, ticket.priority or "P3"
                    )
                    if not sla:
                        logger.warning(
                            "sla_check_no_rule",
                            ticket_id=str(ticket.id),
                            tier_id=tier_id,
                            priority=ticket.priority,
                        )
                        continue

                    elapsed_min = (now - ticket.created_at).total_seconds() / 60

                    # ── Response SLA ──────────────────────────────────────────
                    if ticket.first_response_at is None:
                        resp_pct = (elapsed_min / sla.response_time_min) * 100

                        if resp_pct >= 100 and ticket.response_sla_breached_at is None:
                            await ticket_repo.update_fields(str(ticket.id), {
                                "response_sla_breached_at": now,
                            })
                            notif = Notification(
                                channel="in_app",
                                recipient_id=ticket.assigned_to or ticket.customer_id,
                                ticket_id=ticket.id,
                                is_internal=True,
                                type="sla_breach",
                                title=f"RESPONSE SLA BREACH: {ticket.ticket_number}",
                                message=(
                                    f"Ticket {ticket.ticket_number} exceeded its "
                                    f"{sla.response_time_min} min first-response SLA."
                                ),
                            )
                            await notif_repo.create(notif)
                            logger.warning(
                                "response_sla_breached",
                                ticket_number=ticket.ticket_number,
                            )

                    # ── Resolution SLA ────────────────────────────────────────
                    res_pct = (elapsed_min / sla.resolution_time_min) * 100

                    if 80 <= res_pct < 100:
                        notif = Notification(
                            channel="in_app",
                            recipient_id=ticket.assigned_to or ticket.customer_id,
                            ticket_id=ticket.id,
                            is_internal=True,
                            type="sla_warning",
                            title=f"SLA WARNING: {ticket.ticket_number}",
                            message=(
                                f"Ticket {ticket.ticket_number} has used "
                                f"{res_pct:.0f}% of its "
                                f"{sla.resolution_time_min} min SLA window."
                            ),
                        )
                        await notif_repo.create(notif)
                        logger.info(
                            "sla_warning_issued",
                            ticket_number=ticket.ticket_number,
                            pct=round(res_pct, 1),
                        )

                    elif res_pct >= 100 and ticket.sla_breached_at is None:
                        await ticket_repo.update_fields(str(ticket.id), {
                            "sla_breached_at": now,
                        })
                        notif = Notification(
                            channel="in_app",
                            recipient_id=ticket.assigned_to or ticket.customer_id,
                            ticket_id=ticket.id,
                            is_internal=True,
                            type="sla_breach",
                            title=f"SLA BREACH: {ticket.ticket_number}",
                            message=(
                                f"Ticket {ticket.ticket_number} breached its "
                                f"{sla.resolution_time_min} min resolution SLA."
                            ),
                        )
                        await notif_repo.create(notif)
                        logger.warning(
                            "resolution_sla_breached",
                            ticket_number=ticket.ticket_number,
                            pct=round(res_pct, 1),
                        )

                        from src.core.celery.workers.notification_worker import alert_team_leads
                        alert_team_leads.delay(
                            ticket_id=str(ticket.id),
                            ticket_number=ticket.ticket_number,
                            title=ticket.title or "",
                            priority=ticket.priority or "P3",
                            severity=ticket.severity or "low",
                            assigned_to=str(ticket.assigned_to) if ticket.assigned_to else "",
                            assigned_to_name="Unknown Agent",
                            reason=(
                                f"SLA breach — {res_pct:.0f}% of "
                                f"{sla.resolution_time_min} min window elapsed."
                            ),
                        )

                except Exception as exc:
                    logger.error(
                        "sla_check_ticket_error",
                        ticket_id=str(ticket.id),
                        error=str(exc),
                    )
                    continue

            await session.commit()

    asyncio.run(_check())


# ── Escalate Critical Unresponded ─────────────────────────────────────────────

@celery_app.task(name="ticket.sla.escalate_critical")
def escalate_critical_unresponded() -> None:
    """Alert TLs if no first response within threshold (critical=15 min, high=30 min)."""

    async def _escalate() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            now         = datetime.now(timezone.utc)

            all_open = await ticket_repo.get_open_unmonitored(limit=500)

            for ticket in all_open:
                if ticket.first_response_at is not None:
                    continue

                elapsed_min = (now - ticket.created_at).total_seconds() / 60

                needs_escalation = (
                    (ticket.priority == "critical" and elapsed_min >= 15)
                    or (ticket.priority == "high"     and elapsed_min >= 30)
                )

                if needs_escalation:
                    logger.warning(
                        "escalating_unresponded_ticket",
                        ticket_number=ticket.ticket_number,
                        priority=ticket.priority,
                        elapsed_min=round(elapsed_min, 1),
                    )
                    from src.core.celery.workers.notification_worker import alert_team_leads
                    alert_team_leads.delay(
                        ticket_id=str(ticket.id),
                        ticket_number=ticket.ticket_number,
                        title=ticket.title or "",
                        priority=ticket.priority or "",
                        severity=ticket.severity or "",
                        assigned_to=str(ticket.assigned_to) if ticket.assigned_to else "",
                        assigned_to_name="Unknown Agent",
                        reason=(
                            f"No agent response after {elapsed_min:.0f} mins — "
                            f"immediate intervention required."
                        ),
                    )

    asyncio.run(_escalate())


# ── Auto-Close Resolved ───────────────────────────────────────────────────────

@celery_app.task(name="ticket.sla.auto_close")
def auto_close_resolved_tickets() -> None:
    """Auto-close resolved tickets that received no customer response for 72 h."""

    async def _close() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            repo    = TicketRepository(session)
            tickets = await repo.get_resolved_past_sla()
            now     = datetime.now(timezone.utc)

            closed = 0
            for t in tickets:
                await repo.update_fields(str(t.id), {
                    "status":    "closed",
                    "closed_at": now,
                })
                closed += 1

            await session.commit()
            if closed:
                logger.info("auto_closed_resolved_tickets", count=closed)

    asyncio.run(_close())