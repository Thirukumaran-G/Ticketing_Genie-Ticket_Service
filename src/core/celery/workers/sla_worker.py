"""
SLA Celery worker — periodic SLA monitoring tasks.
src/core/celery/workers/sla_worker.py
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.core.celery.app import celery_app
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_CLOSED_STATUSES = {"resolved", "closed"}


@celery_app.task(name="ticket.sla.check_breaches")
def check_sla_breaches() -> None:
    """
    Uses sla_response_due and sla_resolve_due stamped on the ticket at
    classification time — no need to re-fetch SLA rules or customer tier.
    This is simpler, faster, and works even if the auth service is down.
    """

    async def _check() -> None:
        from src.core.services.notification_service import NotificationService
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            ticket_repo  = TicketRepository(session)
            open_tickets = await ticket_repo.get_open_unmonitored(limit=500)
            now          = datetime.now(timezone.utc)

            for ticket in open_tickets:
                try:
                    if ticket.status in _CLOSED_STATUSES:
                        continue
                    if ticket.status == "on_hold":
                        continue

                    # Skip tickets with no SLA deadlines set yet
                    if not ticket.sla_response_due and not ticket.sla_resolve_due:
                        continue

                    notif_svc = NotificationService(session)

                    # ── Response SLA ──────────────────────────────────────────
                    if (
                        ticket.sla_response_due
                        and ticket.first_response_at is None
                        and ticket.response_sla_breached_at is None
                    ):
                        resp_due   = ticket.sla_response_due
                        total_mins = (resp_due - ticket.created_at).total_seconds() / 60
                        elapsed    = (now - ticket.created_at).total_seconds() / 60
                        hold_mins  = ticket.on_hold_duration_accumulated or 0
                        effective  = max(elapsed - hold_mins, 0)
                        resp_pct   = (effective / total_mins * 100) if total_mins > 0 else 100

                        if now >= resp_due:
                            # Breached — stamp it
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"response_sla_breached_at": now},
                            )
                            logger.warning(
                                "response_sla_breached",
                                ticket_number=ticket.ticket_number,
                                elapsed_min=round(effective, 1),
                            )

                            if ticket.assigned_to:
                                await notif_svc.notify(
                                    recipient_id=str(ticket.assigned_to),
                                    ticket=ticket,
                                    notif_type="sla_breach",
                                    title=f"RESPONSE SLA BREACH: {ticket.ticket_number}",
                                    message=(
                                        f"Ticket {ticket.ticket_number} exceeded its response SLA. "
                                        f"Please submit a breach justification."
                                    ),
                                    is_internal=True,
                                    email_subject=f"[{ticket.ticket_number}] Response SLA Breached",
                                    email_body=(
                                        f"Hi,\n\nTicket {ticket.ticket_number} has breached its "
                                        f"response SLA deadline.\n\n"
                                        f"Please submit a breach justification immediately.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                )

                            await _notify_tl_on_breach(
                                ticket=ticket,
                                breach_type="response",
                                sla_minutes=int(total_mins),
                                notif_svc=notif_svc,
                                session=session,
                            )

                        elif resp_pct >= 80:
                            if ticket.assigned_to:
                                await notif_svc.notify(
                                    recipient_id=str(ticket.assigned_to),
                                    ticket=ticket,
                                    notif_type="sla_warning",
                                    title=f"RESPONSE SLA WARNING: {ticket.ticket_number}",
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used "
                                        f"{resp_pct:.0f}% of its response SLA window. "
                                        f"Please respond to the customer."
                                    ),
                                    is_internal=True,
                                    email_subject=f"[{ticket.ticket_number}] Response SLA Warning",
                                    email_body=(
                                        f"Hi,\n\nTicket {ticket.ticket_number} has used "
                                        f"{resp_pct:.0f}% of its response SLA window.\n\n"
                                        f"Please respond to the customer as soon as possible.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                )
                            logger.info(
                                "response_sla_warning_issued",
                                ticket_number=ticket.ticket_number,
                                pct=round(resp_pct, 1),
                            )

                    # ── Resolution SLA ────────────────────────────────────────
                    if (
                        ticket.sla_resolve_due
                        and ticket.status not in _CLOSED_STATUSES
                        and ticket.sla_breached_at is None
                    ):
                        res_due    = ticket.sla_resolve_due
                        total_mins = (res_due - ticket.created_at).total_seconds() / 60
                        elapsed    = (now - ticket.created_at).total_seconds() / 60
                        hold_mins  = ticket.on_hold_duration_accumulated or 0
                        effective  = max(elapsed - hold_mins, 0)
                        res_pct    = (effective / total_mins * 100) if total_mins > 0 else 100

                        if now >= res_due:
                            # Breached — stamp it
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"sla_breached_at": now},
                            )
                            logger.warning(
                                "resolution_sla_breached",
                                ticket_number=ticket.ticket_number,
                                pct=round(res_pct, 1),
                                elapsed_min=round(effective, 1),
                            )

                            if ticket.assigned_to:
                                await notif_svc.notify(
                                    recipient_id=str(ticket.assigned_to),
                                    ticket=ticket,
                                    notif_type="sla_breach",
                                    title=f"RESOLUTION SLA BREACH: {ticket.ticket_number}",
                                    message=(
                                        f"Ticket {ticket.ticket_number} breached its resolution SLA. "
                                        f"Please submit a breach justification."
                                    ),
                                    is_internal=True,
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA Breached",
                                    email_body=(
                                        f"Hi,\n\nTicket {ticket.ticket_number} has breached its "
                                        f"resolution SLA deadline.\n\n"
                                        f"Please submit a breach justification immediately.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                )

                            await _notify_tl_on_breach(
                                ticket=ticket,
                                breach_type="resolution",
                                sla_minutes=int(total_mins),
                                notif_svc=notif_svc,
                                session=session,
                            )

                            # Alert TL via notification_worker for escalation
                            if ticket.team_id:
                                from sqlalchemy import select
                                from src.data.models.postgres.models import Team
                                r    = await session.execute(
                                    select(Team).where(Team.id == ticket.team_id)
                                )
                                team = r.scalar_one_or_none()
                                if team and team.team_lead_id:
                                    from src.core.celery.workers.notification_worker import alert_team_lead
                                    alert_team_lead.delay(
                                        ticket_id=str(ticket.id),
                                        ticket_number=ticket.ticket_number,
                                        title=ticket.title or "",
                                        priority=ticket.priority or "P3",
                                        severity=ticket.severity or "low",
                                        assigned_to=str(ticket.assigned_to) if ticket.assigned_to else "",
                                        assigned_to_name="Unknown Agent",
                                        team_lead_id=str(team.team_lead_id),
                                        reason=(
                                            f"Resolution SLA breach — {res_pct:.0f}% of window elapsed."
                                        ),
                                    )

                        elif res_pct >= 80:
                            if ticket.assigned_to:
                                await notif_svc.notify(
                                    recipient_id=str(ticket.assigned_to),
                                    ticket=ticket,
                                    notif_type="sla_warning",
                                    title=f"RESOLUTION SLA WARNING: {ticket.ticket_number}",
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used "
                                        f"{res_pct:.0f}% of its resolution SLA window."
                                    ),
                                    is_internal=True,
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA Warning",
                                    email_body=(
                                        f"Hi,\n\nTicket {ticket.ticket_number} has used "
                                        f"{res_pct:.0f}% of its resolution SLA window.\n\n"
                                        f"Please work to resolve this ticket promptly.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                )
                            logger.info(
                                "resolution_sla_warning_issued",
                                ticket_number=ticket.ticket_number,
                                pct=round(res_pct, 1),
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


async def _notify_tl_on_breach(
    ticket,
    breach_type:  str,
    sla_minutes:  int,
    notif_svc,
    session,
) -> None:
    try:
        from sqlalchemy import select
        from src.data.models.postgres.models import Team

        if not ticket.team_id:
            return

        r    = await session.execute(select(Team).where(Team.id == ticket.team_id))
        team = r.scalar_one_or_none()
        if not team or not team.team_lead_id:
            return

        tl_id = str(team.team_lead_id)
        title = f"{breach_type.upper()} SLA BREACH: {ticket.ticket_number}"
        msg   = (
            f"Ticket {ticket.ticket_number} breached its "
            f"{breach_type} SLA ({sla_minutes} min). "
            f"Assigned agent has been notified to submit justification."
        )

        await notif_svc.notify(
            recipient_id=tl_id,
            ticket=ticket,
            notif_type="sla_breach",
            title=title,
            message=msg,
            is_internal=True,
            email_subject=f"[{ticket.ticket_number}] {breach_type.title()} SLA Breached",
            email_body=(
                f"Hi,\n\nTicket {ticket.ticket_number} has breached its "
                f"{breach_type} SLA ({sla_minutes} min).\n\n"
                f"The assigned agent has been notified to submit a justification.\n\n"
                f"— Ticketing Genie"
            ),
        )

        logger.info(
            "tl_sla_breach_notified",
            tl_id=tl_id,
            ticket_id=str(ticket.id),
            breach_type=breach_type,
        )

    except Exception as exc:
        logger.warning(
            "tl_breach_notification_failed",
            ticket_id=str(ticket.id),
            error=str(exc),
        )


# ── Escalate Critical Unresponded ─────────────────────────────────────────────

@celery_app.task(name="ticket.sla.escalate_critical")
def escalate_critical_unresponded() -> None:
    """
    Alert TLs if no first response within threshold:
      - critical/P0: 15 min effective elapsed
      - high/P1:     30 min effective elapsed
    """

    async def _escalate() -> None:
        from sqlalchemy import select
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository
        from src.data.models.postgres.models import Team

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            now         = datetime.now(timezone.utc)
            all_open    = await ticket_repo.get_open_unmonitored(limit=500)

            for ticket in all_open:
                if ticket.first_response_at is not None:
                    continue
                if ticket.status in _CLOSED_STATUSES or ticket.status == "on_hold":
                    continue

                elapsed_min = (now - ticket.created_at).total_seconds() / 60
                hold_mins   = ticket.on_hold_duration_accumulated or 0
                effective   = max(elapsed_min - hold_mins, 0)

                needs_escalation = (
                    (ticket.priority in ("P0", "critical") and effective >= 15)
                    or (ticket.priority in ("P1", "high")  and effective >= 30)
                )

                if needs_escalation:
                    logger.warning(
                        "escalating_unresponded_ticket",
                        ticket_number=ticket.ticket_number,
                        priority=ticket.priority,
                        elapsed_min=round(effective, 1),
                    )

                    if ticket.team_id:
                        r    = await session.execute(
                            select(Team).where(Team.id == ticket.team_id)
                        )
                        team = r.scalar_one_or_none()
                        if team and team.team_lead_id:
                            from src.core.celery.workers.notification_worker import alert_team_lead
                            alert_team_lead.delay(
                                ticket_id=str(ticket.id),
                                ticket_number=ticket.ticket_number,
                                title=ticket.title or "",
                                priority=ticket.priority or "",
                                severity=ticket.severity or "",
                                assigned_to=str(ticket.assigned_to) if ticket.assigned_to else "",
                                assigned_to_name="Unknown Agent",
                                team_lead_id=str(team.team_lead_id),
                                reason=(
                                    f"No first response after {effective:.0f} mins "
                                    f"(excluding hold time). Immediate intervention required."
                                ),
                            )

    asyncio.run(_escalate())


# ── Auto-Close Resolved ───────────────────────────────────────────────────────

@celery_app.task(name="ticket.sla.auto_close")
def auto_close_resolved_tickets() -> None:
    """Auto-close resolved tickets with no customer response for 72h."""

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