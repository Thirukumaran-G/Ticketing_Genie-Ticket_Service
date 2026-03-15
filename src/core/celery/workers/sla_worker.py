"""
SLA Celery worker — periodic SLA monitoring tasks.
src/core/celery/workers/sla_worker.py

Response SLA  — breached when first_response_at is still None after response_time_min.
               'acknowledged' status does NOT cover response SLA.
               Only an agent posting their first message (which stamps first_response_at)
               covers response SLA.

Resolution SLA — breached when ticket is not resolved/closed after resolution_time_min.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.core.celery.app import celery_app
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# Statuses that mean the ticket is fully done — resolution SLA stops here
_CLOSED_STATUSES = {"resolved", "closed"}


@celery_app.task(name="ticket.sla.check_breaches")
def check_sla_breaches() -> None:
    """
    Response SLA  : breached when first_response_at is None and elapsed > response_time_min.
                    'acknowledged' or 'in_progress' status does NOT cover this —
                    only a stamped first_response_at does.
    Resolution SLA: breached when ticket not in resolved/closed and elapsed > resolution_time_min.
    On-hold time is subtracted from elapsed time so paused tickets are not falsely breached.
    Customer is NEVER notified of breach — only agent and TL.
    """

    async def _check() -> None:
        from src.core.celery.utils import fetch_customer_tier
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Notification
        from src.data.repositories.ticket_repository import (
            NotificationRepository,
            TicketRepository,
        )
        from src.data.repositories.admin_repository import SLARuleRepository

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            sla_repo    = SLARuleRepository(session)
            notif_repo  = NotificationRepository(session)

            open_tickets = await ticket_repo.get_open_unmonitored(limit=500)
            now          = datetime.now(timezone.utc)

            for ticket in open_tickets:
                try:
                    # Skip tickets that are fully closed — no SLA action needed
                    if ticket.status in _CLOSED_STATUSES:
                        continue

                    # Skip tickets currently on hold — SLA clock is paused
                    if ticket.status == "on_hold":
                        continue

                    # ── Resolve tier ──────────────────────────────────────────
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

                    # ── Effective elapsed time (excludes on-hold periods) ──────
                    raw_elapsed_min = (
                        now - ticket.created_at
                    ).total_seconds() / 60

                    hold_mins = ticket.on_hold_duration_accumulated or 0

                    effective_elapsed_min = max(raw_elapsed_min - hold_mins, 0)

                    # ── Response SLA ──────────────────────────────────────────
                    # Covered ONLY when first_response_at is stamped.
                    # 'acknowledged', 'in_progress', or any other status change
                    # does NOT count as a response — only the agent's first
                    # actual message stamps first_response_at.
                    if ticket.first_response_at is None:
                        resp_pct = (
                            effective_elapsed_min / sla.response_time_min
                        ) * 100

                        if (
                            resp_pct >= 100
                            and ticket.response_sla_breached_at is None
                        ):
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"response_sla_breached_at": now},
                            )

                            # Notify assigned agent
                            if ticket.assigned_to:
                                agent_notif = Notification(
                                    channel="in_app",
                                    recipient_id=ticket.assigned_to,
                                    ticket_id=ticket.id,
                                    is_internal=True,
                                    type="sla_breach",
                                    title=f"RESPONSE SLA BREACH: {ticket.ticket_number}",
                                    message=(
                                        f"Ticket {ticket.ticket_number} exceeded its "
                                        f"{sla.response_time_min} min first-response SLA. "
                                        f"Please submit a breach justification."
                                    ),
                                )
                                await notif_repo.create(agent_notif)

                            # Notify TL
                            await _notify_tl_on_breach(
                                ticket=ticket,
                                breach_type="response",
                                sla_minutes=sla.response_time_min,
                                notif_repo=notif_repo,
                                session=session,
                            )
                            logger.warning(
                                "response_sla_breached",
                                ticket_number=ticket.ticket_number,
                                effective_elapsed_min=round(effective_elapsed_min, 1),
                                sla_response_time_min=sla.response_time_min,
                                status=ticket.status,
                            )

                        elif 80 <= resp_pct < 100:
                            # Warning: response SLA at 80%+ but not yet breached
                            # Only warn if not already breached
                            resp_warn_notif = Notification(
                                channel="in_app",
                                recipient_id=(
                                    ticket.assigned_to or ticket.customer_id
                                ),
                                ticket_id=ticket.id,
                                is_internal=True,
                                type="sla_warning",
                                title=f"RESPONSE SLA WARNING: {ticket.ticket_number}",
                                message=(
                                    f"Ticket {ticket.ticket_number} has used "
                                    f"{resp_pct:.0f}% of its "
                                    f"{sla.response_time_min} min response SLA window. "
                                    f"Please respond to the customer."
                                ),
                            )
                            await notif_repo.create(resp_warn_notif)
                            logger.info(
                                "response_sla_warning_issued",
                                ticket_number=ticket.ticket_number,
                                pct=round(resp_pct, 1),
                            )

                    # ── Resolution SLA ────────────────────────────────────────
                    # Only check if ticket is NOT already resolved/closed.
                    # Covered when status becomes resolved or closed.
                    if ticket.status not in _CLOSED_STATUSES:
                        res_pct = (
                            effective_elapsed_min / sla.resolution_time_min
                        ) * 100

                        if 80 <= res_pct < 100 and ticket.sla_breached_at is None:
                            warn_notif = Notification(
                                channel="in_app",
                                recipient_id=(
                                    ticket.assigned_to or ticket.customer_id
                                ),
                                ticket_id=ticket.id,
                                is_internal=True,
                                type="sla_warning",
                                title=f"RESOLUTION SLA WARNING: {ticket.ticket_number}",
                                message=(
                                    f"Ticket {ticket.ticket_number} has used "
                                    f"{res_pct:.0f}% of its "
                                    f"{sla.resolution_time_min} min resolution SLA window."
                                ),
                            )
                            await notif_repo.create(warn_notif)
                            logger.info(
                                "resolution_sla_warning_issued",
                                ticket_number=ticket.ticket_number,
                                pct=round(res_pct, 1),
                            )

                        elif res_pct >= 100 and ticket.sla_breached_at is None:
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"sla_breached_at": now},
                            )

                            # Notify agent
                            if ticket.assigned_to:
                                agent_breach_notif = Notification(
                                    channel="in_app",
                                    recipient_id=ticket.assigned_to,
                                    ticket_id=ticket.id,
                                    is_internal=True,
                                    type="sla_breach",
                                    title=f"RESOLUTION SLA BREACH: {ticket.ticket_number}",
                                    message=(
                                        f"Ticket {ticket.ticket_number} breached its "
                                        f"{sla.resolution_time_min} min resolution SLA. "
                                        f"Please submit a breach justification."
                                    ),
                                )
                                await notif_repo.create(agent_breach_notif)

                            # Notify TL via alert_team_leads
                            from src.core.celery.workers.notification_worker import (
                                alert_team_leads,
                            )
                            alert_team_leads.delay(
                                ticket_id=str(ticket.id),
                                ticket_number=ticket.ticket_number,
                                title=ticket.title or "",
                                priority=ticket.priority or "P3",
                                severity=ticket.severity or "low",
                                assigned_to=(
                                    str(ticket.assigned_to) if ticket.assigned_to else ""
                                ),
                                assigned_to_name="Unknown Agent",
                                reason=(
                                    f"Resolution SLA breach — {res_pct:.0f}% of "
                                    f"{sla.resolution_time_min} min window elapsed."
                                ),
                            )
                            logger.warning(
                                "resolution_sla_breached",
                                ticket_number=ticket.ticket_number,
                                pct=round(res_pct, 1),
                                effective_elapsed_min=round(effective_elapsed_min, 1),
                                sla_resolution_time_min=sla.resolution_time_min,
                                status=ticket.status,
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
    breach_type: str,
    sla_minutes: int,
    notif_repo,
    session,
) -> None:
    """Find team lead for this ticket's team and push in_app breach notification."""
    try:
        from sqlalchemy import select
        from src.data.models.postgres.models import Notification, Team

        if not ticket.team_id:
            return

        r = await session.execute(
            select(Team).where(Team.id == ticket.team_id)
        )
        team = r.scalar_one_or_none()
        if not team or not team.team_lead_id:
            return

        tl_notif = Notification(
            channel="in_app",
            recipient_id=team.team_lead_id,
            ticket_id=ticket.id,
            is_internal=True,
            type="sla_breach",
            title=f"{breach_type.upper()} SLA BREACH: {ticket.ticket_number}",
            message=(
                f"Ticket {ticket.ticket_number} breached its "
                f"{breach_type} SLA ({sla_minutes} min). "
                f"Assigned agent has been notified to submit justification."
            ),
        )
        await notif_repo.create(tl_notif)

        # SSE push to TL immediately
        from src.core.sse.sse_manager import sse_manager
        await sse_manager.push(
            str(team.team_lead_id),
            {
                "event": "notification",
                "data": {
                    "type":          "sla_breach",
                    "title":         tl_notif.title,
                    "message":       tl_notif.message,
                    "ticket_number": ticket.ticket_number,
                },
            },
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
      - critical tickets: 15 min effective elapsed
      - high tickets:     30 min effective elapsed
    'acknowledged' does not count — only first_response_at matters.
    """

    async def _escalate() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            now         = datetime.now(timezone.utc)

            all_open = await ticket_repo.get_open_unmonitored(limit=500)

            for ticket in all_open:
                # Skip if agent already responded
                if ticket.first_response_at is not None:
                    continue

                # Skip on hold and closed
                if ticket.status in _CLOSED_STATUSES or ticket.status == "on_hold":
                    continue

                elapsed_min = (now - ticket.created_at).total_seconds() / 60
                hold_mins   = ticket.on_hold_duration_accumulated or 0
                effective   = max(elapsed_min - hold_mins, 0)

                needs_escalation = (
                    (ticket.priority == "critical" and effective >= 15)
                    or (ticket.priority == "high"     and effective >= 30)
                )

                if needs_escalation:
                    logger.warning(
                        "escalating_unresponded_ticket",
                        ticket_number=ticket.ticket_number,
                        priority=ticket.priority,
                        elapsed_min=round(effective, 1),
                        status=ticket.status,
                    )
                    from src.core.celery.workers.notification_worker import alert_team_leads
                    alert_team_leads.delay(
                        ticket_id=str(ticket.id),
                        ticket_number=ticket.ticket_number,
                        title=ticket.title or "",
                        priority=ticket.priority or "",
                        severity=ticket.severity or "",
                        assigned_to=(
                            str(ticket.assigned_to) if ticket.assigned_to else ""
                        ),
                        assigned_to_name="Unknown Agent",
                        reason=(
                            f"No agent first response after {effective:.0f} mins "
                            f"(effective, excluding hold time). "
                            f"Note: status='{ticket.status}' does not cover response SLA — "
                            f"only an agent message does. Immediate intervention required."
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