from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.core.celery.app import celery_app
from src.core.celery.loop import run_async
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_CLOSED_STATUSES = {"resolved", "closed"}

_TIER_LABEL = {
    "enterprise": "Enterprise",
    "standard":   "Standard",
    "starter":    "Starter",
}


def _tier(ticket) -> str:
    raw = (ticket.tier_snapshot or "").lower().strip()
    return _TIER_LABEL.get(raw, ticket.tier_snapshot or "N/A")


# ── SLA breach and warning check ──────────────────────────────────────────────

@celery_app.task(name="ticket.sla.check_breaches")
def check_sla_breaches() -> None:
    """
    Runs every minute via Celery beat.

    Response SLA  : warn agent + TL at 80%, 90%, breach at 100% — each once.
    Resolution SLA: warn agent + TL at 80%, 90%, breach at 100% — each once.

    All notifications are delivered via notify_recipient.delay()
    which handles channel preference (in_app OR email).
    """

    async def _check() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository
        from src.core.celery.workers.notification_worker import notify_recipient
        from sqlalchemy import select
        from src.data.models.postgres.models import Team

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
                    if not ticket.sla_response_due and not ticket.sla_resolve_due:
                        continue

                    tier = _tier(ticket)

                    # Fetch TL id once per ticket
                    tl_id = None
                    if ticket.team_id:
                        r    = await session.execute(
                            select(Team).where(Team.id == ticket.team_id)
                        )
                        team = r.scalar_one_or_none()
                        if team and team.team_lead_id:
                            tl_id = str(team.team_lead_id)

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
                        mins_left  = max(round(total_mins - effective), 0)

                        if now >= resp_due:
                            # ── 100% Breach ───────────────────────────────────
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
                                notify_recipient.delay(
                                    recipient_id=str(ticket.assigned_to),
                                    recipient_type="agent",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_breach",
                                    title=(
                                        f"You have breached the response SLA "
                                        f"on {ticket.ticket_number}"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has exceeded its response "
                                        f"SLA window of {int(total_mins)} minutes. "
                                        f"The customer has not received a first response from you. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Tier: {tier}. "
                                        f"Please respond to the customer immediately and submit "
                                        f"a breach justification through the portal."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Response SLA breached — "
                                        f"your immediate action is required"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its "
                                        f"response SLA.\n\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n\n"
                                        f"The customer has not received a first response within "
                                        f"the allowed window.\n\n"
                                        f"Please respond to the customer immediately and submit "
                                        f"a breach justification through the portal.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                            if tl_id:
                                notify_recipient.delay(
                                    recipient_id=tl_id,
                                    recipient_type="team_lead",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_breach",
                                    title=(
                                        f"Response SLA breached — {ticket.ticket_number} "
                                        f"has had no agent reply"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has breached its response "
                                        f"SLA ({int(total_mins)} min window). "
                                        f"The customer has not received a first response. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Customer Tier: {tier}. "
                                        f"The assigned agent has been notified and must submit "
                                        f"a breach justification. Please review and intervene "
                                        f"if necessary."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Response SLA breached — "
                                        f"immediate action required"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its "
                                        f"response SLA.\n\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n\n"
                                        f"The customer has not received a first response. "
                                        f"The assigned agent has been notified and must submit "
                                        f"a breach justification immediately.\n\n"
                                        f"Please log in to the portal to review and take action.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                        elif resp_pct >= 90 and ticket.response_sla_warned_pct != 90:
                            # ── 90% Warning ───────────────────────────────────
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"response_sla_warned_pct": 90},
                            )
                            logger.warning(
                                "response_sla_warning_90",
                                ticket_number=ticket.ticket_number,
                                pct=round(resp_pct, 1),
                            )

                            if ticket.assigned_to:
                                notify_recipient.delay(
                                    recipient_id=str(ticket.assigned_to),
                                    recipient_type="agent",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"Only {mins_left} minute(s) left to respond — "
                                        f"{ticket.ticket_number} is at 90% of its response SLA"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"response SLA window ({int(total_mins)} min total). "
                                        f"You have approximately {mins_left} minute(s) remaining "
                                        f"to send your first response to the customer. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Tier: {tier}. "
                                        f"Respond immediately to avoid a breach."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Only {mins_left} min left "
                                        f"to respond — act now"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"response SLA window.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please respond to the customer immediately to "
                                        f"avoid an SLA breach.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                            if tl_id:
                                notify_recipient.delay(
                                    recipient_id=tl_id,
                                    recipient_type="team_lead",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"Response SLA at 90% — {ticket.ticket_number} "
                                        f"agent has not replied yet"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"response SLA ({int(total_mins)} min total). "
                                        f"Approximately {mins_left} minute(s) remaining. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Customer Tier: {tier}. "
                                        f"The assigned agent has been notified. "
                                        f"Please monitor and intervene if the agent does not respond."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Response SLA at 90% — "
                                        f"monitor and intervene if needed"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"response SLA window.\n\n"
                                        f"SLA Type      : Response\n"
                                        f"Warning Level : 90% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. If they do not "
                                        f"respond in time, a breach notification will follow.\n\n"
                                        f"Please log in to the portal to monitor this ticket.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                        elif resp_pct >= 80 and ticket.response_sla_warned_pct is None:
                            # ── 80% Warning ───────────────────────────────────
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"response_sla_warned_pct": 80},
                            )
                            logger.info(
                                "response_sla_warning_80",
                                ticket_number=ticket.ticket_number,
                                pct=round(resp_pct, 1),
                            )

                            if ticket.assigned_to:
                                notify_recipient.delay(
                                    recipient_id=str(ticket.assigned_to),
                                    recipient_type="agent",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"{ticket.ticket_number} has used 80% of its "
                                        f"response SLA — please reply to the customer soon"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"response SLA window ({int(total_mins)} min total). "
                                        f"You have approximately {mins_left} minute(s) remaining "
                                        f"to send your first response to the customer. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Tier: {tier}. "
                                        f"Please respond as soon as possible to avoid a breach."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Response SLA at 80% — "
                                        f"please respond to the customer soon"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"response SLA window.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please respond to the customer as soon as possible "
                                        f"to avoid an SLA breach.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                            if tl_id:
                                notify_recipient.delay(
                                    recipient_id=tl_id,
                                    recipient_type="team_lead",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"Response SLA at 80% — {ticket.ticket_number} "
                                        f"agent should reply soon"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"response SLA ({int(total_mins)} min total). "
                                        f"Approximately {mins_left} minute(s) remaining. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Customer Tier: {tier}. "
                                        f"The assigned agent has been notified. "
                                        f"No action needed from you yet unless the agent is unresponsive."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Response SLA at 80% — "
                                        f"heads up"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"response SLA window.\n\n"
                                        f"SLA Type      : Response\n"
                                        f"Warning Level : 80% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. "
                                        f"No action is required from you at this stage unless "
                                        f"the agent does not respond in time.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
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
                        mins_left  = max(round(total_mins - effective), 0)

                        if now >= res_due:
                            # ── 100% Breach ───────────────────────────────────
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
                                notify_recipient.delay(
                                    recipient_id=str(ticket.assigned_to),
                                    recipient_type="agent",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_breach",
                                    title=(
                                        f"You have breached the resolution SLA "
                                        f"on {ticket.ticket_number}"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has exceeded its resolution "
                                        f"SLA window of {int(total_mins)} minutes and is still open. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Tier: {tier}. "
                                        f"Please resolve this ticket immediately and submit "
                                        f"a breach justification through the portal."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Resolution SLA breached — "
                                        f"your immediate action is required"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its "
                                        f"resolution SLA.\n\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n\n"
                                        f"The ticket is still open and has not been resolved "
                                        f"within the allowed window.\n\n"
                                        f"Please resolve this ticket immediately and submit "
                                        f"a breach justification through the portal.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                            if tl_id:
                                notify_recipient.delay(
                                    recipient_id=tl_id,
                                    recipient_type="team_lead",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_breach",
                                    title=(
                                        f"Resolution SLA breached — {ticket.ticket_number} "
                                        f"must be resolved immediately"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has breached its resolution "
                                        f"SLA ({int(total_mins)} min window) and is still open. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Customer Tier: {tier}. "
                                        f"The assigned agent has been notified and must submit "
                                        f"a breach justification. Please review and intervene immediately."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Resolution SLA breached — "
                                        f"immediate intervention required"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its "
                                        f"resolution SLA.\n\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n\n"
                                        f"The ticket is still open. The assigned agent has been "
                                        f"notified and must submit a breach justification "
                                        f"immediately.\n\n"
                                        f"Please log in to the portal to review and intervene.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                        elif res_pct >= 90 and ticket.resolution_sla_warned_pct != 90:
                            # ── 90% Warning ───────────────────────────────────
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"resolution_sla_warned_pct": 90},
                            )
                            logger.warning(
                                "resolution_sla_warning_90",
                                ticket_number=ticket.ticket_number,
                                pct=round(res_pct, 1),
                            )

                            if ticket.assigned_to:
                                notify_recipient.delay(
                                    recipient_id=str(ticket.assigned_to),
                                    recipient_type="agent",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"Only {mins_left} minute(s) left to resolve — "
                                        f"{ticket.ticket_number} is at 90% of its resolution SLA"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"resolution SLA window ({int(total_mins)} min total). "
                                        f"You have approximately {mins_left} minute(s) remaining "
                                        f"to resolve this ticket. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Tier: {tier}. "
                                        f"Please resolve this ticket immediately to avoid a breach."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Only {mins_left} min left "
                                        f"to resolve — act now"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"resolution SLA window.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please resolve this ticket immediately to avoid "
                                        f"an SLA breach.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                            if tl_id:
                                notify_recipient.delay(
                                    recipient_id=tl_id,
                                    recipient_type="team_lead",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"Resolution SLA at 90% — {ticket.ticket_number} "
                                        f"is nearly out of time"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"resolution SLA ({int(total_mins)} min total). "
                                        f"Approximately {mins_left} minute(s) remaining. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Customer Tier: {tier}. "
                                        f"The assigned agent has been notified. "
                                        f"Please monitor closely and intervene if needed."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Resolution SLA at 90% — "
                                        f"monitor and intervene if needed"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its "
                                        f"resolution SLA window.\n\n"
                                        f"SLA Type      : Resolution\n"
                                        f"Warning Level : 90% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. If they do not "
                                        f"resolve the ticket in time, a breach notification "
                                        f"will follow.\n\n"
                                        f"Please log in to the portal to monitor this ticket.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                        elif res_pct >= 80 and ticket.resolution_sla_warned_pct is None:
                            # ── 80% Warning ───────────────────────────────────
                            await ticket_repo.update_fields(
                                str(ticket.id),
                                {"resolution_sla_warned_pct": 80},
                            )
                            logger.info(
                                "resolution_sla_warning_80",
                                ticket_number=ticket.ticket_number,
                                pct=round(res_pct, 1),
                            )

                            if ticket.assigned_to:
                                notify_recipient.delay(
                                    recipient_id=str(ticket.assigned_to),
                                    recipient_type="agent",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"{ticket.ticket_number} has used 80% of its "
                                        f"resolution SLA — please work to close this soon"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"resolution SLA window ({int(total_mins)} min total). "
                                        f"You have approximately {mins_left} minute(s) remaining "
                                        f"to resolve this ticket. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Tier: {tier}. "
                                        f"Please work to resolve this ticket promptly."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Resolution SLA at 80% — "
                                        f"please work to close this ticket soon"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"resolution SLA window.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please work to resolve this ticket promptly to "
                                        f"avoid an SLA breach.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                            if tl_id:
                                notify_recipient.delay(
                                    recipient_id=tl_id,
                                    recipient_type="team_lead",
                                    ticket_id=str(ticket.id),
                                    ticket_number=ticket.ticket_number,
                                    notif_type="sla_warning",
                                    title=(
                                        f"Resolution SLA at 80% — {ticket.ticket_number} "
                                        f"is approaching its deadline"
                                    ),
                                    message=(
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"resolution SLA ({int(total_mins)} min total). "
                                        f"Approximately {mins_left} minute(s) remaining. "
                                        f"Priority: {ticket.priority or 'N/A'} | "
                                        f"Severity: {ticket.severity or 'N/A'} | "
                                        f"Customer Tier: {tier}. "
                                        f"The assigned agent has been notified. "
                                        f"No action needed from you yet unless the agent is "
                                        f"not making progress."
                                    ),
                                    email_subject=(
                                        f"[{ticket.ticket_number}] Resolution SLA at 80% — "
                                        f"heads up"
                                    ),
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its "
                                        f"resolution SLA window.\n\n"
                                        f"SLA Type      : Resolution\n"
                                        f"Warning Level : 80% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. No action is "
                                        f"required from you at this stage unless the agent is "
                                        f"not making progress on the ticket.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                except Exception as exc:
                    logger.error(
                        "sla_check_ticket_error",
                        ticket_id=str(ticket.id),
                        error=str(exc),
                    )
                    continue

            await session.commit()

    run_async(_check())


# ── Post-Breach Escalation Reminders ─────────────────────────────────────────

@celery_app.task(name="ticket.sla.escalate_critical")
def escalate_critical_unresponded() -> None:
    """
    Runs every 5 minutes via Celery beat.

    After resolution SLA is fully breached (sla_breached_at is set),
    keep reminding the TL via notify_recipient.delay() on a repeat
    interval until the ticket is resolved or closed.

    P0 / P1  → every 15 mins
    P2 / P3  → every 30 mins

    Paused while ticket is on_hold.
    Stops when ticket reaches resolved or closed.
    """

    async def _escalate() -> None:
        from sqlalchemy import select
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository
        from src.data.models.postgres.models import Team
        from src.core.celery.workers.notification_worker import notify_recipient

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            now         = datetime.now(timezone.utc)
            all_open    = await ticket_repo.get_open_unmonitored(limit=500)

            for ticket in all_open:
                if ticket.sla_breached_at is None:
                    continue
                if ticket.status in _CLOSED_STATUSES:
                    continue
                if ticket.status == "on_hold":
                    continue

                tier = _tier(ticket)

                if ticket.priority in ("P0", "critical", "P1", "high"):
                    interval_mins = 15
                else:
                    interval_mins = 30

                should_notify = (
                    ticket.escalation_notified_at is None
                    or (now - ticket.escalation_notified_at).total_seconds() / 60 >= interval_mins
                )

                if not should_notify:
                    continue

                breached_since_mins = round(
                    (now - ticket.sla_breached_at).total_seconds() / 60
                )

                if ticket.team_id:
                    r    = await session.execute(
                        select(Team).where(Team.id == ticket.team_id)
                    )
                    team = r.scalar_one_or_none()
                    if team and team.team_lead_id:
                        tl_id = str(team.team_lead_id)

                        # Fetch agent name for the reminder
                        agent_name = "Unknown Agent"
                        if ticket.assigned_to:
                            try:
                                from src.core.celery.utils import fetch_agent_name
                                agent_name = await fetch_agent_name(ticket.assigned_to) or "Unknown Agent"
                            except Exception:
                                pass

                        notify_recipient.delay(
                            recipient_id=tl_id,
                            recipient_type="team_lead",
                            ticket_id=str(ticket.id),
                            ticket_number=ticket.ticket_number,
                            notif_type="sla_escalation_reminder",
                            title=(
                                f"Escalation reminder — {ticket.ticket_number} is still "
                                f"unresolved {breached_since_mins} minute(s) after SLA breach"
                            ),
                            message=(
                                f"Ticket {ticket.ticket_number} breached its resolution SLA "
                                f"{breached_since_mins} minute(s) ago and is still open "
                                f"with status '{ticket.status}'. "
                                f"Priority: {ticket.priority or 'N/A'} | "
                                f"Severity: {ticket.severity or 'N/A'} | "
                                f"Customer Tier: {tier}. "
                                f"Assigned agent: {agent_name}. "
                                f"Immediate intervention is required. "
                                f"You will receive this reminder every {interval_mins} minutes "
                                f"until the ticket is resolved or closed."
                            ),
                            email_subject=(
                                f"[{ticket.ticket_number}] Escalation reminder — still unresolved "
                                f"{breached_since_mins} min after SLA breach"
                            ),
                            email_body=(
                                f"Hi,\n\n"
                                f"This is an escalation reminder for ticket {ticket.ticket_number}.\n\n"
                                f"Ticket Number    : {ticket.ticket_number}\n"
                                f"Title            : {ticket.title or 'N/A'}\n"
                                f"Priority         : {ticket.priority or 'N/A'}\n"
                                f"Severity         : {ticket.severity or 'N/A'}\n"
                                f"Customer Tier    : {tier}\n"
                                f"Current Status   : {ticket.status}\n"
                                f"Assigned Agent   : {agent_name}\n"
                                f"Breached Since   : {breached_since_mins} minute(s) ago\n\n"
                                f"This ticket has breached its resolution SLA and remains open. "
                                f"The assigned agent has not resolved it yet.\n\n"
                                f"Please log in to the portal immediately to intervene, "
                                f"reassign, or escalate this ticket.\n\n"
                                f"You will continue to receive this reminder every "
                                f"{interval_mins} minutes until the ticket is resolved or closed.\n\n"
                                f"— Ticketing Genie"
                            ),
                            is_internal=True,
                        )

                        await ticket_repo.update_fields(
                            str(ticket.id),
                            {"escalation_notified_at": now},
                        )

                        logger.warning(
                            "escalation_reminder_sent",
                            ticket_number=ticket.ticket_number,
                            priority=ticket.priority,
                            tier=tier,
                            breached_since_mins=breached_since_mins,
                            interval_mins=interval_mins,
                        )

            await session.commit()

    run_async(_escalate())


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

    run_async(_close())