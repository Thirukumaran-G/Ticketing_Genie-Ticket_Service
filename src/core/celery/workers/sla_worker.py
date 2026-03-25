from __future__ import annotations

from datetime import datetime, timezone, timedelta

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

# ── IST timezone helper ───────────────────────────────────────────────────────
_IST = timezone(timedelta(hours=5, minutes=30))

def _fmt_dt(dt) -> str:
    """Format a UTC datetime as IST (UTC+5:30) for display in notifications."""
    if dt is None:
        return "N/A"
    return dt.astimezone(_IST).strftime("%d %b %Y, %I:%M %p IST")


def _tier(ticket) -> str:
    raw = (ticket.tier_snapshot or "").lower().strip()
    return _TIER_LABEL.get(raw, ticket.tier_snapshot or "N/A")


# ── SLA breach and warning check ──────────────────────────────────────────────

@celery_app.task(name="ticket.sla.check_breaches")
def check_sla_breaches() -> None:

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
                        due_str    = _fmt_dt(resp_due)   # FIX: IST

                        if now >= resp_due:
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
                                    title=f"Response SLA breached on {ticket.ticket_number} — immediate action required",
                                    message=(
                                        f"You have breached the response SLA on this ticket.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n\n"
                                        f"The customer has not received a first response. "
                                        f"Please respond immediately and submit a breach "
                                        f"justification through the portal."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Response SLA breached — immediate action required",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its response SLA.\n\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n"
                                        f"SLA Deadline : {due_str}\n\n"
                                        f"The customer has not received a first response within "
                                        f"the allowed window.\n\n"
                                        f"Please respond immediately and submit a breach "
                                        f"justification through the portal.\n\n"
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
                                    title=f"Response SLA breached — {ticket.ticket_number} has no agent reply",
                                    message=(
                                        f"A ticket in your team has breached its response SLA.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n\n"
                                        f"The customer has not received a first response. "
                                        f"The assigned agent has been notified. "
                                        f"Please review and intervene if necessary."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Response SLA breached — immediate action required",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its response SLA.\n\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n"
                                        f"SLA Deadline : {due_str}\n"
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
                                    title=f"{ticket.ticket_number} — only {mins_left} min left to respond (90% SLA used)",
                                    message=(
                                        f"Your response SLA is at 90% for this ticket.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please respond to the customer immediately to avoid a breach."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Only {mins_left} min left to respond — act now",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its response SLA.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please respond to the customer immediately to avoid an SLA breach.\n\n"
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
                                    title=f"Response SLA at 90% — {ticket.ticket_number} agent has not replied",
                                    message=(
                                        f"A ticket in your team is at 90% of its response SLA.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"The agent has been notified. Please monitor and "
                                        f"intervene if they do not respond in time."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Response SLA at 90% — monitor and intervene if needed",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its response SLA window.\n\n"
                                        f"SLA Type      : Response\n"
                                        f"Warning Level : 90% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"SLA Deadline  : {due_str}\n"
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
                                    title=f"{ticket.ticket_number} — 80% of response SLA used, reply soon",
                                    message=(
                                        f"Your response SLA is at 80% for this ticket.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please respond to the customer soon to avoid a breach."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Response SLA at 80% — please respond soon",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its response SLA window.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please respond to the customer as soon as possible to avoid an SLA breach.\n\n"
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
                                    title=f"Response SLA at 80% — {ticket.ticket_number} agent should reply soon",
                                    message=(
                                        f"A ticket in your team is at 80% of its response SLA.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"The agent has been notified. No action needed yet "
                                        f"unless the agent is unresponsive."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Response SLA at 80% — heads up",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its response SLA window.\n\n"
                                        f"SLA Type      : Response\n"
                                        f"Warning Level : 80% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"SLA Deadline  : {due_str}\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. No action is "
                                        f"required from you at this stage unless the agent does "
                                        f"not respond in time.\n\n"
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
                        due_str    = _fmt_dt(res_due)   # FIX: IST

                        if now >= res_due:
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
                                    title=f"Resolution SLA breached on {ticket.ticket_number} — immediate action required",
                                    message=(
                                        f"You have breached the resolution SLA on this ticket.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n\n"
                                        f"The ticket is still open. Please resolve it immediately "
                                        f"and submit a breach justification through the portal."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA breached — immediate action required",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its resolution SLA.\n\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n"
                                        f"SLA Deadline : {due_str}\n\n"
                                        f"The ticket is still open. Please resolve it immediately "
                                        f"and submit a breach justification through the portal.\n\n"
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
                                    title=f"Resolution SLA breached — {ticket.ticket_number} must be resolved immediately",
                                    message=(
                                        f"A ticket in your team has breached its resolution SLA.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n\n"
                                        f"The ticket is still open. The agent has been notified. "
                                        f"Please review and intervene immediately."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA breached — immediate intervention required",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has breached its resolution SLA.\n\n"
                                        f"SLA Window   : {int(total_mins)} minutes\n"
                                        f"SLA Deadline : {due_str}\n"
                                        f"Priority     : {ticket.priority or 'N/A'}\n"
                                        f"Severity     : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier: {tier}\n"
                                        f"Ticket Title : {ticket.title or 'N/A'}\n\n"
                                        f"The ticket is still open. The agent has been notified "
                                        f"and must submit a breach justification immediately.\n\n"
                                        f"Please log in to the portal to review and intervene.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                        elif res_pct >= 90 and ticket.resolution_sla_warned_pct != 90:
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
                                    title=f"{ticket.ticket_number} — only {mins_left} min left to resolve (90% SLA used)",
                                    message=(
                                        f"Your resolution SLA is at 90% for this ticket.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please resolve this ticket immediately to avoid a breach."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Only {mins_left} min left to resolve — act now",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its resolution SLA.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please resolve this ticket immediately to avoid an SLA breach.\n\n"
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
                                    title=f"Resolution SLA at 90% — {ticket.ticket_number} is nearly out of time",
                                    message=(
                                        f"A ticket in your team is at 90% of its resolution SLA.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"The agent has been notified. Please monitor closely "
                                        f"and intervene if needed."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA at 90% — monitor and intervene if needed",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 90% of its resolution SLA window.\n\n"
                                        f"SLA Type      : Resolution\n"
                                        f"Warning Level : 90% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"SLA Deadline  : {due_str}\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. If they do not "
                                        f"resolve the ticket in time, a breach notification will follow.\n\n"
                                        f"Please log in to the portal to monitor this ticket.\n\n"
                                        f"— Ticketing Genie"
                                    ),
                                    is_internal=True,
                                )

                        elif res_pct >= 80 and ticket.resolution_sla_warned_pct is None:
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
                                    title=f"{ticket.ticket_number} — 80% of resolution SLA used, close this soon",
                                    message=(
                                        f"Your resolution SLA is at 80% for this ticket.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please work to resolve this ticket promptly to avoid a breach."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA at 80% — please close this ticket soon",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its resolution SLA window.\n\n"
                                        f"Ticket Title   : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"Please work to resolve this ticket promptly to avoid an SLA breach.\n\n"
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
                                    title=f"Resolution SLA at 80% — {ticket.ticket_number} approaching deadline",
                                    message=(
                                        f"A ticket in your team is at 80% of its resolution SLA.\n\n"
                                        f"Ticket Number  : {ticket.ticket_number}\n"
                                        f"Title          : {ticket.title or 'N/A'}\n"
                                        f"Priority       : {ticket.priority or 'N/A'}\n"
                                        f"Severity       : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier  : {tier}\n"
                                        f"SLA Window     : {int(total_mins)} minutes\n"
                                        f"SLA Deadline   : {due_str}\n"
                                        f"Time Remaining : ~{mins_left} minute(s)\n\n"
                                        f"The agent has been notified. No action needed yet "
                                        f"unless the agent is not making progress."
                                    ),
                                    email_subject=f"[{ticket.ticket_number}] Resolution SLA at 80% — heads up",
                                    email_body=(
                                        f"Hi,\n\n"
                                        f"Ticket {ticket.ticket_number} has used 80% of its resolution SLA window.\n\n"
                                        f"SLA Type      : Resolution\n"
                                        f"Warning Level : 80% consumed\n"
                                        f"Time Remaining: ~{mins_left} minute(s)\n"
                                        f"SLA Deadline  : {due_str}\n"
                                        f"Priority      : {ticket.priority or 'N/A'}\n"
                                        f"Severity      : {ticket.severity or 'N/A'}\n"
                                        f"Customer Tier : {tier}\n"
                                        f"Ticket Title  : {ticket.title or 'N/A'}\n\n"
                                        f"The assigned agent has been notified. No action is "
                                        f"required from you at this stage unless the agent is "
                                        f"not making progress.\n\n"
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
                breached_at_str = _fmt_dt(ticket.sla_breached_at)   # FIX: IST

                if ticket.team_id:
                    r    = await session.execute(
                        select(Team).where(Team.id == ticket.team_id)
                    )
                    team = r.scalar_one_or_none()
                    if team and team.team_lead_id:
                        tl_id = str(team.team_lead_id)

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
                                f"Escalation reminder — {ticket.ticket_number} still unresolved "
                                f"{breached_since_mins} min after SLA breach"
                            ),
                            message=(
                                f"This ticket has breached its resolution SLA and is still open.\n\n"
                                f"Ticket Number   : {ticket.ticket_number}\n"
                                f"Title           : {ticket.title or 'N/A'}\n"
                                f"Priority        : {ticket.priority or 'N/A'}\n"
                                f"Severity        : {ticket.severity or 'N/A'}\n"
                                f"Customer Tier   : {tier}\n"
                                f"Current Status  : {ticket.status}\n"
                                f"Assigned Agent  : {agent_name}\n"
                                f"Breached At     : {breached_at_str}\n"
                                f"Breached Since  : {breached_since_mins} minute(s) ago\n\n"
                                f"Immediate intervention is required. You will receive this "
                                f"reminder every {interval_mins} minutes until resolved or closed."
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
                                f"Breached At      : {breached_at_str}\n"
                                f"Breached Since   : {breached_since_mins} minute(s) ago\n\n"
                                f"This ticket has breached its resolution SLA and remains open.\n\n"
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