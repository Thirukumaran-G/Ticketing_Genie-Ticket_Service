# ai_worker.py
from __future__ import annotations

import os
from datetime import timezone, timedelta

from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")

from src.core.celery.app import celery_app
from src.core.celery.loop import run_async
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

ASSIGN_THRESHOLD = 0.45
_embedding_model = None

# ── IST timezone helper ───────────────────────────────────────────────────────
_IST = timezone(timedelta(hours=5, minutes=30))

def _fmt_dt(dt) -> str:
    """Format a UTC datetime as IST (UTC+5:30) for display in notifications."""
    if dt is None:
        return "N/A"
    return dt.astimezone(_IST).strftime("%d %b %Y, %I:%M %p IST")


def _get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        _embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        logger.info("embedding_model_loaded")
    return _embedding_model


# ── AI Classification ─────────────────────────────────────────────────────────

@celery_app.task(name="ticket.ai.classify", bind=True, max_retries=3)
def run_ai_classification(self, ticket_id: str, customer_email: str | None = None) -> dict:

    async def _run() -> dict:
        from src.control.agents.clasifier_agent import ClassifierAgent
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository, NotificationRepository
        from src.data.repositories.admin_repository import (
            SLARuleRepository, SeverityPriorityMapRepository, TierRepository,
        )
        from src.core.services.audit_service import audit_service
        from src.config.settings import settings
        from src.data.models.postgres.models import Notification
        from src.core.sse.redis_subscriber import publish_notification
        from src.core.celery.workers.notification_worker import notify_recipient
        import uuid as _uuid
        from datetime import datetime, timezone, timedelta

        async with CelerySessionFactory() as session:
            repo      = TicketRepository(session)
            sla_repo  = SLARuleRepository(session)
            sev_repo  = SeverityPriorityMapRepository(session)
            tier_repo = TierRepository(session)

            ticket = await repo.get_by_id(ticket_id)
            if not ticket:
                logger.error("ai_classify_ticket_not_found", ticket_id=ticket_id)
                return {"error": f"Ticket {ticket_id} not found"}

            product_description: str | None = None
            product_id_str: str | None = None

            if ticket.product_id:
                product_id_str = str(ticket.product_id)
                from src.core.celery.utils import fetch_product_info
                _, product_description = await fetch_product_info(product_id_str)
            else:
                logger.warning("ai_classify_no_product_id", ticket_id=ticket_id)

            agent  = ClassifierAgent()
            result = await agent.classify(
                title=ticket.title or "",
                description=ticket.description or "",
                product_description=product_description,
                product_id=product_id_str,
                session=session,
            )
            system_severity = result.severity

            tier_id = None
            if ticket.tier_snapshot:
                tier_id = await tier_repo.get_id_by_name(ticket.tier_snapshot)
            else:
                logger.warning("ai_classify_no_tier_snapshot", ticket_id=ticket_id)

            sev_map         = await sev_repo.get_by_severity_and_tier(system_severity, tier_id) if tier_id else None
            system_priority = sev_map.derived_priority if sev_map else "P3"

            sla = await sla_repo.get_by_tier_and_priority(tier_id, system_priority) if tier_id else None

            now              = datetime.now(timezone.utc)
            sla_response_due = (now + timedelta(minutes=sla.response_time_min))   if sla else None
            sla_resolve_due  = (now + timedelta(minutes=sla.resolution_time_min)) if sla else None

            priority_overridden = (
                ticket.customer_priority is not None
                and system_severity != ticket.customer_priority
            )

            fields_to_update = {
                "severity":            system_severity,
                "priority":            system_priority,
                "priority_overridden": priority_overridden,
                "override_reason":     result.reason if priority_overridden else None,
                "sla_response_due":    sla_response_due,
                "sla_resolve_due":     sla_resolve_due,
                "status":              "acknowledged",
            }

            await repo.update_fields(ticket_id, fields_to_update)

            logger.info(
                "ticket_classified",
                ticket_id=ticket_id,
                system_severity=system_severity,
                system_priority=system_priority,
                priority_overridden=priority_overridden,
            )

            try:
                await audit_service.log(
                    entity_type="ticket",
                    entity_id=ticket.id,
                    action="ticket_classified",
                    actor_id=ticket.customer_id,
                    actor_type="system",
                    new_value={
                        "severity":            system_severity,
                        "priority":            system_priority,
                        "priority_overridden": priority_overridden,
                        "sla_response_due":    sla_response_due.isoformat() if sla_response_due else None,
                        "sla_resolve_due":     sla_resolve_due.isoformat() if sla_resolve_due else None,
                    },
                    ticket_id=ticket.id,
                )
            except Exception as exc:
                logger.warning("audit_classify_failed", ticket_id=ticket_id, error=str(exc))

            await session.commit()

            ticket = await repo.get_by_id(ticket_id)

            sla_str         = _fmt_dt(sla_response_due)
            sla_resolve_str = _fmt_dt(sla_resolve_due)
            tier            = ticket.tier_snapshot or "N/A"

            # ── Ticket raised — ALWAYS both in_app + email ──────────────────
            try:
                resolved_email = customer_email or await _safe_fetch_email(ticket.customer_id)

                if resolved_email:
                    from src.core.celery.workers.email_worker import send_notification_email
                    send_notification_email.delay(
                        recipient_id=str(ticket.customer_id),
                        recipient_type="customer",
                        subject=f"[{ticket.ticket_number}] Your support ticket has been received",
                        body=(
                            f"Dear Customer,\n\n"
                            f"Your support ticket has been successfully raised and is now "
                            f"being reviewed by our team.\n\n"
                            f"Ticket Number : {ticket.ticket_number}\n"
                            f"Title         : {ticket.title or 'N/A'}\n"
                            f"Severity      : {system_severity}\n"
                            f"Priority      : {system_priority}\n"
                            f"Customer Tier : {tier}\n"
                            f"Environment   : {ticket.environment or 'N/A'}\n\n"
                            f"SLA Response due  : {sla_str}\n"
                            f"SLA Resolution due: {sla_resolve_str}\n\n"
                            f"You will hear from one of our agents shortly. "
                            f"You can track your ticket status at any time through the portal.\n\n"
                            f"— Ticketing Genie Support Team"
                        ),
                        recipient_email=resolved_email,
                    )
                    logger.info(
                        "ticket_raised_email_sent",
                        ticket_number=ticket.ticket_number,
                        customer_email=resolved_email,
                    )

                notif_repo = NotificationRepository(session)
                notif = Notification(
                    channel="in_app",
                    recipient_id=_uuid.UUID(str(ticket.customer_id)),
                    ticket_id=ticket.id,
                    is_internal=False,
                    type="ticket_raised",
                    title=f"Your ticket {ticket.ticket_number} has been received",
                    message=(
                        f"Your ticket has been successfully raised and is under review.\n\n"
                        f"Ticket   : {ticket.ticket_number}\n"
                        f"Title    : {ticket.title or 'N/A'}\n"
                        f"Priority : {system_priority} | Severity : {system_severity}\n"
                        f"Tier     : {tier}\n\n"
                        f"SLA Response due   : {sla_str}\n"
                        f"SLA Resolution due : {sla_resolve_str}\n\n"
                        f"You will be notified as soon as an agent is assigned."
                    ),
                )
                await notif_repo.create(notif)
                await session.commit()

                publish_notification(
                    settings.CELERY_BROKER_URL,
                    str(ticket.customer_id),
                    {
                        "id":            str(notif.id),
                        "type":          "ticket_raised",
                        "title":         f"Your ticket {ticket.ticket_number} has been received",
                        "message": (
                            f"Ticket: {ticket.ticket_number} | "
                            f"Priority: {system_priority} | "
                            f"Severity: {system_severity} | "
                            f"Tier: {tier} | "
                            f"Response due: {sla_str}"
                        ),
                        "ticket_number": ticket.ticket_number,
                        "ticket_id":     str(ticket.id),
                        "is_internal":   False,
                    },
                )
                logger.info(
                    "ticket_raised_in_app_sent",
                    ticket_id=ticket_id,
                    recipient_id=str(ticket.customer_id),
                )

            except Exception as exc:
                logger.warning("ticket_raised_notification_failed", error=str(exc))

            if priority_overridden:
                run_ai_priority_override.delay(
                    ticket_id,
                    ticket.customer_priority,
                    system_priority,
                    result.reason,
                    customer_email,
                    str(ticket.customer_id),
                )

        run_auto_assign.delay(ticket_id, customer_email=customer_email)
        run_ai_draft.delay(ticket_id)

        return {
            "severity":   system_severity,
            "priority":   system_priority,
            "overridden": priority_overridden,
        }

    try:
        return run_async(_run())
    except Exception as exc:
        logger.error("ai_classify_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


# ── Auto-Assign ───────────────────────────────────────────────────────────────

@celery_app.task(name="ticket.ai.auto_assign", bind=True, max_retries=3)
def run_auto_assign(self, ticket_id: str, customer_email: str | None = None) -> dict:

    async def _run() -> dict:
        from src.core.reddis.assignment_lock import assignment_lock, AssignmentLockError
        from src.core.services.audit_service import audit_service
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Team, TeamMember, Ticket
        from src.data.repositories.ticket_repository import TicketRepository
        from src.config.settings import settings
        from sqlalchemy import select

        try:
            async with assignment_lock(ticket_id):
                return await _do_assign(
                    ticket_id=ticket_id,
                    customer_email=customer_email,
                    settings=settings,
                    audit_service=audit_service,
                    CelerySessionFactory=CelerySessionFactory,
                    Team=Team,
                    TeamMember=TeamMember,
                    Ticket=Ticket,
                    TicketRepository=TicketRepository,
                    select=select,
                )
        except AssignmentLockError as exc:
            logger.warning(
                "auto_assign_lock_failed_routing_to_tl_queue",
                ticket_id=ticket_id,
                error=str(exc),
            )
            await _fallback_to_tl_queue(ticket_id)
            return {"assigned_to": None, "reason": "lock_timeout"}

    try:
        return run_async(_run())
    except Exception as exc:
        logger.error("auto_assign_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=15)


async def _do_assign(
    ticket_id, customer_email, settings,
    audit_service, CelerySessionFactory, Team, TeamMember, Ticket,
    TicketRepository, select,
) -> dict:
    from src.core.celery.workers.notification_worker import notify_recipient

    async with CelerySessionFactory() as session:
        ticket_repo = TicketRepository(session)

        ticket = await ticket_repo.get_by_id(ticket_id)
        if not ticket:
            logger.error("auto_assign_ticket_not_found", ticket_id=ticket_id)
            return {"error": "Ticket not found"}

        if ticket.assigned_to:
            logger.info(
                "auto_assign_skipped_already_assigned",
                ticket_id=ticket_id,
                assigned_to=str(ticket.assigned_to),
            )
            return {"skipped": True, "assigned_to": str(ticket.assigned_to)}

        if not ticket.product_id:
            logger.warning("auto_assign_no_product_id", ticket_id=ticket_id)
            return {"error": "No product_id on ticket"}

        teams_result = await session.execute(
            select(Team).where(
                Team.product_id == ticket.product_id,
                Team.is_active.is_(True),
            )
        )
        teams = list(teams_result.scalars().all())
        if not teams:
            logger.warning("auto_assign_no_teams_for_product", product_id=str(ticket.product_id))
            return {"error": "No teams for product"}

        selected_team = await _llm_pick_team(ticket=ticket, teams=teams)
        if not selected_team:
            selected_team = teams[0]
            logger.warning("auto_assign_team_fallback", ticket_id=ticket_id, team=selected_team.name)

        members_result = await session.execute(
            select(TeamMember).where(
                TeamMember.team_id == selected_team.id,
                TeamMember.is_active.is_(True),
            )
        )
        members = list(members_result.scalars().all())

        if not members:
            logger.warning(
                "auto_assign_no_members_in_team",
                ticket_id=ticket_id,
                team_id=str(selected_team.id),
            )
            await ticket_repo.update_fields(ticket_id, {
                "team_id":     selected_team.id,
                "assigned_to": None,
            })
            await session.commit()
            return {"routed": True, "reason": "no_members_in_team", "team_id": str(selected_team.id)}

        ticket_embedding = _compute_ticket_embedding(ticket)
        workload_map     = await _get_workload_map(session, members)

        scored = sorted(
            [
                (m, _score_member(m, ticket_embedding, workload_map.get(str(m.user_id), 0)))
                for m in members
            ],
            key=lambda x: x[1],
            reverse=True,
        )
        best_member, best_score = scored[0]

        logger.info(
            "auto_assign_best_candidate",
            ticket_id=ticket_id,
            member_id=str(best_member.id),
            score=round(best_score, 3),
            threshold=ASSIGN_THRESHOLD,
        )

        if best_score >= ASSIGN_THRESHOLD:
            from src.core.celery.utils import fetch_agent_name

            agent_name     = await fetch_agent_name(best_member.user_id) or "Support Agent"
            resolved_email = customer_email or await _safe_fetch_email(ticket.customer_id)

            await ticket_repo.update_fields(ticket_id, {
                "team_id":     selected_team.id,
                "assigned_to": best_member.user_id,
                "status":      "assigned",
            })
            await session.commit()

            ticket = await ticket_repo.get_by_id(ticket_id)

            logger.info(
                "ticket_auto_assigned",
                ticket_id=ticket_id,
                agent_user_id=str(best_member.user_id),
                score=round(best_score, 3),
            )

            try:
                await audit_service.log(
                    entity_type="ticket",
                    entity_id=ticket.id,
                    action="ticket_auto_assigned",
                    actor_id=ticket.customer_id,
                    actor_type="system",
                    new_value={
                        "assigned_to": str(best_member.user_id),
                        "team_id":     str(selected_team.id),
                        "score":       round(best_score, 3),
                        "status":      "assigned",
                    },
                    ticket_id=ticket.id,
                )
            except Exception as exc:
                logger.warning("audit_assign_failed", ticket_id=ticket_id, error=str(exc))

            sla_str         = _fmt_dt(ticket.sla_response_due)
            sla_resolve_str = _fmt_dt(ticket.sla_resolve_due)
            tier            = ticket.tier_snapshot or "N/A"

            # ── Agent notification ─────────────────────────────────────────
            notify_recipient.apply_async(
                kwargs=dict(
                    recipient_id=str(best_member.user_id),
                    recipient_type="agent",
                    ticket_id=str(ticket.id),
                    ticket_number=ticket.ticket_number,
                    notif_type="ticket_assigned",
                    title=f"Ticket {ticket.ticket_number} has been assigned to you",
                    message=(
                        f"A new ticket has been assigned to you.\n\n"
                        f"Ticket Number  : {ticket.ticket_number}\n"
                        f"Title          : {ticket.title or 'N/A'}\n"
                        f"Priority       : {ticket.priority or 'N/A'}\n"
                        f"Severity       : {ticket.severity or 'N/A'}\n"
                        f"Customer Tier  : {tier}\n"
                        f"Environment    : {ticket.environment or 'N/A'}\n\n"
                        f"SLA Response due   : {sla_str}\n"
                        f"SLA Resolution due : {sla_resolve_str}\n\n"
                        f"Please open the ticket in the portal and send your first "
                        f"response to the customer before the SLA deadline."
                    ),
                    email_subject=f"[{ticket.ticket_number}] A new ticket has been assigned to you",
                    email_body=(
                        f"Hi {agent_name},\n\n"
                        f"A new support ticket has been assigned to you.\n\n"
                        f"Ticket Number     : {ticket.ticket_number}\n"
                        f"Title             : {ticket.title or 'N/A'}\n"
                        f"Priority          : {ticket.priority or 'N/A'}\n"
                        f"Severity          : {ticket.severity or 'N/A'}\n"
                        f"Customer Tier     : {tier}\n"
                        f"Environment       : {ticket.environment or 'N/A'}\n\n"
                        f"SLA Response due  : {sla_str}\n"
                        f"SLA Resolution due: {sla_resolve_str}\n\n"
                        f"Please log in to the portal to review the ticket details and "
                        f"send your first response to the customer as soon as possible.\n\n"
                        f"— Ticketing Genie"
                    ),
                    is_internal=True,
                ),
                countdown=2,
            )

            # ── Customer notification ──────────────────────────────────────
            notify_recipient.apply_async(
                kwargs=dict(
                    recipient_id=str(ticket.customer_id),
                    recipient_type="customer",
                    ticket_id=str(ticket.id),
                    ticket_number=ticket.ticket_number,
                    notif_type="ticket_assigned",
                    title=f"Your ticket {ticket.ticket_number} has been assigned to an agent",
                    message=(
                        f"Your ticket has been picked up by one of our support agents.\n\n"
                        f"Ticket Number  : {ticket.ticket_number}\n"
                        f"Title          : {ticket.title or 'N/A'}\n"
                        f"Assigned Agent : {agent_name}\n"
                        f"Priority       : {ticket.priority or 'N/A'}\n"
                        f"Severity       : {ticket.severity or 'N/A'}\n"
                        f"Customer Tier  : {tier}\n\n"
                        f"SLA Response due   : {sla_str}\n"
                        f"SLA Resolution due : {sla_resolve_str}\n\n"
                        f"Our agent will reach out to you shortly. "
                        f"You can track your ticket status at any time through the portal."
                    ),
                    email_subject=f"[{ticket.ticket_number}] An agent has been assigned to your ticket",
                    email_body=(
                        f"Dear Customer,\n\n"
                        f"Your ticket {ticket.ticket_number} has been assigned to one of "
                        f"our support agents.\n\n"
                        f"Ticket Number       : {ticket.ticket_number}\n"
                        f"Title               : {ticket.title or 'N/A'}\n"
                        f"Assigned Agent      : {agent_name}\n"
                        f"Priority            : {ticket.priority or 'N/A'}\n"
                        f"Severity            : {ticket.severity or 'N/A'}\n"
                        f"Customer Tier       : {tier}\n\n"
                        f"SLA Response due    : {sla_str}\n"
                        f"SLA Resolution due  : {sla_resolve_str}\n\n"
                        f"Our agent will reach out to you shortly with an update. "
                        f"You can track your ticket status at any time through the portal.\n\n"
                        f"— Ticketing Genie Support Team"
                    ),
                    is_internal=False,
                ),
                countdown=2,
            )

            # ── TL alert for critical tickets ──────────────────────────────
            if ticket.priority in ("P0", "critical") and selected_team.team_lead_id:
                notify_recipient.apply_async(
                    kwargs=dict(
                        recipient_id=str(selected_team.team_lead_id),
                        recipient_type="team_lead",
                        ticket_id=str(ticket.id),
                        ticket_number=ticket.ticket_number,
                        notif_type="ticket_assigned",
                        title=(
                            f"[{ticket.priority}] Critical ticket {ticket.ticket_number} "
                            f"has been assigned to your team"
                        ),
                        message=(
                            f"A critical ticket has been auto-assigned to {agent_name} "
                            f"in your team.\n\n"
                            f"Ticket Number  : {ticket.ticket_number}\n"
                            f"Title          : {ticket.title or 'N/A'}\n"
                            f"Priority       : {ticket.priority or 'N/A'}\n"
                            f"Severity       : {ticket.severity or 'N/A'}\n"
                            f"Customer Tier  : {tier}\n"
                            f"Assigned to    : {agent_name}\n\n"
                            f"SLA Response due   : {sla_str}\n"
                            f"SLA Resolution due : {sla_resolve_str}\n\n"
                            f"Please monitor this ticket closely given its priority level."
                        ),
                        email_subject=(
                            f"[{ticket.ticket_number}] Critical ticket assigned to "
                            f"{agent_name} — heads up"
                        ),
                        email_body=(
                            f"Hi,\n\n"
                            f"A critical ticket has been auto-assigned to a member of your team.\n\n"
                            f"Ticket Number : {ticket.ticket_number}\n"
                            f"Title         : {ticket.title or 'N/A'}\n"
                            f"Priority      : {ticket.priority or 'N/A'}\n"
                            f"Severity      : {ticket.severity or 'N/A'}\n"
                            f"Customer Tier : {tier}\n"
                            f"Assigned to   : {agent_name}\n\n"
                            f"SLA Response due  : {sla_str}\n"
                            f"SLA Resolution due: {sla_resolve_str}\n\n"
                            f"Please monitor this ticket closely given its priority level.\n\n"
                            f"— Ticketing Genie"
                        ),
                        is_internal=True,
                    ),
                    countdown=2,
                )

            return {"assigned_to": str(best_member.user_id), "score": best_score}

        else:
            await ticket_repo.update_fields(ticket_id, {
                "team_id":     selected_team.id,
                "assigned_to": None,
            })
            await session.commit()

            logger.info(
                "auto_assign_below_threshold_unassigned",
                ticket_id=ticket_id,
                team_id=str(selected_team.id),
                best_score=round(best_score, 3),
            )

            try:
                await audit_service.log(
                    entity_type="ticket",
                    entity_id=ticket.id,
                    action="ticket_routed_unassigned",
                    actor_id=ticket.customer_id,
                    actor_type="system",
                    new_value={
                        "team_id":    str(selected_team.id),
                        "best_score": round(best_score, 3),
                        "reason":     "below_threshold",
                    },
                    ticket_id=ticket.id,
                )
            except Exception as exc:
                logger.warning("audit_unassigned_failed", ticket_id=ticket_id, error=str(exc))

            return {
                "assigned_to": None,
                "team_id":     str(selected_team.id),
                "score":       best_score,
                "reason":      "below_threshold",
            }


async def _fallback_to_tl_queue(ticket_id: str) -> None:
    try:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            repo   = TicketRepository(session)
            ticket = await repo.get_by_id(ticket_id)
            if ticket and not ticket.assigned_to:
                logger.info("auto_assign_fallback_already_unassigned", ticket_id=ticket_id)
                return
            logger.info("auto_assign_fallback_leaving_unassigned", ticket_id=ticket_id)
    except Exception as exc:
        logger.error("auto_assign_fallback_error", ticket_id=ticket_id, error=str(exc))


# ── AI Draft ──────────────────────────────────────────────────────────────────

@celery_app.task(name="ticket.ai.draft", bind=True, max_retries=2)
def run_ai_draft(self, ticket_id: str) -> dict:

    async def _run() -> dict:
        from src.control.agents.draft_agent import DraftAgent
        from src.core.services.audit_service import audit_service
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            repo   = TicketRepository(session)
            ticket = await repo.get_by_id(ticket_id)
            if not ticket:
                return {"error": "Not found"}

            agent      = DraftAgent()
            result     = agent.generate_draft(
                ticket_title=ticket.title or "",
                ticket_description=ticket.description or "",
            )
            draft_body = result.body if result else ""
            await repo.update_fields(ticket_id, {"ai_draft": draft_body})
            await session.commit()

            try:
                await audit_service.log(
                    entity_type="ticket",
                    entity_id=ticket.id,
                    action="ai_draft_generated",
                    actor_id=ticket.customer_id,
                    actor_type="system",
                    new_value={"draft_preview": draft_body[:100] if draft_body else ""},
                    ticket_id=ticket.id,
                )
            except Exception as exc:
                logger.warning("audit_draft_failed", ticket_id=ticket_id, error=str(exc))

        logger.info("ai_draft_generated", ticket_id=ticket_id)
        return {"draft_generated": True}

    try:
        return run_async(_run())
    except Exception as exc:
        logger.error("ai_draft_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=20)


# ── Priority Override Notification ────────────────────────────────────────────

@celery_app.task(name="ticket.ai.priority_override", bind=True, max_retries=3)
def run_ai_priority_override(
    self,
    ticket_id:         str,
    original_severity: str,
    new_priority:      str,
    reason:            str,
    customer_email:    str | None = None,
    customer_id:       str | None = None,
) -> None:

    async def _notify() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository
        from src.core.celery.workers.notification_worker import notify_recipient

        async with CelerySessionFactory() as session:
            ticket = await TicketRepository(session).get_by_id(ticket_id)
            if not ticket:
                return

            abstract_reason = _abstract_priority_reason(original_severity, new_priority, reason)

            notify_recipient.apply_async(
                kwargs=dict(
                    recipient_id=str(ticket.customer_id),
                    recipient_type="customer",
                    ticket_id=str(ticket.id),
                    ticket_number=ticket.ticket_number,
                    notif_type="ticket_priority_updated",
                    title=f"Priority updated on your ticket {ticket.ticket_number}",
                    message=(
                        f"Our team has reviewed your ticket and updated its priority.\n\n"
                        f"Ticket Number : {ticket.ticket_number}\n"
                        f"Title         : {ticket.title or 'N/A'}\n\n"
                        f"{abstract_reason}\n\n"
                        f"If you have concerns about this assessment, please reply "
                        f"through the portal."
                    ),
                    email_subject=f"[{ticket.ticket_number}] Your ticket priority has been updated",
                    email_body=(
                        f"Dear Customer,\n\n"
                        f"Our team has reviewed your ticket {ticket.ticket_number} and "
                        f"updated its priority.\n\n"
                        f"Ticket Number : {ticket.ticket_number}\n"
                        f"Title         : {ticket.title or 'N/A'}\n\n"
                        f"{abstract_reason}\n\n"
                        f"If you have any concerns about this assessment, please reply "
                        f"through the portal and our team will be happy to review it.\n\n"
                        f"— Ticketing Genie Support Team"
                    ),
                    is_internal=False,
                ),
                countdown=2,
            )

            logger.info(
                "priority_override_notif_queued",
                ticket_id=ticket_id,
                recipient_id=str(ticket.customer_id),
            )

    try:
        run_async(_notify())
    except Exception as exc:
        logger.error("priority_override_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


def _abstract_priority_reason(original_severity: str, new_priority: str, reason: str) -> str:
    label = {
        "P0": "Critical — this will receive immediate attention from our team",
        "P1": "High — this will be addressed within the same business day",
        "P2": "Medium — this will be addressed within 2 business days",
        "P3": "Standard — this will be addressed within our regular SLA window",
    }.get(new_priority, "Standard — this will be addressed within our regular SLA window")
    return (
        f"Updated priority: {label}.\n\n"
        f"Our assessment: {reason}"
    )


# ── Team routing ──────────────────────────────────────────────────────────────

async def _llm_pick_team(ticket, teams: list) -> object | None:
    from src.control.agents.routing_agent import TeamRoutingAgent

    team_names = [t.name for t in teams]
    agent  = TeamRoutingAgent()
    result = await agent.route(
        title=ticket.title or "",
        description=ticket.description or "",
        team_names=team_names,
    )

    for team in teams:
        if team.name == result.team_name:
            logger.info("auto_assign_team_selected", team=team.name, ticket_id=str(ticket.id))
            return team
    for team in teams:
        if team.name.lower() == result.team_name.lower():
            logger.info("auto_assign_team_selected_ci", team=team.name, ticket_id=str(ticket.id))
            return team

    logger.warning("auto_assign_team_not_matched", returned=result.team_name, available=team_names)
    return None


# ── Embedding helpers ─────────────────────────────────────────────────────────

def _compute_ticket_embedding(ticket) -> list[float] | None:
    if ticket.ticket_embedding is not None:
        emb = ticket.ticket_embedding
        return list(emb) if not isinstance(emb, list) else emb
    try:
        model = _get_embedding_model()
        text  = f"{ticket.title or ''} {ticket.description or ''}".strip()
        if not text:
            return None
        return model.encode(text, normalize_embeddings=True).tolist()
    except Exception as exc:
        logger.warning("ticket_embedding_compute_failed", error=str(exc))
        return None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    import numpy as np
    va    = np.array(a, dtype=float)
    vb    = np.array(b, dtype=float)
    denom = np.linalg.norm(va) * np.linalg.norm(vb)
    if denom == 0:
        return 0.0
    return float(np.dot(va, vb) / denom)


def _score_member(member, ticket_embedding, workload: int) -> float:
    skill_score = 0.0
    if ticket_embedding is not None and member.skill_embedding is not None:
        skill_score = max(
            0.0,
            _cosine_similarity(list(ticket_embedding), list(member.skill_embedding)),
        )
    exp_score  = min((member.experience or 0) / 10.0, 1.0)
    load_score = max(0.0, 1.0 - (workload / 20.0))
    return (skill_score * 0.4) + (exp_score * 0.2) + (load_score * 0.4)


async def _get_workload_map(session, members: list) -> dict[str, int]:
    from src.data.models.postgres.models import Ticket
    from sqlalchemy import select, func

    user_ids = [m.user_id for m in members]
    r = await session.execute(
        select(Ticket.assigned_to, func.count().label("cnt"))
        .where(
            Ticket.assigned_to.in_(user_ids),
            Ticket.status.in_(["assigned", "in_progress", "on_hold"]),
        )
        .group_by(Ticket.assigned_to)
    )
    return {str(row.assigned_to): row.cnt for row in r.fetchall()}


async def _safe_fetch_email(user_id) -> str | None:
    try:
        from src.core.celery.utils import fetch_user_email
        return await fetch_user_email(user_id)
    except Exception as exc:
        logger.warning("fetch_customer_email_failed", error=str(exc))
        return None