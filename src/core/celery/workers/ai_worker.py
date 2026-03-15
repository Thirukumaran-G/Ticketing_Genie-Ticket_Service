"""
AI Celery worker — classification, SLA assignment, auto-assign, draft.
src/core/celery/workers/ai_worker.py
"""
from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")

from src.core.celery.app import celery_app
from src.core.celery.loop import run_async          # ← NEW
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

ASSIGN_THRESHOLD = 0.45

_embedding_model = None


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
            SLARuleRepository,
            SeverityPriorityMapRepository,
            TierRepository,
        )
        from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository
        from src.core.services.audit_service import audit_service
        from src.config.settings import settings
        from datetime import datetime, timezone, timedelta

        async with CelerySessionFactory() as session:
            repo       = TicketRepository(session)
            sla_repo   = SLARuleRepository(session)
            sev_repo   = SeverityPriorityMapRepository(session)
            tier_repo  = TierRepository(session)
            notif_repo = NotificationRepository(session)
            pref_repo  = NotificationPreferenceRepository(session)

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

            sev_map = None
            if tier_id:
                sev_map = await sev_repo.get_by_severity_and_tier(system_severity, tier_id)

            system_priority = sev_map.derived_priority if sev_map else "P3"

            sla = None
            if tier_id:
                sla = await sla_repo.get_by_tier_and_priority(tier_id, system_priority)
                logger.info(
                "sla",
                sla_response =sla.response_time_min,
                sla_resolve =sla.resolution_time_min,
                )


            now = datetime.now(timezone.utc)
            sla_response_due = (now + timedelta(minutes=sla.response_time_min))   if sla else None
            sla_resolve_due  = (now + timedelta(minutes=sla.resolution_time_min)) if sla else None

            priority_overridden = (
                ticket.customer_priority is not None
                and system_severity != ticket.customer_priority
            )

            ticket_embedding = _compute_ticket_embedding(ticket)
            fields_to_update = {
                "severity":            system_severity,
                "priority":            system_priority,
                "priority_overridden": priority_overridden,
                "override_reason":     result.reason if priority_overridden else None,
                "sla_response_due":    sla_response_due,
                "sla_resolve_due":     sla_resolve_due,
            }
            if ticket_embedding and ticket.ticket_embedding is None:
                fields_to_update["ticket_embedding"] = ticket_embedding

            await repo.update_fields(ticket_id, fields_to_update)

            logger.info(
                "ticket_classified",
                ticket_id=ticket_id,
                system_severity=system_severity,
                system_priority=system_priority,
                priority_overridden=priority_overridden,
                sla_resolve_due = sla.resolution_time_min,
                sla_response_due = sla.response_time_min
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
                        "tier_snapshot":       ticket.tier_snapshot,
                        "priority_overridden": priority_overridden,
                        "sla_response_due":    sla_response_due.isoformat() if sla_response_due else None,
                        "sla_resolve_due":     sla_resolve_due.isoformat() if sla_resolve_due else None,
                    },
                    ticket_id=ticket.id,
                )
            except Exception as exc:
                logger.warning("audit_classify_failed", ticket_id=ticket_id, error=str(exc))

            if priority_overridden and customer_email:
                run_ai_priority_override.delay(
                    ticket_id,
                    ticket.customer_priority,
                    system_priority,
                    result.reason,
                    customer_email,
                    str(ticket.customer_id),
                )

            await session.commit()
            await repo.update_fields(ticket_id, {"status": "acknowledged"})
            await session.commit()
            try:
                sla_str = (
                    sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
                    if sla_response_due else "N/A"
                )
                await _notify_actor(
                    actor_id=str(ticket.customer_id),
                    ticket=ticket,
                    pref_repo=pref_repo,
                    notif_repo=notif_repo,
                    session=session,
                    settings=settings,
                    is_internal=False,
                    notif_type="ticket_created",
                    title=f"Ticket {ticket.ticket_number} raised successfully",
                    message=(
                        f"Your ticket '{ticket.title}' has been received. "
                        f"Our team will review and respond shortly. "
                        f"Expected response by: {sla_str}"
                    ),
                    fallback_email=customer_email,
                    email_subject=f"[{ticket.ticket_number}] Your ticket has been received",
                    email_body=(
                        f"Dear Customer,\n\n"
                        f"Your support ticket has been successfully raised.\n\n"
                        f"Ticket: {ticket.ticket_number}\n"
                        f"Title:  {ticket.title}\n\n"
                        f"Our team will review and respond within your SLA window.\n"
                        f"Expected response by: {sla_str}\n\n"
                        f"— Ticketing Genie Support Team"
                    ),
                )
            except Exception as exc:
                logger.warning("ticket_raise_notification_failed", error=str(exc))

            if ticket_embedding:
                try:
                    await _detect_similar_tickets(
                        session=session,
                        ticket_id=ticket_id,
                        embedding=ticket_embedding,
                        product_id=str(ticket.product_id) if ticket.product_id else None,
                    )
                except Exception as exc:
                    logger.warning(
                        "similar_ticket_detection_failed",
                        ticket_id=ticket_id,
                        error=str(exc),
                    )

        run_auto_assign.delay(ticket_id, customer_email=customer_email)
        run_ai_draft.delay(ticket_id)

        return {
            "severity":   system_severity,
            "priority":   system_priority,
            "overridden": priority_overridden,
        }

    try:
        return run_async(_run())        # ← CHANGED from asyncio.run()
    except Exception as exc:
        logger.error("ai_classify_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


async def _detect_similar_tickets(
    session,
    ticket_id:  str,
    embedding:  list[float],
    product_id: str | None,
) -> None:
    from sqlalchemy import select
    from src.data.models.postgres.models import Team, Ticket
    from src.core.services.similar_ticket_service import SimilarTicketService
    import uuid as _uuid

    ticket_result = await session.execute(
        select(Ticket).where(Ticket.id == _uuid.UUID(ticket_id))
    )
    ticket = ticket_result.scalar_one_or_none()
    if not ticket or not ticket.team_id:
        logger.warning("similar_detection_no_team", ticket_id=ticket_id)
        return

    team_result = await session.execute(
        select(Team).where(Team.id == ticket.team_id)
    )
    team = team_result.scalar_one_or_none()
    team_lead_id = str(team.team_lead_id) if team and team.team_lead_id else None

    svc   = SimilarTicketService(session)
    group = await svc.detect_and_group(
        ticket_id=ticket_id,
        embedding=embedding,
        team_lead_id=team_lead_id,
    )

    if group:
        logger.info(
            "similar_tickets_grouped",
            ticket_id=ticket_id,
            group_id=str(group.id),
        )


# ── Auto-Assign ───────────────────────────────────────────────────────────────

@celery_app.task(name="ticket.ai.auto_assign", bind=True, max_retries=3)
def run_auto_assign(self, ticket_id: str, customer_email: str | None = None) -> dict:

    async def _run() -> dict:
        from src.config.settings import settings
        from src.core.reddis.assignment_lock import assignment_lock, AssignmentLockError
        from src.core.sse.redis_subscriber import publish_queue_update, publish_notification
        from src.core.services.audit_service import audit_service
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Team, TeamMember, Ticket
        from src.data.repositories.ticket_repository import TicketRepository, NotificationRepository
        from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository
        from sqlalchemy import select

        try:
            async with assignment_lock(ticket_id):
                return await _do_assign(
                    ticket_id=ticket_id,
                    customer_email=customer_email,
                    settings=settings,
                    publish_queue_update=publish_queue_update,
                    publish_notification=publish_notification,
                    audit_service=audit_service,
                    CelerySessionFactory=CelerySessionFactory,
                    Team=Team,
                    TeamMember=TeamMember,
                    Ticket=Ticket,
                    TicketRepository=TicketRepository,
                    NotificationRepository=NotificationRepository,
                    NotificationPreferenceRepository=NotificationPreferenceRepository,
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
        return run_async(_run())        # ← CHANGED from asyncio.run()
    except Exception as exc:
        logger.error("auto_assign_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=15)


async def _do_assign(
    ticket_id: str,
    customer_email: str | None,
    settings,
    publish_queue_update,
    publish_notification,
    audit_service,
    CelerySessionFactory,
    Team,
    TeamMember,
    Ticket,
    TicketRepository,
    NotificationRepository,
    NotificationPreferenceRepository,
    select,
) -> dict:
    async with CelerySessionFactory() as session:
        ticket_repo = TicketRepository(session)
        notif_repo  = NotificationRepository(session)
        pref_repo   = NotificationPreferenceRepository(session)

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
        teams: list = list(teams_result.scalars().all())

        if not teams:
            logger.warning(
                "auto_assign_no_teams_for_product",
                product_id=str(ticket.product_id),
            )
            return {"error": "No teams for product"}

        selected_team = await _llm_pick_team(ticket=ticket, teams=teams)
        if not selected_team:
            selected_team = teams[0]
            logger.warning(
                "auto_assign_team_fallback",
                ticket_id=ticket_id,
                team=selected_team.name,
            )

        members_result = await session.execute(
            select(TeamMember).where(
                TeamMember.team_id == selected_team.id,
                TeamMember.is_active.is_(True),
            )
        )
        members: list = list(members_result.scalars().all())

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
            return {
                "routed": True,
                "reason": "no_members_in_team",
                "team_id": str(selected_team.id),
            }

        ticket_embedding = _compute_ticket_embedding(ticket)
        workload_map     = await _get_workload_map(session, members)

        scored: list[tuple] = []
        for member in members:
            score = _score_member(
                member=member,
                ticket_embedding=ticket_embedding,
                workload=workload_map.get(str(member.user_id), 0),
            )
            scored.append((member, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        best_member, best_score = scored[0]

        logger.info(
            "auto_assign_best_candidate",
            ticket_id=ticket_id,
            member_id=str(best_member.id),
            score=round(best_score, 3),
            threshold=ASSIGN_THRESHOLD,
        )

        if best_score >= ASSIGN_THRESHOLD:
            from src.core.celery.utils import fetch_agent_name, fetch_user_email

            agent_name     = await fetch_agent_name(best_member.user_id) or "Support Agent"
            resolved_email = customer_email or await _safe_fetch_email(ticket.customer_id)

            await ticket_repo.update_fields(ticket_id, {
                "team_id":     selected_team.id,
                "assigned_to": best_member.user_id,
            })
            await session.commit()

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
                    },
                    ticket_id=ticket.id,
                )
            except Exception as exc:
                logger.warning("audit_assign_failed", ticket_id=ticket_id, error=str(exc))

            sla_str = (
                ticket.sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
                if ticket.sla_response_due else "N/A"
            )
            sla_resolve_str = (
                ticket.sla_resolve_due.strftime("%Y-%m-%d %H:%M UTC")
                if ticket.sla_resolve_due else "N/A"
            )

            await _notify_actor(
                actor_id=str(best_member.user_id),
                ticket=ticket,
                pref_repo=pref_repo,
                notif_repo=notif_repo,
                session=session,
                settings=settings,
                is_internal=True,
                notif_type="ticket_assigned",
                title=f"Ticket {ticket.ticket_number} assigned to you",
                message=(
                    f"Ticket {ticket.ticket_number} auto-assigned to you.\n"
                    f"Priority: {ticket.priority} | Severity: {ticket.severity} | "
                    f"Tier: {ticket.tier_snapshot or 'N/A'}\n"
                    f"SLA Response due: {sla_str}"
                ),
                fallback_email=None,
                email_subject=f"[Ticketing Genie] New ticket assigned: {ticket.ticket_number}",
                email_body=(
                    f"Hi {agent_name},\n\n"
                    f"A new ticket has been assigned to you.\n\n"
                    f"Ticket:      {ticket.ticket_number}\n"
                    f"Title:       {ticket.title}\n"
                    f"Priority:    {ticket.priority}\n"
                    f"Severity:    {ticket.severity}\n"
                    f"Tier:        {ticket.tier_snapshot or 'N/A'}\n"
                    f"Environment: {ticket.environment or 'N/A'}\n\n"
                    f"SLA Response due:   {sla_str}\n"
                    f"SLA Resolution due: {sla_resolve_str}\n\n"
                    f"Please respond to the customer within your SLA window.\n\n"
                    f"— Ticketing Genie"
                ),
                recipient_id_for_email=str(best_member.user_id),
                recipient_type_for_email="persona",
            )

            publish_queue_update(
                settings.CELERY_BROKER_URL,
                str(best_member.user_id),
                _ticket_payload(ticket, "assigned"),
            )

            await _notify_customer(
                ticket=ticket,
                customer_email=resolved_email,
                pref_repo=pref_repo,
                notif_repo=notif_repo,
                session=session,
                event="assigned",
                agent_name=agent_name,
            )

            if ticket.priority in ("P0", "critical") and selected_team.team_lead_id:
                from src.core.celery.workers.notification_worker import alert_team_lead
                alert_team_lead.delay(
                    ticket_id=ticket_id,
                    ticket_number=ticket.ticket_number,
                    title=ticket.title or "",
                    priority=ticket.priority or "",
                    severity=ticket.severity or "",
                    assigned_to=str(best_member.user_id),
                    assigned_to_name=agent_name,
                    team_lead_id=str(selected_team.team_lead_id),
                    reason="Critical ticket auto-assigned — heads up.",
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
                logger.warning(
                    "audit_unassigned_failed",
                    ticket_id=ticket_id,
                    error=str(exc),
                )

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

            agent  = DraftAgent()
            result = agent.generate_draft(
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
        return run_async(_run())        # ← CHANGED from asyncio.run()
    except Exception as exc:
        logger.error("ai_draft_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=20)


# ── Priority Override Notification ────────────────────────────────────────────

@celery_app.task(name="ticket.ai.priority_override")
def run_ai_priority_override(
    ticket_id:         str,
    original_severity: str,
    new_priority:      str,
    reason:            str,
    customer_email:    str | None = None,
    customer_id:       str | None = None,
) -> None:

    async def _notify() -> None:
        from src.config.settings import settings
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository, NotificationRepository
        from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository

        async with CelerySessionFactory() as session:
            ticket = await TicketRepository(session).get_by_id(ticket_id)
            if not ticket:
                return

            notif_repo = NotificationRepository(session)
            pref_repo  = NotificationPreferenceRepository(session)

            abstract_reason = _abstract_priority_reason(original_severity, new_priority, reason)

            title   = f"Ticket {ticket.ticket_number} priority has been updated"
            message = (
                f"We have reviewed your ticket and adjusted its priority "
                f"to ensure the right level of attention.\n\n"
                f"{abstract_reason}"
            )

            await _notify_actor(
                actor_id=str(ticket.customer_id),
                ticket=ticket,
                pref_repo=pref_repo,
                notif_repo=notif_repo,
                session=session,
                settings=settings,
                is_internal=False,
                notif_type="ticket_priority_updated",
                title=title,
                message=message,
                fallback_email=customer_email,
                email_subject=f"[{ticket.ticket_number}] Your ticket priority has been updated",
                email_body=(
                    f"Dear Customer,\n\n"
                    f"We have reviewed your ticket {ticket.ticket_number} and adjusted its priority "
                    f"to ensure it receives the right level of attention.\n\n"
                    f"{abstract_reason}\n\n"
                    f"Our team will be in touch based on the updated priority.\n\n"
                    f"— Ticketing Genie Support Team"
                ),
            )

    run_async(_notify())                # ← CHANGED from asyncio.run()


def _abstract_priority_reason(original_severity: str, new_priority: str, reason: str) -> str:
    priority_label = {
        "P0": "Critical — immediate attention",
        "P1": "High — addressed within the day",
        "P2": "Medium — addressed within 2 business days",
        "P3": "Standard — addressed within our regular SLA",
    }.get(new_priority, "Standard")

    return (
        f"Your ticket has been assigned priority: {priority_label}.\n"
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


# ── Universal preference-aware notification ───────────────────────────────────

async def _notify_actor(
    actor_id:                 str,
    ticket,
    pref_repo,
    notif_repo,
    session,
    settings,
    is_internal:              bool,
    notif_type:               str,
    title:                    str,
    message:                  str,
    fallback_email:           str | None,
    email_subject:            str,
    email_body:               str,
    recipient_id_for_email:   str | None = None,
    recipient_type_for_email: str = "customer",
) -> None:
    from src.core.sse.redis_subscriber import publish_notification
    from src.data.models.postgres.models import Notification
    import uuid as _uuid

    try:
        channel = await pref_repo.get_preferred_contact(actor_id)

        logger.info(
            "notify_actor_channel_resolved",
            actor_id=actor_id,
            channel=channel,
            notif_type=notif_type,
        )

        if channel == "in_app":
            notif = Notification(
                channel="in_app",
                recipient_id=_uuid.UUID(actor_id),
                ticket_id=ticket.id,
                is_internal=is_internal,
                type=notif_type,
                title=title,
                message=message,
            )
            await notif_repo.create(notif)
            await session.commit()

            publish_notification(
                settings.CELERY_BROKER_URL,
                actor_id,
                {
                    "type":          notif_type,
                    "title":         title,
                    "message":       message,
                    "ticket_number": ticket.ticket_number,
                },
            )
        else:
            email_to_use = fallback_email
            if not email_to_use:
                email_to_use = await _safe_fetch_email_str(actor_id)

            if email_to_use:
                from src.core.celery.workers.email_worker import send_notification_email
                send_notification_email.delay(
                    recipient_id=recipient_id_for_email or actor_id,
                    recipient_type=recipient_type_for_email,
                    subject=email_subject,
                    body=email_body,
                    recipient_email=email_to_use,
                )

    except Exception as exc:
        logger.warning(
            "notify_actor_failed",
            actor_id=actor_id,
            notif_type=notif_type,
            error=str(exc),
        )


# ── Customer notification ─────────────────────────────────────────────────────

async def _notify_customer(
    ticket,
    customer_email: str | None,
    pref_repo,
    notif_repo,
    session,
    event:      str,
    agent_name: str | None = None,
) -> None:
    from src.config.settings import settings

    sla_str = (
        ticket.sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
        if ticket.sla_response_due else "N/A"
    )

    if event == "assigned":
        title   = f"Your ticket {ticket.ticket_number} has been assigned"
        message = (
            f"An agent has been assigned to your ticket.\n"
            f"Priority: {ticket.priority} | Expected response by: {sla_str}"
        )
        if agent_name:
            message = f"Agent: {agent_name}\n" + message

        email_subject = f"[{ticket.ticket_number}] Agent assigned — we're on it"
        override_line = ""
        if ticket.priority_overridden and ticket.override_reason:
            override_line = (
                f"\nNote: We reviewed your reported severity and adjusted the priority "
                f"to ensure the right response time.\n"
            )
        email_body = (
            f"Dear Customer,\n\n"
            f"Your ticket {ticket.ticket_number} has been assigned to a support agent.\n\n"
            f"Agent:                {agent_name or 'Support Agent'}\n"
            f"Priority:             {ticket.priority}\n"
            f"Expected response by: {sla_str}\n"
            f"{override_line}\n"
            f"Our agent will reach out to you shortly.\n\n"
            f"— Ticketing Genie Support Team"
        )
    else:
        title   = f"Your ticket {ticket.ticket_number} is being reviewed"
        message = (
            f"Your ticket is being reviewed by our team.\n"
            f"Expected response by: {sla_str}"
        )
        email_subject = f"[{ticket.ticket_number}] Your ticket is being reviewed"
        email_body = (
            f"Dear Customer,\n\n"
            f"Your ticket {ticket.ticket_number} has been received and is being reviewed "
            f"by our team for assignment.\n\n"
            f"Priority:             {ticket.priority}\n"
            f"Expected response by: {sla_str}\n\n"
            f"An agent will be assigned shortly.\n\n"
            f"— Ticketing Genie Support Team"
        )

    await _notify_actor(
        actor_id=str(ticket.customer_id),
        ticket=ticket,
        pref_repo=pref_repo,
        notif_repo=notif_repo,
        session=session,
        settings=settings,
        is_internal=False,
        notif_type=f"ticket_{event}",
        title=title,
        message=message,
        fallback_email=customer_email,
        email_subject=email_subject,
        email_body=email_body,
        recipient_type_for_email="customer",
    )


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
    va = np.array(a, dtype=float)
    vb = np.array(b, dtype=float)
    denom = np.linalg.norm(va) * np.linalg.norm(vb)
    if denom == 0:
        return 0.0
    return float(np.dot(va, vb) / denom)


def _score_member(
    member,
    ticket_embedding: list[float] | None,
    workload: int,
) -> float:
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
            Ticket.status.in_(["new", "open", "in_progress"]),
        )
        .group_by(Ticket.assigned_to)
    )
    return {str(row.assigned_to): row.cnt for row in r.fetchall()}


# ── Misc helpers ──────────────────────────────────────────────────────────────

async def _safe_fetch_email(user_id) -> str | None:
    try:
        from src.core.celery.utils import fetch_user_email
        return await fetch_user_email(user_id)
    except Exception as exc:
        logger.warning("fetch_customer_email_failed", error=str(exc))
        return None


async def _safe_fetch_email_str(actor_id: str) -> str | None:
    try:
        from src.core.celery.utils import fetch_user_email
        import uuid as _uuid
        return await fetch_user_email(_uuid.UUID(actor_id))
    except Exception as exc:
        logger.warning("fetch_email_failed", actor_id=actor_id, error=str(exc))
        return None


def _ticket_payload(ticket, reason: str) -> dict:
    return {
        "ticket_id":     str(ticket.id),
        "ticket_number": ticket.ticket_number,
        "title":         ticket.title or "",
        "priority":      ticket.priority or "",
        "severity":      ticket.severity or "",
        "status":        ticket.status,
        "reason":        reason,
    }