"""
AI Celery worker — classification, SLA assignment, auto-assign (team-aware), draft.
ticket-service: src/core/celery/workers/ai_worker.py
"""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

load_dotenv(override=True)
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "")

from src.core.celery.app import celery_app
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

ASSIGN_THRESHOLD = 0.45

# ── Module-level singleton — loaded once per worker process ───────────────────
_embedding_model = None


def _get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        _embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        logger.info("embedding_model_loaded")
    return _embedding_model


# ── AI Classification ─────────────────────────────────────────────────────────

@celery_app.task(
    name="ticket.ai.classify",
    bind=True,
    max_retries=3,
)
def run_ai_classification(
    self,
    ticket_id: str,
    customer_email: str | None = None,
) -> dict:
    """
    Classify ticket:
      1. LLM derives system severity from title + description
      2. Resolve tier_name → tier_id via auth-service HTTP (cached per process)
      3. severity + tier_id → SeverityPriorityMap → priority
      4. priority + tier_id → SLARule → sla_response_due + sla_resolve_due
    Then chains: auto_assign + draft
    """

    async def _run() -> dict:
        from src.control.agents.clasifier_agent import ClassifierAgent
        from src.core.celery.utils import fetch_tier_id
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository
        from src.data.repositories.admin_repository import (
            SLARuleRepository,
            SeverityPriorityMapRepository,
        )
        from datetime import datetime, timezone, timedelta

        async with CelerySessionFactory() as session:
            repo     = TicketRepository(session)
            sla_repo = SLARuleRepository(session)
            sev_repo = SeverityPriorityMapRepository(session)

            ticket = await repo.get_by_id(ticket_id)
            if not ticket:
                logger.error("ai_classify_ticket_not_found", ticket_id=ticket_id)
                return {"error": f"Ticket {ticket_id} not found"}

            # ── Step 1: LLM classifies system severity ────────────────────────
            agent  = ClassifierAgent()
            result = await agent.classify(
                title=ticket.title or "",
                description=ticket.description or "",
            )
            system_severity = result.severity

            # ── Step 2: resolve tier_name → tier_id via auth-service HTTP ─────
            tier_name = ticket.tier_snapshot or "starter"
            tier_id   = await fetch_tier_id(tier_name)

            if not tier_id:
                logger.warning(
                    "ai_classify_tier_id_unresolved",
                    ticket_id=ticket_id,
                    tier_name=tier_name,
                )

            # ── Step 3: severity + tier_id → priority ─────────────────────────
            sev_map = None
            if tier_id:
                sev_map = await sev_repo.get_by_severity_and_tier(system_severity, tier_id)

            system_priority = sev_map.derived_priority if sev_map else "P3"

            # ── Step 4: SLA deadlines ─────────────────────────────────────────
            sla = None
            if tier_id:
                sla = await sla_repo.get_by_tier_and_priority(tier_id, system_priority)

            now = datetime.now(timezone.utc)
            sla_response_due = (
                now + timedelta(minutes=sla.response_time_min) if sla else None
            )
            sla_resolve_due = (
                now + timedelta(minutes=sla.resolution_time_min) if sla else None
            )

            # ── Step 5: override flag ─────────────────────────────────────────
            priority_overridden = (
                ticket.customer_priority is not None
                and system_severity != ticket.customer_priority
            )

            await repo.update_fields(ticket_id, {
                "severity":            system_severity,
                "priority":            system_priority,
                "priority_overridden": priority_overridden,
                "override_reason":     result.reason if priority_overridden else None,
                "sla_response_due":    sla_response_due,
                "sla_resolve_due":     sla_resolve_due,
            })

            logger.info(
                "ticket_classified",
                ticket_id=ticket_id,
                system_severity=system_severity,
                system_priority=system_priority,
                tier_name=tier_name,
                tier_id=tier_id,
                priority_overridden=priority_overridden,
            )

            # ── Step 6: notify customer of override ───────────────────────────
            if priority_overridden and customer_email:
                run_ai_priority_override.delay(
                    ticket_id,
                    ticket.customer_priority,
                    system_priority,
                    result.reason,
                    customer_email,
                )

            await session.commit()

        # ── Chain: auto_assign + draft run in parallel ────────────────────────
        run_auto_assign.delay(ticket_id, customer_email=customer_email)
        run_ai_draft.delay(ticket_id)

        return {
            "severity":   system_severity,
            "priority":   system_priority,
            "overridden": priority_overridden,
        }

    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error("ai_classify_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


# ── Auto-Assign ───────────────────────────────────────────────────────────────

@celery_app.task(
    name="ticket.ai.auto_assign",
    bind=True,
    max_retries=3,
)
def run_auto_assign(
    self,
    ticket_id: str,
    customer_email: str | None = None,
) -> dict:
    """
    Auto-assign flow:
      1. product_id → active teams for that product
      2. TeamRoutingAgent (ChatGroq + structured output) picks the team
      3. Score all active members of that team:
            skill_score  (cosine similarity: ticket embedding vs member skill embedding)
            exp_score    (years of experience, capped at 10 yrs)
            load_score   (inverse of open ticket count)
      4. best_score >= ASSIGN_THRESHOLD → assign to agent queue
         else                           → route to team_lead queue
      5. Notify agent (in-app + email) and customer (email) in both branches
    """

    async def _run() -> dict:
        from src.config.settings import settings
        from src.core.sse.redis_subscriber import publish_queue_update, publish_notification
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.models.postgres.models import Notification, Team, TeamMember, Ticket
        from src.data.repositories.ticket_repository import (
            TicketRepository,
            NotificationRepository,
        )
        from sqlalchemy import select

        async with CelerySessionFactory() as session:
            ticket_repo = TicketRepository(session)
            notif_repo  = NotificationRepository(session)

            ticket = await ticket_repo.get_by_id(ticket_id)
            if not ticket:
                logger.error("auto_assign_ticket_not_found", ticket_id=ticket_id)
                return {"error": "Ticket not found"}

            if ticket.assigned_to:
                logger.info("auto_assign_skipped_already_assigned", ticket_id=ticket_id)
                return {"skipped": True}

            if not ticket.product_id:
                logger.warning("auto_assign_no_product_id", ticket_id=ticket_id)
                return {"error": "No product_id on ticket"}

            # ── Step 1: teams for this product ────────────────────────────────
            teams_result = await session.execute(
                select(Team).where(
                    Team.product_id == ticket.product_id,
                    Team.is_active.is_(True),
                )
            )
            teams: list[Team] = list(teams_result.scalars().all())

            if not teams:
                logger.warning(
                    "auto_assign_no_teams_for_product",
                    product_id=str(ticket.product_id),
                )
                return {"error": "No teams for product"}

            # ── Step 2: TeamRoutingAgent picks the correct team ───────────────
            selected_team = await _llm_pick_team(ticket=ticket, teams=teams)
            if not selected_team:
                selected_team = teams[0]
                logger.warning(
                    "auto_assign_team_fallback",
                    ticket_id=ticket_id,
                    team=selected_team.name,
                )

            # ── Step 3: load active team members ──────────────────────────────
            members_result = await session.execute(
                select(TeamMember).where(
                    TeamMember.team_id == selected_team.id,
                    TeamMember.is_active.is_(True),
                )
            )
            members: list[TeamMember] = list(members_result.scalars().all())

            if not members:
                logger.warning("auto_assign_no_members", team_id=str(selected_team.id))
                await ticket_repo.update_fields(ticket_id, {"team_id": selected_team.id})
                await session.commit()

                if selected_team.team_lead_id:
                    publish_queue_update(
                        settings.CELERY_BROKER_URL,
                        str(selected_team.team_lead_id),
                        _ticket_payload(ticket, "no_agents_in_team"),
                    )

                _fire_customer_pending_email(ticket, customer_email)
                return {"routed_to_team_lead": True, "reason": "no_members"}

            # ── Step 4: score each member ─────────────────────────────────────
            ticket_embedding = _compute_ticket_embedding(ticket)
            workload_map     = await _get_workload_map(session, members)

            scored: list[tuple[TeamMember, float]] = []
            for member in members:
                score = _score_member(
                    member=member,
                    ticket_embedding=ticket_embedding,
                    workload=workload_map.get(str(member.user_id), 0),
                )
                scored.append((member, score))
                logger.debug(
                    "auto_assign_member_score",
                    member_id=str(member.id),
                    score=round(score, 3),
                )

            scored.sort(key=lambda x: x[1], reverse=True)
            best_member, best_score = scored[0]

            logger.info(
                "auto_assign_best_candidate",
                ticket_id=ticket_id,
                member_id=str(best_member.id),
                score=round(best_score, 3),
                threshold=ASSIGN_THRESHOLD,
            )

            # ── Step 5a: assign to agent ──────────────────────────────────────
            if best_score >= ASSIGN_THRESHOLD:
                from src.core.celery.utils import fetch_agent_name, fetch_user_email

                agent_name     = await fetch_agent_name(best_member.user_id) or "Support Agent"
                resolved_email = customer_email or await fetch_user_email(ticket.customer_id)

                await ticket_repo.update_fields(ticket_id, {
                    "team_id":     selected_team.id,
                    "assigned_to": best_member.user_id,
                })

                notif = Notification(
                    channel="in_app",
                    recipient_id=best_member.user_id,
                    ticket_id=ticket.id,
                    is_internal=True,
                    type="ticket_assigned",
                    title=f"Ticket {ticket.ticket_number} assigned to you",
                    message=(
                        f"Ticket {ticket.ticket_number} auto-assigned to you.\n"
                        f"Priority: {ticket.priority} | Severity: {ticket.severity}\n"
                        f"Tier: {ticket.tier_snapshot or 'N/A'} | "
                        f"SLA Response due: "
                        + (ticket.sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
                           if ticket.sla_response_due else "N/A")
                    ),
                )
                await notif_repo.create(notif)
                await session.commit()

                publish_queue_update(
                    settings.CELERY_BROKER_URL,
                    str(best_member.user_id),
                    _ticket_payload(ticket, "assigned"),
                )
                publish_notification(
                    settings.CELERY_BROKER_URL,
                    str(best_member.user_id),
                    {
                        "type":          "ticket_assigned",
                        "title":         f"Ticket {ticket.ticket_number} assigned to you",
                        "message":       (
                            f"Priority: {ticket.priority} | "
                            f"Severity: {ticket.severity} | "
                            f"Tier: {ticket.tier_snapshot or 'N/A'}"
                        ),
                        "ticket_number": ticket.ticket_number,
                    },
                )

                logger.info(
                    "ticket_auto_assigned",
                    ticket_id=ticket_id,
                    agent_user_id=str(best_member.user_id),
                    agent_name=agent_name,
                    score=round(best_score, 3),
                )

                _fire_agent_email(ticket, best_member, agent_name)
                _fire_customer_assignment_email(ticket, agent_name, resolved_email)

                if ticket.priority in ("P0", "critical"):
                    from src.core.celery.workers.notification_worker import alert_team_leads
                    alert_team_leads.delay(
                        ticket_id=ticket_id,
                        ticket_number=ticket.ticket_number,
                        title=ticket.title or "",
                        priority=ticket.priority or "",
                        severity=ticket.severity or "",
                        assigned_to=str(best_member.user_id),
                        assigned_to_name=agent_name,
                        reason="Critical ticket auto-assigned — immediate TL awareness required.",
                    )

                return {"assigned_to": str(best_member.user_id), "score": best_score}

            # ── Step 5b: below threshold → team lead queue ────────────────────
            else:
                await ticket_repo.update_fields(ticket_id, {
                    "team_id":     selected_team.id,
                    "assigned_to": None,
                })
                await session.commit()

                logger.info(
                    "auto_assign_routed_to_team_lead",
                    ticket_id=ticket_id,
                    team_id=str(selected_team.id),
                    best_score=round(best_score, 3),
                )

                if selected_team.team_lead_id:
                    publish_queue_update(
                        settings.CELERY_BROKER_URL,
                        str(selected_team.team_lead_id),
                        _ticket_payload(ticket, "needs_manual_assign"),
                    )
                    publish_notification(
                        settings.CELERY_BROKER_URL,
                        str(selected_team.team_lead_id),
                        {
                            "type":          "ticket_needs_assign",
                            "title":         f"Ticket {ticket.ticket_number} needs manual assignment",
                            "message":       (
                                f"No agent met threshold ({best_score:.2f}). Please assign."
                            ),
                            "ticket_number": ticket.ticket_number,
                        },
                    )

                resolved_email = customer_email or await _safe_fetch_email(ticket.customer_id)
                _fire_customer_pending_email(ticket, resolved_email)

                return {"routed_to_team_lead": True, "score": best_score}

    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error("auto_assign_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=15)


# ── AI Draft ──────────────────────────────────────────────────────────────────

@celery_app.task(
    name="ticket.ai.draft",
    bind=True,
    max_retries=2,
)
def run_ai_draft(self, ticket_id: str) -> dict:
    """Generate AI draft reply and persist to ticket.ai_draft."""

    async def _run() -> dict:
        from src.control.agents.draft_agent import DraftAgent
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
            await repo.update_fields(ticket_id, {"ai_draft": result.body if result else ""})
            await session.commit()

        logger.info("ai_draft_generated", ticket_id=ticket_id)
        return {"draft_generated": True}

    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.error("ai_draft_error", ticket_id=ticket_id, error=str(exc))
        raise self.retry(exc=exc, countdown=20)


# ── Priority Override Notification ────────────────────────────────────────────

@celery_app.task(name="ticket.ai.priority_override")
def run_ai_priority_override(
    ticket_id: str,
    original_severity: str,
    new_priority: str,
    reason: str,
    customer_email: str | None = None,
) -> None:
    """Notify customer when AI overrides their submitted severity."""

    async def _notify() -> None:
        from src.data.clients.postgres_client import CelerySessionFactory
        from src.data.repositories.ticket_repository import TicketRepository

        async with CelerySessionFactory() as session:
            ticket = await TicketRepository(session).get_by_id(ticket_id)
            if not ticket or not customer_email:
                return

        from src.core.celery.workers.email_worker import send_notification_email
        send_notification_email.delay(
            recipient_id=str(ticket.customer_id),
            recipient_type="customer",
            subject=f"[{ticket.ticket_number}] Priority updated by system",
            body=(
                f"Dear Customer,\n\n"
                f"The severity of your ticket {ticket.ticket_number} has been "
                f"reviewed. Your reported severity was '{original_severity}', "
                f"and our system has assigned priority '{new_priority}'.\n\n"
                f"Reason: {reason}\n\n"
                f"This ensures your ticket receives the appropriate attention.\n\n"
                f"— Ticketing Genie"
            ),
            recipient_email=customer_email,
        )

    asyncio.run(_notify())


# ── Team routing via TeamRoutingAgent ─────────────────────────────────────────

async def _llm_pick_team(ticket, teams: list) -> object | None:
    """
    Use TeamRoutingAgent (ChatGroq + structured output) to pick the best team.

    Exact name match first, then case-insensitive fallback.
    Returns None if name cannot be matched — caller falls back to teams[0].
    """
    from src.control.agents.routing_agent import TeamRoutingAgent

    team_names = [t.name for t in teams]

    agent  = TeamRoutingAgent()
    result = await agent.route(
        title=ticket.title or "",
        description=ticket.description or "",
        severity=ticket.severity or "unknown",
        team_names=team_names,
    )

    # Exact match
    for team in teams:
        if team.name == result.team_name:
            logger.info(
                "auto_assign_team_selected",
                team=team.name,
                reason=result.reason,
                ticket_id=str(ticket.id),
            )
            return team

    # Case-insensitive fallback
    for team in teams:
        if team.name.lower() == result.team_name.lower():
            logger.info(
                "auto_assign_team_selected_case_insensitive",
                team=team.name,
                ticket_id=str(ticket.id),
            )
            return team

    logger.warning(
        "auto_assign_team_name_not_matched",
        returned=result.team_name,
        available=team_names,
    )
    return None


# ── Embedding helpers ─────────────────────────────────────────────────────────

def _compute_ticket_embedding(ticket) -> list[float] | None:
    """
    Return ticket embedding as a plain Python list of floats.
    Uses stored ticket_embedding if present, otherwise computes on the fly
    using the module-level cached SentenceTransformer model.
    """
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
    """
    Composite score:
      skill_score  cosine similarity ticket ↔ member skill embedding  weight=0.5
      exp_score    experience normalised to [0,1], capped at 10 yrs   weight=0.2
      load_score   inverse workload normalised to [0,1]                weight=0.3
    """
    skill_score = 0.0
    if ticket_embedding is not None and member.skill_embedding is not None:
        skill_score = max(
            0.0,
            _cosine_similarity(
                list(ticket_embedding),
                list(member.skill_embedding),
            ),
        )

    exp_score  = min((member.experience or 0) / 10.0, 1.0)
    load_score = max(0.0, 1.0 - (workload / 20.0))

    return (skill_score * 0.5) + (exp_score * 0.2) + (load_score * 0.3)


async def _get_workload_map(session, members: list) -> dict[str, int]:
    """Returns {user_id_str → open_ticket_count} for each member."""
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


def _fire_agent_email(ticket, member, agent_name: str) -> None:
    try:
        from src.core.celery.workers.email_worker import send_notification_email

        sla_response = (
            ticket.sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
            if ticket.sla_response_due else "N/A"
        )
        sla_resolve = (
            ticket.sla_resolve_due.strftime("%Y-%m-%d %H:%M UTC")
            if ticket.sla_resolve_due else "N/A"
        )

        send_notification_email.delay(
            recipient_id=str(member.user_id),
            recipient_type="persona",
            subject=f"[Ticketing Genie] New ticket assigned: {ticket.ticket_number}",
            body=(
                f"Hi {agent_name},\n\n"
                f"A new ticket has been assigned to you.\n\n"
                f"Ticket:      {ticket.ticket_number}\n"
                f"Title:       {ticket.title}\n"
                f"Priority:    {ticket.priority}\n"
                f"Severity:    {ticket.severity}\n"
                f"Tier:        {ticket.tier_snapshot or 'N/A'}\n"
                f"Environment: {ticket.environment or 'N/A'}\n\n"
                f"SLA Response due:   {sla_response}\n"
                f"SLA Resolution due: {sla_resolve}\n\n"
                f"Please respond to the customer within your SLA window.\n\n"
                f"— Ticketing Genie"
            ),
        )
    except Exception as exc:
        logger.warning("agent_email_fire_failed", error=str(exc))


def _fire_customer_assignment_email(
    ticket, agent_name: str, customer_email: str | None
) -> None:
    if not customer_email:
        return
    try:
        from src.core.celery.workers.email_worker import send_notification_email

        sla_response = (
            ticket.sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
            if ticket.sla_response_due else "N/A"
        )

        override_line = ""
        if ticket.priority_overridden and ticket.override_reason:
            override_line = (
                f"\nNote: Your reported severity was reviewed by our system. "
                f"Priority has been set to {ticket.priority}.\n"
                f"Reason: {ticket.override_reason}\n"
            )

        send_notification_email.delay(
            recipient_id=str(ticket.customer_id),
            recipient_type="customer",
            subject=f"[{ticket.ticket_number}] Agent assigned — we're on it",
            body=(
                f"Dear Customer,\n\n"
                f"Your ticket {ticket.ticket_number} has been assigned to our support agent.\n\n"
                f"Agent:                {agent_name}\n"
                f"Priority:             {ticket.priority}\n"
                f"Severity:             {ticket.severity}\n"
                f"Expected response by: {sla_response}\n"
                f"{override_line}\n"
                f"Our agent will reach out to you shortly. "
                f"You can track your ticket status in the portal.\n\n"
                f"— Ticketing Genie Support Team"
            ),
            recipient_email=customer_email,
        )
    except Exception as exc:
        logger.warning("customer_assignment_email_failed", error=str(exc))


def _fire_customer_pending_email(ticket, customer_email: str | None) -> None:
    if not customer_email:
        return
    try:
        from src.core.celery.workers.email_worker import send_notification_email

        sla_response = (
            ticket.sla_response_due.strftime("%Y-%m-%d %H:%M UTC")
            if ticket.sla_response_due else "N/A"
        )

        override_line = ""
        if ticket.priority_overridden and ticket.override_reason:
            override_line = (
                f"\nNote: Your reported severity was reviewed by our system. "
                f"Priority has been set to {ticket.priority}.\n"
                f"Reason: {ticket.override_reason}\n"
            )

        send_notification_email.delay(
            recipient_id=str(ticket.customer_id),
            recipient_type="customer",
            subject=f"[{ticket.ticket_number}] Your ticket is being reviewed",
            body=(
                f"Dear Customer,\n\n"
                f"Your ticket {ticket.ticket_number} has been received and is currently "
                f"being reviewed by our team lead for assignment.\n\n"
                f"Priority:             {ticket.priority}\n"
                f"Severity:             {ticket.severity}\n"
                f"Expected response by: {sla_response}\n"
                f"{override_line}\n"
                f"An agent will be assigned shortly and will reach out to you. "
                f"You can track your ticket status in the portal.\n\n"
                f"— Ticketing Genie Support Team"
            ),
            recipient_email=customer_email,
        )
    except Exception as exc:
        logger.warning("customer_pending_email_failed", error=str(exc))