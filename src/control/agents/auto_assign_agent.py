"""Auto-assign logic — matches ticket to best agent via:

  1. Team membership  — prefer agents on the team linked to the ticket's product
  2. Cosine similarity between ticket embedding and agent skill embedding
  3. Experience weight
  4. Current workload (open ticket count)
  5. Priority/severity urgency multiplier

Falls back to any active agent when no team match is found.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.observability.logging.logger import get_logger

if TYPE_CHECKING:
    from src.data.models.postgres.models import Persona, Ticket

logger = get_logger(__name__)

# ── Urgency weight maps ───────────────────────────────────────────────────────

_PRIORITY_WEIGHT: dict[str, float] = {
    "critical": 2.0,
    "high":     1.5,
    "medium":   1.0,
    "low":      0.5,
}
_SEVERITY_WEIGHT: dict[str, float] = {
    "critical": 2.0,
    "high":     1.5,
    "medium":   1.0,
    "low":      0.5,
}


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Pure-numpy cosine similarity — returns 0.0 if either vector is zero."""
    va = np.array(a, dtype=np.float32)
    vb = np.array(b, dtype=np.float32)
    norm_a = np.linalg.norm(va)
    norm_b = np.linalg.norm(vb)
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(va, vb) / (norm_a * norm_b))


def _score_agent(
    similarity: float,
    experience: int,
    open_count: int,
    priority: str,
    severity: str,
    max_experience: int,
    max_workload: int,
    team_bonus: float = 0.0,
) -> float:
    """Composite score (higher = better).

    Base formula:
        0.50 * similarity
      + 0.25 * norm_experience   (more experienced → better)
      + 0.25 * (1 - norm_workload) (less loaded → better)

    For urgency ≥ 1.75 (high/critical) shift weight toward
    experience and availability.

    A team_bonus (0.10) is added when the agent belongs to the
    team that owns the ticket's product.
    """
    norm_exp      = (experience / max_experience) if max_experience > 0 else 0.0
    norm_workload = (open_count / max_workload)   if max_workload  > 0 else 0.0

    urgency = (
        _PRIORITY_WEIGHT.get(priority, 1.0) + _SEVERITY_WEIGHT.get(severity, 1.0)
    ) / 2.0

    if urgency >= 1.75:
        base = (
            0.40 * similarity
            + 0.35 * norm_exp
            + 0.25 * (1.0 - norm_workload)
        )
    else:
        base = (
            0.50 * similarity
            + 0.25 * norm_exp
            + 0.25 * (1.0 - norm_workload)
        )

    return (base + team_bonus) * urgency


# ── Candidate fetching ────────────────────────────────────────────────────────

async def _fetch_team_agent_ids(
    session: AsyncSession,
    product_id: str,
) -> set[str]:
    """Return persona IDs of agents on the team linked to product_id.

    Uses Team.product_id → TeamMember.agent_id join.
    Returns empty set if no team is linked.
    """
    from src.data.models.postgres.models import Team, TeamMember

    stmt = (
        select(TeamMember.agent_id)
        .join(Team, Team.id == TeamMember.team_id)
        .where(
            Team.product_id == product_id,
            Team.is_active.is_(True),
        )
    )
    result = await session.execute(stmt)
    return {str(row) for row in result.scalars().all()}


async def _fetch_candidates(
    session: AsyncSession,
    product_id: str | None,
) -> list["Persona"]:
    """Return active agent/team_lead personas.

    If product_id is given, restrict to agents on the product's team.
    Falls back to ALL active agents when the team has no members.
    """
    from src.data.models.postgres.models import Persona, Team, TeamMember

    team_member_ids: set[str] = set()
    if product_id:
        team_member_ids = await _fetch_team_agent_ids(session, product_id)

    if team_member_ids:
        stmt = select(Persona).where(
            Persona.is_active.is_(True),
            Persona.user_type.in_(["agent", "team_lead"]),
            Persona.id.in_(team_member_ids),
        )
    else:
        stmt = select(Persona).where(
            Persona.is_active.is_(True),
            Persona.user_type.in_(["agent", "team_lead"]),
        )

    result = await session.execute(stmt)
    agents = list(result.scalars().all())

    # If product-filtered query returned nothing, fall back to all agents
    if not agents and team_member_ids:
        fallback = await session.execute(
            select(Persona).where(
                Persona.is_active.is_(True),
                Persona.user_type.in_(["agent", "team_lead"]),
            )
        )
        agents = list(fallback.scalars().all())

    return agents


async def _fetch_workload(
    session: AsyncSession,
    agent_ids: list[str],
) -> dict[str, int]:
    """Return open-ticket count per agent id (only for supplied agent ids)."""
    from src.data.models.postgres.models import Ticket as TicketORM

    stmt = (
        select(TicketORM.assigned_to, func.count(TicketORM.id).label("cnt"))
        .where(
            TicketORM.status.in_(["new", "acknowledged", "in_progress"]),
            TicketORM.assigned_to.in_(agent_ids),
        )
        .group_by(TicketORM.assigned_to)
    )
    result = await session.execute(stmt)
    return {str(row.assigned_to): row.cnt for row in result}


# ── Public entry point ────────────────────────────────────────────────────────

async def find_best_agent(
    session: AsyncSession,
    ticket: "Ticket",
    ticket_embedding: list[float] | None,
) -> "Persona | None":
    """Score all candidate agents and return the best match.

    Scoring factors (in order of influence):
      - Team membership for ticket's product (bonus 0.10)
      - Skill embedding cosine similarity
      - Experience
      - Current workload
      - Priority / severity urgency multiplier
    """
    priority = (ticket.priority or "low").lower()
    severity = (ticket.severity or "low").lower()
    product_id = str(ticket.product_id) if ticket.product_id else None

    # 1. Fetch candidates (team-filtered or all active)
    agents = await _fetch_candidates(session, product_id)
    if not agents:
        logger.warning("no_active_agents_found", ticket_id=str(ticket.id))
        return None

    # Determine which agents are on the product's team (for bonus)
    team_agent_ids: set[str] = set()
    if product_id:
        team_agent_ids = await _fetch_team_agent_ids(session, product_id)

    # 2. Fetch workload
    agent_ids = [str(a.id) for a in agents]
    workload_map = await _fetch_workload(session, agent_ids)

    # 3. Score each agent
    max_experience = max((a.experience or 0) for a in agents) or 1
    max_workload   = max(workload_map.values(), default=1) or 1

    scored: list[tuple[float, "Persona"]] = []

    for agent in agents:
        open_count  = workload_map.get(str(agent.id), 0)
        experience  = agent.experience or 0
        team_bonus  = 0.10 if str(agent.id) in team_agent_ids else 0.0

        # Cosine similarity — neutral 0.5 when embeddings are absent
        if ticket_embedding is not None and agent.agent_skill_embeddings is not None:
            similarity = _cosine_similarity(ticket_embedding, agent.agent_skill_embeddings)
        else:
            similarity = 0.5

        score = _score_agent(
            similarity=similarity,
            experience=experience,
            open_count=open_count,
            priority=priority,
            severity=severity,
            max_experience=max_experience,
            max_workload=max_workload,
            team_bonus=team_bonus,
        )

        logger.debug(
            "agent_scored",
            agent_id=str(agent.id),
            similarity=round(similarity, 4),
            experience=experience,
            open_count=open_count,
            team_bonus=team_bonus,
            score=round(score, 4),
        )
        scored.append((score, agent))

    if not scored:
        return None

    best_score, best_agent = max(scored, key=lambda x: x[0])
    logger.info(
        "best_agent_selected",
        agent_id=str(best_agent.id),
        score=round(best_score, 4),
        ticket_id=str(ticket.id),
        on_product_team=str(best_agent.id) in team_agent_ids,
    )
    return best_agent