"""
TeamLeadRepository — queries scoped to a team lead's team.
src/data/repositories/team_lead_repository.py
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Team, TeamMember, Ticket


class TeamLeadRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    # ── Team lookup ───────────────────────────────────────────────────────────

    async def get_team_by_lead(self, lead_user_id: str) -> Optional[Team]:
        """Get the team where this user is the team_lead."""
        r = await self._s.execute(
            select(Team).where(
                Team.team_lead_id == uuid.UUID(lead_user_id),
                Team.is_active.is_(True),
            )
        )
        return r.scalar_one_or_none()

    async def get_team_by_id(self, team_id: str) -> Optional[Team]:
        r = await self._s.execute(
            select(Team).where(Team.id == uuid.UUID(team_id))
        )
        return r.scalar_one_or_none()

    # ── Unassigned queue ──────────────────────────────────────────────────────

    async def get_unassigned_tickets(
        self,
        team_id: str,
        limit: int = 100,
    ) -> list[Ticket]:
        r = await self._s.execute(
            select(Ticket)
            .where(
                Ticket.team_id    == uuid.UUID(team_id),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(["new", "open"]),
            )
            .order_by(Ticket.priority.asc(), Ticket.created_at.asc())
            .limit(limit)
        )
        return list(r.scalars().all())

    # ── All tickets for team (assigned + unassigned) ──────────────────────────

    async def get_all_team_tickets(
        self,
        team_id: str,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[Ticket]:
        q = select(Ticket).where(Ticket.team_id == uuid.UUID(team_id))
        if status:
            q = q.where(Ticket.status == status)
        q = q.order_by(Ticket.priority.asc(), Ticket.created_at.asc()).limit(limit)
        r = await self._s.execute(q)
        return list(r.scalars().all())

    # ── Single ticket (must belong to team) ──────────────────────────────────

    async def get_ticket(self, ticket_id: str, team_id: str) -> Optional[Ticket]:
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.id      == uuid.UUID(ticket_id),
                Ticket.team_id == uuid.UUID(team_id),
            )
        )
        return r.scalar_one_or_none()

    # ── Assign ticket to agent ────────────────────────────────────────────────

    async def assign_ticket(self, ticket_id: str, agent_user_id: str) -> None:
        await self._s.execute(
            update(Ticket)
            .where(Ticket.id == uuid.UUID(ticket_id))
            .values(assigned_to=uuid.UUID(agent_user_id), status="open")
        )
        await self._s.flush()

    # ── Update ticket status ──────────────────────────────────────────────────

    async def update_ticket_status(self, ticket_id: str, status: str) -> None:
        from datetime import datetime, timezone
        extra = {}
        now   = datetime.now(timezone.utc)
        if status == "resolved":
            extra = {"resolved_at": now}
        elif status == "closed":
            extra = {"closed_at": now}
        await self._s.execute(
            update(Ticket)
            .where(Ticket.id == uuid.UUID(ticket_id))
            .values(status=status, **extra)
        )
        await self._s.flush()

    # ── Agent workload for team ───────────────────────────────────────────────

    async def get_agent_workloads(
        self, team_id: str
    ) -> list[tuple[TeamMember, int]]:
        """Returns (TeamMember, open_ticket_count) for each active member."""
        members_r = await self._s.execute(
            select(TeamMember).where(
                TeamMember.team_id   == uuid.UUID(team_id),
                TeamMember.is_active.is_(True),
            )
        )
        members = list(members_r.scalars().all())

        user_ids = [m.user_id for m in members]
        if not user_ids:
            return []

        counts_r = await self._s.execute(
            select(Ticket.assigned_to, func.count().label("cnt"))
            .where(
                Ticket.assigned_to.in_(user_ids),
                Ticket.status.in_(["new", "open", "in_progress"]),
            )
            .group_by(Ticket.assigned_to)
        )
        count_map = {str(row.assigned_to): row.cnt for row in counts_r.fetchall()}

        return [
            (member, count_map.get(str(member.user_id), 0))
            for member in members
        ]

    # ── Unassigned count ──────────────────────────────────────────────────────

    async def unassigned_count(self, team_id: str) -> int:
        r = await self._s.execute(
            select(func.count()).select_from(Ticket).where(
                Ticket.team_id    == uuid.UUID(team_id),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(["new", "open"]),
            )
        )
        return r.scalar() or 0