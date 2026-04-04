from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Team, TeamMember, Ticket



# Active (non-terminal) ticket statuses
_ACTIVE_STATUSES = ["new", "acknowledged", "assigned", "in_progress", "on_hold", "reopened"]

# Statuses that mean the ticket has not yet been assigned to an agent
_UNASSIGNED_STATUSES = ["new", "acknowledged"]


class TeamLeadRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    # ── Team lookup ───────────────────────────────────────────────────────────

    async def get_team_by_lead(self, lead_user_id: str) -> Optional[Team]:
        """
        Returns the first active team for this TL.
        TLs lead the same team across multiple products — all are active.
        Used for single-team ops (assign, status, single ticket).
        """
        r = await self._s.execute(
            select(Team)
            .where(
                Team.team_lead_id == uuid.UUID(lead_user_id),
                Team.is_active.is_(True),
            )
            .order_by(Team.created_at.asc())
            .limit(1)
        )
        return r.scalars().first()

    async def get_all_team_ids_by_lead(self, lead_user_id: str) -> list[uuid.UUID]:
        """
        Returns ALL active team IDs for this TL across all products.
        Used for member/ticket aggregation across products.
        """
        r = await self._s.execute(
            select(Team.id)
            .where(
                Team.team_lead_id == uuid.UUID(lead_user_id),
                Team.is_active.is_(True),
            )
            .order_by(Team.created_at.asc())
        )
        return list(r.scalars().all())

    async def get_team_by_id(self, team_id: str) -> Optional[Team]:
        r = await self._s.execute(
            select(Team).where(Team.id == uuid.UUID(team_id))
        )
        return r.scalar_one_or_none()

    # ── Unassigned queue — single team ────────────────────────────────────────

    async def get_unassigned_tickets(
        self,
        team_id: str,
        limit: int = 100,
    ) -> list[Ticket]:
        r = await self._s.execute(
            select(Ticket)
            .where(
                Ticket.team_id == uuid.UUID(team_id),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(_UNASSIGNED_STATUSES),  # fixed: was ["new", "open"]
            )
            .order_by(Ticket.priority.asc(), Ticket.created_at.asc())
            .limit(limit)
        )
        return list(r.scalars().all())

    # ── Unassigned queue — across all product teams ───────────────────────────

    async def get_unassigned_tickets_multi(
        self,
        team_ids: list[str],
        limit: int = 100,
    ) -> list[Ticket]:
        """Unassigned tickets across all product teams for this TL."""
        uuids = [uuid.UUID(tid) for tid in team_ids]
        r = await self._s.execute(
            select(Ticket)
            .where(
                Ticket.team_id.in_(uuids),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(_UNASSIGNED_STATUSES),  # fixed: was ["new", "open"]
            )
            .order_by(Ticket.priority.asc(), Ticket.created_at.asc())
            .limit(limit)
        )
        return list(r.scalars().all())

    # ── All tickets — single team ─────────────────────────────────────────────

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

    # ── All tickets — across all product teams ────────────────────────────────

    async def get_all_team_tickets_multi(
        self,
        team_ids: list[str],
        status: Optional[str] = None,
        limit: int = 200,
    ) -> list[Ticket]:
        """All tickets across all product teams for this TL."""
        uuids = [uuid.UUID(tid) for tid in team_ids]
        q = select(Ticket).where(Ticket.team_id.in_(uuids))
        if status:
            q = q.where(Ticket.status == status)
        q = q.order_by(Ticket.priority.asc(), Ticket.created_at.asc()).limit(limit)
        r = await self._s.execute(q)
        return list(r.scalars().all())

    # ── Single ticket (must belong to any of TL's teams) ─────────────────────

    async def get_ticket(self, ticket_id: str, team_id: str) -> Optional[Ticket]:
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.id      == uuid.UUID(ticket_id),
                Ticket.team_id == uuid.UUID(team_id),
            )
        )
        return r.scalar_one_or_none()

    async def get_ticket_by_any_team(
        self,
        ticket_id: str,
        team_ids: list[str],
    ) -> Optional[Ticket]:
        """Get ticket that belongs to any of the TL's product teams."""
        uuids = [uuid.UUID(tid) for tid in team_ids]
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.id.in_([uuid.UUID(ticket_id)]),
                Ticket.team_id.in_(uuids),
            )
        )
        return r.scalar_one_or_none()

    # ── Assign ticket to agent ────────────────────────────────────────────────

    async def assign_ticket(self, ticket_id: str, agent_user_id: str) -> None:
        await self._s.execute(
            update(Ticket)
            .where(Ticket.id == uuid.UUID(ticket_id))
            .values(assigned_to=uuid.UUID(agent_user_id), status="in_progress")
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

    # ── Agent workload — single team ──────────────────────────────────────────

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
                Ticket.status.in_(_ACTIVE_STATUSES),  # fixed: was ["new", "open", "in_progress"]
            )
            .group_by(Ticket.assigned_to)
        )
        count_map = {str(row.assigned_to): row.cnt for row in counts_r.fetchall()}

        return [
            (member, count_map.get(str(member.user_id), 0))
            for member in members
        ]
    
    async def update_member_skill(
        self,
        agent_user_id: str,
        team_ids:      list[str],
        skill_text:    str,
    ) -> Optional[TeamMember]:
        """
        Update skills JSONB for a TeamMember scoped to the TL's teams.
        Returns the updated TeamMember or None if not found.
        """
        uuids = [uuid.UUID(tid) for tid in team_ids]
        r = await self._s.execute(
            select(TeamMember).where(
                TeamMember.user_id  == uuid.UUID(agent_user_id),
                TeamMember.team_id.in_(uuids),
                TeamMember.is_active.is_(True),
            )
            .limit(1)
        )
        member = r.scalars().first()
        if not member:
            return None
 
        # Merge into existing JSONB, preserving any other keys
        existing = dict(member.skills) if member.skills else {}
        existing["skill_text"] = skill_text
        member.skills = existing
        await self._s.flush()
        return member

    # ── Agent workload — across all product teams ─────────────────────────────

    async def get_agent_workloads_multi(
        self,
        team_ids: list[str],
    ) -> list[tuple[TeamMember, int]]:
        """
        Agent workloads across all product teams.
        Deduplication by user_id is done here in the repo.
        """
        uuids = [uuid.UUID(tid) for tid in team_ids]
        members_r = await self._s.execute(
            select(TeamMember).where(
                TeamMember.team_id.in_(uuids),
                TeamMember.is_active.is_(True),
            )
        )
        members = list(members_r.scalars().all())
        if not members:
            return []

        # Deduplicate members by user_id — same agent in multiple product teams
        seen: set[str] = set()
        unique_members = []
        for m in members:
            uid = str(m.user_id)
            if uid not in seen:
                seen.add(uid)
                unique_members.append(m)

        user_ids = [m.user_id for m in unique_members]
        counts_r = await self._s.execute(
            select(Ticket.assigned_to, func.count().label("cnt"))
            .where(
                Ticket.assigned_to.in_(user_ids),
                Ticket.status.in_(_ACTIVE_STATUSES),  # fixed: was ["new", "open", "in_progress"]
            )
            .group_by(Ticket.assigned_to)
        )
        count_map = {str(row.assigned_to): row.cnt for row in counts_r.fetchall()}

        return [
            (member, count_map.get(str(member.user_id), 0))
            for member in unique_members
        ]

    # ── Unassigned count — single team ────────────────────────────────────────

    async def unassigned_count(self, team_id: str) -> int:
        r = await self._s.execute(
            select(func.count()).select_from(Ticket).where(
                Ticket.team_id    == uuid.UUID(team_id),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(_UNASSIGNED_STATUSES),  # fixed: was ["new", "open"]
            )
        )
        return r.scalar() or 0

    # ── Unassigned count — across all product teams ───────────────────────────

    async def unassigned_count_multi(self, team_ids: list[str]) -> int:
        """Unassigned ticket count across all product teams."""
        uuids = [uuid.UUID(tid) for tid in team_ids]
        r = await self._s.execute(
            select(func.count()).select_from(Ticket).where(
                Ticket.team_id.in_(uuids),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(_UNASSIGNED_STATUSES),  # fixed: was ["new", "open"]
            )
        )
        return r.scalar() or 0

    # ── Thread + internal notes ───────────────────────────────────────────────

    async def get_ticket_conversations(
        self,
        ticket_id: str,
        team_ids: list[str],
    ) -> tuple[list, list]:
        """
        Returns (conversations, attachments) for a ticket.
        Ticket must belong to any of the TL's product teams.
        """
        from src.data.models.postgres.models import Conversation, Attachment

        ticket = await self.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise ValueError(f"Ticket {ticket_id} not found in your team.")

        conv_r = await self._s.execute(
            select(Conversation)
            .where(Conversation.ticket_id == uuid.UUID(ticket_id))
            .order_by(Conversation.created_at.asc())
        )
        conversations = list(conv_r.scalars().all())

        att_r = await self._s.execute(
            select(Attachment)
            .where(Attachment.ticket_id == uuid.UUID(ticket_id))
            .order_by(Attachment.created_at.asc())
        )
        attachments = list(att_r.scalars().all())

        return conversations, attachments

    async def add_internal_note(
        self,
        ticket_id: str,
        team_ids: list[str],
        author_id: str,
        content: str,
    ) -> "Conversation":
        """
        Add an internal-only note to a ticket.
        Ticket must belong to any of the TL's product teams.
        is_internal is always forced True here.
        """
        from src.data.models.postgres.models import Conversation

        ticket = await self.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise ValueError(f"Ticket {ticket_id} not found in your team.")

        note = Conversation(
            ticket_id=uuid.UUID(ticket_id),
            author_id=uuid.UUID(author_id),
            author_type="team_lead",
            content=content,
            is_internal=True,
        )
        self._s.add(note)
        await self._s.flush()
        return note