from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update, delete, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import (
    SimilarTicketGroup,
    SimilarTicketGroupMember,
    Ticket,
)


class TicketGroupRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    # ── Fetch single group with members ──────────────────────────────────────

    async def get_group_with_members(
        self, group_id: str
    ) -> Optional[SimilarTicketGroup]:
        r = await self._s.execute(
            select(SimilarTicketGroup).where(
                SimilarTicketGroup.id == uuid.UUID(group_id)
            )
        )
        group = r.scalar_one_or_none()
        if not group:
            return None
        # eagerly load members + their tickets
        await self._s.refresh(group, ["members"])
        return group

    # ── Fetch all groups containing a given ticket ────────────────────────────

    async def get_groups_for_ticket(
        self, ticket_id: str
    ) -> list[SimilarTicketGroup]:
        member_rows = await self._s.execute(
            select(SimilarTicketGroupMember.group_id).where(
                SimilarTicketGroupMember.ticket_id == uuid.UUID(ticket_id)
            )
        )
        group_ids = [row[0] for row in member_rows.fetchall()]
        if not group_ids:
            return []

        r = await self._s.execute(
            select(SimilarTicketGroup).where(
                SimilarTicketGroup.id.in_(group_ids)
            )
        )
        groups = list(r.scalars().all())
        for g in groups:
            await self._s.refresh(g, ["members"])
        return groups

    # ── List all groups (for TL groups page) ─────────────────────────────────

    async def list_all_groups(self) -> list[SimilarTicketGroup]:
        r = await self._s.execute(
            select(SimilarTicketGroup).order_by(
                SimilarTicketGroup.confirmed_by_lead.asc(),
                SimilarTicketGroup.created_at.desc(),
            )
        )
        groups = list(r.scalars().all())
        for g in groups:
            await self._s.refresh(g, ["members"])
        return groups

    # ── Confirm group ─────────────────────────────────────────────────────────

    async def confirm_group(
        self,
        group_id: str,
        name: Optional[str],
        lead_id: str,
    ) -> Optional[SimilarTicketGroup]:
        r = await self._s.execute(
            select(SimilarTicketGroup).where(
                SimilarTicketGroup.id == uuid.UUID(group_id)
            )
        )
        group = r.scalar_one_or_none()
        if not group:
            return None
        group.confirmed_by_lead = True
        group.confirmed_at = datetime.now(timezone.utc)
        group.confirmed_by = uuid.UUID(lead_id)
        if name:
            group.name = name
        await self._s.flush()
        return group

    # ── Set parent ticket on group ────────────────────────────────────────────

    async def set_parent_ticket(
        self,
        group_id: str,
        parent_ticket_id: str,
    ) -> Optional[SimilarTicketGroup]:
        r = await self._s.execute(
            select(SimilarTicketGroup).where(
                SimilarTicketGroup.id == uuid.UUID(group_id)
            )
        )
        group = r.scalar_one_or_none()
        if not group:
            return None
        group.parent_ticket_id = uuid.UUID(parent_ticket_id)
        await self._s.flush()
        return group

    # ── Remove member from group ──────────────────────────────────────────────

    async def remove_member(
        self,
        group_id: str,
        ticket_id: str,
    ) -> bool:
        r = await self._s.execute(
            select(SimilarTicketGroupMember).where(
                SimilarTicketGroupMember.group_id == uuid.UUID(group_id),
                SimilarTicketGroupMember.ticket_id == uuid.UUID(ticket_id),
            )
        )
        member = r.scalar_one_or_none()
        if not member:
            return False
        await self._s.delete(member)
        await self._s.flush()
        return True

    # ── Get all children of a parent ticket ───────────────────────────────────

    async def get_children_of_parent(
        self, parent_ticket_id: str
    ) -> list[Ticket]:
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.parent_ticket_id == uuid.UUID(parent_ticket_id)
            )
        )
        return list(r.scalars().all())

    # ── Find similar tickets via pgvector cosine similarity ───────────────────

    async def find_similar_tickets(
        self,
        embedding: list[float],
        threshold: float = 0.88,
        limit: int = 10,
        exclude_ticket_id: str | None = None,
        team_id: str | None = None,
    ) -> list[tuple[Ticket, float]]:
        """
        Returns list of (ticket, similarity_score) for open tickets
        whose embedding is within the threshold.
        Uses pgvector <=> operator (cosine distance).
        cosine_similarity = 1 - cosine_distance
        """
        from pgvector.sqlalchemy import Vector
        from sqlalchemy import cast, literal
        import json

        # Build embedding literal for pgvector
        emb_str = "[" + ",".join(str(v) for v in embedding) + "]"

        open_statuses = [
            "new", "acknowledged", "assigned",
            "in_progress", "on_hold", "reopened",
        ]

        query = (
            select(
                Ticket,
                (
                    1 - Ticket.ticket_embedding.cosine_distance(embedding)
                ).label("similarity"),
            )
            .where(
                Ticket.ticket_embedding.is_not(None),
                Ticket.status.in_(open_statuses),
                Ticket.parent_ticket_id.is_(None),  # never group children
            )
        )

        if exclude_ticket_id:
            query = query.where(
                Ticket.id != uuid.UUID(exclude_ticket_id)
            )

        if team_id:
            query = query.where(Ticket.team_id == uuid.UUID(team_id))

        query = query.order_by(
            Ticket.ticket_embedding.cosine_distance(embedding).asc()
        ).limit(limit)

        r = await self._s.execute(query)
        rows = r.fetchall()

        return [
            (row[0], float(row[1]))
            for row in rows
            if float(row[1]) >= threshold
        ]

    # ── Find existing unconfirmed group containing any of these ticket ids ────

    async def find_existing_group_for_tickets(
    self, ticket_ids: list[str]
) -> Optional[SimilarTicketGroup]:
        uuids = [uuid.UUID(tid) for tid in ticket_ids]

        member_rows = await self._s.execute(
            select(SimilarTicketGroupMember.group_id).where(
                SimilarTicketGroupMember.ticket_id.in_(uuids)
            )
        )
        group_ids = list({row[0] for row in member_rows.fetchall()})
        if not group_ids:
            return None

        # Prefer confirmed groups first
        r = await self._s.execute(
            select(SimilarTicketGroup).where(
                SimilarTicketGroup.id.in_(group_ids),
            ).order_by(
                SimilarTicketGroup.confirmed_by_lead.desc(),  # confirmed first
                SimilarTicketGroup.created_at.asc(),          # oldest first
            ).limit(1)
        )
        return r.scalar_one_or_none()

    # ── Add ticket to group ───────────────────────────────────────────────────

    async def add_member(
        self,
        group_id: str,
        ticket_id: str,
        similarity_score: float,
    ) -> SimilarTicketGroupMember:
        # Check if already a member
        r = await self._s.execute(
            select(SimilarTicketGroupMember).where(
                SimilarTicketGroupMember.group_id == uuid.UUID(group_id),
                SimilarTicketGroupMember.ticket_id == uuid.UUID(ticket_id),
            )
        )
        existing = r.scalar_one_or_none()
        if existing:
            return existing

        member = SimilarTicketGroupMember(
            group_id=uuid.UUID(group_id),
            ticket_id=uuid.UUID(ticket_id),
            similarity_score=similarity_score,
        )
        self._s.add(member)
        await self._s.flush()
        return member

    # ── Create new group with initial members ─────────────────────────────────

    async def create_group_with_members(
        self,
        ticket_id_scores: list[tuple[str, float]],
        name: Optional[str] = None,
    ) -> SimilarTicketGroup:
        group = SimilarTicketGroup(name=name)
        self._s.add(group)
        await self._s.flush()

        for tid, score in ticket_id_scores:
            member = SimilarTicketGroupMember(
                group_id=group.id,
                ticket_id=uuid.UUID(tid),
                similarity_score=score,
            )
            self._s.add(member)

        await self._s.flush()
        return group

    # ── Count members in group ────────────────────────────────────────────────

    async def get_member_count(self, group_id: str) -> int:
        r = await self._s.execute(
            select(func.count()).select_from(SimilarTicketGroupMember).where(
                SimilarTicketGroupMember.group_id == uuid.UUID(group_id)
            )
        )
        return r.scalar() or 0