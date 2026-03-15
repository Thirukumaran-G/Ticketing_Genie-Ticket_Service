"""
SimilarTicketRepository — group creation, membership, TL confirmation.
src/data/repositories/similar_ticket_repository.py
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.data.models.postgres.models import (
    SimilarTicketGroup,
    SimilarTicketGroupMember,
    Ticket,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class SimilarTicketRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ── Group CRUD ─────────────────────────────────────────────────────────────

    async def create_group(
        self,
        name: Optional[str] = None,
    ) -> SimilarTicketGroup:
        group = SimilarTicketGroup(
            id=uuid.uuid4(),
            name=name,
            confirmed_by_lead=False,
        )
        self._session.add(group)
        await self._session.flush()
        logger.info("similar_group_created", group_id=str(group.id))
        return group

    async def get_group_by_id(
        self,
        group_id: str,
    ) -> Optional[SimilarTicketGroup]:
        result = await self._session.execute(
            select(SimilarTicketGroup)
            .where(SimilarTicketGroup.id == uuid.UUID(group_id))
            .options(
                selectinload(SimilarTicketGroup.members)
                .selectinload(SimilarTicketGroupMember.ticket)
            )
        )
        return result.scalar_one_or_none()

    async def list_groups(
        self,
        confirmed_only: bool = False,
    ) -> list[SimilarTicketGroup]:
        q = (
            select(SimilarTicketGroup)
            .options(
                selectinload(SimilarTicketGroup.members)
                .selectinload(SimilarTicketGroupMember.ticket)
            )
            .order_by(SimilarTicketGroup.created_at.desc())
        )
        if confirmed_only:
            q = q.where(SimilarTicketGroup.confirmed_by_lead.is_(True))
        result = await self._session.execute(q)
        return list(result.scalars().all())

    async def confirm_group(
        self,
        group_id:     str,
        lead_user_id: str,
        name:         Optional[str] = None,
    ) -> Optional[SimilarTicketGroup]:
        group = await self.get_group_by_id(group_id)
        if not group:
            return None
        group.confirmed_by_lead = True
        group.confirmed_at      = datetime.now(timezone.utc)
        group.confirmed_by      = uuid.UUID(lead_user_id)
        if name:
            group.name = name
        logger.info(
            "similar_group_confirmed",
            group_id=group_id,
            lead_user_id=lead_user_id,
        )
        return group

    async def update_group_name(
        self,
        group_id: str,
        name:     str,
    ) -> Optional[SimilarTicketGroup]:
        group = await self.get_group_by_id(group_id)
        if not group:
            return None
        group.name = name
        return group

    async def delete_group(self, group_id: str) -> bool:
        result = await self._session.execute(
            delete(SimilarTicketGroup).where(
                SimilarTicketGroup.id == uuid.UUID(group_id)
            )
        )
        return result.rowcount > 0

    # ── Member management ──────────────────────────────────────────────────────

    async def add_member(
        self,
        group_id:         str,
        ticket_id:        str,
        similarity_score: float,
    ) -> SimilarTicketGroupMember:
        """
        Add a ticket to a group. Silently returns existing member if already present.
        """
        existing = await self._session.execute(
            select(SimilarTicketGroupMember).where(
                SimilarTicketGroupMember.group_id  == uuid.UUID(group_id),
                SimilarTicketGroupMember.ticket_id == uuid.UUID(ticket_id),
            )
        )
        member = existing.scalar_one_or_none()
        if member:
            # Update score if new match is stronger
            if similarity_score > member.similarity_score:
                member.similarity_score = similarity_score
            return member

        member = SimilarTicketGroupMember(
            id=uuid.uuid4(),
            group_id=uuid.UUID(group_id),
            ticket_id=uuid.UUID(ticket_id),
            similarity_score=similarity_score,
        )
        self._session.add(member)
        await self._session.flush()
        logger.info(
            "similar_group_member_added",
            group_id=group_id,
            ticket_id=ticket_id,
            score=round(similarity_score, 3),
        )
        return member

    async def remove_member(
        self,
        group_id:  str,
        ticket_id: str,
    ) -> bool:
        result = await self._session.execute(
            delete(SimilarTicketGroupMember).where(
                SimilarTicketGroupMember.group_id  == uuid.UUID(group_id),
                SimilarTicketGroupMember.ticket_id == uuid.UUID(ticket_id),
            )
        )
        return result.rowcount > 0

    async def get_groups_for_ticket(
        self,
        ticket_id: str,
    ) -> list[SimilarTicketGroup]:
        """Return all groups that contain this ticket."""
        result = await self._session.execute(
            select(SimilarTicketGroup)
            .join(
                SimilarTicketGroupMember,
                SimilarTicketGroupMember.group_id == SimilarTicketGroup.id,
            )
            .where(
                SimilarTicketGroupMember.ticket_id == uuid.UUID(ticket_id)
            )
            .options(
                selectinload(SimilarTicketGroup.members)
                .selectinload(SimilarTicketGroupMember.ticket)
            )
        )
        return list(result.scalars().unique().all())

    # ── pgvector similarity query ──────────────────────────────────────────────

    async def find_similar_tickets(
        self,
        ticket_id:  str,
        embedding:  list[float],
        threshold:  float = 0.82,
        limit:      int   = 20,
    ) -> list[tuple[Ticket, float]]:
        """
        Find all open tickets whose embedding cosine-similarity to the given
        embedding exceeds threshold. Excludes the source ticket itself and
        closed/resolved tickets older than 30 days.

        Returns list of (Ticket, score) tuples sorted by score descending.
        Requires pgvector extension and the <=> operator.
        """
        from sqlalchemy import text
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        # pgvector cosine distance: 1 - (a <=> b) = cosine similarity
        # We select tickets where cosine_similarity >= threshold
        # cosine distance = 1 - similarity  →  distance <= 1 - threshold
        distance_threshold = 1.0 - threshold

        sql = text("""
            SELECT
                t.id,
                1 - (t.ticket_embedding <=> CAST(:embedding AS vector)) AS similarity
            FROM ticket.ticket t
            WHERE
                t.id != CAST(:ticket_id AS uuid)
                AND t.ticket_embedding IS NOT NULL
                AND t.status NOT IN ('closed', 'resolved')
                AND t.created_at >= :cutoff
                AND (t.ticket_embedding <=> CAST(:embedding AS vector)) <= :distance_threshold
            ORDER BY t.ticket_embedding <=> CAST(:embedding AS vector) ASC
            LIMIT :limit
        """)

        import json as _json
        result = await self._session.execute(
            sql,
            {
                "embedding":          _json.dumps(embedding),
                "ticket_id":          ticket_id,
                "cutoff":             cutoff,
                "distance_threshold": distance_threshold,
                "limit":              limit,
            },
        )
        rows = result.fetchall()

        if not rows:
            return []

        # Bulk fetch Ticket objects
        ticket_ids = [row.id for row in rows]
        score_map  = {row.id: float(row.similarity) for row in rows}

        tickets_result = await self._session.execute(
            select(Ticket).where(Ticket.id.in_(ticket_ids))
        )
        tickets = tickets_result.scalars().all()

        return sorted(
            [(t, score_map[t.id]) for t in tickets],
            key=lambda x: x[1],
            reverse=True,
        )