"""
TicketRepository + NotificationRepository
src/data/repositories/ticket_repository.py
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Notification, Ticket


class TicketRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, ticket: Ticket) -> Ticket:
        self._s.add(ticket)
        await self._s.flush()
        await self._s.refresh(ticket)
        return ticket

    async def get_by_id(self, ticket_id: str) -> Optional[Ticket]:
        r = await self._s.execute(
            select(Ticket).where(Ticket.id == uuid.UUID(ticket_id))
        )
        return r.scalar_one_or_none()

    async def update_fields(self, ticket_id: str, fields: dict[str, Any]) -> None:
        await self._s.execute(
            update(Ticket)
            .where(Ticket.id == uuid.UUID(ticket_id))
            .values(**fields)
        )
        await self._s.flush()

    # ── Customer — my tickets ─────────────────────────────────────────────────

    async def get_by_customer(
        self,
        customer_id: str,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Ticket]:
        """All tickets belonging to this customer, newest first.

        Optionally filter by status. Supports pagination via limit/offset.
        Never exposes tickets from other customers — customer_id is always
        enforced from the JWT, never from the request body.
        """
        q = (
            select(Ticket)
            .where(Ticket.customer_id == uuid.UUID(customer_id))
        )
        if status:
            q = q.where(Ticket.status == status)
        q = q.order_by(Ticket.created_at.desc()).limit(limit).offset(offset)
        r = await self._s.execute(q)
        return list(r.scalars().all())

    async def get_by_id_and_customer(
        self,
        ticket_id: str,
        customer_id: str,
    ) -> Optional[Ticket]:
        """Fetch a single ticket — only if it belongs to this customer.

        Returns None (→ 404) if the ticket exists but belongs to someone else.
        This prevents enumeration attacks where a customer guesses ticket UUIDs.
        """
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.id == uuid.UUID(ticket_id),
                Ticket.customer_id == uuid.UUID(customer_id),
            )
        )
        return r.scalar_one_or_none()

    # ── Agent queue ───────────────────────────────────────────────────────────

    async def get_agent_queue(
        self,
        agent_id: str,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> list[Ticket]:
        statuses = statuses or ["new", "open", "in_progress", "on_hold"]
        r = await self._s.execute(
            select(Ticket)
            .where(
                Ticket.assigned_to == uuid.UUID(agent_id),
                Ticket.status.in_(statuses),
            )
            .order_by(Ticket.priority.asc(), Ticket.created_at.asc())
            .limit(limit)
        )
        return list(r.scalars().all())
    
    async def get_by_id_and_agent(
    self,
    ticket_id: str,
    agent_id: str,
) -> Optional[Ticket]:
        """Fetch a single ticket — only if it is assigned to this agent.

        Returns None (→ 404) if the ticket exists but belongs to someone else.
        Mirrors get_by_id_and_customer to prevent cross-agent enumeration.
        """
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.id == uuid.UUID(ticket_id),
                Ticket.assigned_to == uuid.UUID(agent_id),
            )
        )
        return r.scalar_one_or_none()


    # ── Team lead queue — unassigned tickets for team ─────────────────────────

    async def get_team_lead_queue(
        self,
        team_id: str,
        statuses: list[str] | None = None,
        limit: int = 100,
    ) -> list[Ticket]:
        statuses = statuses or ["new", "open"]
        r = await self._s.execute(
            select(Ticket)
            .where(
                Ticket.team_id == uuid.UUID(team_id),
                Ticket.assigned_to.is_(None),
                Ticket.status.in_(statuses),
            )
            .order_by(Ticket.priority.asc(), Ticket.created_at.asc())
            .limit(limit)
        )
        return list(r.scalars().all())

    # ── SLA helpers ───────────────────────────────────────────────────────────

    async def get_open_unmonitored(self, limit: int = 500) -> list[Ticket]:
        r = await self._s.execute(
            select(Ticket)
            .where(Ticket.status.in_(["new", "open", "in_progress"]))
            .limit(limit)
        )
        return list(r.scalars().all())

    async def get_resolved_past_sla(self) -> list[Ticket]:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
        r = await self._s.execute(
            select(Ticket).where(
                Ticket.status == "resolved",
                Ticket.resolved_at <= cutoff,
            )
        )
        return list(r.scalars().all())

    # ── Ticket number generator ───────────────────────────────────────────────

    async def ticket_number(self) -> str:
        r = await self._s.execute(select(func.count()).select_from(Ticket))
        count = r.scalar() or 0
        return f"TKT-{count + 1:06d}"


class NotificationRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, notif: Notification) -> Notification:
        self._s.add(notif)
        await self._s.flush()
        return notif

    async def get_for_user(
        self,
        recipient_id: str,
        unread_only: bool = False,
        limit: int = 50,
    ) -> list[Notification]:
        q = select(Notification).where(
            Notification.recipient_id == uuid.UUID(recipient_id)
        )
        if unread_only:
            q = q.where(Notification.is_read.is_(False))
        q = q.order_by(Notification.created_at.desc()).limit(limit)
        r = await self._s.execute(q)
        return list(r.scalars().all())

    async def mark_read(self, notif_id: str, recipient_id: str) -> bool:
        r = await self._s.execute(
            select(Notification).where(
                Notification.id == uuid.UUID(notif_id),
                Notification.recipient_id == uuid.UUID(recipient_id),
            )
        )
        notif = r.scalar_one_or_none()
        if not notif:
            return False
        notif.is_read = True
        notif.read_at = datetime.now(timezone.utc)
        await self._s.flush()
        return True