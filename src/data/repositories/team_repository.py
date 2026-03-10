"""
TeamRepository and TeamMemberRepository.
Add to src/data/repositories/team_repository.py
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.data.models.postgres.models import Team, TeamMember


class TeamRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active(self) -> list[Team]:
        result = await self._session.execute(
            select(Team)
            .where(Team.is_active.is_(True))
            .options(selectinload(Team.members))
        )
        return list(result.scalars().all())

    async def get_by_id(self, team_id: str) -> Optional[Team]:
        result = await self._session.execute(
            select(Team)
            .where(Team.id == uuid.UUID(team_id))
            .options(selectinload(Team.members))
        )
        return result.scalar_one_or_none()

    async def deactivate(self, team_id: str) -> None:
        await self._session.execute(
            update(Team)
            .where(Team.id == uuid.UUID(team_id))
            .values(is_active=False)
        )
        await self._session.flush()

    async def list_by_product(self, product_id: str) -> list[Team]:
        result = await self._session.execute(
            select(Team)
            .where(
                Team.product_id == uuid.UUID(product_id),
                Team.is_active.is_(True),
            )
            .options(selectinload(Team.members))
        )
        return list(result.scalars().all())


class TeamMemberRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, member_id: str) -> Optional[TeamMember]:
        result = await self._session.execute(
            select(TeamMember).where(TeamMember.id == uuid.UUID(member_id))
        )
        return result.scalar_one_or_none()

    async def get_by_team_and_user(
        self, team_id: str, user_id: str
    ) -> Optional[TeamMember]:
        result = await self._session.execute(
            select(TeamMember).where(
                TeamMember.team_id == uuid.UUID(team_id),
                TeamMember.user_id == uuid.UUID(user_id),
            )
        )
        return result.scalar_one_or_none()

    async def deactivate(self, member_id: str) -> None:
        await self._session.execute(
            update(TeamMember)
            .where(TeamMember.id == uuid.UUID(member_id))
            .values(is_active=False)
        )
        await self._session.flush()

    async def list_by_team(self, team_id: str) -> list[TeamMember]:
        result = await self._session.execute(
            select(TeamMember).where(
                TeamMember.team_id == uuid.UUID(team_id),
                TeamMember.is_active.is_(True),
            )
        )
        return list(result.scalars().all())