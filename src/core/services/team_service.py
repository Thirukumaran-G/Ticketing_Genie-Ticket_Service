from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import NotFoundException
from src.core.services.audit_service import audit_service
from src.data.models.postgres.models import Team, TeamMember
from src.data.repositories.admin_repository import TeamMemberRepository, TeamRepository
from src.observability.logging.logger import get_logger
from src.schemas.admin_schema import (
    TeamCreateRequest,
    TeamMemberAddRequest,
    TeamMemberResponse,
    TeamResponse,
)

logger = get_logger(__name__)


class TeamService:

    def __init__(self, session: AsyncSession) -> None:
        self._session     = session
        self._team_repo   = TeamRepository(session)
        self._member_repo = TeamMemberRepository(session)

    async def list_teams(self) -> list[Team]:
        return await self._team_repo.list_active()

    async def list_teams_by_product(self, product_id: str) -> list[Team]:
        return await self._team_repo.list_by_product(product_id)

    async def create_team(
        self, payload: TeamCreateRequest, actor_id: str
    ) -> Team:
        team = Team(
            name=payload.name,
            product_id=payload.product_id,
            team_lead_id=payload.team_lead_id,
            is_active=True,
        )
        self._session.add(team)
        await self._session.flush()
        await self._session.refresh(team)
        await audit_service.log(
            entity_type="team",
            entity_id=team.id,
            action="team_created",
            actor_id=uuid.UUID(actor_id),
            actor_type="user",
            new_value={"name": payload.name, "product_id": str(payload.product_id)},
        )
        await self._session.commit()
        logger.info("team_created", team_id=str(team.id), actor_id=actor_id)
        return team

    async def deactivate_team(self, team_id: str, actor_id: str) -> None:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")
        await self._team_repo.deactivate(team_id)
        await audit_service.log(
            entity_type="team",
            entity_id=team.id,
            action="team_deactivated",
            actor_id=uuid.UUID(actor_id),
            actor_type="user",
            old_value={"is_active": True},
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info("team_deactivated", team_id=team_id, actor_id=actor_id)

    async def add_member(
        self, team_id: str, payload: TeamMemberAddRequest, actor_id: str
    ) -> TeamMember:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")

        skills_dict: Optional[dict] = None
        embedding:   Optional[list] = None

        if payload.skill_text:
            skills_dict = {"skill_text": payload.skill_text}
            try:
                from src.core.services.embedding_service import embed_text
                embedding = await embed_text(payload.skill_text)
            except Exception as exc:
                logger.warning(
                    "skill_embedding_failed",
                    team_id=team_id,
                    error=str(exc),
                )

        member = TeamMember(
            team_id=uuid.UUID(team_id),
            user_id=payload.user_id,
            skills=skills_dict,
            skill_embedding=embedding,
            experience=payload.experience,
            is_active=True,
        )
        self._session.add(member)
        await self._session.flush()
        await self._session.refresh(member)
        await audit_service.log(
            entity_type="team_member",
            entity_id=member.id,
            action="team_member_added",
            actor_id=uuid.UUID(actor_id),
            actor_type="user",
            new_value={"team_id": team_id, "user_id": str(payload.user_id)},
        )
        await self._session.commit()
        logger.info(
            "team_member_added",
            member_id=str(member.id),
            team_id=team_id,
            actor_id=actor_id,
        )
        return member

    async def remove_member(self, member_id: str, actor_id: str) -> None:
        member = await self._member_repo.get_by_id(member_id)
        if not member:
            raise NotFoundException(f"Team member {member_id} not found.")
        await self._member_repo.deactivate(member_id)
        await audit_service.log(
            entity_type="team_member",
            entity_id=member.id,
            action="team_member_removed",
            actor_id=uuid.UUID(actor_id),
            actor_type="user",
            old_value={"is_active": True},
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info("team_member_removed", member_id=member_id, actor_id=actor_id)