"""
TeamService — create teams, add/remove members with skill embedding.
src/core/services/team_service.py
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import ConflictException, NotFoundException
from src.core.services.audit_service import audit_service
from src.core.services.embed_service import EmbedService
from src.data.models.postgres.models import Team, TeamMember
from src.data.repositories.team_repository import TeamMemberRepository, TeamRepository
from src.observability.logging.logger import get_logger
from src.schemas.admin_schema import TeamCreateRequest, TeamMemberAddRequest

logger = get_logger(__name__)

_embed_service = EmbedService()


class TeamService:

    def __init__(self, session: AsyncSession) -> None:
        self._session     = session
        self._team_repo   = TeamRepository(session)
        self._member_repo = TeamMemberRepository(session)

    # ── Teams ─────────────────────────────────────────────────────────────────

    async def list_teams(self) -> list[Team]:
        return await self._team_repo.list_active()

    async def list_teams_by_product(self, product_id: str) -> list[Team]:
        return await self._team_repo.list_by_product(product_id)

    async def list_members(self, team_id: str) -> list[TeamMember]:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")
        return await self._member_repo.list_by_team(team_id)

    async def create_team(
        self,
        payload: TeamCreateRequest,
        admin_id: str,
    ) -> Team:
        try:
            team = Team(
                name=payload.name,
                product_id=payload.product_id,
                team_lead_id=payload.team_lead_id,
            )
            self._session.add(team)
            await self._session.flush()
            await self._session.refresh(team)
        except Exception as exc:
            logger.error(
                "team_create_failed",
                name=payload.name,
                admin_id=admin_id,
                error=str(exc),
            )
            raise

        await audit_service.log(
            entity_type="team",
            entity_id=team.id,
            action="team_created",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            new_value={
                "name":         payload.name,
                "product_id":   str(payload.product_id),
                "team_lead_id": str(payload.team_lead_id) if payload.team_lead_id else None,
            },
        )
        await self._session.commit()
        logger.info("team_created", team_id=str(team.id), name=payload.name, admin_id=admin_id)
        return team

    async def deactivate_team(self, team_id: str, admin_id: str) -> None:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")
        try:
            await self._team_repo.deactivate(team_id)
        except Exception as exc:
            logger.error(
                "team_deactivate_failed",
                team_id=team_id,
                admin_id=admin_id,
                error=str(exc),
            )
            raise
        await audit_service.log(
            entity_type="team",
            entity_id=team.id,
            action="team_deactivated",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            old_value={"is_active": True},
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info("team_deactivated", team_id=team_id, admin_id=admin_id)

    # ── Team Members ──────────────────────────────────────────────────────────

    async def add_member(
        self,
        team_id: str,
        payload: TeamMemberAddRequest,
        admin_id: str,
    ) -> TeamMember:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")

        existing = await self._member_repo.get_by_team_and_user(
            team_id, str(payload.user_id)
        )
        if existing and existing.is_active:
            raise ConflictException(
                f"User {payload.user_id} is already a member of this team."
            )

        skill_embedding: list[float] | None = None
        if payload.skill_text:
            try:
                skill_embedding = await _embed_service.embed(payload.skill_text)
            except Exception as exc:
                logger.warning(
                    "skill_embedding_failed",
                    user_id=str(payload.user_id),
                    error=str(exc),
                )

        try:
            member = TeamMember(
                team_id=uuid.UUID(team_id),
                user_id=payload.user_id,
                experience=payload.experience,
                skills={"skill_text": payload.skill_text} if payload.skill_text else None,
                skill_embedding=skill_embedding,
            )
            self._session.add(member)
            await self._session.flush()
            await self._session.refresh(member)
        except Exception as exc:
            logger.error(
                "team_member_add_failed",
                team_id=team_id,
                user_id=str(payload.user_id),
                admin_id=admin_id,
                error=str(exc),
            )
            raise

        await audit_service.log(
            entity_type="team_member",
            entity_id=member.id,
            action="team_member_added",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            new_value={
                "team_id":    team_id,
                "user_id":    str(payload.user_id),
                "experience": payload.experience,
                "has_skills": payload.skill_text is not None,
            },
        )
        await self._session.commit()
        logger.info(
            "team_member_added",
            member_id=str(member.id),
            team_id=team_id,
            user_id=str(payload.user_id),
            admin_id=admin_id,
        )
        return member

    async def remove_member(self, team_id: str, member_id: str, admin_id: str) -> None:
        member = await self._member_repo.get_by_id(member_id)
        if not member:
            raise NotFoundException(f"Team member {member_id} not found.")
        # Verify the member actually belongs to the given team
        if str(member.team_id) != team_id:
            raise NotFoundException(f"Member {member_id} does not belong to team {team_id}.")
        try:
            await self._member_repo.deactivate(member_id)
        except Exception as exc:
            logger.error(
                "team_member_remove_failed",
                member_id=member_id,
                team_id=team_id,
                admin_id=admin_id,
                error=str(exc),
            )
            raise
        await audit_service.log(
            entity_type="team_member",
            entity_id=member.id,
            action="team_member_removed",
            actor_id=uuid.UUID(admin_id),
            actor_type="user",
            old_value={
                "team_id": team_id,
                "user_id": str(member.user_id),
                "is_active": True,
            },
            new_value={"is_active": False},
        )
        await self._session.commit()
        logger.info(
            "team_member_removed",
            member_id=member_id,
            team_id=team_id,
            admin_id=admin_id,
        )