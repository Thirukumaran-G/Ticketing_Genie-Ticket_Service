from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import ConflictException, ForbiddenException, NotFoundException
from src.core.services.audit_service import audit_service
from src.core.services.embed_service import EmbedService
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

_embed_service = EmbedService()


class TeamService:

    def __init__(self, session: AsyncSession) -> None:
        self._session     = session
        self._team_repo   = TeamRepository(session)
        self._member_repo = TeamMemberRepository(session)

    # ── Run once at startup or deploy to ensure the unique index exists ────────
    # No migration file needed — IF NOT EXISTS makes it idempotent.
    async def ensure_indexes(self) -> None:
        await self._session.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_team_member_team_user
            ON ticket.team_member (team_id, user_id)
        """))
        await self._session.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_team_name_product
            ON ticket.team (name, product_id)
            WHERE is_active = TRUE
        """))
        await self._session.commit()

    async def list_teams(self) -> list[Team]:
        return await self._team_repo.list_active()

    async def list_teams_by_product(self, product_id: str) -> list[Team]:
        return await self._team_repo.list_by_product(product_id)

    async def create_team(self, payload: TeamCreateRequest, actor_id: str) -> Team:
        # Enforce one lead per team globally
        if payload.team_lead_id:
            result = await self._session.execute(
                select(Team).where(
                    Team.team_lead_id == payload.team_lead_id,
                    Team.is_active    == True,
                )
            )
            if result.scalar_one_or_none():
                raise ConflictException(
                    "This team lead is already assigned to another active team. "
                    "A lead can only lead one team at a time."
                )

        try:
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
        except IntegrityError as exc:
            await self._session.rollback()
            logger.warning("team_create_integrity_error", detail=str(exc.orig))
            raise ConflictException(
                "A team with this name already exists for this product."
            ) from exc
        except SQLAlchemyError as exc:
            await self._session.rollback()
            logger.error("team_create_db_error", detail=str(exc))
            raise

    async def deactivate_team(self, team_id: str, actor_id: str) -> None:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")
        if not team.is_active:
            # Already inactive — treat as no-op, don't crash
            logger.info("team_already_inactive", team_id=team_id)
            return
        try:
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
        except SQLAlchemyError as exc:
            await self._session.rollback()
            logger.error("team_deactivate_db_error", detail=str(exc))
            raise

    async def remove_lead(self, team_id: str, actor_id: str) -> Team:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")
        if not team.team_lead_id:
            raise NotFoundException("This team has no lead assigned.")
        try:
            old_lead_id = team.team_lead_id
            team.team_lead_id = None
            await self._session.flush()
            await self._session.refresh(team)
            await audit_service.log(
                entity_type="team",
                entity_id=team.id,
                action="team_lead_removed",
                actor_id=uuid.UUID(actor_id),
                actor_type="user",
                old_value={"team_lead_id": str(old_lead_id)},
                new_value={"team_lead_id": None},
            )
            await self._session.commit()
            logger.info("team_lead_removed", team_id=team_id, actor_id=actor_id)
            return team
        except SQLAlchemyError as exc:
            await self._session.rollback()
            logger.error("team_lead_remove_db_error", detail=str(exc))
            raise

    async def add_member(
        self, team_id: str, payload: TeamMemberAddRequest, actor_id: str
    ) -> TeamMember:
        team = await self._team_repo.get_by_id(team_id)
        if not team:
            raise NotFoundException(f"Team {team_id} not found.")

        # Python-level duplicate check (fast path before hitting DB constraint)
        result = await self._session.execute(
            select(TeamMember).where(
                TeamMember.team_id == uuid.UUID(team_id),
                TeamMember.user_id == payload.user_id,
            )
        )
        if result.scalar_one_or_none():
            raise ConflictException(
                f"User {payload.user_id} is already a member of this team."
            )

        skills_dict: Optional[dict] = None
        embedding:   Optional[list] = None

        if payload.skill_text:
            skills_dict = {"skill_text": payload.skill_text}
            try:
                embedding = await _embed_service.embed(payload.skill_text)
                logger.info(
                    "skill_embedding_computed",
                    team_id=team_id,
                    user_id=str(payload.user_id),
                    dims=len(embedding),
                )
            except Exception as exc:
                logger.warning(
                    "skill_embedding_failed",
                    team_id=team_id,
                    user_id=str(payload.user_id),
                    error=str(exc),
                )

        try:
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
                new_value={
                    "team_id":       team_id,
                    "user_id":       str(payload.user_id),
                    "has_embedding": embedding is not None,
                    "skill_text":    payload.skill_text or "",
                },
            )
            await self._session.commit()
            logger.info(
                "team_member_added",
                member_id=str(member.id),
                team_id=team_id,
                actor_id=actor_id,
                has_embedding=embedding is not None,
            )
            return member
        except IntegrityError as exc:
            await self._session.rollback()
            logger.warning("add_member_integrity_error", detail=str(exc.orig))
            raise ConflictException(
                f"User {payload.user_id} is already a member of this team."
            ) from exc
        except SQLAlchemyError as exc:
            await self._session.rollback()
            logger.error("add_member_db_error", detail=str(exc))
            raise

    async def remove_member(self, team_id: str, member_id: str, actor_id: str) -> None:
        member = await self._member_repo.get_by_id(member_id)
        if not member:
            raise NotFoundException(f"Team member {member_id} not found.")
        if str(member.team_id) != team_id:
            raise ForbiddenException("Member does not belong to this team.")

        try:
            # If this member is the team lead, clear the lead reference too
            team = await self._team_repo.get_by_id(team_id)
            if team and team.team_lead_id and str(team.team_lead_id) == str(member.user_id):
                team.team_lead_id = None
                await self._session.flush()
                logger.info(
                    "team_lead_cleared_on_member_remove",
                    team_id=team_id,
                    user_id=str(member.user_id),
                )

            await self._member_repo.hard_delete(member_id)
            await audit_service.log(
                entity_type="team_member",
                entity_id=member.id,
                action="team_member_removed",
                actor_id=uuid.UUID(actor_id),
                actor_type="user",
                old_value={"team_id": team_id, "user_id": str(member.user_id)},
            )
            await self._session.commit()
            logger.info("team_member_hard_deleted", member_id=member_id, actor_id=actor_id)
        except SQLAlchemyError as exc:
            await self._session.rollback()
            logger.error("remove_member_db_error", detail=str(exc))
            raise