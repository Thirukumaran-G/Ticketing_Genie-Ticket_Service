from __future__ import annotations

import uuid
from typing import Optional, List

from sqlalchemy import Table, Column, String, MetaData
from sqlalchemy import select, update
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import (
    EmailConfig,
    KeywordRule,
    ProductConfig,
    SeverityPriorityMap,
    SLARule,
    Team,
    TeamMember,
)

# ── auth.tier reflection — same DB, different schema ─────────────────────────
_auth_tier = Table(
    "tier",
    MetaData(schema="auth"),
    Column("id",   PG_UUID(as_uuid=True)),
    Column("name", String),
)


# ── Email Config ──────────────────────────────────────────────────────────────

class EmailConfigRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> List[EmailConfig]:
        result = await self._session.execute(
            select(EmailConfig).where(EmailConfig.is_active == True).order_by(EmailConfig.key)
        )
        return list(result.scalars().all())

    async def get_by_key(self, key: str) -> Optional[EmailConfig]:
        result = await self._session.execute(
            select(EmailConfig).where(EmailConfig.key == key)
        )
        return result.scalar_one_or_none()

    async def upsert(
        self, key: str, value: str, is_secret: bool, updated_by: str
    ) -> EmailConfig:
        existing = await self.get_by_key(key)
        if existing:
            existing.value      = value
            existing.is_secret  = is_secret
            existing.is_active  = True
            existing.updated_by = uuid.UUID(updated_by)
            await self._session.flush()
            await self._session.refresh(existing)
            return existing
        config = EmailConfig(
            key=key,
            value=value,
            is_secret=is_secret,
            is_active=True,
            updated_by=uuid.UUID(updated_by),
        )
        self._session.add(config)
        await self._session.flush()
        await self._session.refresh(config)
        return config


# ── SLA Rule ──────────────────────────────────────────────────────────────────

class SLARuleRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active(self) -> List[SLARule]:
        result = await self._session.execute(
            select(SLARule)
            .where(SLARule.is_active == True)
            .order_by(SLARule.priority)
        )
        return list(result.scalars().all())

    async def get_by_id(self, rule_id: str) -> Optional[SLARule]:
        result = await self._session.execute(
            select(SLARule).where(SLARule.id == uuid.UUID(rule_id))
        )
        return result.scalar_one_or_none()

    async def get_by_tier_and_priority(self, tier_id: str, priority: str) -> Optional[SLARule]:
        tier_uuid = uuid.UUID(str(tier_id))
        result = await self._session.execute(
            select(SLARule).where(
                SLARule.tier_id  == tier_uuid,
                SLARule.priority == priority,
            )
        )
        return result.scalar_one_or_none()

    async def deactivate(self, rule_id: str) -> None:
        await self._session.execute(
            update(SLARule)
            .where(SLARule.id == uuid.UUID(rule_id))
            .values(is_active=False)
        )
        await self._session.flush()


# ── Severity Priority Map ─────────────────────────────────────────────────────

class SeverityPriorityMapRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> List[SeverityPriorityMap]:
        result = await self._session.execute(
            select(SeverityPriorityMap).order_by(SeverityPriorityMap.severity)
        )
        return list(result.scalars().all())

    async def get_by_id(self, map_id: str) -> Optional[SeverityPriorityMap]:
        result = await self._session.execute(
            select(SeverityPriorityMap).where(
                SeverityPriorityMap.id == uuid.UUID(map_id)
            )
        )
        return result.scalar_one_or_none()

    async def get_by_severity_and_tier(self, severity: str, tier_id: str) -> Optional[SeverityPriorityMap]:
        tier_uuid = uuid.UUID(str(tier_id))
        result = await self._session.execute(
            select(SeverityPriorityMap).where(
                SeverityPriorityMap.severity == severity,
                SeverityPriorityMap.tier_id  == tier_uuid,
            )
        )
        return result.scalar_one_or_none()

    async def delete(self, map_id: str) -> None:
        mapping = await self.get_by_id(map_id)
        if mapping:
            await self._session.delete(mapping)
            await self._session.flush()


# ── Keyword Rule ──────────────────────────────────────────────────────────────

class KeywordRuleRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active(self) -> List[KeywordRule]:
        result = await self._session.execute(
            select(KeywordRule)
            .where(KeywordRule.is_active == True)
            .order_by(KeywordRule.keyword)
        )
        return list(result.scalars().all())

    async def get_by_id(self, rule_id: str) -> Optional[KeywordRule]:
        result = await self._session.execute(
            select(KeywordRule).where(KeywordRule.id == uuid.UUID(rule_id))
        )
        return result.scalar_one_or_none()

    async def get_by_keyword(self, keyword: str) -> Optional[KeywordRule]:
        result = await self._session.execute(
            select(KeywordRule).where(KeywordRule.keyword == keyword)
        )
        return result.scalar_one_or_none()

    async def deactivate(self, rule_id: str) -> None:
        await self._session.execute(
            update(KeywordRule)
            .where(KeywordRule.id == uuid.UUID(rule_id))
            .values(is_active=False)
        )
        await self._session.flush()


# ── Product Config ────────────────────────────────────────────────────────────

class ProductConfigRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active(self) -> List[ProductConfig]:
        result = await self._session.execute(
            select(ProductConfig).where(ProductConfig.is_active == True)
        )
        return list(result.scalars().all())

    async def get_by_product_id(self, product_id: str) -> Optional[ProductConfig]:
        result = await self._session.execute(
            select(ProductConfig).where(
                ProductConfig.product_id == uuid.UUID(product_id)
            )
        )
        return result.scalar_one_or_none()

    async def deactivate_by_product_id(self, product_id: str) -> None:
        await self._session.execute(
            update(ProductConfig)
            .where(ProductConfig.product_id == uuid.UUID(product_id))
            .values(is_active=False)
        )
        await self._session.flush()


# ── Team ──────────────────────────────────────────────────────────────────────

class TeamRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active(self) -> List[Team]:
        result = await self._session.execute(
            select(Team).where(Team.is_active == True).order_by(Team.name)
        )
        return list(result.scalars().all())

    async def get_by_id(self, team_id: str) -> Optional[Team]:
        result = await self._session.execute(
            select(Team).where(Team.id == uuid.UUID(team_id))
        )
        return result.scalar_one_or_none()

    async def list_by_product(self, product_id: str) -> List[Team]:
        result = await self._session.execute(
            select(Team).where(
                Team.product_id == uuid.UUID(product_id),
                Team.is_active  == True,
            )
        )
        return list(result.scalars().all())

    async def deactivate(self, team_id: str) -> None:
        await self._session.execute(
            update(Team)
            .where(Team.id == uuid.UUID(team_id))
            .values(is_active=False)
        )
        await self._session.flush()


# ── Team Member ───────────────────────────────────────────────────────────────

class TeamMemberRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, member_id: str) -> Optional[TeamMember]:
        result = await self._session.execute(
            select(TeamMember).where(TeamMember.id == uuid.UUID(member_id))
        )
        return result.scalar_one_or_none()

    async def deactivate(self, member_id: str) -> None:
        await self._session.execute(
            update(TeamMember)
            .where(TeamMember.id == uuid.UUID(member_id))
            .values(is_active=False)
        )
        await self._session.flush()


# ── Tier Repository — queries auth.tier directly (same DB, auth schema) ───────

class TierRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_id_by_name(self, tier_name: str) -> str | None:
        """
        Returns tier UUID as plain str.
        Queries auth.tier via reflected table — no ORM model import needed.
        Returns None if tier_name not found.
        """
        from sqlalchemy import func

        result = await self._session.execute(
            sa_select(_auth_tier.c.id).where(
                func.lower(_auth_tier.c.name) == tier_name.lower().strip()
            )
        )
        row = result.scalar_one_or_none()
        return str(row) if row is not None else None