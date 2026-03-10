"""
ticket-service: src/data/repositories/admin_repository.py
Full replacement file.

Key changes vs original:
  - Removed get_rule_by_tier_name()        (imported Tier from wrong service)
  - Removed get_by_severity_and_tier_name() (imported Tier from wrong service)
  - Added    get_rule() alias on SLARuleRepository (sla_worker called this name)
  Workers now resolve tier_name → tier_id via HTTP (celery/utils.fetch_tier_id)
  before calling the existing get_by_tier_and_priority / get_by_severity_and_tier.
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import (
    EmailConfig,
    KeywordRule,
    ProductConfig,
    SeverityPriorityMap,
    SLARule,
)


# ── Email Config ──────────────────────────────────────────────────────────────

class EmailConfigRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> list[EmailConfig]:
        result = await self._session.execute(
            select(EmailConfig).where(EmailConfig.is_active.is_(True))
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
            existing.updated_by = uuid.UUID(updated_by)
            await self._session.flush()
            await self._session.refresh(existing)
            return existing

        config = EmailConfig(
            key=key,
            value=value,
            is_secret=is_secret,
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

    async def list_active(self) -> list[SLARule]:
        result = await self._session.execute(
            select(SLARule).where(SLARule.is_active.is_(True))
        )
        return list(result.scalars().all())

    async def get_by_tier_and_priority(
        self, tier_id: str, priority: str
    ) -> Optional[SLARule]:
        """Primary lookup — called after tier_id is resolved via HTTP."""
        result = await self._session.execute(
            select(SLARule).where(
                SLARule.tier_id == uuid.UUID(tier_id),
                SLARule.priority == priority,
                SLARule.is_active.is_(True),
            )
        )
        return result.scalar_one_or_none()

    async def get_rule(self, tier_id: str, priority: str) -> Optional[SLARule]:
        """
        Alias for get_by_tier_and_priority.
        sla_worker calls this name — keeping both so either works.
        NOTE: tier_id must already be a resolved UUID string, not a tier name.
        """
        return await self.get_by_tier_and_priority(tier_id, priority)

    async def get_by_id(self, rule_id: str) -> Optional[SLARule]:
        result = await self._session.execute(
            select(SLARule).where(SLARule.id == uuid.UUID(rule_id))
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

    async def list_all(self) -> list[SeverityPriorityMap]:
        result = await self._session.execute(select(SeverityPriorityMap))
        return list(result.scalars().all())

    async def get_by_severity_and_tier(
        self, severity: str, tier_id: str
    ) -> Optional[SeverityPriorityMap]:
        """Primary lookup — called after tier_id is resolved via HTTP."""
        result = await self._session.execute(
            select(SeverityPriorityMap).where(
                SeverityPriorityMap.severity == severity,
                SeverityPriorityMap.tier_id == uuid.UUID(tier_id),
            )
        )
        return result.scalar_one_or_none()

    async def get_by_id(self, map_id: str) -> Optional[SeverityPriorityMap]:
        result = await self._session.execute(
            select(SeverityPriorityMap).where(
                SeverityPriorityMap.id == uuid.UUID(map_id)
            )
        )
        return result.scalar_one_or_none()

    async def delete(self, map_id: str) -> None:
        obj = await self.get_by_id(map_id)
        if obj:
            await self._session.delete(obj)
            await self._session.flush()


# ── Keyword Rule ──────────────────────────────────────────────────────────────

class KeywordRuleRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_active(self) -> list[KeywordRule]:
        result = await self._session.execute(
            select(KeywordRule).where(KeywordRule.is_active.is_(True))
        )
        return list(result.scalars().all())

    async def get_by_keyword(self, keyword: str) -> Optional[KeywordRule]:
        result = await self._session.execute(
            select(KeywordRule).where(KeywordRule.keyword == keyword)
        )
        return result.scalar_one_or_none()

    async def get_by_id(self, rule_id: str) -> Optional[KeywordRule]:
        result = await self._session.execute(
            select(KeywordRule).where(KeywordRule.id == uuid.UUID(rule_id))
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

    async def list_active(self) -> list[ProductConfig]:
        result = await self._session.execute(
            select(ProductConfig).where(ProductConfig.is_active.is_(True))
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