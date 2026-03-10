"""
Seed script — severity → priority mappings (ticket.severity_priority_map)
Run: uv run python -m src.scripts.seeders.pri_sev_seeder
"""

from __future__ import annotations

import asyncio

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.clients.postgres_client import AsyncSessionFactory
from src.data.models.postgres.models import SeverityPriorityMap
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

PRI_SEV_DATA: list[dict] = [
    {"severity": "critical", "tier": "starter",    "derived_priority": "P0"},
    {"severity": "critical", "tier": "standard",   "derived_priority": "P0"},
    {"severity": "critical", "tier": "enterprise", "derived_priority": "P0"},
    {"severity": "high",     "tier": "starter",    "derived_priority": "P2"},
    {"severity": "high",     "tier": "standard",   "derived_priority": "P1"},
    {"severity": "high",     "tier": "enterprise", "derived_priority": "P0"},
    {"severity": "medium",   "tier": "starter",    "derived_priority": "P3"},
    {"severity": "medium",   "tier": "standard",   "derived_priority": "P2"},
    {"severity": "medium",   "tier": "enterprise", "derived_priority": "P1"},
    {"severity": "low",      "tier": "starter",    "derived_priority": "P3"},
    {"severity": "low",      "tier": "standard",   "derived_priority": "P3"},
    {"severity": "low",      "tier": "enterprise", "derived_priority": "P2"},
]


async def _resolve_tier_ids(session: AsyncSession) -> dict[str, str]:
    result = await session.execute(
        text("SELECT id, name FROM auth.tier WHERE is_active = TRUE")
    )
    rows = result.fetchall()
    if not rows:
        raise RuntimeError("No tiers found in auth.tier — seed auth service first.")
    return {row.name: str(row.id) for row in rows}


async def seed_pri_sev_map() -> None:
    async with AsyncSessionFactory() as session:
        tier_map = await _resolve_tier_ids(session)
        inserted = 0
        skipped  = 0

        for entry in PRI_SEV_DATA:
            tier_name = entry["tier"]
            tier_id   = tier_map.get(tier_name)
            if not tier_id:
                logger.warning("pri_sev_seeder_tier_not_found", tier=tier_name)
                skipped += 1
                continue

            existing = await session.execute(
                select(SeverityPriorityMap).where(
                    SeverityPriorityMap.severity == entry["severity"],
                    SeverityPriorityMap.tier_id  == tier_id,
                )
            )
            if existing.scalar_one_or_none():
                logger.info(
                    "pri_sev_seeder_skip_existing",
                    severity=entry["severity"],
                    tier=tier_name,
                )
                skipped += 1
                continue

            session.add(SeverityPriorityMap(
                severity=entry["severity"],
                tier_id=tier_id,
                derived_priority=entry["derived_priority"],
            ))
            inserted += 1

        await session.commit()
        logger.info("pri_sev_seeder_done", inserted=inserted, skipped=skipped)
        print(f"Priority/Severity seeder done — inserted: {inserted}, skipped: {skipped}")


if __name__ == "__main__":
    asyncio.run(seed_pri_sev_map())