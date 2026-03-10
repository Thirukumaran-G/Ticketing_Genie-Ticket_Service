"""
Seed script — SLA rules (ticket.sla_rule)
Run: uv run python -m src.scripts.seeders.sla_seeder
"""

from __future__ import annotations

import asyncio

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.clients.postgres_client import AsyncSessionFactory
from src.data.models.postgres.models import SLARule
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# tier_name → tier_id resolved at runtime from auth.tier
SLA_DATA: list[dict] = [
    # starter
    {"tier": "starter",    "priority": "P0", "response_time_min": 60,   "resolution_time_min": 480},
    {"tier": "starter",    "priority": "P1", "response_time_min": 240,  "resolution_time_min": 1440},
    {"tier": "starter",    "priority": "P2", "response_time_min": 480,  "resolution_time_min": 2880},
    {"tier": "starter",    "priority": "P3", "response_time_min": 1440, "resolution_time_min": 5760},
    # standard
    {"tier": "standard",   "priority": "P0", "response_time_min": 30,   "resolution_time_min": 240},
    {"tier": "standard",   "priority": "P1", "response_time_min": 120,  "resolution_time_min": 720},
    {"tier": "standard",   "priority": "P2", "response_time_min": 240,  "resolution_time_min": 1440},
    {"tier": "standard",   "priority": "P3", "response_time_min": 480,  "resolution_time_min": 2880},
    # enterprise
    {"tier": "enterprise", "priority": "P0", "response_time_min": 15,   "resolution_time_min": 120},
    {"tier": "enterprise", "priority": "P1", "response_time_min": 60,   "resolution_time_min": 480},
    {"tier": "enterprise", "priority": "P2", "response_time_min": 120,  "resolution_time_min": 960},
    {"tier": "enterprise", "priority": "P3", "response_time_min": 240,  "resolution_time_min": 1440},
]


async def _resolve_tier_ids(session: AsyncSession) -> dict[str, str]:
    """Fetch tier name → id map from auth.tier."""
    result = await session.execute(
        text("SELECT id, name FROM auth.tier WHERE is_active = TRUE")
    )
    rows = result.fetchall()
    if not rows:
        raise RuntimeError("No tiers found in auth.tier — seed auth service first.")
    return {row.name: str(row.id) for row in rows}


async def seed_sla_rules() -> None:
    async with AsyncSessionFactory() as session:
        tier_map = await _resolve_tier_ids(session)
        inserted = 0
        skipped = 0

        for entry in SLA_DATA:
            tier_name = entry["tier"]
            tier_id   = tier_map.get(tier_name)
            if not tier_id:
                logger.warning("sla_seeder_tier_not_found", tier=tier_name)
                skipped += 1
                continue

            existing = await session.execute(
                select(SLARule).where(
                    SLARule.tier_id == tier_id,
                    SLARule.priority == entry["priority"],
                )
            )
            if existing.scalar_one_or_none():
                logger.info("sla_seeder_skip_existing", tier=tier_name, priority=entry["priority"])
                skipped += 1
                continue

            session.add(SLARule(
                tier_id=tier_id,
                priority=entry["priority"],
                response_time_min=entry["response_time_min"],
                resolution_time_min=entry["resolution_time_min"],
            ))
            inserted += 1

        await session.commit()
        logger.info("sla_seeder_done", inserted=inserted, skipped=skipped)
        print(f"SLA seeder done — inserted: {inserted}, skipped: {skipped}")


if __name__ == "__main__":
    asyncio.run(seed_sla_rules())