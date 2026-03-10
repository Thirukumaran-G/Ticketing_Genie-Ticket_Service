"""
Seed script — keyword rules (ticket.keyword_rule)
Run: uv run python -m src.scripts.seeders.keyword_seeder
"""

from __future__ import annotations

import asyncio

from sqlalchemy import select

from src.data.clients.postgres_client import AsyncSessionFactory
from src.data.models.postgres.models import KeywordRule
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

KEYWORD_DATA: list[dict] = [
    # critical
    {"keyword": "outage",            "severity": "critical"},
    {"keyword": "down",              "severity": "critical"},
    {"keyword": "data loss",         "severity": "critical"},
    {"keyword": "not working",       "severity": "critical"},
    {"keyword": "completely broken", "severity": "critical"},
    {"keyword": "production down",   "severity": "critical"},
    {"keyword": "system down",       "severity": "critical"},
    # high
    {"keyword": "unable to pay",     "severity": "high"},
    {"keyword": "payment failed",    "severity": "high"},
    {"keyword": "login failure",     "severity": "high"},
    {"keyword": "cannot login",      "severity": "high"},
    {"keyword": "access denied",     "severity": "high"},
    {"keyword": "broken",            "severity": "high"},
    {"keyword": "not loading",       "severity": "high"},
    {"keyword": "500 error",         "severity": "high"},
    # medium
    {"keyword": "slow",              "severity": "medium"},
    {"keyword": "ui bug",            "severity": "medium"},
    {"keyword": "incorrect data",    "severity": "medium"},
    {"keyword": "wrong result",      "severity": "medium"},
    {"keyword": "not displaying",    "severity": "medium"},
    {"keyword": "error message",     "severity": "medium"},
    {"keyword": "unexpected behavior","severity": "medium"},
    # low
    {"keyword": "feature request",   "severity": "low"},
    {"keyword": "question",          "severity": "low"},
    {"keyword": "how to",            "severity": "low"},
    {"keyword": "documentation",     "severity": "low"},
    {"keyword": "suggestion",        "severity": "low"},
    {"keyword": "minor",             "severity": "low"},
    {"keyword": "cosmetic",          "severity": "low"},
    {"keyword": "typo",              "severity": "low"},
]


async def seed_keyword_rules() -> None:
    async with AsyncSessionFactory() as session:
        inserted = 0
        skipped  = 0

        for entry in KEYWORD_DATA:
            existing = await session.execute(
                select(KeywordRule).where(KeywordRule.keyword == entry["keyword"])
            )
            if existing.scalar_one_or_none():
                logger.info("keyword_seeder_skip_existing", keyword=entry["keyword"])
                skipped += 1
                continue

            session.add(KeywordRule(
                keyword=entry["keyword"],
                severity=entry["severity"],
            ))
            inserted += 1

        await session.commit()
        logger.info("keyword_seeder_done", inserted=inserted, skipped=skipped)
        print(f"Keyword seeder done — inserted: {inserted}, skipped: {skipped}")


if __name__ == "__main__":
    asyncio.run(seed_keyword_rules())