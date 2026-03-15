"""
Assignment lock — prevents double-assignment of tickets during concurrent
Celery task execution and manual TL assignment.

Uses Redis SET NX PX (atomic set-if-not-exists with TTL).
Lock key:  assign_lock:{ticket_id}
TTL:       10 seconds (LOCK_TTL_MS)
Retry:     up to MAX_RETRIES times, RETRY_DELAY_S seconds apart
On failure after retries: raises AssignmentLockError — caller routes
          ticket to TL unassigned queue instead of crashing.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis

from src.config.settings import settings
from src.core.celery.loop import get_worker_loop
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

LOCK_TTL_MS   = 10_000
RETRY_DELAY_S = 2.0
MAX_RETRIES   = 3

_redis_client: aioredis.Redis | None = None


def _get_redis() -> aioredis.Redis:
    """Return Redis client bound to the persistent worker loop."""
    global _redis_client
    loop = get_worker_loop()
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            settings.CELERY_BROKER_URL,
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis_client


def _lock_key(ticket_id: str) -> str:
    return f"assign_lock:{ticket_id}"


class AssignmentLockError(Exception):
    """Raised when a ticket lock cannot be acquired after all retries."""


async def acquire_lock(ticket_id: str) -> bool:
    redis  = _get_redis()
    key    = _lock_key(ticket_id)
    result = await redis.set(key, "1", px=LOCK_TTL_MS, nx=True)
    return result is True


async def release_lock(ticket_id: str) -> None:
    try:
        redis = _get_redis()
        await redis.delete(_lock_key(ticket_id))
    except Exception as exc:
        logger.warning(
            "assignment_lock_release_failed",
            ticket_id=ticket_id,
            error=str(exc),
        )


@asynccontextmanager
async def assignment_lock(ticket_id: str) -> AsyncIterator[None]:
    acquired  = False
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            acquired = await acquire_lock(ticket_id)
            if acquired:
                logger.info(
                    "assignment_lock_acquired",
                    ticket_id=ticket_id,
                    attempt=attempt,
                )
                break
            logger.warning(
                "assignment_lock_contention",
                ticket_id=ticket_id,
                attempt=attempt,
                retry_in_s=RETRY_DELAY_S,
            )
            await asyncio.sleep(RETRY_DELAY_S)
        except Exception as exc:
            last_exc = exc
            logger.error(
                "assignment_lock_acquire_error",
                ticket_id=ticket_id,
                attempt=attempt,
                error=str(exc),
            )
            await asyncio.sleep(RETRY_DELAY_S)

    if not acquired:
        raise AssignmentLockError(
            f"Could not acquire assignment lock for ticket {ticket_id} "
            f"after {MAX_RETRIES} attempts"
        ) from last_exc

    try:
        yield
    finally:
        if acquired:
            await release_lock(ticket_id)
            logger.info("assignment_lock_released", ticket_id=ticket_id)