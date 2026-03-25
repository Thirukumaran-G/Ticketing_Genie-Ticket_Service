"""
ticket-service: src/core/celery/utils.py
"""

from __future__ import annotations

import httpx
from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_tier_id_cache: dict[str, str] = {}


# ── Tier resolution ───────────────────────────────────────────────────────────

async def fetch_tier_id(tier_name: str) -> str | None:
    if tier_name in _tier_id_cache:
        return _tier_id_cache[tier_name]

    url = (
        f"{settings.AUTH_SERVICE_URL}"
        f"/api/v1/auth/internal/tiers/by-name/{tier_name}"
    )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)

        if resp.status_code == 200:
            tier_id = resp.json()["tier_id"]
            _tier_id_cache[tier_name] = tier_id
            logger.info("fetch_tier_id_ok", tier_name=tier_name, tier_id=tier_id)
            return tier_id

        logger.warning(
            "fetch_tier_id_not_found",
            tier_name=tier_name,
            status=resp.status_code,
        )
    except Exception as exc:
        logger.error("fetch_tier_id_error", tier_name=tier_name, error=str(exc))

    return None


async def fetch_customer_tier(
    customer_id: str,
    product_id:  str | None = None,
) -> tuple[str, str | None]:
    url = (
        f"{settings.AUTH_SERVICE_URL}"
        f"/api/v1/auth/internal/customers/{customer_id}/tier"
    )
    params: dict = {}
    if product_id:
        params["product_id"] = product_id

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url, params=params)

        if resp.status_code == 200:
            data      = resp.json()
            tier_name = data["tier_name"]
            tier_id   = data["tier_id"]
            _tier_id_cache[tier_name] = tier_id
            logger.info(
                "fetch_customer_tier_ok",
                customer_id=customer_id,
                tier_name=tier_name,
                tier_id=tier_id,
            )
            return tier_name, tier_id

        logger.warning(
            "fetch_customer_tier_not_found",
            customer_id=customer_id,
            status=resp.status_code,
        )
    except Exception as exc:
        logger.error(
            "fetch_customer_tier_error",
            customer_id=customer_id,
            error=str(exc),
        )

    return "starter", None


# ── User / agent helpers ──────────────────────────────────────────────────────

async def fetch_user_email(user_id) -> str | None:
    """
    Fetch email address for any user (agent, team lead, customer).
    user_id accepts str or UUID — always coerced to str for the URL.
    """
    url = (
        f"{settings.AUTH_SERVICE_URL}"
        f"/api/v1/auth/internal/users/{str(user_id)}/email"
    )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
        if resp.status_code == 200:
            return resp.json().get("email")
        logger.warning(
            "fetch_user_email_not_found",
            user_id=str(user_id),
            status=resp.status_code,
        )
    except Exception as exc:
        logger.warning("fetch_user_email_error", user_id=str(user_id), error=str(exc))
    return None


async def fetch_customer_email(customer_id: str) -> str | None:
    """Alias — customers are regular users in auth-service."""
    return await fetch_user_email(customer_id)


async def fetch_agent_name(user_id) -> str | None:
    """
    Fetch full_name for an agent or team lead from auth-service.
    Uses /internal/users/{user_id} which returns the full user object
    including full_name. The /email endpoint does NOT return full_name.
    user_id accepts str or UUID — always coerced to str for the URL.
    """
    url = (
        f"{settings.AUTH_SERVICE_URL}"
        f"/api/v1/auth/internal/users/{str(user_id)}"   # FIX: was /email, now /users/{id}
    )
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
        if resp.status_code == 200:
            return resp.json().get("full_name")
        logger.warning(
            "fetch_agent_name_not_found",
            user_id=str(user_id),
            status=resp.status_code,
        )
    except Exception as exc:
        logger.warning("fetch_agent_name_error", user_id=str(user_id), error=str(exc))
    return None


async def fetch_product_info(product_id: str) -> tuple[str | None, str | None]:
    import uuid as _uuid
    from sqlalchemy import text

    try:
        from src.data.clients.postgres_client import CelerySessionFactory
        async with CelerySessionFactory() as session:
            result = await session.execute(
                text("SELECT name, description FROM auth.product WHERE id = :pid"),
                {"pid": _uuid.UUID(product_id)},
            )
            row = result.fetchone()
            if row:
                logger.info(
                    "fetch_product_info_ok",
                    product_id=product_id,
                    name=row.name,
                )
                return row.name, row.description
            logger.warning("fetch_product_info_not_found", product_id=product_id)
    except Exception as exc:
        logger.error("fetch_product_info_error", product_id=product_id, error=str(exc))

    return None, None