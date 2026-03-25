from __future__ import annotations

import httpx

from src.config.settings import settings
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class AuthHttpClient:
    """Thin async HTTP wrapper around auth-service internal API."""

    def __init__(self) -> None:
        self._base_url = settings.AUTH_SERVICE_URL
        self._timeout  = 5.0

    # ── User lookup ────────────────────────────────────────────────────────────

    async def get_user_by_id(self, user_id: str) -> dict | None:
        url = f"{self._base_url}/api/v1/auth/internal/users/{user_id}"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
            logger.warning("auth_get_user_failed", user_id=user_id, status=resp.status_code)
            return None
        except httpx.TimeoutException:
            logger.error("auth_get_user_timeout", user_id=user_id)
            return None
        except Exception as exc:
            logger.error("auth_get_user_error", user_id=user_id, error=str(exc))
            return None

    async def get_user_email(self, user_id: str) -> str | None:
        user = await self.get_user_by_id(user_id)
        return user.get("email") if user else None

    async def get_customer_email(self, customer_id: str) -> str | None:
        """Alias used by EmailClient.resolve_email."""
        return await self.get_user_email(customer_id)

    async def get_user(self, user_id: str) -> dict:
        """GET /internal/users/{user_id} — includes preferred_contact field."""
        url = f"{self._base_url}/api/v1/auth/internal/users/{user_id}"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
            logger.warning("auth_get_user_full_failed", user_id=user_id, status=resp.status_code)
            return {}
        except httpx.TimeoutException:
            logger.error("auth_get_user_full_timeout", user_id=user_id)
            return {}
        except Exception as exc:
            logger.error("auth_get_user_full_error", user_id=user_id, error=str(exc))
            return {}

    async def set_user_preferred_contact(self, user_id: str, value: str) -> None:
        """PATCH /internal/users/{user_id}/preferred-contact"""
        url = f"{self._base_url}/api/v1/auth/internal/users/{user_id}/preferred-contact"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.patch(url, json={"preferred_contact": value})
            if resp.status_code not in (200, 204):
                logger.warning(
                    "auth_set_preferred_contact_failed",
                    user_id=user_id,
                    status=resp.status_code,
                )
        except httpx.TimeoutException:
            logger.error("auth_set_preferred_contact_timeout", user_id=user_id)
        except Exception as exc:
            logger.error("auth_set_preferred_contact_error", user_id=user_id, error=str(exc))

    # ── Tiers / Products ──────────────────────────────────────────────────────

    async def get_tiers(self, token: str) -> list[dict]:
        """GET /api/v1/admin/tiers — list tiers from auth-service."""
        url = f"{self._base_url}/api/v1/admin/tiers"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            if resp.status_code == 200:
                return resp.json()
            logger.warning("auth_get_tiers_failed", status=resp.status_code)
            return []
        except Exception as exc:
            logger.error("auth_get_tiers_error", error=str(exc))
            return []

    async def get_products(self, token: str) -> list[dict]:
        """GET /api/v1/admin/products — list products from auth-service."""
        url = f"{self._base_url}/api/v1/admin/products"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            if resp.status_code == 200:
                return resp.json()
            logger.warning("auth_get_products_failed", status=resp.status_code)
            return []
        except Exception as exc:
            logger.error("auth_get_products_error", error=str(exc))
            return []