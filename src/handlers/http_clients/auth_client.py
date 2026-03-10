"""
ticket-service: src/handlers/http_clients/auth_client.py
"""

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

    # ── Token validation ───────────────────────────────────────────────────────

    async def validate_token(self, token: str) -> dict | None:
        """
        POST /api/v1/auth/internal/validate

        Returns:
        {
            "valid": true,
            "actor_id": "uuid",
            "role": "customer|agent|support_team_lead|admin",
            "email": "user@example.com",
            "customer_tier": "starter|standard|enterprise|null",
            "customer_id": "uuid|null"
        }
        """
        url = f"{self._base_url}/api/v1/auth/internal/validate"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(url, json={"refresh_token": token})
            if resp.status_code == 200:
                return resp.json()
            logger.warning("auth_validate_token_failed", status=resp.status_code, detail=resp.text[:200])
            return None
        except httpx.TimeoutException:
            logger.error("auth_validate_token_timeout", url=url)
            return None
        except Exception as exc:
            logger.error("auth_validate_token_error", error=str(exc))
            return None

    # ── User lookup ────────────────────────────────────────────────────────────

    async def get_user_by_id(self, user_id: str) -> dict | None:
        """
        GET /api/v1/auth/internal/users/{user_id}

        Returns:
        {
            "id": "uuid",
            "email": "user@example.com",
            "full_name": "Jane Doe",
            "role": "customer",
            "is_active": true
        }

        ── Auth-service endpoint to add ─────────────────────────────────────────
        @router.get("/internal/users/{user_id}")
        async def get_user_internal(user_id: str, session: AsyncSession = Depends(get_db)):
            user = await session.get(User, uuid.UUID(user_id))
            if not user:
                raise HTTPException(404)
            role = await session.get(Role, user.role_id)
            return {
                "id":        str(user.id),
                "email":     user.email,
                "full_name": user.full_name,
                "role":      role.name if role else None,
                "is_active": user.is_active,
            }
        ─────────────────────────────────────────────────────────────────────────
        """
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