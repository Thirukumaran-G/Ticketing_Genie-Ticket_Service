from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class NotificationPreferenceRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_preferred_contact(self, user_id: str) -> str:
        """
        Returns 'email' or 'in_app'.
        Defaults to 'in_app' if not set or if the cross-schema query fails —
        this ensures SSE notifications always fire even when auth schema is
        unreachable from the ticket service DB connection.
        """
        try:
            result = await self._session.execute(
                text("SELECT preferred_contact FROM auth.user WHERE id = :uid"),
                {"uid": user_id},
            )
            row = result.fetchone()
            if not row or not row.preferred_contact:
                logger.info(
                    "preference_not_found_defaulting_in_app",
                    user_id=user_id,
                )
                return "in_app"
            return row.preferred_contact
        except Exception as exc:
            logger.warning(
                "get_preferred_contact_failed_defaulting_in_app",
                user_id=user_id,
                error=str(exc),
            )
            return "in_app"

    async def set_preferred_contact(self, user_id: str, value: str) -> str:
        """Explicitly sets preferred_contact to 'email' or 'in_app'."""
        try:
            await self._session.execute(
                text("UPDATE auth.user SET preferred_contact = :val WHERE id = :uid"),
                {"val": value, "uid": user_id},
            )
            logger.info("notification_preference_set", user_id=user_id, new=value)
        except Exception as exc:
            logger.warning(
                "set_preferred_contact_failed",
                user_id=user_id,
                error=str(exc),
            )
        return value