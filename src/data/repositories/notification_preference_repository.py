# src/data/repository/notification_preference_repository.py
from __future__ import annotations

from src.handlers.http_clients.auth_client import AuthHttpClient
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class NotificationPreferenceRepository:
    """
    Fetches / sets notification preferences via the auth-service HTTP API.
    No direct cross-schema DB queries — auth schema stays fully owned by
    the auth service.
    """

    def __init__(self) -> None:
        self._client = AuthHttpClient()

    async def get_preferred_contact(self, user_id: str) -> str:
        """
        Returns 'email' or 'in_app'.
        Defaults to 'in_app' if the auth service is unreachable or user
        has no preference set.
        """
        try:
            data = await self._client.get_user(user_id)    
            preference = data.get("preferred_contact")
            if not preference:
                logger.info(
                    "preference_not_found_defaulting_in_app",
                    user_id=user_id,
                )
                return "in_app"
            return preference
        except Exception as exc:
            logger.warning(
                "get_preferred_contact_failed_defaulting_in_app",
                user_id=user_id,
                error=str(exc),
            )
            return "in_app"

    async def set_preferred_contact(self, user_id: str, value: str) -> str:
        """
        Delegates preference update to auth service.
        Falls back silently — notification will still fire via in_app.
        """
        try:
            await self._client.set_user_preferred_contact(user_id, value)
            logger.info("notification_preference_set", user_id=user_id, new=value)
        except Exception as exc:
            logger.warning(
                "set_preferred_contact_failed",
                user_id=user_id,
                error=str(exc),
            )
        return value