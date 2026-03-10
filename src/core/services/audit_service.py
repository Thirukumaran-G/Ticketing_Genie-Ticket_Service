from datetime import datetime, timezone
from typing import Any

from src.data.clients.mongo_client import get_audit_collection
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class AuditLogService:
    """Persists audit events to MongoDB audit_log collection."""

    async def log(
        self,
        entity_type: str,
        entity_id: int,
        action: str,
        actor_id: int,
        actor_type: str,
        old_value: dict | None = None,
        new_value: dict | None = None,
        changed_fields: list[str] | None = None,
        reason: str | None = None,
        ticket_id: int | None = None,
    ) -> None:
        """Write an audit log entry to MongoDB."""
        collection = get_audit_collection()
        document: dict[str, Any] = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "action": action,
            "actor_id": actor_id,
            "actor_type": actor_type,
            "old_value": old_value,
            "new_value": new_value,
            "changed_fields": changed_fields,
            "reason": reason,
            "ticket_id": ticket_id,
            "created_at": datetime.now(timezone.utc),
        }
        try:
            await collection.insert_one(document)
        except Exception as exc:
            # Never let audit failure break the main flow
            logger.error("audit_log_write_failed", error=str(exc), action=action)
            
audit_service = AuditLogService()