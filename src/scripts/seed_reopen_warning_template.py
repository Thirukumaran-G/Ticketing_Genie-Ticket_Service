from __future__ import annotations

import asyncio
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

TEMPLATES = [
    {
        "key":     "reopen_warning",
        "name":    "Reopen Warning",
        "subject": "Important notice regarding your ticket [{ticket_number}]",
        "body": (
            "Dear {customer_name},\n\n"
            "We noticed that your ticket {ticket_number} — {ticket_title} "
            "has been reopened {reopen_count} times.\n\n"
            "To help us serve you better, we kindly request that you raise a "
            "new ticket for any new or ongoing issues rather than reopening a "
            "previously closed one.\n\n"
            "This ensures your issue gets the correct priority and is handled "
            "by the right team without delay.\n\n"
            "{custom_message}\n\n"
            "Thank you for your understanding.\n\n"
            "Best regards,\n"
            "{team_lead_name}\n"
            "Support Team Lead\n"
            "Ticketing Genie"
        ),
        "variables": [
            "customer_name",
            "ticket_number",
            "ticket_title",
            "reopen_count",
            "custom_message",
            "team_lead_name",
        ],
        "is_active": True,
    }
]


async def seed() -> None:
    from src.data.clients.postgres_client import CelerySessionFactory
    from src.data.models.postgres.models import NotificationTemplate
    from sqlalchemy import select

    async with CelerySessionFactory() as session:
        for tpl_data in TEMPLATES:
            existing = await session.execute(
                select(NotificationTemplate).where(
                    NotificationTemplate.key == tpl_data["key"]
                )
            )
            row = existing.scalar_one_or_none()
            if row:
                row.name      = tpl_data["name"]
                row.variables = tpl_data["variables"]
                row.is_active = tpl_data["is_active"]
                logger.info("reopen_warning_template_exists_skipping_body", key=tpl_data["key"])
            else:
                import uuid6
                new_tpl = NotificationTemplate(
                    id=uuid6.uuid7(),
                    key=tpl_data["key"],
                    name=tpl_data["name"],
                    subject=tpl_data["subject"],
                    body=tpl_data["body"],
                    variables=tpl_data["variables"],
                    is_active=tpl_data["is_active"],
                )
                session.add(new_tpl)
                logger.info("reopen_warning_template_seeded", key=tpl_data["key"])

        await session.commit()
        logger.info("reopen_warning_seed_complete")


if __name__ == "__main__":
    asyncio.run(seed())